#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "zmalloc.h"
#include "serverassert.h"
#include "allocator_defrag.h"

#define UNUSED(x) (void)(x)

#define UTILIZATION_BUFFER_SIZE 1000
#define ALPHA 0.1 // EWMA factor, adjust as needed

typedef struct {
    double ewma;
    double utilization_buffer[UTILIZATION_BUFFER_SIZE];
    int buffer_index;
    int buffer_full;
} UtilizationTracker;

double getEWMAUtilization(UtilizationTracker *tracker);
double getP50Utilization(UtilizationTracker *tracker);
void updateUtilization(UtilizationTracker *tracker, double utilization);
void initUtilizationTracker(UtilizationTracker *tracker);

#if defined(HAVE_DEFRAG) && defined(USE_JEMALLOC)

#define STRINGIFY_(x) #x
#define STRINGIFY(x) STRINGIFY_(x)


#define SLAB_NFREE(out, i) out[(i) * 3]
#define SLAB_LEN(out, i) out[(i) * 3 + 2]
#define SLAB_NUM_REGS(out, i) out[(i) * 3 + 1]

#define UTILIZATION_THRESHOLD_FACTOR_MILI (1 / 8) // 12.5% additional utilization

typedef struct {
    size_t mib[6];
    size_t miblen;
} JeMallctlMib;

/// @brief Helper struct to store MIB (Management Information Base) information for jemalloc bin queries.
typedef struct {
    JeMallctlMib curr_slabs;
    JeMallctlMib nonfull_slabs;
    JeMallctlMib curr_regs;
    JeMallctlMib nmalloc;
    JeMallctlMib ndealloc;
} JeBinQueryHelper;

/// @brief Struct representing bin information.
typedef struct JeBinInfo {
    unsigned long reg_size;        ///< Size of each region in the bin.
    unsigned long nregs;           ///< Total number of regions in the bin.
    unsigned long len;             ///< Length or size of the bin (unused in this implementation).
    JeBinQueryHelper query_helper; ///< Helper struct containing MIB information for bin queries.
} JeBinInfo;

/// @brief Struct representing defragmentation statistics for a bin.
typedef struct JeDefragBinStats {
    unsigned long bhits;    ///< Number of hits (regions that should be defragmented).
    unsigned long bmisses;  ///< Number of misses (regions that should not be defragmented).
    unsigned long nmalloc;  ///< Number of malloc operations (unused in this implementation).
    unsigned long ndealloc; ///< Number of dealloc operations (unused in this implementation).
} JeDefragBinStats;

/// @brief Struct representing overall defragmentation statistics.
typedef struct JeDefragStats {
    unsigned long hits;       ///< Total number of hits (regions that should be defragmented).
    unsigned long misses;     ///< Total number of misses (regions that should not be defragmented).
    unsigned long hit_bytes;  ///< Total number of bytes that should be defragmented.
    unsigned long miss_bytes; ///< Total number of bytes that should not be defragmented.
    unsigned long ncalls;     ///< Number of calls to the defragmentation function.
    unsigned long nptrs;      ///< Total number of pointers analyzed for defragmentation.
} JeDefragStats;

/// @brief Struct representing the latest usage information for a bin.
typedef struct JeBinUsage {
    unsigned long curr_slabs;         ///< Current number of slabs in the bin.
    unsigned long curr_nonfull_slabs; ///< Current number of non-full slabs in the bin.
    unsigned long curr_full_slabs;    ///< Current number of full slabs in the bin (calculated from other fields).
    unsigned long curr_regs;          ///< Current number of regions in the bin.

    unsigned long cycle_max_regs_threshold;
    unsigned long cycle_num_misses;
    unsigned long cycle_num_hits;
    unsigned long cycle_hits_to_reach_target;
    UtilizationTracker utilization_tracker;
    JeDefragBinStats stat; ///< Defragmentation statistics for the bin.
} JeBinUsage;

/// @brief Struct representing the latest usage information across all bins.
typedef struct JeUsageLatest {
    JeBinUsage *bins_usage; ///< Array of bin usage information structs.
    JeDefragStats stats;    ///< Overall defragmentation statistics.
} JeUsageLatest;

/// @brief Struct representing the configuration for jemalloc bins.
typedef struct JeBinsConfig {
    unsigned long nbins; ///< Number of bins in the configuration.
    JeBinInfo *bin_info; ///< Array of bin information structs.
    JeMallctlMib util_batch_query;
    JeMallctlMib util_query;
    DefragStrategy defrag_strategy;
    DefragSelectionStrategy selection_strategy;
    DefragRulesRecalc recalc_rule;
    DefragRulesFree free_rule;
    DefragRulesAlloc alloc_rule;
    int select_threshold_factor; // -/+ % of utilization
} JeBinsConfig;

static int defrag_supported = 0;
static size_t jemalloc_quantum = 0;
static JeBinsConfig arena_bin_conf = {0};
static JeUsageLatest usage_latest = {0};


/* -----------------------------------------------------------------------------
 * Alloc/Free API that are cooperative with defrag
 * -------------------------------------------------------------------------- */
/**
 * Use separate tcache for freeing memory to defrag. This tcache is only used for free and no allocation will be
 * done from it. Using this tcache enables us to use normal allocation during defrag reducing the overhead of alloc.
 * Free is also improved because the access to the arena is amortized.
 * */

static __thread int free_tcache_id = -1;
static __thread int alloc_tcache_id = -1;
/* Allocation and free functions that bypass the thread cache
 * and go straight to the allocator arena bins.
 * Currently implemented only for jemalloc. Used for online defragmentation. */
void *defrag_jemalloc_alloc(size_t size) {
    if (arena_bin_conf.alloc_rule == RULE_ALLOC_USE_UD_TCACHE) {
        size_t sz = sizeof(int);
        // initiliza thread cache in case it's not init already
        if (alloc_tcache_id == -1) assert(!je_mallctl("tcache.create", &alloc_tcache_id, &sz, NULL, 0));
        return je_mallocx(size, MALLOCX_TCACHE(alloc_tcache_id));
    } else if (arena_bin_conf.alloc_rule == RULE_ALLOC_USE_TCACHE) {
        return je_mallocx(size, 0);
    } else {
        return je_mallocx(size, MALLOCX_TCACHE_NONE);
    }
}

// free ptr, make sure it is freed to arena before reallocated.
void defrag_jemalloc_free(void *ptr, size_t size) {
    if (ptr == NULL) return;
    if (arena_bin_conf.free_rule == RULE_FREE_USE_UD_TCACHE) {
        size_t sz = sizeof(int);
        // initiliza thread cache in case it's not init already
        if (free_tcache_id == -1) assert(!je_mallctl("tcache.create", &free_tcache_id, &sz, NULL, 0));
        je_sdallocx(ptr, size, MALLOCX_TCACHE(free_tcache_id));
    } else if (arena_bin_conf.free_rule == RULE_FREE_USE_TCACHE) {
        je_sdallocx(ptr, size, 0);
    } else {
        je_sdallocx(ptr, size, MALLOCX_TCACHE_NONE);
    }
}

/* -----------------------------------------------------------------------------
 * Helper functions for jemalloc translation between size and index
 * -------------------------------------------------------------------------- */
#define LG_QUANTOM_8_FIRST_POW2 3
#define SIZE_CLASS_GROUP_SZ 4

#define LG_QUANTOM_OFFSET_3 ((64 >> LG_QUANTOM_8_FIRST_POW2) - 1)
#define LG_QUANTOM_OFFSET_4 (64 >> 4)

#define get_binind_normal(_sz, _offset, _last_sz_pow2)                                                                 \
    ((SIZE_CLASS_GROUP_SZ - (((1 << (_last_sz_pow2)) - (_sz)) >> ((_last_sz_pow2) - LG_QUANTOM_8_FIRST_POW2))) +       \
     (((_last_sz_pow2) - (LG_QUANTOM_8_FIRST_POW2 + 3)) - 1) * SIZE_CLASS_GROUP_SZ + (_offset))
/* Get the bin index in bin array from the reg_size.
 *
 * these are reverse engineered mapping of reg_size -> binind. We need this information because the utilization query
 * returns the size of the buffer and not the bin index, and we need the bin index to access it's usage information
 *
 * Note: In case future PR will return the binind (that is better API anyway) we can get rid of
 * these conversion functions*/
inline unsigned jemalloc_sz2binind_lgq3(size_t sz) {
    if (sz <= (1 << (LG_QUANTOM_8_FIRST_POW2 + 3))) {
        // for sizes: 8, 16, 24, 32, 40, 48, 56, 64
        return (sz >> 3) - 1;
    }
    // following groups have SIZE_CLASS_GROUP_SZ size-class that are
    unsigned long long last_sz_in_group_pow2 = 64 - __builtin_clzll(sz - 1);
    return get_binind_normal(sz, LG_QUANTOM_OFFSET_3, last_sz_in_group_pow2);
}

inline unsigned jemalloc_sz2binind_lgq4(size_t sz) {
    if (sz <= (1 << (LG_QUANTOM_8_FIRST_POW2 + 3))) {
        // for sizes: 8, 16, 32, 48, 64
        return (sz >> 4);
    }
    // following groups have SIZE_CLASS_GROUP_SZ size-class that are
    unsigned long long last_sz_in_group_pow2 = 64 - __builtin_clzll(sz - 1);
    return get_binind_normal(sz, LG_QUANTOM_OFFSET_4, last_sz_in_group_pow2);
}

/* -----------------------------------------------------------------------------
 * Get INFO string about the defrag.
 * -------------------------------------------------------------------------- */
/*
 * add defrag info string into info
 */
// sds allocatorGetDefragInfo(sds info) {
//     if (!defrag_supported) return info;
//     JeBinInfo *binfo;
//     JeBinUsage *busage;
//     unsigned nbins = arena_bin_conf.nbins;
//     if (nbins > 0) {
//         info = sdscatprintf(info,
//                             "jemalloc_quantum:%d\r\n"
//                             "hit_ratio:%lu%%,hits:%lu,misses:%lu\r\n"
//                             "hit_bytes:%lu,miss_bytes:%lu\r\n"
//                             "ncalls_util_batches:%lu,ncalls_util_ptrs:%lu\r\n",
//                             (int)jemalloc_quantum,
//                             (usage_latest.stats.hits + usage_latest.stats.misses)
//                                 ? usage_latest.stats.hits / (usage_latest.stats.hits + usage_latest.stats.misses)
//                                 : 0,
//                             usage_latest.stats.hits, usage_latest.stats.misses, usage_latest.stats.hit_bytes,
//                             usage_latest.stats.miss_bytes, usage_latest.stats.ncalls, usage_latest.stats.nptrs);
//         for (unsigned j = 0; j < nbins; j++) {
//             binfo = &arena_bin_conf.bin_info[j];
//             busage = &usage_latest.bins_usage[j];
//             info = sdscatprintf(info,
//                                 "[%d][%lu]::"
//                                 "nregs:%lu,nslabs:%lu,nnonfull:%lu,"
//                                 "hit_rate:%lu%%,hit:%lu,miss:%lu,nmalloc:%lu,ndealloc:%lu\r\n",
//                                 j, binfo->reg_size, busage->curr_regs, busage->curr_slabs,
//                                 busage->curr_nonfull_slabs, (busage->stat.bhits + busage->stat.bmisses)
//                                     ? busage->stat.bhits / (busage->stat.bhits + busage->stat.bmisses)
//                                     : 0,
//                                 busage->stat.bhits, busage->stat.bmisses, busage->stat.nmalloc,
//                                 busage->stat.ndealloc);
//         }
//     }
//     return info;
// }

/* -----------------------------------------------------------------------------
 * Interface functions to get fragmentation info from jemalloc
 * -------------------------------------------------------------------------- */
#define ARENA_TO_QUERY MALLCTL_ARENAS_ALL

void allocatorSetStrategyConfig(DefragStrategy defrag_strategy) {
    arena_bin_conf.defrag_strategy = defrag_strategy;
}
void allocatorSetSelectConfig(DefragSelectionStrategy selection_strategy) {
    arena_bin_conf.selection_strategy = selection_strategy;
}
void allocatorSetRefreshConfig(DefragRulesRecalc recalc) {
    arena_bin_conf.recalc_rule = recalc;
}
void allocatorSetFreeConfig(DefragRulesFree recalc) {
    arena_bin_conf.free_rule = recalc;
}
void allocatorSetAllocConfig(DefragRulesAlloc recalc) {
    arena_bin_conf.alloc_rule = recalc;
}

void allocatorSetThresholdConfig(int threshold) {
    arena_bin_conf.select_threshold_factor = threshold;
}
/* -----------------------------------------------------------------------------
 * Interface functions to get fragmentation info from jemalloc
 * -------------------------------------------------------------------------- */

static int initJemallocCtlMib(const char *name, JeMallctlMib *mib) {
    mib->miblen = sizeof(mib->mib) / sizeof(size_t);
    return je_mallctlnametomib(name, mib->mib, &mib->miblen);
}

static int queryJemallocCtlMib(const JeMallctlMib *mib, void *value, size_t *size) {
    return je_mallctlbymib(mib->mib, mib->miblen, value, size, NULL, 0);
}

static int initBinQueryHelper(JeBinQueryHelper *helper, unsigned bin_index) {
    char buf[128];
    snprintf(buf, sizeof(buf), "stats.arenas.%d.bins.%u.curregs", ARENA_TO_QUERY, bin_index);
    if (initJemallocCtlMib(buf, &helper->curr_regs) != 0) return -1;

    snprintf(buf, sizeof(buf), "stats.arenas.%d.bins.%u.curslabs", ARENA_TO_QUERY, bin_index);
    if (initJemallocCtlMib(buf, &helper->curr_slabs) != 0) return -1;

    snprintf(buf, sizeof(buf), "stats.arenas.%d.bins.%u.nonfull_slabs", ARENA_TO_QUERY, bin_index);
    if (initJemallocCtlMib(buf, &helper->nonfull_slabs) != 0) return -1;

    snprintf(buf, sizeof(buf), "stats.arenas.%d.bins.%u.nmalloc", ARENA_TO_QUERY, bin_index);
    if (initJemallocCtlMib(buf, &helper->nmalloc) != 0) return -1;

    snprintf(buf, sizeof(buf), "stats.arenas.%d.bins.%u.ndalloc", ARENA_TO_QUERY, bin_index);
    if (initJemallocCtlMib(buf, &helper->ndealloc) != 0) return -1;

    return 0;
}

/**
 * @brief Initializes the defragmentation module for the jemalloc memory allocator.
 *
 * This function performs the necessary setup and initialization steps for the defragmentation module.
 * It retrieves the configuration information for the jemalloc arenas and bins, and initializes the usage
 * statistics data structure.
 *
 * @return 0 on success, or a non-zero error code on failure.
 *
 * The initialization process involves the following steps:
 * 1. Check if defragmentation is supported by the current jemalloc version.
 * 2. Retrieve the arena bin configuration information using the `je_mallctlbymib` function.
 * 3. Initialize the `usage_latest` structure with the bin usage statistics and configuration data.
 * 4. Set the `defrag_supported` flag to indicate that defragmentation is enabled.
 *
 * Note: This function must be called before using any other defragmentation-related functionality.
 * It should be called during the initialization phase of the application or module that uses the
 * defragmentation feature.
 */
int allocatorDefragInit(void) {
    if (defrag_supported) return 0;
    unsigned long long epoch = 1;
    size_t sz = sizeof(epoch);
    je_mallctl("epoch", &epoch, &sz, &epoch, sz);
    size_t len = sizeof(jemalloc_quantum);
    assert(!je_mallctl("arenas.quantum", &jemalloc_quantum, &len, NULL, 0));
    // lg-quantom can be 3 or 4
    assert((jemalloc_quantum == 8) || (jemalloc_quantum == 16));

    unsigned nbins;
    sz = sizeof(nbins);
    if (je_mallctl("arenas.nbins", &nbins, &sz, NULL, 0) != 0) goto error;
    arena_bin_conf.bin_info = zcalloc(sizeof(JeBinInfo) * nbins);
    for (unsigned j = 0; j < nbins; j++) {
        JeBinInfo *binfo = &arena_bin_conf.bin_info[j];
        char buf[128];

        snprintf(buf, sizeof(buf), "arenas.bin.%u.size", j);
        sz = sizeof(size_t);
        if (je_mallctl(buf, &binfo->reg_size, &sz, NULL, 0) != 0) goto error;

        snprintf(buf, sizeof(buf), "arenas.bin.%u.nregs", j);
        sz = sizeof(uint32_t);
        if (je_mallctl(buf, &binfo->nregs, &sz, NULL, 0) != 0) goto error;

        binfo->len = binfo->reg_size * binfo->nregs;

        if (initBinQueryHelper(&binfo->query_helper, j) != 0) goto error;

        // Verify reverse mapping
        if (jemalloc_quantum == 8) {
            assert(jemalloc_sz2binind_lgq3(binfo->reg_size) == j);
        } else {
            assert(jemalloc_sz2binind_lgq4(binfo->reg_size) == j);
        }
    }
    arena_bin_conf.nbins = nbins;
    usage_latest.bins_usage = zcalloc(nbins * sizeof(JeBinUsage));
    if (!usage_latest.bins_usage) goto error;

    if (initJemallocCtlMib("experimental.utilization.batch_query", &arena_bin_conf.util_batch_query) != 0) goto error;
    if (initJemallocCtlMib("experimental.utilization.query", &arena_bin_conf.util_query) != 0) goto error;

    defrag_supported = 1;
    return 0;
error:
    free(arena_bin_conf.bin_info);
    free(usage_latest.bins_usage);
    return -1;
}

/* Total size of consumed meomry in unused regs in small bins (AKA external fragmentation). */
unsigned long allocatorGetFragmentationSmallBins(int new_iter) {
    unsigned long frag = 0;
    // todo for frag calculation, should we consider sizes above page size?
    // especially in case of single reg in slab
    for (unsigned j = 0; j < arena_bin_conf.nbins; j++) {
        JeBinInfo *binfo = &arena_bin_conf.bin_info[j];
        JeBinUsage *busage = &usage_latest.bins_usage[j];
        size_t curregs, curslabs, curr_nonfull_slabs, nmalloc, ndealloc;

        size_t sz = sizeof(size_t);
        queryJemallocCtlMib(&binfo->query_helper.curr_regs, &curregs, &sz);
        queryJemallocCtlMib(&binfo->query_helper.curr_slabs, &curslabs, &sz);
        queryJemallocCtlMib(&binfo->query_helper.nonfull_slabs, &curr_nonfull_slabs, &sz);
        queryJemallocCtlMib(&binfo->query_helper.nmalloc, &nmalloc, &sz);
        queryJemallocCtlMib(&binfo->query_helper.ndealloc, &ndealloc, &sz);

        if (new_iter || !(arena_bin_conf.recalc_rule == RULE_RECALC_ON_FULL_ITER)) {
            busage->stat.nmalloc = nmalloc;
            busage->stat.ndealloc = ndealloc;
            busage->curr_slabs = curslabs;
            busage->curr_nonfull_slabs = curr_nonfull_slabs;
            busage->curr_regs = curregs;
            busage->curr_full_slabs = curslabs - curr_nonfull_slabs;
            unsigned long regs_nonfull = curregs - busage->curr_full_slabs * binfo->nregs;
            // todo calculate the chanced to defrag an element, if defrag is 20% than we should be able to free 20% of
            // slabs these 20% are taken from the nonfull slabs, so we should have some free space in the nonfull slabs
            if (new_iter && busage->curr_nonfull_slabs) {
                // number of regs we need to move to reach the target is avg_num_regs_nonfull_slab *
                // slabs_free_potential avg_num_regs_nonfull_slab == total_regs_nonfull_slabs/busage->curr_nonfull_slabs
                // slabs_free_potential == ((binfo->nregs * curslabs) - curregs)/binfo->nregs
                busage->cycle_hits_to_reach_target = (((binfo->nregs * curslabs) - curregs) * regs_nonfull) /
                                                     (binfo->nregs * busage->curr_nonfull_slabs);
                // take into account the select_threshold_factor that would cause us to move more regs than the target
                busage->cycle_hits_to_reach_target =
                    busage->cycle_hits_to_reach_target * (1000 + arena_bin_conf.select_threshold_factor);
                busage->cycle_max_regs_threshold = regs_nonfull * (1000 + arena_bin_conf.select_threshold_factor);
                busage->cycle_max_regs_threshold /= busage->curr_nonfull_slabs;
                busage->cycle_max_regs_threshold /= 2;
                busage->cycle_num_hits = 0;
                busage->cycle_num_misses = 0;
            }
        }


        /* Calculate the fragmentation bytes for the current bin and add it to the total. */
        frag += ((binfo->nregs * curslabs) - curregs) * binfo->reg_size;
    }
    return frag;
}

/**
 * @brief Determines whether defragmentation should be performed for a given allocation.
 *
 * @param binfo Pointer to the bin information structure.
 * @param busage Pointer to the bin usage structure.
 * @param nalloced Number of allocated regions in the bin.
 * @param ptr Pointer to the allocated memory region (unused in this implementation).
 *
 * @return 1 if defragmentation should be performed, 0 otherwise.
 *
 * This function checks the following conditions to determine if defragmentation should be performed:
 * 1. If the number of allocated regions (nalloced) is equal to the total number of regions (binfo->nregs),
 *    defragmentation is not necessary as moving regions is guaranteed not to change the fragmentation ratio.
 * 2. If the number of non-full slabs (busage->curr_nonfull_slabs) is less than 2, defragmentation is not performed
 *    because there is no other slab to move regions to.
 * 3. If slab utilization < 'avg usilization'*1.125 [code 1.125 == (1000+UTILIZATION_THRESHOLD_FACTOR_MILI)/1000]
 *    than we should defrag. This is aligned with previous je_defrag_hint implementation.
 */
static int selectPtrHeuristic(void *ptr) {
    unsigned long long page_addr = ((unsigned long long)ptr) >> 14;
    switch (arena_bin_conf.selection_strategy) {
    case SELECT_NORMAL: return 1;
    case SELECT_PAGES_LOWER: {
        static unsigned long long num_accept = 0;
        static unsigned long long num_reject = 0;
        static unsigned long long max_addr = 0;
        static unsigned long long min_addr = (unsigned long long)-1;
        if (page_addr < min_addr) {
            min_addr = page_addr;
            num_accept++;
            return 1;
        }
        if (page_addr > max_addr) {
            max_addr = page_addr;
            if (num_reject < (num_accept * 110) / 100) {
                num_reject++;
                return 0;
            }
        }
        if (page_addr < (max_addr - min_addr) / 2) {
            min_addr = page_addr;
            num_accept++;
            return 1;
        }
        max_addr = page_addr;
        num_reject++;
        return false;
    }
    case SELECT_RANDOM: return (random() % 2 == 0);
    default: return 1;
    }
}


#define INTERPOLATE(x, x1, x2, y1, y2) ((y1) + ((x) - (x1)) * ((y2) - (y1)) / ((x2) - (x1)))
#define LIMIT(y, min, max) ((y) < (min) ? min : ((y) > (max) ? max : (y)))
static int selectHeuristic(JeBinInfo *binfo, JeBinUsage *busage, unsigned long nalloced, void *ptr) {
    UNUSED(ptr);
    if (binfo->nregs == nalloced || busage->curr_nonfull_slabs < 2) return false;
    if (arena_bin_conf.selection_strategy == SELECT_PROGRESSIVE) {
        /* Calculate the adaptive aggressiveness of the defrag based on the current
         * fragmentation and configurations. */
        unsigned long curr_progress = busage->cycle_hits_to_reach_target - busage->cycle_num_hits;
        unsigned long threshold_target = INTERPOLATE(curr_progress, 1000, busage->cycle_hits_to_reach_target, 1000,
                                                     busage->cycle_max_regs_threshold);
        threshold_target = LIMIT(threshold_target, 1000, busage->cycle_max_regs_threshold);
        if (nalloced * 1000 <= threshold_target) {
            busage->cycle_num_hits++;
            return 1;
        }
        busage->cycle_num_misses++;
        return 0;
    } else if (arena_bin_conf.selection_strategy == SELECT_UTILIZATION_TREND) {
        double utilization = (double)nalloced / binfo->nregs;
        updateUtilization(&busage->utilization_tracker, utilization);

        double p50_utilization = getP50Utilization(&busage->utilization_tracker);
        double ewma_utilization = getEWMAUtilization(&busage->utilization_tracker);

        // Combine P50 and EWMA for decision making
        double threshold = (p50_utilization + ewma_utilization) / 2;

        // Adjust threshold based on recency (use EWMA to give more weight to recent observations)
        threshold *= (1 + ALPHA * (ewma_utilization - p50_utilization));

        if (utilization < threshold) {
            return selectPtrHeuristic(ptr);
        }

        return 0;
    } else {
        /** we do not want to defrag if:
         * 1. nregs == nalloced. In this case moving is guaranteed to not change the frag ratio
         * 2. number of nonfull slabs is < 2. If we ignore the currslab we don't have anything to move
         * 3. keep the original algorithm as in je_hint.
         * */
        size_t allocated_nonfull = busage->curr_regs - busage->curr_full_slabs * binfo->nregs;
        if (nalloced * busage->curr_nonfull_slabs >
            allocated_nonfull + allocated_nonfull * arena_bin_conf.select_threshold_factor / 1000) {
            return 0;
        }

        return selectPtrHeuristic(ptr);
    }
}

/*
 * @brief Handles the results of the defragmentation analysis for multiple memory regions.
 *
 * @param conf Pointer to the configuration structure for the jemalloc arenas and bins.
 * @param usage Pointer to the usage statistics structure for the jemalloc arenas and bins.
 * @param results Array of results for each memory region to be analyzed.
 * @param ptrs Array of pointers to the memory regions to be analyzed.
 * @param num Number of memory regions in the ptrs array.
 * @param jemalloc_quantum lg-quantom of the jemalloc allocator [8 or 16].
 *
 * For each result it checks if defragmentation should be performed based on should_defrag function.
 * If defragmentation should NOT be performed, it sets the corresponding pointer in the ptrs array to NULL.
 * */
static void
handleHintResults(JeBinsConfig *conf, JeUsageLatest *usage, size_t *results, void **ptrs, size_t num, size_t quantom) {
    for (unsigned i = 0; i < num; i++) {
        unsigned long num_regs = SLAB_NUM_REGS(results, i);
        unsigned long slablen = SLAB_LEN(results, i);
        unsigned long nfree = SLAB_NFREE(results, i);
        assert(num_regs > 0);
        assert(slablen > 0);
        assert(nfree != (unsigned long)-1);
        if (num_regs == 1) {
            // single reg in slab, no need to defrag
            ptrs[i] = NULL;
            continue;
        }
        unsigned bsz = slablen / num_regs;
        // get the index depending on quantom used
        unsigned binind = (quantom == 8) ? jemalloc_sz2binind_lgq3(bsz) : jemalloc_sz2binind_lgq4(bsz);
        // make sure binind is in range and reverse map is correct
        assert(binind < conf->nbins && bsz == conf->bin_info[binind].reg_size && nfree < num_regs);

        JeBinInfo *binfo = &conf->bin_info[binind];
        JeBinUsage *busage = &usage->bins_usage[binind];

        if (!selectHeuristic(binfo, busage, binfo->nregs - nfree, ptrs[i])) {
            // MISS: utilization level is higher than threshold then set the ptr to NULL and caller will not defrag it
            ptrs[i] = NULL;
            // update miss statistics
            busage->stat.bmisses++;
            usage->stats.misses++;
            usage->stats.miss_bytes += bsz;
        } else { // HIT
            // update hit statistics
            busage->stat.bhits++;
            usage->stats.hits++;
            usage->stats.hit_bytes += bsz;
        }
    }
}

#define MAX_NUM_PTRS 100
int je_get_defrag_hint(void *ptr);
/**
 * @brief Performs defragmentation analysis for multiple memory regions.
 *
 * @param ptrs Array of pointers to memory regions to be analyzed.
 * @param num Number of memory regions in the ptrs array.
 * @param jemalloc_quantum Log base 2 of the quantum size for the current jemalloc configuration
 *        passed as --lg-quantom=3 [or 4].
 *
 * This function analyzes the provided memory regions and determines whether defragmentation should be performed
 * for each region based on the utilization and fragmentation levels. It updates the statistics for hits and misses
 * based on the defragmentation decision.
 *
 *  */
void allocatorDefragHint(void **ptrs, unsigned long num) {
    assert(defrag_supported);
    assert(num < MAX_NUM_PTRS);

    JeBinsConfig *conf = &arena_bin_conf;
    JeUsageLatest *usage = &usage_latest;

    if (conf->defrag_strategy == DEF_FRAG_JE_HINT) {
        for (unsigned j = 0; j < num; j++) {
            if (!je_get_defrag_hint(ptrs[j]) || !selectPtrHeuristic(ptrs[j])) {
                ptrs[j] = NULL;
                usage->stats.misses++;
            } else {
                usage->stats.hits++;
            }
        }
    } else if (conf->defrag_strategy == DEF_FRAG_JE_CTL) {
        static __thread size_t out[3 * MAX_NUM_PTRS] = {0};
        size_t out_sz = sizeof(size_t) * num * 3;
        size_t in_sz = sizeof(const void *) * num;
        for (unsigned j = 0; j < num * 3; j++) {
            out[j] = -1;
        }
        // get the results from the query
        je_mallctlbymib(arena_bin_conf.util_batch_query.mib, arena_bin_conf.util_batch_query.miblen, out, &out_sz, ptrs,
                        in_sz);
        // handle results with appropriate quantom value
        handleHintResults(conf, usage, out, ptrs, num, jemalloc_quantum);
    } else if (conf->defrag_strategy == DEF_FRAG_ALL) {
        for (unsigned j = 0; j < num; j++) {
            if (!selectPtrHeuristic(ptrs[j])) {
                ptrs[j] = NULL;
                usage->stats.misses++;
            } else {
                usage->stats.hits++;
            }
        }
    }
    // update overall stats, regardless of hits or misses
    usage->stats.ncalls++;
    usage->stats.nptrs += num;
}

#else
int allocatorDefragInit(void) {
    return -1;
}
void defrag_jemalloc_free(void *ptr, size_t size) {
    UNUSED(ptr);
    UNUSED(size);
}
__attribute__((malloc)) void *defrag_jemalloc_alloc(size_t size) {
    UNUSED(size);
    return NULL;
}
unsigned long allocatorGetFragmentationSmallBins(int new_iter) {
    UNUSED(new_iter);
    return 0;
}
// sds allocatorGetDefragInfo(sds info) {
//     return info;
// }
void allocatorDefragHint(void **ptrs, unsigned long num) {
    UNUSED(ptrs);
    UNUSED(num);
}
#endif

void initUtilizationTracker(UtilizationTracker *tracker) {
    tracker->ewma = 0.0;
    memset(tracker->utilization_buffer, 0, sizeof(tracker->utilization_buffer));
    tracker->buffer_index = 0;
    tracker->buffer_full = 0;
}

void updateUtilization(UtilizationTracker *tracker, double utilization) {
    // Update EWMA
    tracker->ewma = ALPHA * utilization + (1 - ALPHA) * tracker->ewma;

    // Update circular buffer
    tracker->utilization_buffer[tracker->buffer_index] = utilization;
    tracker->buffer_index = (tracker->buffer_index + 1) % UTILIZATION_BUFFER_SIZE;
    if (tracker->buffer_index == 0) {
        tracker->buffer_full = 1;
    }
}

int compare_doubles(const void *a, const void *b) {
    double diff = *(double *)a - *(double *)b;
    if (diff < 0) return -1;
    if (diff > 0) return 1;
    return 0;
}

double getP50Utilization(UtilizationTracker *tracker) {
    int size = tracker->buffer_full ? UTILIZATION_BUFFER_SIZE : tracker->buffer_index;
    if (size == 0) return 0.0;

    double sorted_buffer[UTILIZATION_BUFFER_SIZE];
    memcpy(sorted_buffer, tracker->utilization_buffer, size * sizeof(double));
    qsort(sorted_buffer, size, sizeof(double), compare_doubles);

    double p50 = sorted_buffer[size / 2];
    return p50;
}

double getEWMAUtilization(UtilizationTracker *tracker) {
    return tracker->ewma;
}
