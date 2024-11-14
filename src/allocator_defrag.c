/* Copyright 2024- Valkey contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */
#include <stdio.h>
#include "zmalloc.h"
#include "serverassert.h"
#include "allocator_defrag.h"

#define UNUSED(x) (void)(x)

#if defined(HAVE_DEFRAG) && defined(USE_JEMALLOC)

#define STRINGIFY_(x) #x
#define STRINGIFY(x) STRINGIFY_(x)

#define BATCH_QUERY_ARGS_OUT 3
#define SLAB_NFREE(out, i) out[(i) * BATCH_QUERY_ARGS_OUT]
#define SLAB_LEN(out, i) out[(i) * BATCH_QUERY_ARGS_OUT + 2]
#define SLAB_NUM_REGS(out, i) out[(i) * BATCH_QUERY_ARGS_OUT + 1]

#define UTILIZATION_THRESHOLD_FACTOR_MILI (125) // 12.5% additional utilization

/*
 * Represents a precomputed key for querying jemalloc statistics.
 *
 * The `jeMallctlKey` structure stores a key corresponding to a specific jemalloc
 * statistics field name. This key is used with the `je_mallctlbymib` interface
 * to query statistics more efficiently, bypassing the need for runtime string
 * lookup and translation performed by `je_mallctl`.
 *
 * - `je_mallctlnametomib` is called once for each statistics field to precompute
 *   and store the key corresponding to the field name.
 * - Subsequent queries use `je_mallctlbymib` with the stored key, avoiding the
 *   overhead of repeated string-based lookups.
 *
 */
typedef struct jeMallctlKey {
    size_t key[6]; // The precomputed key used to query jemalloc statistics.
    size_t keylen; // The length of the key array.
} jeMallctlKey;

/* Stores MIB (Management Information Base) keys for jemalloc bin queries.
 *
 * This struct holds precomputed `jeMallctlKey` values for querying various
 * jemalloc bin-related statistics efficiently.
 */
typedef struct jeBinInfoKeys {
    jeMallctlKey curr_slabs;    /* Key to query the current number of slabs in the bin. */
    jeMallctlKey nonfull_slabs; /* Key to query the number of non-full slabs in the bin. */
    jeMallctlKey curr_regs;     /* Key to query the current number of regions in the bin. */
} jeBinInfoKeys;

/* Represents detailed information about a jemalloc bin.
 *
 * This struct provides metadata about a jemalloc bin, including the size of
 * its regions, total number of regions, and related MIB keys for efficient
 * queries.
 */
typedef struct jeBinInfo {
    unsigned long reg_size;  /* Size of each region in the bin. */
    unsigned long nregs;     /* Total number of regions in the bin. */
    jeBinInfoKeys info_keys; /* Precomputed MIB keys for querying bin statistics. */
} jeBinInfo;

/* Represents the configuration for jemalloc bins.
 *
 * This struct contains information about the number of bins and metadata for
 * each bin, as well as precomputed keys for batch utility queries and epoch updates.
 */
typedef struct jeBinsConf {
    unsigned long nbins;           /* Number of bins in the jemalloc configuration. */
    jeBinInfo *bin_info;           /* Array of `jeBinInfo` structs, one for each bin. */
    jeMallctlKey util_batch_query; /* Key to query batch utilization information. */
    jeMallctlKey epoch;            /* Key to trigger statistics sync between threads. */
} jeBinsConf;

/* Represents the latest usage statistics for a jemalloc bin.
 *
 * This struct tracks the current usage of a bin, including the number of slabs
 * and regions, and calculates the number of full slabs from other fields.
 */
typedef struct jeBusage {
    unsigned long curr_slabs;         /* Current number of slabs in the bin. */
    unsigned long curr_nonfull_slabs; /* Current number of non-full slabs in the bin. */
    unsigned long curr_full_slabs;    /* Current number of full slabs in the bin (calculated from other fields). */
    unsigned long curr_regs;          /* Current number of regions in the bin. */
} jeBusage;


static int defrag_supported = 0;
static size_t jemalloc_quantum = 0;
static jeBinsConf arena_bin_conf = {0, NULL, {{0}, 0}, {{0}, 0}};
static jeBusage *curr_usage = NULL;


/* -----------------------------------------------------------------------------
 * Alloc/Free API that are cooperative with defrag
 * -------------------------------------------------------------------------- */

/* Allocation and free functions that bypass the thread cache
 * and go straight to the allocator arena bins.
 * Currently implemented only for jemalloc. Used for online defragmentation.
 */
void *allocatorDefragAlloc(size_t size) {
    void *ptr = je_mallocx(size, MALLOCX_TCACHE_NONE);
    return ptr;
}
void allocatorDefragFree(void *ptr, size_t size) {
    if (ptr == NULL) return;
    je_sdallocx(ptr, size, MALLOCX_TCACHE_NONE);
}

/* -----------------------------------------------------------------------------
 * Helper functions for jemalloc translation between size and index
 * -------------------------------------------------------------------------- */
#define LG_QUANTOM_8_FIRST_POW2 3
#define SIZE_CLASS_GROUP_SZ 4

#define LG_QUANTOM_OFFSET_3 ((64 >> LG_QUANTOM_8_FIRST_POW2) - 1)

#define getBinindNormal(_sz, _offset, _last_sz_pow2)                                                             \
    ((SIZE_CLASS_GROUP_SZ - (((1 << (_last_sz_pow2)) - (_sz)) >> ((_last_sz_pow2) - LG_QUANTOM_8_FIRST_POW2))) + \
     (((_last_sz_pow2) - (LG_QUANTOM_8_FIRST_POW2 + 3)) - 1) * SIZE_CLASS_GROUP_SZ + (_offset))

/* Get the bin index in bin array from the reg_size.
 *
 * these are reverse engineered mapping of reg_size -> binind. We need this information because the utilization query
 * returns the size of the buffer and not the bin index, and we need the bin index to access it's usage information
 *
 * Note: In case future PR will return the binind (that is better API anyway) we can get rid of
 * these conversion functions
 */
inline unsigned jeSize2BinIndexLgQ3(size_t sz) {
    if (sz <= (1 << (LG_QUANTOM_8_FIRST_POW2 + 3))) {
        // for sizes: 8, 16, 24, 32, 40, 48, 56, 64
        return (sz >> 3) - 1;
    }
    // following groups have SIZE_CLASS_GROUP_SZ size-class that are
    uint64_t last_sz_in_group_pow2 = 64 - __builtin_clzll(sz - 1);
    return getBinindNormal(sz, LG_QUANTOM_OFFSET_3, last_sz_in_group_pow2);
}

/* -----------------------------------------------------------------------------
 * Interface functions to get fragmentation info from jemalloc
 * -------------------------------------------------------------------------- */
#define ARENA_TO_QUERY MALLCTL_ARENAS_ALL

static inline void jeRefreshStats(const jeBinsConf *arena_bin_conf) {
    uint64_t epoch = 1; // Value doesn't matter
    size_t sz = sizeof(epoch);
    je_mallctlbymib(arena_bin_conf->epoch.key, arena_bin_conf->epoch.keylen, &epoch, &sz, &epoch, sz); // Refresh stats
}

/* Extract key that corresponds to the given name for fast query. This should be called once for each key_name */
static int jeQueryKeyInit(const char *key_name, jeMallctlKey *key_info) {
    key_info->keylen = sizeof(key_info->key) / sizeof(key_info->key[0]);
    int res = je_mallctlnametomib(key_name, key_info->key, &key_info->keylen);
    assert(key_info->keylen <= sizeof(key_info->key) / sizeof(key_info->key[0]));
    return res;
}

/* Query jemalloc control interface using previously extracted key (with jeQueryKeyInit) instead of name string.
 * This interface (named MIB in jemalloc) is faster as it avoids string dict lookup at run-time. */
static inline int jeQueryCtlInterface(const jeMallctlKey *key_info, void *value, size_t *size) {
    return je_mallctlbymib(key_info->key, key_info->keylen, value, size, NULL, 0);
}

static int binQueryHelperInitialization(jeBinInfoKeys *helper, unsigned bin_index) {
    char buf[128];

    /* Mib of fetch number of used regions in the bin */
    snprintf(buf, sizeof(buf), "stats.arenas." STRINGIFY(ARENA_TO_QUERY) ".bins.%d.curregs", bin_index);
    if (jeQueryKeyInit(buf, &helper->curr_regs) != 0) return -1;
    /* Mib of fetch number of current slabs in the bin */
    snprintf(buf, sizeof(buf), "stats.arenas." STRINGIFY(ARENA_TO_QUERY) ".bins.%d.curslabs", bin_index);
    if (jeQueryKeyInit(buf, &helper->curr_slabs) != 0) return -1;
    /* Mib of fetch nonfull slabs */
    snprintf(buf, sizeof(buf), "stats.arenas." STRINGIFY(ARENA_TO_QUERY) ".bins.%d.nonfull_slabs", bin_index);
    if (jeQueryKeyInit(buf, &helper->nonfull_slabs) != 0) return -1;

    return 0;
}

/* Initializes the defragmentation system for the jemalloc memory allocator.
 *
 * This function performs the necessary setup and initialization steps for the defragmentation system.
 * It retrieves the configuration information for the jemalloc arenas and bins, and initializes the usage
 * statistics data structure.
 *
 * return 0 on success, or a non-zero error code on failure.
 *
 * The initialization process involves the following steps:
 * 1. Check if defragmentation is supported by the current jemalloc version.
 * 2. Retrieve the arena bin configuration information using the `je_mallctlbymib` function.
 * 3. Initialize the `usage_latest` structure with the bin usage statistics and configuration data.
 * 4. Set the `defrag_supported` flag to indicate that defragmentation is enabled.
 *
 * Note: This function must be called before using any other defragmentation-related functionality.
 * It should be called during the initialization phase of the code that uses the
 * defragmentation feature.
 */
int allocatorDefragInit(void) {
    char buf[100];
    jeBinInfo *bin_info;
    unsigned nbins;
    size_t sz = sizeof(nbins);
    int je_res = 0;

    // the init should be called only once, fail if unexpected call
    assert(!defrag_supported);

    // Get the mib of the per memory pointers query command that is used during defrag scan over memory
    if (jeQueryKeyInit("experimental.utilization.batch_query", &arena_bin_conf.util_batch_query) != 0) return -1;

    je_res = jeQueryKeyInit("epoch", &arena_bin_conf.epoch);
    assert(je_res == 0);
    jeRefreshStats(&arena_bin_conf);

    size_t len = sizeof(jemalloc_quantum);
    je_mallctl("arenas.quantum", &jemalloc_quantum, &len, NULL, 0);
    // lg-quantum should be 3
    assert(jemalloc_quantum == 8);

    je_res = je_mallctl("arenas.nbins", &nbins, &sz, NULL, 0);
    assert(je_res == 0 && nbins != 0);
    arena_bin_conf.nbins = nbins;

    arena_bin_conf.bin_info = zcalloc(sizeof(jeBinInfo) * nbins);
    assert(arena_bin_conf.bin_info != NULL);
    curr_usage = zcalloc(sizeof(jeBusage) * nbins);
    assert(curr_usage != NULL);

    for (unsigned j = 0; j < nbins; j++) {
        bin_info = &arena_bin_conf.bin_info[j];
        /* The size of the current bin */
        snprintf(buf, sizeof(buf), "arenas.bin.%d.size", j);
        sz = sizeof(size_t);
        je_res = je_mallctl(buf, &bin_info->reg_size, &sz, NULL, 0);
        assert(je_res == 0);
        /* Number of regions per slab */
        snprintf(buf, sizeof(buf), "arenas.bin.%d.nregs", j);
        sz = sizeof(uint32_t);
        je_res = je_mallctl(buf, &bin_info->nregs, &sz, NULL, 0);
        assert(je_res == 0);

        // init bin specific fast query keys
        je_res = binQueryHelperInitialization(&bin_info->info_keys, j);
        assert(je_res == 0);

        /* verify the reverse map of reg_size to bin index */
        assert(jeSize2BinIndexLgQ3(bin_info->reg_size) == j);
    }

    // defrag is supported mark it to enable defrag queries
    defrag_supported = 1;
    return 0;
}

/* Total size of consumed meomry in unused regs in small bins (AKA external fragmentation).
 * The function will refresh the epoch.
 *
 * return total fragmentation bytes
 */
unsigned long allocatorDefragGetFragSmallbins(void) {
    assert(defrag_supported);
    unsigned long frag = 0;
    jeRefreshStats(&arena_bin_conf);
    for (unsigned j = 0; j < arena_bin_conf.nbins; j++) {
        size_t sz;
        jeBinInfo *bin_info = &arena_bin_conf.bin_info[j];
        jeBusage *bin_usage = &curr_usage[j];

        /* Number of used regions in the bin */
        sz = sizeof(size_t);
        /* Number of current slabs in the bin */
        jeQueryCtlInterface(&bin_info->info_keys.curr_regs, &bin_usage->curr_regs, &sz);
        /* Number of current slabs in the bin */
        jeQueryCtlInterface(&bin_info->info_keys.curr_slabs, &bin_usage->curr_slabs, &sz);
        /* Number of non full slabs in the bin */
        jeQueryCtlInterface(&bin_info->info_keys.nonfull_slabs, &bin_usage->curr_nonfull_slabs, &sz);

        bin_usage->curr_full_slabs = bin_usage->curr_slabs - bin_usage->curr_nonfull_slabs;
        /* Calculate the fragmentation bytes for the current bin and add it to the total. */
        frag += ((bin_info->nregs * bin_usage->curr_slabs) - bin_usage->curr_regs) * bin_info->reg_size;
    }
    return frag;
}

/* Determines whether defragmentation should be performed on a pointer based on jemalloc information.
 *
 * bin_info Pointer to the bin information structure.
 * bin_usage Pointer to the bin usage structure.
 * nalloced Number of allocated regions in the bin.
 *
 * return 1 if defragmentation should be performed, 0 otherwise.
 *
 * This function checks the following conditions to determine if defragmentation should be performed:
 * 1. If the number of allocated regions (nalloced) is equal to the total number of regions (bin_info->nregs),
 *    defragmentation is not necessary as moving regions is guaranteed not to change the fragmentation ratio.
 * 2. If the number of non-full slabs (bin_usage->curr_nonfull_slabs) is less than 2, defragmentation is not performed
 *    because there is no other slab to move regions to.
 * 3. If slab utilization < 'avg utilization'*1.125 [code 1.125 == (1000+UTILIZATION_THRESHOLD_FACTOR_MILI)/1000]
 *    than we should defrag. This is aligned with previous je_defrag_hint implementation.
 */
static inline int makeDefragDecision(jeBinInfo *bin_info, jeBusage *bin_usage, unsigned long nalloced) {
    size_t allocated_nonfull = bin_usage->curr_regs - bin_usage->curr_full_slabs * bin_info->nregs;
    if (bin_info->nregs == nalloced || bin_usage->curr_nonfull_slabs < 2 ||
        1000 * nalloced * bin_usage->curr_nonfull_slabs > (1000 + UTILIZATION_THRESHOLD_FACTOR_MILI) * allocated_nonfull) {
        return 0;
    }
    return 1;
}

/*
 * Performs defragmentation analysis for multiple memory regions.
 *
 * ptr - ptr to memory regions to be analyzed.
 *
 * This function analyzes the provided memory region and determines whether defragmentation should be performed
 * based on the utilization and fragmentation levels.
 *
 */
int allocatorShouldDefrag(void *ptr) {
    assert(defrag_supported);
    size_t out[BATCH_QUERY_ARGS_OUT];
    size_t out_sz = sizeof(size_t) * BATCH_QUERY_ARGS_OUT;
    size_t in_sz = sizeof(const void *);
    for (unsigned j = 0; j < BATCH_QUERY_ARGS_OUT; j++) {
        out[j] = -1;
    }
    je_mallctlbymib(arena_bin_conf.util_batch_query.key,
                    arena_bin_conf.util_batch_query.keylen,
                    out, &out_sz,
                    &ptr, in_sz);
    // handle results with appropriate quantum value
    unsigned long num_regs = SLAB_NUM_REGS(out, 0);
    unsigned long slablen = SLAB_LEN(out, 0);
    unsigned long nfree = SLAB_NFREE(out, 0);
    assert(num_regs > 0);
    assert(slablen > 0);
    assert(nfree != (unsigned long)-1);
    unsigned bsz = slablen / num_regs;
    // check that the allocation is not too large
    if (bsz > arena_bin_conf.bin_info[arena_bin_conf.nbins - 1].reg_size) {
        return 0;
    }
    // get the index based on quantum used
    unsigned binind = jeSize2BinIndexLgQ3(bsz);
    // make sure binind is in range and reverse map is correct
    assert(binind < arena_bin_conf.nbins && bsz == arena_bin_conf.bin_info[binind].reg_size);

    return makeDefragDecision(&arena_bin_conf.bin_info[binind],
                              &curr_usage[binind],
                              arena_bin_conf.bin_info[binind].nregs - nfree);
}

#else

int allocatorDefragInit(void) {
    return -1;
}
void allocatorDefragFree(void *ptr, size_t size) {
    UNUSED(ptr);
    UNUSED(size);
}
__attribute__((malloc)) void *allocatorDefragAlloc(size_t size) {
    UNUSED(size);
    return NULL;
}
unsigned long allocatorDefragGetFragSmallbins(void) {
    return 0;
}

int allocatorShouldDefrag(void *ptr) {
    UNUSED(ptr);
    return 0;
}
#endif
