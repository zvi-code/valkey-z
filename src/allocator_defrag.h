#ifndef ALLOCATOR_DEFRAG_H
#define ALLOCATOR_DEFRAG_H
// #include "sds.h"

typedef enum DefragStrategy {
    DEF_FRAG_JE_HINT = 0,
    DEF_FRAG_JE_CTL = 1,
    DEF_FRAG_ALL = 2,
} DefragStrategy;

typedef enum DefragSelectionStrategy {
    SELECT_NORMAL = 0,
    SELECT_PAGES_LOWER = 2,
    SELECT_RANDOM = 3,
    SELECT_PROGRESSIVE = 4,       // per-bin make the selection stricter as we progress the iteration
    SELECT_UTILIZATION_TREND = 5, // per-bin make the selection stricter as we progress the iteration
} DefragSelectionStrategy;


typedef enum DefragRulesAlloc {
    RULE_ALLOC_NONE,
    RULE_ALLOC_USE_TCACHE,
    RULE_ALLOC_USE_UD_TCACHE,
} DefragRulesAlloc;

typedef enum DefragRulesFree {
    RULE_FREE_NONE,
    RULE_FREE_USE_TCACHE,
    RULE_FREE_USE_UD_TCACHE,
} DefragRulesFree;

typedef enum DefragRulesRecalc {
    RULE_NONE,
    RULE_RECALC_ON_FULL_ITER,
} DefragRulesRecalc;

int allocatorDefragInit(void);
void defrag_jemalloc_free(void *ptr, size_t size);
__attribute__((malloc)) void *defrag_jemalloc_alloc(size_t size);
unsigned long allocatorGetFragmentationSmallBins(int new_iter);
// sds allocatorGetDefragInfo(sds info);
void allocatorDefragHint(void **ptrs, unsigned long num);
void allocatorSetStrategyConfig(DefragStrategy defrag_strategy);
void allocatorSetSelectConfig(DefragSelectionStrategy selection_strategy);
void allocatorSetRefreshConfig(DefragRulesRecalc recalc);
void allocatorSetFreeConfig(DefragRulesFree recalc);
void allocatorSetAllocConfig(DefragRulesAlloc recalc);
void allocatorSetThresholdConfig(int threshold);
#endif /* ALLOCATOR_DEFRAG_H */
