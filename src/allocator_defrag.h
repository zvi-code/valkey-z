#ifndef __ALLOCATOR_DEFRAG_H
#define __ALLOCATOR_DEFRAG_H
#include "sds.h"

int allocatorDefragInit(void);
void allocatorDefragFree(void *ptr, size_t size);
__attribute__((malloc)) void *allocatorDefragAlloc(size_t size);
unsigned long allocatorDefragGetFragSmallbins(void);
int allocatorShouldDefrag(void *ptr);

#endif /* __ALLOCATOR_DEFRAG_H */
