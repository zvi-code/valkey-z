#ifndef __VALKEY_BENCHMARK_UTILS_H
#define __VALKEY_BENCHMARK_UTILS_H
#include <stdio.h>
#include <stdlib.h>
#include "sds.h"
/* Field snapshot for temporal diff calculations */
typedef struct fieldSnapshot {
    sds field_name;
    char* value_str;
    long long value;
    long long*  per_node_values;  /* Array of per-node values */
    int node_count;
    int valid;
} fieldSnapshot;

/* Cluster snapshot at a point in time */
typedef struct clusterSnapshot {
    long long timestamp_ms;
    int num_fields;
    fieldSnapshot *fields;
    int num_nodes;
    sds *node_identifiers;  /* Store node IP:port for validation */
} clusterSnapshot;

/* Callback types */
typedef int (*matcherCallBack)(const char *line, const char *prefix);
typedef long long (*parseCallBack)(const char *value);
typedef void (*clusterAggregationCallBack)(void **opaque, long long value, 
                                           int node_idx, int is_last_node);
typedef void (*displayCallBack)(const char *field_name, void *opaque, int node_count);
typedef void (*diffCallBack)(const char *field_name, fieldSnapshot *old, 
                             fieldSnapshot *new_snap, long long time_delta_ms, int node_idx);

/* Read from replica options */
typedef enum readFromReplica {
    FROM_PRIMARY_ONLY = 0, /* default option */
    FROM_REPLICA_ONLY,
    FROM_ALL
} readFromReplica;

/* Extended field type with temporal diff support */
typedef struct infoFieldType {
    char* prefix_match;
    char* name;    
    matcherCallBack match;
    parseCallBack parse;
    clusterAggregationCallBack agg;
    displayCallBack disp;
    diffCallBack diff;           /* Calculate and display diff */
    int track_per_node;          /* Whether to store per-node values */
    readFromReplica nodes_to_aggregate; /* Whether to read from replicas */
    int is_last; /* Whether we should re-process this line for next field, or stop */
} infoFieldType;

// getMemoryInfoClusterGeneric();
clusterSnapshot* getSearchInfo(long long *search_memory, long long *search_reclaimable, 
                   long long *search_total_docs, long long *search_ingest_field_vector, 
                   long long *search_background_indexing_status);
clusterSnapshot* getInfoCluster(void);
clusterSnapshot* getFtInfoStatistics(const char *index_name);
void getFullInfo(const char *index_name);
void* compareInfoSnapshots(clusterSnapshot *old_infoall, clusterSnapshot *new_snap_infoall, clusterSnapshot *old_ftinfo, clusterSnapshot *new_snap_ftinfo, clusterSnapshot *old_infosearch, clusterSnapshot *new_snap_infosearch);
#endif /* __VALKEY_BENCHMARK_UTILS_H */