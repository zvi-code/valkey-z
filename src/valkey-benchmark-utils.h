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


/* Read from replica options */
typedef enum readFromReplica {
    FROM_PRIMARY_ONLY = 0, /* default option */
    FROM_REPLICA_ONLY,
    FROM_ALL
} readFromReplica;

typedef enum {
    DIFF_NONE = 0,
    DIFF_RATE_COUNT,        /* delta / time_sec */
    DIFF_RATE_MICROSEC,     /* (delta / 1M) / time_sec - for microsecond counters */
    DIFF_MEMORY_GROWTH,     /* (delta_bytes / MB) / time_sec */
    DIFF_PERCENTAGE_CHANGE  /* (delta / old_value) * 100 */
} DiffType;

typedef enum {
    DISPLAY_INTEGER,
    DISPLAY_MEMORY_MB,
    DISPLAY_MEMORY_HUMAN,
    DISPLAY_PERCENTAGE,  /* From fixed-point scaled by 1000 */
    DISPLAY_FLOAT,       /* From fixed-point scaled by 1000 */
    DISPLAY_LATENCY_USEC,
    DISPLAY_MINMAX
} DisplayFormat;

typedef enum {
    AGG_SUM,
    AGG_AVERAGE,
    AGG_MAX,
    AGG_MINMAX
} AggregationType;

/* Generic numeric parser with conversion strategy */
typedef enum {
    PARSE_INTEGER,
    PARSE_MEMORY,      /* Handles K/M/G suffixes */
    PARSE_FLOAT_FIXED, /* Returns value * 1000 */
    PARSE_PERCENTILE,   /* Extracts p50/p99/p99.9 from string */
    PARSE_CMDSTATS    /* Extracts from cmdstat_<cmd>: calls=...,usec=...,usec_per_call=...,rejected=...,failed=... */
} ParseStrategy;

typedef struct {
    ParseStrategy strategy;
    const char *key;  /* For percentile: "p50", "p99", "p99.9" */
} ParseConfig;

/* Extended field type with temporal diff support */
typedef struct infoFieldType {
    char* prefix_match;
    matcherCallBack match;
    ParseConfig parse_config;
    AggregationType aggregation_type;
    DisplayFormat display_format;
    DiffType diff_type;
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