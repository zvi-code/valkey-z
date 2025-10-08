/**
 * Vector ID to Cluster Tag Mapping Implementation
 *
 * This module implements vector ID mapping functionality using the generic
 * cluster scanner. It provides specialized key processing for vector keys
 * to extract vector IDs and their corresponding cluster tags.
 */

#include "vector-id-mapping.h"
#include "cluster-scan.h"
#include "cluster-utils.h"
#include "zmalloc.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <assert.h>

void initClusterTagMap(clusterTagMap *tag_map, uint64_t initial_capacity) {
    memset(tag_map, 0, sizeof(clusterTagMap));
    tag_map->capacity = initial_capacity;
    tag_map->mappings = zcalloc(tag_map->capacity * sizeof(vectorClusterMapping));
    pthread_mutex_init(&tag_map->mutex, NULL);
}

void addClusterTagMapping(clusterTagMap *tag_map, uint64_t vector_id, const char *cluster_tag) {
    if (!tag_map || !cluster_tag) return;
    assert(vector_id < 1000000000);  // Prevent overflow on doubling capacity
    pthread_mutex_lock(&tag_map->mutex);

    /* Expand capacity if needed */
    if (vector_id >= tag_map->capacity) {
        tag_map->mappings = zrealloc(tag_map->mappings,
                                   2 * vector_id * sizeof(vectorClusterMapping));
        memset(&tag_map->mappings[tag_map->capacity], 0,
               (2 * vector_id - tag_map->capacity) * sizeof(vectorClusterMapping));
        tag_map->capacity = 2 * vector_id;
    }
    if (tag_map->mappings[vector_id].cluster_tag[0] == '\0') {
        /* New entry */
        assert(tag_map->count < tag_map->capacity);
        tag_map->count++;
    }
    pthread_mutex_unlock(&tag_map->mutex);
    
    /* Add new mapping */
    memcpy(tag_map->mappings[vector_id].cluster_tag, cluster_tag, 5);
    tag_map->mappings[vector_id].cluster_tag[5] = '\0';

}

const char* getClusterTagForVector(clusterTagMap *tag_map, uint64_t vector_id) {
    if (!tag_map) return NULL;
    if (vector_id >= tag_map->capacity) return NULL;
    if (tag_map->mappings[vector_id].cluster_tag[0] == '\0') return NULL;
    assert(tag_map->mappings[vector_id].cluster_tag[0] == '{');
    return tag_map->mappings[vector_id].cluster_tag;
}

void vectorMappingProgressCallback(uint64_t keys_processed, int active_threads, void *user_data) {
    printf("[VECTOR-MAPPING] Processed %lu keys, %d threads active\n",
           keys_processed, active_threads);
}

int buildVectorIdMappings(const char *prefix,
                         struct clusterNode **nodes,
                         int node_count,
                         clusterTagMap *tag_map,
                        keyProcessorCallback key_processor) {
    if (!prefix || !nodes || !tag_map) {
        return -1;
    }

    /* Create scan pattern: prefix* */
    char pattern[256];
    snprintf(pattern, sizeof(pattern), "%s*", prefix);
    tag_map->prefix = zstrdup(prefix);
    tag_map->prefix_len = strlen(prefix);
    /* Configure cluster scan */
    clusterScanConfig scan_config;
    initClusterScanConfig(&scan_config, pattern, nodes, node_count,
                         key_processor, tag_map);

    /* Set performance parameters for vector scanning */
    setClusterScanPerformance(&scan_config, 1000, node_count, 50000);
    setClusterScanProgressCallback(&scan_config, vectorMappingProgressCallback);

    /* Execute the scan */
    clusterScanResults results;
    printf("[VECTOR-MAPPING] Starting cluster scan for pattern '%s'\n", pattern);

    int scan_result = executeClusterScan(&scan_config, &results);

    if (scan_result == 0) {
        printf("[VECTOR-MAPPING] Successfully built %lu vector ID mappings\n", tag_map->count);
        printf("[VECTOR-MAPPING] Scan completed: %lu keys in %.2f seconds (%.1f keys/sec)\n",
               results.total_keys_processed,
               results.total_scan_time_ms / 1000.0,
               results.keys_per_second);
    } else {
        printf("[VECTOR-MAPPING] Scan failed with error code %d\n", scan_result);
    }

    return scan_result;
}

void cleanupClusterTagMap(clusterTagMap *tag_map) {
    if (!tag_map) return;

    if (tag_map->mappings) {
        zfree(tag_map->mappings);
        tag_map->mappings = NULL;
    }
    pthread_mutex_destroy(&tag_map->mutex);
    memset(tag_map, 0, sizeof(clusterTagMap));
}

size_t getClusterTagMapCount(clusterTagMap *tag_map) {
    if (!tag_map) return 0;
    return tag_map->count;
}