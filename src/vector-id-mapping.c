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

/**
 * Extract vector ID and cluster tag from a vector key
 * Expected format: prefix{cluster_tag}:vector_id
 * Example: "zvec_large_{06S}:000001234567890123"
 */
static int parseVectorKey(const char *key, uint64_t *vector_id, char *cluster_tag, size_t tag_size) {
    if (!key || !vector_id || !cluster_tag) {
        return -1;
    }

    /* Extract cluster tag using shared utility function */
    if (extractClusterTag(key, 0, cluster_tag, tag_size) != 0) {
        return -1;
    }

    /* Find vector ID after the last colon */
    const char *id_start = strrchr(key, ':');
    if (!id_start) return -1;

    id_start++; /* Skip the colon */

    /* Parse vector ID */
    char *endptr;
    *vector_id = strtoull(id_start, &endptr, 10);
    if (*endptr != '\0') return -1;  /* Invalid number */

    return 0;
}

/**
 * Key processor callback for vector ID mapping
 * This function is called for each key discovered during cluster scan
 */
static int vectorKeyProcessor(const char *key, void *user_data, int thread_id) {
    clusterTagMap *tag_map = (clusterTagMap*)user_data;
    uint64_t vector_id;
    char cluster_tag[6];

    /* Parse the vector key to extract ID and cluster tag */
    if (parseVectorKey(key, &vector_id, cluster_tag, sizeof(cluster_tag)) == 0) {
        /* Add mapping to the table */
        addClusterTagMapping(tag_map, vector_id, cluster_tag);
        return 0;
    }

    /* Key format doesn't match expected vector key pattern */
    return 0;  /* Continue processing other keys */
}

void initClusterTagMap(clusterTagMap *tag_map, uint64_t initial_capacity) {
    memset(tag_map, 0, sizeof(clusterTagMap));
    tag_map->capacity = initial_capacity;
    tag_map->mappings = zmalloc(tag_map->capacity * sizeof(vectorClusterMapping));
    pthread_mutex_init(&tag_map->mutex, NULL);
}

void addClusterTagMapping(clusterTagMap *tag_map, uint64_t vector_id, const char *cluster_tag) {
    if (!tag_map || !cluster_tag) return;

    pthread_mutex_lock(&tag_map->mutex);

    /* Expand capacity if needed */
    if (tag_map->count >= tag_map->capacity) {
        tag_map->capacity *= 2;
        tag_map->mappings = zrealloc(tag_map->mappings,
                                   tag_map->capacity * sizeof(vectorClusterMapping));
    }

    /* Add new mapping */
    tag_map->mappings[tag_map->count].vector_id = vector_id;
    strncpy(tag_map->mappings[tag_map->count].cluster_tag, cluster_tag,
            sizeof(tag_map->mappings[tag_map->count].cluster_tag) - 1);
    tag_map->mappings[tag_map->count].cluster_tag[
        sizeof(tag_map->mappings[tag_map->count].cluster_tag) - 1] = '\0';
    tag_map->count++;

    pthread_mutex_unlock(&tag_map->mutex);
}

const char* getClusterTagForVector(clusterTagMap *tag_map, uint64_t vector_id) {
    if (!tag_map) return NULL;

    pthread_mutex_lock(&tag_map->mutex);

    /* Linear search for vector ID */
    for (uint64_t i = 0; i < tag_map->count; i++) {
        if (tag_map->mappings[i].vector_id == vector_id) {
            pthread_mutex_unlock(&tag_map->mutex);
            return tag_map->mappings[i].cluster_tag;
        }
    }

    pthread_mutex_unlock(&tag_map->mutex);
    return NULL;
}

void vectorMappingProgressCallback(uint64_t keys_processed, int active_threads, void *user_data) {
    printf("[VECTOR-MAPPING] Processed %lu keys, %d threads active\n",
           keys_processed, active_threads);
}

int buildVectorIdMappings(const char *prefix,
                         struct clusterNode **nodes,
                         int node_count,
                         clusterTagMap *tag_map) {
    if (!prefix || !nodes || !tag_map) {
        return -1;
    }

    /* Create scan pattern: prefix* */
    char pattern[256];
    snprintf(pattern, sizeof(pattern), "%s*", prefix);

    /* Configure cluster scan */
    clusterScanConfig scan_config;
    initClusterScanConfig(&scan_config, pattern, nodes, node_count,
                         vectorKeyProcessor, tag_map);

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