/**
 * valkey-benchmark-vgen.c - Vector Generator Integration Implementation
 * 
 * This file implements the integration between valkey-benchmark and the vector
 * generator library. It provides thread-safe vector generation, placeholder
 * replacement, and recall tracking for vector search benchmarks.
 */

#include "valkey-benchmark-vgen.h"
#include "fmacros.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <math.h>
#include <pthread.h>
#include <stdatomic.h>

#include "sds.h"
#include "zmalloc.h"
#include <valkey/valkey.h>

/* Include vector generator from utils/vgenerator */
#include "../../utils/vgenerator/vector_generator.h"

/* Global vector generator instance */
static vector_generator_t *vgen_instance = NULL;
static pthread_rwlock_t vgen_lock = PTHREAD_RWLOCK_INITIALIZER;

/* Configuration values stored at init time */
static int vgen_cluster_mode = 0;
static char vgen_prefix[256] = "";

/* Ground truth storage for recall calculation */
/* Note: ground_truth_entry_t is defined in vector_generator.h */
#define MAX_GROUND_TRUTH_ENTRIES 100000
typedef struct {
    vector_key_t query_key;
    ground_truth_entry_t neighbors[NEIGHBORS_PER_QUERY];  /* Store top-10 neighbors */
    int neighbor_count;
} stored_ground_truth_t;

typedef struct {
    stored_ground_truth_t *entries;
    uint64_t capacity;
    uint64_t count;
    pthread_mutex_t lock;
} ground_truth_storage_t;

static ground_truth_storage_t ground_truth = {
    .entries = NULL,
    .capacity = 0,
    .count = 0,
    .lock = PTHREAD_MUTEX_INITIALIZER
};

/* Recall tracking structure */
typedef struct {
    uint64_t total_queries;
    double total_recall;
    double min_recall;
    double max_recall;
    uint64_t current_query_index;  /* Index for matching replies to ground truth */
    pthread_mutex_t lock;
} recall_tracker_t;

static recall_tracker_t recall_tracker = {
    .total_queries = 0,
    .total_recall = 0.0,
    .min_recall = 1.0,
    .max_recall = 0.0,
    .current_query_index = 0,
    .lock = PTHREAD_MUTEX_INITIALIZER
};

/* Thread-local iterator pool */
typedef struct {
    vector_iterator_t *ingestion_iter;
    vector_iterator_t *query_iter;
    vector_iterator_t *deletion_iter;
    pthread_mutex_t lock;
} thread_iterator_pool_t;

#define MAX_THREADS 500
static thread_iterator_pool_t thread_pools[MAX_THREADS];
static int thread_pools_initialized = 0;

/**
 * Initialize thread iterator pools.
 */
static void init_thread_pools(void) {
    if (thread_pools_initialized) return;
    
    for (int i = 0; i < MAX_THREADS; i++) {
        thread_pools[i].ingestion_iter = NULL;
        thread_pools[i].query_iter = NULL;
        thread_pools[i].deletion_iter = NULL;
        pthread_mutex_init(&thread_pools[i].lock, NULL);
    }
    thread_pools_initialized = 1;
}

/**
 * Cleanup thread iterator pools.
 */
static void cleanup_thread_pools(void) {
    if (!thread_pools_initialized) return;
    
    for (int i = 0; i < MAX_THREADS; i++) {
        pthread_mutex_lock(&thread_pools[i].lock);
        
        if (thread_pools[i].ingestion_iter) {
            vg_iterator_destroy(thread_pools[i].ingestion_iter);
            thread_pools[i].ingestion_iter = NULL;
        }
        if (thread_pools[i].query_iter) {
            vg_iterator_destroy(thread_pools[i].query_iter);
            thread_pools[i].query_iter = NULL;
        }
        if (thread_pools[i].deletion_iter) {
            vg_iterator_destroy(thread_pools[i].deletion_iter);
            thread_pools[i].deletion_iter = NULL;
        }
        
        pthread_mutex_unlock(&thread_pools[i].lock);
        pthread_mutex_destroy(&thread_pools[i].lock);
    }
    thread_pools_initialized = 0;
}

/**
 * Initialize the vector generator from benchmark configuration.
 */
int vgen_init_from_config(uint32_t dimensions, uint64_t initial_capacity,
                           uint32_t num_centroids, float radius,
                           float sparsity, uint64_t seed,
                           int cluster_mode, const char *prefix) {
    pthread_rwlock_wrlock(&vgen_lock);
    
    if (vgen_instance != NULL) {
        fprintf(stderr, "Warning: Vector generator already initialized\n");
        pthread_rwlock_unlock(&vgen_lock);
        return 0;
    }
    
    /* Store configuration values for later use */
    vgen_cluster_mode = cluster_mode;
    if (prefix) {
        snprintf(vgen_prefix, sizeof(vgen_prefix), "%s", prefix);
    }
    
    /* Create generator configuration from parameters */
    generator_config_t vgen_config = {
        .dimensions = dimensions > 0 ? dimensions : 128,
        .initial_capacity = initial_capacity,
        .sparsity = sparsity,
        .radius = radius,
        .num_centroids = num_centroids,
        .seed = seed
    };
    
    /* Validate configuration */
    if (vgen_config.dimensions < 4 || vgen_config.dimensions > 2048) {
        fprintf(stderr, "Error: Vector dimensions must be between 4 and 2048 (got %u)\n", 
                vgen_config.dimensions);
        pthread_rwlock_unlock(&vgen_lock);
        return -1;
    }
    
    if (vgen_config.sparsity < 0.0f || vgen_config.sparsity > 1.0f) {
        fprintf(stderr, "Error: Sparsity must be between 0.0 and 1.0 (got %.2f)\n", 
                vgen_config.sparsity);
        pthread_rwlock_unlock(&vgen_lock);
        return -1;
    }
    
    /* Initialize the vector generator */
    vgen_instance = vg_init(&vgen_config);
    if (vgen_instance == NULL) {
        fprintf(stderr, "Error: Failed to initialize vector generator\n");
        pthread_rwlock_unlock(&vgen_lock);
        return -1;
    }
    
    /* Initialize thread pools */
    init_thread_pools();
    
    /* Initialize ground truth storage */
    pthread_mutex_init(&ground_truth.lock, NULL);
    ground_truth.capacity = MAX_GROUND_TRUTH_ENTRIES;
    ground_truth.count = 0;
    ground_truth.entries = calloc(ground_truth.capacity, sizeof(stored_ground_truth_t));
    if (ground_truth.entries == NULL) {
        fprintf(stderr, "Error: Failed to allocate ground truth storage\n");
        vg_destroy(vgen_instance);
        vgen_instance = NULL;
        pthread_rwlock_unlock(&vgen_lock);
        return -1;
    }
    
    /* Initialize recall tracker */
    pthread_mutex_init(&recall_tracker.lock, NULL);
    vgen_reset_recall_stats();
    
    printf("Vector generator initialized:\n");
    printf("  Dimensions: %u\n", vgen_config.dimensions);
    printf("  Initial capacity: %lu\n", vgen_config.initial_capacity);
    printf("  Centroids: %u\n", vgen_config.num_centroids);
    printf("  Radius: %.2f\n", vgen_config.radius);
    printf("  Sparsity: %.2f\n", vgen_config.sparsity);
    printf("  Seed: %lu\n", vgen_config.seed);
    
    pthread_rwlock_unlock(&vgen_lock);
    return 0;
}

/**
 * Cleanup and destroy the vector generator instance.
 */
void vgen_cleanup(void) {
    pthread_rwlock_wrlock(&vgen_lock);
    
    if (vgen_instance == NULL) {
        pthread_rwlock_unlock(&vgen_lock);
        return;
    }
    
    /* Cleanup thread pools first */
    cleanup_thread_pools();
    
    /* Cleanup ground truth storage */
    pthread_mutex_lock(&ground_truth.lock);
    if (ground_truth.entries) {
        free(ground_truth.entries);
        ground_truth.entries = NULL;
        ground_truth.count = 0;
        ground_truth.capacity = 0;
    }
    pthread_mutex_unlock(&ground_truth.lock);
    pthread_mutex_destroy(&ground_truth.lock);
    
    /* Destroy vector generator */
    vg_destroy(vgen_instance);
    vgen_instance = NULL;
    
    /* Cleanup recall tracker */
    pthread_mutex_destroy(&recall_tracker.lock);
    
    pthread_rwlock_unlock(&vgen_lock);
    pthread_rwlock_destroy(&vgen_lock);
}

/**
 * Replace both key and vector placeholders for ground truth ingestion.
 * This ingests the reserved-range vectors that serve as ground truth neighbors.
 */
void vgen_replace_ground_truth_placeholder(int thread_id, const size_t *key_indices, const size_t key_count,
                                            const size_t *vec_indices, const size_t vec_count,
                                            char *cmd, uint64_t *key_counter,
                                            uint64_t *vector_counter) {
    if (!vgen_is_initialized()) return;
    if (key_count == 0 && vec_count == 0) return;
    
    /* Should have matching counts */
    if (key_count != vec_count) {
        fprintf(stderr, "Warning: key_count (%zu) != vec_count (%zu) in ground truth ingestion\n",
                key_count, vec_count);
        return;
    }
    
    pthread_rwlock_rdlock(&vgen_lock);
    
    /* Get or create ground truth iterator for this thread */
    thread_iterator_pool_t *pool = &thread_pools[thread_id];
    
    pthread_mutex_lock(&pool->lock);
    
    /* For ground truth, we need to iterate over ALL possible neighbor keys from reserved range.
     * The vector generator pre-allocates neighbor keys for each query in the reserved range.
     * We need to extract all unique neighbor keys and generate vectors for them. */
    
    /* Get the total number of ground truth vectors we need to ingest.
     * This is: num_query_vectors * NEIGHBORS_PER_QUERY unique keys from reserved range */
    extern vector_generator_t* vgen_instance;
    
    /* For now, use a simple approach: generate vectors for keys in reserved range (1-999999)
     * that will be used as neighbors. We'll use a separate static counter. */
    
    static uint64_t ground_truth_key_index = 0;
    
    /* Generate vectors for reserved range keys */
    for (size_t i = 0; i < key_count; i++) {
        /* Use sequential keys from reserved range starting at 1 */
        vector_key_t key = (ground_truth_key_index++) % 1000000; /* Reserved range: 0-999999 */
        if (key == 0) key = 1; /* Skip key 0, start from 1 */
        
        /* DEBUG: Print ground truth key being generated */
        static int gt_debug_count = 0;
        if (gt_debug_count < 10) {
            printf("[VGEN GROUND_TRUTH] Ingesting reserved key: %lu (iteration %d)\n", key, gt_debug_count);
            gt_debug_count++;
        }
        
        /* Replace key placeholder */
        if (i < key_count) {
            char key_buf[32];
            int key_len = snprintf(key_buf, sizeof(key_buf), "%lu", key);
            
            char *key_placeholder = cmd + key_indices[i];
            memset(key_placeholder, 0, 16);
            memcpy(key_placeholder, key_buf, key_len < 16 ? key_len : 16);
            
            uint32_t actual_key_len = (uint32_t)key_len;
            memcpy(key_placeholder + 16, &actual_key_len, 4);
        }
        
        /* Replace vector placeholder - generate vector for this reserved key */
        if (i < vec_count) {
            char *vec_placeholder = cmd + vec_indices[i];
            uint32_t dims = vg_get_dimensions(vgen_instance);
            
            /* Generate vector from the reserved key */
            float *vector_data = malloc(dims * sizeof(float));
            if (vector_data) {
                vg_generate_vector_from_key(vgen_instance, key, vector_data);
                memcpy(vec_placeholder, vector_data, dims * sizeof(float));
                free(vector_data);
            }
        }
    }
    
    pthread_mutex_unlock(&pool->lock);
    pthread_rwlock_unlock(&vgen_lock);
    
    (void)key_counter;
    (void)vector_counter;
}

/**
 * Replace key placeholder for deletion operations.
 */
void vgen_replace_key_placeholder(int thread_id, const size_t *indices, const size_t count,
                                   char *cmd, uint64_t *key_counter) {
    if (!vgen_is_initialized() || count == 0) return;
    
    pthread_rwlock_rdlock(&vgen_lock);
    
    /* Get or create deletion iterator for this thread */
    thread_iterator_pool_t *pool = &thread_pools[thread_id];
    
    pthread_mutex_lock(&pool->lock);
    
    if (pool->deletion_iter == NULL) {
        /* Create deletion iterator - iterate over previously ingested vectors */
        pool->deletion_iter = vg_get_deletion_iterator(vgen_instance, count);
    }
    
    /* Get keys from deletion iterator and replace placeholders */
    for (size_t i = 0; i < count; i++) {
        vector_key_t key;
        if (!vg_iterator_next_key(pool->deletion_iter, &key)) {
            /* Iterator exhausted, recreate it */
            vg_iterator_destroy(pool->deletion_iter);
            pool->deletion_iter = vg_get_deletion_iterator(vgen_instance, count);
            if (!vg_iterator_next_key(pool->deletion_iter, &key)) {
                /* Still no keys available - skip */
                continue;
            }
        }
        
        /* Format key - key number only, prefix already in template */
        char key_buf[32];
        int key_len = snprintf(key_buf, sizeof(key_buf), "%lu", key);
        
        /* Replace placeholder in command buffer */
        /* The placeholder location includes the prefix{tag}: already */
        /* We need to replace just the 16-byte VGEN_KEY_PLACEHOLDER + 4 byte length */
        char *placeholder = cmd + indices[i];
        
        /* Write the actual key number into the 16-byte placeholder space */
        memset(placeholder, 0, 16);  /* Clear placeholder first */
        memcpy(placeholder, key_buf, key_len < 16 ? key_len : 16);
        
        /* Write the actual key length in the 4-byte length field */
        uint32_t actual_key_len = (uint32_t)key_len;
        memcpy(placeholder + 16, &actual_key_len, 4);
    }
    
    pthread_mutex_unlock(&pool->lock);
    pthread_rwlock_unlock(&vgen_lock);
    
    (void)key_counter; /* Unused for vgen */
}

/**
 * Replace vector placeholder for query operations.
 */
uint64_t vgen_replace_vector_placeholder_query(int thread_id, const size_t *indices, const size_t count,
                                            char *cmd, uint64_t *vector_counter) {
    if (!vgen_is_initialized() || count == 0) return UINT64_MAX;
    
    pthread_rwlock_rdlock(&vgen_lock);
    
    /* Get or create query iterator for this thread */
    thread_iterator_pool_t *pool = &thread_pools[thread_id];
    
    pthread_mutex_lock(&pool->lock);
    
    if (pool->query_iter == NULL) {
        /* Create query iterator with unlimited count - we'll cycle through queries */
        pool->query_iter = vg_get_query_iterator(vgen_instance, UINT64_MAX);
    }
    
    uint64_t query_idx = UINT64_MAX;
    
    /* Get query vectors and replace placeholders */
    for (size_t i = 0; i < count; i++) {
        query_vector_t query;
        if (!vg_iterator_next_query(pool->query_iter, &query)) {
            /* Iterator exhausted, reset it to cycle through queries again */
            pool->query_iter->current = 0;
            if (!vg_iterator_next_query(pool->query_iter, &query)) {
                /* Still no vectors available - skip */
                continue;
            }
        }
        
        /* Replace placeholder with vector data */
        char *placeholder = cmd + indices[i];
        /* VGEN_VECTOR_PLACEHOLDER is 16 bytes at the start of the vector */
        /* We need to replace the ENTIRE vector, not just the placeholder */
        uint32_t dims = vg_get_dimensions(vgen_instance);
        size_t vector_bytes = dims * sizeof(float);
        
        /* Replace the entire vector data (placeholder is at the beginning) */
        memcpy(placeholder, query.vector.data, vector_bytes);
        
        /* Store ground truth for recall calculation and get query index */
        pthread_mutex_lock(&ground_truth.lock);
        if (ground_truth.count < ground_truth.capacity) {
            query_idx = ground_truth.count;  /* Store the index before incrementing */
            stored_ground_truth_t *entry = &ground_truth.entries[ground_truth.count];
            entry->query_key = query.vector.key;
            
            /* DEBUG: Print query key and ground truth */
            static int query_debug_count = 0;
            if (query_debug_count < 30) {
                printf("[VGEN QUERY] Query key: %lu, Expected neighbors: ", query.vector.key);
                for (int j = 0; j < NEIGHBORS_PER_QUERY && j < 5; j++) {
                    printf("%lu ", query.ground_truth[j].key);
                }
                printf("...\n");
                query_debug_count++;
            }
            
            /* Copy ground truth neighbors from query */
            entry->neighbor_count = NEIGHBORS_PER_QUERY;
            for (int j = 0; j < NEIGHBORS_PER_QUERY; j++) {
                entry->neighbors[j] = query.ground_truth[j];
            }
            ground_truth.count++;
        }
        pthread_mutex_unlock(&ground_truth.lock);
    }
    
    pthread_mutex_unlock(&pool->lock);
    pthread_rwlock_unlock(&vgen_lock);
    
    (void)vector_counter; /* Unused for vgen */
    return query_idx;
}

/**
 * Replace both key and vector placeholders for ingestion operations.
 */
void vgen_replace_vector_and_key_placeholder(int thread_id, const size_t *key_indices, const size_t key_count,
                                              const size_t *vec_indices, const size_t vec_count,
                                              char *cmd, uint64_t *key_counter,
                                              uint64_t *vector_counter) {
    if (!vgen_is_initialized()) return;
    if (key_count == 0 && vec_count == 0) return;
    
    /* Should have matching counts for ingestion */
    if (key_count != vec_count) {
        fprintf(stderr, "Warning: key_count (%zu) != vec_count (%zu) in ingestion\n",
                key_count, vec_count);
        return;
    }
    
    pthread_rwlock_rdlock(&vgen_lock);
    
    /* Get or create ingestion iterator for this thread */
    thread_iterator_pool_t *pool = &thread_pools[thread_id];
    
    pthread_mutex_lock(&pool->lock);
    
    if (pool->ingestion_iter == NULL) {
        /* Create ingestion iterator with random order */
        pool->ingestion_iter = vg_get_ingestion_iterator(vgen_instance, 
                                                         key_count, 
                                                         GEN_ORDER_RANDOM);
    }
    
    /* Get vectors from ingestion iterator and replace both keys and vectors */
    for (size_t i = 0; i < key_count; i++) {
        vector_t vec;
        if (!vg_iterator_next(pool->ingestion_iter, &vec)) {
            /* Iterator exhausted, recreate it */
            vg_iterator_destroy(pool->ingestion_iter);
            pool->ingestion_iter = vg_get_ingestion_iterator(vgen_instance, 
                                                             key_count,
                                                             GEN_ORDER_RANDOM);
            if (!vg_iterator_next(pool->ingestion_iter, &vec)) {
                /* Still no vectors available - skip */
                continue;
            }
        }
        
        /* Replace key placeholder */
        if (i < key_count) {
            /* Format key number only - prefix already in template */
            char key_buf[32];
            int key_len = snprintf(key_buf, sizeof(key_buf), "%lu", vec.key);
            
            /* DEBUG: Print key being generated for ingestion */
            static int debug_count = 0;
            if (debug_count < 10) {
                printf("[VGEN INGESTION] Generated key: %lu (iteration %d)\n", vec.key, debug_count);
                debug_count++;
            }
            
            char *key_placeholder = cmd + key_indices[i];
            /* Write key number into 16-byte placeholder space */
            memset(key_placeholder, 0, 16);  /* Clear first */
            memcpy(key_placeholder, key_buf, key_len < 16 ? key_len : 16);
            
            /* Write actual key length in 4-byte length field */
            uint32_t actual_key_len = (uint32_t)key_len;
            memcpy(key_placeholder + 16, &actual_key_len, 4);
        }
        
        /* Replace vector placeholder */
        if (i < vec_count) {
            uint32_t dims = vg_get_dimensions(vgen_instance);
            size_t vector_bytes = dims * sizeof(float);
            char *vec_placeholder = cmd + vec_indices[i];
            
            /* Replace entire vector data */
            memcpy(vec_placeholder, vec.data, vector_bytes);
        }
    }
    
    pthread_mutex_unlock(&pool->lock);
    pthread_rwlock_unlock(&vgen_lock);
    
    (void)key_counter;
    (void)vector_counter;
}

/**
 * Compute recall for a search result.
 */
void vgen_compute_recall(uint64_t query_idx, void *reply) {
    if (!vgen_is_initialized() || !reply) return;
    
    valkeyReply *r = (valkeyReply *)reply;
    
    /* FT.SEARCH returns an array: [total_count, key1, fields1, key2, fields2, ...] */
    if (r->type != VALKEY_REPLY_ARRAY || r->elements < 1) {
        return;
    }
    
    /* Skip if no valid query index */
    if (query_idx == UINT64_MAX) {
        return;
    }
    
    /* Check if we have ground truth for this query */
    pthread_mutex_lock(&ground_truth.lock);
    if (query_idx >= ground_truth.count) {
        pthread_mutex_unlock(&ground_truth.lock);
        return;  /* No ground truth available */
    }
    
    stored_ground_truth_t *gt_entry = &ground_truth.entries[query_idx];
    int expected_neighbors = gt_entry->neighbor_count;
    uint64_t query_key = gt_entry->query_key;
    pthread_mutex_unlock(&ground_truth.lock);
    
    if (expected_neighbors == 0) {
        return;  /* No neighbors to compare */
    }
    
    /* Extract returned keys from the reply */
    /* Format: [count, key1, fields1, key2, fields2, ...] */
    vector_key_t returned_keys[NEIGHBORS_PER_QUERY];
    int returned_count = 0;
    
    /* Parse the reply to extract keys (skip element 0 which is the count) */
    for (size_t i = 1; i < r->elements && returned_count < NEIGHBORS_PER_QUERY; i += 2) {
        if (i >= r->elements) break;
        
        valkeyReply *keyReply = r->element[i];
        if (keyReply && (keyReply->type == VALKEY_REPLY_STRING || keyReply->type == VALKEY_REPLY_STATUS)) {
            /* Parse the key string to extract the vector key (numeric ID) */
            /* Expected format: "vec:<prefix>:<key>" or similar */
            const char *key_str = keyReply->str;
            const char *last_colon = strrchr(key_str, ':');
            if (last_colon != NULL) {
                vector_key_t key = (vector_key_t)atoll(last_colon + 1);
                returned_keys[returned_count++] = key;
            }
        }
    }
    
    /* Compute recall: count how many of the returned keys are in the ground truth neighbors */
    int matches = 0;
    pthread_mutex_lock(&ground_truth.lock);
    
    /* Debug output for first 20 queries */
    static _Atomic int debug_count = 0;
    int current_debug = atomic_fetch_add(&debug_count, 1);
    int should_debug = (current_debug < 20);
    
    if (should_debug) {
        printf("\n[RECALL DEBUG #%d] Query idx=%lu, query_key=%lu\n", 
               current_debug, query_idx, query_key);
        printf("  Expected neighbors (%d): ", expected_neighbors);
        for (int j = 0; j < expected_neighbors && j < 5; j++) {
            printf("%lu ", gt_entry->neighbors[j].key);
        }
        if (expected_neighbors > 5) printf("...");
        printf("\n");
        printf("  Returned neighbors (%d): ", returned_count);
        for (int i = 0; i < returned_count && i < 5; i++) {
            printf("%lu ", returned_keys[i]);
        }
        if (returned_count > 5) printf("...");
        printf("\n");
    }
    
    for (int i = 0; i < returned_count; i++) {
        for (int j = 0; j < expected_neighbors; j++) {
            if (returned_keys[i] == gt_entry->neighbors[j].key) {
                matches++;
                break;
            }
        }
    }
    
    if (should_debug) {
        printf("  Matches: %d/%d\n", matches, returned_count < expected_neighbors ? returned_count : expected_neighbors);
    }
    
    pthread_mutex_unlock(&ground_truth.lock);
    
    /* Calculate recall@K where K is the minimum of returned and expected */
    int k = returned_count < expected_neighbors ? returned_count : expected_neighbors;
    double recall = (k > 0) ? ((double)matches / (double)k) : 0.0;
    
    /* Update recall statistics */
    pthread_mutex_lock(&recall_tracker.lock);
    recall_tracker.total_queries++;
    recall_tracker.total_recall += recall;
    if (recall < recall_tracker.min_recall) {
        recall_tracker.min_recall = recall;
    }
    if (recall > recall_tracker.max_recall) {
        recall_tracker.max_recall = recall;
    }
    pthread_mutex_unlock(&recall_tracker.lock);
}

/**
 * Get ground truth for a specific query index.
 */
int vgen_get_ground_truth(uint64_t query_idx, uint64_t *neighbors) {
    if (!vgen_is_initialized() || !neighbors) return 0;
    
    pthread_mutex_lock(&ground_truth.lock);
    if (query_idx >= ground_truth.count) {
        pthread_mutex_unlock(&ground_truth.lock);
        return 0;
    }
    
    stored_ground_truth_t *gt_entry = &ground_truth.entries[query_idx];
    int count = gt_entry->neighbor_count;
    
    for (int i = 0; i < count; i++) {
        neighbors[i] = gt_entry->neighbors[i].key;
    }
    
    pthread_mutex_unlock(&ground_truth.lock);
    return count;
}

/**
 * Get current query index for ground truth lookup.
 */
uint64_t vgen_get_current_query_index(void) {
    if (!vgen_is_initialized()) return 0;
    
    pthread_mutex_lock(&recall_tracker.lock);
    uint64_t idx = recall_tracker.current_query_index;
    pthread_mutex_unlock(&recall_tracker.lock);
    
    return idx;
}

/**
 * Print recall statistics report.
 */
void vgen_print_recall_report(void) {
    pthread_mutex_lock(&recall_tracker.lock);
    
    if (recall_tracker.total_queries == 0) {
        pthread_mutex_unlock(&recall_tracker.lock);
        return;
    }
    
    double avg_recall = recall_tracker.total_recall / recall_tracker.total_queries;
    
    printf("\n");
    printf("====== Recall Statistics ======\n");
    printf("  Total queries: %lu\n", recall_tracker.total_queries);
    printf("  Average recall: %.2f%%\n", avg_recall * 100.0);
    printf("  Min recall: %.2f%%\n", recall_tracker.min_recall * 100.0);
    printf("  Max recall: %.2f%%\n", recall_tracker.max_recall * 100.0);
    printf("\n");
    
    pthread_mutex_unlock(&recall_tracker.lock);
}

/**
 * Get vector generator statistics.
 */
void vgen_get_stats(uint64_t *total_generated, uint64_t *total_deleted,
                    uint64_t *active_vectors) {
    pthread_rwlock_rdlock(&vgen_lock);
    
    if (vgen_instance == NULL) {
        if (total_generated) *total_generated = 0;
        if (total_deleted) *total_deleted = 0;
        if (active_vectors) *active_vectors = 0;
        pthread_rwlock_unlock(&vgen_lock);
        return;
    }
    
    generator_stats_t stats;
    vg_get_stats(vgen_instance, &stats);
    
    if (total_generated) *total_generated = stats.total_generated;
    if (total_deleted) *total_deleted = stats.total_deleted;
    if (active_vectors) *active_vectors = stats.active_vectors;
    
    pthread_rwlock_unlock(&vgen_lock);
}

/**
 * Check if vector generator is initialized.
 */
int vgen_is_initialized(void) {
    pthread_rwlock_rdlock(&vgen_lock);
    int initialized = (vgen_instance != NULL);
    pthread_rwlock_unlock(&vgen_lock);
    return initialized;
}

/**
 * Reset recall tracking statistics.
 */
void vgen_reset_recall_stats(void) {
    pthread_mutex_lock(&recall_tracker.lock);
    
    recall_tracker.total_queries = 0;
    recall_tracker.total_recall = 0.0;
    recall_tracker.min_recall = 1.0;
    recall_tracker.max_recall = 0.0;
    
    pthread_mutex_unlock(&recall_tracker.lock);
}
