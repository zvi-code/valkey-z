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

/* Recall tracking structure */
typedef struct {
    uint64_t total_queries;
    double total_recall;
    double min_recall;
    double max_recall;
    pthread_mutex_t lock;
} recall_tracker_t;

static recall_tracker_t recall_tracker = {
    .total_queries = 0,
    .total_recall = 0.0,
    .min_recall = 1.0,
    .max_recall = 0.0,
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
    
    /* Destroy vector generator */
    vg_destroy(vgen_instance);
    vgen_instance = NULL;
    
    /* Cleanup recall tracker */
    pthread_mutex_destroy(&recall_tracker.lock);
    
    pthread_rwlock_unlock(&vgen_lock);
    pthread_rwlock_destroy(&vgen_lock);
}

/**
 * Replace key placeholder for deletion operations.
 */
void vgen_replace_key_placeholder(const size_t *indices, const size_t count,
                                   char *cmd, uint64_t *key_counter) {
    if (!vgen_is_initialized() || count == 0) return;
    
    pthread_rwlock_rdlock(&vgen_lock);
    
    /* Get or create deletion iterator for this thread */
    int thread_id = 0; /* TODO: Get actual thread ID from config */
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
void vgen_replace_vector_placeholder_query(const size_t *indices, const size_t count,
                                            char *cmd, uint64_t *vector_counter) {
    if (!vgen_is_initialized() || count == 0) return;
    
    pthread_rwlock_rdlock(&vgen_lock);
    
    /* Get or create query iterator for this thread */
    int thread_id = 0; /* TODO: Get actual thread ID from config */
    thread_iterator_pool_t *pool = &thread_pools[thread_id];
    
    pthread_mutex_lock(&pool->lock);
    
    if (pool->query_iter == NULL) {
        /* Create query iterator */
        pool->query_iter = vg_get_query_iterator(vgen_instance, count);
    }
    
    /* Get query vectors and replace placeholders */
    for (size_t i = 0; i < count; i++) {
        query_vector_t query;
        if (!vg_iterator_next_query(pool->query_iter, &query)) {
            /* Iterator exhausted, recreate it */
            vg_iterator_destroy(pool->query_iter);
            pool->query_iter = vg_get_query_iterator(vgen_instance, count);
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
        
        /* Store ground truth for later recall computation */
        /* TODO: Phase 5 - Store ground truth mapping for recall calculation */
    }
    
    pthread_mutex_unlock(&pool->lock);
    pthread_rwlock_unlock(&vgen_lock);
    
    (void)vector_counter; /* Unused for vgen */
}

/**
 * Replace both key and vector placeholders for ingestion operations.
 */
void vgen_replace_vector_and_key_placeholder(const size_t *key_indices, const size_t key_count,
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
    int thread_id = 0; /* TODO: Get actual thread ID from config */
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
            char *vec_placeholder = cmd + vec_indices[i];
            uint32_t dims = vg_get_dimensions(vgen_instance);
            size_t vector_bytes = dims * sizeof(float);
            
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
void vgen_compute_recall(client c, void *reply) {
    /* Placeholder implementation for Phase 1 */
    /* Will be implemented in Phase 5 */
    (void)c;
    (void)reply;
    
    /* TODO: Phase 5 - Extract keys from reply and compare with ground truth */
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
