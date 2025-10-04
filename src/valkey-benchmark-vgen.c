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

/* External config reference - will be properly linked */
extern struct config config;

/* Global vector generator instance */
static vector_generator_t *vgen_instance = NULL;
static pthread_rwlock_t vgen_lock = PTHREAD_RWLOCK_INITIALIZER;

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
int vgen_init_from_config(void) {
    pthread_rwlock_wrlock(&vgen_lock);
    
    if (vgen_instance != NULL) {
        fprintf(stderr, "Warning: Vector generator already initialized\n");
        pthread_rwlock_unlock(&vgen_lock);
        return 0;
    }
    
    /* Create generator configuration from benchmark config */
    generator_config_t vgen_config = {
        .dimensions = 128,           /* Default, will be overridden */
        .initial_capacity = 1000000, /* Default 1M vectors */
        .sparsity = 0.0,            /* Default no sparsity */
        .radius = 5.0,              /* Default clustering radius */
        .num_centroids = 10,        /* Default 10 clusters */
        .seed = 42                  /* Default seed */
    };
    
    /* TODO: Read from config structure once config fields are added in Phase 2 */
    /* For now, use defaults or environment variables for testing */
    const char *env_capacity = getenv("VGEN_CAPACITY");
    if (env_capacity) {
        vgen_config.initial_capacity = strtoull(env_capacity, NULL, 10);
    }
    
    const char *env_centroids = getenv("VGEN_CENTROIDS");
    if (env_centroids) {
        vgen_config.num_centroids = (uint32_t)atoi(env_centroids);
    }
    
    const char *env_radius = getenv("VGEN_RADIUS");
    if (env_radius) {
        vgen_config.radius = (float)atof(env_radius);
    }
    
    const char *env_sparsity = getenv("VGEN_SPARSITY");
    if (env_sparsity) {
        vgen_config.sparsity = (float)atof(env_sparsity);
    }
    
    const char *env_seed = getenv("VGEN_SEED");
    if (env_seed) {
        vgen_config.seed = strtoull(env_seed, NULL, 10);
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
    /* Placeholder implementation for Phase 1 */
    /* Will be implemented in Phase 4 */
    (void)indices;
    (void)count;
    (void)cmd;
    (void)key_counter;
    
    /* TODO: Phase 4 - Get deletion iterator and replace keys */
}

/**
 * Replace vector placeholder for query operations.
 */
void vgen_replace_vector_placeholder_query(const size_t *indices, const size_t count,
                                            char *cmd, uint64_t *vector_counter) {
    /* Placeholder implementation for Phase 1 */
    /* Will be implemented in Phase 4 */
    (void)indices;
    (void)count;
    (void)cmd;
    (void)vector_counter;
    
    /* TODO: Phase 4 - Get query iterator and replace vectors with ground truth */
}

/**
 * Replace both key and vector placeholders for ingestion operations.
 */
void vgen_replace_vector_and_key_placeholder(const size_t *key_indices, const size_t key_count,
                                              const size_t *vec_indices, const size_t vec_count,
                                              char *cmd, uint64_t *key_counter,
                                              uint64_t *vector_counter) {
    /* Placeholder implementation for Phase 1 */
    /* Will be implemented in Phase 4 */
    (void)key_indices;
    (void)key_count;
    (void)vec_indices;
    (void)vec_count;
    (void)cmd;
    (void)key_counter;
    (void)vector_counter;
    
    /* TODO: Phase 4 - Get ingestion iterator and replace both keys and vectors */
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
