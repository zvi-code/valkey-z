/**
 * vector_generator.h - Complete header with all required functions
 */

#ifndef VECTOR_GENERATOR_H
#define VECTOR_GENERATOR_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdatomic.h>
#include <pthread.h>

/* Constants */
#define MAX_QUERY_VECTORS 100000
#define RESERVED_KEY_RANGE 1000000
#define NEIGHBORS_PER_QUERY 10

/* Type definitions */
typedef uint64_t vector_key_t;

typedef struct {
    vector_key_t key;
    float* data;
} vector_t;

typedef struct {
    vector_key_t key;
    float distance;
} ground_truth_entry_t;

typedef struct {
    vector_t vector;
    ground_truth_entry_t ground_truth[10];
} query_vector_t;

typedef enum {
    GEN_ORDER_RANDOM,
    GEN_ORDER_CENTROID
} generation_order_t;

typedef struct {
    uint32_t dimensions;
    uint64_t initial_capacity;
    float sparsity;
    float radius;
    uint32_t num_centroids;
    uint64_t seed;
} generator_config_t;

typedef struct {
    float* coordinates;
    uint64_t start_key;
    uint64_t vector_count;
} centroid_t;

typedef struct {
    uint8_t* bits;
    size_t size_bytes;
    uint32_t num_hashes;
} bloom_filter_t;

typedef struct {
    atomic_uint_fast64_t next_key;
    atomic_uint_fast64_t deleted_count;
    atomic_uint_fast64_t active_count;
} key_allocator_t;

/* Pre-computed query ground truth entry */
typedef struct {
    vector_key_t query_key;
    vector_key_t neighbor_keys[NEIGHBORS_PER_QUERY];
    float neighbor_distances[NEIGHBORS_PER_QUERY];
    bool computed;
    pthread_mutex_t compute_mutex;
} query_ground_truth_t;

/* Main generator structure */
typedef struct vector_generator {
    generator_config_t config;
    uint32_t dimensions;
    uint64_t pinned_threshold;
    
    centroid_t* centroids;
    uint32_t num_centroids;
    
    key_allocator_t allocator;
    bloom_filter_t* deleted_keys;
    pthread_rwlock_t delete_lock;
    
    /* Reserved key management */
    uint64_t query_key_start;       /* Start of reserved range (1) */
    uint64_t query_key_end;         /* End of reserved range (1M) */
    uint64_t general_key_start;     /* Start of general keys (1M+1) */
    
    /* Pre-computed ground truth */
    query_ground_truth_t* query_ground_truth;
    uint32_t num_query_vectors;
    atomic_uint_fast32_t query_index;
} vector_generator_t;

typedef enum {
    ITER_TYPE_INGESTION = 0,
    ITER_TYPE_QUERY = 1,
    ITER_TYPE_DELETION = 2
} iterator_type_t;

typedef struct vector_iterator {
    vector_generator_t* generator;
    iterator_type_t type;
    generation_order_t order;
    
    uint64_t count;
    uint64_t current;
    uint64_t* key_sequence;
    
    uint32_t current_centroid;
    uint64_t vectors_in_centroid;
    
    float* vector_buffer;
} vector_iterator_t;

typedef struct {
    uint64_t total_generated;
    uint64_t total_deleted;
    uint64_t active_vectors;
    uint64_t pinned_vectors;
} generator_stats_t;

/* Public API */
vector_generator_t* vg_init(const generator_config_t* config);
void vg_destroy(vector_generator_t* gen);
vector_iterator_t* vg_get_ingestion_iterator(vector_generator_t* gen, uint64_t count, generation_order_t order);
vector_iterator_t* vg_get_query_iterator(vector_generator_t* gen, uint64_t count);
vector_iterator_t* vg_get_deletion_iterator(vector_generator_t* gen, uint64_t count);
bool vg_iterator_next(vector_iterator_t* iter, vector_t* vec);
bool vg_iterator_next_query(vector_iterator_t* iter, query_vector_t* query);
bool vg_iterator_next_key(vector_iterator_t* iter, vector_key_t* key);
void vg_iterator_destroy(vector_iterator_t* iter);
void vg_get_stats(const vector_generator_t* gen, generator_stats_t* stats);
void vg_generate_vector_from_key(const vector_generator_t* gen, vector_key_t key, float* output);
float vg_compute_l2_distance(const float* v1, const float* v2, uint32_t dimensions);

/* For testing ground truth accuracy */
void vg_find_ground_truth(const vector_generator_t* gen, const vector_t* query, 
                         ground_truth_entry_t* ground_truth, uint32_t k);

static inline uint32_t vg_get_dimensions(const vector_generator_t* gen) {
    return gen->dimensions;
}

/* Test helper functions - inline implementations */
static inline uint32_t vg_prng_next(uint64_t* state) {
    *state ^= *state << 13;
    *state ^= *state >> 17;
    *state ^= *state << 5;
    return (uint32_t)(*state >> 32);
}

static inline float vg_prng_to_float(uint32_t prng_val) {
    return ((float)prng_val / (float)UINT32_MAX) * 2.0f - 1.0f;
}

#endif /* VECTOR_GENERATOR_H */