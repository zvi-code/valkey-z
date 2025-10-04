/**
 * valkey-benchmark-vgen.h - Vector Generator Integration for valkey-benchmark
 * 
 * This module provides the integration layer between valkey-benchmark and the
 * vector generator library. It handles vector generation, placeholder replacement,
 * recall tracking, and iterator management in a thread-safe manner.
 */

#ifndef VALKEY_BENCHMARK_VGEN_H
#define VALKEY_BENCHMARK_VGEN_H

#include <stdint.h>
#include <stddef.h>
#include <pthread.h>

/* Forward declarations */
struct config;
typedef struct _client *client;

/**
 * Initialize the vector generator from benchmark configuration.
 * 
 * This should be called once during benchmark initialization, after command-line
 * arguments have been parsed and before any clients are created.
 * 
 * @param dimensions Vector dimensions
 * @param initial_capacity Initial vector capacity
 * @param num_centroids Number of centroids for clustering
 * @param radius Clustering radius  
 * @param sparsity Sparsity level (0.0-1.0)
 * @param seed Random seed
 * @param cluster_mode Whether cluster mode is enabled
 * @param prefix Key prefix for search operations
 * @param num_threads Number of threads for iterator pool (0 = use default)
 * @return 0 on success, -1 on failure
 */
int vgen_init_from_config(uint32_t dimensions, uint64_t initial_capacity,
                           uint32_t num_centroids, float radius,
                           float sparsity, uint64_t seed,
                           int cluster_mode, const char *prefix, int num_threads);

/**
 * Cleanup and destroy the vector generator instance.
 * 
 * This should be called before program exit to free all resources.
 */
void vgen_cleanup(void);

/**
 * Replace both key and vector placeholders for ground truth ingestion.
 * 
 * This function is used to ingest the reserved-range vectors that will serve
 * as ground truth neighbors for queries. Should be called once during setup
 * before running any queries.
 * 
 * @param key_indices Array of key placeholder positions
 * @param key_count Number of key placeholders
 * @param vec_indices Array of vector placeholder positions
 * @param vec_count Number of vector placeholders
 * @param cmd Command buffer to modify in-place
 * @param key_counter Atomic counter for key generation
 * @param vector_counter Atomic counter for vector generation
 */
void vgen_replace_ground_truth_placeholder(int thread_id, const size_t *key_indices, const size_t key_count,
                                            const size_t *vec_indices, const size_t vec_count,
                                            char *cmd, uint64_t *key_counter,
                                            uint64_t *vector_counter);

/**
 * Replace key placeholder in command buffer for deletion operations.
 * 
 * This function replaces VGEN_KEY_PLACEHOLDER with actual keys from the
 * deletion iterator. Keys are selected from previously ingested vectors.
 * 
 * @param indices Array of placeholder positions in command buffer
 * @param count Number of placeholders to replace
 * @param cmd Command buffer to modify in-place
 * @param key_counter Atomic counter for key generation (may be used for tracking)
 */
void vgen_replace_key_placeholder(int thread_id, const size_t *indices, const size_t count,
                                   char *cmd, uint64_t *key_counter);

/**
 * Replace vector placeholders with query vectors from query iterator.
 * 
 * This function replaces VGEN_VECTOR_PLACEHOLDER with query vectors from the
 * query iterator. Ground truth is automatically stored for later recall computation.
 * 
 * @param indices Array of placeholder positions in command buffer
 * @param count Number of placeholders to replace
 * @param cmd Command buffer to modify in-place
 * @param vector_counter Atomic counter for vector generation
 * @return Query index for recall tracking (or UINT64_MAX if no query)
 */
uint64_t vgen_replace_vector_placeholder_query(int thread_id, const size_t *indices, const size_t count,
                                            char *cmd, uint64_t *vector_counter);

/**
 * Replace both key and vector placeholders for ingestion operations.
 * 
 * This function replaces both VGEN_KEY_PLACEHOLDER and VGEN_VECTOR_PLACEHOLDER
 * with corresponding keys and vectors from the ingestion iterator.
 * 
 * @param key_indices Array of key placeholder positions
 * @param key_count Number of key placeholders
 * @param vec_indices Array of vector placeholder positions
 * @param vec_count Number of vector placeholders
 * @param cmd Command buffer to modify in-place
 * @param key_counter Atomic counter for key generation
 * @param vector_counter Atomic counter for vector generation
 */
void vgen_replace_vector_and_key_placeholder(int thread_id, const size_t *key_indices, const size_t key_count,
                                              const size_t *vec_indices, const size_t vec_count,
                                              char *cmd, uint64_t *key_counter,
                                              uint64_t *vector_counter);

/**
 * Compute recall for a search result and update tracking statistics.
 * 
 * This function should be called in the readHandler when search results are received.
 * It extracts the returned keys, compares them with ground truth, and updates
 * recall statistics.
 * 
 * @param query_idx The query index for this search (from client->vgen_query_index)
 * @param reply The search response (valkeyReply*)
 */
void vgen_compute_recall(uint64_t query_idx, void *reply);

/**
 * Print recall statistics report.
 * 
 * This function should be called at the end of benchmarking to display
 * recall metrics (average, min, max, etc.).
 */
void vgen_print_recall_report(void);

/**
 * Get ground truth for a specific query index (for debugging/printing).
 * 
 * @param query_idx The query index
 * @param neighbors Output array to store neighbor keys (caller must allocate at least NEIGHBORS_PER_QUERY elements)
 * @return Number of neighbors (0 if no ground truth available)
 */
int vgen_get_ground_truth(uint64_t query_idx, uint64_t *neighbors);

/**
 * Get the index of the next query to be processed (for ground truth lookup).
 * This returns current_query_index which will be used for the next recall computation.
 * 
 * @return The current query index (next to be processed)
 */
uint64_t vgen_get_current_query_index(void);

/**
 * Get vector generator statistics.
 * 
 * @param total_generated Output: total vectors generated
 * @param total_deleted Output: total vectors deleted
 * @param active_vectors Output: currently active vectors
 */
void vgen_get_stats(uint64_t *total_generated, uint64_t *total_deleted,
                    uint64_t *active_vectors);

/**
 * Check if vector generator is initialized and active.
 * 
 * @return 1 if initialized, 0 otherwise
 */
int vgen_is_initialized(void);

/**
 * Reset recall tracking statistics.
 * 
 * This should be called before starting a new benchmark run.
 */
void vgen_reset_recall_stats(void);

/**
 * Get iterator pool statistics for monitoring and debugging.
 * 
 * @param active_ingestion Output: number of threads with active ingestion iterators
 * @param active_query Output: number of threads with active query iterators
 * @param active_deletion Output: number of threads with active deletion iterators
 */
void vgen_get_iterator_stats(int *active_ingestion, int *active_query, int *active_deletion);

#endif /* VALKEY_BENCHMARK_VGEN_H */
