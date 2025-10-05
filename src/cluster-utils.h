#ifndef CLUSTER_UTILS_H
#define CLUSTER_UTILS_H

#include <stddef.h>

/**
 * Shared Cluster Utilities
 *
 * Common functions for handling cluster-related operations like
 * cluster tag extraction from keys and buffers.
 */

/**
 * Extract cluster tag from key format (handles both buffer offsets and key strings)
 *
 * This function can handle two cases:
 * 1. Complete key strings like "prefix{tag}:vector_id" - extracts tag from between { }
 * 2. Raw cluster tag data at buffer positions - copies the raw data directly
 *
 * @param source Source string or buffer position
 * @param max_len Maximum length to read (0 for unlimited)
 * @param output_tag Output buffer for extracted tag
 * @param output_size Size of output buffer
 * @return 0 on success, -1 on error
 */
int extractClusterTag(const char *source, size_t max_len, char *output_tag, size_t output_size);

/**
 * Convenience wrapper for extracting cluster tag from complete key strings
 * @param key Complete key string
 * @return Allocated cluster tag string (caller must free) or NULL on error
 */
char* extractClusterTagFromKey(const char *key);

#endif /* CLUSTER_UTILS_H */