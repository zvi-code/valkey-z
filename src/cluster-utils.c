/**
 * Shared Cluster Utilities Implementation
 */

#include "cluster-utils.h"
#include "zmalloc.h"
#include <string.h>

int extractClusterTag(const char *source, size_t max_len, char *output_tag, size_t output_size) {
    if (!source || !output_tag || output_size < 6) return -1;

    const char *tag_start = strchr(source, '{');
    if (!tag_start) {
        /* No braces found - could be raw cluster tag at buffer position */
        /* Copy the raw cluster tag data, respecting both max_len and output_size */
        size_t copy_len = (max_len > 0 && max_len < output_size - 1) ? max_len : output_size - 1;
        memcpy(output_tag, source, copy_len);
        output_tag[copy_len] = '\0';
        return 0;
    }

    const char *tag_end = strchr(tag_start, '}');
    if (!tag_end || tag_end <= tag_start + 1) return -1;

    /* Extract tag between { and } */
    size_t tag_len = tag_end - tag_start - 1;
    if (tag_len == 0 || tag_len >= output_size) return -1;

    memcpy(output_tag, tag_start + 1, tag_len);
    output_tag[tag_len] = '\0';
    return 0;
}

char* extractClusterTagFromKey(const char *key) {
    if (!key) return NULL;

    char temp_tag[6];
    if (extractClusterTag(key, 0, temp_tag, sizeof(temp_tag)) != 0) {
        return NULL;
    }

    char *cluster_tag = zmalloc(strlen(temp_tag) + 1);
    strcpy(cluster_tag, temp_tag);
    return cluster_tag;
}