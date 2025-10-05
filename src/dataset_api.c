#include "dataset_api.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>

/* Internal context */
struct dataset_ctx {
    int fd;
    void *mmap_base;
    size_t mmap_size;
    dataset_header_t *header;
    float *vectors;
    float *queries;
    int64_t *ground_truth;
};

/* Path resolution: try multiple locations */
static int resolve_dataset_path(const char *name, char *out, size_t size) {
    const char *search_paths[] = {
        "%s",                           /* Direct path */
        "./_datasets_prepared/%s.bin",  /* Local prepared */
        "./utils/datasets/%s.bin",      /* Utils location */
        "/var/datasets/%s.bin",         /* System location */
        NULL
    };

    for (int i = 0; search_paths[i]; i++) {
        snprintf(out, size, search_paths[i], name);
        if (access(out, R_OK) == 0) return 0;
    }

    fprintf(stderr, "Dataset not found: %s\n", name);
    return -1;
}

dataset_ctx_t* dataset_init(const char *dataset_name, dataset_info_t *info) {
    char path[1024];
    if (resolve_dataset_path(dataset_name, path, sizeof(path)) != 0) {
        return NULL;
    }

    /* Open file read-only */
    int fd = open(path, O_RDONLY);
    if (fd < 0) {
        fprintf(stderr, "Failed to open: %s (%s)\n", path, strerror(errno));
        return NULL;
    }

    /* Get file size */
    struct stat st;
    if (fstat(fd, &st) != 0) {
        fprintf(stderr, "Failed to stat: %s\n", strerror(errno));
        close(fd);
        return NULL;
    }

    /* mmap entire file - OS handles caching */
    void *base = mmap(NULL, st.st_size, PROT_READ, MAP_SHARED, fd, 0);
    if (base == MAP_FAILED) {
        fprintf(stderr, "Failed to mmap: %s\n", strerror(errno));
        close(fd);
        return NULL;
    }

    /* Advise sequential access for prefill, random for queries */
    madvise(base, st.st_size, MADV_WILLNEED);

    /* Validate header */
    dataset_header_t *header = (dataset_header_t*)base;
    if (header->magic != DATASET_MAGIC) {
        fprintf(stderr, "Invalid magic: 0x%x (expected 0x%x)\n", header->magic, DATASET_MAGIC);
        munmap(base, st.st_size);
        close(fd);
        return NULL;
    }

    /* Allocate context */
    dataset_ctx_t *ctx = calloc(1, sizeof(dataset_ctx_t));
    if (!ctx) {
        munmap(base, st.st_size);
        close(fd);
        return NULL;
    }

    /* Initialize pointers into mmap */
    ctx->fd = fd;
    ctx->mmap_base = base;
    ctx->mmap_size = st.st_size;
    ctx->header = header;
    ctx->vectors = (float*)((uint8_t*)base + header->vectors_offset);
    ctx->queries = (float*)((uint8_t*)base + header->queries_offset);
    ctx->ground_truth = (int64_t*)((uint8_t*)base + header->ground_truth_offset);

    /* Return metadata */
    if (info) {
        info->distance_metric = header->distance_metric;
        info->dtype = header->dtype;
        info->dim = header->dim;
        info->num_vectors = header->num_vectors;
        info->num_queries = header->num_queries;
        info->num_neighbors = header->num_neighbors;
    }

    fprintf(stderr, "Dataset loaded: %s (%.2f GB, %lu vecs, %u dims)\n",
            header->dataset_name,
            (double)st.st_size / (1024*1024*1024),
            header->num_vectors, header->dim);

    return ctx;
}

int dataset_prefill(dataset_ctx_t *ctx, uint64_t index,
                    uint64_t *id_out, float *vec_out) {
    if (!ctx || index >= ctx->header->num_vectors) {
        return -1;
    }

    /* ID is the index itself (can extend later) */
    *id_out = index;

    /* Copy vector - cache-friendly sequential access */
    if (vec_out) {
        const uint32_t dim = ctx->header->dim;
        const float *src = ctx->vectors + (index * dim);
        memcpy(vec_out, src, dim * sizeof(float));
    }

    return 0;
}

int dataset_query(dataset_ctx_t *ctx, uint64_t query_index,
                  float *query_vec_out) {
    if (!ctx || query_index >= ctx->header->num_queries) {
        return -1;
    }

    const uint32_t dim = ctx->header->dim;
    // const uint32_t num_neighbors = ctx->header->num_neighbors;

    /* Copy query vector */
    const float *query_src = ctx->queries + (query_index * dim);
    memcpy(query_vec_out, query_src, dim * sizeof(float));

    /* Copy ground truth */
    // const int64_t *gt_src = ctx->ground_truth + (query_index * num_neighbors);
    // for (uint32_t i = 0; i < num_neighbors; i++) {
    //     neighbors_out[i] = (uint64_t)gt_src[i];
    // }

    return 0;
}

int dataset_get_info(dataset_ctx_t *ctx, dataset_info_t *info) {
    if (!ctx || !info) return -1;

    info->distance_metric = ctx->header->distance_metric;
    info->dtype = ctx->header->dtype;
    info->dim = ctx->header->dim;
    info->num_vectors = ctx->header->num_vectors;
    info->num_queries = ctx->header->num_queries;
    info->num_neighbors = ctx->header->num_neighbors;

    return 0;
}

int dataset_get_neighbors(dataset_ctx_t *ctx, uint64_t query_index, uint64_t *neighbors_out) {
    if (!ctx || !neighbors_out) return -1;
    if (query_index >= ctx->header->num_queries) return -1;

    /* Copy ground truth neighbors for this query */
    int64_t *gt_start = ctx->ground_truth + query_index * ctx->header->num_neighbors;
    for (uint32_t i = 0; i < ctx->header->num_neighbors; i++) {
        neighbors_out[i] = (uint64_t)gt_start[i];
    }

    return 0;
}

void dataset_destroy(dataset_ctx_t *ctx) {
    if (!ctx) return;

    if (ctx->mmap_base) munmap(ctx->mmap_base, ctx->mmap_size);
    if (ctx->fd >= 0) close(ctx->fd);
    free(ctx);
}