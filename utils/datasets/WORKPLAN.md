# Dataset Integration Implementation Plan

## High-Level Overview

**Goal**: Integrate HDF5-based dataset support into valkey-benchmark for deterministic vector workloads with ground truth validation.

**Architecture**:
- Binary dataset format (mmap-optimized)
- C API for O(1) vector/query access
- Python conversion tool (HDF5 → binary)
- Atomic counter-based thread distribution
- Read-only shared dataset context
- Recall tracking and reporting

**Components**:
1. Binary dataset format specification
2. C API library (`dataset_api.c/h`)
3. Python dataset converter (`prepare_binary.py`)
4. valkey-benchmark integration
5. Recall tracking system

---

## Phase 1: Binary Format & C API Foundation

**Duration**: 2-3 days  
**Goal**: Create standalone dataset library with test harness

### Task 1.1: Define Binary Format Header

**File**: `src/dataset_api.h`

```c
#ifndef DATASET_API_H
#define DATASET_API_H

#include <stdint.h>
#include <stddef.h>

#define DATASET_MAGIC 0xVECDB001
#define DATASET_VERSION 1

/* Distance metrics */
typedef enum {
    DISTANCE_L2 = 0,
    DISTANCE_COSINE = 1,
    DISTANCE_IP = 2
} distance_metric_t;

/* Data types */
typedef enum {
    DTYPE_FLOAT32 = 0,
    DTYPE_FLOAT16 = 1
} dtype_t;

/* Dataset metadata */
typedef struct {
    distance_metric_t distance_metric;
    dtype_t dtype;
    uint32_t dim;
    uint64_t num_vectors;
    uint64_t num_queries;
    uint32_t num_neighbors;
} dataset_info_t;

/* 4KB-aligned header for cache efficiency */
typedef struct __attribute__((packed)) {
    uint32_t magic;
    uint32_t version;
    char dataset_name[256];
    uint8_t distance_metric;
    uint8_t dtype;
    uint8_t padding[2];
    uint32_t dim;
    uint64_t num_vectors;
    uint64_t num_queries;
    uint32_t num_neighbors;
    uint8_t padding2[4];
    uint64_t vectors_offset;
    uint64_t queries_offset;
    uint64_t ground_truth_offset;
    uint8_t reserved[3808];  /* Pad to 4KB */
} dataset_header_t;

/* Opaque context handle */
typedef struct dataset_ctx dataset_ctx_t;

/* API Functions */
dataset_ctx_t* dataset_init(const char *dataset_name, dataset_info_t *info);
int datasetGetVector(dataset_ctx_t *ctx, uint64_t index, uint64_t *id_out, float *vec_out);
int datasetSetQueryVec(dataset_ctx_t *ctx, uint64_t query_index, float *query_vec_out, uint64_t *neighbors_out);
int dataset_get_info(dataset_ctx_t *ctx, dataset_info_t *info);
void dataset_destroy(dataset_ctx_t *ctx);

#endif /* DATASET_API_H */
```

**Testing**: Compile header, verify struct sizes:
```bash
gcc -c dataset_api.h -o /dev/null
python3 -c "print('Header size:', 4096)"  # Verify padding math
```

### Task 1.2: Implement Core C API

**File**: `src/dataset_api.c`

```c
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
        fprintf(stderr, "Invalid magic: 0x%x\n", header->magic);
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

int datasetGetVector(dataset_ctx_t *ctx, uint64_t index, 
                    uint64_t *id_out, float *vec_out) {
    if (!ctx || index >= ctx->header->num_vectors) {
        return -1;
    }
    
    /* ID is the index itself (can extend later) */
    *id_out = index;
    
    /* Copy vector - cache-friendly sequential access */
    const uint32_t dim = ctx->header->dim;
    const float *src = ctx->vectors + (index * dim);
    memcpy(vec_out, src, dim * sizeof(float));
    
    return 0;
}

int datasetSetQueryVec(dataset_ctx_t *ctx, uint64_t query_index,
                  float *query_vec_out, uint64_t *neighbors_out) {
    if (!ctx || query_index >= ctx->header->num_queries) {
        return -1;
    }
    
    const uint32_t dim = ctx->header->dim;
    const uint32_t num_neighbors = ctx->header->num_neighbors;
    
    /* Copy query vector */
    const float *query_src = ctx->queries + (query_index * dim);
    memcpy(query_vec_out, query_src, dim * sizeof(float));
    
    /* Copy ground truth */
    const int64_t *gt_src = ctx->ground_truth + (query_index * num_neighbors);
    for (uint32_t i = 0; i < num_neighbors; i++) {
        neighbors_out[i] = (uint64_t)gt_src[i];
    }
    
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

void dataset_destroy(dataset_ctx_t *ctx) {
    if (!ctx) return;
    
    if (ctx->mmap_base) munmap(ctx->mmap_base, ctx->mmap_size);
    if (ctx->fd >= 0) close(ctx->fd);
    free(ctx);
}
```

**Testing**: Create minimal test harness:
```c
/* test_dataset_api.c */
#include "dataset_api.h"
#include <assert.h>

int main(void) {
    dataset_info_t info;
    dataset_ctx_t *ctx = dataset_init("test_dataset", &info);
    assert(ctx != NULL);
    assert(info.dim > 0);
    
    /* Test prefill */
    uint64_t id;
    float *vec = malloc(info.dim * sizeof(float));
    assert(datasetGetVector(ctx, 0, &id, vec) == 0);
    assert(id == 0);
    
    /* Test query */
    uint64_t *neighbors = malloc(info.num_neighbors * sizeof(uint64_t));
    assert(datasetSetQueryVec(ctx, 0, vec, neighbors) == 0);
    
    free(vec);
    free(neighbors);
    dataset_destroy(ctx);
    printf("✓ All tests passed\n");
    return 0;
}
```

```bash
gcc -o test_dataset dataset_api.c test_dataset_api.c -lm
./test_dataset
```

---

## Phase 2: Python Dataset Converter

**Duration**: 1-2 days  
**Goal**: Convert HDF5 datasets to binary format

### Task 2.1: Basic Converter Implementation

**File**: `src/datasets/prepare_binary.py`

```python
#!/usr/bin/env python3
import numpy as np
import h5py
import struct
from pathlib import Path
import sys

DATASET_MAGIC = 0xVECDB001
DISTANCE_L2 = 0
DISTANCE_COSINE = 1

def align_to(size, alignment=64):
    return ((size + alignment - 1) // alignment) * alignment

def prepare_dataset_binary(h5_path, output_path, dataset_name, 
                          distance_metric=DISTANCE_L2):
    print(f"Loading dataset from {h5_path}...")
    
    with h5py.File(h5_path, 'r') as f:
        # Load vectors (train set)
        vectors = np.array(f['train'], dtype=np.float32)
        queries = np.array(f['test'], dtype=np.float32)
        
        # Load or compute ground truth
        if 'neighbors' in f:
            ground_truth = np.array(f['neighbors'], dtype=np.int64)
        else:
            print("Computing ground truth (this may take a while)...")
            ground_truth = compute_ground_truth(vectors, queries, k=100)
    
    num_vectors, dim = vectors.shape
    num_queries = len(queries)
    num_neighbors = ground_truth.shape[1]
    
    print(f"Dataset: {num_vectors} vectors, {num_queries} queries, dim={dim}")
    
    # Calculate offsets (64-byte aligned)
    header_size = 4096
    vectors_offset = header_size
    vectors_size = align_to(num_vectors * dim * 4)
    
    queries_offset = vectors_offset + vectors_size
    queries_size = align_to(num_queries * dim * 4)
    
    ground_truth_offset = queries_offset + queries_size
    ground_truth_size = num_queries * num_neighbors * 8
    
    total_size = ground_truth_offset + ground_truth_size
    print(f"Output size: {total_size / (1024**3):.2f} GB")
    
    # Write binary file
    with open(output_path, 'wb') as f:
        # Write header (4KB)
        header = struct.pack(
            '<I I 256s B B 2x I Q Q I 4x Q Q Q 3808x',
            DATASET_MAGIC,
            1,  # version
            dataset_name.encode('utf-8')[:256],
            distance_metric,
            0,  # dtype: FLOAT32
            dim,
            num_vectors,
            num_queries,
            num_neighbors,
            vectors_offset,
            queries_offset,
            ground_truth_offset
        )
        f.write(header)
        
        # Write vectors
        f.seek(vectors_offset)
        vectors.tofile(f)
        f.write(b'\x00' * (vectors_size - num_vectors * dim * 4))
        
        # Write queries
        f.seek(queries_offset)
        queries.tofile(f)
        f.write(b'\x00' * (queries_size - num_queries * dim * 4))
        
        # Write ground truth
        f.seek(ground_truth_offset)
        ground_truth.tofile(f)
    
    print(f"✓ Dataset written to {output_path}")
    verify_dataset(output_path)

def compute_ground_truth(vectors, queries, k=100):
    """Brute force k-NN ground truth computation"""
    from scipy.spatial.distance import cdist
    
    print(f"Computing distances for {len(queries)} queries...")
    distances = cdist(queries, vectors, metric='euclidean')
    
    print(f"Finding top-{k} neighbors...")
    neighbors = np.argsort(distances, axis=1)[:, :k]
    
    return neighbors.astype(np.int64)

def verify_dataset(path):
    with open(path, 'rb') as f:
        magic = struct.unpack('<I', f.read(4))[0]
        assert magic == DATASET_MAGIC, f"Invalid magic: 0x{magic:x}"
    print("✓ Verification passed")

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("Usage: prepare_binary.py <input.h5> <output.bin> [name]")
        sys.exit(1)
    
    h5_path = Path(sys.argv[1])
    output_path = Path(sys.argv[2])
    name = sys.argv[3] if len(sys.argv) > 3 else h5_path.stem
    
    prepare_dataset_binary(h5_path, output_path, name)
```

**Testing**:
```bash
# Create minimal test dataset
python3 << EOF
import h5py
import numpy as np

with h5py.File('test_mini.h5', 'w') as f:
    f['train'] = np.random.randn(1000, 128).astype(np.float32)
    f['test'] = np.random.randn(10, 128).astype(np.float32)
    f['neighbors'] = np.random.randint(0, 1000, (10, 100), dtype=np.int64)
EOF

# Convert
python3 prepare_binary.py test_mini.h5 test_mini.bin test_mini

# Verify with C API
./test_dataset test_mini
```

### Task 2.2: Integration with Dataset Preparation Tool

**File**: `src/datasets/prepare_binary.py` (add CLI wrapper)

```python
def add_to_vst_cli():
    """CLI integration point for vst dataset prepare-binary"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Convert HDF5 dataset to binary format'
    )
    parser.add_argument('input', help='Input HDF5 file')
    parser.add_argument('output', help='Output binary file')
    parser.add_argument('--name', help='Dataset name')
    parser.add_argument('--metric', choices=['L2', 'COSINE'], 
                       default='L2', help='Distance metric')
    
    args = parser.parse_args()
    
    metric_map = {'L2': DISTANCE_L2, 'COSINE': DISTANCE_COSINE}
    prepare_dataset_binary(
        args.input, args.output,
        args.name or Path(args.input).stem,
        metric_map[args.metric]
    )
```

**Testing**:
```bash
# From vst CLI (future integration)
vst dataset prepare-binary test_mini.h5 test_mini.bin --metric L2
```

---

## Phase 3: valkey-benchmark Integration - Configuration

**Duration**: 1 day  
**Goal**: Add dataset config options and initialization

### Task 3.1: Add Configuration Structure

**File**: `valkey-benchmark.c` (add to config struct)

```c
static struct config {
    // ... existing fields ...
    
    /* Dataset configuration */
    int use_dataset;              /* Enable dataset mode */
    sds dataset_name;             /* Dataset identifier or path */
    void *dataset_ctx;            /* Opaque dataset context */
    uint64_t dataset_num_vectors; /* Total vectors */
    uint64_t dataset_num_queries; /* Total queries */
    uint32_t dataset_num_neighbors; /* Ground truth k */
    _Atomic uint64_t dataset_prefill_counter;  /* Insert counter */
    _Atomic uint64_t dataset_query_counter;    /* Query counter */
} config;
```

### Task 3.2: Add Command-Line Options

**File**: `valkey-benchmark.c` (in parseOptions)

```c
} else if (!strcmp(argv[i], "--dataset")) {
    if (lastarg) goto invalid;
    config.dataset_name = sdsnew(argv[++i]);
    config.use_dataset = 1;
} else if (!strcmp(argv[i], "--dataset-path")) {
    if (lastarg) goto invalid;
    sdsfree(config.dataset_name);
    config.dataset_name = sdsnew(argv[++i]);
    config.use_dataset = 1;
```

Add to help text:
```c
" --dataset <name>   Use dataset for vector workloads (searches in standard locations)\n"
" --dataset-path <path> Explicit path to dataset binary file\n"
```

### Task 3.3: Dataset Initialization

**File**: `valkey-benchmark.c` (in main, after search index creation)

```c
/* Initialize dataset if enabled */
if (config.use_dataset) {
    /* Validate mutual exclusion */
    if (config.is_vector_generator) {
        fprintf(stderr, "ERROR: --dataset and --use_vgen cannot be used together\n");
        exit(1);
    }
    
    dataset_info_t info;
    config.dataset_ctx = dataset_init(config.dataset_name, &info);
    
    if (!config.dataset_ctx) {
        fprintf(stderr, "Failed to initialize dataset: %s\n", config.dataset_name);
        exit(1);
    }
    
    /* Store metadata */
    config.dataset_num_vectors = info.num_vectors;
    config.dataset_num_queries = info.num_queries;
    config.dataset_num_neighbors = info.num_neighbors;
    
    /* Override vector dimension from dataset */
    if (config.search.vector_dim != info.dim) {
        fprintf(stderr, "WARNING: Overriding --vector-dim %d with dataset dim %d\n",
                config.search.vector_dim, info.dim);
        config.search.vector_dim = info.dim;
    }
    
    /* Initialize counters */
    atomic_store(&config.dataset_prefill_counter, 0);
    atomic_store(&config.dataset_query_counter, 0);
    
    printf("✓ Dataset loaded: %lu vectors, %lu queries, %u dims, %u neighbors\n",
           info.num_vectors, info.num_queries, info.dim, info.num_neighbors);
}
```

**Testing**:
```bash
# Compile with dataset API
gcc -o valkey-benchmark valkey-benchmark.c dataset_api.c ... -lm

# Test initialization
./valkey-benchmark --dataset test_mini --help
./valkey-benchmark --dataset-path ./test_mini.bin -t ping -n 10
```

---

## Phase 4: Placeholder System Integration

**Duration**: 1 day  
**Goal**: Add dataset placeholders and command templates

### Task 4.1: Define Dataset Placeholders

**File**: `valkey-benchmark.c`

```c
#define DATASET_KEY_PLACEHOLDER "__d_key_ph__"      // 12 bytes
#define DATASET_VECTOR_PLACEHOLDER "__d_vec_ph__"   // 12 bytes
#define DATASET_KEY_PLACEHOLDER_INDEX 14
#define DATASET_VECTOR_PLACEHOLDER_INDEX 15
#define PLACEHOLDER_NUM_OF 16  // Updated

static const struct {
    const char *name;
    int len;
} PLACEHOLDERS[PLACEHOLDER_NUM_OF] = {
    // ... existing 14 placeholders ...
    {DATASET_KEY_PLACEHOLDER, 12},
    {DATASET_VECTOR_PLACEHOLDER, 12},
};

#define DATASET_KEY_ID_WIDTH 16  /* 16 decimal digits for vector ID */
```

### Task 4.2: Update Template Creation Functions

**File**: `valkey-benchmark.c`

```c
static sds getVectorKey(void) {
    sds key;
    
    /* Dataset mode - use placeholder */
    if (config.use_dataset) {
        if (config.cluster_mode) {
            key = sdscatprintf(sdsempty(), "%s{tag}:", config.search.prefix);
        } else {
            key = sdscatprintf(sdsempty(), "%s", config.search.prefix);
        }
        /* Append placeholder (will be replaced with vector ID) */
        key = sdscatlen(key, DATASET_KEY_PLACEHOLDER, 12);
        return key;
    }
    
    /* Existing vgen/random code */
    if (config.is_vector_generator) {
        // ... vgen code ...
    }
    
    // ... existing random code ...
}

static sds createVectorTemplate(uint64_t key_idx) {
    /* Dataset mode - placeholder for entire vector */
    if (config.use_dataset) {
        float *vector = zmalloc(config.search.vector_dim * sizeof(float));
        
        /* Mark first 12 bytes with placeholder for identification */
        memcpy(vector, DATASET_VECTOR_PLACEHOLDER, 12);
        /* Fill rest with recognizable pattern */
        memset((uint8_t*)vector + 12, 0xDD, 
               config.search.vector_dim * sizeof(float) - 12);
        
        sds vector_data = sdsnewlen(vector, 
                                    config.search.vector_dim * sizeof(float));
        zfree(vector);
        return vector_data;
    }
    
    /* Existing vgen/random code */
    // ...
}
```

**Testing**:
```bash
# Verify placeholders are found in command templates
./valkey-benchmark --dataset test_mini --search -t vec-insert -n 1 -c 1 \
  --debug-placeholders  # (add debug flag to print placeholder positions)
```

---

## Phase 5: Core Replacement Logic

**Duration**: 2 days  
**Goal**: Implement O(1) placeholder replacement with dataset

### Task 5.1: Dataset Replacement Function

**File**: `valkey-benchmark.c`

```c
/* Extract vector ID from fully formatted key */
static uint64_t extract_vector_id_from_key(const char *key, size_t keylen) {
    const char *id_start = key;
    
    /* Skip prefix (vec:) */
    const char *colon = strchr(key, ':');
    if (colon) {
        id_start = colon + 1;
        /* Skip cluster tag {tag}: if present */
        if (*id_start == '{') {
            colon = strchr(id_start, ':');
            if (colon) id_start = colon + 1;
        }
    }
    
    return strtoull(id_start, NULL, 10);
}

/* Main dataset replacement function */
static uint64_t replacePlaceholderDataset(
    int thread_id,
    const size_t key_count, const size_t *key_indices,
    _Atomic uint64_t *key_counter,
    const size_t vec_count, const size_t *vec_indices,
    _Atomic uint64_t *vector_counter,
    char *cmd)
{
    if (!config.use_dataset) return UINT64_MAX;
    
    /* Validate placeholder consistency */
    assert((key_count == vec_count) || (vec_count == 0) || (key_count == 0));
    
    /* INSERT/PREFILL: both key and vector replacement */
    if (key_count > 0 && vec_count > 0) {
        /* Atomic fetch - no thread conflicts */
        uint64_t dataset_idx = atomic_fetch_add(
            &config.dataset_prefill_counter, 1
        );
        
        /* Check bounds - no wrapping for insert */
        if (dataset_idx >= config.dataset_num_vectors) {
            return UINT64_MAX;  /* Exhausted dataset */
        }
        
        uint64_t vector_id;
        float *vector = zmalloc(config.search.vector_dim * sizeof(float));
        
        /* O(1) mmap read - thread-safe */
        if (datasetGetVector(config.dataset_ctx, dataset_idx, 
                           &vector_id, vector) != 0) {
            zfree(vector);
            return UINT64_MAX;
        }
        
        /* Replace key placeholder: vec:0000001234567890 */
        for (size_t i = 0; i < key_count; i++) {
            char *key_ph = cmd + key_indices[i];
            snprintf(key_ph, DATASET_KEY_ID_WIDTH + 1, "%016lu", vector_id);
        }
        
        /* Replace vector data (memcpy from mmap) */
        for (size_t i = 0; i < vec_count; i++) {
            char *vec_ph = cmd + vec_indices[i];
            memcpy(vec_ph, vector, config.search.vector_dim * sizeof(float));
        }
        
        zfree(vector);
        return UINT64_MAX;
    }
    
    /* QUERY: only vector, return query_idx for recall */
    if (vec_count > 0 && key_count == 0) {
        uint64_t query_idx = atomic_fetch_add(
            &config.dataset_query_counter, 1
        );
        query_idx %= config.dataset_num_queries;  /* Wrap for repeated queries */
        
        float *query_vector = zmalloc(config.search.vector_dim * sizeof(float));
        uint64_t *neighbors = zmalloc(
            config.dataset_num_neighbors * sizeof(uint64_t)
        );
        
        if (datasetSetQueryVec(config.dataset_ctx, query_idx, 
                         query_vector, neighbors) != 0) {
            zfree(query_vector);
            zfree(neighbors);
            return UINT64_MAX;
        }
        
        /* Replace vector placeholder */
        for (size_t i = 0; i < vec_count; i++) {
            char *vec_ph = cmd + vec_indices[i];
            memcpy(vec_ph, query_vector, 
                   config.search.vector_dim * sizeof(float));
        }
        
        zfree(query_vector);
        zfree(neighbors);
        return query_idx;  /* For recall tracking */
    }
    
    /* DELETE: only key replacement */
    if (key_count > 0 && vec_count == 0) {
        uint64_t dataset_idx = atomic_fetch_add(
            &config.dataset_prefill_counter, 1
        );
        
        if (dataset_idx >= config.dataset_num_vectors) {
            return UINT64_MAX;
        }
        
        uint64_t vector_id;
        float dummy;
        datasetGetVector(config.dataset_ctx, dataset_idx, &vector_id, &dummy);
        
        for (size_t i = 0; i < key_count; i++) {
            char *key_ph = cmd + key_indices[i];
            snprintf(key_ph, DATASET_KEY_ID_WIDTH + 1, "%016lu", vector_id);
        }
    }
    
    return UINT64_MAX;
}
```

### Task 5.2: Integrate into replacePlaceholders()

**File**: `valkey-benchmark.c`

```c
static void replacePlaceholders(client c, char *cmd_data, int cmd_count) {
    static _Atomic uint64_t seq_key[PLACEHOLDER_NUM_OF] = {0};
    
    for (int cmd_index = 0; cmd_index < cmd_count; cmd_index++) {
        char *cmd = cmd_data + cmd_index * placeholders.cmd_len;
        
        /* ... existing placeholder replacement ... */
        
        /* Handle dataset placeholders (AFTER vgen check) */
        if (config.use_dataset) {
            uint64_t query_idx = replacePlaceholderDataset(
                c->thread_id,
                placeholders.count[DATASET_KEY_PLACEHOLDER_INDEX],
                placeholders.indices[DATASET_KEY_PLACEHOLDER_INDEX],
                &seq_key[DATASET_KEY_PLACEHOLDER_INDEX],
                placeholders.count[DATASET_VECTOR_PLACEHOLDER_INDEX],
                placeholders.indices[DATASET_VECTOR_PLACEHOLDER_INDEX],
                &seq_key[DATASET_VECTOR_PLACEHOLDER_INDEX],
                cmd
            );
            
            /* Enqueue query index for recall tracking */
            if (query_idx != UINT64_MAX) {
                c->vgen_query_indices[(c->vgen_query_tail++) % 
                                      c->vgen_query_capacity] = query_idx;
            }
        }
    }
}
```

**Testing**:
```bash
# Test insert with dataset
./valkey-benchmark --dataset test_mini --search \
  --search-name test_idx --vector-dim 128 \
  -t vec-insert -n 100 -c 4 --threads 2

# Verify keys in Valkey
valkey-cli --scan --pattern "vec:*" | head -10
# Should show: vec:0000000000000000, vec:0000000000000001, etc.
```

---

## Phase 6: Recall Tracking System

**Duration**: 2 days  
**Goal**: Implement ground truth validation and reporting

### Task 6.1: Recall Statistics Structure

**File**: `valkey-benchmark.c`

```c
/* Recall statistics */
typedef struct {
    uint64_t total_queries;
    uint64_t total_matches;
    double sum_recall;
    double min_recall;
    double max_recall;
    uint64_t perfect_recalls;
    uint64_t zero_recalls;
    pthread_mutex_t mutex;
} recallStats;

static recallStats dataset_recall_stats;

static void initRecallStats(void) {
    memset(&dataset_recall_stats, 0, sizeof(recallStats));
    dataset_recall_stats.min_recall = 1.0;
    pthread_mutex_init(&dataset_recall_stats.mutex, NULL);
}

static void updateRecallStats(float recall) {
    pthread_mutex_lock(&dataset_recall_stats.mutex);
    
    dataset_recall_stats.total_queries++;
    dataset_recall_stats.sum_recall += recall;
    
    if (recall < dataset_recall_stats.min_recall) {
        dataset_recall_stats.min_recall = recall;
    }
    if (recall > dataset_recall_stats.max_recall) {
        dataset_recall_stats.max_recall = recall;
    }
    if (recall >= 0.9999) {
        dataset_recall_stats.perfect_recalls++;
    }
    if (recall < 0.0001) {
        dataset_recall_stats.zero_recalls++;
    }
    
    dataset_recall_stats.total_matches += (uint64_t)(recall * config.search.k);
    
    pthread_mutex_unlock(&dataset_recall_stats.mutex);
}
```

### Task 6.2: Recall Computation Logic

**File**: `valkey-benchmark.c`

```c
static void dataset_compute_recall(valkeyReply *reply, uint64_t query_idx) {
    if (!reply || reply->type != VALKEY_REPLY_ARRAY || reply->elements < 2) {
        return;
    }
    
    /* Get ground truth from dataset */
    uint64_t *gt_neighbors = zmalloc(
        config.dataset_num_neighbors * sizeof(uint64_t)
    );
    float dummy_query[1];
    datasetSetQueryVec(config.dataset_ctx, query_idx, dummy_query, gt_neighbors);
    
    /* Extract returned vector IDs from search results */
    int k = config.search.k;
    uint64_t *returned_ids = zmalloc(k * sizeof(uint64_t));
    int returned_count = 0;
    
    /* Parse FT.SEARCH results: [count, key1, fields1, key2, fields2, ...] */
    for (size_t i = 1; i < reply->elements && returned_count < k; i += 2) {
        valkeyReply *key_reply = reply->element[i];
        if (key_reply->type == VALKEY_REPLY_STRING) {
            returned_ids[returned_count++] = 
                extract_vector_id_from_key(key_reply->str, key_reply->len);
        }
    }
    
    /* Calculate recall@k */
    int matches = 0;
    for (int i = 0; i < returned_count; i++) {
        for (uint32_t j = 0; j < config.dataset_num_neighbors && j < (uint32_t)k; j++) {
            if (returned_ids[i] == gt_neighbors[j]) {
                matches++;
                break;
            }
        }
    }
    
    float recall = returned_count > 0 ? (float)matches / k : 0.0f;
    updateRecallStats(recall);
    
    zfree(gt_neighbors);
    zfree(returned_ids);
}
```

### Task 6.3: Integrate into readHandler

**File**: `valkey-benchmark.c` (in readHandler, after reply processing)

```c
/* Compute recall if using dataset */
if (config.use_dataset && c->vgen_query_head < c->vgen_query_tail) {
    uint64_t query_idx = c->vgen_query_indices[
        (c->vgen_query_head++) % c->vgen_query_capacity
    ];
    dataset_compute_recall(reply, query_idx);
}
```

### Task 6.4: Recall Reporting

**File**: `valkey-benchmark.c`

```c
static void showDatasetRecallReport(void) {
    if (!config.use_dataset || dataset_recall_stats.total_queries == 0) {
        return;
    }
    
    double avg_recall = dataset_recall_stats.sum_recall / 
                       dataset_recall_stats.total_queries;
    
    printf("\n");
    printf("====== Dataset Recall Statistics ======\n");
    printf("  Dataset: %s\n", config.dataset_name);
    printf("  Ground truth neighbors: %u\n", config.dataset_num_neighbors);
    printf("  Queries evaluated: %lu\n", dataset_recall_stats.total_queries);
    printf("\n");
    printf("  Recall@%d:\n", config.search.k);
    printf("    Average:  %.2f%%\n", avg_recall * 100.0);
    printf("    Min:      %.2f%%\n", dataset_recall_stats.min_recall * 100.0);
    printf("    Max:      %.2f%%\n", dataset_recall_stats.max_recall * 100.0);
    printf("\n");
    printf("  Query distribution:\n");
    printf("    Perfect recall (100%%): %lu (%.1f%%)\n",
           dataset_recall_stats.perfect_recalls,
           (float)dataset_recall_stats.perfect_recalls / 
           dataset_recall_stats.total_queries * 100.0);
    printf("    Zero recall (0%%):      %lu (%.1f%%)\n",
           dataset_recall_stats.zero_recalls,
           (float)dataset_recall_stats.zero_recalls / 
           dataset_recall_stats.total_queries * 100.0);
    printf("\n");
    printf("  Total matches: %lu / %lu (%.2f%%)\n",
           dataset_recall_stats.total_matches,
           dataset_recall_stats.total_queries * config.search.k,
           (float)dataset_recall_stats.total_matches /
           (dataset_recall_stats.total_queries * config.search.k) * 100.0);
}

/* Add to showLatencyReport() */
static void showLatencyReport(void) {
    // ... existing latency reporting ...
    
    /* Add dataset recall at the end */
    showDatasetRecallReport();
}
```

**Testing**:
```bash
# Test query with recall
./valkey-benchmark --dataset test_mini --search \
  --search-name test_idx --vector-dim 128 \
  -t vec-query -n 1000 -c 10 --threads 4

# Should output recall statistics
```

---

## Phase 7: Validation & Testing

**Duration**: 2-3 days  
**Goal**: End-to-end validation and edge case testing

### Task 7.1: Counter Reset in Benchmark Loop

**File**: `valkey-benchmark.c` (in benchmark/benchmarkSequence)

```c
static void benchmarkSequence(const char *title, char *cmd, int len, int seqlen) {
    // ... existing initialization ...
    
    /* Reset dataset counters for this benchmark */
    if (config.use_dataset) {
        atomic_store(&config.dataset_prefill_counter, 0);
        atomic_store(&config.dataset_query_counter, 0);
        initRecallStats();
        
        /* Warn if requests exceed dataset for insert */
        int is_insert = (strstr(title, "VEC-INSERT") != NULL ||
                        strstr(title, "VEC-GROUND-TRUTH") != NULL);
        
        if (is_insert && config.requests > config.dataset_num_vectors) {
            fprintf(stderr, 
                "WARNING: Requests (%d) exceeds dataset vectors (%lu)\n"
                "         Only %lu vectors will be inserted.\n",
                config.requests, config.dataset_num_vectors,
                config.dataset_num_vectors);
        }
    }
    
    // ... rest of benchmark ...
}
```

### Task 7.2: Comprehensive Test Suite

**File**: `tests/test_dataset_integration.sh`

```bash
#!/bin/bash
set -e

DATASET="test_mini"
VALKEY_HOST="127.0.0.1"
VALKEY_PORT="6379"

echo "=== Dataset Integration Test Suite ==="

# Test 1: Initialization
echo "Test 1: Dataset initialization..."
./valkey-benchmark --dataset ${DATASET} -t ping -n 1 -c 1 || exit 1
echo "✓ Passed"

# Test 2: Insert workload
echo "Test 2: Vector insert with dataset..."
./valkey-benchmark --dataset ${DATASET} --search \
  --search-name test_dataset_idx --vector-dim 128 \
  -t vec-insert -n 100 -c 4 --threads 2 \
  -h ${VALKEY_HOST} -p ${VALKEY_PORT} || exit 1

# Verify keys
KEY_COUNT=$(valkey-cli -h ${VALKEY_HOST} -p ${VALKEY_PORT} \
  --scan --pattern "vec:*" | wc -l)
if [ "$KEY_COUNT" -ne "100" ]; then
  echo "✗ Expected 100 keys, got $KEY_COUNT"
  exit 1
fi
echo "✓ Passed"

# Test 3: Query workload with recall
echo "Test 3: Vector query with recall..."
OUTPUT=$(./valkey-benchmark --dataset ${DATASET} --search \
  --search-name test_dataset_idx --vector-dim 128 \
  -t vec-query -n 100 -c 4 --threads 2 \
  -h ${VALKEY_HOST} -p ${VALKEY_PORT})

# Check for recall report
if ! echo "$OUTPUT" | grep -q "Recall Statistics"; then
  echo "✗ Missing recall statistics"
  exit 1
fi
echo "✓ Passed"

# Test 4: Multi-threading safety
echo "Test 4: High concurrency test..."
./valkey-benchmark --dataset ${DATASET} --search \
  --search-name test_dataset_idx --vector-dim 128 \
  -t vec-insert -n 500 -c 50 --threads 10 \
  -h ${VALKEY_HOST} -p ${VALKEY_PORT} || exit 1
echo "✓ Passed"

# Test 5: Query wrapping (more queries than dataset)
echo "Test 5: Query wrapping test..."
./valkey-benchmark --dataset ${DATASET} --search \
  --search-name test_dataset_idx --vector-dim 128 \
  -t vec-query -n 50 -c 1 --threads 1 \
  -h ${VALKEY_HOST} -p ${VALKEY_PORT} || exit 1
echo "✓ Passed (queries > dataset_num_queries)"

echo "=== All tests passed ==="
```

### Task 7.3: Performance Benchmarks

```bash
# Performance test script
#!/bin/bash

echo "=== Dataset Performance Test ==="

# Measure mmap overhead
time ./valkey-benchmark --dataset openai-5m -t ping -n 1 -c 1

# Measure insert throughput
./valkey-benchmark --dataset openai-5m --search \
  --search-name perf_test --vector-dim 1536 \
  -t vec-insert -n 100000 -c 50 --threads 10

# Measure query throughput with recall
./valkey-benchmark --dataset openai-5m --search \
  --search-name perf_test --vector-dim 1536 \
  -t vec-query -n 10000 -c 50 --threads 10 -P 10
```

---

## Phase 8: Documentation & Polish

**Duration**: 1 day  
**Goal**: Complete documentation and cleanup

### Task 8.1: User Documentation

**File**: `docs/DATASET_USAGE.md`

```markdown
# Dataset Integration Guide

## Overview
valkey-benchmark supports loading pre-computed vector datasets for deterministic
benchmarking with ground truth validation.

## Quick Start

### 1. Prepare Dataset
```bash
# Convert HDF5 to binary format
python3 src/datasets/prepare_binary.py \
  datasets/openai-5m.h5 \
  _datasets_prepared/openai-5m.bin \
  --metric L2

# Verify
ls -lh _datasets_prepared/openai-5m.bin
```

### 2. Run Insert Workload
```bash
valkey-benchmark --dataset openai-5m --search \
  --search-name my_index --vector-dim 1536 \
  -t vec-insert -n 1000000 -c 50 --threads 10
```

### 3. Run Query Workload with Recall
```bash
valkey-benchmark --dataset openai-5m --search \
  --search-name my_index --vector-dim 1536 \
  -t vec-query -n 10000 -c 50 --threads 10
```

## Dataset Format

Binary format optimized for mmap:
- Header: 4KB (metadata)
- Vectors: Contiguous float32 arrays (64-byte aligned)
- Queries: Test vectors for querying
- Ground Truth: int64 neighbor IDs per query

## CLI Options

- `--dataset <name>`: Load dataset by name (searches standard paths)
- `--dataset-path <path>`: Explicit path to binary file

## Performance Characteristics

- **Init**: O(1) after OS mmap (~1s for 100GB file)
- **Prefill**: ~100ns per vector (memcpy from mmap)
- **Query**: ~200ns per query (vector + ground truth)
- **Memory**: Zero runtime allocation in hot path
- **Thread Safety**: Fully lock-free with atomic counters

## Limitations

- Insert workload stops when dataset exhausted (no wrapping)
- Query workload wraps around dataset
- Maximum 2^64 vectors supported
- Float32 only (float16 support planned)
```

### Task 8.2: Code Cleanup Checklist

```
[ ] Remove debug printf statements
[ ] Verify all error paths free memory
[ ] Add assert() for invariants
[ ] Check all atomic operations use correct memory_order
[ ] Verify placeholder bounds checking
[ ] Add comments to complex algorithms
[ ] Run valgrind on test suite
[ ] Run clang-tidy
[ ] Update CHANGELOG
```

---

## Final Integration Checklist

### Build System
```bash
# Update Makefile
SRCS += dataset_api.c
LIBS += -lm

# Test clean build
make clean
make valkey-benchmark
```

### End-to-End Workflow
```bash
# 1. Prepare dataset
python3 src/datasets/prepare_binary.py \
  datasets/sift-128d.h5 sift-128d.bin

# 2. Insert vectors
valkey-benchmark --dataset sift-128d --search \
  --search-name sift_idx -t vec-insert -n 1000000

# 3. Query with recall
valkey-benchmark --dataset sift-128d --search \
  --search-name sift_idx -t vec-query -n 10000

# 4. Verify recall > 90%
```

### Performance Validation
- [ ] Insert: >100k ops/sec with 50 clients
- [ ] Query: >50k ops/sec with 50 clients
- [ ] Recall: >90% for HNSW with proper parameters
- [ ] Memory: <100MB overhead for 100GB dataset

This phased approach enables incremental testing at each step while building toward full dataset integration with ground truth validation.


┌─────────────────────────────────────────────────────────────┐
│                    valkey-benchmark (C)                     │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐     │
│  │ Prefill      │  │ Query        │  │ Validation   │     │
│  │ Threads      │  │ Threads      │  │ Thread       │     │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘     │
│         │                  │                  │             │
│         └──────────────────┴──────────────────┘             │
│                            │                                │
└────────────────────────────┼────────────────────────────────┘
                             │ Thread-safe O(1) access
┌────────────────────────────┼────────────────────────────────┐
│                    Dataset C API                            │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ dataset_init()      - Initialize dataset             │  │
│  │ dataset_get_info()  - Get metadata                   │  │
│  │ datasetGetVector()   - Get vector by index (O(1))     │  │
│  │ datasetSetQueryVec()     - Get query + ground truth (O(1))│  │
│  │ dataset_destroy()   - Cleanup                        │  │
│  └──────────────────────────────────────────────────────┘  │
│  ┌──────────────────────────────────────────────────────┐  │
│  │         Memory-Mapped Data Structures                │  │
│  │  - Vector array (mmap'd, read-only)                  │  │
│  │  - Query array (mmap'd, read-only)                   │  │
│  │  - Ground truth array (mmap'd, read-only)            │  │
│  │  - Atomic counters for thread coordination           │  │
│  └──────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
                             │
┌────────────────────────────┼────────────────────────────────┐
│              Python Dataset Preparation Tool                │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ HDF5 → Binary Format Converter                       │  │
│  │  - Pack vectors into contiguous float32 arrays       │  │
│  │  - Pre-compute ground truth in efficient layout      │  │
│  │  - Generate metadata header                          │  │
│  │  - Align for mmap performance                        │  │
│  └──────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘