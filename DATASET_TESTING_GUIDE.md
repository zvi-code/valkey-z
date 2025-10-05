# Dataset Integration Testing Guide

This guide provides step-by-step instructions for downloading, preparing, and testing the valkey-benchmark dataset integration with recall tracking.

## Overview

The dataset integration allows valkey-benchmark to use real-world vector datasets (like GloVe) for deterministic testing with ground truth validation. This enables accurate recall measurement for vector search performance.

## Prerequisites

- Valkey cluster running (Redis/Valkey with vector search support)
- Python 3 with required packages
- Sufficient disk space (~1GB for GloVe dataset)

## Step 1: Download the GloVe Dataset

Download the GloVe-25-angular dataset from ann-benchmarks:

```bash
# Download the HDF5 dataset (25-dimensional GloVe vectors)
wget http://ann-benchmarks.com/glove-25-angular.hdf5 -O glove-25.hdf5
```

This dataset contains:
- 1,183,514 training vectors (25 dimensions)
- 10,000 test queries (25 dimensions)
- Precomputed ground truth neighbors

## Step 2: Install Python Dependencies

```bash
# Install required Python packages
pip3 install h5py numpy
```

## Step 3: Convert Dataset to Binary Format

Create the binary dataset format that valkey-benchmark can read:

```bash
# Run the conversion script
python3 create_large_dataset.py
```

This creates `large_dataset.bin` with:
- Binary header with dataset metadata
- All 1.18M vectors in float32 format
- 10K queries with precomputed ground truth
- File size: ~122MB

## Step 4: Build valkey-benchmark

Make sure you have the latest valkey-benchmark with dataset support:

```bash
cd /home/ubuntu/valkey/build-debug
make -j$(nproc) valkey-benchmark
```

## Step 5: Set Up Your Cluster Connection

Export your cluster hostname:

```bash
# Replace with your actual cluster endpoint
export HOST="your-cluster-endpoint.com"

# Test connection
./bin/valkey-cli -h $HOST -c --no-auth-warning PING
```

## Step 6: Insert the Full Dataset (Ground Truth Phase)

Clear any existing data and insert all 1.18M vectors:

```bash
# Clear the cluster
./bin/valkey-cli -h $HOST -c --no-auth-warning FLUSHALL

# Insert entire dataset (takes ~2-3 minutes)
./bin/valkey-benchmark -h $HOST --cluster --rfr no \
  --dataset large_dataset.bin \
  -t vec-ground-truth --search --vector-dim 25 \
  --search-name large_scale_25 --search-prefix zvec_large_: \
  -n 1183514 -c 10 --clean
```

Expected output:
- RPS: ~11-15K insertions per second
- Progress updates every few seconds
- Final confirmation of 1.18M vectors inserted

## Step 7: Verify Dataset Insertion

Check that all vectors were indexed:

```bash
# Check index status
./bin/valkey-cli -h $HOST -c --no-auth-warning FT.INFO large_scale_25
```

Look for:
- `num_docs`: Should show 1,183,514
- `num_records`: Should show 1,183,514
- `attributes.size`: Should show 1.18M

## Step 8: Run Query Benchmark with Recall Tracking

Execute 10K queries with recall measurement:

```bash
# Run 10K queries with recall tracking
./bin/valkey-benchmark -h $HOST --cluster --rfr no \
  --dataset large_dataset.bin \
  -t vec-query --search --vector-dim 25 \
  --search-name large_scale_25 --search-prefix zvec_large_: \
  -n 10000 -c 10 --threads 10
```

## Step 9: Analyze Results

The benchmark output will show:

### Performance Metrics
```
VEC-QUERY: rps=700.0 (overall: 700.0) avg_msec=14.3 (overall: 14.3)
```

### Recall Statistics
```
====== Recall Statistics ======
  Total queries: 10000
  Average recall: 85.50%
  Min recall: 70.00%
  Max recall: 100.00%
  Recall distribution:
    [90-100%]: 3245 queries (32.45%)
    [80-90%]:  4123 queries (41.23%)
    [70-80%]:  2632 queries (26.32%)
```

### Index Information
- Search latency: ~45μs p50
- Memory usage: ~2.5GB
- HNSW edges: ~18.9M

## Understanding the Results

### Good Recall Performance
- **85%+ average recall**: Excellent for large-scale HNSW
- **70%+ minimum recall**: Strong consistency
- **High throughput**: 700+ QPS with good latency

### What the Numbers Mean
- **Recall**: Percentage of true nearest neighbors found in top-10 results
- **RPS**: Queries per second throughput
- **avg_msec**: Average query latency in milliseconds

## Troubleshooting

### Common Issues

1. **Connection refused**
   ```bash
   # Check if HOST is set correctly
   echo $HOST

   # Test basic connectivity
   ./bin/valkey-cli -h $HOST -c --no-auth-warning PING
   ```

2. **Index limit reached**
   ```bash
   # List existing indexes
   ./bin/valkey-cli -h $HOST -c --no-auth-warning FT._LIST

   # Drop old indexes if needed
   ./bin/valkey-cli -h $HOST -c --no-auth-warning FT.DROPINDEX old_index_name
   ```

3. **Low recall performance**
   - Ensure the full dataset was inserted (check `num_docs`)
   - Verify all 1.18M vectors are present before running queries
   - Try increasing `ef_search` parameter for higher recall

4. **Memory issues**
   - Monitor cluster memory usage
   - The full dataset requires ~2.5GB of search memory

## Testing with Different Datasets

To test with other datasets:

1. Convert your HDF5 dataset using the same format
2. Update the dimension parameter (`--vector-dim`)
3. Adjust the number of vectors (`-n`) in ground truth insertion
4. Use appropriate index and prefix names

## Performance Tuning

### For Higher Recall
```bash
# Use higher ef_search (default is 200)
# This requires modifying the search command template in valkey-benchmark
```

### For Higher Throughput
```bash
# Increase concurrency
-c 20 --threads 20

# Use read replicas for queries (if available)
--rfr yes
```

### For Lower Latency
```bash
# Reduce batch size, increase threads
-c 5 --threads 15
```

## Expected Performance Benchmarks

| Scale | Vectors | Recall | QPS | Latency |
|-------|---------|--------|-----|---------|
| Small | 10K | 95%+ | 2000+ | <5ms |
| Medium | 100K | 90%+ | 1500+ | <8ms |
| Large | 1M+ | 85%+ | 700+ | <15ms |

## Next Steps

- Test with different vector dimensions
- Experiment with FLAT vs HNSW indexes
- Compare performance across different dataset types
- Integrate into CI/CD pipeline for regression testing

This testing framework validates that vector search maintains quality at scale and provides reproducible benchmarks for performance optimization.