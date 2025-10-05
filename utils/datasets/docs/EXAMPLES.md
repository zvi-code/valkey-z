# Example Usage Patterns

This document provides practical examples of common testing scenarios and usage patterns.

## Table of Contents

- [Quick Testing Examples](#quick-testing-examples)
- [Memory Analysis Examples](#memory-analysis-examples)
- [Performance Testing Examples](#performance-testing-examples)
- [Production Validation Examples](#production-validation-examples)
- [Custom Workload Examples](#custom-workload-examples)

## Quick Testing Examples

### 1. Basic Functionality Test

Quick verification that everything is working:

```bash
# 1-minute smoke test
vst run quick --duration 60

# Check if basic operations work
vst info valkey
vst info system
```

### 2. Dataset Validation Test

Test with different datasets:

```bash
# Test with small dataset
vst run quick --dataset sift-128d --duration 120

# Test with large dataset
vst run quick --dataset openai-5m --duration 300
```

### 3. Configuration Validation

```bash
# Test with custom output directory
vst run quick --duration 180 --output ./validation-test

# Verify results
ls -la ./validation-test/
head ./validation-test/metrics.csv
```

## Memory Analysis Examples

### 1. Memory Growth Analysis

Create `memory-growth.yaml`:

```yaml
name: memory_growth_analysis
description: Analyze memory usage patterns during data ingestion

dataset: openai-5m

global_config:
  n_threads: 4
  n_clients: 200
  batch_size: 1000

steps:
  # Baseline measurement
  - name: baseline
    type: checkpoint
    parameters:
      collect_full_metrics: true
      
  # Progressive loading with measurements
  - name: load_100k
    type: workload
    workload: ingest
    parameters:
      target_vectors: 100000
      
  - name: checkpoint_100k
    type: checkpoint
    parameters:
      collect_full_metrics: true
      
  - name: load_500k
    type: workload
    workload: ingest
    parameters:
      target_vectors: 500000
      
  - name: checkpoint_500k
    type: checkpoint
    parameters:
      collect_full_metrics: true
      
  - name: load_1m
    type: workload
    workload: ingest
    parameters:
      target_vectors: 1000000
      
  - name: final_checkpoint
    type: checkpoint
    parameters:
      collect_full_metrics: true
```

Run the analysis:

```bash
vst run scenario memory-growth.yaml --output ./memory-analysis
```

### 2. Memory Fragmentation Test

Create `fragmentation-test.yaml`:

```yaml
name: fragmentation_test
description: Test memory fragmentation patterns

dataset: openai-5m

global_config:
  n_threads: 2
  n_clients: 100
  batch_size: 500

steps:
  # Load data
  - name: initial_load
    type: workload
    workload: ingest
    parameters:
      target_vectors: 500000
      
  - name: checkpoint_after_load
    type: checkpoint
    parameters:
      collect_full_metrics: true
      
  # Delete some data to create fragmentation
  - name: shrink_data
    type: workload
    workload: shrink
    parameters:
      target_vectors: 250000
      
  - name: checkpoint_after_shrink
    type: checkpoint
    parameters:
      collect_full_metrics: true
      
  # Add data back
  - name: grow_again
    type: workload
    workload: ingest
    parameters:
      target_vectors: 500000
      
  - name: final_checkpoint
    type: checkpoint
    parameters:
      collect_full_metrics: true
```

### 3. Memory Leak Detection

Create `leak-detection.yaml`:

```yaml
name: leak_detection
description: Long-running test to detect memory leaks

dataset: sift-128d

global_config:
  n_threads: 1
  n_clients: 10
  batch_size: 100

steps:
  # Baseline
  - name: baseline
    type: checkpoint
    parameters:
      collect_full_metrics: true
      
  # Repeated load/query/delete cycles
  - name: cycle_1
    type: workload
    workload: ingest
    parameters:
      target_vectors: 100000
      
  - name: query_cycle_1
    type: workload
    workload: query
    duration_seconds: 300
    parameters:
      queries_per_second: 10
      
  - name: delete_cycle_1
    type: workload
    workload: shrink
    parameters:
      target_vectors: 0
      
  - name: checkpoint_cycle_1
    type: checkpoint
    parameters:
      collect_full_metrics: true
      
  # Repeat for multiple cycles...
  # (Add more cycles as needed)
```

## Performance Testing Examples

### 1. Throughput Benchmark

Create `throughput-test.yaml`:

```yaml
name: throughput_benchmark
description: Measure maximum ingestion and query throughput

dataset: openai-5m

global_config:
  n_threads: 8
  n_clients: 1000
  batch_size: 2000

steps:
  # Maximum ingestion throughput
  - name: max_ingestion
    type: workload
    workload: ingest
    parameters:
      target_vectors: 1000000
      
  - name: checkpoint_after_ingestion
    type: checkpoint
    parameters:
      collect_full_metrics: true
      
  # Maximum query throughput
  - name: max_queries
    type: workload
    workload: query
    duration_seconds: 600
    parameters:
      queries_per_second: 1000  # High load
      k: 10
```

### 2. Latency Analysis

Create `latency-test.yaml`:

```yaml
name: latency_analysis
description: Analyze query latency under different loads

dataset: sift-128d

global_config:
  n_threads: 4
  n_clients: 100
  batch_size: 500

steps:
  # Load test data
  - name: load_data
    type: workload
    workload: ingest
    parameters:
      target_vectors: 500000
      
  # Low load queries
  - name: low_load_queries
    type: workload
    workload: query
    duration_seconds: 180
    parameters:
      queries_per_second: 10
      k: 10
      
  - name: checkpoint_low_load
    type: checkpoint
    parameters:
      collect_full_metrics: true
      
  # Medium load queries
  - name: medium_load_queries
    type: workload
    workload: query
    duration_seconds: 180
    parameters:
      queries_per_second: 50
      k: 10
      
  - name: checkpoint_medium_load
    type: checkpoint
    parameters:
      collect_full_metrics: true
      
  # High load queries
  - name: high_load_queries
    type: workload
    workload: query
    duration_seconds: 180
    parameters:
      queries_per_second: 200
      k: 10
      
  - name: checkpoint_high_load
    type: checkpoint
    parameters:
      collect_full_metrics: true
```

### 3. Scaling Test

Create `scaling-test.yaml`:

```yaml
name: scaling_test
description: Test how performance scales with data size

dataset: openai-5m

global_config:
  n_threads: 4
  n_clients: 200
  batch_size: 1000

steps:
  # Test with 100K vectors
  - name: load_100k
    type: workload
    workload: ingest
    parameters:
      target_vectors: 100000
      
  - name: query_100k
    type: workload
    workload: query
    duration_seconds: 60
    parameters:
      queries_per_second: 50
      k: 10
      
  - name: checkpoint_100k
    type: checkpoint
    parameters:
      collect_full_metrics: true
      
  # Test with 500K vectors
  - name: load_500k
    type: workload
    workload: ingest
    parameters:
      target_vectors: 500000
      
  - name: query_500k
    type: workload
    workload: query
    duration_seconds: 60
    parameters:
      queries_per_second: 50
      k: 10
      
  - name: checkpoint_500k
    type: checkpoint
    parameters:
      collect_full_metrics: true
      
  # Test with 1M vectors
  - name: load_1m
    type: workload
    workload: ingest
    parameters:
      target_vectors: 1000000
      
  - name: query_1m
    type: workload
    workload: query
    duration_seconds: 60
    parameters:
      queries_per_second: 50
      k: 10
      
  - name: checkpoint_1m
    type: checkpoint
    parameters:
      collect_full_metrics: true
```

## Production Validation Examples

### 1. Production Load Simulation

Create `production-simulation.yaml`:

```yaml
name: production_simulation
description: Simulate production workload patterns

dataset: openai-5m

global_config:
  n_threads: 8
  n_clients: 500
  batch_size: 1000

steps:
  # Initial data load (simulate existing data)
  - name: existing_data
    type: workload
    workload: ingest
    parameters:
      target_vectors: 2000000
      
  # Simulate daily ingestion
  - name: daily_ingestion
    type: workload
    workload: ingest
    parameters:
      target_vectors: 2100000  # 100K new vectors
      
  # Simulate user queries
  - name: user_queries
    type: workload
    workload: query
    duration_seconds: 3600  # 1 hour
    parameters:
      queries_per_second: 20  # Realistic load
      k: 10
      
  - name: checkpoint_after_queries
    type: checkpoint
    parameters:
      collect_full_metrics: true
      
  # Simulate maintenance (data cleanup)
  - name: maintenance
    type: workload
    workload: shrink
    parameters:
      target_vectors: 2000000  # Remove old data
      
  - name: final_checkpoint
    type: checkpoint
    parameters:
      collect_full_metrics: true
```

### 2. Peak Load Test

Create `peak-load-test.yaml`:

```yaml
name: peak_load_test
description: Test system behavior under peak load conditions

dataset: openai-5m

global_config:
  n_threads: 16
  n_clients: 2000
  batch_size: 1000

steps:
  # Prepare data
  - name: prepare_data
    type: workload
    workload: ingest
    parameters:
      target_vectors: 1000000
      
  # Gradual ramp-up
  - name: ramp_up_queries
    type: workload
    workload: query
    duration_seconds: 300
    parameters:
      queries_per_second: 100
      k: 10
      
  # Peak load
  - name: peak_load
    type: workload
    workload: query
    duration_seconds: 600
    parameters:
      queries_per_second: 500  # Peak load
      k: 10
      
  - name: checkpoint_peak
    type: checkpoint
    parameters:
      collect_full_metrics: true
      
  # Sustained high load
  - name: sustained_load
    type: workload
    workload: query
    duration_seconds: 1800  # 30 minutes
    parameters:
      queries_per_second: 200
      k: 10
      
  - name: final_checkpoint
    type: checkpoint
    parameters:
      collect_full_metrics: true
```

## Custom Workload Examples

### 1. Mixed Workload Pattern

Create a configuration that combines multiple operations:

```yaml
name: mixed_workload
description: Realistic mixed read/write workload

dataset: openai-5m

global_config:
  n_threads: 6
  n_clients: 300
  batch_size: 500

steps:
  # Base data
  - name: base_load
    type: workload
    workload: ingest
    parameters:
      target_vectors: 500000
      
  # Simulate real application: 80% reads, 20% writes
  - name: mixed_phase_1
    type: workload
    workload: query  # Primarily queries
    duration_seconds: 240  # 4 minutes
    parameters:
      queries_per_second: 100
      k: 10
      
  - name: write_phase_1
    type: workload
    workload: ingest  # Some writes
    parameters:
      target_vectors: 550000  # Add 50K vectors
      
  - name: checkpoint_1
    type: checkpoint
    parameters:
      collect_full_metrics: true
      
  # Continue pattern
  - name: mixed_phase_2
    type: workload
    workload: query
    duration_seconds: 240
    parameters:
      queries_per_second: 120
      k: 10
      
  - name: write_phase_2
    type: workload
    workload: ingest
    parameters:
      target_vectors: 600000
      
  - name: final_checkpoint
    type: checkpoint
    parameters:
      collect_full_metrics: true
```

## Running the Examples

### Basic Execution

```bash
# Validate first
vst validate scenario memory-growth.yaml

# Run with output directory
vst run scenario memory-growth.yaml --output ./memory-test

# Check results
ls -la ./memory-test/
```

### With Custom Configuration

Create `high-performance.yaml`:

```yaml
valkey:
  max_connections: 2000
  
workload:
  n_threads: 16
  n_clients_per_thread: 125
  batch_size: 2000
  
monitoring:
  sampling_interval: 5.0
```

Run with custom config:

```bash
vst run scenario throughput-test.yaml \
  --config high-performance.yaml \
  --output ./performance-test
```

### Batch Testing

Run multiple scenarios:

```bash
#!/bin/bash
# batch-test.sh

scenarios=(
  "memory-growth.yaml"
  "throughput-test.yaml"
  "latency-test.yaml"
  "scaling-test.yaml"
)

for scenario in "${scenarios[@]}"; do
  echo "Running $scenario..."
  vst run scenario "$scenario" --output "./results/$(basename $scenario .yaml)"
  sleep 30  # Cool-down between tests
done
```

## Analyzing Results

### Basic Analysis

```bash
# Check metrics summary
head -20 ./results/memory-test/summary.csv

# Plot memory usage over time (requires additional tools)
python -c "
import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv('./results/memory-test/metrics.csv')
df['timestamp'] = pd.to_datetime(df['timestamp'])

plt.figure(figsize=(12, 6))
plt.subplot(2, 1, 1)
plt.plot(df['timestamp'], df['rss_mb'], label='RSS MB')
plt.plot(df['timestamp'], df['allocated_mb'], label='Allocated MB')
plt.legend()
plt.title('Memory Usage Over Time')

plt.subplot(2, 1, 2)
plt.plot(df['timestamp'], df['fragmentation_ratio'], label='Fragmentation Ratio')
plt.legend()
plt.title('Memory Fragmentation')

plt.tight_layout()
plt.savefig('./results/memory-test/analysis.png')
plt.show()
"
```

### Compare Results

```bash
# Compare different test runs
python -c "
import pandas as pd

# Load multiple test results
results = {}
for test in ['memory-test', 'performance-test', 'scaling-test']:
    try:
        df = pd.read_csv(f'./results/{test}/summary.csv')
        results[test] = df
        print(f'{test}: Peak memory = {df[\"peak_memory_mb\"].max():.1f} MB')
    except FileNotFoundError:
        print(f'Results not found for {test}')
"
```

## Best Practices

### 1. Start Small

Always begin with small-scale tests:
- Use smaller datasets initially
- Reduce client counts and batch sizes
- Short duration tests first

### 2. Progressive Testing

Build up complexity gradually:
1. Basic functionality test
2. Single workload test
3. Multi-phase scenarios
4. Peak load testing

### 3. Monitor Resources

```bash
# Check system resources before testing
vst info system

# Monitor during testing (in another terminal)
watch 'free -h && docker stats --no-stream'
```

### 4. Validate Everything

```bash
# Always validate scenarios
vst validate scenario my-scenario.yaml

# Use dry runs for complex scenarios
vst run scenario my-scenario.yaml --dry-run
```

### 5. Clean Up

```bash
# Clean up test data between runs
valkey-cli FLUSHALL

# Organize results
mkdir -p ./test-results/$(date +%Y%m%d)
mv ./output/* ./test-results/$(date +%Y%m%d)/
```

These examples provide a solid foundation for comprehensive stress testing of your Valkey instances. Adapt the scenarios based on your specific use cases and requirements.
