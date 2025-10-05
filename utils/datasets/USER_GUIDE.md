# Valkey Search Delete Test - User Guide

A comprehensive memory stress testing tool for Valkey-Search with vector operations. This guide covers everything you need to know to use the tool effectively.

## Table of Contents

- [Overview](#overview)
- [Quick Start](#quick-start)
- [Getting Started](#getting-started)
- [Dataset Management](#dataset-management)
- [Scenario Testing](#scenario-testing)
- [S3 Integration](#s3-integration)
- [Visualization and Analysis](#visualization-and-analysis)
- [Best Practices](#best-practices)
- [Troubleshooting](#troubleshooting)

## Overview

### Features

- **🔄 Multiple Workload Types**: Built-in support for ingestion, querying, and data shrinking workloads
- **📊 Memory Monitoring**: Real-time memory usage tracking with detailed metrics
- **🎯 Scenario-Based Testing**: Define complex test scenarios with multiple phases
- **📈 Performance Metrics**: Comprehensive performance and latency measurements
- **⚙️ Configurable**: Flexible configuration for different testing needs
- **📦 Dataset Management**: Built-in dataset download and management
- **🛡️ Validation**: Configuration and scenario validation before execution
- **🌐 Multi-Platform**: Support for Linux, macOS, Windows (WSL), and cloud platforms
- **☁️ S3 Integration**: AWS S3 support for dataset storage and management
- **📊 Visualization**: Generate charts and comparisons from test results

### Prerequisites

Before you begin, ensure you have:

- [ ] Python 3.10 or higher installed
- [ ] A running Valkey instance with Search module
- [ ] At least 4GB of available RAM
- [ ] Network access for dataset downloads (optional)

## Quick Start

### 1. Installation

> **Note**: This package is in development. Install from source for latest features.

```bash
# Quick system check
git clone https://github.com/zvi-code/search_delete_test.git
cd search_delete_test
python3 setup_check.py

# Install (requires Python 3.9+)
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
pip install -e .

# Verify installation
python verify_installation.py
vst --help
```

### 2. Setup Valkey

```bash
# Using Docker (recommended)
docker run -d --name valkey-stack \
  -p 6379:6379 \
  valkey/valkey-stack-server:latest

# Verify connection
vst info valkey
```

### 3. Run Your First Test

```bash
# Quick 5-minute stress test
vst run quick --duration 300

# With specific dataset
vst dataset quick-setup small
vst run quick --dataset sift-128d --duration 600

# Check results
ls ./output/
```

## Getting Started

### Step 1: Verify Installation

```bash
# Check the tool is working
vst version

# Check system information
vst info system

# Test Valkey connection
vst info valkey
```

Expected output for `vst info valkey`:
```
Valkey Server Information (localhost:6379)
==================================================

Server:
  valkey_version: 7.2.0
  valkey_mode: standalone
  os: Linux
...
```

### Step 2: Understanding the Output

#### Metrics File (`metrics.csv`)
Contains time-series data of memory usage:
- `timestamp`: When the measurement was taken
- `rss_mb`: Resident Set Size in MB
- `active_mb`: Active memory in MB
- `allocated_mb`: Allocated memory in MB
- `fragmentation_ratio`: Memory fragmentation ratio

#### Summary File (`summary.csv`)
Contains aggregated test results:
- Total vectors processed
- Average throughput
- Memory usage statistics
- Performance metrics

### Step 3: Create Your First Custom Scenario

Create a file called `my-first-scenario.yaml`:

```yaml
name: my_first_test
description: My first custom stress test

dataset: openai-5m

global_config:
  n_threads: 2
  n_clients: 100
  batch_size: 500

steps:
  # Load some data
  - name: load_data
    type: workload
    workload: ingest
    parameters:
      target_vectors: 100000
      
  # Take a measurement
  - name: checkpoint_after_load
    type: checkpoint
    parameters:
      collect_full_metrics: true
      
  # Run some queries
  - name: query_test
    type: workload
    workload: query
    duration_seconds: 60
    parameters:
      queries_per_second: 10
      k: 10
```

### Validate and Run the Scenario

```bash
# First, validate the scenario
vst validate scenario my-first-scenario.yaml

# Run a dry-run to check everything
vst run scenario my-first-scenario.yaml --dry-run

# Actually run the scenario
vst run scenario my-first-scenario.yaml --output ./my-test
```

## Dataset Management

### Quick Dataset Setup

```bash
# List all available datasets
vst dataset list

# Quick setup by scale
vst dataset quick-setup small    # Testing datasets
vst dataset quick-setup medium   # Production evaluation
vst dataset quick-setup large    # Large-scale testing

# Download specific datasets
vst dataset download openai-5m --output ./_datasets_downloads
vst dataset download sift-128d --force

# Dataset information
vst dataset info openai-5m
vst dataset validate ./_datasets_downloads/openai-5m.h5
```

### Available Dataset Types

#### Standard Datasets (ANN-Benchmarks)
- `sift-128d` - SIFT descriptors (1M vectors, 128d)
- `gist-960d` - GIST descriptors (1M vectors, 960d)
- `glove-25d` - GloVe word embeddings (1.2M vectors, 25d)
- `glove-50d` - GloVe word embeddings (1.2M vectors, 50d)
- `glove-100d` - GloVe word embeddings (1.2M vectors, 100d)
- `glove-200d` - GloVe word embeddings (1.2M vectors, 200d)

#### Large-Scale Datasets (Big-ANN)
- `fb-ssnpp` - Facebook SimSearchNet++ (1.17B vectors, 256d)
- `msft-turing-10m` - Microsoft Turing ANNS (10M vectors)
- `msft-turing-100m` - Microsoft Turing ANNS (100M vectors)
- `msft-spacev-1b` - Microsoft SPACEV (1B vectors)

#### Sparse Datasets (MS MARCO)
- `msmarco-100k` - MS MARCO sparse (100K documents)
- `msmarco-1m` - MS MARCO sparse (1M documents)
- `msmarco-full` - MS MARCO sparse (8.8M documents)

#### VectorDBBench Datasets
- `openai-small` - OpenAI embeddings (50K vectors)
- `openai-medium` - OpenAI embeddings (500K vectors)
- `openai-large` - OpenAI embeddings (5M vectors)
- `cohere-1m` - Cohere multilingual (1M vectors)

### Dataset Download Commands

```bash
# Download Big-ANN datasets
vst dataset download-bigann fb-ssnpp --parallel 8

# Download sparse datasets
vst dataset download-sparse 1M --output /data/datasets

# Download VectorDBBench datasets
vst dataset download-vdb openai-small --format hdf5

# Convert datasets to different formats
vst dataset create-vdb-format ./sift-128d.h5 ./vdb-output
```

## Scenario Testing

### Built-in Scenarios

Pre-built scenarios for various testing patterns:

```bash
# List available scenarios
vst run list-scenarios

# Run built-in scenarios (specify file path)
vst run scenario ./config/scenarios/continuous_growth.yaml
vst run scenario ./config/scenarios/memory_leak_detection.yaml
vst run scenario ./config/scenarios/production_simulation.yaml

# Validate before running
vst run validate ./config/scenarios/my-scenario.yaml
```

**Available scenarios:**
- `continuous_growth` - Continuous memory growth testing
- `grow_shrink_grow` - Oscillating memory patterns
- `high_velocity_ingestion` - High-speed data ingestion
- `memory_leak_detection` - Memory leak simulation and detection
- `index_rebuild_memory_pattern` - Index rebuild impact testing
- `production_simulation` - Real-world production workload patterns
- `cluster_memory_distribution` - Memory distribution across cluster nodes
- `memory_fragmentation_analysis` - Memory fragmentation pattern analysis
- `vector_expansion_memory_impact` - Vector expansion memory impact testing

### Custom Scenario Development

#### Basic Scenario Structure

```yaml
name: my_custom_scenario
description: Description of what this scenario tests

# Dataset to use
dataset: openai-5m

# Global configuration
global_config:
  n_threads: 4
  n_clients: 200
  batch_size: 1000

# Test steps
steps:
  - name: initial_load
    type: workload
    workload: ingest
    parameters:
      target_vectors: 500000
      
  - name: memory_checkpoint
    type: checkpoint
    parameters:
      collect_full_metrics: true
      
  - name: query_phase
    type: workload
    workload: query
    duration_seconds: 300
    parameters:
      queries_per_second: 50
      k: 10
```

#### Advanced Scenario Features

```yaml
# Multi-phase scenario with different workloads
steps:
  # Phase 1: Initial data load
  - name: phase1_load
    type: workload
    workload: ingest
    parameters:
      target_vectors: 1000000
      batch_size: 2000
      
  # Phase 2: Mixed workload
  - name: phase2_mixed
    type: workload
    workload: mixed
    duration_seconds: 600
    parameters:
      ingest_ratio: 0.3
      query_ratio: 0.7
      queries_per_second: 100
      
  # Phase 3: Memory pressure test
  - name: phase3_pressure
    type: workload
    workload: ingest
    parameters:
      target_vectors: 2000000
      aggressive_mode: true
      
  # Phase 4: Cleanup and shrink
  - name: phase4_cleanup
    type: workload
    workload: shrink
    parameters:
      target_reduction: 0.5
```

## S3 Integration

Advanced S3 integration for large-scale datasets:

### Basic S3 Commands

```bash
# List S3 datasets
vst prep list

# Download from S3
vst prep download my-dataset

# Prepare datasets for S3 with RDB generation
vst prep prepare ./dataset.h5 --compress zstd

# Get detailed dataset info
vst prep info my-dataset

# Validate S3 dataset
vst prep validate ./dataset
```

### S3 Configuration

```yaml
# config/s3-config.yaml
s3:
  bucket_name: "my-vss-datasets"
  region: "us-east-1"
  multipart_threshold: 67108864    # 64MB
  max_concurrency: 10
  download_threads: 4

datasets:
  default_compression: "zstd"
  batch_size: 1000
  memory_limit_gb: 32.0
```

### Dataset Preparation Workflow

```bash
# 1. Prepare local dataset for S3
vst prep prepare ./my-dataset.h5 \
  --compress zstd \
  --create-index \
  --index-algorithm HNSW

# 2. Upload to S3 (configured separately)
# This creates RDB files and metadata

# 3. Download prepared dataset
vst prep download my-dataset --output ./_datasets_prepared

# 4. Use in scenarios
vst run scenario my-scenario.yaml --dataset ./_datasets_prepared/my-dataset
```

## Visualization and Analysis

Generate insights from your test results:

### Basic Visualization

```bash
# Generate visualizations from test results
vst visualize generate ./output/scenario-results

# Compare multiple test runs
vst visualize compare ./test1 ./test2 ./test3

# Export data for external analysis
vst visualize export-data ./results ./analysis/data.csv

# List scenarios with available data
vst visualize list-scenarios
```

### Understanding Visualizations

The tool generates several types of visualizations:

1. **Memory Usage Over Time**: Shows RSS, active, and allocated memory
2. **Performance Metrics**: Throughput and latency trends
3. **Memory Fragmentation**: Fragmentation ratio analysis
4. **Comparison Charts**: Side-by-side scenario comparisons

### Custom Analysis

```bash
# Export raw data for custom analysis
vst visualize export-data ./scenario-results ./analysis/

# Generated files:
# - metrics.csv: Time-series metrics
# - summary.csv: Aggregated statistics
# - performance.csv: Performance data
# - memory.csv: Memory-specific metrics
```

## Best Practices

### Performance Tuning

1. **Adjust thread count** based on your CPU:
   ```bash
   # Check CPU count
   vst info system
   
   # Modify scenario to use appropriate thread count
   ```

2. **Scale client connections** based on your system:
   ```yaml
   global_config:
     n_threads: 4        # Number of CPU cores
     n_clients: 200      # Start with 50 per thread
     batch_size: 1000    # Increase for better throughput
   ```

### Memory Testing

1. **Start small** and increase load:
   ```yaml
   steps:
     - name: small_load
       type: workload
       workload: ingest
       parameters:
         target_vectors: 10000
         
     - name: medium_load
       type: workload
       workload: ingest
       parameters:
         target_vectors: 100000
         
     - name: large_load
       type: workload
       workload: ingest
       parameters:
         target_vectors: 1000000
   ```

2. **Add checkpoints** to measure memory at each stage:
   ```yaml
   - name: checkpoint_after_small
     type: checkpoint
     parameters:
       collect_full_metrics: true
   ```

### Configuration Management

Create a custom configuration file `my-config.yaml`:

```yaml
valkey:
  host: localhost
  port: 6379
  max_connections: 500  # Adjust based on your Valkey setup

workload:
  n_threads: 4
  n_clients_per_thread: 50
  batch_size: 500
  operation_timeout: 30.0

monitoring:
  sampling_interval: 5.0  # More frequent sampling
```

Use it with scenarios:
```bash
vst run scenario my-scenario.yaml --config my-config.yaml
```

## Troubleshooting

### Connection Issues

```bash
# Problem: "Connection refused"
# Solution: Check if Valkey is running
docker ps
valkey-cli ping

# Problem: "Search module not found"
# Solution: Use Valkey Stack
docker run -d -p 6379:6379 valkey/valkey-stack-server:latest
```

### Memory Issues

```bash
# Problem: Out of memory
# Solution: Reduce test parameters
# In scenario file, reduce:
# - target_vectors
# - n_clients
# - batch_size
```

### Performance Issues

```bash
# Problem: Tests are too slow
# Solution: Check system resources
vst info system

# And tune configuration:
# - Increase batch_size
# - Adjust n_threads to match CPU cores
# - Reduce sampling_interval
```

### Common Error Messages

#### "Dataset not found"
```bash
# Download the dataset first
vst dataset download openai-5m
# Or use quick setup
vst dataset quick-setup small
```

#### "Valkey connection timeout"
```bash
# Check Valkey is running and accessible
vst info valkey --host localhost --port 6379
# Increase timeout in config
```

#### "Insufficient memory"
```bash
# Reduce workload parameters
# Check available memory
vst info system
```

## Example Workflows

### Complete Testing Workflow

```bash
# 1. Setup
docker run -d -p 6379:6379 valkey/valkey-stack-server:latest
vst info valkey

# 2. Quick test
vst run quick --duration 60

# 3. Custom scenario
cat > test-scenario.yaml << EOF
name: learning_test
description: Learning the tool
dataset: openai-5m
global_config:
  n_threads: 2
  n_clients: 50
  batch_size: 200
steps:
  - name: load_small
    type: workload
    workload: ingest
    parameters:
      target_vectors: 50000
  - name: measure
    type: checkpoint
    parameters:
      collect_full_metrics: true
  - name: query_test
    type: workload
    workload: query
    duration_seconds: 30
    parameters:
      queries_per_second: 5
EOF

# 4. Run custom scenario
vst validate scenario test-scenario.yaml
vst run scenario test-scenario.yaml --output ./learning-test

# 5. Check results
ls -la ./learning-test/
head ./learning-test/metrics.csv
```

### Production Evaluation Workflow

```bash
# 1. Setup production-scale dataset
vst dataset quick-setup medium

# 2. Validate scenario
vst validate scenario production_simulation.yaml

# 3. Run with dry-run first
vst run scenario production_simulation.yaml --dry-run

# 4. Execute full scenario
vst run scenario production_simulation.yaml --output ./prod-test

# 5. Analyze results
vst visualize generate ./prod-test
```

### Billion-Scale Testing Workflow

```bash
# 1. Check system resources
vst info system --hardware --memory

# 2. Setup billion-scale dataset
vst dataset quick-setup billion --output /data/billion

# 3. Configure for large-scale
export VST_OUTPUT_DIR=/data/output
export VST_DATASET_DIR=/data/billion

# 4. Validate large scenario
vst validate scenario vector_expansion_memory_impact.yaml --check-datasets

# 5. Execute with monitoring
vst run scenario vector_expansion_memory_impact.yaml --variables "duration=3600"
```

You're now ready to start comprehensive stress testing of your Valkey instances! 🚀

For more detailed technical information, see the [Developer Guide](DEVELOPER_GUIDE.md) and [API Reference](API_REFERENCE.md).
