# API Reference

Complete reference for all Valkey Search Delete Stress Test CLI commands and configuration options.

## Table of Contents

- [Command Structure](#command-structure)
- [Global Options](#global-options)
- [Commands Overview](#commands-overview)
- [Run Commands](#run-commands)
- [Dataset Commands](#dataset-commands)
- [Info Commands](#info-commands)
- [Prep Commands](#prep-commands)
- [Validate Commands](#validate-commands)
- [Visualize Commands](#visualize-commands)
- [Version Command](#version-command)
- [Configuration Reference](#configuration-reference)
- [Environment Variables](#environment-variables)
- [Exit Codes](#exit-codes)

## Command Structure

```
vst [GLOBAL_OPTIONS] COMMAND [COMMAND_OPTIONS] [ARGUMENTS]
```

## Global Options

| Option | Short | Description |
|--------|-------|-------------|
| `--verbose` | `-v` | Enable verbose output |
| `--quiet` | `-q` | Suppress non-error output |
| `--help` | | Show help message |

## Commands Overview

| Command | Description |
|---------|-------------|
| [`run`](#run-commands) | Run stress test scenarios |
| [`dataset`](#dataset-commands) | Dataset management |
| [`info`](#info-commands) | Display system information |
| [`prep`](#prep-commands) | Dataset preparation and S3 management |
| [`validate`](#validate-commands) | Validate configurations |
| [`visualize`](#visualize-commands) | Generate visualizations from scenario data |
| [`version`](#version-command) | Display version information |

---

## Run Commands

Execute stress tests and scenarios.

### `vst run quick`

Run a quick stress test with default settings.

```bash
vst run quick [OPTIONS]
```

| Option | Short | Type | Default | Description |
|--------|-------|------|---------|-------------|
| `--dataset` | `-d` | TEXT | openai-5m | Dataset to use |
| `--workload` | `-w` | TEXT | mixed | Workload type |
| `--duration` | `-t` | INTEGER | 300 | Duration in seconds |
| `--output` | `-o` | PATH | None | Output directory |

**Examples:**
```bash
# 10-minute test with default settings
vst run quick --duration 600

# Test with specific dataset
vst run quick --dataset sift-128d --duration 300

# Custom output location
vst run quick --output ./test-results

# Custom workload type
vst run quick --workload ingest --duration 120
```

### `vst run scenario`

Run a stress test scenario from configuration file.

```bash
vst run scenario [OPTIONS] SCENARIO_FILE
```

**Examples:**
```bash
# Run built-in scenario
vst run scenario continuous_growth

# Run custom scenario file
vst run scenario ./my-scenario.yaml

# Run with custom output directory
vst run scenario memory_test.yaml --output ./scenario-results
```

### `vst run list-scenarios`

List available built-in scenarios.

```bash
vst run list-scenarios
```

### `vst run validate`

Validate a scenario file without executing it.

```bash
vst run validate SCENARIO_FILE
```

**Example:**
```bash
vst run validate ./custom-scenario.yaml
```

---

## Dataset Commands

Comprehensive dataset management.

### `vst dataset list`

List all available datasets with details.

```bash
vst dataset list [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `--format` | Filter by format (hdf5, bigann, sparse, etc.) |
| `--size` | Filter by size range (small, medium, large, billion) |
| `--source` | Filter by source (ann-benchmarks, big-ann, ms-marco, etc.) |

**Examples:**
```bash
# List all datasets
vst dataset list

# Filter by format
vst dataset list --format hdf5

# Filter by size
vst dataset list --size large

# Filter by source
vst dataset list --source big-ann
```

### `vst dataset download`

Download a dataset.

```bash
vst dataset download [OPTIONS] NAME
```

| Option | Short | Type | Description |
|--------|-------|------|-------------|
| `--output` | `-o` | PATH | Output directory |
| `--force` | `-f` | FLAG | Overwrite existing files |

**Examples:**
```bash
# Download a dataset
vst dataset download openai-5m

# Download to specific directory
vst dataset download sift-128d --output /data/datasets

# Force overwrite existing files
vst dataset download openai-5m --force
```

### `vst dataset info`

Display detailed information about a dataset.

```bash
vst dataset info DATASET_PATH_OR_NAME
```

**Examples:**
```bash
# Info about downloaded dataset
vst dataset info ./_datasets_downloads/openai-5m.h5

# Info about registry dataset
vst dataset info openai-5m

# Info with metadata
vst dataset info sift-128d --verbose
```

### `vst dataset quick-setup`

One-command setup for recommended datasets by scale.

```bash
vst dataset quick-setup [OPTIONS] SCALE
```

| Option | Short | Type | Default | Description |
|--------|-------|------|---------|-------------|
| `--output` | `-o` | PATH | ./_datasets_downloads | Output directory |
| `--verify` | | FLAG | True | Verify download integrity |

**Available scales:**
- `small` - ANN-Benchmarks SIFT (1M vectors) - Good for testing
- `medium` - Big-ANN 10M vectors - Production evaluation  
- `large` - Big-ANN 100M vectors - Large-scale testing
- `billion` - Facebook SSN++ (1.17B vectors) - Billion-scale evaluation

**Examples:**
```bash
# Quick setup for testing
vst dataset quick-setup small

# Production-scale setup
vst dataset quick-setup medium

# Billion-scale evaluation
vst dataset quick-setup billion --output /data/billion-scale
```

### `vst dataset download-bigann`

Download Big-ANN competition datasets (10M to 1.17B vectors).

```bash
vst dataset download-bigann [OPTIONS] DATASET_NAME
```

| Option | Short | Type | Default | Description |
|--------|-------|------|---------|-------------|
| `--output` | `-o` | PATH | ./_datasets_downloads | Output directory |
| `--parallel` | `-p` | INTEGER | 4 | Parallel download threads |

**Available datasets:**
- `fb-ssnpp` - Facebook SimSearchNet++ (1.17B vectors, 256d)
- `msft-turing-10m` - Microsoft Turing ANNS (10M vectors)
- `msft-turing-100m` - Microsoft Turing ANNS (100M vectors)
- `msft-spacev-1b` - Microsoft SPACEV (1B vectors)

**Examples:**
```bash
# Download Facebook SimSearchNet++ (1.17B vectors)
vst dataset download-bigann fb-ssnpp

# Download with custom output
vst dataset download-bigann msft-turing-10m --output /data/datasets

# Download with parallel threads
vst dataset download-bigann fb-ssnpp --parallel 8
```

### `vst dataset download-sparse`

Download MS MARCO sparse datasets with ground truth.

```bash
vst dataset download-sparse [OPTIONS] SIZE
```

| Option | Short | Type | Default | Description |
|--------|-------|------|---------|-------------|
| `--output` | `-o` | PATH | ./_datasets_downloads | Output directory |
| `--include-gt` | | FLAG | True | Include ground truth files |

**Available sizes:**
- `small` or `100k` - 100K documents
- `1M` - 1 million documents  
- `full` or `8.8M` - Full 8.8M documents

**Examples:**
```bash
# Download 1M sparse dataset
vst dataset download-sparse 1M

# Download small dataset without ground truth
vst dataset download-sparse small --no-include-gt

# Custom output directory
vst dataset download-sparse full --output /data/sparse
```

### `vst dataset download-vdb`

Download VectorDBBench datasets with usage instructions.

```bash
vst dataset download-vdb [OPTIONS] DATASET_NAME
```

| Option | Short | Type | Default | Description |
|--------|-------|------|---------|-------------|
| `--output` | `-o` | PATH | ./_datasets_downloads | Output directory |
| `--format` | | TEXT | parquet | Output format (parquet, hdf5) |

**Available datasets:**
- `openai-small` - OpenAI embeddings (50K vectors)
- `openai-medium` - OpenAI embeddings (500K vectors)
- `openai-large` - OpenAI embeddings (5M vectors)
- `sift-500k` - SIFT descriptors (500K vectors)
- `cohere-1m` - Cohere multilingual (1M vectors)
- `laion-100m` - LAION CLIP embeddings (100M vectors)

**Examples:**
```bash
# Download OpenAI small dataset
vst dataset download-vdb openai-small

# Download with HDF5 format
vst dataset download-vdb sift-500k --format hdf5
```

### `vst dataset formats`

Display comprehensive format documentation.

```bash
vst dataset formats [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `--format` | Show details for specific format |
| `--examples` | Include loading examples |

**Supported formats:**
- **HDF5** (.h5, .hdf5) - Standard ANN-Benchmarks format
- **U8BIN/FBIN** (.u8bin, .fbin) - Facebook Big-ANN binary format
- **Sparse CSR** (.csr.gz) - MS MARCO sparse format
- **Parquet** (.parquet) - VectorDBBench format
- **NumPy** (.npy, .npz) - NumPy array format
- **Range Results** (.rangeres) - Facebook SSN++ ground truth format

### `vst dataset validate`

Validate dataset format and integrity.

```bash
vst dataset validate [OPTIONS] DATASET_PATH
```

| Option | Description |
|--------|-------------|
| `--check-integrity` | Verify data integrity |
| `--check-format` | Validate format compliance |
| `--verbose` | Detailed validation output |

---

## Info Commands

Display system, service, and configuration information.

### `vst info system`

Display comprehensive system information.

```bash
vst info system [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `--hardware` | Include hardware details |
| `--memory` | Include memory information |
| `--python` | Include Python environment details |

**Information displayed:**
- Operating system and version
- Python version and executable path
- Available memory and CPU cores
- Disk space information
- Package installation status

### `vst info valkey`

Display Valkey server information and test connectivity.

```bash
vst info valkey [OPTIONS]
```

| Option | Short | Type | Default | Description |
|--------|-------|------|---------|-------------|
| `--host` | `-h` | TEXT | localhost | Valkey host |
| `--port` | `-p` | INTEGER | 6379 | Valkey port |
| `--password` | `-a` | TEXT | None | Valkey password |
| `--database` | `-d` | INTEGER | 0 | Valkey database number |
| `--test-modules` | | FLAG | False | Test Search module availability |

**Examples:**
```bash
# Local Valkey info
vst info valkey

# Remote Valkey server
vst info valkey --host myvalkey.example.com --port 6380

# With authentication
vst info valkey --password mypassword

# Test Search module
vst info valkey --test-modules
```

### `vst info workloads`

List available workload types and their descriptions.

```bash
vst info workloads [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `--details` | Show detailed workload descriptions |
| `--parameters` | Show configurable parameters |

**Available workloads:**
- `ingestion` - Data ingestion workload
- `search` - Vector search workload  
- `mixed` - Combined ingestion and search
- `delete` - Data deletion workload
- `update` - Data update workload
- `index_rebuild` - Index rebuilding workload

### `vst info config`

Display current configuration and settings.

```bash
vst info config [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `--defaults` | Show default configuration |
| `--environment` | Show environment variables |
| `--paths` | Show configuration file paths |

---

## Prep Commands

Dataset preparation and S3 management.

### `vst prep list`

List available datasets in S3.

```bash
vst prep list
```

### `vst prep download`

Download dataset from S3.

```bash
vst prep download [OPTIONS] DATASET_NAME
```

### `vst prep prepare`

Prepare dataset for S3 storage with RDB generation.

```bash
vst prep prepare [OPTIONS] DATASET_PATH
```

### `vst prep info`

Show detailed information about a dataset.

```bash
vst prep info DATASET_NAME
```

### `vst prep validate`

Validate dataset integrity and functionality.

```bash
vst prep validate DATASET_PATH
```

### `vst prep estimate`

Estimate preparation time and resource requirements.

```bash
vst prep estimate DATASET_PATH
```

### `vst prep delete`

Delete dataset from S3.

```bash
vst prep delete DATASET_NAME
```

---

## Validate Commands

Validate configurations, scenarios, and datasets before execution.

### `vst validate scenario`

Validate a scenario configuration file.

```bash
vst validate scenario [OPTIONS] SCENARIO_FILE
```

| Option | Description |
|--------|-------------|
| `--strict` | Enable strict validation mode |
| `--check-datasets` | Validate referenced datasets exist |

**Examples:**
```bash
# Basic scenario validation
vst validate scenario ./my-scenario.yaml

# Strict validation
vst validate scenario ./scenario.yaml --strict

# Check dataset references
vst validate scenario ./scenario.yaml --check-datasets
```

### `vst validate config`

Validate a configuration file.

```bash
vst validate config [OPTIONS] CONFIG_FILE
```

| Option | Description |
|--------|-------------|
| `--check-connections` | Test Valkey connections |
| `--check-paths` | Validate file paths |

**Examples:**
```bash
# Basic config validation
vst validate config ./config.yaml

# Full validation with connectivity tests
vst validate config ./config.yaml --check-connections --check-paths
```

### `vst validate dataset`

Validate dataset format and accessibility.

```bash
vst validate dataset [OPTIONS] DATASET_PATH
```

| Option | Description |
|--------|-------------|
| `--format` | Expected format validation |
| `--sample` | Sample data for validation |

---

## Visualize Commands

Generate visualizations from scenario execution data.

### `vst visualize generate`

Generate visualizations from scenario execution data.

```bash
vst visualize generate [OPTIONS] DATA_PATH
```

### `vst visualize compare`

Compare metrics across multiple scenarios.

```bash
vst visualize compare [OPTIONS] SCENARIO_PATHS...
```

### `vst visualize list-scenarios`

List available scenarios with metrics data.

```bash
vst visualize list-scenarios
```

### `vst visualize export-data`

Export scenario data in various formats for external analysis.

```bash
vst visualize export-data [OPTIONS] DATA_PATH OUTPUT_PATH
```

---

## Version Command

### `vst version`

Display version information and component details.

```bash
vst version [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `--components` | Show component versions |
| `--check-updates` | Check for available updates |

---

## Configuration Reference

### Configuration File Structure

The tool uses YAML configuration files with the following structure:

```yaml
# Valkey connection settings
valkey:
  host: localhost
  port: 6379
  db: 0
  max_connections: 1000
  password: null
  ssl: false
  socket_timeout: 30.0
  retry_on_timeout: true

# Vector index configuration
index:
  algorithm: HNSW
  dimensions: 1536
  distance_metric: L2
  m: 16
  ef_construction: 356
  ef_runtime: 200
  initial_cap: 10000
  block_size: 1024

# Workload execution settings  
workload:
  n_threads: 8
  n_clients_per_thread: 125
  batch_size: 1000
  operation_timeout: 30.0
  max_retries: 3
  retry_delay: 1.0
  jitter: true

# Monitoring and metrics
monitoring:
  sampling_interval: 10.0
  memory_metrics:
    - rss_mb
    - active_mb
    - allocated_mb
    - fragmentation_ratio
  performance_metrics:
    - operations_per_second
    - latency_p50
    - latency_p95
    - latency_p99
  collection_enabled: true
  export_format: json

# S3 configuration
s3:
  bucket_name: "vss-datasets"
  region: "us-east-1"
  multipart_threshold: 67108864
  max_concurrency: 10
  download_threads: 4

# Dataset preparation
datasets:
  default_compression: "zstd"
  batch_size: 1000
  memory_limit_gb: null
  create_index_by_default: true
  default_index_algorithm: "HNSW"
  default_distance_metric: "COSINE"
```

### Valkey Configuration Options

```yaml
valkey:
  # Connection settings
  host: localhost              # Valkey server hostname
  port: 6379                  # Valkey server port
  db: 0                       # Valkey database number (0-15)
  password: null              # Valkey password
  username: null              # Valkey username (Valkey 6.0+)
  
  # SSL/TLS settings
  ssl: false                  # Enable SSL/TLS
  ssl_cert_reqs: required     # SSL certificate requirements
  ssl_ca_certs: null          # CA certificate file path
  
  # Connection pooling
  max_connections: 1000       # Maximum connections in pool
  retry_on_timeout: true      # Retry operations on timeout
  socket_timeout: 30.0        # Socket operation timeout (seconds)
  socket_connect_timeout: 30.0 # Socket connection timeout (seconds)
  health_check_interval: 30   # Health check interval (seconds)
  
  # Cluster mode settings
  cluster_mode: false
  startup_nodes: []
  skip_full_coverage_check: false
  
  # Sentinel mode settings
  sentinel_mode: false
  sentinels: []
  service_name: mymaster
```

### Index Configuration Options

```yaml
index:
  # Basic parameters
  algorithm: HNSW             # Index algorithm: HNSW, FLAT
  dimensions: 1536            # Vector dimensions
  distance_metric: L2         # Distance metric: L2, IP, COSINE
  initial_cap: 10000          # Initial index capacity
  block_size: 1024           # Memory block size
  
  # HNSW-specific parameters
  m: 16                      # Number of bi-directional links
  ef_construction: 356       # Size of dynamic candidate list for construction
  ef_runtime: 200           # Size of dynamic candidate list for search
  max_m: 16                 # Maximum connections for level 0
  max_m0: 32                # Maximum connections for other levels
  ml: 1.0                   # Level normalization factor
```

### Workload Configuration Options

```yaml
workload:
  # Concurrency settings
  n_threads: 8              # Number of worker threads
  n_clients_per_thread: 125 # Clients per thread
  batch_size: 1000          # Operations per batch
  
  # Timeout and retry configuration
  operation_timeout: 30.0   # Operation timeout (seconds)
  max_retries: 3           # Maximum retry attempts
  retry_delay: 1.0         # Base retry delay (seconds)
  exponential_backoff: true # Use exponential backoff
  jitter: true             # Add random jitter to retries
  
  # Rate limiting
  max_operations_per_second: null # Rate limit (null = unlimited)
  burst_capacity: 1000     # Burst operation capacity
  
  # Memory management
  vector_cache_size: 10000  # Number of vectors to cache
  enable_vector_pooling: true # Reuse vector objects
  gc_frequency: 1000       # Garbage collection frequency
```

### Monitoring Configuration Options

```yaml
monitoring:
  # Collection settings
  collection_enabled: true  # Enable metrics collection
  sampling_interval: 10.0  # Collection interval (seconds)
  buffer_size: 1000       # Metrics buffer size
  
  # Memory metrics
  memory_metrics:
    - rss_mb               # Resident Set Size
    - vms_mb               # Virtual Memory Size  
    - active_mb            # Active memory
    - allocated_mb         # Allocated memory
    - fragmentation_ratio  # Memory fragmentation
    - peak_allocated_mb    # Peak allocated memory
    - cache_hit_ratio     # Cache hit ratio
  
  # Performance metrics
  performance_metrics:
    - operations_per_second # Throughput
    - latency_p50          # 50th percentile latency
    - latency_p95          # 95th percentile latency  
    - latency_p99          # 99th percentile latency
    - latency_max          # Maximum latency
    - error_rate           # Error percentage
    - connection_count     # Active connections
    - queue_depth          # Operation queue depth
  
  # Export settings
  export_format: json       # json, csv, prometheus, influxdb
  export_interval: 60      # Export interval (seconds)
  output_file: metrics.json # Local file output
  retention_days: 7        # Days to retain metrics
```

### S3 Configuration Options

```yaml
s3:
  # Basic settings
  bucket_name: "vss-datasets"      # S3 bucket name
  region: "us-east-1"              # AWS region
  access_key_id: null              # AWS access key ID
  secret_access_key: null          # AWS secret access key
  session_token: null              # AWS session token
  
  # Performance settings
  multipart_threshold: 67108864    # 64MB - files larger use multipart upload
  max_concurrency: 10              # Maximum concurrent uploads/downloads
  multipart_chunksize: 67108864    # 64MB chunk size for multipart uploads
  download_threads: 4              # Number of download threads
  
  # Advanced settings
  use_ssl: true                    # Use HTTPS for S3 connections
  endpoint_url: null               # Custom S3 endpoint
  signature_version: "s3v4"        # AWS signature version
  addressing_style: "auto"         # Bucket addressing style
```

### Dataset Preparation Configuration

```yaml
datasets:
  # Processing settings
  default_compression: "zstd"      # Compression: none, zstd, or lz4
  default_block_size: 1000         # Default block size for streaming
  batch_size: 1000                 # Batch size for dataset processing
  
  # Memory management
  memory_limit_gb: null            # Optional memory limit for processing
  memory_warning_threshold: 0.8    # Warning threshold (80% of limit)
  gc_frequency: 1000               # Garbage collection frequency
  
  # Index creation defaults
  create_index_by_default: true    # Whether to create indexes by default
  default_index_algorithm: "HNSW"  # Default index algorithm
  default_distance_metric: "COSINE" # Default distance metric
  default_hnsw_m: 16               # Default HNSW M parameter
  default_hnsw_ef_construction: 200 # Default HNSW ef_construction
  
  # Processing limits
  max_vector_dimensions: 4096      # Maximum supported vector dimensions
  max_dataset_size_gb: 1000.0      # Maximum dataset size in GB
  processing_timeout_minutes: 480  # Processing timeout (8 hours)
  
  # Valkey connection for RDB generation
  valkey_host: "localhost"         # Valkey host for RDB generation
  valkey_port: 6379                # Valkey port for RDB generation
  valkey_password: null            # Valkey password (if required)
```

---

## Environment Variables

Configure default behavior using environment variables:

### Valkey Settings
```bash
export VST_VALKEY_HOST=valkey.example.com
export VST_VALKEY_PORT=6379
export VST_VALKEY_PASSWORD=secret123
export VST_VALKEY_DB=0
export VST_VALKEY_SSL=true
export VST_VALKEY_MAX_CONNECTIONS=2000
```

### Index Settings
```bash
export VST_INDEX_ALGORITHM=HNSW
export VST_INDEX_DIMENSIONS=1536
export VST_INDEX_M=16
export VST_INDEX_EF_CONSTRUCTION=356
export VST_INDEX_EF_RUNTIME=200
```

### Workload Settings
```bash
export VST_WORKLOAD_THREADS=16
export VST_WORKLOAD_CLIENTS=2000
export VST_WORKLOAD_BATCH_SIZE=1000
export VST_WORKLOAD_TIMEOUT=60
```

### S3 Configuration
```bash
# AWS credentials
export AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
export AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
export AWS_SESSION_TOKEN=your_session_token
export AWS_DEFAULT_REGION=us-east-1
export AWS_S3_BUCKET=your-vss-datasets-bucket

# S3 performance settings
export VST_S3_MULTIPART_THRESHOLD=67108864     # 64MB
export VST_S3_MAX_CONCURRENCY=10
export VST_S3_DOWNLOAD_THREADS=4
export VST_S3_MAX_RETRIES=3
```

### Dataset Preparation
```bash
# Processing settings
export VST_MEMORY_LIMIT_GB=32.0
export VST_DEFAULT_COMPRESSION=zstd
export VST_DATASET_BATCH_SIZE=1000
export VST_PROCESSING_TIMEOUT_MINUTES=480

# Output and logging
export VST_OUTPUT_DIR=/path/to/output
export VST_LOG_LEVEL=INFO
```

### General Settings
```bash
export VST_CONFIG_FILE=/path/to/config.yaml
export VST_OUTPUT_DIR=./output
export VST_DATASET_DIR=./_datasets_downloads
export VST_LOG_LEVEL=INFO
```

---

## Exit Codes

| Code | Description |
|------|-------------|
| 0 | Success |
| 1 | General error |
| 2 | Invalid command line arguments |
| 3 | Configuration error |
| 4 | Dataset error |
| 5 | Valkey connection error |

---

## Configuration Utilities

The tool includes a configuration utility script for managing configurations:

### Using the Configuration Utility

```bash
# Show current configuration
python scripts/config_util.py info

# Show configuration from a specific file
python scripts/config_util.py info --config config/production.yaml

# Validate a configuration file
python scripts/config_util.py validate config/my_config.yaml

# Generate an example configuration file
python scripts/config_util.py generate config/example.yaml

# Show all available environment variables
python scripts/config_util.py env-vars
```

### Configuration Validation

```bash
# Validate configuration file
vst config validate --config my_config.yaml

# Check current configuration
vst config show

# Test Valkey connection with current config
vst info valkey
```

---

For detailed usage examples and workflows, see the [User Guide](USER_GUIDE.md).
For installation instructions, see the [Installation Guide](INSTALLATION_GUIDE.md).
