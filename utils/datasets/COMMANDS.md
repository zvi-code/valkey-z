# Commands Reference Guide

Complete reference for all Valkey Search Delete Stress Test CLI commands, based on the actual CLI implementation.

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

List available datasets.

```bash
vst dataset list
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

Display information about a dataset.

```bash
vst dataset info DATASET_PATH_OR_NAME
```

**Examples:**
```bash
# Info about downloaded dataset
vst dataset info ./_datasets_downloads/openai-5m.h5

# Info about registry dataset
vst dataset info openai-5m
```

### `vst dataset quick-setup`

Quick setup with recommended datasets.

```bash
vst dataset quick-setup [OPTIONS] SCALE
```

**Available scales:**
- `small` - Small dataset for testing
- `medium` - Medium-scale dataset  
- `large` - Large-scale dataset
- `billion` - Billion-scale dataset

**Examples:**
```bash
# Quick setup for testing
vst dataset quick-setup small

# Production-scale setup
vst dataset quick-setup medium

# Large-scale evaluation
vst dataset quick-setup large
```

### `vst dataset convert`

Convert dataset between formats.

```bash
vst dataset convert [OPTIONS] INPUT_PATH OUTPUT_PATH
```

### `vst dataset validate`

Validate dataset integrity.

```bash
vst dataset validate DATASET_PATH
```

---

## Info Commands

Display system and service information.

### `vst info system`

Display system information.

```bash
vst info system
```

**Information displayed:**
- Operating system and version
- Python version and executable path
- Available memory and CPU cores
- Disk space information
- Package installation status

### `vst info valkey`

Display Valkey server information.

```bash
vst info valkey [OPTIONS]
```

**Examples:**
```bash
# Local Valkey info
vst info valkey

# Remote Valkey server (use appropriate options)
vst info valkey
```

### `vst info workloads`

List available workloads.

```bash
vst info workloads
```

**Available workloads:**
- `ingest` - Data ingestion workload
- `query` - Vector search workload  
- `mixed` - Combined ingestion and search
- `shrink` - Data shrinking workload

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

Validate configurations and scenarios.

### `vst validate scenario`

Validate a scenario configuration file.

```bash
vst validate scenario SCENARIO_FILE
```

**Examples:**
```bash
# Validate scenario file
vst validate scenario ./my-scenario.yaml

# Validate built-in scenario
vst validate scenario continuous_growth
```

### `vst validate config`

Validate a configuration file.

```bash
vst validate config CONFIG_FILE
```

**Example:**
```bash
vst validate config ./config.yaml
```

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

Display version information.

```bash
vst version
```

---

## Environment Variables

Configure default behavior using environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `VST_CONFIG_FILE` | Default configuration file | None |
| `VST_OUTPUT_DIR` | Default output directory | ./output |
| `VST_VALKEY_HOST` | Default Valkey host | localhost |
| `VST_VALKEY_PORT` | Default Valkey port | 6379 |
| `VST_VALKEY_PASSWORD` | Default Valkey password | None |
| `VST_DATASET_DIR` | Default dataset directory | ./_datasets_downloads |
| `VST_LOG_LEVEL` | Logging level | INFO |

## Exit Codes

| Code | Description |
|------|-------------|
| 0 | Success |
| 1 | General error |
| 2 | Invalid command line arguments |
| 3 | Configuration error |
| 4 | Dataset error |
| 5 | Valkey connection error |

## Common Workflows

### Basic Testing Workflow

```bash
# 1. Check system requirements
vst info system

# 2. Test Valkey connectivity
vst info valkey

# 3. Download test dataset
vst dataset quick-setup small

# 4. Run quick test
vst run quick --duration 300

# 5. Check results
ls ./output/
```

### Dataset Management Workflow

```bash
# 1. List available datasets
vst dataset list

# 2. Download datasets
vst dataset download openai-5m
vst dataset download sift-128d --output ./_datasets_downloads

# 3. Validate downloads
vst dataset validate ./_datasets_downloads/openai-5m.h5
vst dataset info openai-5m

# 4. Quick setup for testing
vst dataset quick-setup medium
```

### Scenario Testing Workflow

```bash
# 1. List available scenarios
vst run list-scenarios

# 2. Validate scenario
vst validate scenario continuous_growth

# 3. Run scenario
vst run scenario continuous_growth --output ./scenario-test

# 4. Generate visualizations
vst visualize generate ./scenario-test
```

### S3 Dataset Preparation Workflow

```bash
# 1. List S3 datasets
vst prep list

# 2. Download from S3
vst prep download my-dataset

# 3. Get dataset info
vst prep info my-dataset

# 4. Validate dataset
vst prep validate ./_datasets_downloads/my-dataset

# 5. Estimate preparation requirements
vst prep estimate ./_datasets_downloads/my-dataset
```

### Performance Analysis Workflow

```bash
# 1. Run multiple scenarios
vst run scenario memory_test --output ./test1
vst run scenario performance_test --output ./test2

# 2. Compare results
vst visualize compare ./test1 ./test2

# 3. Export data for analysis
vst visualize export-data ./test1 ./analysis/test1.csv

# 4. List scenarios with data
vst visualize list-scenarios
```

## Tips and Best Practices

### Performance Optimization

1. **Dataset Selection**: Use appropriate dataset sizes for your testing goals
2. **Output Management**: Specify output directories to organize results
3. **Workload Tuning**: Choose appropriate workload types (ingest, query, mixed, shrink)
4. **Duration Setting**: Set reasonable test durations based on your requirements

### Error Handling

1. **Always validate** scenarios and configs before execution
2. **Check system resources** before large tests
3. **Monitor output directory** space during execution
4. **Use force flag** when needed to overwrite existing datasets

### Debugging

1. **Use verbose mode** (`--verbose`) for detailed logs
2. **Start with small tests** to verify setup
3. **Check connectivity** with `vst info valkey`
4. **Validate configurations** before running

### Visualization and Analysis

1. **Generate visualizations** after scenario execution
2. **Compare multiple scenarios** to understand performance patterns
3. **Export data** for external analysis tools
4. **Use prep commands** for S3-based dataset management

---

For more detailed information, see:
- [Installation Guide](INSTALL.md)
- [Getting Started Guide](docs/GETTING_STARTED.md)
- [Configuration Guide](docs/CONFIGURATION.md)
- [Scenario Development Guide](docs/SCENARIOS.md)

### Core Dataset Commands

#### `vst dataset list`

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

#### `vst dataset info`

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

### Specialized Download Commands

#### `vst dataset download-bigann`

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

#### `vst dataset download-sparse`

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

#### `vst dataset download-vdb`

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

### Quick Setup Commands

#### `vst dataset quick-setup`

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

### Format and Utility Commands

#### `vst dataset formats`

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

**Examples:**
```bash
# Show all formats
vst dataset formats

# Details for specific format
vst dataset formats --format bigann

# With loading examples
vst dataset formats --examples
```

#### `vst dataset benchmark-info`

Display detailed benchmark information.

```bash
vst dataset benchmark-info [OPTIONS] BENCHMARK_NAME
```

**Available benchmarks:**
- `ann-benchmarks` - Standard ANN benchmarks
- `big-ann` - NeurIPS Big-ANN competition
- `vectordbbench` - VectorDBBench suite
- `ms-marco` - Microsoft MARCO sparse retrieval

**Examples:**
```bash
# Big-ANN competition info
vst dataset benchmark-info big-ann

# ANN-Benchmarks details  
vst dataset benchmark-info ann-benchmarks

# VectorDBBench information
vst dataset benchmark-info vectordbbench
```

#### `vst dataset create-vdb-format`

Convert datasets to VectorDBBench Parquet format.

```bash
vst dataset create-vdb-format [OPTIONS] INPUT_PATH OUTPUT_DIR
```

| Option | Short | Type | Description |
|--------|-------|------|-------------|
| `--train-size` | | INTEGER | Training set size |
| `--test-size` | | INTEGER | Test set size |
| `--split-ratio` | | FLOAT | Train/test split ratio (0.8) |

**Examples:**
```bash
# Convert HDF5 to VectorDBBench format
vst dataset create-vdb-format ./sift-128d.h5 ./vdb-output

# Custom split sizes
vst dataset create-vdb-format ./data.h5 ./output --train-size 800000 --test-size 200000
```

#### `vst dataset validate`

Validate dataset format and integrity.

```bash
vst dataset validate [OPTIONS] DATASET_PATH
```

| Option | Description |
|--------|-------------|
| `--check-integrity` | Verify data integrity |
| `--check-format` | Validate format compliance |
| `--verbose` | Detailed validation output |

**Examples:**
```bash
# Basic validation
vst dataset validate ./dataset.h5

# Full integrity check
vst dataset validate ./dataset.h5 --check-integrity

# Verbose format validation
vst dataset validate ./dataset.h5 --check-format --verbose
```

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

**Examples:**
```bash
# Basic system info
vst info system

# Detailed hardware info
vst info system --hardware

# Memory details
vst info system --memory
```

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

## Environment Variables

Configure default behavior using environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `VST_CONFIG_FILE` | Default configuration file | None |
| `VST_OUTPUT_DIR` | Default output directory | ./output |
| `VST_VALKEY_HOST` | Default Valkey host | localhost |
| `VST_VALKEY_PORT` | Default Valkey port | 6379 |
| `VST_VALKEY_PASSWORD` | Default Valkey password | None |
| `VST_DATASET_DIR` | Default dataset directory | ./_datasets_downloads |
| `VST_LOG_LEVEL` | Logging level | INFO |

**Examples:**
```bash
# Set default Valkey connection
export VST_VALKEY_HOST=myvalkey.example.com
export VST_VALKEY_PORT=6380

# Set default directories
export VST_OUTPUT_DIR=/data/output
export VST_DATASET_DIR=/data/datasets

# Enable debug logging
export VST_LOG_LEVEL=DEBUG
```

## Exit Codes

| Code | Description |
|------|-------------|
| 0 | Success |
| 1 | General error |
| 2 | Invalid command line arguments |
| 3 | Configuration error |
| 4 | Dataset error |
| 5 | Valkey connection error |

## Common Workflows

### Basic Testing Workflow

```bash
# 1. Check system requirements
vst info system

# 2. Test Valkey connectivity
vst info valkey

# 3. Download test dataset
vst dataset quick-setup small

# 4. Run quick test
vst run quick --duration 300

# 5. Check results
ls ./output/
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
vst info config --paths
```

### Dataset Management Workflow

```bash
# 1. List available datasets
vst dataset list

# 2. Get format information
vst dataset formats

# 3. Download datasets
vst dataset download-bigann fb-ssnpp
vst dataset download-sparse 1M

# 4. Validate downloads
vst dataset validate ./_datasets_downloads/fb-ssnpp-database.u8bin
vst dataset validate ./_datasets_downloads/msmarco-sparse-1M.csr.gz

# 5. Convert to common format (if needed)
vst dataset create-vdb-format ./_datasets_downloads/custom.h5 ./vdb-format/
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

### Development and Testing Workflow

```bash
# 1. Setup development environment
vst dataset quick-setup small

# 2. Create custom scenario
cp config/scenarios/continuous_growth.yaml ./my-test.yaml

# 3. Validate custom scenario
vst validate scenario ./my-test.yaml --strict

# 4. Test with dry-run
vst run scenario ./my-test.yaml --dry-run

# 5. Quick execution test
vst run scenario ./my-test.yaml --variables "duration=60"

# 6. Full execution
vst run scenario ./my-test.yaml
```

## Advanced Usage

### Custom Dataset Integration

```bash
# 1. Validate your dataset format
vst dataset validate ./my-dataset.h5

# 2. Get format details
vst dataset formats --format hdf5 --examples

# 3. Convert if needed
vst dataset create-vdb-format ./my-dataset.h5 ./converted/

# 4. Test with quick run
vst run quick --dataset ./my-dataset.h5 --duration 60
```

### Performance Monitoring

```bash
# 1. Enable verbose logging
export VST_LOG_LEVEL=DEBUG

# 2. Run with monitoring
vst run scenario memory_fragmentation_analysis.yaml --verbose

# 3. Check system during run
vst info system --memory  # In another terminal

# 4. Analyze output
ls -la ./output/
vst dataset info ./output/metrics.csv
```

### Cluster Testing

```bash
# 1. Test individual nodes
vst info valkey --host node1.cluster.com
vst info valkey --host node2.cluster.com

# 2. Run distributed scenario
vst run scenario cluster_memory_distribution.yaml \
  --variables "valkey_hosts=node1,node2,node3"

# 3. Aggregate results
vst dataset info ./output/cluster-metrics-*.csv
```

## Tips and Best Practices

### Performance Optimization

1. **Connection Tuning**: Use appropriate Valkey connection settings
2. **Batch Sizing**: Adjust batch sizes based on available memory
3. **Thread Configuration**: Match thread count to CPU cores
4. **Dataset Selection**: Use appropriate dataset sizes for your testing goals

### Error Handling

1. **Always validate** scenarios and configs before execution
2. **Use dry-run** for complex scenarios
3. **Check system resources** before large tests
4. **Monitor output directory** space during execution

### Debugging

1. **Use verbose mode** (`--verbose`) for detailed logs
2. **Start with small tests** to verify setup
3. **Check connectivity** with `vst info valkey`
4. **Validate all configurations** before running

### Resource Management

1. **Monitor memory usage** during tests
2. **Clean up output directories** after tests
3. **Use appropriate dataset sizes** for your system
4. **Set reasonable timeouts** in configurations

---

For more detailed information, see:
- [Installation Guide](INSTALL.md)
- [Getting Started Guide](docs/GETTING_STARTED.md)
- [Configuration Guide](docs/CONFIGURATION.md)
- [Scenario Development Guide](docs/SCENARIOS.md)
