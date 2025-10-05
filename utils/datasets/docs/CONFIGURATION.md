# Configuration Guide

This comprehensive guide covers all configuration options for the Search Module Delete Test Testing tool, including the new S3 dataset storage and dataset preparation features.

## Table of Contents

- [Overview](#overview)
- [Configuration Files](#configuration-files)
- [Valkey Configuration](#valkey-configuration)
- [Index Configuration](#index-configuration)
- [Workload Configuration](#workload-configuration)
- [Monitoring Configuration](#monitoring-configuration)
- [S3 Configuration](#s3-configuration)
- [Dataset Preparation Configuration](#dataset-preparation-configuration)
- [Environment Variables](#environment-variables)
- [Configuration Validation](#configuration-validation)
- [Configuration Utilities](#configuration-utilities)
- [Advanced Configuration](#advanced-configuration)
- [Best Practices](#best-practices)
- [Troubleshooting](#troubleshooting)

## Overview

The Search Module Delete Test Testing tool uses YAML configuration files to define:

- **Valkey Connection Settings**: Host, port, authentication, connection pooling
- **Vector Index Parameters**: Algorithm, dimensions, distance metrics
- **Workload Settings**: Concurrency, batch sizes, timeouts
- **Monitoring Options**: Metrics collection, sampling intervals
- **S3 Storage Settings**: Dataset storage and retrieval from AWS S3
- **Dataset Preparation**: Converting and preparing vector datasets for testing
- **Output Configuration**: Logging and metrics export options

Configuration precedence (highest to lowest):
1. Command-line arguments (`--config`, `--valkey-host`, etc.)
2. Environment variables (`VST_VALKEY_HOST`, `AWS_S3_BUCKET`, etc.)
3. Custom configuration file
4. Default configuration values

## Configuration Files

### Default Configuration

The tool ships with a default configuration at `config/default.yaml`:

```yaml
# Valkey connection settings
valkey:
  host: localhost
  port: 6379
  db: 0
  max_connections: 1000
  password: null
  ssl: false
  ssl_cert_reqs: required
  socket_timeout: 30.0
  socket_connect_timeout: 30.0
  retry_on_timeout: true
  health_check_interval: 30

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
    - resident_mb
    - allocated_mb
    - fragmentation_ratio
  performance_metrics:
    - operations_per_second
    - latency_p50
    - latency_p95
    - latency_p99
  collection_enabled: true
  export_format: json
```

### Custom Configuration

Create custom configurations for different environments:

```yaml
# config/production.yaml
valkey:
  host: valkey-cluster.example.com
  port: 6379
  password: "${VALKEY_PASSWORD}"
  ssl: true
  max_connections: 2000

workload:
  n_threads: 16
  n_clients_per_thread: 250
  batch_size: 2000
  operation_timeout: 60.0

monitoring:
  sampling_interval: 5.0
  export_format: prometheus
```

```yaml
# config/development.yaml
valkey:
  host: localhost
  port: 6379
  db: 1

workload:
  n_threads: 2
  n_clients_per_thread: 10
  batch_size: 100

monitoring:
  sampling_interval: 1.0
  collection_enabled: true
```

## Valkey Configuration

### Basic Connection Settings

```yaml
valkey:
  # Required settings
  host: localhost              # Valkey server hostname
  port: 6379                  # Valkey server port
  db: 0                       # Valkey database number (0-15)
  
  # Optional authentication
  password: null              # Valkey password (or use environment variable)
  username: null              # Valkey username (Valkey 6.0+)
  
  # SSL/TLS settings
  ssl: false                  # Enable SSL/TLS
  ssl_cert_reqs: required     # SSL certificate requirements
  ssl_ca_certs: null          # CA certificate file path
  ssl_cert_file: null         # Client certificate file path
  ssl_key_file: null          # Client private key file path
```

### Connection Pooling

```yaml
valkey:
  # Connection pool settings
  max_connections: 1000       # Maximum connections in pool
  max_connections_per_pool: 50 # Max connections per pool instance
  retry_on_timeout: true      # Retry operations on timeout
  socket_timeout: 30.0        # Socket operation timeout (seconds)
  socket_connect_timeout: 30.0 # Socket connection timeout (seconds)
  socket_keepalive: true      # Enable TCP keepalive
  socket_keepalive_options: {}
  
  # Health checking
  health_check_interval: 30   # Health check interval (seconds)
  connection_pool_class: null # Custom connection pool class
```

### Valkey Cluster Configuration

```yaml
valkey:
  # Cluster mode settings
  cluster_mode: true
  startup_nodes:
    - host: valkey-node-1.example.com
      port: 7001
    - host: valkey-node-2.example.com  
      port: 7002
    - host: valkey-node-3.example.com
      port: 7003
  
  # Cluster-specific options
  skip_full_coverage_check: false
  max_connections_per_node: 100
  readonly_mode: false
```

### Valkey Sentinel Configuration

```yaml
valkey:
  # Sentinel mode settings
  sentinel_mode: true
  sentinels:
    - host: sentinel-1.example.com
      port: 26379
    - host: sentinel-2.example.com
      port: 26379
    - host: sentinel-3.example.com
      port: 26379
  
  # Sentinel options
  service_name: mymaster
  sentinel_kwargs:
    password: sentinel_password
```

## Index Configuration

### Vector Index Settings

```yaml
index:
  # Basic index parameters
  algorithm: HNSW             # Index algorithm: HNSW, FLAT
  dimensions: 1536            # Vector dimensions
  distance_metric: L2         # Distance metric: L2, IP, COSINE
  initial_cap: 10000          # Initial index capacity
  block_size: 1024           # Memory block size
  
  # HNSW-specific parameters
  m: 16                      # Number of bi-directional links for every vertex
  ef_construction: 356       # Size of dynamic candidate list
  ef_runtime: 200           # Size of dynamic candidate list for search
  max_m: 16                 # Maximum number of connections for level 0
  max_m0: 32                # Maximum number of connections for other levels
  ml: 1.0                   # Level normalization factor
```

### Index Algorithm Details

#### HNSW (Hierarchical Navigable Small World)

```yaml
index:
  algorithm: HNSW
  m: 16                     # Typical range: 4-64
                           # Higher values: better recall, more memory
                           # Lower values: faster builds, less memory
  
  ef_construction: 356      # Typical range: 100-800  
                           # Higher values: better index quality, slower build
                           # Recommended: 2x ef_runtime
  
  ef_runtime: 200          # Typical range: 50-400
                           # Higher values: better recall, slower search
                           # Balance based on recall requirements
```

#### FLAT (Brute Force)

```yaml
index:
  algorithm: FLAT           # Exact search, 100% recall
  block_size: 1024         # Memory allocation block size
  initial_cap: 10000       # Initial vector capacity
```

### Distance Metrics

```yaml
index:
  # L2 (Euclidean) distance
  distance_metric: L2       # Best for: general purpose, normalized vectors
  
  # Inner Product  
  distance_metric: IP       # Best for: similarity scoring, recommendation
  
  # Cosine similarity
  distance_metric: COSINE   # Best for: text embeddings, angular similarity
```

## Workload Configuration

### Concurrency Settings

```yaml
workload:
  # Thread and client configuration
  n_threads: 8              # Number of worker threads
  n_clients_per_thread: 125 # Clients per thread
  total_clients: 1000       # Alternative: specify total directly
  
  # Batch processing
  batch_size: 1000          # Operations per batch
  max_batch_size: 5000      # Maximum batch size limit
  min_batch_size: 10        # Minimum batch size limit
  adaptive_batching: false  # Enable adaptive batch sizing
```

### Operation Settings

```yaml
workload:
  # Timeout and retry configuration
  operation_timeout: 30.0   # Operation timeout (seconds)
  max_retries: 3           # Maximum retry attempts
  retry_delay: 1.0         # Base retry delay (seconds)
  exponential_backoff: true # Use exponential backoff
  jitter: true             # Add random jitter to retries
  
  # Rate limiting
  max_operations_per_second: null # Rate limit (null = unlimited)
  burst_capacity: 1000     # Burst operation capacity
```

### Memory Management

```yaml
workload:
  # Memory optimization
  vector_cache_size: 10000  # Number of vectors to cache
  enable_vector_pooling: true # Reuse vector objects
  gc_frequency: 1000       # Garbage collection frequency
  
  # Resource limits
  max_memory_usage_mb: 8192 # Maximum memory usage
  memory_warning_threshold: 0.8 # Warning at 80% usage
```

## Monitoring Configuration

### Metrics Collection

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
    - resident_mb          # Resident memory
    - allocated_mb         # Allocated memory
    - fragmentation_ratio  # Memory fragmentation
    - peak_allocated_mb    # Peak allocated memory
    - cache_hit_ratio     # Cache hit ratio
```

### Performance Metrics

```yaml
monitoring:
  # Performance tracking
  performance_metrics:
    - operations_per_second # Throughput
    - latency_p50          # 50th percentile latency
    - latency_p95          # 95th percentile latency  
    - latency_p99          # 99th percentile latency
    - latency_max          # Maximum latency
    - error_rate           # Error percentage
    - connection_count     # Active connections
    - queue_depth          # Operation queue depth
```

### Export Configuration

```yaml
monitoring:
  # Export formats
  export_format: json       # json, csv, prometheus, influxdb
  export_interval: 60      # Export interval (seconds)
  
  # Output destinations
  output_file: metrics.json # Local file output
  prometheus_port: 8080    # Prometheus metrics port
  influxdb_url: null       # InfluxDB endpoint
  
  # Retention settings
  retention_days: 7        # Days to retain metrics
  max_file_size_mb: 100   # Maximum output file size
```

## S3 Configuration

Configure AWS S3 for dataset storage and retrieval:

### Basic S3 Settings

```yaml
s3:
  bucket_name: "vss-datasets"      # S3 bucket name
  region: "us-east-1"              # AWS region
  
  # AWS credentials (usually from environment/IAM)
  access_key_id: null              # AWS access key ID
  secret_access_key: null          # AWS secret access key
  session_token: null              # AWS session token (for temporary credentials)
```

### Upload/Download Performance

```yaml
s3:
  # Upload settings
  multipart_threshold: 67108864    # 64MB - files larger use multipart upload
  max_concurrency: 10              # Maximum concurrent uploads/downloads
  multipart_chunksize: 67108864    # 64MB chunk size for multipart uploads
  
  # Download settings
  download_threads: 4              # Number of download threads
  
  # Retry settings
  max_retries: 3                   # Maximum retry attempts
  retry_delay: 1.0                 # Delay between retries in seconds
```

### Advanced S3 Configuration

```yaml
s3:
  # Advanced settings
  use_ssl: true                    # Use HTTPS for S3 connections
  endpoint_url: null               # Custom S3 endpoint (for S3-compatible services)
  signature_version: "s3v4"        # AWS signature version
  addressing_style: "auto"         # Bucket addressing style: auto, path, virtual
  
  # Transfer settings
  use_accelerate_endpoint: false   # Use S3 Transfer Acceleration
  use_dualstack_endpoint: false    # Use dual-stack (IPv4/IPv6) endpoint
```

## Dataset Preparation Configuration

Configure dataset preparation and RDB generation:

### Basic Dataset Preparation

```yaml
datasets:
  # Default processing settings
  default_compression: "zstd"      # Compression: none, zstd, or lz4
  default_block_size: 1000         # Default block size for streaming
  
  # Valkey connection for RDB generation
  valkey_host: "localhost"         # Valkey host for RDB generation
  valkey_port: 6379                # Valkey port for RDB generation
  valkey_password: null            # Valkey password (if required)
  batch_size: 1000                 # Batch size for dataset processing
```

### RDB Generation Settings

```yaml
datasets:
  # Memory management
  memory_limit_gb: null            # Optional memory limit for RDB generation
  memory_warning_threshold: 0.8    # Warning threshold (80% of limit)
  gc_frequency: 1000               # Garbage collection frequency (batches)
  
  # RDB generation options
  rdb_compression: true            # Enable RDB compression
  rdb_checksum: true               # Enable RDB checksums
  save_policy: "auto"              # RDB save policy: auto, manual, disabled
```

### Index Creation Defaults

```yaml
datasets:
  # Index creation settings
  create_index_by_default: true    # Whether to create indexes by default
  default_index_algorithm: "HNSW"  # Default index algorithm: HNSW or FLAT
  default_distance_metric: "COSINE" # Default distance metric: L2, IP, or COSINE
  default_hnsw_m: 16               # Default HNSW M parameter (1-512)
  default_hnsw_ef_construction: 200 # Default HNSW ef_construction (1-4096)
  
  # Index optimization
  index_parallel_creation: true    # Create indexes in parallel
  index_creation_timeout: 300      # Index creation timeout (seconds)
```

### Processing Limits and Timeouts

```yaml
datasets:
  # Processing limits
  max_vector_dimensions: 4096      # Maximum supported vector dimensions
  max_dataset_size_gb: 1000.0      # Maximum dataset size in GB
  max_file_count: 10000            # Maximum number of files per dataset
  
  # Timeouts
  processing_timeout_minutes: 480  # Processing timeout (8 hours)
  upload_timeout_minutes: 120      # S3 upload timeout (2 hours)
  download_timeout_minutes: 60     # S3 download timeout (1 hour)
  
  # Resource limits
  max_cpu_cores: null              # Maximum CPU cores to use (null = auto)
  temp_dir: "/tmp/vst"             # Temporary directory for processing
  cleanup_temp_files: true         # Clean up temporary files after processing
```

## Environment Variables

Override configuration values using environment variables:

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
# Valkey connection for RDB generation
export VST_VALKEY_HOST=localhost
export VST_VALKEY_PORT=6379
export VST_VALKEY_PASSWORD=your_valkey_password

# Processing settings
export VST_MEMORY_LIMIT_GB=32.0
export VST_DEFAULT_COMPRESSION=zstd
export VST_DATASET_BATCH_SIZE=1000
export VST_PROCESSING_TIMEOUT_MINUTES=480

# Output and logging
export VST_OUTPUT_DIR=/path/to/output
export VST_LOG_LEVEL=INFO
```

### Using Environment Variables in Config Files

```yaml
valkey:
  host: "${VST_VALKEY_HOST:-localhost}"
  port: "${VST_VALKEY_PORT:-6379}"
  password: "${VST_VALKEY_PASSWORD}"

s3:
  bucket_name: "${AWS_S3_BUCKET:-vss-datasets}"
  region: "${AWS_DEFAULT_REGION:-us-east-1}"
  access_key_id: "${AWS_ACCESS_KEY_ID}"
  secret_access_key: "${AWS_SECRET_ACCESS_KEY}"

datasets:
  valkey_host: "${VST_VALKEY_HOST:-localhost}"
  valkey_port: "${VST_VALKEY_PORT:-6379}"
  memory_limit_gb: "${VST_MEMORY_LIMIT_GB}"
  
workload:
  n_threads: "${VST_WORKLOAD_THREADS:-8}"
  batch_size: "${VST_WORKLOAD_BATCH_SIZE:-1000}"
```

## Configuration Utilities

The Valkey Search Delete Stress Test framework includes a powerful configuration utility script to help manage and validate configurations.

### Using the Configuration Utility

```bash
# Show current configuration (with defaults and environment variables)
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

### Configuration Information Display

The `info` command provides a comprehensive overview of the current configuration:

```bash
$ python scripts/config_util.py info
Current Configuration:
==================================================
Valkey:
  Host: localhost:6379
  Database: 0
  Max Connections: 1000

Vector Index:
  Algorithm: HNSW
  Dimensions: 1536
  Distance Metric: COSINE
  M: 16
  EF Construction: 356
  EF Runtime: 200

S3 Storage:
  Bucket: vss-datasets
  Region: us-east-1
  Multipart Threshold: 64MB
  Max Concurrency: 10
  Download Threads: 4

Dataset Preparation:
  Default Compression: zstd
  Valkey Host: localhost:6379
  Batch Size: 1000
  Max Vector Dimensions: 4096
  Max Dataset Size: 1000.0GB
  Processing Timeout: 480 minutes
```

### Configuration Validation

The `validate` command checks configuration files for errors:

```bash
$ python scripts/config_util.py validate config/production.yaml
✓ Configuration file 'config/production.yaml' is valid

$ python scripts/config_util.py validate config/invalid.yaml
✗ Configuration file 'config/invalid.yaml' is invalid: S3 bucket name cannot be empty
```

### Generating Example Configurations

Create template configuration files:

```bash
# Generate a complete example configuration
python scripts/config_util.py generate config/my_example.yaml

# The generated file includes all sections with default values and comments
```

## Configuration Validation

### Automatic Validation

The tool automatically validates configurations on startup:

```bash
# Validate configuration file
vst config validate --config my_config.yaml

# Check current configuration
vst config show

# Test Valkey connection with current config
vst info valkey
```

### Validation Rules

#### Valkey Configuration
- Host must be reachable
- Port must be valid (1-65535)
- Database number must be 0-15
- SSL settings must be consistent

#### Index Configuration
- Algorithm must be HNSW or FLAT
- Dimensions must be positive integer
- Distance metric must be L2, IP, or COSINE
- HNSW parameters must be within valid ranges

#### Workload Configuration
- Thread count must be positive
- Client count must be positive
- Batch size must be between min and max limits
- Timeout values must be positive

### Custom Validation

Create custom validation rules:

```yaml
# config/validation.yaml
validation:
  valkey:
    connection_timeout: 5.0
    required_modules:
      - valkey-search
      - valkey-JSON
  
  index:
    max_dimensions: 4096
    min_ef_construction: 100
  
  workload:
    max_threads: 32
    max_clients: 10000
```

## Advanced Configuration

### Performance Tuning

#### High-Throughput Configuration

```yaml
# config/high_throughput.yaml
valkey:
  max_connections: 5000
  socket_keepalive: true
  retry_on_timeout: false

workload:
  n_threads: 32
  n_clients_per_thread: 156
  batch_size: 2000
  operation_timeout: 10.0
  
index:
  algorithm: FLAT  # For maximum throughput
  block_size: 4096

monitoring:
  sampling_interval: 1.0
  buffer_size: 10000
```

#### Low-Latency Configuration

```yaml
# config/low_latency.yaml
valkey:
  socket_timeout: 1.0
  socket_connect_timeout: 1.0
  max_connections: 100

workload:
  n_threads: 4
  n_clients_per_thread: 25
  batch_size: 1
  operation_timeout: 1.0

index:
  algorithm: HNSW
  ef_runtime: 50  # Lower for faster search

monitoring:
  sampling_interval: 0.1
```

#### Memory-Optimized Configuration

```yaml
# config/memory_optimized.yaml
valkey:
  max_connections: 50

workload:
  n_threads: 2
  n_clients_per_thread: 25
  batch_size: 100
  vector_cache_size: 1000
  enable_vector_pooling: true

index:
  algorithm: HNSW
  m: 8  # Lower memory usage
  initial_cap: 1000
  block_size: 256

monitoring:
  buffer_size: 100
  memory_warning_threshold: 0.7
```

### Multi-Environment Configuration

#### Development Environment

```yaml
# config/environments/development.yaml
valkey:
  host: localhost
  port: 6379
  db: 1

workload:
  n_threads: 2
  n_clients_per_thread: 10
  batch_size: 100

monitoring:
  sampling_interval: 1.0
  collection_enabled: true
  export_format: json
```

#### Staging Environment

```yaml
# config/environments/staging.yaml
valkey:
  host: staging-valkey.internal
  port: 6379
  password: "${STAGING_VALKEY_PASSWORD}"
  ssl: true

workload:
  n_threads: 8
  n_clients_per_thread: 50
  batch_size: 500

monitoring:
  sampling_interval: 5.0
  export_format: prometheus
  prometheus_port: 8080
```

#### Production Environment

```yaml
# config/environments/production.yaml
valkey:
  cluster_mode: true
  startup_nodes:
    - host: valkey-1.prod.internal
      port: 7001
    - host: valkey-2.prod.internal
      port: 7001
    - host: valkey-3.prod.internal
      port: 7001
  password: "${PRODUCTION_VALKEY_PASSWORD}"
  ssl: true
  max_connections: 2000

workload:
  n_threads: 16
  n_clients_per_thread: 125
  batch_size: 1000
  operation_timeout: 30.0

monitoring:
  sampling_interval: 10.0
  export_format: influxdb
  influxdb_url: "https://influxdb.prod.internal:8086"
  retention_days: 30
```

## Best Practices

### Configuration Management

1. **Use Environment-Specific Configs**
   ```bash
   # Use different configs per environment
   vst run --config config/environments/production.yaml
   ```

2. **Version Control Configurations**
   ```bash
   # Keep configs in version control
   git add config/
   git commit -m "Update production configuration"
   ```

3. **Secure Sensitive Values**
   ```yaml
   # Use environment variables for secrets
   valkey:
     password: "${VALKEY_PASSWORD}"
   ```

4. **Document Configuration Changes**
   ```yaml
   # Add comments explaining configuration choices
   workload:
     n_threads: 16  # Optimized for 16-core production servers
     batch_size: 1000  # Balance between throughput and memory
   ```

### Performance Optimization

1. **Start with Defaults**
   - Begin with default configuration
   - Profile and identify bottlenecks
   - Adjust specific parameters incrementally

2. **Match Hardware Resources**
   ```yaml
   workload:
     n_threads: 8  # Match CPU core count
     n_clients_per_thread: 125  # Adjust based on memory
   ```

3. **Monitor Resource Usage**
   ```yaml
   monitoring:
     memory_metrics: [rss_mb, fragmentation_ratio]
     performance_metrics: [operations_per_second, latency_p95]
   ```

4. **Test Configuration Changes**
   ```bash
   # Test new configuration with small dataset
   vst run quick --config new_config.yaml
   ```

### Troubleshooting Common Issues

#### Connection Issues

```yaml
# Increase timeouts for unreliable networks
valkey:
  socket_timeout: 60.0
  socket_connect_timeout: 30.0
  retry_on_timeout: true
  max_retries: 5
```

#### Memory Issues

```yaml
# Reduce memory usage
workload:
  batch_size: 100  # Smaller batches
  vector_cache_size: 1000  # Smaller cache
  n_clients_per_thread: 50  # Fewer concurrent clients

monitoring:
  memory_warning_threshold: 0.6  # Earlier warnings
```

#### Performance Issues

```yaml
# Optimize for performance
valkey:
  max_connections: 2000  # More connections
  socket_keepalive: true  # Reuse connections

workload:
  batch_size: 2000  # Larger batches
  operation_timeout: 60.0  # More time for operations

index:
  algorithm: FLAT  # Fastest search (if memory allows)
```

## Troubleshooting

### Configuration Validation Errors

**Error: Invalid algorithm 'HNWS'**
```yaml
# Fix typo in algorithm name
index:
  algorithm: HNSW  # Not 'HNWS'
```

**Error: ef_construction must be between 1 and 4096**
```yaml
# Adjust parameter to valid range
index:
  ef_construction: 356  # Not 5000
```

### Connection Errors

**Error: Connection refused**
```bash
# Check Valkey is running
valkey-cli ping

# Verify host and port
vst info valkey --config your_config.yaml
```

**Error: Authentication failed**
```yaml
# Verify password configuration
valkey:
  password: "${VALKEY_PASSWORD}"
```

### Performance Issues

**Low throughput**
1. Increase batch size
2. Add more threads/clients
3. Check network latency
4. Verify Valkey performance

**High memory usage**
1. Reduce batch size
2. Lower client count
3. Enable vector pooling
4. Use smaller cache size

**High latency**
1. Reduce ef_runtime for HNSW
2. Use FLAT algorithm
3. Increase operation timeout
4. Check system resources

### Getting Help

For additional help with configuration:

1. **Check built-in help**
   ```bash
   vst config --help
   vst run --help
   ```

2. **Validate configuration**
   ```bash
   vst config validate --config your_config.yaml
   ```

3. **Check system compatibility**
   ```bash
   vst info system
   vst info valkey
   ```

4. **Enable debug logging**
   ```bash
   vst run --log-level DEBUG --config your_config.yaml
   ```
