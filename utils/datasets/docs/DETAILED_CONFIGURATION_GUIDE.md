# Comprehensive Configuration Guide

This detailed guide provides complete coverage of configuration options for the Search Module Delete Test Testing tool, including advanced setups, multi-environment configurations, and optimization techniques.

## Table of Contents

- [Configuration Fundamentals](#configuration-fundamentals)
- [Core Configuration Sections](#core-configuration-sections)
- [Environment-Specific Configurations](#environment-specific-configurations)
- [Advanced Configuration Patterns](#advanced-configuration-patterns)
- [Performance Tuning](#performance-tuning)
- [Security and Authentication](#security-and-authentication)
- [Monitoring and Observability](#monitoring-and-observability)
- [Configuration Validation and Testing](#configuration-validation-and-testing)
- [Troubleshooting Common Issues](#troubleshooting-common-issues)
- [Best Practices and Patterns](#best-practices-and-patterns)

## Configuration Fundamentals

### Configuration File Structure

The Valkey-search delete stress testing tool uses YAML configuration files with a hierarchical structure:

```yaml
# Main configuration structure
valkey:              # Valkey connection settings
  host: localhost
  port: 6379
  # ... other valkey settings

index:              # Vector index configuration
  algorithm: HNSW
  dimensions: 1536
  # ... other index settings

workload:           # Workload execution settings
  n_threads: 8
  batch_size: 1000
  # ... other workload settings

monitoring:         # Metrics and monitoring
  sampling_interval: 10.0
  # ... other monitoring settings

output:             # Output and logging
  csv_path: output/metrics.csv
  # ... other output settings
```

### Configuration Precedence

Configuration values are resolved in this order (highest to lowest precedence):

1. **Command-line arguments**
   ```bash
   vst run --valkey-host prod-valkey.example.com --batch-size 2000
   ```

2. **Environment variables**
   ```bash
   export VST_VALKEY_HOST=prod-valkey.example.com
   export VST_WORKLOAD_BATCH_SIZE=2000
   ```

3. **Custom configuration file**
   ```bash
   vst run --config production.yaml
   ```

4. **Default configuration**
   ```yaml
   # config/default.yaml (built-in defaults)
   ```

### Environment Variable Mapping

All configuration options can be overridden via environment variables:

```bash
# Valkey settings
export VST_VALKEY_HOST=localhost
export VST_VALKEY_PORT=6379
export VST_VALKEY_PASSWORD=mypassword
export VST_VALKEY_SSL=true

# Index settings
export VST_INDEX_ALGORITHM=HNSW
export VST_INDEX_DIMENSIONS=1536
export VST_INDEX_DISTANCE_METRIC=L2

# Workload settings
export VST_WORKLOAD_N_THREADS=16
export VST_WORKLOAD_BATCH_SIZE=1000
export VST_WORKLOAD_OPERATION_TIMEOUT=30.0

# Monitoring settings
export VST_MONITORING_SAMPLING_INTERVAL=5.0
export VST_MONITORING_COLLECTION_ENABLED=true
```

### Configuration Validation

```bash
# Validate configuration file
vst validate config my_config.yaml

# Show effective configuration
vst config show

# Test Valkey connection
vst info valkey --config my_config.yaml

# Validate and show all sections
vst config validate --config my_config.yaml --detailed
```

## Core Configuration Sections

### Valkey Configuration

#### Basic Connection Settings

```yaml
valkey:
  # Required connection settings
  host: localhost                    # Valkey server hostname or IP
  port: 6379                        # Valkey server port
  db: 0                             # Valkey database number (0-15)
  
  # Authentication (optional)
  password: null                    # Valkey password
  username: null                    # Valkey username (Valkey 6.0+)
  
  # Connection pooling
  max_connections: 1000             # Maximum connections in pool
  max_connections_per_pool: 50      # Max connections per pool instance
  
  # Timeouts and retries
  socket_timeout: 30.0              # Socket operation timeout (seconds)
  socket_connect_timeout: 30.0      # Socket connection timeout (seconds)
  retry_on_timeout: true            # Retry operations on timeout
  max_retries: 3                    # Maximum retry attempts
  
  # Connection health
  health_check_interval: 30         # Health check interval (seconds)
  socket_keepalive: true            # Enable TCP keepalive
  socket_keepalive_options: {}      # TCP keepalive options
```

#### SSL/TLS Configuration

```yaml
valkey:
  # SSL/TLS settings
  ssl: true                         # Enable SSL/TLS
  ssl_cert_reqs: required           # Certificate requirements (required/optional/none)
  ssl_ca_certs: /path/to/ca.pem     # CA certificate file
  ssl_cert_file: /path/to/cert.pem  # Client certificate file
  ssl_key_file: /path/to/key.pem    # Client private key file
  ssl_check_hostname: true          # Verify hostname in certificate
  ssl_ciphers: null                 # Allowed SSL ciphers
  ssl_keyfile_password: null        # Private key password
```

#### Valkey Cluster Configuration

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
  skip_full_coverage_check: false   # Skip cluster coverage check
  max_connections_per_node: 100     # Max connections per cluster node
  readonly_mode: false              # Enable read-only mode
  decode_responses: true            # Decode responses to strings
  cluster_require_full_coverage: true # Require full cluster coverage
```

#### Valkey Sentinel Configuration

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
  service_name: mymaster            # Service name to monitor
  sentinel_kwargs:
    password: sentinel_password     # Sentinel password
    socket_timeout: 0.1            # Sentinel socket timeout
    socket_connect_timeout: 0.1    # Sentinel connect timeout
```

### Index Configuration

#### HNSW Algorithm Settings

```yaml
index:
  # Algorithm selection
  algorithm: HNSW                   # HNSW (recommended) or FLAT
  
  # Vector dimensions and metrics
  dimensions: 1536                  # Vector dimensions (must match data)
  distance_metric: L2               # L2, IP, or COSINE
  
  # HNSW-specific parameters
  m: 16                            # Number of connections (8-64, default: 16)
  ef_construction: 356             # Search width during construction (100-1000)
  ef_runtime: 200                  # Search width during queries (10-1000)
  
  # Memory and performance
  initial_cap: 10000               # Initial capacity hint
  block_size: 1024                 # Block size for memory allocation
  max_elements: null               # Maximum elements (null = unlimited)
  
  # Advanced options
  seed: 42                         # Random seed for reproducibility
  allow_replace_deleted: true      # Allow replacing deleted elements
```

#### FLAT Algorithm Settings

```yaml
index:
  # Flat (brute-force) algorithm
  algorithm: FLAT
  dimensions: 1536
  distance_metric: L2
  
  # FLAT-specific settings
  block_size: 4096                 # Larger blocks for better throughput
  use_mmap: false                  # Use memory mapping for large datasets
  normalize_vectors: false         # Auto-normalize vectors
```

#### Distance Metrics

```yaml
index:
  # L2 (Euclidean) distance - most common
  distance_metric: L2
  # Best for: General purpose, normalized vectors
  # Formula: sqrt(sum((a_i - b_i)^2))
  
  # Inner Product - for similarity scoring
  distance_metric: IP
  # Best for: Similarity scoring, recommendation systems
  # Formula: sum(a_i * b_i)
  
  # Cosine similarity - for angular similarity
  distance_metric: COSINE
  # Best for: Text embeddings, angular similarity
  # Formula: 1 - (dot(a,b) / (norm(a) * norm(b)))
```

### Workload Configuration

#### Concurrency and Threading

```yaml
workload:
  # Thread configuration
  n_threads: 8                     # Number of worker threads
  n_clients_per_thread: 125        # Clients per thread
  total_clients: 1000              # Alternative: specify total directly
  
  # Thread affinity (advanced)
  thread_affinity: false           # Pin threads to CPU cores
  thread_priority: normal          # Thread priority (low/normal/high)
  
  # Async processing
  async_mode: false                # Enable async operations
  async_pool_size: 100             # Async operation pool size
```

#### Batch Processing

```yaml
workload:
  # Batch size configuration
  batch_size: 1000                 # Operations per batch
  max_batch_size: 5000             # Maximum batch size limit
  min_batch_size: 10               # Minimum batch size limit
  adaptive_batching: false         # Enable adaptive batch sizing
  
  # Batch optimization
  batch_timeout: 1.0               # Batch assembly timeout (seconds)
  batch_compression: false         # Compress batch data
  batch_validation: true           # Validate batch operations
```

#### Operation Settings

```yaml
workload:
  # Timeout and retry configuration
  operation_timeout: 30.0          # Operation timeout (seconds)
  max_retries: 3                   # Maximum retry attempts
  retry_delay: 1.0                 # Base retry delay (seconds)
  exponential_backoff: true        # Use exponential backoff
  jitter: true                     # Add random jitter to retries
  
  # Rate limiting
  max_operations_per_second: null  # Rate limit (null = unlimited)
  burst_capacity: 1000             # Burst operation capacity
  rate_limiter_algorithm: token_bucket # token_bucket or sliding_window
```

#### Memory Management

```yaml
workload:
  # Memory optimization
  vector_cache_size: 10000         # Number of vectors to cache
  enable_vector_pooling: true      # Reuse vector objects
  gc_frequency: 1000               # Garbage collection frequency
  
  # Resource limits
  max_memory_usage_mb: 8192        # Maximum memory usage
  memory_warning_threshold: 0.8    # Warning at 80% usage
  memory_check_interval: 30        # Memory check interval (seconds)
```

### Monitoring Configuration

#### Metrics Collection

```yaml
monitoring:
  # Collection settings
  collection_enabled: true         # Enable metrics collection
  sampling_interval: 10.0          # Collection interval (seconds)
  buffer_size: 1000               # Metrics buffer size
  
  # Memory metrics
  memory_metrics:
    - rss_mb                      # Resident Set Size
    - vms_mb                      # Virtual Memory Size
    - active_mb                   # Active memory
    - resident_mb                 # Resident memory
    - allocated_mb                # Allocated memory
    - fragmentation_ratio         # Memory fragmentation
    - peak_allocated_mb           # Peak allocated memory
    - cache_hit_ratio             # Cache hit ratio
    - swap_mb                     # Swap memory usage
  
  # Performance metrics
  performance_metrics:
    - operations_per_second       # Throughput
    - latency_p50                 # 50th percentile latency
    - latency_p95                 # 95th percentile latency
    - latency_p99                 # 99th percentile latency
    - latency_max                 # Maximum latency
    - error_rate                  # Error percentage
    - connection_count            # Active connections
    - queue_depth                 # Operation queue depth
    - cpu_usage_percent           # CPU utilization
    - disk_io_mb                  # Disk I/O in MB
```

#### Export Configuration

```yaml
monitoring:
  # Export formats and destinations
  export_format: json             # json, csv, prometheus, influxdb
  export_interval: 60             # Export interval (seconds)
  
  # Output destinations
  output_file: metrics.json       # Local file output
  prometheus_port: 8080           # Prometheus metrics port
  prometheus_path: /metrics       # Prometheus metrics path
  influxdb_url: null              # InfluxDB endpoint
  influxdb_database: valkey_metrics # InfluxDB database name
  influxdb_username: null         # InfluxDB username
  influxdb_password: null         # InfluxDB password
  
  # Retention and rotation
  retention_days: 7               # Days to retain metrics
  max_file_size_mb: 100          # Maximum output file size
  rotate_files: true              # Enable file rotation
  compress_old_files: true        # Compress rotated files
```

## Environment-Specific Configurations

### Development Environment

```yaml
# config/environments/development.yaml
# Optimized for local development and debugging

valkey:
  host: localhost
  port: 6379
  db: 1                           # Use separate database
  max_connections: 50             # Lower connection count
  socket_timeout: 10.0            # Shorter timeouts for quick feedback

index:
  algorithm: HNSW
  dimensions: 1536
  m: 8                           # Lower M for faster builds
  ef_construction: 100           # Faster construction
  ef_runtime: 50                 # Faster search

workload:
  n_threads: 2                   # Conservative for development
  n_clients_per_thread: 10       # Lower concurrency
  batch_size: 100                # Smaller batches
  operation_timeout: 10.0        # Quick timeouts
  max_retries: 2                 # Fewer retries

monitoring:
  collection_enabled: true
  sampling_interval: 1.0         # Frequent sampling for debugging
  memory_metrics:
    - rss_mb
    - allocated_mb
    - fragmentation_ratio
  performance_metrics:
    - operations_per_second
    - latency_p95
  export_format: json
  output_file: debug/metrics.json

output:
  log_level: DEBUG               # Verbose logging
  csv_path: debug/metrics.csv
  summary_path: debug/summary.csv
  enable_detailed_logging: true
```

### Staging Environment

```yaml
# config/environments/staging.yaml
# Production-like environment for testing

valkey:
  host: staging-valkey.internal
  port: 6379
  password: "${STAGING_VALKEY_PASSWORD}"
  ssl: true
  ssl_cert_reqs: required
  max_connections: 500

index:
  algorithm: HNSW
  dimensions: 1536
  m: 16
  ef_construction: 200
  ef_runtime: 100

workload:
  n_threads: 8
  n_clients_per_thread: 50
  batch_size: 500
  operation_timeout: 30.0
  max_retries: 3

monitoring:
  collection_enabled: true
  sampling_interval: 5.0
  export_format: prometheus
  prometheus_port: 8080
  memory_metrics:
    - rss_mb
    - active_mb
    - fragmentation_ratio
    - peak_allocated_mb
  performance_metrics:
    - operations_per_second
    - latency_p50
    - latency_p95
    - latency_p99
    - error_rate

output:
  log_level: INFO
  csv_path: staging/metrics.csv
  summary_path: staging/summary.csv
```

### Production Environment

```yaml
# config/environments/production.yaml
# Optimized for production workloads

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
  ssl_cert_reqs: required
  max_connections: 2000
  max_connections_per_node: 100
  socket_timeout: 30.0
  socket_connect_timeout: 30.0
  health_check_interval: 30
  retry_on_timeout: true

index:
  algorithm: HNSW
  dimensions: 1536
  distance_metric: L2
  m: 16
  ef_construction: 400           # Higher quality
  ef_runtime: 200
  initial_cap: 100000
  block_size: 4096

workload:
  n_threads: 16                  # Match production server cores
  n_clients_per_thread: 125
  batch_size: 1000
  operation_timeout: 60.0
  max_retries: 3
  retry_delay: 2.0
  exponential_backoff: true
  jitter: true

monitoring:
  collection_enabled: true
  sampling_interval: 10.0
  buffer_size: 10000
  memory_metrics:
    - rss_mb
    - active_mb
    - resident_mb
    - allocated_mb
    - fragmentation_ratio
    - peak_allocated_mb
    - cache_hit_ratio
  performance_metrics:
    - operations_per_second
    - latency_p50
    - latency_p95
    - latency_p99
    - latency_max
    - error_rate
    - connection_count
  export_format: influxdb
  influxdb_url: "https://influxdb.prod.internal:8086"
  influxdb_database: valkey_metrics
  influxdb_username: "${INFLUXDB_USERNAME}"
  influxdb_password: "${INFLUXDB_PASSWORD}"
  retention_days: 30

output:
  log_level: WARN                # Minimal logging in production
  csv_path: /var/log/valkey/metrics.csv
  summary_path: /var/log/valkey/summary.csv
  max_log_size_mb: 100
  log_rotation: true

# Resource limits for production
resource_limits:
  max_memory_usage_mb: 16384     # 16GB limit
  memory_warning_threshold: 0.8
  max_cpu_usage_percent: 80
  disk_warning_threshold_percent: 85
```

## Advanced Configuration Patterns

### High-Performance Configuration

```yaml
# config/high_performance.yaml
# Optimized for maximum throughput

valkey:
  max_connections: 5000
  socket_keepalive: true
  retry_on_timeout: false         # Fail fast for high throughput
  socket_timeout: 5.0            # Short timeouts

workload:
  n_threads: 32                  # High thread count
  n_clients_per_thread: 156      # Many clients
  batch_size: 2000               # Large batches
  operation_timeout: 10.0
  
  # Disable retries for maximum speed
  max_retries: 0
  
  # Memory optimizations
  vector_cache_size: 50000
  enable_vector_pooling: true
  gc_frequency: 5000

index:
  algorithm: FLAT                # Fastest search (if memory allows)
  block_size: 8192              # Large blocks for throughput
  
monitoring:
  sampling_interval: 1.0         # Frequent monitoring
  buffer_size: 50000            # Large buffer
  
  # Focus on throughput metrics
  performance_metrics:
    - operations_per_second
    - latency_p50
    - latency_p95
    - queue_depth
    - cpu_usage_percent
```

### Low-Latency Configuration

```yaml
# config/low_latency.yaml
# Optimized for minimum latency

valkey:
  socket_timeout: 1.0
  socket_connect_timeout: 1.0
  max_connections: 100           # Fewer connections for lower overhead

workload:
  n_threads: 4
  n_clients_per_thread: 25       # Lower concurrency
  batch_size: 1                  # Single operations for lowest latency
  operation_timeout: 1.0
  
  # Aggressive retry settings
  max_retries: 1
  retry_delay: 0.1
  
  # Memory settings
  vector_cache_size: 1000
  gc_frequency: 100

index:
  algorithm: HNSW
  ef_runtime: 50                 # Lower for faster search
  m: 8                          # Lower M for speed
  ef_construction: 100

monitoring:
  sampling_interval: 0.1         # Very frequent sampling
  
  # Focus on latency metrics
  performance_metrics:
    - latency_p50
    - latency_p95
    - latency_p99
    - latency_max
    - operations_per_second
```

### Memory-Optimized Configuration

```yaml
# config/memory_optimized.yaml
# Minimizes memory usage

valkey:
  max_connections: 50            # Fewer connections

workload:
  n_threads: 2
  n_clients_per_thread: 25
  batch_size: 100               # Smaller batches
  
  # Memory management
  vector_cache_size: 1000       # Small cache
  enable_vector_pooling: true
  gc_frequency: 100             # Frequent GC
  max_memory_usage_mb: 2048     # 2GB limit
  memory_warning_threshold: 0.7 # Early warnings

index:
  algorithm: HNSW
  m: 8                          # Lower memory usage
  initial_cap: 1000
  block_size: 256               # Smaller blocks

monitoring:
  buffer_size: 100              # Small buffer
  memory_warning_threshold: 0.7
  
  # Focus on memory metrics
  memory_metrics:
    - rss_mb
    - allocated_mb
    - fragmentation_ratio
    - peak_allocated_mb
    - cache_hit_ratio
```

## Performance Tuning

### CPU Optimization

```yaml
# Optimize for CPU-bound workloads
workload:
  n_threads: 16                  # Match physical CPU cores
  thread_affinity: true          # Pin threads to cores
  thread_priority: high          # Higher thread priority
  
  # Reduce context switching
  batch_size: 2000
  n_clients_per_thread: 100

index:
  algorithm: HNSW
  # Optimize for CPU cache
  block_size: 4096              # Cache-friendly block size
  
monitoring:
  # Monitor CPU usage
  performance_metrics:
    - cpu_usage_percent
    - operations_per_second
    - latency_p95
```

### Memory Optimization

```yaml
# Optimize for memory-constrained environments
workload:
  # Reduce memory footprint
  vector_cache_size: 5000
  enable_vector_pooling: true
  gc_frequency: 500
  
  # Memory limits
  max_memory_usage_mb: 4096
  memory_warning_threshold: 0.8

index:
  # Memory-efficient settings
  m: 8
  initial_cap: 10000
  block_size: 1024

monitoring:
  # Comprehensive memory monitoring
  memory_metrics:
    - rss_mb
    - vms_mb
    - allocated_mb
    - fragmentation_ratio
    - peak_allocated_mb
    - cache_hit_ratio
    - swap_mb
```

### Network Optimization

```yaml
# Optimize for network-bound workloads
valkey:
  # Connection pooling
  max_connections: 1000
  socket_keepalive: true
  socket_keepalive_options:
    TCP_KEEPIDLE: 600
    TCP_KEEPINTVL: 30
    TCP_KEEPCNT: 3
  
  # Buffer sizes
  socket_buffer_size: 65536
  
workload:
  # Batch operations to reduce network round trips
  batch_size: 1000
  
  # Connection reuse
  connection_pool_size: 100
  connection_reuse: true

monitoring:
  # Network metrics
  performance_metrics:
    - operations_per_second
    - latency_p95
    - connection_count
    - network_bytes_sent
    - network_bytes_recv
```

## Security and Authentication

### Basic Authentication

```yaml
valkey:
  # Username/password authentication
  username: valkey_user
  password: "${VALKEY_PASSWORD}"    # Use environment variable
  
  # Database isolation
  db: 5                           # Use dedicated database
```

### SSL/TLS Configuration

```yaml
valkey:
  # Enable SSL/TLS
  ssl: true
  ssl_cert_reqs: required
  
  # Certificate files
  ssl_ca_certs: /etc/ssl/certs/valkey-ca.pem
  ssl_cert_file: /etc/ssl/certs/valkey-client.pem
  ssl_key_file: /etc/ssl/private/valkey-client.key
  ssl_keyfile_password: "${SSL_KEY_PASSWORD}"
  
  # SSL options
  ssl_check_hostname: true
  ssl_ciphers: "ECDHE+AESGCM:ECDHE+CHACHA20:DHE+AESGCM:DHE+CHACHA20:!aNULL:!MD5:!DSS"
  ssl_protocol_version: "TLSv1.2"
```

### Advanced Security

```yaml
valkey:
  # Connection security
  ssl: true
  ssl_cert_reqs: required
  ssl_check_hostname: true
  
  # Authentication
  username: "${VALKEY_USERNAME}"
  password: "${VALKEY_PASSWORD}"
  
  # Access control
  db: 0
  
  # Network security
  socket_timeout: 30.0
  socket_connect_timeout: 10.0
  
# Additional security measures
security:
  # Audit logging
  audit_logging: true
  audit_log_path: /var/log/valkey/audit.log
  
  # Rate limiting
  rate_limiting: true
  max_requests_per_minute: 10000
  
  # IP restrictions
  allowed_ips:
    - 10.0.0.0/8
    - 192.168.0.0/16
```

## Configuration Validation and Testing

### Validation Commands

```bash
# Basic validation
vst validate config my_config.yaml

# Detailed validation with warnings
vst validate config my_config.yaml --strict

# Validate configuration and test connectivity
vst validate config my_config.yaml --test-connection

# Show effective configuration
vst config show --config my_config.yaml

# Compare configurations
vst config diff config1.yaml config2.yaml
```

### Automated Testing

```yaml
# config/test_config.yaml
# Configuration for automated testing

valkey:
  host: localhost
  port: 6379
  db: 15                          # Use highest database for testing
  
workload:
  n_threads: 1                    # Single thread for predictable testing
  batch_size: 10                  # Small batches
  operation_timeout: 5.0          # Short timeout for fast failures

monitoring:
  collection_enabled: true
  sampling_interval: 0.1          # Frequent sampling for tests
  export_format: json
  output_file: tests/metrics.json

output:
  log_level: DEBUG
  csv_path: tests/test_metrics.csv
```

### Configuration Templates

Create reusable configuration templates:

```yaml
# config/templates/base.yaml
# Base configuration template

valkey: &valkey_defaults
  host: localhost
  port: 6379
  max_connections: 1000
  socket_timeout: 30.0

index: &index_defaults
  algorithm: HNSW
  dimensions: 1536
  distance_metric: L2
  m: 16
  ef_construction: 356
  ef_runtime: 200

workload: &workload_defaults
  n_threads: 8
  n_clients_per_thread: 125
  batch_size: 1000
  operation_timeout: 30.0

monitoring: &monitoring_defaults
  collection_enabled: true
  sampling_interval: 10.0
  export_format: json

# Compose configurations from templates
valkey: *valkey_defaults
index: *index_defaults
workload: *workload_defaults
monitoring: *monitoring_defaults
```

## Troubleshooting Common Issues

### Connection Issues

**Problem**: Connection refused or timeout errors

```yaml
# Solutions
valkey:
  # Increase timeouts
  socket_timeout: 60.0
  socket_connect_timeout: 30.0
  retry_on_timeout: true
  max_retries: 5
  
  # Connection pooling
  max_connections: 500
  health_check_interval: 30
```

**Problem**: SSL certificate errors

```yaml
valkey:
  ssl: true
  ssl_cert_reqs: required
  ssl_check_hostname: true
  
  # Verify certificate paths
  ssl_ca_certs: /correct/path/to/ca.pem
  ssl_cert_file: /correct/path/to/cert.pem
  ssl_key_file: /correct/path/to/key.pem
```

### Performance Issues

**Problem**: Low throughput

```yaml
# Solutions
workload:
  # Increase concurrency
  n_threads: 16
  n_clients_per_thread: 200
  
  # Larger batches
  batch_size: 2000
  
  # Optimize timeouts
  operation_timeout: 60.0

valkey:
  # More connections
  max_connections: 2000
```

**Problem**: High latency

```yaml
# Solutions
workload:
  # Reduce concurrency
  n_threads: 4
  n_clients_per_thread: 50
  
  # Smaller batches
  batch_size: 100
  
  # Shorter timeouts
  operation_timeout: 10.0

index:
  # Faster search
  ef_runtime: 50
```

### Memory Issues

**Problem**: Out of memory errors

```yaml
# Solutions
workload:
  # Memory limits
  max_memory_usage_mb: 8192
  memory_warning_threshold: 0.8
  
  # Smaller caches
  vector_cache_size: 5000
  
  # More frequent GC
  gc_frequency: 500
  
  # Smaller batches
  batch_size: 500
```

### Configuration Errors

**Problem**: Invalid configuration values

```bash
# Debug configuration
vst validate config my_config.yaml --verbose

# Show current configuration
vst config show --config my_config.yaml

# Test specific sections
vst validate config my_config.yaml --section valkey
vst validate config my_config.yaml --section workload
```

## Best Practices and Patterns

### Configuration Management

1. **Use Environment-Specific Configs**
   ```bash
   # Development
   vst run --config config/environments/development.yaml
   
   # Staging
   vst run --config config/environments/staging.yaml
   
   # Production
   vst run --config config/environments/production.yaml
   ```

2. **Environment Variables for Secrets**
   ```yaml
   valkey:
     password: "${VALKEY_PASSWORD}"
     ssl_key_file: "${SSL_KEY_PATH}"
   ```

3. **Configuration Validation in CI/CD**
   ```bash
   # In CI pipeline
   vst validate config config/production.yaml --strict
   vst config test --config config/production.yaml
   ```

### Performance Optimization

1. **Start with Defaults**
   - Begin with default configuration
   - Profile and identify bottlenecks
   - Adjust specific parameters incrementally

2. **Match Hardware Resources**
   ```yaml
   workload:
     n_threads: 8              # Match CPU core count
     n_clients_per_thread: 125 # Adjust based on memory
   ```

3. **Monitor Resource Usage**
   ```yaml
   monitoring:
     memory_metrics: [rss_mb, fragmentation_ratio]
     performance_metrics: [operations_per_second, latency_p95]
   ```

### Security Best Practices

1. **Use Strong Authentication**
   ```yaml
   valkey:
     username: "${VALKEY_USERNAME}"
     password: "${VALKEY_PASSWORD}"
   ```

2. **Enable SSL/TLS in Production**
   ```yaml
   valkey:
     ssl: true
     ssl_cert_reqs: required
     ssl_check_hostname: true
   ```

3. **Secure Configuration Files**
   ```bash
   # Set appropriate permissions
   chmod 600 config/production.yaml
   chown app:app config/production.yaml
   ```

### Monitoring and Observability

1. **Comprehensive Metrics Collection**
   ```yaml
   monitoring:
     memory_metrics: [rss_mb, allocated_mb, fragmentation_ratio]
     performance_metrics: [operations_per_second, latency_p95, error_rate]
   ```

2. **Export to External Systems**
   ```yaml
   monitoring:
     export_format: prometheus
     prometheus_port: 8080
   ```

3. **Set Up Alerts**
   ```yaml
   monitoring:
     memory_warning_threshold: 0.8
     latency_warning_threshold: 100.0
     error_rate_threshold: 0.05
   ```

This comprehensive configuration guide provides detailed coverage of all configuration aspects, from basic setups to advanced production deployments, helping users optimize their Valkey-search delete stress testing environments for any scenario.
