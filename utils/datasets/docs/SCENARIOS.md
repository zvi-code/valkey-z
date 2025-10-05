# Scenario Guide

This comprehensive guide covers creating, configuring, and managing test scenarios for the Search Module Delete Test Testing tool. Scenarios define complex test workflows that simulate real-world usage patterns.

## Table of Contents

- [Overview](#overview)
- [Scenario Structure](#scenario-structure)
- [Built-in Scenarios](#built-in-scenarios)
- [Creating Custom Scenarios](#creating-custom-scenarios)
- [Workload Types](#workload-types)
- [Step Types](#step-types)
- [Global Configuration](#global-configuration)
- [Advanced Patterns](#advanced-patterns)
- [Scenario Validation](#scenario-validation)
- [Best Practices](#best-practices)
- [Troubleshooting](#troubleshooting)

## Overview

Scenarios are YAML files that define multi-step test workflows. They allow you to:

- **Simulate Real Workloads**: Model production usage patterns
- **Test Memory Patterns**: Analyze growth, shrinkage, and fragmentation
- **Validate Performance**: Measure system behavior under different loads
- **Automate Testing**: Create repeatable test suites

### Scenario Components

A scenario consists of:
- **Metadata**: Name, description, dataset reference
- **Global Config**: Default settings for all steps  
- **Steps**: Ordered list of operations to execute
- **Parameters**: Step-specific configuration

## Scenario Structure

### Basic Structure

```yaml
# Basic scenario structure
name: scenario_name
description: Human-readable description of the scenario

# Dataset to use (optional, can be specified per step)
dataset: openai-5m

# Global configuration applied to all steps
global_config:
  n_threads: 8
  n_clients: 1000
  batch_size: 1000

# Ordered list of steps to execute
steps:
  - name: step_1
    type: workload
    workload: ingest
    parameters:
      target_vectors: 1000000
  
  - name: step_2  
    type: checkpoint
    parameters:
      collect_full_metrics: true
```

### Complete Example

```yaml
# Example: Memory growth analysis scenario
name: memory_growth_analysis
description: |
  Analyzes memory usage patterns during data ingestion and querying.
  Tests memory allocation, fragmentation, and cleanup behavior.

dataset: openai-5m
timeout_seconds: 3600

global_config:
  n_threads: 8
  n_clients: 1000
  batch_size: 1000
  operation_timeout: 30.0

metadata:
  author: Data Team
  version: "1.2"
  tags: [memory, performance, production]
  requirements:
    min_memory_gb: 8
    min_valkey_version: "7.0"

steps:
  # Initial data load
  - name: initial_load
    type: workload
    workload: ingest
    description: Load initial dataset
    parameters:
      target_vectors: 500000
      
  - name: memory_checkpoint_1
    type: checkpoint
    description: Capture baseline memory metrics
    parameters:
      collect_full_metrics: true
      force_gc: true
      
  # Query workload
  - name: query_phase
    type: workload  
    workload: query
    duration_seconds: 300
    description: Mixed query workload
    parameters:
      query_k: 10
      queries_per_second: 100
      
  # Additional data growth
  - name: growth_phase
    type: workload
    workload: ingest
    description: Grow dataset to test memory scaling
    parameters:
      target_vectors: 1000000
      
  - name: memory_checkpoint_2
    type: checkpoint
    description: Capture post-growth metrics
    parameters:
      collect_full_metrics: true
      compare_with: memory_checkpoint_1
      
  # Cleanup and shrinkage
  - name: shrink_phase
    type: workload
    workload: shrink
    description: Remove vectors to test memory cleanup
    parameters:
      target_vectors: 250000
      
  - name: final_checkpoint
    type: checkpoint
    description: Final memory analysis
    parameters:
      collect_full_metrics: true
      generate_report: true
```

## Built-in Scenarios

The tool comes with several pre-built scenarios for common testing patterns.

### Continuous Growth

Tests memory behavior during continuous data growth.

```yaml
# config/scenarios/continuous_growth.yaml
name: continuous_growth
description: Test memory patterns with continuous data growth

dataset: openai-5m

global_config:
  n_threads: 8
  n_clients: 1000
  batch_size: 1000

steps:
  - name: initial_load
    type: workload
    workload: ingest
    parameters:
      target_vectors: 1000000
      
  - name: growth_phase_1
    type: workload
    workload: ingest
    parameters:
      target_vectors: 2000000
      
  - name: checkpoint_1
    type: checkpoint
    parameters:
      collect_full_metrics: true
      
  - name: growth_phase_2
    type: workload
    workload: ingest
    parameters:
      target_vectors: 3000000
      
  - name: checkpoint_2
    type: checkpoint
    parameters:
      collect_full_metrics: true
      
  - name: growth_phase_3
    type: workload
    workload: ingest
    parameters:
      target_vectors: 5000000
      
  - name: final_checkpoint
    type: checkpoint
    parameters:
      collect_full_metrics: true
      generate_report: true
```

### Grow-Shrink-Grow

Tests memory cleanup and fragmentation patterns.

```yaml
# config/scenarios/grow_shrink_grow.yaml
name: grow_shrink_grow
description: Test memory allocation, cleanup, and fragmentation patterns

dataset: openai-5m

global_config:
  n_threads: 8
  n_clients: 1000
  batch_size: 1000

steps:
  # Initial growth phase
  - name: initial_growth
    type: workload
    workload: ingest
    parameters:
      target_vectors: 2000000
      
  - name: growth_checkpoint
    type: checkpoint
    parameters:
      collect_full_metrics: true
      
  # Shrink phase
  - name: shrink_phase
    type: workload
    workload: shrink
    parameters:
      target_vectors: 500000
      
  - name: shrink_checkpoint
    type: checkpoint
    parameters:
      collect_full_metrics: true
      
  # Second growth phase  
  - name: regrowth_phase
    type: workload
    workload: ingest
    parameters:
      target_vectors: 3000000
      
  - name: final_checkpoint
    type: checkpoint
    parameters:
      collect_full_metrics: true
      analyze_fragmentation: true
```

### Mixed Workload

Simulates realistic production workloads with mixed operations.

```yaml
# config/scenarios/mixed_workload.yaml
name: mixed_workload
description: Mixed read/write workload simulating production usage

dataset: openai-5m

global_config:
  n_threads: 8
  n_clients: 1000
  batch_size: 500

steps:
  # Initial data load
  - name: bootstrap
    type: workload
    workload: ingest
    parameters:
      target_vectors: 1000000
      
  # Mixed operations phase 1
  - name: mixed_phase_1
    type: workload
    workload: mixed
    duration_seconds: 600
    parameters:
      ingest_ratio: 0.3
      query_ratio: 0.6
      update_ratio: 0.1
      query_k: 10
      
  - name: checkpoint_1
    type: checkpoint
    parameters:
      collect_full_metrics: true
      
  # High query load
  - name: query_burst
    type: workload
    workload: query
    duration_seconds: 300
    parameters:
      query_k: 10
      queries_per_second: 500
      
  # Mixed operations phase 2
  - name: mixed_phase_2
    type: workload
    workload: mixed
    duration_seconds: 600
    parameters:
      ingest_ratio: 0.1
      query_ratio: 0.8
      update_ratio: 0.1
      query_k: 20
      
  - name: final_checkpoint
    type: checkpoint
    parameters:
      collect_full_metrics: true
      generate_performance_report: true
```

## Creating Custom Scenarios

### Step-by-Step Guide

1. **Define Scenario Metadata**
   ```yaml
   name: my_custom_scenario
   description: Custom scenario for specific testing needs
   dataset: my_dataset
   ```

2. **Set Global Configuration**
   ```yaml
   global_config:
     n_threads: 4
     n_clients: 500  
     batch_size: 1000
     operation_timeout: 30.0
   ```

3. **Add Steps**
   ```yaml
   steps:
     - name: setup_phase
       type: workload
       workload: ingest
       parameters:
         target_vectors: 100000
   ```

4. **Validate and Test**
   ```bash
   vst scenario validate --scenario my_scenario.yaml
   vst run --scenario my_scenario.yaml --dry-run
   ```

### Custom Scenario Examples

#### Performance Regression Testing

```yaml
# scenarios/performance_regression.yaml
name: performance_regression
description: Baseline performance testing for regression detection

dataset: openai-1m

global_config:
  n_threads: 8
  n_clients: 1000
  batch_size: 1000

steps:
  # Baseline load
  - name: baseline_load
    type: workload
    workload: ingest
    parameters:
      target_vectors: 1000000
      
  # Performance baseline
  - name: baseline_queries
    type: workload
    workload: query
    duration_seconds: 180
    parameters:
      query_k: 10
      record_latencies: true
      
  - name: baseline_checkpoint
    type: checkpoint
    parameters:
      collect_full_metrics: true
      tag: baseline
      
  # Load additional data
  - name: scale_load
    type: workload
    workload: ingest  
    parameters:
      target_vectors: 2000000
      
  # Performance at scale
  - name: scale_queries
    type: workload
    workload: query
    duration_seconds: 180
    parameters:
      query_k: 10
      record_latencies: true
      
  - name: scale_checkpoint
    type: checkpoint
    parameters:
      collect_full_metrics: true
      tag: scale
      compare_with: baseline_checkpoint
```

#### Memory Leak Detection

```yaml
# scenarios/memory_leak_detection.yaml
name: memory_leak_detection
description: Long-running test to detect memory leaks

dataset: openai-5m

global_config:
  n_threads: 4
  n_clients: 200
  batch_size: 500

steps:
  # Initial stable state
  - name: initial_load
    type: workload
    workload: ingest
    parameters:
      target_vectors: 500000
      
  - name: initial_checkpoint
    type: checkpoint
    parameters:
      collect_full_metrics: true
      
  # Repeated cycles to detect leaks
  - name: cycle_1
    type: workload
    workload: mixed
    duration_seconds: 1800  # 30 minutes
    parameters:
      ingest_ratio: 0.2
      query_ratio: 0.7
      update_ratio: 0.1
      
  - name: checkpoint_1
    type: checkpoint
    parameters:
      collect_full_metrics: true
      
  - name: cycle_2
    type: workload
    workload: mixed
    duration_seconds: 1800
    parameters:
      ingest_ratio: 0.2
      query_ratio: 0.7
      update_ratio: 0.1
      
  - name: checkpoint_2
    type: checkpoint
    parameters:
      collect_full_metrics: true
      analyze_memory_trends: true
      
  - name: cycle_3
    type: workload
    workload: mixed
    duration_seconds: 1800
    parameters:
      ingest_ratio: 0.2
      query_ratio: 0.7
      update_ratio: 0.1
      
  - name: final_checkpoint
    type: checkpoint
    parameters:
      collect_full_metrics: true
      detect_memory_leaks: true
```

## Workload Types

### Ingest Workload

Adds vectors to the index.

```yaml
steps:
  - name: data_ingestion
    type: workload
    workload: ingest
    parameters:
      target_vectors: 1000000    # Total vectors to ingest
      batch_size: 1000          # Override global batch size
      start_index: 0            # Starting vector index
      shuffle: false            # Randomize vector order
      validate_ingestion: true  # Verify vectors were added
```

### Query Workload

Performs vector similarity searches.

```yaml
steps:
  - name: search_queries
    type: workload
    workload: query
    duration_seconds: 300       # Run for 5 minutes
    parameters:
      query_k: 10              # Number of results per query
      query_vectors: 1000      # Number of different query vectors
      queries_per_second: 100  # Target query rate
      record_latencies: true   # Record detailed timing
      verify_results: false    # Validate against ground truth
```

### Shrink Workload

Removes vectors from the index.

```yaml
steps:
  - name: data_cleanup
    type: workload
    workload: shrink
    parameters:
      target_vectors: 500000    # Target final vector count
      removal_strategy: random  # random, oldest, newest
      batch_size: 1000         # Vectors per removal batch
      verify_removal: true     # Verify vectors were removed
```

### Mixed Workload

Combines multiple operation types.

```yaml
steps:
  - name: production_simulation
    type: workload
    workload: mixed
    duration_seconds: 600
    parameters:
      ingest_ratio: 0.3        # 30% ingest operations
      query_ratio: 0.6         # 60% query operations  
      update_ratio: 0.1        # 10% update operations
      query_k: 10
      ingest_batch_size: 100
      query_batch_size: 50
```

### Custom Workload

Use custom workload implementations.

```yaml
steps:
  - name: custom_operations
    type: workload
    workload: custom_mixed     # Reference to custom workload class
    duration_seconds: 300
    parameters:
      custom_param_1: value1
      custom_param_2: value2
```

## Step Types

### Workload Steps

Execute specific workloads against the system.

```yaml
- name: workload_step
  type: workload
  workload: ingest           # Workload type
  duration_seconds: 300      # Optional: run for specific time
  max_operations: 10000      # Optional: limit operation count
  description: "Load data"   # Optional: step description
  
  # Step-specific configuration
  config:
    n_threads: 4            # Override global setting
    batch_size: 500         # Override global setting
    
  # Workload parameters
  parameters:
    target_vectors: 1000000
```

### Checkpoint Steps

Collect metrics and analyze system state.

```yaml
- name: metrics_collection
  type: checkpoint
  description: "Collect memory metrics"
  parameters:
    collect_full_metrics: true      # Comprehensive metrics
    force_gc: true                 # Force garbage collection
    
    # Analysis options
    analyze_fragmentation: true    # Memory fragmentation analysis
    detect_memory_leaks: false     # Memory leak detection
    generate_report: true          # Generate analysis report
    
    # Comparison options
    compare_with: previous_checkpoint  # Compare with previous checkpoint
    tag: growth_phase              # Tag for identification
```

### Wait Steps

Pause execution for system stabilization.

```yaml
- name: stabilization_wait
  type: wait
  parameters:
    duration_seconds: 60          # Wait duration
    reason: "Memory stabilization" # Optional: reason for wait
    
    # Conditional waiting
    wait_for_memory_stable: true  # Wait until memory usage stabilizes
    memory_stability_window: 30   # Seconds of stable memory
    max_wait_seconds: 300        # Maximum wait time
```

### Cleanup Steps

Reset or clean system state.

```yaml
- name: index_cleanup
  type: cleanup
  parameters:
    cleanup_type: full           # full, partial, index_only
    preserve_connections: true   # Keep Valkey connections
    force_cleanup: false        # Force immediate cleanup
```

### Conditional Steps

Execute steps based on conditions.

```yaml
- name: conditional_step
  type: conditional
  condition:
    metric: memory_usage_mb
    operator: greater_than
    value: 4096
  
  # Steps to execute if condition is true
  if_true:
    - name: memory_cleanup
      type: cleanup
      parameters:
        cleanup_type: partial
        
  # Steps to execute if condition is false  
  if_false:
    - name: continue_load
      type: workload
      workload: ingest
      parameters:
        target_vectors: 2000000
```

## Global Configuration

Global configuration provides default settings for all steps in a scenario.

### Basic Global Config

```yaml
global_config:
  # Concurrency settings
  n_threads: 8
  n_clients: 1000
  n_clients_per_thread: 125
  
  # Batch processing
  batch_size: 1000
  max_batch_size: 5000
  
  # Timeouts
  operation_timeout: 30.0
  step_timeout: 1800
  
  # Error handling
  max_retries: 3
  retry_delay: 1.0
  continue_on_error: false
```

### Advanced Global Config

```yaml
global_config:
  # Resource management
  max_memory_usage_mb: 8192
  memory_warning_threshold: 0.8
  
  # Connection settings
  valkey_pool_size: 100
  connection_timeout: 30.0
  
  # Monitoring
  metrics_collection_interval: 10.0
  enable_detailed_logging: false
  
  # Performance optimization
  enable_vector_caching: true
  vector_cache_size: 10000
  use_connection_pooling: true
  
  # Quality settings
  verify_operations: false
  record_operation_times: true
  detailed_error_reporting: true
```

### Step-Level Overrides

Individual steps can override global configuration:

```yaml
global_config:
  n_threads: 8
  batch_size: 1000

steps:
  - name: high_throughput_step
    type: workload
    workload: ingest
    config:
      n_threads: 16        # Override: use more threads
      batch_size: 2000     # Override: larger batches
    parameters:
      target_vectors: 1000000
      
  - name: low_latency_step
    type: workload
    workload: query
    config:
      n_threads: 4         # Override: fewer threads
      batch_size: 1        # Override: single operations
    parameters:
      query_k: 10
```

## Advanced Patterns

### Parallel Step Execution

Execute multiple steps concurrently:

```yaml
steps:
  - name: parallel_operations
    type: parallel
    steps:
      - name: ingest_thread
        type: workload
        workload: ingest
        parameters:
          target_vectors: 500000
          
      - name: query_thread
        type: workload
        workload: query
        duration_seconds: 300
        parameters:
          query_k: 10
          
      - name: monitoring_thread
        type: checkpoint
        interval_seconds: 30
        parameters:
          collect_memory_metrics: true
```

### Loop Patterns

Repeat steps multiple times:

```yaml
steps:
  - name: load_test_loop
    type: loop
    iterations: 5
    steps:
      - name: load_iteration
        type: workload
        workload: ingest
        parameters:
          target_vectors: 200000
          
      - name: check_iteration  
        type: checkpoint
        parameters:
          collect_full_metrics: true
          tag: "iteration_${loop_index}"
```

### Conditional Execution

Execute steps based on runtime conditions:

```yaml
steps:
  - name: initial_load
    type: workload
    workload: ingest
    parameters:
      target_vectors: 1000000
      
  - name: memory_check
    type: checkpoint
    parameters:
      collect_memory_metrics: true
      
  - name: additional_load
    type: conditional
    condition:
      metric: memory_usage_mb
      operator: less_than
      value: 4096
    if_true:
      - name: more_data
        type: workload
        workload: ingest
        parameters:
          target_vectors: 2000000
```

### Error Handling

Handle step failures gracefully:

```yaml
steps:
  - name: risky_operation
    type: workload
    workload: ingest
    parameters:
      target_vectors: 5000000
    error_handling:
      on_failure: continue    # continue, stop, retry
      max_retries: 3
      retry_delay: 10
      
  - name: cleanup_on_error
    type: cleanup
    trigger: on_previous_error
    parameters:
      cleanup_type: partial
```

## Scenario Validation

### Syntax Validation

Validate scenario YAML syntax and structure:

```bash
# Validate scenario file
vst scenario validate --scenario my_scenario.yaml

# Check for common issues
vst scenario check --scenario my_scenario.yaml --strict

# Dry run to test execution plan
vst run --scenario my_scenario.yaml --dry-run
```

### Runtime Validation

The tool performs runtime validation:

- **Parameter Validation**: Check parameter types and ranges
- **Resource Validation**: Verify sufficient system resources
- **Dependency Validation**: Check dataset and workload availability
- **Logic Validation**: Validate step sequences and dependencies

### Custom Validation Rules

Create custom validation rules:

```yaml
# validation/custom_rules.yaml
validation_rules:
  scenario:
    max_steps: 20
    max_duration_hours: 4
    required_fields: [name, description, steps]
    
  steps:
    workload:
      required_parameters: [target_vectors]
      max_target_vectors: 10000000
      
    checkpoint:
      min_interval_seconds: 10
      
  global_config:
    max_threads: 32
    max_clients: 10000
    min_batch_size: 1
    max_batch_size: 10000
```

## Best Practices

### Scenario Design

1. **Start Simple**
   ```yaml
   # Begin with basic scenarios
   steps:
     - name: simple_load
       type: workload
       workload: ingest
       parameters:
         target_vectors: 100000
   ```

2. **Add Checkpoints**
   ```yaml
   # Include checkpoints for analysis
   steps:
     - name: load_data
       type: workload
       workload: ingest
       parameters:
         target_vectors: 1000000
         
     - name: analyze_memory
       type: checkpoint
       parameters:
         collect_full_metrics: true
   ```

3. **Use Descriptive Names**
   ```yaml
   # Clear, descriptive step names
   steps:
     - name: initial_data_ingestion
       description: "Load baseline dataset for testing"
       type: workload
       workload: ingest
   ```

4. **Document Scenarios**
   ```yaml
   name: production_simulation
   description: |
     Simulates production workload patterns including:
     - Initial data load (1M vectors)
     - Mixed read/write operations
     - Memory analysis checkpoints
     - Performance validation
     
   metadata:
     author: Performance Team
     purpose: regression_testing
     requirements:
       min_memory_gb: 8
       estimated_duration_minutes: 45
   ```

### Performance Optimization

1. **Right-size Concurrency**
   ```yaml
   global_config:
     n_threads: 8           # Match CPU cores
     n_clients: 1000        # Balance load
     batch_size: 1000       # Optimize throughput
   ```

2. **Use Appropriate Workloads**
   ```yaml
   # High-throughput ingestion
   - name: bulk_load
     type: workload
     workload: ingest
     config:
       batch_size: 5000
     
   # Low-latency queries
   - name: interactive_search
     type: workload  
     workload: query
     config:
       batch_size: 1
   ```

3. **Monitor Resource Usage**
   ```yaml
   steps:
     - name: resource_checkpoint
       type: checkpoint
       parameters:
         collect_full_metrics: true
         analyze_bottlenecks: true
   ```

### Memory Testing

1. **Test Growth Patterns**
   ```yaml
   # Gradual growth testing
   steps:
     - name: small_load
       type: workload
       workload: ingest
       parameters:
         target_vectors: 500000
         
     - name: medium_load
       type: workload
       workload: ingest
       parameters:
         target_vectors: 1500000
         
     - name: large_load
       type: workload
       workload: ingest
       parameters:
         target_vectors: 3000000
   ```

2. **Test Cleanup Behavior**
   ```yaml
   # Memory cleanup testing
   steps:
     - name: fill_memory
       type: workload
       workload: ingest
       parameters:
         target_vectors: 2000000
         
     - name: shrink_data
       type: workload
       workload: shrink
       parameters:
         target_vectors: 500000
         
     - name: check_cleanup
       type: checkpoint
       parameters:
         analyze_fragmentation: true
   ```

## Troubleshooting

### Common Issues

#### Scenario Validation Errors

**Error: Invalid workload type 'ingests'**
```yaml
# Fix typo in workload name
- name: load_step
  type: workload
  workload: ingest  # Not 'ingests'
```

**Error: Missing required parameter 'target_vectors'**
```yaml
# Add required parameters
- name: ingest_step
  type: workload
  workload: ingest
  parameters:
    target_vectors: 1000000  # Required parameter
```

#### Step Execution Failures

**Error: Step timeout after 1800 seconds**
```yaml
# Increase step timeout
global_config:
  step_timeout: 3600  # 1 hour

# Or per-step timeout
- name: long_running_step
  type: workload
  workload: ingest
  timeout_seconds: 7200  # 2 hours
  parameters:
    target_vectors: 5000000
```

**Error: Insufficient memory for operation**
```yaml
# Reduce memory usage
global_config:
  batch_size: 500      # Smaller batches
  n_clients: 500       # Fewer clients

# Add memory check
- name: memory_check
  type: checkpoint
  parameters:
    check_available_memory: true
    min_available_mb: 2048
```

#### Performance Issues

**Slow step execution**
```yaml
# Optimize for performance
global_config:
  n_threads: 16       # More threads
  batch_size: 2000    # Larger batches
  
# Use faster workloads
- name: fast_ingest
  type: workload
  workload: ingest
  config:
    skip_verification: true  # Skip verification for speed
```

**High memory usage**
```yaml
# Add memory monitoring
- name: memory_monitor
  type: checkpoint
  interval_seconds: 30
  parameters:
    collect_memory_metrics: true
    memory_threshold_mb: 6144
    action_on_threshold: pause
```

### Debugging Scenarios

1. **Enable Debug Logging**
   ```bash
   vst run --scenario my_scenario.yaml --log-level DEBUG
   ```

2. **Use Dry Run Mode**
   ```bash
   vst run --scenario my_scenario.yaml --dry-run
   ```

3. **Test Individual Steps**
   ```bash
   vst run --scenario my_scenario.yaml --step load_step
   ```

4. **Validate Configuration**
   ```bash
   vst scenario validate --scenario my_scenario.yaml --verbose
   ```

### Getting Help

For additional help with scenarios:

1. **Built-in Help**
   ```bash
   vst scenario --help
   vst run --help
   ```

2. **Example Scenarios**
   ```bash
   # View built-in scenarios
   ls config/scenarios/
   
   # Copy and modify examples
   cp config/scenarios/continuous_growth.yaml my_scenario.yaml
   ```

3. **Scenario Documentation**
   ```bash
   vst scenario docs
   vst workload list  # Available workloads
   ```
