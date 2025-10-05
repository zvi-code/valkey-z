# High-Level Architecture 

## Goals:

Create a comprehensive memory stress-testing package for Valkey-Search (valkeysearch compatible module)
Pre-fill with up to 1B vectors from public datasets (OpenAI, HuggingFace, BIGANN)
Test memory usage across different vector index lifecycle scenarios
Extend datasets by generating vectors with norms > max dataset norm, creating Expand_n(dataset) sets
Calculate recall using query datasets and ground-truth vectors
Support index shrinking via random vector deletion
Generate workloads alternating between extension/shrink operations with query workloads
Continuously collect memory metrics (INFO MEMORY + jemalloc stats)
Record results to CSV and Prometheus pushgateway
Support both single-instance and clustered Valkey deployments
Design for extensibility (future workload additions)
**Advanced visualization and analysis capabilities with interactive dashboards**
**Comprehensive CLI with validation, dataset management, and configuration testing**
**Production-ready configuration management with environment-specific settings**
**Phase-based memory analysis with correlation and trend identification**

Constraints:

Must download and prepare public datasets for ingestion
Must support optional pre-fill skip
Vector extension must follow specific mathematical rules (||Vn||₂ > NormMax)
Key naming convention for extended vectors: expand_n_<original_key>
Query and ground-truth vectors must be naturally extended for Expand_n(dataset)
**Support multiple output formats (CSV, JSON, interactive HTML dashboards)**
**Configuration validation and testing before execution**
**Memory analysis with phase information and correlation tracking**


## 1. Module Breakdown
```
search_delete_test/
├── core/
│   ├── dataset.py          # Dataset loading, vector operations, norm calculations
│   ├── vector_ops.py       # Vector generation, expansion logic (||Vn||₂ > NormMax)
│   ├── connection.py       # Connection pool management, async Valkey operations
│   ├── metrics.py          # Memory sampling, metric collection, CSV writing
│   └── config.py           # Configuration management and validation
│
├── workload/
│   ├── base.py            # Abstract workload class, plugin interface
│   ├── executor.py        # Multi-threaded workload orchestration
│   ├── ingest.py          # Parallel vector insertion workload
│   ├── query.py           # KNN query workload with recall calculation
│   ├── shrink.py          # Random deletion workload
│   └── mixed.py           # Mixed workload patterns
│
├── scenarios/
│   ├── loader.py          # YAML scenario parser
│   ├── runner.py          # Scenario execution engine
│   ├── models.py          # Data models for scenarios
│   └── builtin/           # Pre-defined scenarios (7+ advanced patterns)
│       ├── memory_fragmentation_analysis.yaml
│       ├── high_velocity_ingestion.yaml
│       ├── vector_expansion_memory_impact.yaml
│       ├── memory_leak_detection.yaml
│       ├── production_simulation.yaml
│       ├── cluster_memory_distribution.yaml
│       └── index_rebuild_memory_pattern.yaml
│
├── monitoring/
│   ├── collector.py       # Async INFO MEMORY collector
│   ├── aggregator.py      # Metric aggregation and percentile calculation
│   └── exporter.py        # CSV, JSON, and Prometheus exporter
│
├── visualization/
│   ├── __init__.py        # Visualization module
│   └── advanced_visualizer.py  # Interactive dashboards, phase analysis
│
└── cli/
    ├── main.py            # Typer CLI entry point
    ├── utils.py           # CLI utilities
    └── commands/          # Command modules
        ├── run.py         # Scenario execution commands
        ├── dataset.py     # Dataset management commands
        ├── validate.py    # Configuration validation commands
        ├── info.py        # System and Valkey information commands
        └── visualize.py   # Visualization generation commands
```
## 2. Enhanced Data Flow Diagram

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│ Public Datasets │────▶│ Dataset Loader   │────▶│ Vector Expander │
│ (OpenAI, BIGANN)│     │ (dataset.py)     │     │ (vector_ops.py) │
└─────────────────┘     └──────────────────┘     └────────┬────────┘
                                                           │
                         ┌─────────────────────────────────┴─────────────────────────────────┐
                         │                                                                   │
                         ▼                                                                   ▼
                  ┌──────────────┐                                                   ┌──────────────┐
                  │ Ground Truth │                                                   │ Query Vectors│
                  │   Storage    │                                                   │   Storage    │
                  └──────┬───────┘                                                   └──────┬───────┘
                         │                                                                   │
                         └─────────────────────┬─────────────────────────────────────────────┘
                                               │
                                               ▼
                                     ┌─────────────────┐       ┌──────────────────┐
                                     │ Configuration   │◀──────│ CLI Commands     │
                                     │ Management      │       │ (run, validate,  │
                                     │ (config.py)     │       │ dataset, info)   │
                                     └────────┬────────┘       └──────────────────┘
                                              │
                                              ▼
                                    ┌─────────────────┐
                                    │ Scenario Runner │
                                    │ (runner.py)     │
                                    └────────┬────────┘
                                             │
              ┌──────────────────────────────┼──────────────────────────────┐
              │                              │                              │
              ▼                              ▼                              ▼
    ┌───────────────┐              ┌───────────────┐              ┌──────────────┐
    │Ingest Workload│              │Query Workload │              │Shrink Workload│
    │ (ingest.py)   │              │ (query.py)    │              │ (shrink.py)  │
    └───────┬───────┘              └───────┬───────┘              └──────┬───────┘
            │                              │                              │
            └──────────────────────────────┼──────────────────────────────┘
                                           │
                                           ▼
                         ┌──────────────────┐      ┌────────────────┐
                         │ Connection Pool  │◀─────│ Async Monitor  │
                         │ (connection.py)  │      │ (collector.py) │
                         └────────┬─────────┘      └────────┬───────┘
                                  │                         │
                                  ▼                         │
                            ┌───────────┐                   │
                            │  Valkey   │◀──────────────────┘
                            │  Instance │     INFO MEMORY + 
                            └─────┬─────┘     jemalloc stats
                                  │
                                  ▼
               ┌──────────────────┴──────────────────┐
               │                                     │
               ▼                                     ▼
    ┌───────────────────┐                ┌─────────────────────┐
    │ Metrics Export    │                │ Visualization       │
    │ • CSV Files       │                │ • Interactive HTML  │
    │ • JSON Export     │                │ • Memory Analysis   │
    │ • Prometheus      │                │ • Phase Correlation │
    │ (exporter.py)     │                │ (advanced_visualizer│
    └───────────────────┘                └─────────────────────┘
                                                   │
                                                   ▼
                                         ┌─────────────────────┐
                                         │ Analysis Outputs    │
                                         │ • Phase Dashboards  │
                                         │ • Memory Patterns   │
                                         │ • Performance Plots │
                                         │ • Correlation Maps  │
                                         └─────────────────────┘
```
## 3. Third-Party Libraries

### Core Dependencies
```yaml
dependencies:
  - numpy>=1.24.0          # Vector operations, norm calculations
  - valkey>=6.0.0           # Async Valkey client with connection pooling
  - psutil>=5.9.0          # System memory monitoring
  - prometheus-client>=0.19.0  # Prometheus metric export
  - typer>=0.9.0           # CLI framework
  - pyyaml>=6.0            # YAML configuration parsing
  - h5py>=3.10.0           # HDF5 dataset loading (OpenAI format)
  - aiofiles>=23.0         # Async file operations
  - pandas>=2.0.0          # CSV operations and data analysis
  - rich>=13.0.0           # Enhanced CLI output and formatting

### Visualization Dependencies (Optional)
visualization_extras:
  - matplotlib>=3.7.0      # Static chart generation
  - plotly>=5.17.0         # Interactive dashboard creation
  - seaborn>=0.12.0        # Statistical visualization
  - kaleido>=0.2.1         # Static image export from plotly

### Development Dependencies
dev_dependencies:
  - pytest>=7.4.0         # Test framework
  - pytest-asyncio>=0.21.0 # Async test support
  - pytest-cov>=4.1.0     # Test coverage
  - black>=23.0.0          # Code formatting
  - ruff>=0.1.0            # Linting
  - mypy>=1.7.0            # Type checking
```
## 4. Extended Workload Plugin Architecture

### Base Workload Framework
New workloads extend the abstract base class with comprehensive lifecycle management:

```python
# workload/base.py
class BaseWorkload(ABC):
    @abstractmethod
    async def execute(self, 
                     connection_pool: ConnectionPool,
                     dataset: Dataset,
                     config: Dict[str, Any]) -> WorkloadResult:
        """Execute the workload with full context."""
        pass
    
    @abstractmethod
    def get_metrics(self) -> Dict[str, float]:
        """Return workload-specific metrics."""
        pass
    
    def validate_config(self, config: Dict[str, Any]) -> bool:
        """Validate workload configuration."""
        return True
    
    def get_phase_info(self) -> Optional<Dict[str, Any]]:
        """Return phase information for visualization."""
        return None
```

### Built-in Workload Types

```python
# workload/ingest.py - High-throughput vector insertion
class IngestWorkload(BaseWorkload):
    """Parallel vector insertion with batching and error handling."""
    pass

# workload/query.py - KNN search with recall calculation  
class QueryWorkload(BaseWorkload):
    """Vector similarity search with performance tracking."""
    pass

# workload/shrink.py - Random deletion for memory testing
class ShrinkWorkload(BaseWorkload):
    """Random vector deletion for memory fragmentation analysis."""
    pass

# workload/mixed.py - Combined operation patterns
class MixedWorkload(BaseWorkload):
    """Configurable mix of operations for realistic testing."""
    pass
```

### Workload Registration
Workloads register via decorator for dynamic discovery:

```python
@register_workload("mixed_70_30")
class MixedWorkload(BaseWorkload):
    """70% queries, 30% ingestion workload."""
    async def execute(self, ...):
        # Implementation with phase tracking
        pass

@register_workload("memory_leak_detector")
class MemoryLeakDetector(BaseWorkload):
    """Long-running workload for leak detection."""
    pass
```

### Advanced Scenario Support

```python
# scenarios/models.py - Comprehensive scenario definition
class ScenarioStep:
    workload_type: str
    duration: float
    config: Dict[str, Any]
    phase_name: str           # For visualization grouping
    success_criteria: Dict    # Validation thresholds
    
class Scenario:
    name: str
    description: str
    steps: List<ScenarioStep>
    global_config: Dict
    visualization_config: Dict  # Visualization preferences
```
## 5. Advanced Visualization and Analysis System

### Visualization Architecture

```python
# visualization/advanced_visualizer.py
class AdvancedVisualizer:
    """Comprehensive visualization system with phase analysis."""
    
    def generate_dashboard(self, metrics_data: MetricsData) -> str:
        """Generate interactive HTML dashboard."""
        pass
    
    def create_memory_analysis(self, data: MetricsData) -> Path:
        """Generate memory analysis charts with phase information."""
        pass
    
    def create_performance_correlation(self, data: MetricsData) -> Path:
        """Generate performance correlation analysis."""
        pass
    
    def create_phase_comparison(self, data: MetricsData) -> Path:
        """Generate phase comparison heatmaps."""
        pass
```

### Visualization Features

1. **Interactive Dashboards**
   - Real-time metric exploration with plotly
   - Phase-based filtering and analysis
   - Correlation analysis tools
   - Export capabilities

2. **Memory Analysis Charts**
   - Memory usage patterns over time
   - Phase annotations and transitions
   - Fragmentation trend analysis
   - Growth pattern identification

3. **Performance Correlation**
   - Memory vs performance scatter plots
   - Latency correlation analysis
   - Resource utilization patterns
   - QPS vs memory efficiency

4. **Phase Analysis**
   - Statistical comparison across phases
   - Performance metric aggregation
   - Trend analysis and anomaly detection
   - Phase transition impact analysis

### CLI Integration

```bash
# List available scenarios with metrics data
vst visualize list-scenarios

# Generate comprehensive dashboard
vst visualize generate scenario_name --output-dir ./visuals

# Compare multiple scenarios
vst visualize compare scenario1 scenario2 --metric memory_usage

# Export data for external analysis
vst visualize export-data scenario_name --format csv
```

## 6. Enhanced Configuration Management

### Multi-Environment Support

```yaml
# Environment-specific configuration inheritance
# config/environments/production.yaml
valkey:
  host: "${VALKEY_HOST}"
  port: "${VALKEY_PORT}"
  password: "${VALKEY_PASSWORD}"
  ssl: true
  ssl_cert_reqs: required

security:
  audit_logging: true
  rate_limiting: true
  allowed_ips:
    - 10.0.0.0/8

workload:
  n_threads: 32
  max_memory_usage_mb: 32768
  
monitoring:
  export_format: prometheus
  prometheus_port: 8080
```

### Configuration Validation

```python
# CLI validation commands
vst validate config production.yaml --strict
vst validate scenario memory_test.yaml
vst config test --config production.yaml --dry-run
```

### Performance Tuning Profiles

```yaml
# High-throughput configuration
high_performance:
  workload:
    n_threads: 32
    n_clients_per_thread: 156
    batch_size: 2000
    operation_timeout: 10.0

# Low-latency configuration  
low_latency:
  workload:
    batch_size: 1
    socket_timeout: 1.0
    max_retries: 1

# Memory-optimized configuration
memory_optimized:
  workload:
    vector_cache_size: 1000
    gc_frequency: 100
    max_memory_usage_mb: 2048
```

## 7. Built-in Advanced Scenarios

### Production-Ready Scenario Library

The system includes 7+ sophisticated scenarios targeting different memory stress patterns:

1. **Memory Fragmentation Analysis** - Multi-phase fragmentation testing (45 min, 16GB target)
2. **High Velocity Ingestion** - High-throughput stress testing (30 min, 32GB target)
3. **Vector Expansion Memory Impact** - Expansion feature analysis (40 min, 24GB target)
4. **Memory Leak Detection** - Long-running leak detection (4 hours, 8GB target)
5. **Production Simulation** - Realistic workload patterns (60 min, 12GB target)
6. **Cluster Memory Distribution** - Multi-node analysis (60 min, 8GB per node)
7. **Index Rebuild Memory Pattern** - Index operation impact (50 min, 20GB target)

### Scenario Configuration Structure

```yaml
# Example scenario with phase information
scenario:
  name: "memory_fragmentation_analysis"
  description: "Multi-phase fragmentation testing"
  phases:
    - name: "initial_growth"
      workload_type: "ingest"
      duration: 600
      target_memory_gb: 8
      
    - name: "fragmentation_cycle"
      workload_type: "mixed"
      duration: 1200
      config:
        operation_mix: [50, 30, 20]  # ingest, query, shrink
        
    - name: "cleanup_analysis"
      workload_type: "shrink"
      duration: 600
      target_reduction: 0.5
      
  visualization:
    focus_metrics: ["fragmentation_ratio", "allocated_mb"]
    phase_analysis: true
    correlation_analysis: true
```

## 8. Extended CLI Architecture

### Comprehensive Command Structure

```bash
# Core execution commands
vst run quick [OPTIONS]                    # Quick stress test
vst run scenario SCENARIO [OPTIONS]        # Run specific scenario
vst run list-scenarios                     # List available scenarios
vst run validate SCENARIO                  # Validate scenario

# Dataset management
vst dataset download DATASET_NAME          # Download public datasets
vst dataset list                          # List available datasets
vst dataset info DATASET_NAME             # Show dataset information
vst dataset validate PATH                 # Validate dataset format

# Configuration and validation
vst validate config CONFIG_FILE           # Validate configuration
vst validate scenario SCENARIO_FILE       # Validate scenario
vst config test --config CONFIG           # Test configuration

# System information and diagnostics
vst info system                          # Show system information
vst info valkey [--host HOST]             # Test Valkey connectivity
vst info datasets                        # Show dataset status

# Advanced visualization
vst visualize generate SCENARIO           # Generate visualizations
vst visualize list-scenarios              # List scenarios with data
vst visualize compare SCENARIO1 SCENARIO2 # Compare scenarios
vst visualize export-data SCENARIO        # Export analysis data

# Version and help
vst version                               # Show version information
vst --help                               # Show help
```

### CLI Features

1. **Rich Output** - Enhanced terminal output with progress bars and colors
2. **Validation** - Comprehensive validation before execution
3. **Dry Run Support** - Test scenarios without execution
4. **Environment Variables** - Flexible configuration via env vars
5. **Error Handling** - Detailed error messages and debugging support

## 9. Updated Key Assumptions

### Memory and Performance Model
- **Memory Metrics**: Valkey RSS/Active/Resident memory metrics accurately reflect vector index memory usage
- **Extended Metrics**: jemalloc statistics provide detailed memory allocation insights
- **Memory Phases**: Phase-based analysis provides meaningful memory pattern identification
- **Fragmentation Tracking**: Memory fragmentation ratios correlate with operational performance

### Vector and Dataset Model
- **Vector Distribution**: OpenAI dataset vectors follow expected distribution for norm calculations
- **Expansion Mathematics**: Extended vectors (Vn + original) maintain nearest neighbor relationships
- **Dataset Scaling**: Up to 1B vectors can be processed with appropriate hardware configuration
- **Format Compatibility**: HDF5 format supports efficient large-scale vector operations

### System Performance and Scalability
- **Async Performance**: Valkey-py async operations with configurable client counts scale appropriately
- **Thread Scaling**: Multi-threaded workloads scale linearly with available CPU cores
- **Memory Efficiency**: Vector caching and pooling provide measurable performance improvements
- **Connection Pooling**: Connection pool management prevents resource exhaustion

### Operational and Clustering
- **Thread Safety**: Valkey-Search handles concurrent operations without data corruption
- **Cluster Support**: Multi-node deployments maintain consistent memory distribution patterns
- **Configuration Validation**: YAML validation prevents configuration-related failures
- **Phase Transitions**: Workload phase transitions are detectable in memory metrics

### Data Management and Storage
- **Batch Optimization**: Configurable batch sizes (100-2000) balance memory and performance
- **Key Naming**: Namespace conventions (expand_*) prevent key collisions
- **File I/O**: CSV and JSON exports handle large metric datasets efficiently
- **Visualization Data**: Interactive dashboards perform efficiently with typical metric volumes

### Monitoring and Analysis
- **Sampling Frequency**: Configurable sampling intervals (0.1-10 seconds) provide appropriate granularity
- **Metric Correlation**: Memory and performance metrics show meaningful correlations
- **Phase Detection**: Automated phase detection accurately identifies scenario transitions
- **Export Formats**: Multiple export formats (CSV, JSON, HTML) maintain data integrity

### Security and Production Readiness
- **Authentication**: Valkey authentication and SSL/TLS support production security requirements
- **Environment Isolation**: Environment-specific configurations prevent cross-environment issues
- **Resource Limits**: Memory and connection limits prevent system resource exhaustion
- **Audit Trails**: Comprehensive logging supports production troubleshooting and analysis

### Visualization and User Experience
- **Interactive Performance**: Plotly dashboards handle typical memory stress test data volumes efficiently
- **Phase Visualization**: Phase-based color coding and annotations provide clear visual insights
- **Export Compatibility**: Generated visualizations are compatible with common reporting tools
- **CLI Responsiveness**: Command-line interface provides timely feedback for all operations

### Integration and Extensibility
- **Plugin Architecture**: New workloads integrate seamlessly with existing framework
- **Scenario Extensibility**: Custom scenarios follow established patterns and validation rules
- **Metric Extensibility**: Additional metrics can be integrated without breaking existing functionality
- **API Stability**: Core interfaces remain stable for backward compatibility

Please confirm these updated assumptions align with your production requirements and use cases. The design now reflects a comprehensive, production-ready memory stress testing framework with advanced visualization and analysis capabilities.
