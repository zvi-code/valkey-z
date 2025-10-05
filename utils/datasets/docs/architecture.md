# Architecture Overview

## Components

### Core Module
- **Dataset**: Handles HDF5 dataset loading and vector access
- **VectorOps**: Mathematical operations for vector expansion
- **Connection**: Valkey connection pool management
- **Metrics**: Metric collection and storage

### Workload Module
- **Base**: Abstract base class for all workloads
- **Executor**: Multi-threaded execution engine
- **Ingest**: Vector insertion workload
- **Query**: KNN search with recall calculation
- **Shrink**: Random deletion workload

### Scenarios Module
- **Loader**: YAML scenario parsing
- **Runner**: Scenario orchestration
- **Models**: Data models for scenarios

### Monitoring Module
- **Collector**: Async memory metric collection
- **Aggregator**: Statistical calculations
- **Exporter**: CSV and Prometheus export

## Data Flow

1. Dataset loaded from HDF5
2. Scenario parsed from YAML
3. Workloads executed via thread pool
4. Metrics collected asynchronously
5. Results exported to CSV
