# Dataset Class Architecture - Usage Guide

## Overview

The new Dataset class provides a comprehensive abstraction for managing datasets throughout their entire lifecycle, from source files to production-ready vector databases. This guide demonstrates how to use the Dataset class and its related components.

## Key Components

### 1. Dataset Class
The central abstraction that tracks:
- Dataset state and lifecycle
- Source information
- S3 storage locations
- Components (train, query, ground truth)
- Subsets with different vector counts
- RDB files for each configuration

### 2. DatasetState Enum
Represents the dataset lifecycle states:
- `SOURCE_ONLY`: Only source files exist
- `VKV_CONVERTED`: Converted to VKV format
- `RDB_GENERATED`: RDB files created
- `S3_UPLOADED`: Uploaded to S3
- `READY`: Fully prepared and validated
- `FAILED`: Preparation failed

### 3. DatasetRegistry
Manages multiple datasets with features for:
- Registration and retrieval
- Filtering by state or tags
- Bulk operations
- Persistence to JSON

## Basic Usage

### Creating a Dataset

```python
from datasets import Dataset, DatasetSource, DatasetState

# Create a new dataset
dataset = Dataset(
    name="my-vectors-1m",
    tags=["production", "text-embeddings"]
)

# Set source information
dataset.source = DatasetSource(
    type="local",
    location="/data/vectors.hdf5",
    format="hdf5",
    metadata={"original_size": "5GB"}
)
```

### State Management

```python
# Transition through states
dataset.transition_to(DatasetState.VKV_CONVERTED)
dataset.transition_to(DatasetState.RDB_GENERATED)
dataset.transition_to(DatasetState.S3_UPLOADED)
dataset.transition_to(DatasetState.READY)

# Check state
if dataset.is_ready():
    print("Dataset is ready for use")

# Handle failures
if something_went_wrong:
    dataset.mark_failed("Error during RDB generation")
```

### Managing Subsets

```python
from datasets import DatasetSubset

# Add subsets
subset_10k = DatasetSubset(
    size=10000,
    s3_prefix="my-vectors-1m/subsets/10000",
    vkv_path="my-vectors-1m/subsets/10000/train.vkv",
    rdb_files={
        "vectors_only": "my-vectors-1m/subsets/10000/vectors.rdb",
        "indexed_HNSW_L2": "my-vectors-1m/subsets/10000/indexed.rdb"
    }
)

dataset.add_subset(10000, subset_10k)

# Retrieve subsets
exact_subset = dataset.get_subset(10000)
closest_subset = dataset.get_closest_subset(15000)  # Returns 10k subset
```

### RDB File Management

```python
from datasets.metadata import RDBFileInfo
from core.config import IndexSpec

# Add RDB file information
rdb_info = RDBFileInfo(
    filename="vectors.rdb",
    size_bytes=100_000_000,
    memory_usage_bytes=120_000_000,
    key_count=1_000_000,
    creation_time="2024-01-15T10:30:00Z",
    valkey_version="7.0",
    loading_time_seconds=45.2
)

dataset.add_rdb_file("vectors_only", rdb_info)

# Retrieve RDB by index specification
index_spec = IndexSpec(algorithm="HNSW", metric="L2")
indexed_rdb = dataset.get_rdb_by_spec(index_spec)
```

## Integration with DatasetPreparer

### Using the Integration Layer

```python
from datasets import Dataset, DatasetRegistry
from datasets.preparer_integration import DatasetPreparerIntegration
from datasets.converter import SourceFormat
from pathlib import Path

# Create dataset
dataset = Dataset(name="new-dataset")

# Prepare with tracking
async with DatasetPreparer(s3_config, rdb_config) as preparer:
    dataset = await DatasetPreparerIntegration.prepare_with_dataset_tracking(
        preparer=preparer,
        dataset=dataset,
        source_path=Path("/data/vectors.hdf5"),
        source_format=SourceFormat.HDF5,
        index_spec=IndexSpec(algorithm="HNSW", metric="L2"),
        subset_sizes=[10000, 100000, 500000],
        description="Production dataset with 1M vectors"
    )

# Dataset is now fully tracked and ready
print(f"Dataset state: {dataset.state}")
print(f"Available subsets: {dataset.list_subset_sizes()}")
```

### Converting Existing Metadata

```python
# Create Dataset from existing DatasetMetadata
dataset = DatasetPreparerIntegration.create_dataset_from_metadata(
    name="existing-dataset",
    metadata=existing_metadata,
    s3_bucket="my-datasets",
    s3_prefix="existing-dataset"
)
```

## Dataset Registry Usage

### Basic Registry Operations

```python
from datasets import DatasetRegistry

# Create registry
registry = DatasetRegistry()

# Register datasets
registry.register(dataset1)
registry.register(dataset2)

# Retrieve dataset
dataset = registry.get("my-vectors-1m")

# List datasets
all_datasets = registry.list()
ready_datasets = registry.list(ready_only=True)
failed_datasets = registry.list(state=DatasetState.FAILED)

# Find by tag
ml_datasets = registry.find_by_tag("machine-learning")
```

### Persistence

```python
from pathlib import Path

# Save registry
registry.save(Path("./dataset_registry.json"))

# Load registry
loaded_registry = DatasetRegistry.load(Path("./dataset_registry.json"))
```

### Bulk Operations

```python
# Update multiple datasets
dataset_names = ["dataset1", "dataset2", "dataset3"]
results = registry.bulk_update_state(dataset_names, DatasetState.READY)

# Get statistics
stats = registry.get_statistics()
print(f"Total datasets: {stats['total_datasets']}")
print(f"Ready datasets: {stats['ready_datasets']}")
print(f"State distribution: {stats['states']}")
```

## Advanced Usage

### Custom Subset Selection

```python
def get_optimal_subset(dataset: Dataset, target_size: int, max_size: int) -> DatasetSubset:
    """Get optimal subset within constraints."""
    available_sizes = [s for s in dataset.list_subset_sizes() if s <= max_size]
    
    if not available_sizes:
        return None
    
    # Find closest to target
    closest_size = min(available_sizes, key=lambda x: abs(x - target_size))
    return dataset.get_subset(closest_size)
```

### Dataset Validation

```python
def validate_production_dataset(dataset: Dataset) -> bool:
    """Validate dataset meets production requirements."""
    errors = dataset.validate()
    
    if errors:
        print(f"Validation errors: {errors}")
        return False
    
    # Check required components
    required_components = ["train", "query", "metadata"]
    for component in required_components:
        if not dataset.has_component(component):
            print(f"Missing component: {component}")
            return False
    
    # Check RDB files
    if not dataset.get_rdb_by_spec(None):  # vectors-only
        print("Missing vectors-only RDB")
        return False
    
    # Check at least one subset
    if not dataset.list_subset_sizes():
        print("No subsets available")
        return False
    
    return True
```

### S3 Manifest Generation

```python
# Get all S3 objects for a dataset
manifest = dataset.get_s3_manifest()

print("Components:", manifest["components"])
print("RDB files:", manifest["rdb_files"])
print("Subset files:", manifest["subsets"])

# Use for batch operations, cleanup, or migration
```

## Error Handling

```python
try:
    # Attempt dataset preparation
    dataset = await prepare_dataset(...)
    
except Exception as e:
    # Mark dataset as failed with error details
    dataset.mark_failed(f"Preparation failed: {str(e)}")
    
    # Save state for debugging
    registry.register(dataset)
    registry.save()
    
    # Can retry later
    if dataset.can_transition_to(DatasetState.SOURCE_ONLY):
        dataset.transition_to(DatasetState.SOURCE_ONLY)
        # Retry preparation...
```

## Best Practices

1. **Always validate state transitions**: Use `can_transition_to()` before attempting transitions
2. **Tag datasets appropriately**: Use tags for environment (dev/prod), data type, version, etc.
3. **Persist registry regularly**: Save after important operations
4. **Handle failures gracefully**: Always mark failed state with descriptive error messages
5. **Use subsets for testing**: Start with smaller subsets before processing full datasets
6. **Document metadata**: Include descriptions and relevant metadata for future reference

## Migration from Legacy Code

If you have existing code using the old dataset management approach:

```python
# Old approach
metadata = prepare_dataset_old_way(...)
upload_to_s3(...)

# New approach - wrap in Dataset class
dataset = Dataset(name="legacy-dataset")
dataset.update_from_metadata(metadata)
dataset.s3_bucket = "my-bucket"
dataset.s3_prefix = "legacy-dataset"
dataset.transition_to(DatasetState.READY)

# Register for future use
registry.register(dataset)
```

## CLI Integration Example

```python
import typer
from datasets import Dataset, DatasetRegistry

app = typer.Typer()

@app.command()
def list_datasets(state: str = None, ready_only: bool = False):
    """List all registered datasets."""
    registry = DatasetRegistry.load(Path("./registry.json"))
    
    if state:
        datasets = registry.list(state=DatasetState[state.upper()])
    else:
        datasets = registry.list(ready_only=ready_only)
    
    for dataset in datasets:
        print(f"{dataset.name} - {dataset.state.value} - {len(dataset.subsets)} subsets")

@app.command()
def dataset_info(name: str):
    """Show detailed dataset information."""
    registry = DatasetRegistry.load(Path("./registry.json"))
    dataset = registry.get(name)
    
    if not dataset:
        print(f"Dataset '{name}' not found")
        return
    
    print(f"Name: {dataset.name}")
    print(f"State: {dataset.state.value}")
    print(f"Source: {dataset.source.type if dataset.source else 'N/A'}")
    print(f"S3 Location: s3://{dataset.s3_bucket}/{dataset.s3_prefix}")
    print(f"Subsets: {dataset.list_subset_sizes()}")
    print(f"Tags: {', '.join(dataset.tags)}")
```

This comprehensive Dataset class architecture provides a robust foundation for managing vector datasets at scale, with clear state tracking, flexible subset management, and seamless integration with existing infrastructure.