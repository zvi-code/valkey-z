#!/usr/bin/env python3
"""
Complete example demonstrating the Dataset class architecture.

This example shows:
1. Creating datasets with state management
2. Managing subsets and RDB files
3. Using the registry for dataset management
4. Serialization and persistence
"""

import sys
from pathlib import Path
from datetime import datetime, timezone
import tempfile

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.datasets import (
    Dataset, DatasetState, DatasetSource, DatasetSubset, DatasetRegistry
)
from src.datasets.metadata import DatasetInfo, DatasetMetadata, VectorStats, StorageInfo, RDBFileInfo
from src.datasets.rdb_generator import IndexSpec, IndexAlgorithm, MetricType


def main():
    """Demonstrate Dataset class functionality."""
    
    print("=" * 60)
    print("Dataset Class Architecture Example")
    print("=" * 60)
    
    # 1. Create a new dataset
    print("\n1. Creating a new dataset...")
    dataset = Dataset(
        name="example-vectors-1m",
        tags=["example", "text-embeddings", "production"]
    )
    
    # Set source information
    dataset.source = DatasetSource(
        type="local",
        location="/data/vectors/openai-1m.hdf5",
        format="hdf5",
        metadata={"original_size": "2.5GB", "download_date": "2024-01-15"}
    )
    
    print(f"   Created dataset: {dataset.name}")
    print(f"   Initial state: {dataset.state}")
    print(f"   Source: {dataset.source.location}")
    
    # 2. Simulate preparation pipeline with state transitions
    print("\n2. Simulating dataset preparation pipeline...")
    
    # VKV Conversion
    dataset.transition_to(DatasetState.VKV_CONVERTED)
    dataset.add_component("train", "example-vectors-1m/train.vkv")
    dataset.add_component("query", "example-vectors-1m/query.vkv")
    dataset.add_component("ground_truth", "example-vectors-1m/ground_truth.json")
    print(f"   State: {dataset.state} - Added VKV components")
    
    # RDB Generation
    rdb_vectors = RDBFileInfo(
        filename="vectors.rdb",
        size_bytes=800_000_000,  # 800MB
        memory_usage_bytes=950_000_000,  # 950MB in memory
        key_count=1_000_000,
        creation_time=datetime.now(timezone.utc).isoformat(),
        valkey_version="7.0",
        loading_time_seconds=45.2
    )
    
    rdb_indexed = RDBFileInfo(
        filename="vectors_indexed.rdb",
        size_bytes=1_200_000_000,  # 1.2GB
        memory_usage_bytes=1_400_000_000,  # 1.4GB in memory
        key_count=1_000_000,
        creation_time=datetime.now(timezone.utc).isoformat(),
        valkey_version="7.0",
        loading_time_seconds=67.8,
        index_name="vector_index",
        index_creation_time_seconds=22.3,
        index_spec={"algorithm": "HNSW", "metric": "L2", "M": 16, "EF_CONSTRUCTION": 200}
    )
    
    dataset.add_rdb_file("vectors_only", rdb_vectors)
    dataset.add_rdb_file("indexed_HNSW_L2", rdb_indexed)
    dataset.transition_to(DatasetState.RDB_GENERATED)
    print(f"   State: {dataset.state} - Generated RDB files")
    
    # S3 Upload
    dataset.s3_bucket = "vss-datasets-production"
    dataset.s3_prefix = "example-vectors-1m"
    dataset.transition_to(DatasetState.S3_UPLOADED)
    print(f"   State: {dataset.state} - Uploaded to S3")
    
    # 3. Add subsets for different scales
    print("\n3. Adding dataset subsets...")
    
    subset_sizes = [1000, 10000, 100000, 500000]
    for size in subset_sizes:
        subset = DatasetSubset(
            size=size,
            s3_prefix=f"example-vectors-1m/subsets/{size}",
            vkv_path=f"example-vectors-1m/subsets/{size}/train.vkv",
            rdb_files={
                "vectors_only": f"example-vectors-1m/subsets/{size}/vectors.rdb",
                "indexed_HNSW_L2": f"example-vectors-1m/subsets/{size}/indexed.rdb"
            },
            creation_time=datetime.now(timezone.utc).isoformat(),
            metadata={"sampling_method": "random", "seed": 42}
        )
        dataset.add_subset(size, subset)
    
    print(f"   Added subsets: {dataset.list_subset_sizes()}")
    
    # 4. Test subset selection
    print("\n4. Testing subset selection...")
    
    # Exact match
    exact_subset = dataset.get_subset(10000)
    print(f"   Exact subset (10k): {exact_subset.size if exact_subset else 'Not found'}")
    
    # Closest match
    closest_subset = dataset.get_closest_subset(75000)
    print(f"   Closest to 75k: {closest_subset.size if closest_subset else 'Not found'}")
    
    closest_subset = dataset.get_closest_subset(750000)
    print(f"   Closest to 750k: {closest_subset.size if closest_subset else 'Not found'}")
    
    # 5. Test RDB file retrieval by index spec
    print("\n5. Testing RDB file retrieval...")
    
    # Get vectors-only RDB
    vectors_rdb = dataset.get_rdb_by_spec(None)
    print(f"   Vectors-only RDB: {vectors_rdb.filename if vectors_rdb else 'Not found'}")
    
    # Get indexed RDB
    index_spec = IndexSpec(
        algorithm=IndexAlgorithm.HNSW,
        metric=MetricType.L2,
        dimension=1536  # OpenAI embedding dimension
    )
    indexed_rdb = dataset.get_rdb_by_spec(index_spec)
    print(f"   Indexed RDB: {indexed_rdb.filename if indexed_rdb else 'Not found'}")
    
    # 6. Complete preparation and validate
    print("\n6. Completing dataset preparation...")
    
    # Create metadata for final state
    dataset_info = DatasetInfo(
        name=dataset.name,
        source="OpenAI text-embedding-ada-002",
        version="1.0",
        created_at=dataset.created_at,
        processing_version="2.1.0",
        description="1M OpenAI text embeddings for production testing"
    )
    
    vector_stats = VectorStats(
        count=1_000_000,
        dimension=1536,
        data_type="float32",
        norm_min=0.1,
        norm_max=2.8,
        norm_mean=1.2,
        norm_std=0.3
    )
    
    storage_info = StorageInfo(
        key_pattern="vec:*",
        key_encoding="utf-8",
        data_format="vkv",
        compression="zstd",
        total_size_bytes=800_000_000
    )
    
    metadata = DatasetMetadata(
        dataset=dataset_info,
        vectors=vector_stats,
        storage=storage_info,
        rdb_files={"vectors_only": rdb_vectors, "indexed_HNSW_L2": rdb_indexed},
        checksums={
            "train.vkv": "a1b2c3d4e5f6...",
            "vectors.rdb": "f6e5d4c3b2a1...",
            "vectors_indexed.rdb": "123456789abc..."
        }
    )
    
    dataset.update_from_metadata(metadata)
    dataset.transition_to(DatasetState.READY)
    print(f"   State: {dataset.state} - Dataset ready for use!")
    
    # 7. Registry management
    print("\n7. Dataset registry management...")
    
    registry = DatasetRegistry()
    
    # Register our dataset
    success = registry.register(dataset)
    print(f"   Registration successful: {success}")
    
    # Create additional datasets for demonstration
    small_dataset = Dataset(
        name="small-test-dataset",
        state=DatasetState.VKV_CONVERTED,
        tags=["test", "development"]
    )
    small_dataset.add_component("train", "small-test/train.vkv")
    registry.register(small_dataset)
    
    failed_dataset = Dataset(name="failed-dataset", state=DatasetState.FAILED)
    failed_dataset.mark_failed("Conversion failed - invalid file format")
    registry.register(failed_dataset)
    
    # Registry operations
    all_datasets = registry.list()
    ready_datasets = registry.list(ready_only=True)
    test_datasets = registry.find_by_tag("test")
    
    print(f"   Total datasets: {len(all_datasets)}")
    print(f"   Ready datasets: {len(ready_datasets)}")
    print(f"   Test datasets: {len(test_datasets)}")
    
    # Registry statistics
    stats = registry.get_statistics()
    print(f"   Registry stats: {stats}")
    
    # 8. Serialization and persistence
    print("\n8. Testing serialization and persistence...")
    
    # Serialize dataset to JSON
    json_str = dataset.to_json()
    print(f"   JSON size: {len(json_str):,} characters")
    
    # Deserialize
    dataset_copy = Dataset.from_json(json_str)
    print(f"   Deserialized: {dataset_copy.name} (state: {dataset_copy.state})")
    
    # Save registry to file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        registry.save(Path(f.name))
        registry_file = f.name
    
    # Load registry
    loaded_registry = DatasetRegistry.load(Path(registry_file))
    loaded_dataset = loaded_registry.get("example-vectors-1m")
    
    print(f"   Registry persistence: {loaded_dataset.name if loaded_dataset else 'Failed'}")
    
    # Cleanup
    Path(registry_file).unlink()
    
    # 9. Generate S3 manifest
    print("\n9. Generating S3 manifest...")
    
    manifest = dataset.get_s3_manifest()
    print(f"   Components: {len(manifest['components'])} files")
    print(f"   RDB files: {len(manifest['rdb_files'])} files")
    print(f"   Subset files: {sum(len(files) for files in manifest['subsets'].values())} files")
    
    # 10. Validation
    print("\n10. Dataset validation...")
    
    errors = dataset.validate()
    if errors:
        print(f"   Validation errors: {errors}")
    else:
        print("   ✓ Dataset is valid and ready for production use!")
    
    print("\n" + "=" * 60)
    print("Example completed successfully!")
    print("=" * 60)


if __name__ == "__main__":
    main()