"""
Unit tests for the Dataset class architecture.

Tests cover:
- State management and transitions
- Subset management
- RDB file mapping
- Serialization/deserialization
- Dataset registry operations
- Integration with metadata
"""

import pytest
import json
from pathlib import Path
from datetime import datetime, timezone
import tempfile

from datasets.dataset import (
    Dataset, DatasetState, DatasetSource, DatasetSubset, DatasetRegistry
)
from datasets.metadata import DatasetInfo, DatasetMetadata, RDBFileInfo, VectorStats, StorageInfo
from datasets.rdb_generator import IndexSpec, IndexAlgorithm, MetricType


class TestDatasetState:
    """Test dataset state management."""
    
    def test_initial_state(self):
        """Test dataset initialization with default state."""
        dataset = Dataset(name="test-dataset")
        assert dataset.state == DatasetState.SOURCE_ONLY
        assert dataset.name == "test-dataset"
        assert len(dataset.state_history) == 1
        assert dataset.state_history[0][1] == DatasetState.SOURCE_ONLY.value
    
    def test_valid_state_transitions(self):
        """Test valid state transitions."""
        dataset = Dataset(name="test-dataset")
        
        # SOURCE_ONLY -> VKV_CONVERTED
        assert dataset.transition_to(DatasetState.VKV_CONVERTED) is True
        assert dataset.state == DatasetState.VKV_CONVERTED
        
        # VKV_CONVERTED -> RDB_GENERATED
        assert dataset.transition_to(DatasetState.RDB_GENERATED) is True
        assert dataset.state == DatasetState.RDB_GENERATED
        
        # RDB_GENERATED -> S3_UPLOADED
        assert dataset.transition_to(DatasetState.S3_UPLOADED) is True
        assert dataset.state == DatasetState.S3_UPLOADED
        
        # S3_UPLOADED -> READY
        assert dataset.transition_to(DatasetState.READY) is True
        assert dataset.state == DatasetState.READY
        
        # Check state history
        assert len(dataset.state_history) == 5
    
    def test_invalid_state_transitions(self):
        """Test invalid state transitions."""
        dataset = Dataset(name="test-dataset")
        
        # Cannot jump directly to READY
        assert dataset.transition_to(DatasetState.READY) is False
        assert dataset.state == DatasetState.SOURCE_ONLY
        
        # Cannot go backwards
        dataset.transition_to(DatasetState.VKV_CONVERTED)
        assert dataset.transition_to(DatasetState.SOURCE_ONLY) is False
    
    def test_failed_state_handling(self):
        """Test failed state transitions and recovery."""
        dataset = Dataset(name="test-dataset")
        dataset.transition_to(DatasetState.VKV_CONVERTED)
        
        # Mark as failed
        dataset.mark_failed("Conversion error")
        assert dataset.state == DatasetState.FAILED
        assert dataset.error_message == "Conversion error"
        
        # Can restart from failed
        assert dataset.transition_to(DatasetState.SOURCE_ONLY) is True
        assert dataset.error_message is None


class TestDatasetSource:
    """Test DatasetSource functionality."""
    
    def test_source_creation(self):
        """Test creating dataset source."""
        source = DatasetSource(
            type="url",
            location="https://example.com/dataset.hdf5",
            format="hdf5",
            metadata={"size": "1GB"}
        )
        
        assert source.type == "url"
        assert source.location == "https://example.com/dataset.hdf5"
        assert source.format == "hdf5"
        assert source.metadata["size"] == "1GB"
    
    def test_source_serialization(self):
        """Test source serialization/deserialization."""
        source = DatasetSource(
            type="local",
            location="/data/vectors.parquet",
            format="parquet",
            downloaded_path=Path("/tmp/vectors.parquet")
        )
        
        # Serialize
        data = source.to_dict()
        assert data["type"] == "local"
        assert data["downloaded_path"] == "/tmp/vectors.parquet"
        
        # Deserialize
        source2 = DatasetSource.from_dict(data)
        assert source2.type == source.type
        assert source2.downloaded_path == source.downloaded_path


class TestDatasetSubset:
    """Test DatasetSubset functionality."""
    
    def test_subset_creation(self):
        """Test creating dataset subset."""
        subset = DatasetSubset(
            size=10000,
            s3_prefix="dataset/subsets/10000",
            vkv_path="dataset/subsets/10000/train.vkv",
            rdb_files={
                "vectors_only": "dataset/subsets/10000/vectors.rdb",
                "indexed_hnsw_l2": "dataset/subsets/10000/indexed.rdb"
            }
        )
        
        assert subset.size == 10000
        assert subset.s3_prefix == "dataset/subsets/10000"
        assert len(subset.rdb_files) == 2
    
    def test_get_rdb_by_spec(self):
        """Test getting RDB by index specification."""
        subset = DatasetSubset(
            size=10000,
            s3_prefix="dataset/subsets/10000",
            rdb_files={
                "vectors_only": "dataset/subsets/10000/vectors.rdb",
                "indexed_HNSW_L2": "dataset/subsets/10000/indexed_hnsw.rdb"
            }
        )
        
        # Get vectors-only RDB
        assert subset.get_rdb_by_spec(None) == "dataset/subsets/10000/vectors.rdb"
        
        # Get indexed RDB
        index_spec = IndexSpec(algorithm=IndexAlgorithm.HNSW, metric=MetricType.L2, dimension=128)
        assert subset.get_rdb_by_spec(index_spec) == "dataset/subsets/10000/indexed_hnsw.rdb"


class TestDataset:
    """Test main Dataset class functionality."""
    
    def test_dataset_creation(self):
        """Test creating dataset with all components."""
        dataset = Dataset(
            name="test-vectors",
            state=DatasetState.SOURCE_ONLY,
            source=DatasetSource(type="local", location="/data/vectors.hdf5"),
            s3_bucket="test-bucket",
            s3_prefix="test-vectors"
        )
        
        assert dataset.name == "test-vectors"
        assert dataset.state == DatasetState.SOURCE_ONLY
        assert dataset.source.type == "local"
        assert dataset.s3_bucket == "test-bucket"
    
    def test_subset_management(self):
        """Test subset addition and retrieval."""
        dataset = Dataset(name="test-dataset")
        
        # Add subsets
        subset1 = DatasetSubset(size=1000, s3_prefix="test/subsets/1000")
        subset2 = DatasetSubset(size=5000, s3_prefix="test/subsets/5000")
        subset3 = DatasetSubset(size=10000, s3_prefix="test/subsets/10000")
        
        dataset.add_subset(1000, subset1)
        dataset.add_subset(5000, subset2)
        dataset.add_subset(10000, subset3)
        
        # Test exact retrieval
        assert dataset.get_subset(5000) == subset2
        
        # Test closest subset
        assert dataset.get_closest_subset(7500) == subset2
        assert dataset.get_closest_subset(15000) == subset3
        assert dataset.get_closest_subset(500) is None
        
        # Test listing
        assert dataset.list_subset_sizes() == [1000, 5000, 10000]
    
    def test_rdb_file_management(self):
        """Test RDB file addition and retrieval."""
        dataset = Dataset(name="test-dataset")
        
        # Add RDB files
        rdb1 = RDBFileInfo(
            filename="vectors.rdb",
            size_bytes=1000000,
            memory_usage_bytes=1200000,
            key_count=10000,
            creation_time=datetime.now(timezone.utc).isoformat(),
            valkey_version="7.0",
            loading_time_seconds=1.5
        )
        
        rdb2 = RDBFileInfo(
            filename="indexed.rdb",
            size_bytes=1500000,
            memory_usage_bytes=1800000,
            key_count=10000,
            creation_time=datetime.now(timezone.utc).isoformat(),
            valkey_version="7.0",
            loading_time_seconds=2.0,
            index_name="vector_idx",
            index_spec={"algorithm": "HNSW", "metric": "L2"}
        )
        
        dataset.add_rdb_file("vectors_only", rdb1)
        dataset.add_rdb_file("indexed_HNSW_L2", rdb2)
        
        # Test retrieval
        assert dataset.get_rdb_by_spec(None) == rdb1
        
        index_spec = IndexSpec(algorithm=IndexAlgorithm.HNSW, metric=MetricType.L2, dimension=128)
        assert dataset.get_rdb_by_spec(index_spec) == rdb2
    
    def test_component_management(self):
        """Test component tracking."""
        dataset = Dataset(name="test-dataset")
        
        dataset.add_component("train", "test-dataset/train.vkv")
        dataset.add_component("query", "test-dataset/query.vkv")
        dataset.add_component("metadata", "test-dataset/metadata.json")
        
        assert dataset.has_component("train") is True
        assert dataset.has_component("ground_truth") is False
        assert dataset.get_component("query") == "test-dataset/query.vkv"
    
    def test_serialization(self):
        """Test dataset serialization/deserialization."""
        # Create complex dataset
        dataset = Dataset(
            name="test-dataset",
            state=DatasetState.READY,
            source=DatasetSource(type="s3", location="s3://bucket/data.hdf5"),
            s3_bucket="test-bucket",
            s3_prefix="test-dataset",
            tags=["production", "1M-vectors"]
        )
        
        # Add components
        dataset.add_component("train", "test-dataset/train.vkv")
        
        # Add subset
        subset = DatasetSubset(size=10000, s3_prefix="test-dataset/subsets/10000")
        dataset.add_subset(10000, subset)
        
        # Add RDB
        rdb = RDBFileInfo(
            filename="vectors.rdb",
            size_bytes=1000000,
            memory_usage_bytes=1200000,
            key_count=10000,
            creation_time=datetime.now(timezone.utc).isoformat(),
            valkey_version="7.0",
            loading_time_seconds=1.5
        )
        dataset.add_rdb_file("vectors_only", rdb)
        
        # Serialize to JSON
        json_str = dataset.to_json()
        data = json.loads(json_str)
        
        assert data["name"] == "test-dataset"
        assert data["state"] == "ready"
        assert data["source"]["type"] == "s3"
        assert len(data["tags"]) == 2
        
        # Deserialize
        dataset2 = Dataset.from_json(json_str)
        
        assert dataset2.name == dataset.name
        assert dataset2.state == dataset.state
        assert dataset2.source.type == dataset.source.type
        assert dataset2.tags == dataset.tags
        assert len(dataset2.subsets) == 1
        assert len(dataset2.rdb_files) == 1
    
    def test_validation(self):
        """Test dataset validation."""
        # Empty dataset
        dataset = Dataset(name="")
        errors = dataset.validate()
        assert "Dataset name is required" in errors
        
        # VKV_CONVERTED without components
        dataset = Dataset(name="test", state=DatasetState.VKV_CONVERTED)
        errors = dataset.validate()
        assert "VKV_CONVERTED state requires components" in errors
        
        # READY without metadata
        dataset = Dataset(name="test", state=DatasetState.READY)
        errors = dataset.validate()
        assert "READY state requires complete metadata" in errors


class TestDatasetRegistry:
    """Test DatasetRegistry functionality."""
    
    def test_registry_operations(self):
        """Test basic registry operations."""
        registry = DatasetRegistry()
        
        # Register datasets (use S3_UPLOADED state to avoid metadata requirement)
        dataset1 = Dataset(name="dataset1", state=DatasetState.S3_UPLOADED, s3_bucket="test", s3_prefix="dataset1")
        dataset2 = Dataset(name="dataset2", state=DatasetState.VKV_CONVERTED)
        dataset2.add_component("train", "dataset2/train.vkv")  # Add required component
        dataset3 = Dataset(name="dataset3", state=DatasetState.S3_UPLOADED, tags=["test"], s3_bucket="test", s3_prefix="dataset3")
        
        assert registry.register(dataset1) is True
        assert registry.register(dataset2) is True
        assert registry.register(dataset3) is True
        
        # Get dataset
        assert registry.get("dataset1") == dataset1
        assert registry.get("nonexistent") is None
        
        # List datasets
        all_datasets = registry.list()
        assert len(all_datasets) == 3
        
        ready_datasets = registry.list(ready_only=True)
        assert len(ready_datasets) == 0  # No datasets in READY state
        
        vkv_datasets = registry.list(state=DatasetState.VKV_CONVERTED)
        assert len(vkv_datasets) == 1
        
        # Find by tag
        test_datasets = registry.find_by_tag("test")
        assert len(test_datasets) == 1
        assert test_datasets[0].name == "dataset3"
        
        # Remove dataset
        assert registry.remove("dataset2") is True
        assert len(registry.list()) == 2
    
    def test_registry_persistence(self):
        """Test saving and loading registry."""
        with tempfile.TemporaryDirectory() as tmpdir:
            registry_path = Path(tmpdir) / "registry.json"
            
            # Create and save registry
            registry1 = DatasetRegistry(registry_path=registry_path)
            
            dataset = Dataset(
                name="test-dataset",
                state=DatasetState.S3_UPLOADED,  # Use S3_UPLOADED instead of READY
                tags=["production"],
                s3_bucket="test",
                s3_prefix="test-dataset"
            )
            dataset.add_component("train", "test-dataset/train.vkv")
            
            registry1.register(dataset)
            registry1.save()
            
            # Load registry
            registry2 = DatasetRegistry.load(registry_path)
            
            assert len(registry2.list()) == 1
            loaded_dataset = registry2.get("test-dataset")
            assert loaded_dataset.name == "test-dataset"
            assert loaded_dataset.state == DatasetState.S3_UPLOADED
            assert loaded_dataset.tags == ["production"]
            assert loaded_dataset.has_component("train") is True
    
    def test_bulk_operations(self):
        """Test bulk state updates."""
        registry = DatasetRegistry()
        
        # Register multiple datasets
        for i in range(5):
            dataset = Dataset(name=f"dataset{i}", state=DatasetState.VKV_CONVERTED)
            dataset.add_component("train", f"dataset{i}/train.vkv")  # Add required component
            registry.register(dataset)
        
        # Bulk update
        names = [f"dataset{i}" for i in range(3)]
        results = registry.bulk_update_state(names, DatasetState.RDB_GENERATED)
        
        assert all(results.values())
        assert registry.get("dataset0").state == DatasetState.RDB_GENERATED
        assert registry.get("dataset3").state == DatasetState.VKV_CONVERTED
    
    def test_registry_statistics(self):
        """Test registry statistics."""
        registry = DatasetRegistry()
        
        # Add datasets in various states
        # Create a valid READY dataset with metadata
        ready_dataset1 = Dataset(name="d1", state=DatasetState.READY, s3_bucket="test", s3_prefix="d1")
        ready_dataset1.dataset_metadata = DatasetMetadata(
            dataset=DatasetInfo(name="d1", source="test", version="1.0", created_at="2024-01-01T00:00:00Z", processing_version="1.0"),
            vectors=VectorStats(count=1000, dimension=128, data_type="float32", norm_min=0.0, norm_max=1.0, norm_mean=0.5, norm_std=0.1),
            storage=StorageInfo(key_pattern="vec:*", key_encoding="utf-8", data_format="vkv", compression="zstd", total_size_bytes=10000),
            rdb_files={},
            checksums={}
        )
        
        ready_dataset2 = Dataset(name="d2", state=DatasetState.READY, s3_bucket="test", s3_prefix="d2")
        ready_dataset2.dataset_metadata = DatasetMetadata(
            dataset=DatasetInfo(name="d2", source="test", version="1.0", created_at="2024-01-01T00:00:00Z", processing_version="1.0"),
            vectors=VectorStats(count=1000, dimension=128, data_type="float32", norm_min=0.0, norm_max=1.0, norm_mean=0.5, norm_std=0.1),
            storage=StorageInfo(key_pattern="vec:*", key_encoding="utf-8", data_format="vkv", compression="zstd", total_size_bytes=10000),
            rdb_files={},
            checksums={}
        )
        
        registry.register(ready_dataset1)
        registry.register(ready_dataset2)
        d3 = Dataset(name="d3", state=DatasetState.VKV_CONVERTED)
        d3.add_component("train", "d3/train.vkv")  # Add required component
        registry.register(d3)
        registry.register(Dataset(name="d4", state=DatasetState.FAILED))
        
        # Add subsets and RDB files
        d1 = registry.get("d1")
        d1.add_subset(1000, DatasetSubset(size=1000, s3_prefix="d1/1000"))
        d1.add_subset(5000, DatasetSubset(size=5000, s3_prefix="d1/5000"))
        
        stats = registry.get_statistics()
        
        assert stats["total_datasets"] == 4
        assert stats["ready_datasets"] == 2
        assert stats["states"]["ready"] == 2
        assert stats["states"]["vkv_converted"] == 1
        assert stats["states"]["failed"] == 1
        assert stats["total_subsets"] == 2


class TestIntegrationWithMetadata:
    """Test integration with existing metadata classes."""
    
    def test_update_from_metadata(self):
        """Test updating dataset from DatasetMetadata."""
        # Create DatasetMetadata
        dataset_info = DatasetInfo(
            name="test-vectors",
            source="OpenAI",
            version="1.0",
            created_at=datetime.now(timezone.utc).isoformat(),
            processing_version="1.0.0",
            description="Test dataset"
        )
        
        vector_stats = VectorStats(
            count=10000,
            dimension=128,
            data_type="float32",
            norm_min=0.5,
            norm_max=2.0,
            norm_mean=1.0,
            norm_std=0.2
        )
        
        storage_info = StorageInfo(
            key_pattern="vec:*",
            key_encoding="utf-8",
            data_format="vkv",
            compression="zstd",
            total_size_bytes=1000000
        )
        
        rdb_info = RDBFileInfo(
            filename="vectors.rdb",
            size_bytes=1000000,
            memory_usage_bytes=1200000,
            key_count=10000,
            creation_time=datetime.now(timezone.utc).isoformat(),
            valkey_version="7.0",
            loading_time_seconds=1.5
        )
        
        metadata = DatasetMetadata(
            dataset=dataset_info,
            vectors=vector_stats,
            storage=storage_info,
            rdb_files={"vectors_only": rdb_info},
            checksums={"vectors.rdb": "abc123"}
        )
        
        # Create dataset and update
        dataset = Dataset(name="test-vectors")
        dataset.update_from_metadata(metadata)
        
        assert dataset.dataset_info == dataset_info
        assert dataset.dataset_metadata == metadata
        assert "vectors_only" in dataset.rdb_files
        assert dataset.rdb_files["vectors_only"] == rdb_info


if __name__ == "__main__":
    pytest.main([__file__, "-v"])