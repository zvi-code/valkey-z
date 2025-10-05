"""
Dataset Class Architecture

Comprehensive Dataset class that serves as the central abstraction for representing 
datasets throughout their entire lifecycle, from source files to production-ready 
vector databases.
"""

from __future__ import annotations

from enum import Enum
from dataclasses import dataclass, field, asdict
from typing import Dict, Any, List, Optional, Tuple, Union
from pathlib import Path
from datetime import datetime, timezone
import json
import logging

from .metadata import DatasetInfo, DatasetMetadata, VectorStats, RDBFileInfo, SubsetInfo
from .rdb_generator import IndexSpec

logger = logging.getLogger(__name__)


class DatasetState(Enum):
    """Dataset lifecycle states."""
    SOURCE_ONLY = "source_only"
    VKV_CONVERTED = "vkv_converted" 
    RDB_GENERATED = "rdb_generated"
    S3_UPLOADED = "s3_uploaded"
    READY = "ready"
    FAILED = "failed"


@dataclass
class DatasetSource:
    """Represents the source of a dataset."""
    type: str  # "url", "local", "s3", "dataset_macro"
    location: str  # URL, file path, S3 path, or dataset macro name
    format: Optional[str] = None  # "hdf5", "parquet", "csv", "vkv", etc.
    metadata: Dict[str, Any] = field(default_factory=dict)
    downloaded_path: Optional[Path] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        data = asdict(self)
        if self.downloaded_path:
            data['downloaded_path'] = str(self.downloaded_path)
        return data
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> DatasetSource:
        """Create from dictionary."""
        if 'downloaded_path' in data and data['downloaded_path']:
            data['downloaded_path'] = Path(data['downloaded_path'])
        return cls(**data)


@dataclass
class DatasetSubset:
    """Represents a subset of a dataset with specific vector count."""
    size: int  # Number of vectors in this subset
    s3_prefix: str  # S3 path prefix for subset files
    vkv_path: Optional[str] = None  # S3 path to VKV file
    rdb_files: Dict[str, str] = field(default_factory=dict)  # type -> S3 path
    metadata: Dict[str, Any] = field(default_factory=dict)
    validation_status: Optional[str] = None
    creation_time: Optional[str] = None
    
    def get_rdb_by_spec(self, index_spec: Optional[IndexSpec]) -> Optional[str]:
        """Get RDB file path for specific index specification."""
        if index_spec is None:
            return self.rdb_files.get('vectors_only')
        
        # Generate key from index spec
        spec_key = self._generate_spec_key(index_spec)
        return self.rdb_files.get(spec_key)
    
    def _generate_spec_key(self, index_spec: IndexSpec) -> str:
        """Generate unique key for index specification."""
        # This should match the key generation in RDB management
        return f"indexed_{index_spec.algorithm.value}_{index_spec.metric.value}"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> DatasetSubset:
        """Create from dictionary."""
        return cls(**data)


@dataclass
class Dataset:
    """
    Comprehensive dataset representation that encapsulates all dataset information.
    
    This class serves as the central abstraction for datasets, tracking:
    - Source information and download status
    - S3 storage locations and organization
    - Dataset components (training data, ground truth, VKV files)
    - Subsets with different vector counts
    - RDB files for each configuration
    - Current state in the preparation pipeline
    """
    
    # Core identification
    name: str
    state: DatasetState = DatasetState.SOURCE_ONLY
    
    # Source information
    source: Optional[DatasetSource] = None
    
    # Metadata objects
    dataset_info: Optional[DatasetInfo] = None
    dataset_metadata: Optional[DatasetMetadata] = None
    
    # S3 organization
    s3_bucket: Optional[str] = None
    s3_prefix: Optional[str] = None
    
    # Component tracking
    components: Dict[str, str] = field(default_factory=dict)  # component -> S3 path
    
    # Subset management
    subsets: Dict[int, DatasetSubset] = field(default_factory=dict)  # size -> subset
    
    # RDB file mapping
    rdb_files: Dict[str, RDBFileInfo] = field(default_factory=dict)  # key -> RDB info
    
    # State tracking
    state_history: List[Tuple[str, str]] = field(default_factory=list)  # (timestamp, state)
    error_message: Optional[str] = None
    
    # Additional metadata
    tags: List[str] = field(default_factory=list)
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    updated_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    
    def __post_init__(self):
        """Initialize state history."""
        if not self.state_history:
            self.state_history.append((self.created_at, self.state.value))
    
    # State Management Methods
    
    def transition_to(self, new_state: DatasetState) -> bool:
        """
        Transition to a new state with validation.
        
        Args:
            new_state: Target state
            
        Returns:
            True if transition successful, False otherwise
        """
        if not self._is_valid_transition(self.state, new_state):
            logger.error(f"Invalid state transition: {self.state} -> {new_state}")
            return False
        
        self.state = new_state
        self.updated_at = datetime.now(timezone.utc).isoformat()
        self.state_history.append((self.updated_at, new_state.value))
        
        # Clear error message on successful transitions
        if new_state != DatasetState.FAILED:
            self.error_message = None
        
        logger.info(f"Dataset {self.name} transitioned to {new_state}")
        return True
    
    def _is_valid_transition(self, from_state: DatasetState, to_state: DatasetState) -> bool:
        """Validate state transitions."""
        valid_transitions = {
            DatasetState.SOURCE_ONLY: [DatasetState.VKV_CONVERTED, DatasetState.FAILED],
            DatasetState.VKV_CONVERTED: [DatasetState.RDB_GENERATED, DatasetState.FAILED],
            DatasetState.RDB_GENERATED: [DatasetState.S3_UPLOADED, DatasetState.FAILED],
            DatasetState.S3_UPLOADED: [DatasetState.READY, DatasetState.FAILED],
            DatasetState.READY: [DatasetState.FAILED],  # Can fail from ready state
            DatasetState.FAILED: [DatasetState.SOURCE_ONLY]  # Can restart from failed
        }
        
        return to_state in valid_transitions.get(from_state, [])
    
    def mark_failed(self, error_message: str):
        """Mark dataset as failed with error message."""
        self.transition_to(DatasetState.FAILED)
        self.error_message = error_message
        self.updated_at = datetime.now(timezone.utc).isoformat()
    
    def is_ready(self) -> bool:
        """Check if dataset is ready for use."""
        return self.state == DatasetState.READY
    
    def can_transition_to(self, target_state: DatasetState) -> bool:
        """Check if transition to target state is valid."""
        return self._is_valid_transition(self.state, target_state)
    
    # Subset Management Methods
    
    def add_subset(self, size: int, subset: DatasetSubset):
        """Add or update a subset."""
        self.subsets[size] = subset
        self.updated_at = datetime.now(timezone.utc).isoformat()
    
    def get_subset(self, size: int) -> Optional[DatasetSubset]:
        """Get subset by exact size."""
        return self.subsets.get(size)
    
    def get_closest_subset(self, target_size: int) -> Optional[DatasetSubset]:
        """Get closest subset that is smaller than or equal to target size."""
        valid_sizes = [s for s in self.subsets.keys() if s <= target_size]
        if not valid_sizes:
            return None
        
        closest_size = max(valid_sizes)
        return self.subsets[closest_size]
    
    def list_subset_sizes(self) -> List[int]:
        """Get list of available subset sizes."""
        return sorted(self.subsets.keys())
    
    # RDB File Management Methods
    
    def add_rdb_file(self, key: str, rdb_info: RDBFileInfo):
        """Add RDB file information."""
        self.rdb_files[key] = rdb_info
        self.updated_at = datetime.now(timezone.utc).isoformat()
    
    def get_rdb_by_spec(self, index_spec: Optional[IndexSpec], 
                        subset_size: Optional[int] = None) -> Optional[RDBFileInfo]:
        """Get RDB file for specific index specification and optional subset."""
        if subset_size:
            subset = self.get_subset(subset_size)
            if not subset:
                subset = self.get_closest_subset(subset_size)
            if subset:
                rdb_path = subset.get_rdb_by_spec(index_spec)
                if rdb_path:
                    # Look for RDB info by path
                    for rdb_info in self.rdb_files.values():
                        if rdb_info.filename == rdb_path:
                            return rdb_info
        else:
            # Look in main RDB files
            if index_spec is None:
                return self.rdb_files.get('vectors_only')
            
            spec_key = self._generate_spec_key(index_spec)
            return self.rdb_files.get(spec_key)
        
        return None
    
    def _generate_spec_key(self, index_spec: IndexSpec) -> str:
        """Generate unique key for index specification."""
        return f"indexed_{index_spec.algorithm.value}_{index_spec.metric.value}"
    
    # Component Management Methods
    
    def add_component(self, component_type: str, s3_path: str):
        """Add dataset component (train, query, ground_truth, etc.)."""
        self.components[component_type] = s3_path
        self.updated_at = datetime.now(timezone.utc).isoformat()
    
    def get_component(self, component_type: str) -> Optional[str]:
        """Get S3 path for component."""
        return self.components.get(component_type)
    
    def has_component(self, component_type: str) -> bool:
        """Check if component exists."""
        return component_type in self.components
    
    # Integration Methods
    
    def update_from_metadata(self, metadata: DatasetMetadata):
        """Update dataset from DatasetMetadata object."""
        self.dataset_metadata = metadata
        if metadata.dataset:
            self.dataset_info = metadata.dataset
        
        # Update RDB files
        for rdb_type, rdb_info in metadata.rdb_files.items():
            self.add_rdb_file(rdb_type, rdb_info)
        
        # Update subsets if present
        if metadata.subsets:
            for subset_info in metadata.subsets:
                subset = DatasetSubset(
                    size=subset_info.size,
                    s3_prefix=f"{self.s3_prefix}/subsets/{subset_info.size}",
                    metadata={"creation_time": subset_info.creation_time}
                )
                self.add_subset(subset_info.size, subset)
        
        self.updated_at = datetime.now(timezone.utc).isoformat()
    
    def get_s3_manifest(self) -> Dict[str, List[str]]:
        """Get manifest of all S3 objects for this dataset."""
        manifest = {
            "components": list(self.components.values()),
            "rdb_files": [],
            "subsets": {}
        }
        
        # Add RDB file paths
        for rdb_info in self.rdb_files.values():
            if hasattr(rdb_info, 'filename'):
                manifest["rdb_files"].append(f"{self.s3_prefix}/rdb/{rdb_info.filename}")
        
        # Add subset files
        for size, subset in self.subsets.items():
            subset_files = []
            if subset.vkv_path:
                subset_files.append(subset.vkv_path)
            subset_files.extend(subset.rdb_files.values())
            manifest["subsets"][size] = subset_files
        
        return manifest
    
    # Serialization Methods
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "name": self.name,
            "state": self.state.value,
            "source": self.source.to_dict() if self.source else None,
            "dataset_info": asdict(self.dataset_info) if self.dataset_info else None,
            "dataset_metadata": self.dataset_metadata.to_dict() if self.dataset_metadata else None,
            "s3_bucket": self.s3_bucket,
            "s3_prefix": self.s3_prefix,
            "components": self.components,
            "subsets": {k: v.to_dict() for k, v in self.subsets.items()},
            "rdb_files": {k: v.to_dict() for k, v in self.rdb_files.items()},
            "state_history": self.state_history,
            "error_message": self.error_message,
            "tags": self.tags,
            "created_at": self.created_at,
            "updated_at": self.updated_at
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> Dataset:
        """Create from dictionary."""
        # Parse enums
        state = DatasetState(data.get("state", DatasetState.SOURCE_ONLY.value))
        
        # Parse nested objects
        source = DatasetSource.from_dict(data["source"]) if data.get("source") else None
        
        dataset_info = None
        if data.get("dataset_info"):
            dataset_info = DatasetInfo(**data["dataset_info"])
        
        dataset_metadata = None  
        if data.get("dataset_metadata"):
            dataset_metadata = DatasetMetadata.from_dict(data["dataset_metadata"])
        
        # Parse subsets
        subsets = {}
        for size_str, subset_data in data.get("subsets", {}).items():
            subsets[int(size_str)] = DatasetSubset.from_dict(subset_data)
        
        # Parse RDB files
        rdb_files = {}
        for key, rdb_data in data.get("rdb_files", {}).items():
            rdb_files[key] = RDBFileInfo.from_dict(rdb_data)
        
        return cls(
            name=data["name"],
            state=state,
            source=source,
            dataset_info=dataset_info,
            dataset_metadata=dataset_metadata,
            s3_bucket=data.get("s3_bucket"),
            s3_prefix=data.get("s3_prefix"),
            components=data.get("components", {}),
            subsets=subsets,
            rdb_files=rdb_files,
            state_history=data.get("state_history", []),
            error_message=data.get("error_message"),
            tags=data.get("tags", []),
            created_at=data.get("created_at", datetime.now(timezone.utc).isoformat()),
            updated_at=data.get("updated_at", datetime.now(timezone.utc).isoformat())
        )
    
    def to_json(self, indent: int = 2) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), indent=indent)
    
    @classmethod
    def from_json(cls, json_str: str) -> Dataset:
        """Create from JSON string."""
        data = json.loads(json_str)
        return cls.from_dict(data)
    
    # Validation Methods
    
    def validate(self) -> List[str]:
        """Validate dataset consistency and completeness."""
        errors = []
        
        # Basic validation
        if not self.name:
            errors.append("Dataset name is required")
        
        # State-specific validation
        if self.state == DatasetState.VKV_CONVERTED:
            if not self.components:
                errors.append("VKV_CONVERTED state requires components")
        
        if self.state == DatasetState.RDB_GENERATED:
            if not self.rdb_files:
                errors.append("RDB_GENERATED state requires RDB files")
        
        if self.state == DatasetState.S3_UPLOADED:
            if not self.s3_bucket or not self.s3_prefix:
                errors.append("S3_UPLOADED state requires S3 location")
        
        if self.state == DatasetState.READY:
            if not self.dataset_metadata:
                errors.append("READY state requires complete metadata")
        
        # Validate subsets
        for size, subset in self.subsets.items():
            if not subset.s3_prefix:
                errors.append(f"Subset {size} missing S3 prefix")
        
        return errors


@dataclass
class DatasetRegistry:
    """Registry for managing multiple datasets."""
    
    datasets: Dict[str, Dataset] = field(default_factory=dict)
    registry_path: Optional[Path] = None
    
    def register(self, dataset: Dataset) -> bool:
        """Register a new dataset."""
        if dataset.name in self.datasets:
            logger.warning(f"Dataset {dataset.name} already registered, updating")
        
        validation_errors = dataset.validate()
        if validation_errors:
            logger.error(f"Dataset validation failed: {validation_errors}")
            return False
        
        self.datasets[dataset.name] = dataset
        logger.info(f"Registered dataset: {dataset.name}")
        
        if self.registry_path:
            self.save()
        
        return True
    
    def get(self, name: str) -> Optional[Dataset]:
        """Get dataset by name."""
        return self.datasets.get(name)
    
    def list(self, state: Optional[DatasetState] = None, 
             ready_only: bool = False) -> List[Dataset]:
        """List datasets with optional filtering."""
        datasets = list(self.datasets.values())
        
        if state:
            datasets = [d for d in datasets if d.state == state]
        
        if ready_only:
            datasets = [d for d in datasets if d.is_ready()]
        
        return datasets
    
    def remove(self, name: str) -> bool:
        """Remove dataset from registry."""
        if name in self.datasets:
            del self.datasets[name]
            logger.info(f"Removed dataset: {name}")
            
            if self.registry_path:
                self.save()
            
            return True
        return False
    
    def find_by_tag(self, tag: str) -> List[Dataset]:
        """Find datasets by tag."""
        return [d for d in self.datasets.values() if tag in d.tags]
    
    def find_by_source(self, source_type: str) -> List[Dataset]:
        """Find datasets by source type."""
        return [d for d in self.datasets.values() 
                if d.source and d.source.type == source_type]
    
    def bulk_update_state(self, names: List[str], new_state: DatasetState) -> Dict[str, bool]:
        """Update state for multiple datasets."""
        results = {}
        for name in names:
            dataset = self.get(name)
            if dataset:
                results[name] = dataset.transition_to(new_state)
            else:
                results[name] = False
        
        if self.registry_path:
            self.save()
        
        return results
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get registry statistics."""
        states = {}
        for dataset in self.datasets.values():
            state = dataset.state.value
            states[state] = states.get(state, 0) + 1
        
        total_subsets = sum(len(d.subsets) for d in self.datasets.values())
        total_rdb_files = sum(len(d.rdb_files) for d in self.datasets.values())
        
        return {
            "total_datasets": len(self.datasets),
            "states": states,
            "total_subsets": total_subsets,
            "total_rdb_files": total_rdb_files,
            "ready_datasets": len([d for d in self.datasets.values() if d.is_ready()])
        }
    
    # Persistence Methods
    
    def save(self, path: Optional[Path] = None):
        """Save registry to JSON file."""
        save_path = path or self.registry_path
        if not save_path:
            raise ValueError("No save path specified")
        
        data = {
            "version": "1.0",
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "datasets": {name: dataset.to_dict() 
                       for name, dataset in self.datasets.items()}
        }
        
        save_path.parent.mkdir(parents=True, exist_ok=True)
        with open(save_path, 'w') as f:
            json.dump(data, f, indent=2)
        
        logger.info(f"Saved registry to {save_path}")
    
    @classmethod
    def load(cls, path: Path) -> DatasetRegistry:
        """Load registry from JSON file."""
        if not path.exists():
            logger.warning(f"Registry file not found: {path}")
            return cls(registry_path=path)
        
        with open(path, 'r') as f:
            data = json.load(f)
        
        registry = cls(registry_path=path)
        
        for name, dataset_data in data.get("datasets", {}).items():
            try:
                dataset = Dataset.from_dict(dataset_data)
                registry.datasets[name] = dataset
            except Exception as e:
                logger.error(f"Failed to load dataset {name}: {e}")
        
        logger.info(f"Loaded {len(registry.datasets)} datasets from {path}")
        return registry