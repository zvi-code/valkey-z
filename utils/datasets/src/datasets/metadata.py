"""
Dataset Metadata Management

Comprehensive metadata structures for S3-stored vector datasets.
Handles dataset information, RDB statistics, and validation data.
"""

from dataclasses import dataclass, asdict
from typing import Dict, Any, List, Optional, Union
from datetime import datetime
from pathlib import Path
import json
import numpy as np


@dataclass
class VectorStats:
    """Statistics about vector data."""
    count: int
    dimension: int
    data_type: str  # "float32", "float64", "int8", "uint8"
    
    # Norm statistics
    norm_min: float
    norm_max: float
    norm_mean: float
    norm_std: float
    
    # Optional additional statistics
    sparsity_ratio: Optional[float] = None  # Ratio of zero values
    value_range_min: Optional[float] = None
    value_range_max: Optional[float] = None
    
    @classmethod
    def from_vectors(cls, vectors: np.ndarray) -> 'VectorStats':
        """Calculate statistics from vector array."""
        if vectors.ndim != 2:
            raise ValueError("Expected 2D array (vectors x dimensions)")
            
        count, dimension = vectors.shape
        data_type = str(vectors.dtype)
        
        # Calculate vector norms
        norms = np.linalg.norm(vectors, axis=1)
        norm_min = float(norms.min())
        norm_max = float(norms.max())  
        norm_mean = float(norms.mean())
        norm_std = float(norms.std())
        
        # Calculate sparsity ratio (ratio of zero values)
        total_elements = vectors.size
        zero_elements = np.count_nonzero(vectors == 0)
        sparsity_ratio = float(zero_elements / total_elements) if total_elements > 0 else 0.0
        
        # Value range across all dimensions
        value_range_min = float(vectors.min())
        value_range_max = float(vectors.max())
        
        return cls(
            count=count,
            dimension=dimension,
            data_type=data_type,
            norm_min=norm_min,
            norm_max=norm_max,
            norm_mean=norm_mean,
            norm_std=norm_std,
            sparsity_ratio=sparsity_ratio,
            value_range_min=value_range_min,
            value_range_max=value_range_max
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'VectorStats':
        """Create from dictionary."""
        return cls(**data)


@dataclass
class RDBFileInfo:
    """Information about an RDB file."""
    filename: str
    size_bytes: int
    memory_usage_bytes: int
    key_count: int
    creation_time: str  # ISO format
    valkey_version: str
    loading_time_seconds: float
    
    # Index-specific information (only for indexed RDBs)
    index_name: Optional[str] = None
    index_creation_time_seconds: Optional[float] = None
    index_spec: Optional[Dict[str, Any]] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'RDBFileInfo':
        """Create from dictionary."""
        return cls(**data)


@dataclass
class SubsetInfo:
    """Information about a dataset subset."""
    size: int
    path: str
    creation_time: str
    sampling_method: str  # "random", "sequential", "stratified"
    seed: Optional[int] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'SubsetInfo':
        """Create from dictionary."""
        return cls(**data)


@dataclass
class StorageInfo:
    """Information about data storage format."""
    key_pattern: str
    key_encoding: str
    data_format: str  # "vkv", "json", "hash"
    compression: str  # "none", "zstd", "lz4"
    total_size_bytes: int
    block_size: Optional[int] = None  # For VKV format
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'StorageInfo':
        """Create from dictionary.""" 
        return cls(**data)


@dataclass
class DatasetInfo:
    """Basic dataset information."""
    name: str
    source: str  # "OpenAI text-embedding-ada-002", "SIFT-1M", etc.
    version: str
    created_at: str  # ISO format
    processing_version: str
    description: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'DatasetInfo':
        """Create from dictionary."""
        return cls(**data)


@dataclass
class DatasetMetadata:
    """Complete metadata for a dataset in S3."""
    dataset: DatasetInfo
    vectors: VectorStats
    storage: StorageInfo
    rdb_files: Dict[str, RDBFileInfo]  # "vectors_only", "vectors_indexed"
    checksums: Dict[str, str]  # filename -> sha256 hash
    
    # Optional components
    subsets: Optional[List[SubsetInfo]] = None
    query_vectors: Optional[VectorStats] = None
    ground_truth_info: Optional[Dict[str, Any]] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert complete metadata to dictionary."""
        result = {
            'dataset': self.dataset.to_dict(),
            'vectors': self.vectors.to_dict(),
            'storage': self.storage.to_dict(),
            'rdb_files': {k: v.to_dict() for k, v in self.rdb_files.items()},
            'checksums': self.checksums.copy()
        }
        
        if self.subsets:
            result['subsets'] = [s.to_dict() for s in self.subsets]
        if self.query_vectors:
            result['query_vectors'] = self.query_vectors.to_dict()
        if self.ground_truth_info:
            result['ground_truth_info'] = self.ground_truth_info.copy()
            
        return result
    
    def to_json(self, indent: int = 2) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), indent=indent)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'DatasetMetadata':
        """Create from dictionary."""
        # Parse required fields
        dataset = DatasetInfo.from_dict(data['dataset'])
        vectors = VectorStats.from_dict(data['vectors'])
        storage = StorageInfo.from_dict(data['storage'])
        
        # Parse RDB files
        rdb_files = {k: RDBFileInfo.from_dict(v) for k, v in data['rdb_files'].items()}
        
        # Get checksums
        checksums = data.get('checksums', {})
        
        # Parse optional fields
        subsets = None
        if 'subsets' in data and data['subsets']:
            subsets = [SubsetInfo.from_dict(s) for s in data['subsets']]
            
        query_vectors = None
        if 'query_vectors' in data and data['query_vectors']:
            query_vectors = VectorStats.from_dict(data['query_vectors'])
            
        ground_truth_info = data.get('ground_truth_info')
        
        return cls(
            dataset=dataset,
            vectors=vectors,
            storage=storage,
            rdb_files=rdb_files,
            checksums=checksums,
            subsets=subsets,
            query_vectors=query_vectors,
            ground_truth_info=ground_truth_info
        )
    
    @classmethod
    def from_json(cls, json_str: str) -> 'DatasetMetadata':
        """Create from JSON string."""
        data = json.loads(json_str)
        return cls.from_dict(data)
    
    @classmethod
    def from_file(cls, file_path: Path) -> 'DatasetMetadata':
        """Load metadata from JSON file."""
        with open(file_path, 'r') as f:
            return cls.from_json(f.read())
    
    def save_to_file(self, file_path: Path):
        """Save metadata to JSON file."""
        with open(file_path, 'w') as f:
            f.write(self.to_json())
    
    def add_rdb_info(self, rdb_type: str, rdb_info: RDBFileInfo):
        """Add RDB file information."""
        self.rdb_files[rdb_type] = rdb_info
    
    def add_subset(self, subset_info: SubsetInfo):
        """Add subset information."""
        if self.subsets is None:
            self.subsets = []
        self.subsets.append(subset_info)
    
    def update_checksum(self, filename: str, checksum: str):
        """Update file checksum."""
        self.checksums[filename] = checksum
    
    def get_total_storage_size(self) -> int:
        """Calculate total storage size across all files."""
        total = self.storage.total_size_bytes
        
        # Add RDB file sizes
        for rdb_info in self.rdb_files.values():
            total += rdb_info.size_bytes
            
        # Add subset file sizes if available  
        if self.subsets:
            for subset in self.subsets:
                # Estimate subset size based on proportion
                proportion = subset.size / self.vectors.count
                total += int(self.storage.total_size_bytes * proportion)
                
        return total
    
    def get_memory_requirements(self, include_index: bool = True) -> Dict[str, int]:
        """Estimate memory requirements for loading dataset."""
        # Base vector memory (assuming float32 = 4 bytes per element)
        bytes_per_element = 4  # Could be dynamic based on data_type
        base_memory = self.vectors.count * self.vectors.dimension * bytes_per_element
        
        # Valkey overhead (approximately 25%)
        valkey_overhead = int(base_memory * 0.25)
        vectors_memory = base_memory + valkey_overhead
        
        result = {
            'vectors_only': vectors_memory,
            'base_vectors': base_memory,
            'valkey_overhead': valkey_overhead
        }
        
        # Add index memory if available
        if include_index and 'vectors_indexed' in self.rdb_files:
            rdb_info = self.rdb_files['vectors_indexed']
            # Estimate index memory as difference between indexed and non-indexed RDB
            if 'vectors_only' in self.rdb_files:
                index_memory_estimate = rdb_info.memory_usage_bytes - self.rdb_files['vectors_only'].memory_usage_bytes
            else:
                # Fallback estimate: 20% of vector memory for HNSW index
                index_memory_estimate = int(base_memory * 0.2)
                
            result['with_index'] = vectors_memory + index_memory_estimate
            result['index_estimate'] = index_memory_estimate
            
        return result
    
    def validate(self) -> List[str]:
        """Validate metadata consistency and completeness."""
        errors = []
        
        # Check required fields
        if not self.dataset.name:
            errors.append("Dataset name is required")
        if not self.dataset.source:
            errors.append("Dataset source is required")
        if self.vectors.count <= 0:
            errors.append("Vector count must be positive")
        if self.vectors.dimension <= 0:
            errors.append("Vector dimension must be positive")
            
        # Validate RDB files
        if not self.rdb_files:
            errors.append("At least one RDB file is required")
        else:
            for rdb_type, rdb_info in self.rdb_files.items():
                if rdb_info.key_count != self.vectors.count:
                    errors.append(f"RDB {rdb_type} key count ({rdb_info.key_count}) doesn't match vector count ({self.vectors.count})")
                    
        # Validate checksums
        for filename, checksum in self.checksums.items():
            if not checksum or len(checksum) != 64:  # SHA256 is 64 hex chars
                errors.append(f"Invalid checksum format for {filename}")
                
        # Validate subsets
        if self.subsets:
            for subset in self.subsets:
                if subset.size > self.vectors.count:
                    errors.append(f"Subset size ({subset.size}) exceeds dataset size ({self.vectors.count})")
                    
        return errors


class MetadataBuilder:
    """Helper class for building dataset metadata."""
    
    def __init__(self, dataset_name: str, source: str):
        """Initialize metadata builder."""
        self.dataset_name = dataset_name
        self.source = source
        self.creation_time = datetime.now().isoformat()
        self.processing_version = "1.0.0"  # Should be configurable
        
        # Initialize components
        self.vector_stats = None
        self.storage_info = None
        self.rdb_infos = {}
        self.checksums = {}
        self.subsets = []
        self.query_vectors = None
        self.ground_truth_info = None
        self.description = None
    
    def set_vector_stats(self, stats: VectorStats):
        """Set main vector statistics."""
        self.vector_stats = stats
        
    def set_description(self, description: str):
        """Set dataset description."""
        self.description = description
    
    def add_rdb_generation_results(self, rdb_type: str, 
                                  generation_stats: Dict[str, Any]):
        """Add RDB generation results to metadata."""
        # Extract relevant information from generation stats
        rdb_info = RDBFileInfo(
            filename=generation_stats['filename'],
            size_bytes=generation_stats['file_size_bytes'],
            memory_usage_bytes=generation_stats['memory_usage_bytes'],
            key_count=generation_stats['key_count'],
            creation_time=generation_stats.get('creation_time', datetime.now().isoformat()),
            valkey_version=generation_stats.get('valkey_version', 'unknown'),
            loading_time_seconds=generation_stats['loading_time_seconds'],
            index_name=generation_stats.get('index_name'),
            index_creation_time_seconds=generation_stats.get('index_creation_time_seconds'),
            index_spec=generation_stats.get('index_spec')
        )
        self.rdb_infos[rdb_type] = rdb_info
    
    def add_file_checksums(self, file_checksums: Dict[str, str]):
        """Add file checksums to metadata."""
        self.checksums.update(file_checksums)
    
    def set_storage_info(self, key_pattern: str, compression: str, 
                        total_size: int, **kwargs):
        """Set storage information."""
        self.storage_info = StorageInfo(
            key_pattern=key_pattern,
            key_encoding=kwargs.get('key_encoding', 'utf-8'),
            data_format=kwargs.get('data_format', 'vkv'),
            compression=compression,
            total_size_bytes=total_size,
            block_size=kwargs.get('block_size')
        )
    
    def add_subset_info(self, size: int, path: str, sampling_method: str, seed: Optional[int] = None):
        """Add subset information."""
        subset_info = SubsetInfo(
            size=size,
            path=path,
            creation_time=datetime.now().isoformat(),
            sampling_method=sampling_method,
            seed=seed
        )
        self.subsets.append(subset_info)
    
    def set_query_vectors(self, stats: VectorStats):
        """Set query vector statistics."""
        self.query_vectors = stats
        
    def set_ground_truth_info(self, info: Dict[str, Any]):
        """Set ground truth information."""
        self.ground_truth_info = info
    
    def build(self) -> DatasetMetadata:
        """Build complete DatasetMetadata instance."""
        # Validate required information
        if not self.vector_stats:
            raise ValueError("Vector statistics are required")
        if not self.storage_info:
            raise ValueError("Storage information is required")
        if not self.rdb_infos:
            raise ValueError("At least one RDB file info is required")
            
        # Create dataset info
        dataset_info = DatasetInfo(
            name=self.dataset_name,
            source=self.source,
            version="1.0",  # Could be configurable
            created_at=self.creation_time,
            processing_version=self.processing_version,
            description=self.description
        )
        
        # Build metadata
        metadata = DatasetMetadata(
            dataset=dataset_info,
            vectors=self.vector_stats,
            storage=self.storage_info,
            rdb_files=self.rdb_infos,
            checksums=self.checksums,
            subsets=self.subsets if self.subsets else None,
            query_vectors=self.query_vectors,
            ground_truth_info=self.ground_truth_info
        )
        
        # Validate before returning
        validation_errors = metadata.validate()
        if validation_errors:
            raise ValueError(f"Metadata validation failed: {validation_errors}")
            
        return metadata


class MetadataValidator:
    """Validates dataset metadata for consistency."""
    
    def validate_metadata(self, metadata: DatasetMetadata) -> Dict[str, List[str]]:
        """Comprehensive metadata validation."""
        errors = {
            'critical': [],
            'warnings': [],
            'info': []
        }
        
        # Validate basic structure
        basic_errors = metadata.validate()
        errors['critical'].extend(basic_errors)
        
        # Check vector statistics consistency
        if metadata.vectors.norm_min < 0:
            errors['warnings'].append("Minimum norm is negative, which is unusual")
        if metadata.vectors.norm_max > 1000:
            errors['warnings'].append("Maximum norm is very large, check for outliers")
        if metadata.vectors.sparsity_ratio and metadata.vectors.sparsity_ratio > 0.9:
            errors['info'].append(f"Dataset is very sparse ({metadata.vectors.sparsity_ratio:.1%} zeros)")
            
        # Validate RDB information consistency
        if len(metadata.rdb_files) > 1:
            sizes = [rdb.memory_usage_bytes for rdb in metadata.rdb_files.values()]
            if max(sizes) / min(sizes) > 5:  # More than 5x difference
                errors['warnings'].append("Large memory usage difference between RDB files")
                
        # Check subset information
        if metadata.subsets:
            total_subset_size = sum(s.size for s in metadata.subsets)
            if total_subset_size > metadata.vectors.count * 2:
                errors['warnings'].append("Total subset size exceeds dataset size significantly")
                
        # Validate storage efficiency
        if metadata.storage.compression == 'none' and metadata.storage.total_size_bytes > 1e9:  # 1GB
            errors['info'].append("Large dataset without compression - consider enabling compression")
            
        return errors
    
    def validate_against_files(self, metadata: DatasetMetadata, 
                             file_paths: Dict[str, Path]) -> Dict[str, bool]:
        """Validate metadata against actual files."""
        results = {}
        
        for filename, expected_checksum in metadata.checksums.items():
            if filename not in file_paths:
                results[filename] = False
                continue
                
            file_path = file_paths[filename]
            if not file_path.exists():
                results[filename] = False
                continue
                
            # Check file size for RDB files
            if filename in [rdb.filename for rdb in metadata.rdb_files.values()]:
                actual_size = file_path.stat().st_size
                rdb_info = next(rdb for rdb in metadata.rdb_files.values() if rdb.filename == filename)
                if abs(actual_size - rdb_info.size_bytes) > 1024:  # Allow 1KB difference
                    results[filename] = False
                    continue
                    
            # TODO: Validate checksums (would require computing actual checksums)
            # For now, assume valid if file exists and size matches
            results[filename] = True
            
        return results
    
    def suggest_fixes(self, validation_errors: Dict[str, List[str]]) -> List[str]:
        """Suggest fixes for validation errors."""
        suggestions = []
        
        for error in validation_errors['critical']:
            if "Vector count" in error and "doesn't match" in error:
                suggestions.append("Regenerate RDB files to ensure consistent vector counts")
            elif "checksum format" in error:
                suggestions.append("Recalculate checksums using SHA256")
            elif "required" in error:
                suggestions.append("Ensure all required metadata fields are populated during preparation")
                
        for warning in validation_errors['warnings']:
            if "norm" in warning and "large" in warning:
                suggestions.append("Consider normalizing vectors or checking for data quality issues")
            elif "memory usage difference" in warning:
                suggestions.append("Review index configuration for memory efficiency")
                
        return suggestions
