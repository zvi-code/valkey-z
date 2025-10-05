"""
Dataset Format Converter

Converts various input formats (HDF5, BVECS, NPY, CSV, etc.) to unified VKV format.
Supports automatic format detection and handles different vector storage conventions.
"""

import numpy as np
from pathlib import Path
from typing import Iterator, Tuple, Optional, Dict, Any, List
from enum import Enum
import logging
from abc import ABC, abstractmethod
import struct
import time

from .formats import VKVWriter, DataType, CompressionType, VKVFormat

# Optional dependencies
try:
    import h5py
    HAS_H5PY = True
except ImportError:
    HAS_H5PY = False

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

logger = logging.getLogger(__name__)


class SourceFormat(Enum):
    """Supported input dataset formats."""
    HDF5 = "hdf5"
    BVECS = "bvecs" 
    FVECS = "fvecs"
    NPY = "npy"
    NPZ = "npz"
    CSV = "csv"
    PARQUET = "parquet"
    AUTO = "auto"


class BaseConverter(ABC):
    """Abstract base class for format converters."""
    
    @abstractmethod
    def detect_format(self, file_path: Path) -> bool:
        """Detect if file matches this converter's format."""
        pass
    
    @abstractmethod
    def get_metadata(self, file_path: Path) -> Dict[str, Any]:
        """Extract metadata from source file."""
        pass
    
    @abstractmethod
    def stream_vectors(self, file_path: Path, 
                      key_prefix: str = "vec") -> Iterator[Tuple[str, np.ndarray]]:
        """Stream vectors from source file with generated keys."""
        pass


class HDF5Converter(BaseConverter):
    """Converter for HDF5 format (OpenAI, HuggingFace datasets)."""
    
    def detect_format(self, file_path: Path) -> bool:
        """Check if file is HDF5 format."""
        if not HAS_H5PY:
            return False
            
        if not str(file_path).lower().endswith(('.h5', '.hdf5', '.hdf')):
            return False
            
        try:
            with h5py.File(file_path, 'r') as f:
                # Basic validation - should be able to open as HDF5
                return True
        except (OSError, IOError):
            return False
    
    def get_metadata(self, file_path: Path) -> Dict[str, Any]:
        """Extract metadata from HDF5 file."""
        if not HAS_H5PY:
            raise ImportError("h5py is required for HDF5 support")
            
        with h5py.File(file_path, 'r') as f:
            # Try common dataset names
            dataset_names = ['train', 'vectors', 'embeddings', 'data']
            dataset = None
            dataset_name = None
            
            for name in dataset_names:
                if name in f:
                    dataset = f[name]
                    dataset_name = name
                    break
            
            if dataset is None:
                # Fall back to first dataset found
                def find_datasets(name, obj):
                    if isinstance(obj, h5py.Dataset) and len(obj.shape) == 2:
                        return name
                datasets = []
                f.visititems(lambda name, obj: datasets.append(name) if isinstance(obj, h5py.Dataset) and len(obj.shape) == 2 else None)
                if datasets:
                    dataset_name = datasets[0]
                    dataset = f[dataset_name]
                else:
                    raise ValueError("No suitable 2D datasets found in HDF5 file")
            
            vector_count, dimension = dataset.shape
            data_type = str(dataset.dtype)
            
            # Extract any attributes
            attributes = dict(dataset.attrs) if hasattr(dataset, 'attrs') else {}
            
            return {
                'vector_count': vector_count,
                'dimension': dimension,
                'data_type': data_type,
                'dataset_name': dataset_name,
                'attributes': attributes,
                'file_size': file_path.stat().st_size
            }
    
    def stream_vectors(self, file_path: Path, 
                      key_prefix: str = "vec") -> Iterator[Tuple[str, np.ndarray]]:
        """Stream vectors from HDF5 file."""
        if not HAS_H5PY:
            raise ImportError("h5py is required for HDF5 support")
            
        with h5py.File(file_path, 'r') as f:
            metadata = self.get_metadata(file_path)
            dataset_name = metadata['dataset_name']
            dataset = f[dataset_name]
            
            chunk_size = 1000  # Process in chunks to manage memory
            vector_count = dataset.shape[0]
            
            for start_idx in range(0, vector_count, chunk_size):
                end_idx = min(start_idx + chunk_size, vector_count)
                chunk = dataset[start_idx:end_idx]
                
                for i, vector in enumerate(chunk):
                    vector_idx = start_idx + i
                    key = f"{key_prefix}:{dataset_name}:{vector_idx}"
                    yield key, vector


class BVECSConverter(BaseConverter):
    """Converter for BVECS/FVECS format (BIGANN datasets)."""
    
    def detect_format(self, file_path: Path) -> bool:
        """Check if file is BVECS/FVECS format."""
        extension = file_path.suffix.lower()
        if extension not in ['.bvecs', '.fvecs']:
            return False
            
        try:
            # Try to read first dimension value
            with open(file_path, 'rb') as f:
                dim_bytes = f.read(4)
                if len(dim_bytes) != 4:
                    return False
                dimension = struct.unpack('<I', dim_bytes)[0]
                # Dimension should be reasonable (between 1 and 100k)
                return 1 <= dimension <= 100000
        except (OSError, struct.error):
            return False
    
    def get_metadata(self, file_path: Path) -> Dict[str, Any]:
        """Extract metadata from BVECS/FVECS file."""
        extension = file_path.suffix.lower()
        is_float = extension == '.fvecs'
        data_type = 'float32' if is_float else 'uint8'
        element_size = 4 if is_float else 1
        
        with open(file_path, 'rb') as f:
            # Read dimension from first vector
            dim_bytes = f.read(4)
            dimension = struct.unpack('<I', dim_bytes)[0]
            
            # Calculate total vectors based on file size
            file_size = file_path.stat().st_size
            vector_size = 4 + (dimension * element_size)  # 4 bytes for dimension + vector data
            vector_count = file_size // vector_size
            
            return {
                'vector_count': vector_count,
                'dimension': dimension,
                'data_type': data_type,
                'element_size': element_size,
                'file_size': file_size
            }
    
    def stream_vectors(self, file_path: Path,
                      key_prefix: str = "vec") -> Iterator[Tuple[str, np.ndarray]]:
        """Stream vectors from BVECS/FVECS file."""
        extension = file_path.suffix.lower()
        is_float = extension == '.fvecs'
        dtype = np.float32 if is_float else np.uint8
        
        with open(file_path, 'rb') as f:
            vector_idx = 0
            
            while True:
                # Read dimension
                dim_bytes = f.read(4)
                if len(dim_bytes) != 4:
                    break  # End of file
                    
                dimension = struct.unpack('<I', dim_bytes)[0]
                
                # Read vector data
                vector_bytes = f.read(dimension * (4 if is_float else 1))
                if len(vector_bytes) != dimension * (4 if is_float else 1):
                    break  # Incomplete vector
                    
                # Convert to numpy array
                vector = np.frombuffer(vector_bytes, dtype=dtype)
                key = f"{key_prefix}:{vector_idx}"
                
                yield key, vector
                vector_idx += 1


class NPYConverter(BaseConverter):
    """Converter for NumPy array format."""
    
    def detect_format(self, file_path: Path) -> bool:
        """Check if file is NPY format."""
        if not str(file_path).lower().endswith('.npy'):
            return False
            
        try:
            # Try to load header without loading data
            with open(file_path, 'rb') as f:
                version = np.lib.format.read_magic(f)
                shape, fortran_order, dtype = np.lib.format.read_array_header_1_0(f)
                return len(shape) == 2  # Should be 2D array
        except (ValueError, OSError):
            return False
    
    def get_metadata(self, file_path: Path) -> Dict[str, Any]:
        """Extract metadata from NPY file."""
        with open(file_path, 'rb') as f:
            version = np.lib.format.read_magic(f)
            shape, fortran_order, dtype = np.lib.format.read_array_header_1_0(f)
            
            if len(shape) != 2:
                raise ValueError(f"Expected 2D array, got shape {shape}")
                
            vector_count, dimension = shape
            
            return {
                'vector_count': vector_count,
                'dimension': dimension,
                'data_type': str(dtype),
                'fortran_order': fortran_order,
                'file_size': file_path.stat().st_size
            }
    
    def stream_vectors(self, file_path: Path,
                      key_prefix: str = "vec") -> Iterator[Tuple[str, np.ndarray]]:
        """Stream vectors from NPY file."""
        # Try to use memory mapping for efficient access
        try:
            array = np.load(file_path, mmap_mode='r', allow_pickle=False)
        except ValueError as e:
            if "allow_pickle=False" in str(e):
                # Fall back to loading with pickle support if needed
                array = np.load(file_path, mmap_mode='r', allow_pickle=True)
            else:
                raise
        
        if array.ndim != 2:
            raise ValueError(f"Expected 2D array, got shape {array.shape}")
            
        for i, vector in enumerate(array):
            key = f"{key_prefix}:{i}"
            # Make a copy to avoid memory mapping issues
            yield key, vector.copy()


class NPZConverter(BaseConverter):
    """Converter for compressed NumPy archive format."""
    
    def detect_format(self, file_path: Path) -> bool:
        """Check if file is NPZ format."""
        if not str(file_path).lower().endswith('.npz'):
            return False
            
        try:
            with np.load(file_path, allow_pickle=False) as data:
                # Check if it contains at least one 2D array
                return any(arr.ndim == 2 for arr in data.values())
        except ValueError as e:
            if "allow_pickle=False" in str(e):
                try:
                    with np.load(file_path, allow_pickle=True) as data:
                        return any(arr.ndim == 2 for arr in data.values())
                except (ValueError, OSError):
                    return False
            return False
        except (ValueError, OSError):
            return False
    
    def get_metadata(self, file_path: Path) -> Dict[str, Any]:
        """Extract metadata from NPZ file."""
        try:
            with np.load(file_path, allow_pickle=False) as data:
                return self._extract_npz_metadata(data, file_path)
        except ValueError as e:
            if "allow_pickle=False" in str(e):
                with np.load(file_path, allow_pickle=True) as data:
                    return self._extract_npz_metadata(data, file_path)
            else:
                raise
    
    def _extract_npz_metadata(self, data, file_path: Path) -> Dict[str, Any]:
        """Helper method to extract metadata from loaded NPZ data."""
        arrays_info = {}
        
        # Common array names to look for
        priority_names = ['train', 'test', 'vectors', 'embeddings', 'data']
        
        for name in data.files:
            arr = data[name]
            if arr.ndim == 2:
                vector_count, dimension = arr.shape
                arrays_info[name] = {
                    'vector_count': vector_count,
                    'dimension': dimension,
                    'data_type': str(arr.dtype)
                }
        
        if not arrays_info:
            raise ValueError("No 2D arrays found in NPZ file")
            
        # Select primary array (prefer priority names)
        primary_array_name = None
        for name in priority_names:
            if name in arrays_info:
                primary_array_name = name
                break
        
        if primary_array_name is None:
            primary_array_name = list(arrays_info.keys())[0]
            
        primary_info = arrays_info[primary_array_name]
        primary_info.update({
            'primary_array': primary_array_name,
            'all_arrays': arrays_info,
            'file_size': file_path.stat().st_size
        })
        
        return primary_info
    
    def stream_vectors(self, file_path: Path,
                      key_prefix: str = "vec") -> Iterator[Tuple[str, np.ndarray]]:
        """Stream vectors from NPZ file."""
        try:
            with np.load(file_path, allow_pickle=False) as data:
                metadata = self.get_metadata(file_path)
                primary_array_name = metadata['primary_array']
                array = data[primary_array_name]
                
                for i, vector in enumerate(array):
                    key = f"{key_prefix}:{primary_array_name}:{i}"
                    yield key, vector.copy()
        except ValueError as e:
            if "allow_pickle=False" in str(e):
                with np.load(file_path, allow_pickle=True) as data:
                    metadata = self.get_metadata(file_path)
                    primary_array_name = metadata['primary_array']
                    array = data[primary_array_name]
                    
                    for i, vector in enumerate(array):
                        key = f"{key_prefix}:{primary_array_name}:{i}"
                        yield key, vector.copy()
            else:
                raise


class CSVConverter(BaseConverter):
    """Converter for CSV format datasets."""
    
    def detect_format(self, file_path: Path) -> bool:
        """Check if file is CSV format."""
        if not str(file_path).lower().endswith('.csv'):
            return False
            
        try:
            # Try to read first few lines
            with open(file_path, 'r') as f:
                lines = [f.readline().strip() for _ in range(3)]
                # Basic CSV validation - should have consistent comma count
                if len(lines) < 2:
                    return False
                comma_counts = [line.count(',') for line in lines if line]
                return len(set(comma_counts)) == 1  # All lines should have same comma count
        except (OSError, UnicodeDecodeError):
            return False
    
    def get_metadata(self, file_path: Path) -> Dict[str, Any]:
        """Extract metadata from CSV file."""
        if not HAS_PANDAS:
            raise ImportError("pandas is required for CSV support")
            
        # Read just the header and first row to determine structure
        sample_df = pd.read_csv(file_path, nrows=1)
        
        # Count total rows efficiently
        with open(file_path, 'r') as f:
            vector_count = sum(1 for line in f) - 1  # Subtract header
            
        # Determine vector columns (assume all numeric columns except 'id', 'key', 'label')
        exclude_cols = {'id', 'key', 'label', 'class', 'target'}
        vector_cols = [col for col in sample_df.columns 
                      if col.lower() not in exclude_cols and 
                      pd.api.types.is_numeric_dtype(sample_df[col])]
        
        dimension = len(vector_cols)
        
        return {
            'vector_count': vector_count,
            'dimension': dimension,
            'data_type': str(sample_df[vector_cols].dtypes.iloc[0]),
            'vector_columns': vector_cols,
            'all_columns': list(sample_df.columns),
            'file_size': file_path.stat().st_size
        }
    
    def stream_vectors(self, file_path: Path,
                      key_prefix: str = "vec") -> Iterator[Tuple[str, np.ndarray]]:
        """Stream vectors from CSV file."""
        if not HAS_PANDAS:
            raise ImportError("pandas is required for CSV support")
            
        metadata = self.get_metadata(file_path)
        vector_cols = metadata['vector_columns']
        
        # Stream in chunks to manage memory
        chunk_size = 1000
        for chunk_idx, chunk in enumerate(pd.read_csv(file_path, chunksize=chunk_size)):
            for row_idx, row in chunk.iterrows():
                # Use ID column if available, otherwise generate key
                if 'id' in chunk.columns:
                    key = f"{key_prefix}:{row['id']}"
                elif 'key' in chunk.columns:
                    key = f"{key_prefix}:{row['key']}"
                else:
                    global_idx = chunk_idx * chunk_size + (row_idx % chunk_size)
                    key = f"{key_prefix}:{global_idx}"
                    
                vector = row[vector_cols].values.astype(np.float32)
                yield key, vector


class DatasetConverter:
    """Main converter that handles multiple input formats."""
    
    def __init__(self):
        """Initialize converter with all format handlers."""
        self.converters = {
            SourceFormat.HDF5: HDF5Converter(),
            SourceFormat.BVECS: BVECSConverter(), 
            SourceFormat.FVECS: BVECSConverter(),  # Same converter handles both
            SourceFormat.NPY: NPYConverter(),
            SourceFormat.NPZ: NPZConverter(),
            SourceFormat.CSV: CSVConverter(),
        }
        logger.info("Initialized DatasetConverter with format support")
    
    def detect_format(self, file_path: Path) -> SourceFormat:
        """Auto-detect input file format."""
        file_path = Path(file_path)
        
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
            
        # Try each converter's detect_format method
        for format_type, converter in self.converters.items():
            try:
                if converter.detect_format(file_path):
                    logger.info(f"Detected format {format_type.value} for {file_path}")
                    return format_type
            except Exception as e:
                logger.debug(f"Format detection failed for {format_type.value}: {e}")
                
        raise ValueError(f"Could not detect format for file: {file_path}")
    
    def get_source_metadata(self, file_path: Path, 
                           format_hint: Optional[SourceFormat] = None) -> Dict[str, Any]:
        """Extract metadata from source file."""
        file_path = Path(file_path)
        
        # Use format hint if provided, otherwise auto-detect
        if format_hint:
            source_format = format_hint
        else:
            source_format = self.detect_format(file_path)
            
        converter = self.converters[source_format]
        metadata = converter.get_metadata(file_path)
        
        # Add common metadata fields
        metadata.update({
            'source_format': source_format.value,
            'source_path': str(file_path),
            'file_size': file_path.stat().st_size
        })
        
        logger.info(f"Extracted metadata: {metadata['vector_count']} vectors, "
                   f"dimension {metadata['dimension']}, format {source_format.value}")
        
        return metadata
    
    def convert_to_vkv(self, source_path: Path, output_path: Path,
                       dataset_name: str,
                       format_hint: Optional[SourceFormat] = None,
                       compression: CompressionType = CompressionType.ZSTD,
                       block_size: int = 1000,
                       key_prefix: str = "vec") -> Dict[str, Any]:
        """Convert source dataset to VKV format."""
        source_path = Path(source_path)
        output_path = Path(output_path)
        
        logger.info(f"Starting conversion of {source_path} to {output_path}")
        
        # Auto-detect format if not provided
        if format_hint:
            source_format = format_hint
        else:
            source_format = self.detect_format(source_path)
            
        # Get converter and source metadata
        converter = self.converters[source_format]
        source_metadata = converter.get_metadata(source_path)
        
        # Determine optimal data type for vectors
        source_dtype = source_metadata['data_type']
        if 'float' in source_dtype:
            if 'float64' in source_dtype:
                vkv_data_type = DataType.FLOAT64
            else:
                vkv_data_type = DataType.FLOAT32
        elif 'int8' in source_dtype:
            vkv_data_type = DataType.INT8
        elif 'uint8' in source_dtype:
            vkv_data_type = DataType.UINT8
        else:
            # Default to float32 for unknown types
            vkv_data_type = DataType.FLOAT32
            logger.warning(f"Unknown source dtype {source_dtype}, defaulting to float32")
        
        # Create output directory if needed
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Convert using VKVWriter
        vectors_written = 0
        start_time = time.time()
        
        try:
            with VKVWriter(
                output_path, 
                source_metadata['dimension'],
                vkv_data_type,
                compression,
                block_size
            ) as writer:
                
                vector_stream = converter.stream_vectors(source_path, key_prefix)
                
                # Convert vectors in batches
                batch = []
                batch_size = 1000
                
                for key, vector in vector_stream:
                    # Convert vector to target dtype if needed
                    target_dtype = VKVFormat.NUMPY_DTYPES[vkv_data_type]
                    if vector.dtype != target_dtype:
                        vector = vector.astype(target_dtype)
                        
                    batch.append((key, vector))
                    
                    if len(batch) >= batch_size:
                        writer.write_vectors(iter(batch))
                        vectors_written += len(batch)
                        batch = []
                        
                        if vectors_written % 10000 == 0:
                            logger.info(f"Converted {vectors_written} vectors...")
                
                # Write remaining vectors
                if batch:
                    writer.write_vectors(iter(batch))
                    vectors_written += len(batch)
                    
        except Exception as e:
            # Clean up on failure
            if output_path.exists():
                output_path.unlink()
            raise RuntimeError(f"Conversion failed: {e}") from e
            
        conversion_time = time.time() - start_time
        output_size = output_path.stat().st_size
        
        # Calculate compression ratio
        estimated_raw_size = (vectors_written * source_metadata['dimension'] * 
                            VKVFormat.DTYPE_SIZES[vkv_data_type])
        compression_ratio = estimated_raw_size / output_size if output_size > 0 else 1.0
        
        result = {
            'source_format': source_format.value,
            'vectors_converted': vectors_written,
            'output_size_bytes': output_size,
            'conversion_time_seconds': conversion_time,
            'compression_ratio': compression_ratio,
            'vkv_data_type': vkv_data_type.name,
            'block_size': block_size,
            'compression': compression.name
        }
        
        logger.info(f"Conversion completed: {vectors_written} vectors in {conversion_time:.1f}s, "
                   f"output size {output_size:,} bytes (compression ratio: {compression_ratio:.2f}x)")
        
        return result
    
    def split_dataset(self, source_path: Path, 
                     train_output: Path, query_output: Path, gt_output: Path,
                     train_vectors_key: str = "train",
                     query_vectors_key: str = "test", 
                     ground_truth_key: str = "neighbors") -> Dict[str, Any]:
        """Split multi-part dataset into separate VKV files."""
        source_path = Path(source_path)
        
        # This is primarily for NPZ files with multiple arrays
        if not source_path.suffix.lower() == '.npz':
            raise ValueError("Dataset splitting is currently only supported for NPZ files")
            
        results = {}
        
        try:
            with np.load(source_path, allow_pickle=False) as data:
                self._process_split_data(data, results, train_vectors_key, query_vectors_key, ground_truth_key,
                                        train_output, query_output, gt_output)
        except ValueError as e:
            if "allow_pickle=False" in str(e):
                with np.load(source_path, allow_pickle=True) as data:
                    self._process_split_data(data, results, train_vectors_key, query_vectors_key, ground_truth_key,
                                            train_output, query_output, gt_output)
            else:
                raise
        
        return results
    
    def _process_split_data(self, data, results, train_vectors_key, query_vectors_key, ground_truth_key,
                           train_output, query_output, gt_output):
        """Helper method to process split data from loaded NPZ file."""
        # Process train vectors
        if train_vectors_key in data:
            train_result = self._convert_array_to_vkv(
                data[train_vectors_key], train_output, f"train"
            )
            results['train'] = train_result
            
        # Process query vectors  
        if query_vectors_key in data:
            query_result = self._convert_array_to_vkv(
                data[query_vectors_key], query_output, f"query"
            )
            results['query'] = query_result
            
        # Process ground truth (if present)
        if ground_truth_key in data and gt_output:
            # Ground truth is typically integer indices, not vectors
            gt_array = data[ground_truth_key]
            # Save as NPY for now since it's not vector data
            np.save(gt_output, gt_array)
            results['ground_truth'] = {
                'format': 'npy',
                'shape': gt_array.shape,
                'dtype': str(gt_array.dtype),
                'file_size': gt_output.stat().st_size
            }
            
        logger.info(f"Split dataset into {len(results)} components")
    
    def validate_conversion(self, source_path: Path, vkv_path: Path,
                           sample_count: int = 100) -> bool:
        """Validate VKV conversion by comparing samples."""
        from .formats import VKVReader
        
        logger.info(f"Validating conversion with {sample_count} samples")
        
        # Get source format and converter
        source_format = self.detect_format(source_path)
        converter = self.converters[source_format]
        
        # Read samples from both source and VKV
        source_samples = {}
        sample_keys = []
        
        # Collect samples from source
        for i, (key, vector) in enumerate(converter.stream_vectors(source_path)):
            if i >= sample_count:
                break
            source_samples[key] = vector
            sample_keys.append(key)
            
        # Read corresponding samples from VKV
        with VKVReader(vkv_path) as reader:
            vkv_metadata = reader.get_metadata()
            
            for batch in reader.stream_vectors(max_vectors=sample_count):
                for key, vector in batch:
                    if key in source_samples:
                        source_vector = source_samples[key]
                        
                        # Compare with appropriate tolerance
                        if not np.allclose(source_vector, vector, rtol=1e-5, atol=1e-8):
                            logger.error(f"Vector mismatch for key {key}")
                            return False
                            
        logger.info("Conversion validation passed")
        return True
    
    def estimate_vkv_size(self, source_path: Path, 
                         compression: CompressionType = CompressionType.ZSTD) -> int:
        """Estimate output VKV file size."""
        source_metadata = self.get_source_metadata(source_path)
        
        vector_count = source_metadata['vector_count']
        dimension = source_metadata['dimension']
        
        # Estimate raw vector data size (assume float32)
        vector_data_size = vector_count * dimension * 4
        
        # Estimate key overhead (assume average key length of 20 chars)
        key_overhead = vector_count * (2 + 20)  # 2 bytes length + 20 chars
        
        # Add VKV format overhead (headers, block structure)
        format_overhead = VKVFormat.HEADER_SIZE + (vector_count // 1000) * VKVFormat.BLOCK_HEADER_SIZE
        
        raw_size = vector_data_size + key_overhead + format_overhead
        
        # Apply compression ratio estimates
        compression_ratios = {
            CompressionType.NONE: 1.0,
            CompressionType.ZSTD: 0.3,  # 70% compression typically
            CompressionType.LZ4: 0.5,   # 50% compression typically
        }
        
        compression_ratio = compression_ratios.get(compression, 0.3)
        estimated_size = int(raw_size * compression_ratio)
        
        logger.debug(f"Estimated VKV size: {estimated_size:,} bytes "
                    f"(compression: {compression.name}, ratio: {compression_ratio})")
        
        return estimated_size
    
    def _convert_array_to_vkv(self, array: np.ndarray, output_path: Path, 
                             key_prefix: str) -> Dict[str, Any]:
        """Helper to convert numpy array to VKV format."""
        if array.ndim != 2:
            raise ValueError(f"Expected 2D array, got shape {array.shape}")
            
        vector_count, dimension = array.shape
        
        # Determine data type
        if array.dtype == np.float64:
            vkv_data_type = DataType.FLOAT64
        elif array.dtype == np.float32:
            vkv_data_type = DataType.FLOAT32
        elif array.dtype == np.int8:
            vkv_data_type = DataType.INT8
        elif array.dtype == np.uint8:
            vkv_data_type = DataType.UINT8
        else:
            vkv_data_type = DataType.FLOAT32
            array = array.astype(np.float32)
            
        # Write to VKV
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        with VKVWriter(output_path, dimension, vkv_data_type) as writer:
            vectors = [(f"{key_prefix}:{i}", vector) for i, vector in enumerate(array)]
            writer.write_vectors(iter(vectors))
            
        return {
            'vector_count': vector_count,
            'dimension': dimension,
            'data_type': vkv_data_type.name,
            'file_size': output_path.stat().st_size
        }


class DatasetSplitter:
    """Utilities for creating dataset subsets."""
    
    def create_random_subset(self, source_vkv: Path, output_vkv: Path,
                            subset_size: int, seed: int = 42) -> Dict[str, Any]:
        """Create random subset of vectors from source VKV file."""
        from .formats import VKVReader
        
        np.random.seed(seed)
        
        # Get source metadata
        with VKVReader(source_vkv) as reader:
            metadata = reader.get_metadata()
            total_vectors = metadata['vector_count']
            
            if subset_size >= total_vectors:
                raise ValueError(f"Subset size {subset_size} >= total vectors {total_vectors}")
                
            # Generate random indices
            selected_indices = np.random.choice(total_vectors, size=subset_size, replace=False)
            selected_indices = set(selected_indices)
            
            # Create output writer
            output_vkv.parent.mkdir(parents=True, exist_ok=True)
            
            # Determine data type from metadata
            data_type_map = {
                'float32': DataType.FLOAT32,
                'float64': DataType.FLOAT64,
                'int8': DataType.INT8,
                'uint8': DataType.UINT8
            }
            vkv_data_type = data_type_map.get(metadata['data_type'], DataType.FLOAT32)
            
            vectors_written = 0
            
            with VKVWriter(output_vkv, metadata['dimension'], vkv_data_type) as writer:
                current_index = 0
                
                for batch in reader.stream_vectors():
                    selected_batch = []
                    
                    for key, vector in batch:
                        if current_index in selected_indices:
                            selected_batch.append((key, vector))
                            vectors_written += 1
                        current_index += 1
                        
                    if selected_batch:
                        writer.write_vectors(iter(selected_batch))
                        
                    if vectors_written >= subset_size:
                        break
                        
        result = {
            'subset_size': vectors_written,
            'original_size': total_vectors,
            'sampling_method': 'random',
            'seed': seed,
            'file_size': output_vkv.stat().st_size
        }
        
        logger.info(f"Created random subset: {vectors_written} vectors from {total_vectors}")
        return result
    
    def create_sequential_subset(self, source_vkv: Path, output_vkv: Path,
                               subset_size: int, start_index: int = 0) -> Dict[str, Any]:
        """Create sequential subset starting from given index."""
        from .formats import VKVReader
        
        with VKVReader(source_vkv) as reader:
            metadata = reader.get_metadata()
            total_vectors = metadata['vector_count']
            
            if start_index + subset_size > total_vectors:
                actual_subset_size = total_vectors - start_index
                logger.warning(f"Requested subset size {subset_size} reduced to {actual_subset_size}")
            else:
                actual_subset_size = subset_size
                
            # Create output writer
            output_vkv.parent.mkdir(parents=True, exist_ok=True)
            
            data_type_map = {
                'float32': DataType.FLOAT32,
                'float64': DataType.FLOAT64,
                'int8': DataType.INT8,
                'uint8': DataType.UINT8
            }
            vkv_data_type = data_type_map.get(metadata['data_type'], DataType.FLOAT32)
            
            vectors_written = 0
            
            with VKVWriter(output_vkv, metadata['dimension'], vkv_data_type) as writer:
                for batch in reader.stream_vectors(start_offset=start_index, max_vectors=actual_subset_size):
                    writer.write_vectors(iter(batch))
                    vectors_written += len(batch)
                    
        result = {
            'subset_size': vectors_written,
            'original_size': total_vectors,
            'sampling_method': 'sequential',
            'start_index': start_index,
            'file_size': output_vkv.stat().st_size
        }
        
        logger.info(f"Created sequential subset: {vectors_written} vectors starting from {start_index}")
        return result
    
    def create_stratified_subset(self, source_vkv: Path, output_vkv: Path,
                               subset_size: int, norm_bins: int = 10) -> Dict[str, Any]:
        """Create subset maintaining vector norm distribution."""
        from .formats import VKVReader
        
        with VKVReader(source_vkv) as reader:
            metadata = reader.get_metadata()
            total_vectors = metadata['vector_count']
            
            if subset_size >= total_vectors:
                raise ValueError(f"Subset size {subset_size} >= total vectors {total_vectors}")
                
            # First pass: analyze norms
            logger.info("Analyzing vector norms for stratified sampling...")
            norms = []
            vector_data = []
            
            for batch in reader.stream_vectors():
                for key, vector in batch:
                    norm = np.linalg.norm(vector)
                    norms.append(norm)
                    vector_data.append((key, vector))
                    
            norms = np.array(norms)
            
            # Create bins based on norm distribution
            bin_edges = np.histogram_bin_edges(norms, bins=norm_bins)
            bin_indices = np.digitize(norms, bin_edges) - 1
            bin_indices = np.clip(bin_indices, 0, norm_bins - 1)
            
            # Calculate samples per bin
            unique_bins, bin_counts = np.unique(bin_indices, return_counts=True)
            bin_proportions = bin_counts / len(norms)
            samples_per_bin = (bin_proportions * subset_size).astype(int)
            
            # Adjust for rounding errors
            total_samples = samples_per_bin.sum()
            if total_samples < subset_size:
                # Add remaining samples to largest bins
                diff = subset_size - total_samples
                largest_bins = np.argsort(samples_per_bin)[-diff:]
                samples_per_bin[largest_bins] += 1
                
            # Select vectors from each bin
            selected_vectors = []
            
            for bin_idx in unique_bins:
                bin_mask = bin_indices == bin_idx
                bin_vector_indices = np.where(bin_mask)[0]
                
                if len(bin_vector_indices) > 0:
                    n_samples = min(samples_per_bin[bin_idx], len(bin_vector_indices))
                    selected_indices = np.random.choice(bin_vector_indices, size=n_samples, replace=False)
                    
                    for idx in selected_indices:
                        selected_vectors.append(vector_data[idx])
                        
            # Write selected vectors
            output_vkv.parent.mkdir(parents=True, exist_ok=True)
            
            data_type_map = {
                'float32': DataType.FLOAT32,
                'float64': DataType.FLOAT64,
                'int8': DataType.INT8,
                'uint8': DataType.UINT8
            }
            vkv_data_type = data_type_map.get(metadata['data_type'], DataType.FLOAT32)
            
            with VKVWriter(output_vkv, metadata['dimension'], vkv_data_type) as writer:
                writer.write_vectors(iter(selected_vectors))
                
        result = {
            'subset_size': len(selected_vectors),
            'original_size': total_vectors,
            'sampling_method': 'stratified',
            'norm_bins': norm_bins,
            'bin_distribution': {f'bin_{i}': int(count) for i, count in enumerate(samples_per_bin)},
            'file_size': output_vkv.stat().st_size
        }
        
        logger.info(f"Created stratified subset: {len(selected_vectors)} vectors "
                   f"across {norm_bins} norm bins")
        return result
