"""
Streaming API for S3 Vector Dataset Integration

Provides efficient streaming interfaces for reading and writing vector data
to/from S3, optimized for the stress testing framework.
"""

import asyncio
from pathlib import Path
from typing import AsyncIterator, Iterator, Tuple, Optional, Dict, Any, List
import numpy as np
from io import BytesIO
import logging

from .formats import VKVReader, VKVWriter, DataType, CompressionType
from .s3_manager import S3DatasetManager, S3StreamingReader


class VKVStreamReader:
    """Streaming reader for VKV files from S3 or local storage."""
    
    def __init__(self, source_path: str, s3_manager: Optional[S3DatasetManager] = None):
        self.source_path = source_path
        self.s3_manager = s3_manager
        self.is_s3 = source_path.startswith('s3://')
        self.reader = {}
        self.metadata = None
        self.logger = logging.getLogger(__name__)
        self._closed = False
    
    async def __aenter__(self):
        """Initialize the appropriate reader."""
        try:
            if self.is_s3:
                if not self.s3_manager:
                    raise ValueError("S3 manager required for S3 paths")
                
                # Parse S3 path
                path_parts = self.source_path.replace('s3://', '').split('/', 1)
                if len(path_parts) != 2:
                    raise ValueError(f"Invalid S3 path format: {self.source_path}")
                
                bucket, key = path_parts
                
                # Initialize S3 streaming reader
                self.reader = S3StreamingReader(
                    self.s3_manager.s3_client,
                    bucket, key
                )
                await self.reader.__aenter__()
                
            else:
                # Initialize local VKV reader
                local_path = Path(self.source_path)
                if not local_path.exists():
                    raise FileNotFoundError(f"VKV file not found: {local_path}")
                
                self.reader = VKVReader(local_path)
                await self.reader.__aenter__()
            
            # Load metadata
            self.metadata = await self._load_metadata()
            
            return self
            
        except Exception as e:
            self.logger.error(f"Failed to initialize VKV stream reader: {e}")
            await self._cleanup()
            raise
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Close reader and clean up resources."""
        await self._cleanup()
    
    async def _cleanup(self):
        """Clean up resources."""
        if not self._closed and self.reader:
            try:
                if hasattr(self.reader, '__aexit__'):
                    await self.reader.__aexit__(None, None, None)
                elif hasattr(self.reader, 'close'):
                    await self.reader.close()
            except Exception as e:
                self.logger.warning(f"Error during cleanup: {e}")
            finally:
                self._closed = True
    
    async def _load_metadata(self) -> Dict[str, Any]:
        """Load and parse metadata from the VKV file."""
        try:
            if hasattr(self.reader, 'get_metadata'):
                return await self.reader.get_metadata()
            elif hasattr(self.reader, 'metadata'):
                return self.reader.metadata
            else:
                # Try to read header info
                header = await self.reader.read_header()
                return {
                    'vector_count': header.get('vector_count', 0),
                    'dimension': header.get('dimension', 0),
                    'data_type': header.get('data_type', DataType.FLOAT32),
                    'compression': header.get('compression', CompressionType.NONE)
                }
        except Exception as e:
            self.logger.warning(f"Failed to load metadata: {e}")
            return {}
    
    async def get_metadata(self) -> Dict[str, Any]:
        """Get dataset metadata."""
        if self.metadata is None:
            self.metadata = await self._load_metadata()
        return self.metadata
    
    async def stream_vectors(self, start_offset: int = 0,
                           max_vectors: Optional[int] = None,
                           batch_size: int = 1000) -> AsyncIterator[List[Tuple[str, np.ndarray]]]:
        """Stream vectors in batches."""
        if self._closed or not self.reader:
            raise RuntimeError("Reader is closed or not initialized")
        
        try:
            metadata = await self.get_metadata()
            total_vectors = metadata.get('vector_count', 0)
            
            if start_offset >= total_vectors:
                self.logger.warning(f"Start offset {start_offset} >= total vectors {total_vectors}")
                return
            
            # Calculate effective range
            end_offset = total_vectors
            if max_vectors:
                end_offset = min(start_offset + max_vectors, total_vectors)
            
            self.logger.info(f"Streaming vectors {start_offset}-{end_offset} in batches of {batch_size}")
            
            current_offset = start_offset
            batch = []
            
            # Stream vectors from reader
            async for vector_data in self.reader.read_vectors():
                if current_offset < start_offset:
                    current_offset += 1
                    continue
                
                if current_offset >= end_offset:
                    break
                
                # Create vector key (can be customized)
                vector_key = f"vector_{current_offset}"
                
                # Convert bytes to numpy array if needed
                if isinstance(vector_data, bytes):
                    # Assume float32 for now (could be determined from metadata)
                    vector_array = np.frombuffer(vector_data, dtype=np.float32)
                else:
                    vector_array = vector_data
                
                batch.append((vector_key, vector_array))
                current_offset += 1
                
                # Yield batch when full
                if len(batch) >= batch_size:
                    yield batch
                    batch = []
            
            # Yield remaining vectors
            if batch:
                yield batch
                
        except Exception as e:
            self.logger.error(f"Error during vector streaming: {e}")
            raise
    
    async def stream_for_stress_test(self, batch_size: int = 1000,
                                   max_batches: Optional[int] = None) -> AsyncIterator[Dict[str, np.ndarray]]:
        """Stream vectors optimized for stress test ingestion."""
        batch_count = 0
        
        try:
            async for vector_batch in self.stream_vectors(batch_size=batch_size):
                if max_batches and batch_count >= max_batches:
                    break
                
                # Convert to dictionary format for Valkey operations
                vector_dict = {}
                for key, vector in vector_batch:
                    # Convert numpy array to bytes for Valkey storage
                    vector_dict[key] = vector.tobytes()
                
                yield vector_dict
                batch_count += 1
                
        except Exception as e:
            self.logger.error(f"Error during stress test streaming: {e}")
            raise
    
    async def get_sample_vectors(self, count: int = 100, 
                               random_seed: Optional[int] = None) -> List[Tuple[str, np.ndarray]]:
        """Get sample vectors for validation/testing."""
        import random
        
        if random_seed is not None:
            random.seed(random_seed)
        
        metadata = await self.get_metadata()
        total_vectors = metadata.get('vector_count', 0)
        
        if count >= total_vectors:
            # Return all vectors if sample size >= total
            samples = []
            async for batch in self.stream_vectors():
                samples.extend(batch)
            return samples
        else:
            # Sample random indices
            sample_indices = sorted(random.sample(range(total_vectors), count))
            samples = []
            current_index = 0
            
            async for batch in self.stream_vectors():
                for key, vector in batch:
                    if current_index in sample_indices:
                        samples.append((key, vector))
                        if len(samples) >= count:
                            return samples
                    current_index += 1
            
            return samples
    
    async def count_vectors(self) -> int:
        """Count total vectors in dataset."""
        metadata = await self.get_metadata()
        vector_count = metadata.get('vector_count', 0)
        
        if vector_count > 0:
            return vector_count
        
        # Fallback: count by streaming if metadata unavailable
        count = 0
        async for batch in self.stream_vectors():
            count += len(batch)
        
        return count


class VKVStreamWriter:
    """Streaming writer for VKV files to S3 or local storage."""
    
    def __init__(self, output_path: str, dimension: int,
                 data_type: DataType = DataType.FLOAT32,
                 compression: CompressionType = CompressionType.ZSTD,
                 block_size: int = 1000,
                 s3_manager: Optional[S3DatasetManager] = None):
        self.output_path = output_path
        self.dimension = dimension
        self.data_type = data_type
        self.compression = compression
        self.block_size = block_size
        self.s3_manager = s3_manager
        self.is_s3 = output_path.startswith('s3://')
        self.writer = None
        self.vector_count = 0
        self.buffer = []
        self.logger = logging.getLogger(__name__)
        self._start_time = None
    
    async def __aenter__(self):
        """Initialize appropriate writer (local or S3)."""
        self._start_time = asyncio.get_event_loop().time()
        
        try:
            if self.is_s3:
                if not self.s3_manager:
                    raise ValueError("S3 manager required for S3 destinations")
                
                # For S3, we'll write to a temporary local file first
                import tempfile
                self.temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.vkv')
                self.writer = VKVWriter(
                    Path(self.temp_file.name),
                    dimension=self.dimension,
                    data_type=self.data_type,
                    compression=self.compression
                )
            else:
                # Local file writer
                local_path = Path(self.output_path)
                local_path.parent.mkdir(parents=True, exist_ok=True)
                
                self.writer = VKVWriter(
                    local_path,
                    dimension=self.dimension,
                    data_type=self.data_type,
                    compression=self.compression
                )
            
            await self.writer.__aenter__()
            return self
            
        except Exception as e:
            self.logger.error(f"Failed to initialize VKV stream writer: {e}")
            raise
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Finalize write and upload."""
        try:
            # Write any remaining vectors in buffer
            if self.buffer:
                await self._write_buffer()
            
            # Close the writer
            if self.writer:
                await self.writer.__aexit__(exc_type, exc_val, exc_tb)
            
            # Upload to S3 if needed
            if self.is_s3 and hasattr(self, 'temp_file'):
                await self._upload_to_s3()
                
                # Clean up temporary file
                import os
                os.unlink(self.temp_file.name)
                
        except Exception as e:
            self.logger.error(f"Error during writer cleanup: {e}")
            if hasattr(self, 'temp_file'):
                import os
                try:
                    os.unlink(self.temp_file.name)
                except:
                    pass
            raise
    
    async def _upload_to_s3(self):
        """Upload temporary file to S3."""
        # Parse S3 path
        path_parts = self.output_path.replace('s3://', '').split('/', 1)
        if len(path_parts) != 2:
            raise ValueError(f"Invalid S3 path format: {self.output_path}")
        
        bucket, key = path_parts
        
        # Upload file to S3
        from .s3_manager import DatasetUploader
        uploader = DatasetUploader(self.s3_manager)
        
        upload_result = await uploader.upload_file(
            Path(self.temp_file.name),
            bucket,
            key
        )
        
        if not upload_result.get('success', False):
            raise RuntimeError(f"S3 upload failed: {upload_result.get('error', 'Unknown error')}")
        
        self.logger.info(f"Successfully uploaded VKV to S3: {self.output_path}")
    
    async def write_vectors(self, vectors: AsyncIterator[Tuple[str, np.ndarray]]):
        """Write vectors from async iterator."""
        async for key, vector in vectors:
            await self.write_vector(key, vector)
    
    async def write_vector(self, key: str, vector: np.ndarray):
        """Write a single vector."""
        if vector.shape != (self.dimension,):
            raise ValueError(f"Vector dimension mismatch: expected {self.dimension}, got {vector.shape}")
        
        self.buffer.append((key, vector))
        self.vector_count += 1
        
        # Write buffer when full
        if len(self.buffer) >= self.block_size:
            await self._write_buffer()
    
    async def write_vector_batch(self, vector_batch: List[Tuple[str, np.ndarray]]):
        """Write a batch of vectors."""
        for key, vector in vector_batch:
            await self.write_vector(key, vector)
    
    async def _write_buffer(self):
        """Write buffered vectors to the underlying writer."""
        if not self.buffer:
            return
        
        # Extract vectors from buffer
        vectors = [vector for _, vector in self.buffer]
        
        # Convert to appropriate format and write
        await self.writer.write_vectors(vectors)
        
        # Clear buffer
        self.buffer.clear()
    
    async def finalize(self) -> Dict[str, Any]:
        """Finalize writing and return statistics."""
        end_time = asyncio.get_event_loop().time()
        total_time = end_time - (self._start_time or end_time)
        
        # Get file size
        if self.is_s3 and hasattr(self, 'temp_file'):
            file_size = Path(self.temp_file.name).stat().st_size
        else:
            file_size = Path(self.output_path).stat().st_size if Path(self.output_path).exists() else 0
        
        return {
            'vector_count': self.vector_count,
            'file_size_bytes': file_size,
            'dimension': self.dimension,
            'data_type': self.data_type.name,
            'compression': self.compression.name,
            'write_time_seconds': total_time,
            'vectors_per_second': self.vector_count / max(total_time, 0.001),
            'output_path': self.output_path,
            'is_s3': self.is_s3
        }


class S3VectorStreamer:
    """High-level interface for streaming vectors to/from S3."""
    
    def __init__(self, s3_manager: S3DatasetManager):
        self.s3_manager = s3_manager
        self.logger = logging.getLogger(__name__)
    
    async def stream_dataset_for_loading(self, dataset_name: str,
                                       subset_size: Optional[int] = None,
                                       component: str = "train") -> VKVStreamReader:
        """Create stream reader for dataset loading."""
        # Build S3 path for dataset component
        if subset_size:
            s3_path = f"s3://{self.s3_manager.config.bucket}/{dataset_name}/subsets/subset_{subset_size}/{component}.vkv"
        else:
            s3_path = f"s3://{self.s3_manager.config.bucket}/{dataset_name}/{component}.vkv"
        
        self.logger.info(f"Creating stream reader for: {s3_path}")
        
        return VKVStreamReader(s3_path, self.s3_manager)
    
    async def create_dataset_writer(self, dataset_name: str,
                                  component: str,
                                  dimension: int,
                                  **kwargs) -> VKVStreamWriter:
        """Create stream writer for dataset upload."""
        # Build S3 destination path
        s3_path = f"s3://{self.s3_manager.config.bucket}/{dataset_name}/{component}.vkv"
        
        self.logger.info(f"Creating stream writer for: {s3_path}")
        
        return VKVStreamWriter(
            s3_path,
            dimension=dimension,
            s3_manager=self.s3_manager,
            **kwargs
        )
    
    async def copy_dataset_subset(self, source_dataset: str,
                                dest_dataset: str,
                                subset_size: int,
                                sampling_method: str = "sequential") -> Dict[str, Any]:
        """Copy subset of dataset to new location."""
        start_time = asyncio.get_event_loop().time()
        
        try:
            # Create source reader
            source_reader = await self.stream_dataset_for_loading(source_dataset)
            
            async with source_reader:
                # Get source metadata
                source_metadata = await source_reader.get_metadata()
                dimension = source_metadata.get('dimension', 0)
                total_vectors = source_metadata.get('vector_count', 0)
                
                if subset_size >= total_vectors:
                    self.logger.warning(f"Subset size {subset_size} >= total vectors {total_vectors}, copying all")
                    subset_size = total_vectors
                
                # Create destination writer
                dest_writer = await self.create_dataset_writer(
                    dest_dataset, "train", dimension
                )
                
                async with dest_writer:
                    copied_count = 0
                    
                    if sampling_method == "sequential":
                        # Sequential sampling - take first N vectors
                        async for batch in source_reader.stream_vectors(max_vectors=subset_size):
                            await dest_writer.write_vector_batch(batch)
                            copied_count += len(batch)
                            
                            if copied_count >= subset_size:
                                break
                                
                    elif sampling_method == "random":
                        # Random sampling - collect all vectors first, then sample
                        import random
                        all_vectors = []
                        
                        async for batch in source_reader.stream_vectors():
                            all_vectors.extend(batch)
                        
                        # Sample randomly
                        if len(all_vectors) > subset_size:
                            sampled_vectors = random.sample(all_vectors, subset_size)
                        else:
                            sampled_vectors = all_vectors
                        
                        await dest_writer.write_vector_batch(sampled_vectors)
                        copied_count = len(sampled_vectors)
                    
                    else:
                        raise ValueError(f"Unsupported sampling method: {sampling_method}")
                    
                    # Finalize and get stats
                    write_stats = await dest_writer.finalize()
                
                copy_time = asyncio.get_event_loop().time() - start_time
                
                return {
                    'success': True,
                    'source_dataset': source_dataset,
                    'dest_dataset': dest_dataset,
                    'subset_size': subset_size,
                    'copied_vectors': copied_count,
                    'sampling_method': sampling_method,
                    'copy_time': copy_time,
                    'write_stats': write_stats
                }
                
        except Exception as e:
            self.logger.error(f"Failed to copy dataset subset: {e}")
            return {
                'success': False,
                'error': str(e),
                'copy_time': asyncio.get_event_loop().time() - start_time
            }
    
    async def validate_dataset_stream(self, dataset_name: str,
                                    sample_count: int = 1000) -> Dict[str, Any]:
        """Validate dataset by streaming samples."""
        try:
            reader = await self.stream_dataset_for_loading(dataset_name)
            
            async with reader:
                metadata = await reader.get_metadata()
                expected_dim = metadata.get('dimension', 0)
                
                # Get sample vectors
                samples = await reader.get_sample_vectors(sample_count)
                
                # Validate samples
                valid_count = 0
                dimension_errors = 0
                nan_errors = 0
                type_errors = 0
                
                for key, vector in samples:
                    try:
                        # Check dimension
                        if len(vector.shape) != 1 or vector.shape[0] != expected_dim:
                            dimension_errors += 1
                            continue
                        
                        # Check for NaN values
                        if np.isnan(vector).any():
                            nan_errors += 1
                            continue
                        
                        # Check data type
                        if not np.issubdtype(vector.dtype, np.floating):
                            type_errors += 1
                            continue
                        
                        valid_count += 1
                        
                    except Exception:
                        type_errors += 1
                
                validation_rate = valid_count / len(samples) if samples else 0
                
                return {
                    'valid': validation_rate >= 0.95,  # 95% validation rate required
                    'dataset_name': dataset_name,
                    'sample_count': len(samples),
                    'valid_vectors': valid_count,
                    'validation_rate': validation_rate,
                    'dimension_errors': dimension_errors,
                    'nan_errors': nan_errors,
                    'type_errors': type_errors,
                    'expected_dimension': expected_dim,
                    'metadata': metadata
                }
                
        except Exception as e:
            self.logger.error(f"Dataset validation failed: {e}")
            return {
                'valid': False,
                'dataset_name': dataset_name,
                'error': str(e)
            }


class StressTestStreamer:
    """Specialized streamer for stress test integration."""
    
    def __init__(self, s3_manager: Optional[S3DatasetManager] = None):
        self.s3_manager = s3_manager
        self.logger = logging.getLogger(__name__)
    
    async def stream_for_ingestion_workload(self, dataset_source: str,
                                          batch_size: int = 1000,
                                          rate_limit: Optional[float] = None,
                                          key_prefix: str = "vec") -> AsyncIterator[Dict[str, np.ndarray]]:
        """Stream vectors optimized for ingestion workload."""
        # TODO: Open dataset stream (S3 or local)
        # TODO: Stream in batches suitable for Valkey operations
        # TODO: Apply rate limiting if specified
        # TODO: Format keys for stress test conventions
        # TODO: Yield {key: vector} dictionaries
        pass
    
    async def stream_query_vectors(self, dataset_source: str,
                                 query_count: int,
                                 random_seed: Optional[int] = None) -> List[np.ndarray]:
        """Stream query vectors for search workload."""
        # TODO: Load query component of dataset
        # TODO: Sample requested number of query vectors
        # TODO: Return list of query vectors
        pass
    
    async def stream_ground_truth(self, dataset_source: str,
                                query_indices: List[int]) -> Dict[int, List[int]]:
        """Stream ground truth data for recall calculation."""
        # TODO: Load ground truth component
        # TODO: Extract ground truth for specified query indices
        # TODO: Return mapping of query index to neighbor indices
        pass
    
    def estimate_streaming_performance(self, dataset_name: str,
                                     batch_size: int = 1000) -> Dict[str, float]:
        """Estimate streaming performance for dataset."""
        # TODO: Get dataset metadata
        # TODO: Estimate streaming rate based on size and compression
        # TODO: Factor in S3 vs local performance differences
        # TODO: Return performance estimates (vectors/sec, MB/sec, etc.)
        pass


class BatchProcessor:
    """Utility for processing vector batches efficiently."""
    
    @staticmethod
    def batch_vectors(vector_iterator: Iterator[Tuple[str, np.ndarray]], 
                     batch_size: int) -> Iterator[List[Tuple[str, np.ndarray]]]:
        """Group vectors into batches."""
        batch = []
        for key, vector in vector_iterator:
            batch.append((key, vector))
            if len(batch) >= batch_size:
                yield batch
                batch = []
        
        # Yield final partial batch if needed
        if batch:
            yield batch
    
    @staticmethod
    async def batch_vectors_async(vector_iterator: AsyncIterator[Tuple[str, np.ndarray]], 
                                 batch_size: int) -> AsyncIterator[List[Tuple[str, np.ndarray]]]:
        """Group async vectors into batches."""
        batch = []
        async for key, vector in vector_iterator:
            batch.append((key, vector))
            if len(batch) >= batch_size:
                yield batch
                batch = []
        
        # Yield final partial batch if needed
        if batch:
            yield batch
    
    @staticmethod
    def vectors_to_valkey_format(vectors: List[Tuple[str, np.ndarray]]) -> Dict[str, bytes]:
        """Convert vector batch to Valkey command format."""
        valkey_batch = {}
        for key, vector in vectors:
            # Convert numpy array to bytes for Valkey storage
            if vector.dtype != np.float32:
                vector = vector.astype(np.float32)
            valkey_batch[key] = vector.tobytes()
        return valkey_batch
    
    @staticmethod
    def estimate_batch_memory(vectors: List[Tuple[str, np.ndarray]]) -> int:
        """Estimate memory usage of vector batch."""
        total_bytes = 0
        for key, vector in vectors:
            # Vector data size
            total_bytes += vector.nbytes
            # Key string size (rough estimate including Python overhead)
            total_bytes += len(key.encode('utf-8')) + 50  # 50 bytes Python string overhead
            # Tuple overhead
            total_bytes += 28  # Approximate tuple overhead
        
        return total_bytes


class StreamingCache:
    """Local cache for frequently accessed S3 datasets."""
    
    def __init__(self, cache_dir: Path, max_cache_size_gb: float = 10.0):
        # TODO: Initialize cache directory
        # TODO: Set up cache size management
        self.cache_dir = cache_dir
        self.max_cache_size = max_cache_size_gb * 1024**3
        self.cache_index = {}
    
    async def get_cached_stream(self, dataset_name: str, 
                              component: str) -> Optional[VKVStreamReader]:
        """Get cached stream reader if available."""
        # TODO: Check if dataset component is cached
        # TODO: Validate cache freshness
        # TODO: Return stream reader for cached file
        pass
    
    async def cache_dataset_component(self, dataset_name: str,
                                    component: str,
                                    s3_source: str) -> Path:
        """Download and cache dataset component."""
        # TODO: Download component from S3 to cache
        # TODO: Update cache index
        # TODO: Manage cache size limits
        # TODO: Return path to cached file
        pass
    
    def cleanup_cache(self, keep_recent_days: int = 7):
        """Clean up old cache entries."""
        # TODO: Identify old cache entries
        # TODO: Remove files older than keep_recent_days
        # TODO: Update cache index
        pass
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache usage statistics."""
        # TODO: Calculate cache directory size
        # TODO: Count cached datasets
        # TODO: Return cache statistics
        pass
