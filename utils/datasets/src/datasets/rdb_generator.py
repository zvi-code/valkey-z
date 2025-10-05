"""
RDB Generation Service

Manages Valkey instances for generating RDB files with vector data.
Supports both vectors-only and vectors+index RDB creation.
"""

import asyncio
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple
from dataclasses import dataclass
from enum import Enum
import logging
import time
import psutil
import valkey.asyncio as valkey

from .streaming import VKVStreamReader
from .metadata import VectorStats


class IndexAlgorithm(Enum):
    """Supported vector index algorithms."""
    FLAT = "FLAT"
    HNSW = "HNSW" 
    IVF = "IVF"


class MetricType(Enum):
    """Supported distance metrics."""
    COSINE = "COSINE"
    L2 = "L2"
    IP = "IP"  # Inner Product


@dataclass
class IndexSpec:
    """Specification for vector index creation."""
    algorithm: IndexAlgorithm
    metric: MetricType
    dimension: int
    
    # HNSW-specific parameters
    m: Optional[int] = 16
    ef_construction: Optional[int] = 200
    ef_runtime: Optional[int] = 10
    
    # IVF-specific parameters  
    nlist: Optional[int] = None
    nprobe: Optional[int] = None
    
    # FLAT-specific parameters (none currently)
    
    def to_ft_create_params(self) -> List[str]:
        """Convert to FT.CREATE parameter list."""
        params = []
        
        # Add vector field specification
        params.extend([
            "vector", "VECTOR", 
            self.algorithm.value,
            str(self.dimension * 2 + 4),  # Parameter count estimate
            "TYPE", "FLOAT32",
            "DIM", str(self.dimension),
            "DISTANCE_METRIC", self.metric.value
        ])
        
        # Add algorithm-specific parameters
        if self.algorithm == IndexAlgorithm.HNSW:
            if self.m is not None:
                params.extend(["M", str(self.m)])
            if self.ef_construction is not None:
                params.extend(["EF_CONSTRUCTION", str(self.ef_construction)])
            if self.ef_runtime is not None:
                params.extend(["EF_RUNTIME", str(self.ef_runtime)])
        
        elif self.algorithm == IndexAlgorithm.IVF:
            if self.nlist is not None:
                params.extend(["NLIST", str(self.nlist)])
            if self.nprobe is not None:
                params.extend(["NPROBE", str(self.nprobe)])
        
        # FLAT algorithm doesn't need additional parameters
        
        return params


@dataclass
class RDBGenerationConfig:
    """Configuration for RDB generation process."""
    valkey_host: str = "localhost"
    valkey_port: int = 6379
    valkey_password: Optional[str] = None
    valkey_db: int = 0
    
    memory_limit_gb: Optional[float] = None
    batch_size: int = 1000
    max_retries: int = 3
    retry_delay: float = 1.0
    
    # Monitoring settings
    memory_sample_interval: float = 10.0
    progress_report_interval: int = 10000
    
    # RDB settings
    rdb_compression: bool = True
    save_timeout: int = 3600  # 1 hour max for BGSAVE


class ValkeyInstance:
    """Manages a Valkey instance for RDB generation."""
    
    def __init__(self, config: RDBGenerationConfig):
        """Initialize Valkey instance manager."""
        self.config = config
        self.connection = None
        self.memory_samples = []
        self.logger = logging.getLogger(__name__)
        self._monitoring_task = None
    
    async def __aenter__(self):
        """Async context manager entry - establish Valkey connection."""
        try:
            # Establish Valkey connection
            self.connection = valkey.Valkey(
                host=self.config.valkey_host,
                port=self.config.valkey_port,
                password=self.config.valkey_password,
                db=self.config.valkey_db,
                decode_responses=False,  # Keep binary for vector data
                socket_connect_timeout=30,
                socket_timeout=60,
                retry_on_timeout=True
            )
            
            # Validate Valkey server version and modules
            server_info = await self.connection.info()
            valkey_version = server_info.get('valkey_version', '0.0.0')
            self.logger.info(f"Connected to Valkey {valkey_version}")
            
            # Check for valkey-search availability
            try:
                modules = await self.connection.execute_command("MODULE", "LIST")
                has_search = any(b'search' in str(module).lower() for module in modules)
                if not has_search:
                    self.logger.warning("Search module not found - index creation will fail")
            except Exception as e:
                self.logger.warning(f"Could not check modules: {e}")
            
            return self
            
        except Exception as e:
            self.logger.error(f"Failed to connect to Valkey: {e}")
            raise
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit - cleanup connections."""
        if self._monitoring_task:
            self._monitoring_task.cancel()
            try:
                await self._monitoring_task
            except asyncio.CancelledError:
                pass
        
        if self.connection:
            await self.connection.close()
    
    async def ping(self) -> bool:
        """Test connection to Valkey instance."""
        try:
            response = await self.connection.ping()
            return response is True
        except Exception as e:
            self.logger.error(f"Ping failed: {e}")
            return False
    
    async def get_info(self) -> Dict[str, Any]:
        """Get Valkey INFO command output."""
        try:
            info = await self.connection.info()
            return dict(info)
        except Exception as e:
            self.logger.error(f"Failed to get info: {e}")
            return {}
    
    async def flush_all(self):
        """Clear all data from Valkey instance."""
        try:
            await self.connection.flushall()
            self.logger.info("Flushed all data from Valkey instance")
        except Exception as e:
            self.logger.error(f"Failed to flush data: {e}")
            raise
    
    async def get_memory_usage(self) -> Dict[str, int]:
        """Get current memory usage statistics."""
        try:
            info = await self.connection.info('memory')
            
            memory_stats = {
                'used_memory': info.get('used_memory', 0),
                'used_memory_rss': info.get('used_memory_rss', 0),
                'used_memory_peak': info.get('used_memory_peak', 0),
                'used_memory_dataset': info.get('used_memory_dataset', 0),
                'used_memory_overhead': info.get('used_memory_overhead', 0),
                'mem_fragmentation_ratio': info.get('mem_fragmentation_ratio', 1.0)
            }
            
            # Add jemalloc stats if available
            if 'mem_allocator' in info and info['mem_allocator'] == 'jemalloc':
                memory_stats.update({
                    'active_memory': info.get('active_memory', 0),
                    'resident_memory': info.get('resident_memory', 0)
                })
            
            return memory_stats
            
        except Exception as e:
            self.logger.error(f"Failed to get memory usage: {e}")
            return {}
    
    async def monitor_memory(self, interval: float = 10.0) -> None:
        """Background task to monitor memory usage."""
        self.logger.info(f"Starting memory monitoring (interval: {interval}s)")
        
        try:
            while True:
                memory_stats = await self.get_memory_usage()
                if memory_stats:
                    sample = {
                        'timestamp': time.time(),
                        'memory_stats': memory_stats
                    }
                    self.memory_samples.append(sample)
                    
                    # Check memory limits
                    if self.config.memory_limit_gb:
                        used_memory_gb = memory_stats.get('used_memory', 0) / (1024**3)
                        if used_memory_gb > self.config.memory_limit_gb:
                            self.logger.warning(
                                f"Memory usage ({used_memory_gb:.2f}GB) exceeds limit "
                                f"({self.config.memory_limit_gb}GB)"
                            )
                
                await asyncio.sleep(interval)
                
        except asyncio.CancelledError:
            self.logger.info("Memory monitoring stopped")
            raise
        except Exception as e:
            self.logger.error(f"Memory monitoring error: {e}")
    
    def start_memory_monitoring(self, interval: float = None):
        """Start background memory monitoring task."""
        if interval is None:
            interval = self.config.memory_sample_interval
        
        if self._monitoring_task is None or self._monitoring_task.done():
            self._monitoring_task = asyncio.create_task(self.monitor_memory(interval))
    
    def stop_memory_monitoring(self):
        """Stop background memory monitoring task."""
        if self._monitoring_task and not self._monitoring_task.done():
            self._monitoring_task.cancel()


class VectorLoader:
    """Loads vectors from VKV files into Valkey."""
    
    def __init__(self, valkey_instance: ValkeyInstance):
        self.valkey = valkey_instance
        self.stats = {
            'vectors_loaded': 0,
            'bytes_loaded': 0,
            'load_start_time': None,
            'load_end_time': None,
            'errors': 0
        }
    
    async def load_from_vkv(self, vkv_path: Path, 
                           key_pattern: str = "vec:{dataset}:{index}",
                           dataset_name: str = "dataset",
                           start_offset: int = 0,
                           max_vectors: Optional[int] = None) -> Dict[str, Any]:
        """Load vectors from VKV file into Valkey."""
        from .formats import VKVReader
        
        self.stats['load_start_time'] = time.time()
        self.stats['vectors_loaded'] = 0
        self.stats['bytes_loaded'] = 0
        self.stats['errors'] = 0
        
        try:
            # Start memory monitoring task
            self.valkey.start_memory_monitoring()
            
            # Open VKV file with VKVStreamReader
            async with VKVReader(vkv_path) as reader:
                metadata = reader.get_metadata()
                total_vectors = metadata.get('vector_count', 0)
                
                if max_vectors:
                    total_vectors = min(total_vectors, max_vectors)
                
                self.logger.info(f"Loading {total_vectors} vectors from {vkv_path}")
                
                batch_vectors = []
                vector_index = start_offset
                
                # Stream vectors in batches
                async for vector_data in reader.read_vectors():
                    if vector_index < start_offset:
                        vector_index += 1
                        continue
                    
                    if max_vectors and (vector_index - start_offset) >= max_vectors:
                        break
                    
                    # Format key according to pattern
                    key = self.format_key(key_pattern, dataset_name, vector_index)
                    batch_vectors.append((key, vector_data))
                    
                    # Process batch when full
                    if len(batch_vectors) >= self.valkey.config.batch_size:
                        loaded = await self.load_batch(batch_vectors)
                        self.stats['vectors_loaded'] += loaded
                        self.stats['bytes_loaded'] += sum(len(v[1]) for v in batch_vectors)
                        
                        # Progress reporting
                        if self.stats['vectors_loaded'] % self.valkey.config.progress_report_interval == 0:
                            elapsed = time.time() - self.stats['load_start_time']
                            rate = self.stats['vectors_loaded'] / elapsed
                            self.logger.info(
                                f"Loaded {self.stats['vectors_loaded']} vectors "
                                f"({rate:.1f} vectors/sec)"
                            )
                        
                        batch_vectors = []
                    
                    vector_index += 1
                
                # Process remaining vectors
                if batch_vectors:
                    loaded = await self.load_batch(batch_vectors)
                    self.stats['vectors_loaded'] += loaded
                    self.stats['bytes_loaded'] += sum(len(v[1]) for v in batch_vectors)
            
            self.stats['load_end_time'] = time.time()
            
            # Stop memory monitoring
            self.valkey.stop_memory_monitoring()
            
            return self.get_loading_stats()
            
        except Exception as e:
            self.logger.error(f"Failed to load vectors from VKV: {e}")
            self.stats['errors'] += 1
            self.stats['load_end_time'] = time.time()
            raise
    
    async def load_batch(self, vectors: List[Tuple[str, bytes]], 
                        use_pipeline: bool = True) -> int:
        """Load a batch of vectors using Valkey pipeline."""
        if not vectors:
            return 0
        
        try:
            if use_pipeline:
                # Create Valkey pipeline for batch operations
                pipe = self.valkey.connection.pipeline()
                
                # Add HSET commands for each vector
                for key, vector_data in vectors:
                    pipe.hset(key, mapping={'vector': vector_data})
                
                # Execute pipeline
                results = await pipe.execute()
                
                # Count successful operations
                successful = sum(1 for result in results if result)
                return successful
            else:
                # Load vectors individually (fallback)
                successful = 0
                for key, vector_data in vectors:
                    try:
                        await self.valkey.connection.hset(key, mapping={'vector': vector_data})
                        successful += 1
                    except Exception as e:
                        self.logger.warning(f"Failed to load vector {key}: {e}")
                        self.stats['errors'] += 1
                
                return successful
                
        except Exception as e:
            self.logger.error(f"Batch loading failed: {e}")
            self.stats['errors'] += 1
            return 0
    
    def format_key(self, pattern: str, dataset: str, index: int) -> str:
        """Format vector key according to pattern."""
        # Replace placeholders in pattern with actual values
        key = pattern.format(dataset=dataset, index=index)
        
        # Validate generated key length and characters
        if len(key) > 512:  # Valkey key length limit
            raise ValueError(f"Generated key too long: {key}")
        
        return key
    
    def get_loading_stats(self) -> Dict[str, Any]:
        """Get statistics from loading process."""
        load_time = (self.stats['load_end_time'] or time.time()) - (self.stats['load_start_time'] or time.time())
        
        stats = {
            'vectors_loaded': self.stats['vectors_loaded'],
            'bytes_loaded': self.stats['bytes_loaded'],
            'errors': self.stats['errors'],
            'load_time_seconds': load_time,
            'vectors_per_second': self.stats['vectors_loaded'] / max(load_time, 0.001),
            'bytes_per_second': self.stats['bytes_loaded'] / max(load_time, 0.001),
            'memory_samples': len(self.valkey.memory_samples)
        }
        
        # Add final memory usage
        if self.valkey.memory_samples:
            latest_memory = self.valkey.memory_samples[-1]['memory_stats']
            stats['final_memory_usage'] = latest_memory
        
        return stats


class IndexBuilder:
    """Builds vector indexes using FT.CREATE."""
    
    def __init__(self, valkey_instance: ValkeyInstance):
        self.valkey = valkey_instance
        self.build_stats = {}
    
    async def create_index(self, index_name: str, index_spec: IndexSpec,
                          key_pattern: str = "vec:*",
                          vector_field: str = "vector") -> Dict[str, Any]:
        """Create vector index using FT.CREATE."""
        start_time = time.time()
        
        try:
            # Build FT.CREATE command with index specification
            create_cmd = ["FT.CREATE", index_name, "ON", "HASH", "PREFIX", "1", key_pattern, "SCHEMA"]
            
            # Add vector field and algorithm parameters
            vector_params = index_spec.to_ft_create_params()
            create_cmd.extend(vector_params)
            
            self.logger.info(f"Creating index {index_name} with command: {' '.join(create_cmd)}")
            
            # Execute FT.CREATE command
            result = await self.valkey.connection.execute_command(*create_cmd)
            
            if result != b'OK':
                raise RuntimeError(f"Index creation failed: {result}")
            
            # Monitor index build progress
            build_stats = await self.wait_for_index_build(index_name)
            
            creation_time = time.time() - start_time
            
            return {
                'index_name': index_name,
                'creation_time': creation_time,
                'build_stats': build_stats,
                'success': True
            }
            
        except Exception as e:
            self.logger.error(f"Failed to create index {index_name}: {e}")
            return {
                'index_name': index_name,
                'creation_time': time.time() - start_time,
                'success': False,
                'error': str(e)
            }
    
    async def wait_for_index_build(self, index_name: str, 
                                  timeout: int = 3600) -> Dict[str, Any]:
        """Wait for index build completion."""
        start_time = time.time()
        last_indexed = 0
        
        self.logger.info(f"Waiting for index {index_name} to build...")
        
        while time.time() - start_time < timeout:
            try:
                # Poll FT.INFO periodically to check build status
                info = await self.get_index_info(index_name)
                
                if not info:
                    await asyncio.sleep(5)
                    continue
                
                # Check if indexing is complete
                indexing = info.get('indexing', True)
                num_docs = info.get('num_docs', 0)
                
                if not indexing:
                    self.logger.info(f"Index {index_name} build completed with {num_docs} documents")
                    return {
                        'completed': True,
                        'num_docs': num_docs,
                        'build_time': time.time() - start_time,
                        'final_info': info
                    }
                
                # Report progress
                if num_docs != last_indexed:
                    elapsed = time.time() - start_time
                    rate = num_docs / max(elapsed, 0.001)
                    self.logger.info(f"Index build progress: {num_docs} docs ({rate:.1f} docs/sec)")
                    last_indexed = num_docs
                
                await asyncio.sleep(10)  # Check every 10 seconds
                
            except Exception as e:
                self.logger.warning(f"Error checking index status: {e}")
                await asyncio.sleep(5)
        
        # Timeout reached
        self.logger.error(f"Index build timeout after {timeout} seconds")
        return {
            'completed': False,
            'timeout': True,
            'build_time': time.time() - start_time
        }
    
    async def get_index_info(self, index_name: str) -> Dict[str, Any]:
        """Get index information using FT.INFO."""
        try:
            # Execute FT.INFO command
            info_result = await self.valkey.connection.execute_command("FT.INFO", index_name)
            
            # Parse index information (FT.INFO returns a list of key-value pairs)
            info = {}
            for i in range(0, len(info_result), 2):
                key = info_result[i].decode('utf-8') if isinstance(info_result[i], bytes) else str(info_result[i])
                value = info_result[i + 1]
                
                # Convert bytes to string if needed
                if isinstance(value, bytes):
                    try:
                        value = value.decode('utf-8')
                    except UnicodeDecodeError:
                        value = str(value)
                
                # Try to convert numeric values
                if isinstance(value, str) and value.isdigit():
                    value = int(value)
                elif isinstance(value, str) and value.replace('.', '').isdigit():
                    try:
                        value = float(value)
                    except ValueError:
                        pass
                
                info[key] = value
            
            return info
            
        except Exception as e:
            self.logger.error(f"Failed to get index info for {index_name}: {e}")
            return {}
    
    async def validate_index(self, index_name: str, 
                           sample_vectors: int = 100) -> bool:
        """Validate index by performing sample queries."""
        try:
            # Get index info first
            info = await self.get_index_info(index_name)
            if not info or info.get('num_docs', 0) == 0:
                self.logger.error(f"Index {index_name} is empty or not found")
                return False
            
            # Get random vectors from database for testing
            sample_keys = []
            cursor = 0
            
            while len(sample_keys) < sample_vectors:
                cursor, keys = await self.valkey.connection.scan(
                    cursor=cursor, 
                    match="vec:*", 
                    count=sample_vectors * 2
                )
                sample_keys.extend(keys[:sample_vectors - len(sample_keys)])
                
                if cursor == 0:  # Full scan completed
                    break
            
            if not sample_keys:
                self.logger.error("No vectors found for validation")
                return False
            
            # Perform sample queries
            successful_queries = 0
            for key in sample_keys[:min(10, len(sample_keys))]:  # Test with first 10 keys
                try:
                    # Get vector data
                    vector_data = await self.valkey.connection.hget(key, "vector")
                    if not vector_data:
                        continue
                    
                    # Perform FT.SEARCH query (simple existence check)
                    search_result = await self.valkey.connection.execute_command(
                        "FT.SEARCH", index_name, "*", "LIMIT", "0", "1"
                    )
                    
                    if search_result and len(search_result) > 0:
                        successful_queries += 1
                    
                except Exception as e:
                    self.logger.warning(f"Query validation failed for {key}: {e}")
            
            success_rate = successful_queries / min(10, len(sample_keys))
            is_valid = success_rate >= 0.8  # 80% success rate required
            
            self.logger.info(f"Index validation: {successful_queries}/{min(10, len(sample_keys))} queries successful")
            
            return is_valid
            
        except Exception as e:
            self.logger.error(f"Index validation failed: {e}")
            return False


class RDBGenerator:
    """Main service for generating RDB files."""
    
    def __init__(self, config: RDBGenerationConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)
    
    async def generate_vectors_only_rdb(self, vkv_path: Path, output_rdb: Path,
                                       dataset_name: str,
                                       key_pattern: str = "vec:{dataset}:{index}",
                                       subset_size: Optional[int] = None) -> Dict[str, Any]:
        """Generate RDB with vectors only (no index)."""
        start_time = time.time()
        
        try:
            # Create ValkeyInstance with configuration
            async with ValkeyInstance(self.config) as valkey:
                # Flush existing data to start clean
                await valkey.flush_all()
                
                # Load vectors from VKV file using VectorLoader
                loader = VectorLoader(valkey)
                
                max_vectors = subset_size if subset_size else None
                loading_stats = await loader.load_from_vkv(
                    vkv_path, key_pattern, dataset_name, 
                    max_vectors=max_vectors
                )
                
                # Monitor memory usage during loading
                memory_stats = await valkey.get_memory_usage()
                
                # Execute BGSAVE to create RDB file
                self.logger.info("Starting BGSAVE to create RDB file...")
                save_start = time.time()
                
                # Trigger background save
                result = await valkey.connection.bgsave()
                if result != b'OK':
                    raise RuntimeError(f"BGSAVE failed: {result}")
                
                # Wait for BGSAVE completion
                while True:
                    info = await valkey.get_info()
                    if info.get('rdb_bgsave_in_progress', 0) == 0:
                        break
                    
                    save_elapsed = time.time() - save_start
                    if save_elapsed > self.config.save_timeout:
                        raise TimeoutError(f"BGSAVE timeout after {save_elapsed:.1f} seconds")
                    
                    await asyncio.sleep(1)
                
                save_time = time.time() - save_start
                
                # Copy RDB file to output location
                # Valkey typically saves to dump.rdb in the data directory
                # We need to find and copy the actual RDB file
                rdb_info = await self._find_and_copy_rdb(valkey, output_rdb)
                
                total_time = time.time() - start_time
                
                return {
                    'success': True,
                    'dataset_name': dataset_name,
                    'output_path': str(output_rdb),
                    'subset_size': subset_size,
                    'loading_stats': loading_stats,
                    'memory_stats': memory_stats,
                    'save_time': save_time,
                    'total_time': total_time,
                    'rdb_info': rdb_info
                }
                
        except Exception as e:
            self.logger.error(f"Failed to generate vectors-only RDB: {e}")
            return {
                'success': False,
                'dataset_name': dataset_name,
                'error': str(e),
                'total_time': time.time() - start_time
            }
    
    async def _find_and_copy_rdb(self, valkey: ValkeyInstance, output_path: Path) -> Dict[str, Any]:
        """Find and copy the generated RDB file."""
        try:
            # Get Valkey configuration to find RDB file location
            config_info = await valkey.connection.config_get('dir')
            data_dir = Path(config_info.get('dir', '/tmp'))
            
            rdb_filename = await valkey.connection.config_get('dbfilename')
            rdb_file = data_dir / rdb_filename.get('dbfilename', 'dump.rdb')
            
            if not rdb_file.exists():
                # Fallback: look for any .rdb files in common locations
                possible_locations = [
                    Path('/tmp/dump.rdb'),
                    Path('/var/lib/valkey/dump.rdb'),
                    Path('./dump.rdb')
                ]
                
                for location in possible_locations:
                    if location.exists():
                        rdb_file = location
                        break
                else:
                    raise FileNotFoundError("Could not find generated RDB file")
            
            # Copy to output location
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Read and write in chunks for large files
            with open(rdb_file, 'rb') as src, open(output_path, 'wb') as dst:
                chunk = src.read(8192)
                while chunk:
                    dst.write(chunk)
                    chunk = src.read(8192)
            
            rdb_size = output_path.stat().st_size
            
            self.logger.info(f"RDB file copied to {output_path} ({rdb_size} bytes)")
            
            return {
                'source_path': str(rdb_file),
                'output_path': str(output_path),
                'file_size': rdb_size
            }
            
        except Exception as e:
            self.logger.error(f"Failed to copy RDB file: {e}")
            raise
    
    async def generate_indexed_rdb(self, vectors_rdb: Path, output_path: Path,
                                  index_spec: IndexSpec, 
                                  index_name: str = "idx:vectors",
                                  vector_field: str = "vector") -> Dict[str, Any]:
        """Generate RDB with vectors and index."""
        start_time = time.time()
        
        try:
            # Create ValkeyInstance with configuration
            async with ValkeyInstance(self.config) as valkey:
                # Flush existing data
                await valkey.flush_all()
                
                # Load vectors-only RDB
                self.logger.info(f"Loading vectors from RDB: {vectors_rdb}")
                
                # Use DEBUG RELOAD or direct file copy to data directory
                await self._load_rdb_file(valkey, vectors_rdb)
                
                # Verify vectors loaded
                info = await valkey.get_info()
                key_count = info.get('db0', {}).get('keys', 0) if 'db0' in info else 0
                
                if key_count == 0:
                    raise RuntimeError("No vectors found after loading RDB")
                
                self.logger.info(f"Loaded {key_count} vectors from RDB")
                
                # Create vector index using IndexBuilder
                builder = IndexBuilder(valkey)
                index_stats = await builder.create_index(
                    index_name, index_spec, vector_field=vector_field
                )
                
                if not index_stats['success']:
                    raise RuntimeError(f"Index creation failed: {index_stats.get('error', 'Unknown error')}")
                
                # Validate index
                is_valid = await builder.validate_index(index_name)
                if not is_valid:
                    self.logger.warning("Index validation failed, but continuing...")
                
                # Monitor memory usage during index build
                memory_stats = await valkey.get_memory_usage()
                
                # Execute BGSAVE to create indexed RDB
                self.logger.info("Creating indexed RDB file...")
                save_start = time.time()
                
                result = await valkey.connection.bgsave()
                if result != b'OK':
                    raise RuntimeError(f"BGSAVE failed: {result}")
                
                # Wait for BGSAVE completion
                while True:
                    info = await valkey.get_info()
                    if info.get('rdb_bgsave_in_progress', 0) == 0:
                        break
                    
                    save_elapsed = time.time() - save_start
                    if save_elapsed > self.config.save_timeout:
                        raise TimeoutError(f"BGSAVE timeout after {save_elapsed:.1f} seconds")
                    
                    await asyncio.sleep(1)
                
                save_time = time.time() - save_start
                
                # Copy RDB file to output location
                rdb_info = await self._find_and_copy_rdb(valkey, output_path)
                
                total_time = time.time() - start_time
                
                return {
                    'success': True,
                    'output_path': str(output_path),
                    'index_name': index_name,
                    'vector_count': key_count,
                    'index_stats': index_stats,
                    'index_valid': is_valid,
                    'memory_stats': memory_stats,
                    'save_time': save_time,
                    'total_time': total_time,
                    'rdb_info': rdb_info
                }
                
        except Exception as e:
            self.logger.error(f"Failed to generate indexed RDB: {e}")
            return {
                'success': False,
                'error': str(e),
                'total_time': time.time() - start_time
            }
    
    async def _load_rdb_file(self, valkey: ValkeyInstance, rdb_path: Path):
        """Load RDB file into Valkey instance."""
        try:
            # Get Valkey data directory
            config_info = await valkey.connection.config_get('dir')
            data_dir = Path(config_info.get('dir', '/tmp'))
            
            rdb_filename_config = await valkey.connection.config_get('dbfilename')
            target_rdb = data_dir / rdb_filename_config.get('dbfilename', 'dump.rdb')
            
            # Copy RDB file to Valkey data directory
            import shutil
            shutil.copy2(rdb_path, target_rdb)
            
            # Use DEBUG RELOAD to load the RDB file
            result = await valkey.connection.execute_command('DEBUG', 'RELOAD')
            
            if result != b'OK':
                raise RuntimeError(f"DEBUG RELOAD failed: {result}")
            
            self.logger.info(f"Successfully loaded RDB file: {rdb_path}")
            
        except Exception as e:
            self.logger.error(f"Failed to load RDB file: {e}")
            raise
    
    async def generate_both_rdbs(self, vkv_path: Path, 
                                vectors_rdb: Path, indexed_rdb: Path,
                                dataset_name: str, index_spec: IndexSpec,
                                **kwargs) -> Dict[str, Any]:
        """Generate both vectors-only and indexed RDBs."""
        start_time = time.time()
        
        try:
            self.logger.info(f"Generating both RDBs from {vkv_path}")
            
            # Generate vectors-only RDB first
            self.logger.info("Step 1: Generating vectors-only RDB...")
            vectors_result = await self.generate_vectors_only_rdb(
                vkv_path, vectors_rdb, dataset_name, **kwargs
            )
            
            if not vectors_result['success']:
                raise RuntimeError(f"Vectors-only RDB generation failed: {vectors_result.get('error', 'Unknown error')}")
            
            # Generate indexed RDB using the vectors-only RDB
            self.logger.info("Step 2: Generating indexed RDB...")
            indexed_result = await self.generate_indexed_rdb(
                vectors_rdb, indexed_rdb, index_spec
            )
            
            if not indexed_result['success']:
                raise RuntimeError(f"Indexed RDB generation failed: {indexed_result.get('error', 'Unknown error')}")
            
            # Validate both RDB files
            validation_results = {}
            
            expected_vectors = vectors_result['loading_stats']['vectors_loaded']
            
            self.logger.info("Step 3: Validating generated RDBs...")
            vectors_validation = await self.validate_rdb(vectors_rdb, expected_vectors)
            indexed_validation = await self.validate_rdb(indexed_rdb, expected_vectors)
            
            validation_results = {
                'vectors_rdb': vectors_validation,
                'indexed_rdb': indexed_validation
            }
            
            total_time = time.time() - start_time
            
            return {
                'success': True,
                'dataset_name': dataset_name,
                'vectors_rdb': str(vectors_rdb),
                'indexed_rdb': str(indexed_rdb),
                'vectors_result': vectors_result,
                'indexed_result': indexed_result,
                'validation_results': validation_results,
                'total_time': total_time,
                'vector_count': expected_vectors
            }
            
        except Exception as e:
            self.logger.error(f"Failed to generate both RDBs: {e}")
            return {
                'success': False,
                'dataset_name': dataset_name,
                'error': str(e),
                'total_time': time.time() - start_time
            }
    
    async def create_subset_rdbs(self, base_vkv: Path, output_dir: Path,
                               subset_sizes: List[int], dataset_name: str,
                               index_spec: Optional[IndexSpec] = None) -> Dict[str, Dict[str, Any]]:
        """Create RDBs for multiple subset sizes."""
        results = {}
        
        try:
            self.logger.info(f"Creating RDBs for {len(subset_sizes)} subset sizes")
            
            # Ensure output directory exists
            output_dir.mkdir(parents=True, exist_ok=True)
            
            for subset_size in subset_sizes:
                subset_start = time.time()
                subset_name = f"{dataset_name}_subset_{subset_size}"
                
                self.logger.info(f"Processing subset {subset_name} with {subset_size} vectors")
                
                # Create subset directory
                subset_dir = output_dir / f"subset_{subset_size}"
                subset_dir.mkdir(exist_ok=True)
                
                # Define output paths
                vectors_rdb = subset_dir / f"{subset_name}_vectors.rdb"
                
                try:
                    # Generate vectors-only RDB for this subset
                    vectors_result = await self.generate_vectors_only_rdb(
                        base_vkv, vectors_rdb, dataset_name,
                        subset_size=subset_size
                    )
                    
                    if not vectors_result['success']:
                        raise RuntimeError(f"Vectors RDB generation failed: {vectors_result.get('error', 'Unknown')}")
                    
                    subset_result = {
                        'subset_size': subset_size,
                        'vectors_rdb': str(vectors_rdb),
                        'vectors_result': vectors_result
                    }
                    
                    # Generate indexed RDB if index specification provided
                    if index_spec:
                        indexed_rdb = subset_dir / f"{subset_name}_indexed.rdb"
                        
                        indexed_result = await self.generate_indexed_rdb(
                            vectors_rdb, indexed_rdb, index_spec
                        )
                        
                        subset_result.update({
                            'indexed_rdb': str(indexed_rdb),
                            'indexed_result': indexed_result,
                            'has_index': True
                        })
                    else:
                        subset_result['has_index'] = False
                    
                    # Validate RDBs
                    expected_vectors = vectors_result['loading_stats']['vectors_loaded']
                    vectors_validation = await self.validate_rdb(vectors_rdb, expected_vectors)
                    
                    validation_results = {'vectors_rdb': vectors_validation}
                    
                    if index_spec and 'indexed_rdb' in subset_result:
                        indexed_validation = await self.validate_rdb(
                            Path(subset_result['indexed_rdb']), expected_vectors
                        )
                        validation_results['indexed_rdb'] = indexed_validation
                    
                    subset_result.update({
                        'validation_results': validation_results,
                        'subset_time': time.time() - subset_start,
                        'success': True
                    })
                    
                    results[subset_name] = subset_result
                    
                    self.logger.info(f"Completed subset {subset_name} in {subset_result['subset_time']:.1f}s")
                    
                except Exception as e:
                    self.logger.error(f"Failed to create subset {subset_name}: {e}")
                    results[subset_name] = {
                        'subset_size': subset_size,
                        'success': False,
                        'error': str(e),
                        'subset_time': time.time() - subset_start
                    }
            
            # Calculate summary statistics
            successful_subsets = sum(1 for r in results.values() if r.get('success', False))
            total_vectors = sum(
                r.get('vectors_result', {}).get('loading_stats', {}).get('vectors_loaded', 0)
                for r in results.values() if r.get('success', False)
            )
            
            self.logger.info(f"Completed {successful_subsets}/{len(subset_sizes)} subsets successfully")
            
            return {
                'summary': {
                    'total_subsets': len(subset_sizes),
                    'successful_subsets': successful_subsets,
                    'failed_subsets': len(subset_sizes) - successful_subsets,
                    'total_vectors_processed': total_vectors,
                    'output_directory': str(output_dir)
                },
                'subsets': results
            }
            
        except Exception as e:
            self.logger.error(f"Failed to create subset RDBs: {e}")
            return {
                'summary': {
                    'total_subsets': len(subset_sizes),
                    'successful_subsets': 0,
                    'failed_subsets': len(subset_sizes),
                    'error': str(e)
                },
                'subsets': results
            }
    
    def estimate_memory_requirements(self, vector_count: int, dimension: int,
                                   with_index: bool = False,
                                   index_spec: Optional[IndexSpec] = None) -> Dict[str, int]:
        """Estimate memory requirements for RDB generation."""
        # Base memory for vectors (each vector = dimension * 4 bytes for float32s)
        vector_memory = vector_count * dimension * 4
        
        # Valkey overhead per key (approximately 90-100 bytes per key)
        valkey_overhead = vector_count * 95
        
        # Hash field overhead (field name + value overhead)
        hash_overhead = vector_count * 50  # "vector" field + Valkey hash structure
        
        # Total base memory for vectors
        base_memory = vector_memory + valkey_overhead + hash_overhead
        
        # Index memory estimation
        index_memory = 0
        if with_index and index_spec:
            if index_spec.algorithm == "FLAT":
                # FLAT index stores all vectors in memory + metadata
                index_memory = vector_memory + (vector_count * 20)  # 20 bytes metadata per vector
                
            elif index_spec.algorithm == "HNSW":
                # HNSW requires additional memory for graph structure
                # Rough estimate: 2-3x vector data + connections
                m_connections = index_spec.parameters.get("M", 16)
                max_connections = m_connections * 2  # bidirectional connections
                connection_memory = vector_count * max_connections * 8  # 8 bytes per connection
                
                # Graph levels (logarithmic)
                import math
                levels = max(1, int(math.log2(vector_count)) - 2)
                level_memory = vector_count * levels * 4  # 4 bytes per level info
                
                index_memory = vector_memory + connection_memory + level_memory
                
            elif index_spec.algorithm == "IVF":
                # IVF requires centroids + inverted lists
                nlist = index_spec.parameters.get("nlist", min(vector_count // 100, 1000))
                centroid_memory = nlist * dimension * 4  # centroids
                
                # Inverted lists overhead
                avg_list_size = vector_count // nlist
                list_overhead = nlist * (avg_list_size * 8)  # 8 bytes per list entry
                
                index_memory = centroid_memory + list_overhead + (vector_memory * 0.1)  # 10% additional
        
        # Safety margins
        safety_margin = 0.25  # 25% safety margin
        base_with_margin = int(base_memory * (1 + safety_margin))
        index_with_margin = int(index_memory * (1 + safety_margin))
        
        # Total memory requirements
        total_memory = base_with_margin + index_with_margin
        
        # Additional Valkey working memory (for operations, pipelines, etc.)
        working_memory = min(total_memory * 0.2, 500 * 1024 * 1024)  # 20% or max 500MB
        
        estimated_total = total_memory + int(working_memory)
        
        return {
            'vector_memory_bytes': vector_memory,
            'valkey_overhead_bytes': valkey_overhead + hash_overhead,
            'base_memory_bytes': base_memory,
            'index_memory_bytes': index_memory,
            'safety_margin_bytes': int((total_memory - base_memory) if index_memory else (base_with_margin - base_memory)),
            'working_memory_bytes': int(working_memory),
            'total_estimated_bytes': estimated_total,
            'total_estimated_mb': estimated_total // (1024 * 1024),
            'total_estimated_gb': round(estimated_total / (1024 * 1024 * 1024), 2),
            'with_index': with_index,
            'algorithm': index_spec.algorithm if index_spec else None
        }
    
    async def validate_rdb(self, rdb_path: Path, 
                          expected_vectors: int) -> Dict[str, Any]:
        """Validate generated RDB file."""
        start_time = time.time()
        
        try:
            if not rdb_path.exists():
                return {
                    'valid': False,
                    'error': f"RDB file not found: {rdb_path}",
                    'validation_time': time.time() - start_time
                }
            
            file_size = rdb_path.stat().st_size
            if file_size == 0:
                return {
                    'valid': False,
                    'error': "RDB file is empty",
                    'file_size': file_size,
                    'validation_time': time.time() - start_time
                }
            
            # Create temporary Valkey instance for validation
            temp_config = RDBGenerationConfig(
                host=self.config.host,
                port=self.config.port + 1000,  # Use different port
                password=self.config.password,
                memory_limit=self.config.memory_limit,
                timeout=60  # Shorter timeout for validation
            )
            
            async with ValkeyInstance(temp_config) as temp_valkey:
                # Load RDB file
                await self._load_rdb_file(temp_valkey, rdb_path)
                
                # Check basic info
                info = await temp_valkey.get_info()
                db_info = info.get('db0', {})
                actual_keys = db_info.get('keys', 0)
                
                # Validate key count
                key_count_valid = actual_keys == expected_vectors
                if not key_count_valid:
                    self.logger.warning(f"Key count mismatch: expected {expected_vectors}, found {actual_keys}")
                
                # Sample vector validation
                sample_size = min(10, actual_keys)
                sample_results = []
                
                if actual_keys > 0:
                    # Get sample keys
                    cursor = 0
                    sample_keys = []
                    
                    while len(sample_keys) < sample_size:
                        cursor, keys = await temp_valkey.connection.scan(
                            cursor=cursor, match="vec:*", count=sample_size * 2
                        )
                        sample_keys.extend(keys[:sample_size - len(sample_keys)])
                        
                        if cursor == 0:
                            break
                    
                    # Validate sample vectors
                    for key in sample_keys:
                        try:
                            vector_data = await temp_valkey.connection.hget(key, "vector")
                            
                            if vector_data:
                                # Basic vector validation (check if it's proper binary data)
                                vector_valid = len(vector_data) > 0 and len(vector_data) % 4 == 0
                                sample_results.append({
                                    'key': key.decode() if isinstance(key, bytes) else str(key),
                                    'valid': vector_valid,
                                    'data_length': len(vector_data)
                                })
                            else:
                                sample_results.append({
                                    'key': key.decode() if isinstance(key, bytes) else str(key),
                                    'valid': False,
                                    'error': 'No vector data found'
                                })
                                
                        except Exception as e:
                            sample_results.append({
                                'key': key.decode() if isinstance(key, bytes) else str(key),
                                'valid': False,
                                'error': str(e)
                            })
                
                # Check for indexes
                has_index = False
                index_info = None
                
                try:
                    # Try to get index information
                    indexes = await temp_valkey.connection.execute_command("FT._LIST")
                    if indexes:
                        has_index = True
                        # Get info for first index
                        if len(indexes) > 0:
                            index_name = indexes[0].decode() if isinstance(indexes[0], bytes) else str(indexes[0])
                            index_info_result = await temp_valkey.connection.execute_command("FT.INFO", index_name)
                            
                            # Parse index info
                            index_info = {}
                            for i in range(0, len(index_info_result), 2):
                                key = index_info_result[i].decode('utf-8') if isinstance(index_info_result[i], bytes) else str(index_info_result[i])
                                value = index_info_result[i + 1]
                                if isinstance(value, bytes):
                                    try:
                                        value = value.decode('utf-8')
                                    except UnicodeDecodeError:
                                        value = str(value)
                                index_info[key] = value
                            
                except Exception as e:
                    self.logger.debug(f"Index check failed (may not have index): {e}")
                
                # Calculate validation results
                sample_valid_count = sum(1 for r in sample_results if r.get('valid', False))
                sample_success_rate = sample_valid_count / max(len(sample_results), 1)
                
                overall_valid = (
                    key_count_valid and 
                    sample_success_rate >= 0.8 and  # 80% of samples must be valid
                    file_size > 1000  # Minimum reasonable file size
                )
                
                validation_time = time.time() - start_time
                
                return {
                    'valid': overall_valid,
                    'file_path': str(rdb_path),
                    'file_size': file_size,
                    'expected_vectors': expected_vectors,
                    'actual_vectors': actual_keys,
                    'key_count_valid': key_count_valid,
                    'sample_validation': {
                        'sample_size': len(sample_results),
                        'valid_samples': sample_valid_count,
                        'success_rate': sample_success_rate,
                        'sample_results': sample_results
                    },
                    'has_index': has_index,
                    'index_info': index_info,
                    'validation_time': validation_time
                }
                
        except Exception as e:
            self.logger.error(f"RDB validation failed: {e}")
            return {
                'valid': False,
                'error': str(e),
                'file_path': str(rdb_path),
                'validation_time': time.time() - start_time
            }
    
    def cleanup_temp_files(self, temp_dir: Path):
        """Clean up temporary files from RDB generation."""
        try:
            import shutil
            
            if not temp_dir.exists():
                self.logger.debug(f"Temp directory does not exist: {temp_dir}")
                return
            
            files_removed = 0
            dirs_removed = 0
            
            # Remove temporary files
            for item in temp_dir.rglob("*"):
                try:
                    if item.is_file():
                        # Check for specific temporary file patterns
                        if (item.suffix in ['.rdb', '.tmp', '.temp'] or 
                            'temp' in item.name.lower() or
                            item.name.startswith('dump')):
                            
                            item.unlink()
                            files_removed += 1
                            self.logger.debug(f"Removed temp file: {item}")
                            
                except Exception as e:
                    self.logger.warning(f"Failed to remove temp file {item}: {e}")
            
            # Remove empty directories
            for item in sorted(temp_dir.rglob("*"), key=lambda p: str(p), reverse=True):
                try:
                    if item.is_dir() and not any(item.iterdir()):
                        item.rmdir()
                        dirs_removed += 1
                        self.logger.debug(f"Removed empty temp dir: {item}")
                        
                except Exception as e:
                    self.logger.warning(f"Failed to remove temp dir {item}: {e}")
            
            # Remove the main temp directory if empty
            try:
                if temp_dir.exists() and not any(temp_dir.iterdir()):
                    temp_dir.rmdir()
                    dirs_removed += 1
                    self.logger.debug(f"Removed main temp dir: {temp_dir}")
                    
            except Exception as e:
                self.logger.warning(f"Failed to remove main temp dir {temp_dir}: {e}")
            
            self.logger.info(f"Cleanup completed: {files_removed} files, {dirs_removed} directories removed")
            
        except Exception as e:
            self.logger.error(f"Cleanup failed: {e}")
            
    async def cleanup_valkey_files(self, data_dir: Path):
        """Clean up Valkey-specific temporary files."""
        try:
            valkey_temp_files = [
                "dump.rdb",
                "appendonly.aof", 
                "valkey.log",
                "temp*.rdb"
            ]
            
            files_removed = 0
            
            for pattern in valkey_temp_files:
                if '*' in pattern:
                    # Handle glob patterns
                    import glob
                    for file_path in glob.glob(str(data_dir / pattern)):
                        try:
                            Path(file_path).unlink()
                            files_removed += 1
                            self.logger.debug(f"Removed Valkey temp file: {file_path}")
                        except Exception as e:
                            self.logger.warning(f"Failed to remove Valkey temp file {file_path}: {e}")
                else:
                    # Handle exact file names
                    file_path = data_dir / pattern
                    if file_path.exists():
                        try:
                            file_path.unlink()
                            files_removed += 1
                            self.logger.debug(f"Removed Valkey temp file: {file_path}")
                        except Exception as e:
                            self.logger.warning(f"Failed to remove Valkey temp file {file_path}: {e}")
            
            self.logger.info(f"Valkey cleanup completed: {files_removed} files removed")
            
        except Exception as e:
            self.logger.error(f"Valkey cleanup failed: {e}")
