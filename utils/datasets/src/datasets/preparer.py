"""
Dataset Preparer - Main Orchestrator

Coordinates the complete dataset preparation pipeline from source files
to S3-ready datasets with RDBs, metadata, and subsets.

Now includes integrated Dataset class support for full lifecycle tracking.
"""

import asyncio
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple, Union
import logging
import tempfile
import shutil
from datetime import datetime, timezone

from .converter import DatasetConverter, SourceFormat
from .rdb_generator import RDBGenerator, RDBGenerationConfig, IndexSpec
from .s3_manager import S3DatasetManager, S3Config
from .metadata import DatasetMetadata, DatasetInfo, VectorStats, MetadataBuilder
from .streaming import VKVStreamReader, S3VectorStreamer
from .formats import CompressionType
from .dataset import Dataset, DatasetState, DatasetSource, DatasetRegistry


class DatasetPreparer:
    """Main orchestrator for dataset preparation pipeline."""
    
    def __init__(self, s3_config: S3Config, rdb_config: RDBGenerationConfig, 
                 dataset_registry: Optional[DatasetRegistry] = None,
                 use_dataset_tracking: bool = True):
        self.s3_config = s3_config
        self.rdb_config = rdb_config
        self.s3_manager = S3DatasetManager(s3_config)
        self.rdb_generator = RDBGenerator(rdb_config)
        self.converter = DatasetConverter()
        self.logger = logging.getLogger(__name__)
        self.temp_dir = None
        
        # Dataset tracking support
        self.use_dataset_tracking = use_dataset_tracking
        self.dataset_registry = dataset_registry
        self._current_dataset: Optional[Dataset] = None
        
        self.stats = {
            'start_time': None,
            'phases': {},
            'total_files_created': 0,
            'total_bytes_processed': 0,
            'errors': []
        }
    
    async def __aenter__(self):
        """Initialize temporary directory and services."""
        self.temp_dir = Path(tempfile.mkdtemp(prefix="vss_datasets_"))
        self.stats['start_time'] = asyncio.get_event_loop().time()
        
        await self.s3_manager.__aenter__()
        
        self.logger.info(f"Dataset preparation workspace: {self.temp_dir}")
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Clean up temporary files and services."""
        try:
            if self.temp_dir and self.temp_dir.exists():
                shutil.rmtree(self.temp_dir)
                self.logger.info(f"Cleaned up workspace: {self.temp_dir}")
        except Exception as e:
            self.logger.warning(f"Failed to clean up workspace: {e}")
        
        await self.s3_manager.__aexit__(exc_type, exc_val, exc_tb)
        
        # Log preparation summary
        if self.stats['start_time']:
            total_time = asyncio.get_event_loop().time() - self.stats['start_time']
            self.logger.info(f"Preparation completed in {total_time:.1f}s, "
                           f"{self.stats['total_files_created']} files created, "
                           f"{self.stats['total_bytes_processed'] / (1024**3):.2f} GB processed")
            shutil.rmtree(self.temp_dir)
        await self.s3_manager.__aexit__(exc_type, exc_val, exc_tb)
    
    async def prepare_dataset_with_tracking(self, dataset: Dataset,
                                          source_path: Path,
                                          source_format: Optional[SourceFormat] = None,
                                          index_spec: Optional[IndexSpec] = None,
                                          subset_sizes: Optional[List[int]] = None,
                                          description: Optional[str] = None,
                                          **kwargs) -> Dataset:
        """
        Complete dataset preparation pipeline with full Dataset tracking.
        
        Args:
            dataset: Dataset object to track (will be updated with progress)
            source_path: Path to source data
            source_format: Source format (auto-detected if None)
            index_spec: Index specification for RDB generation
            subset_sizes: List of subset sizes to create
            description: Dataset description
            **kwargs: Additional arguments
            
        Returns:
            Updated Dataset object with complete tracking information
        """
        self._current_dataset = dataset
        
        try:
            # Update dataset state
            dataset.transition_to(DatasetState.VKV_CONVERTED)
            
            # Set source information
            if not dataset.source:
                dataset.source = DatasetSource(
                    type="file",
                    location=str(source_path),
                    format=source_format.value if source_format else "auto"
                )
            
            # Prepare using existing pipeline but update dataset along the way
            metadata = await self.prepare_dataset(
                dataset_name=dataset.name,
                source_path=source_path,
                source_format=source_format,
                index_spec=index_spec,
                subset_sizes=subset_sizes,
                description=description,
                **kwargs
            )
            
            # Update dataset with final results
            dataset.update_from_metadata(metadata)
            dataset.transition_to(DatasetState.READY)
            
            # Register if registry is available
            if self.dataset_registry:
                self.dataset_registry.register(dataset)
                self.logger.info(f"Dataset {dataset.name} registered in registry")
            
            return dataset
            
        except Exception as e:
            dataset.mark_failed(str(e))
            if self.dataset_registry:
                self.dataset_registry.register(dataset)  # Save failed state
            raise
        finally:
            self._current_dataset = None
    
    async def prepare_dataset(self, dataset_name: str,
                            source_path: Path,
                            source_format: Optional[SourceFormat] = None,
                            index_spec: Optional[IndexSpec] = None,
                            subset_sizes: Optional[List[int]] = None,
                            description: Optional[str] = None,
                            **kwargs) -> DatasetMetadata:
        """Complete dataset preparation pipeline."""
        self.logger.info(f"Starting dataset preparation for {dataset_name}")
        
        # Create Dataset object for tracking if enabled
        if self.use_dataset_tracking and not self._current_dataset:
            dataset = Dataset(name=dataset_name)
            if self.dataset_registry:
                self.dataset_registry.register(dataset)
            self._current_dataset = dataset
        
        try:
            # Validate inputs
            validation_result = self.validate_source_dataset(source_path, source_format)
            if not validation_result.get('valid', False):
                raise ValueError(f"Source validation failed: {validation_result.get('error', 'Unknown error')}")
            
            # Phase 1: Convert source to VKV format
            if self._current_dataset:
                self._current_dataset.transition_to(DatasetState.VKV_CONVERTED)
            
            phase_start = asyncio.get_event_loop().time()
            vkv_files = await self._convert_source_to_vkv(
                source_path, dataset_name, source_format, **kwargs
            )
            self.stats['phases']['conversion'] = asyncio.get_event_loop().time() - phase_start
            
            # Phase 2: Generate RDB files
            if self._current_dataset:
                self._current_dataset.transition_to(DatasetState.RDB_GENERATED)
                
            phase_start = asyncio.get_event_loop().time()
            rdb_files = await self._generate_rdb_files(
                vkv_files, dataset_name, index_spec
            )
            self.stats['phases']['rdb_generation'] = asyncio.get_event_loop().time() - phase_start
            
            # Phase 3: Create subsets if requested
            phase_start = asyncio.get_event_loop().time()
            subset_info = await self._create_subsets(
                vkv_files, dataset_name, subset_sizes, index_spec
            )
            self.stats['phases']['subset_creation'] = asyncio.get_event_loop().time() - phase_start
            
            # Phase 4: Upload to S3
            if self._current_dataset:
                self._current_dataset.transition_to(DatasetState.S3_UPLOADED)
                
            phase_start = asyncio.get_event_loop().time()
            upload_results = await self._upload_to_s3(
                dataset_name, vkv_files, rdb_files, subset_info
            )
            self.stats['phases']['s3_upload'] = asyncio.get_event_loop().time() - phase_start
            
            # Phase 5: Create and upload metadata
            phase_start = asyncio.get_event_loop().time()
            metadata = await self._create_metadata(
                dataset_name, source_path, vkv_files, rdb_files, 
                subset_info, upload_results, description
            )
            self.stats['phases']['metadata_creation'] = asyncio.get_event_loop().time() - phase_start
            
            # Update dataset tracking with final state
            if self._current_dataset:
                self._current_dataset.update_from_metadata(metadata)
                self._current_dataset.transition_to(DatasetState.READY)
                if self.dataset_registry:
                    self.dataset_registry.register(self._current_dataset)
                    self.logger.info(f"Dataset {dataset_name} updated in registry")
            
            self.logger.info(f"Dataset preparation completed for {dataset_name}")
            return metadata
            
        except Exception as e:
            self.logger.error(f"Dataset preparation failed: {e}")
            self.stats['errors'].append(str(e))
            
            # Mark dataset as failed if tracking
            if self._current_dataset:
                self._current_dataset.mark_failed(str(e))
                if self.dataset_registry:
                    self.dataset_registry.register(self._current_dataset)
                    self.logger.info(f"Dataset {dataset_name} marked as failed in registry")
            
            await self._cleanup_failed_preparation(dataset_name)
            raise
    
    async def _convert_source_to_vkv(self, source_path: Path,
                                   dataset_name: str,
                                   source_format: Optional[SourceFormat],
                                   **kwargs) -> Dict[str, Path]:
        """Phase 1: Convert source files to VKV format."""
        self.logger.info("Phase 1: Converting source to VKV format")
        
        # Auto-detect format if not specified
        if source_format is None:
            source_format = self.converter.detect_format(source_path)
            self.logger.info(f"Auto-detected format: {source_format}")
        
        # Set up output paths in temp directory
        vkv_dir = self.temp_dir / "vkv"
        vkv_dir.mkdir(exist_ok=True)
        
        vkv_files = {}
        
        # Convert source to VKV format
        try:
            conversion_result = await self.converter.convert_dataset(
                source_path=source_path,
                output_dir=vkv_dir,
                source_format=source_format,
                compression=CompressionType.ZSTD,
                dataset_name=dataset_name,
                **kwargs
            )
            
            if not conversion_result.get('success', False):
                raise RuntimeError(f"Conversion failed: {conversion_result.get('error', 'Unknown error')}")
            
            # Collect generated VKV files
            output_files = conversion_result.get('output_files', {})
            for component, file_path in output_files.items():
                if file_path and Path(file_path).exists():
                    vkv_files[component] = Path(file_path)
                    self.stats['total_files_created'] += 1
                    self.stats['total_bytes_processed'] += Path(file_path).stat().st_size
            
            self.logger.info(f"Generated {len(vkv_files)} VKV files: {list(vkv_files.keys())}")
            
            return vkv_files
            
        except Exception as e:
            self.logger.error(f"VKV conversion failed: {e}")
            raise
    
    async def _generate_rdb_files(self, vkv_files: Dict[str, Path],
                                dataset_name: str,
                                index_spec: Optional[IndexSpec]) -> Dict[str, Path]:
        """Phase 2: Generate RDB files from VKV data."""
        self.logger.info("Phase 2: Generating RDB files")
        
        if 'train' not in vkv_files:
            self.logger.warning("No training vectors found, skipping RDB generation")
            return {}
        
        rdb_dir = self.temp_dir / "rdb"
        rdb_dir.mkdir(exist_ok=True)
        
        rdb_files = {}
        
        try:
            train_vkv = vkv_files['train']
            
            # Generate vectors-only RDB
            vectors_rdb = rdb_dir / f"{dataset_name}_vectors.rdb"
            
            vectors_result = await self.rdb_generator.generate_vectors_only_rdb(
                vkv_path=train_vkv,
                output_rdb=vectors_rdb,
                dataset_name=dataset_name
            )
            
            if not vectors_result.get('success', False):
                raise RuntimeError(f"Vectors RDB generation failed: {vectors_result.get('error', 'Unknown')}")
            
            rdb_files['vectors'] = vectors_rdb
            self.stats['total_files_created'] += 1
            self.stats['total_bytes_processed'] += vectors_rdb.stat().st_size
            
            # Generate indexed RDB if index specification provided
            if index_spec:
                indexed_rdb = rdb_dir / f"{dataset_name}_indexed.rdb"
                
                indexed_result = await self.rdb_generator.generate_indexed_rdb(
                    vectors_rdb=vectors_rdb,
                    output_path=indexed_rdb,
                    index_spec=index_spec
                )
                
                if indexed_result.get('success', False):
                    rdb_files['indexed'] = indexed_rdb
                    self.stats['total_files_created'] += 1
                    self.stats['total_bytes_processed'] += indexed_rdb.stat().st_size
                else:
                    self.logger.warning(f"Indexed RDB generation failed: {indexed_result.get('error', 'Unknown')}")
            
            self.logger.info(f"Generated {len(rdb_files)} RDB files")
            return rdb_files
            
        except Exception as e:
            self.logger.error(f"RDB generation failed: {e}")
            raise
    
    async def _create_subsets(self, vkv_files: Dict[str, Path],
                            dataset_name: str,
                            subset_sizes: Optional[List[int]],
                            index_spec: Optional[IndexSpec]) -> Dict[int, Dict[str, Path]]:
        """Phase 3: Create dataset subsets."""
        if not subset_sizes:
            return {}
        
        self.logger.info(f"Phase 3: Creating subsets for sizes: {subset_sizes}")
        
        if 'train' not in vkv_files:
            self.logger.warning("No training vectors found, skipping subset creation")
            return {}
        
        subset_dir = self.temp_dir / "subsets"
        subset_dir.mkdir(exist_ok=True)
        
        subset_info = {}
        
        try:
            # Create RDBs for each subset size
            subset_results = await self.rdb_generator.create_subset_rdbs(
                base_vkv=vkv_files['train'],
                output_dir=subset_dir,
                subset_sizes=subset_sizes,
                dataset_name=dataset_name,
                index_spec=index_spec
            )
            
            # Organize subset results
            for subset_name, result in subset_results.get('subsets', {}).items():
                if result.get('success', False):
                    subset_size = result['subset_size']
                    subset_files = {}
                    
                    if 'vectors_rdb' in result:
                        subset_files['vectors'] = Path(result['vectors_rdb'])
                        self.stats['total_files_created'] += 1
                        self.stats['total_bytes_processed'] += Path(result['vectors_rdb']).stat().st_size
                    
                    if 'indexed_rdb' in result:
                        subset_files['indexed'] = Path(result['indexed_rdb'])
                        self.stats['total_files_created'] += 1
                        self.stats['total_bytes_processed'] += Path(result['indexed_rdb']).stat().st_size
                    
                    subset_info[subset_size] = subset_files
            
            self.logger.info(f"Created subsets for {len(subset_info)} sizes")
            return subset_info
            
        except Exception as e:
            self.logger.error(f"Subset creation failed: {e}")
            raise
    
    async def _upload_to_s3(self, dataset_name: str,
                          vkv_files: Dict[str, Path],
                          rdb_files: Dict[str, Path],
                          subset_info: Dict[int, Dict[str, Path]]) -> Dict[str, Any]:
        """Phase 4: Upload all files to S3."""
        self.logger.info("Phase 4: Uploading to S3")
        
        upload_results = {
            'checksums': {},
            'upload_stats': {},
            'total_size_bytes': 0,
            'uploaded_files': []
        }
        
        try:
            from .s3_manager import DatasetUploader
            uploader = DatasetUploader(self.s3_manager)
            
            # Upload VKV files
            for component, file_path in vkv_files.items():
                s3_key = f"{dataset_name}/{component}.vkv"
                
                upload_result = await uploader.upload_file(
                    file_path,
                    self.s3_config.bucket,
                    s3_key
                )
                
                if upload_result.get('success', False):
                    upload_results['checksums'][s3_key] = upload_result.get('etag', '')
                    upload_results['total_size_bytes'] += file_path.stat().st_size
                    upload_results['uploaded_files'].append(s3_key)
                    self.logger.info(f"Uploaded VKV: {s3_key}")
                else:
                    raise RuntimeError(f"Failed to upload {s3_key}: {upload_result.get('error', 'Unknown')}")
            
            # Upload RDB files
            for rdb_type, file_path in rdb_files.items():
                s3_key = f"{dataset_name}/rdb/{dataset_name}_{rdb_type}.rdb"
                
                upload_result = await uploader.upload_file(
                    file_path,
                    self.s3_config.bucket,
                    s3_key
                )
                
                if upload_result.get('success', False):
                    upload_results['checksums'][s3_key] = upload_result.get('etag', '')
                    upload_results['total_size_bytes'] += file_path.stat().st_size
                    upload_results['uploaded_files'].append(s3_key)
                    self.logger.info(f"Uploaded RDB: {s3_key}")
                else:
                    self.logger.warning(f"Failed to upload {s3_key}: {upload_result.get('error', 'Unknown')}")
            
            # Upload subset files
            for subset_size, subset_files in subset_info.items():
                for rdb_type, file_path in subset_files.items():
                    s3_key = f"{dataset_name}/subsets/subset_{subset_size}/{dataset_name}_subset_{subset_size}_{rdb_type}.rdb"
                    
                    upload_result = await uploader.upload_file(
                        file_path,
                        self.s3_config.bucket,
                        s3_key
                    )
                    
                    if upload_result.get('success', False):
                        upload_results['checksums'][s3_key] = upload_result.get('etag', '')
                        upload_results['total_size_bytes'] += file_path.stat().st_size
                        upload_results['uploaded_files'].append(s3_key)
                        self.logger.info(f"Uploaded subset RDB: {s3_key}")
                    else:
                        self.logger.warning(f"Failed to upload {s3_key}: {upload_result.get('error', 'Unknown')}")
            
            self.logger.info(f"Uploaded {len(upload_results['uploaded_files'])} files, "
                           f"{upload_results['total_size_bytes'] / (1024**3):.2f} GB total")
            
            return upload_results
            
        except Exception as e:
            self.logger.error(f"S3 upload failed: {e}")
            raise
    
    async def _create_metadata(self, dataset_name: str,
                             source_path: Path,
                             vkv_files: Dict[str, Path],
                             rdb_files: Dict[str, Path],
                             subset_info: Dict[int, Dict[str, Path]],
                             upload_results: Dict[str, Any],
                             description: Optional[str]) -> DatasetMetadata:
        """Phase 5: Create comprehensive metadata."""
        self.logger.info("Phase 5: Creating and uploading metadata")
        
        try:
            # Build metadata using MetadataBuilder
            builder = MetadataBuilder()
            
            # Extract dataset info from VKV files
            if 'train' in vkv_files:
                async with VKVStreamReader(str(vkv_files['train'])) as reader:
                    vkv_metadata = await reader.get_metadata()
                    
                    dataset_info = DatasetInfo(
                        name=dataset_name,
                        source=str(source_path),
                        version="1.0",
                        created_at=datetime.now(timezone.utc).isoformat(),
                        processing_version="1.0",
                        description=description or f"Prepared dataset: {dataset_name}"
                    )
                    
                    # Build vector stats - sample vectors to calculate norms
                    sample_vectors = await reader.read_vectors(limit=1000)  # Sample for statistics
                    if len(sample_vectors) > 0:
                        import numpy as np
                        vectors_array = np.array(sample_vectors)
                        norms = np.linalg.norm(vectors_array, axis=1)
                        
                        vector_stats = VectorStats(
                            count=vkv_metadata.get('vector_count', 0),
                            dimension=vkv_metadata.get('dimension', 0),
                            data_type=str(vkv_metadata.get('data_type', 'float32')),
                            norm_min=float(norms.min()),
                            norm_max=float(norms.max()),
                            norm_mean=float(norms.mean()),
                            norm_std=float(norms.std())
                        )
                    else:
                        # Fallback with default values
                        vector_stats = VectorStats(
                            count=vkv_metadata.get('vector_count', 0),
                            dimension=vkv_metadata.get('dimension', 0),
                            data_type=str(vkv_metadata.get('data_type', 'float32')),
                            norm_min=0.0,
                            norm_max=1.0,
                            norm_mean=0.5,
                            norm_std=0.1
                        )
            else:
                raise ValueError("No training data found for metadata creation")
            
            # Create storage info
            from .metadata import StorageInfo, RDBFileInfo
            storage_info = StorageInfo(
                key_pattern="vector:{id}",
                key_encoding="utf-8", 
                data_format="vkv",
                compression="zstd",
                total_size_bytes=upload_results.get('total_size_bytes', 0),
                block_size=1000
            )
            
            # Create RDB file info
            rdb_info = {}
            for rdb_type, rdb_path in rdb_files.items():
                rdb_info[rdb_type] = RDBFileInfo(
                    filename=f"{dataset_name}_{rdb_type}.rdb",
                    size_bytes=rdb_path.stat().st_size,
                    memory_usage_bytes=rdb_path.stat().st_size,  # Estimate
                    key_count=vector_stats.count,
                    creation_time=datetime.now(timezone.utc).isoformat(),
                    valkey_version="7.2",
                    loading_time_seconds=0.0  # Unknown at creation time
                )
            
            # Create comprehensive metadata
            metadata = DatasetMetadata(
                dataset=dataset_info,
                vectors=vector_stats,
                storage=storage_info,
                rdb_files=rdb_info,
                checksums=upload_results.get('checksums', {})
            )
            
            # Upload metadata to S3
            metadata_json = metadata.to_json()
            metadata_key = f"{dataset_name}/metadata.json"
            
            from .s3_manager import DatasetUploader
            uploader = DatasetUploader(self.s3_manager)
            
            # Create temporary metadata file
            metadata_file = self.temp_dir / "metadata.json"
            with open(metadata_file, 'w') as f:
                f.write(metadata_json)
            
            upload_result = await uploader.upload_file(
                metadata_file,
                self.s3_config.bucket,
                metadata_key
            )
            
            if not upload_result.get('success', False):
                raise RuntimeError(f"Failed to upload metadata: {upload_result.get('error', 'Unknown')}")
            
            self.logger.info(f"Uploaded metadata: {metadata_key}")
            
            return metadata
            
        except Exception as e:
            self.logger.error(f"Metadata creation failed: {e}")
            raise
    
    async def _cleanup_failed_preparation(self, dataset_name: str):
        """Clean up artifacts from failed preparation."""
        try:
            self.logger.info(f"Cleaning up failed preparation for {dataset_name}")
            
            # Try to remove any partial uploads from S3
            try:
                # List and remove any files under the dataset path
                dataset_objects = await self.s3_manager.list_dataset_files(dataset_name)
                if dataset_objects:
                    self.logger.info(f"Removing {len(dataset_objects)} partial uploads from S3")
                    for obj in dataset_objects:
                        try:
                            await self.s3_manager.delete_file(obj['Key'])
                        except Exception as e:
                            self.logger.warning(f"Failed to delete {obj['Key']}: {e}")
            except Exception as e:
                self.logger.warning(f"Failed to clean up S3 objects: {e}")
            
            self.logger.info("Cleanup completed")
            
        except Exception as e:
            self.logger.error(f"Cleanup failed: {e}")
    
    def validate_source_dataset(self, source_path: Path,
                              source_format: Optional[SourceFormat] = None) -> Dict[str, Any]:
        """Validate source dataset before preparation."""
        try:
            # Check file existence and readability
            if not source_path.exists():
                return {'valid': False, 'error': f"Source path does not exist: {source_path}"}
            
            if source_path.is_file() and not source_path.stat().st_size > 0:
                return {'valid': False, 'error': "Source file is empty"}
            
            # Validate format compatibility
            if source_format is None:
                try:
                    detected_format = self.converter.detect_format(source_path)
                    source_format = detected_format
                except Exception as e:
                    return {'valid': False, 'error': f"Could not detect format: {e}"}
            
            # Estimate requirements
            file_size = 0
            if source_path.is_file():
                file_size = source_path.stat().st_size
            elif source_path.is_dir():
                file_size = sum(f.stat().st_size for f in source_path.rglob('*') if f.is_file())
            
            # Rough estimates (will vary based on format and compression)
            estimated_vkv_size = file_size * 1.2  # VKV might be slightly larger due to headers
            estimated_rdb_size = file_size * 2.5  # RDB typically larger due to Valkey overhead
            estimated_temp_space = file_size * 4   # Need space for original + VKV + RDBs
            
            return {
                'valid': True,
                'source_format': source_format,
                'source_size_bytes': file_size,
                'estimated_vkv_size_bytes': int(estimated_vkv_size),
                'estimated_rdb_size_bytes': int(estimated_rdb_size),
                'estimated_temp_space_bytes': int(estimated_temp_space),
                'estimated_s3_upload_size_bytes': int(estimated_vkv_size + estimated_rdb_size)
            }
            
        except Exception as e:
            return {'valid': False, 'error': str(e)}
    
    async def estimate_preparation_time(self, source_path: Path,
                                      subset_sizes: Optional[List[int]] = None,
                                      with_index: bool = True) -> Dict[str, float]:
        """Estimate time required for dataset preparation."""
        # TODO: Analyze source dataset size and complexity
        # TODO: Estimate conversion time based on format
        # TODO: Estimate RDB generation time
        # TODO: Estimate S3 upload time
        # TODO: Return time estimates for each phase
        pass
    
    async def prepare_from_existing_vkv(self, dataset_name: str,
                                      vkv_files: Dict[str, Path],
                                      index_spec: Optional[IndexSpec] = None,
                                      subset_sizes: Optional[List[int]] = None) -> DatasetMetadata:
        """Prepare dataset from existing VKV files."""
        # TODO: Skip conversion phase
        # TODO: Start from RDB generation phase
        # TODO: Continue with normal pipeline
        pass
    
    def get_preparation_stats(self) -> Dict[str, Any]:
        """Get comprehensive preparation statistics."""
        # TODO: Return statistics from all phases
        # TODO: Include timing, file sizes, error counts
        # TODO: Calculate overall efficiency metrics
        pass


class DatasetValidator:
    """Validates prepared datasets in S3."""
    
    def __init__(self, s3_manager: S3DatasetManager):
        self.s3_manager = s3_manager
        self.logger = logging.getLogger(__name__)
    
    async def validate_complete_dataset(self, dataset_name: str) -> Dict[str, Any]:
        """Comprehensive validation of prepared dataset."""
        # TODO: Download and validate metadata
        # TODO: Check file integrity using checksums
        # TODO: Validate RDB files can be loaded
        # TODO: Test vector streaming functionality
        # TODO: Validate subset consistency
        # TODO: Return detailed validation results
        pass
    
    async def validate_rdb_functionality(self, dataset_name: str,
                                       rdb_type: str = "vectors_indexed") -> bool:
        """Test RDB loading and basic functionality."""
        # TODO: Download RDB file
        # TODO: Load in temporary Valkey instance
        # TODO: Test basic operations (GET, FT.SEARCH if indexed)
        # TODO: Validate vector count and index status
        # TODO: Return success status
        pass
    
    async def validate_streaming_performance(self, dataset_name: str,
                                           batch_size: int = 1000) -> Dict[str, float]:
        """Test streaming performance of dataset."""
        # TODO: Stream sample batches from dataset
        # TODO: Measure streaming rate and latency
        # TODO: Test different batch sizes
        # TODO: Return performance metrics
        pass


class DatasetMigrator:
    """Handles dataset migration and updates."""
    
    def __init__(self, s3_manager: S3DatasetManager):
        self.s3_manager = s3_manager
        self.logger = logging.getLogger(__name__)
    
    async def migrate_dataset_format(self, dataset_name: str,
                                   new_compression: CompressionType,
                                   new_block_size: int) -> DatasetMetadata:
        """Migrate dataset to new format parameters."""
        # TODO: Download existing VKV files
        # TODO: Convert to new format parameters
        # TODO: Regenerate RDBs if needed
        # TODO: Upload migrated dataset
        # TODO: Update metadata
        pass
    
    async def add_subsets_to_existing(self, dataset_name: str,
                                    new_subset_sizes: List[int]) -> DatasetMetadata:
        """Add new subsets to existing dataset."""
        # TODO: Download main dataset files
        # TODO: Generate new subsets
        # TODO: Upload subset files
        # TODO: Update metadata with new subsets
        pass
    
    async def regenerate_rdb_with_new_index(self, dataset_name: str,
                                          new_index_spec: IndexSpec) -> DatasetMetadata:
        """Regenerate indexed RDB with new index specification."""
        # TODO: Download vectors-only RDB
        # TODO: Generate new indexed RDB
        # TODO: Upload new RDB file
        # TODO: Update metadata
        pass
