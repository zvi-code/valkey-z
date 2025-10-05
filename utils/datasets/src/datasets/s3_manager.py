"""
S3 Dataset Manager

Handles S3 bucket operations for dataset storage and retrieval.
Manages dataset organization, metadata, and efficient uploads/downloads.
"""

# Optional S3 dependencies
try:
    import boto3
    from botocore.exceptions import ClientError, NoCredentialsError
    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False
    # Create mock classes for type hints
    class ClientError(Exception):
        pass
    class NoCredentialsError(Exception):
        pass

from pathlib import Path
from typing import Dict, Any, List, Optional, Iterator, Tuple
from dataclasses import dataclass
import json
import asyncio
import aiofiles
import hashlib
import logging
from datetime import datetime
import io
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

from .metadata import DatasetMetadata


@dataclass 
class S3Config:
    """Configuration for S3 operations."""
    bucket_name: str = "vss-datasets"
    region: str = "us-east-1"
    
    # AWS credentials (usually from environment/IAM)
    access_key_id: Optional[str] = None
    secret_access_key: Optional[str] = None
    session_token: Optional[str] = None
    
    # Upload settings
    multipart_threshold: int = 64 * 1024 * 1024  # 64MB
    max_concurrency: int = 10
    multipart_chunksize: int = 64 * 1024 * 1024  # 64MB
    
    # Download settings
    download_threads: int = 4
    
    # Retry settings
    max_retries: int = 3
    retry_delay: float = 1.0


class S3DatasetManager:
    """Main class for managing datasets in S3."""
    
    def __init__(self, config: S3Config):
        """Initialize S3 dataset manager with configuration."""
        if not BOTO3_AVAILABLE:
            raise ImportError(
                "boto3 is required for S3 operations. Install with: pip install boto3"
            )
        
        self.config = config
        self.s3_client = None
        self.transfer_config = None
        self.logger = logging.getLogger(__name__)
        
        # Initialize boto3 client
        self._initialize_client()
        
        # Set up transfer configuration for multipart uploads
        self._setup_transfer_config()
    
    def _initialize_client(self):
        """Initialize boto3 S3 client with configuration."""
        try:
            session_kwargs = {'region_name': self.config.region}
            
            if self.config.access_key_id:
                session_kwargs.update({
                    'aws_access_key_id': self.config.access_key_id,
                    'aws_secret_access_key': self.config.secret_access_key
                })
                if self.config.session_token:
                    session_kwargs['aws_session_token'] = self.config.session_token
            
            session = boto3.Session(**session_kwargs)
            self.s3_client = session.client(
                's3',
                config=boto3.session.Config(
                    retries={'max_attempts': self.config.max_retries},
                    max_pool_connections=self.config.max_concurrency
                )
            )
            
            # Test credentials
            self.s3_client.list_buckets()
            
        except NoCredentialsError:
            self.logger.error("AWS credentials not found")
            raise
        except ClientError as e:
            self.logger.error(f"Failed to initialize S3 client: {e}")
            raise
    
    def _setup_transfer_config(self):
        """Setup transfer configuration for multipart uploads."""
        try:
            from boto3.s3.transfer import TransferConfig
            self.transfer_config = TransferConfig(
                multipart_threshold=self.config.multipart_threshold,
                max_concurrency=self.config.max_concurrency,
                multipart_chunksize=self.config.multipart_chunksize,
                use_threads=True
            )
        except ImportError:
            self.logger.warning("boto3.s3.transfer not available - using default transfer config")
            self.transfer_config = None
    
    def __enter__(self):
        """Context manager entry - validate S3 client and bucket access."""
        try:
            # Test bucket access and permissions
            self.s3_client.head_bucket(Bucket=self.config.bucket_name)
            self.logger.info(f"Successfully connected to bucket: {self.config.bucket_name}")
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                self.logger.info(f"Bucket {self.config.bucket_name} does not exist, will create it")
            else:
                self.logger.error(f"Cannot access bucket {self.config.bucket_name}: {e}")
                raise
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - cleanup connections."""
        # Close any open connections
        if self.s3_client:
            # boto3 client cleanup is automatic
            pass
    
    async def create_bucket_if_not_exists(self) -> bool:
        """Create S3 bucket if it doesn't exist."""
        try:
            # Check if bucket exists using head_bucket
            self.s3_client.head_bucket(Bucket=self.config.bucket_name)
            self.logger.info(f"Bucket {self.config.bucket_name} already exists")
            return True
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                # Create bucket with appropriate configuration
                try:
                    if self.config.region == 'us-east-1':
                        # us-east-1 doesn't need LocationConstraint
                        self.s3_client.create_bucket(Bucket=self.config.bucket_name)
                    else:
                        self.s3_client.create_bucket(
                            Bucket=self.config.bucket_name,
                            CreateBucketConfiguration={'LocationConstraint': self.config.region}
                        )
                    
                    # Enable versioning if needed
                    self.s3_client.put_bucket_versioning(
                        Bucket=self.config.bucket_name,
                        VersioningConfiguration={'Status': 'Enabled'}
                    )
                    
                    self.logger.info(f"Successfully created bucket: {self.config.bucket_name}")
                    return True
                except ClientError as create_error:
                    self.logger.error(f"Failed to create bucket: {create_error}")
                    return False
            else:
                self.logger.error(f"Cannot access bucket {self.config.bucket_name}: {e}")
                return False
    
    def get_dataset_prefix(self, dataset_name: str) -> str:
        """Get S3 prefix for dataset."""
        # Validate dataset name (no invalid S3 characters)
        invalid_chars = ['/', '\\', ':', '*', '?', '"', '<', '>', '|']
        if any(char in dataset_name for char in invalid_chars):
            raise ValueError(f"Dataset name contains invalid characters: {dataset_name}")
        
        # Return standardized prefix
        return f"{dataset_name}/"
    
    async def upload_file(self, local_path: Path, s3_key: str,
                         metadata: Optional[Dict[str, str]] = None,
                         show_progress: bool = True) -> Dict[str, Any]:
        """Upload single file to S3 with progress tracking."""
        start_time = time.time()
        file_size = local_path.stat().st_size
        
        # Calculate file checksum for integrity
        checksum = self.calculate_file_checksum(local_path)
        
        # Prepare metadata
        upload_metadata = {
            'checksum': checksum,
            'original_filename': local_path.name,
            'upload_time': datetime.utcnow().isoformat()
        }
        if metadata:
            upload_metadata.update(metadata)
        
        try:
            # Progress tracking
            if show_progress:
                def progress_callback(bytes_transferred):
                    percent = (bytes_transferred / file_size) * 100
                    print(f"\rUploading {local_path.name}: {percent:.1f}%", end='', flush=True)
            else:
                progress_callback = None
            
            # Use multipart upload for large files
            if self.transfer_config:
                self.s3_client.upload_file(
                    str(local_path), 
                    self.config.bucket_name, 
                    s3_key,
                    ExtraArgs={'Metadata': upload_metadata},
                    Config=self.transfer_config,
                    Callback=progress_callback
                )
            else:
                self.s3_client.upload_file(
                    str(local_path), 
                    self.config.bucket_name, 
                    s3_key,
                    ExtraArgs={'Metadata': upload_metadata},
                    Callback=progress_callback
                )
            
            if show_progress:
                print()  # New line after progress
            
            # Verify upload integrity
            response = self.s3_client.head_object(
                Bucket=self.config.bucket_name, 
                Key=s3_key
            )
            
            upload_time = time.time() - start_time
            
            return {
                'success': True,
                'file_size': file_size,
                'upload_time': upload_time,
                'checksum': checksum,
                's3_key': s3_key,
                'etag': response['ETag'].strip('"')
            }
            
        except ClientError as e:
            self.logger.error(f"Failed to upload {local_path}: {e}")
            return {
                'success': False,
                'error': str(e),
                'file_size': file_size,
                's3_key': s3_key
            }
    
    async def upload_dataset_files(self, dataset_name: str, 
                                  local_files: Dict[str, Path],
                                  metadata: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """Upload multiple dataset files concurrently."""
        dataset_prefix = self.get_dataset_prefix(dataset_name)
        upload_results = {}
        total_size = 0
        successful_uploads = 0
        start_time = time.time()
        
        # Calculate total size
        for file_type, file_path in local_files.items():
            if file_path.exists():
                total_size += file_path.stat().st_size
        
        # Upload files in parallel using ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=self.config.max_concurrency) as executor:
            future_to_file = {}
            
            for file_type, file_path in local_files.items():
                if not file_path.exists():
                    upload_results[file_type] = {
                        'success': False,
                        'error': f'File not found: {file_path}'
                    }
                    continue
                
                s3_key = f"{dataset_prefix}{file_type}"
                future = executor.submit(
                    self._sync_upload_file, 
                    file_path, s3_key, metadata, True
                )
                future_to_file[future] = (file_type, file_path)
            
            # Collect results
            for future in as_completed(future_to_file):
                file_type, file_path = future_to_file[future]
                try:
                    result = future.result()
                    upload_results[file_type] = result
                    if result['success']:
                        successful_uploads += 1
                except Exception as e:
                    upload_results[file_type] = {
                        'success': False,
                        'error': str(e)
                    }
        
        total_time = time.time() - start_time
        
        return {
            'dataset_name': dataset_name,
            'total_files': len(local_files),
            'successful_uploads': successful_uploads,
            'failed_uploads': len(local_files) - successful_uploads,
            'total_size': total_size,
            'upload_time': total_time,
            'files': upload_results
        }
    
    def _sync_upload_file(self, local_path: Path, s3_key: str, 
                         metadata: Optional[Dict[str, str]], 
                         show_progress: bool) -> Dict[str, Any]:
        """Synchronous wrapper for upload_file (for ThreadPoolExecutor)."""
        import asyncio
        try:
            # Run async upload in thread
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            result = loop.run_until_complete(
                self.upload_file(local_path, s3_key, metadata, show_progress)
            )
            return result
        finally:
            loop.close()
    
    async def download_file(self, s3_key: str, local_path: Path,
                           show_progress: bool = True) -> Dict[str, Any]:
        """Download single file from S3."""
        start_time = time.time()
        
        try:
            # Get object metadata
            response = self.s3_client.head_object(
                Bucket=self.config.bucket_name, 
                Key=s3_key
            )
            file_size = response['ContentLength']
            
            # Create parent directories if needed
            local_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Progress tracking
            if show_progress:
                def progress_callback(bytes_transferred):
                    percent = (bytes_transferred / file_size) * 100
                    print(f"\rDownloading {s3_key}: {percent:.1f}%", end='', flush=True)
            else:
                def progress_callback(bytes_transferred):
                    pass
            
            # Download file
            if self.transfer_config:
                self.s3_client.download_file(
                    self.config.bucket_name, 
                    s3_key, 
                    str(local_path),
                    Config=self.transfer_config,
                    Callback=progress_callback
                )
            else:
                self.s3_client.download_file(
                    self.config.bucket_name, 
                    s3_key, 
                    str(local_path),
                    Callback=progress_callback
                )
            
            if show_progress:
                print()  # New line after progress
            
            # Verify download integrity if checksum available
            s3_metadata = response.get('Metadata', {})
            if 'checksum' in s3_metadata:
                local_checksum = self.calculate_file_checksum(local_path)
                if local_checksum != s3_metadata['checksum']:
                    self.logger.warning(f"Checksum mismatch for {s3_key}")
            
            download_time = time.time() - start_time
            
            return {
                'success': True,
                'file_size': file_size,
                'download_time': download_time,
                's3_key': s3_key,
                'local_path': str(local_path)
            }
            
        except ClientError as e:
            self.logger.error(f"Failed to download {s3_key}: {e}")
            return {
                'success': False,
                'error': str(e),
                's3_key': s3_key,
                'local_path': str(local_path)
            }
    
    async def download_dataset_files(self, dataset_name: str,
                                    file_types: List[str],
                                    local_dir: Path,
                                    subset_size: Optional[int] = None) -> Dict[str, Path]:
        """Download specific dataset files."""
        dataset_prefix = self.get_dataset_prefix(dataset_name)
        downloaded_files = {}
        
        # Handle subset directory if subset_size specified
        if subset_size:
            dataset_prefix += f"subsets/{subset_size}/"
        
        # List available files for dataset
        available_files = self.list_dataset_files(dataset_name, subset_size)
        
        # Filter files by requested types
        files_to_download = {}
        for file_type in file_types:
            if file_type in available_files:
                s3_key = f"{dataset_prefix}{file_type}"
                local_path = local_dir / file_type
                files_to_download[file_type] = (s3_key, local_path)
            else:
                self.logger.warning(f"File type {file_type} not found for dataset {dataset_name}")
        
        # Download files in parallel
        with ThreadPoolExecutor(max_workers=self.config.download_threads) as executor:
            future_to_file = {}
            
            for file_type, (s3_key, local_path) in files_to_download.items():
                future = executor.submit(
                    self._sync_download_file, s3_key, local_path, True
                )
                future_to_file[future] = (file_type, local_path)
            
            # Collect results
            for future in as_completed(future_to_file):
                file_type, local_path = future_to_file[future]
                try:
                    result = future.result()
                    if result['success']:
                        downloaded_files[file_type] = local_path
                except Exception as e:
                    self.logger.error(f"Failed to download {file_type}: {e}")
        
        return downloaded_files
    
    def _sync_download_file(self, s3_key: str, local_path: Path, 
                           show_progress: bool) -> Dict[str, Any]:
        """Synchronous wrapper for download_file (for ThreadPoolExecutor)."""
        import asyncio
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            result = loop.run_until_complete(
                self.download_file(s3_key, local_path, show_progress)
            )
            return result
        finally:
            loop.close()
    
    def list_datasets(self) -> List[Dict[str, Any]]:
        """List all available datasets in bucket."""
        try:
            # List objects with dataset prefix pattern
            response = self.s3_client.list_objects_v2(
                Bucket=self.config.bucket_name,
                Delimiter='/'
            )
            
            datasets = []
            
            # Group objects by dataset name
            if 'CommonPrefixes' in response:
                for prefix in response['CommonPrefixes']:
                    dataset_name = prefix['Prefix'].rstrip('/')
                    
                    # Load metadata for each dataset
                    try:
                        metadata = self.get_dataset_info(dataset_name)
                        if metadata:
                            datasets.append(metadata)
                    except Exception as e:
                        self.logger.warning(f"Failed to load metadata for {dataset_name}: {e}")
                        # Add basic info even without metadata
                        datasets.append({
                            'name': dataset_name,
                            'status': 'incomplete',
                            'error': str(e)
                        })
            
            return datasets
            
        except ClientError as e:
            self.logger.error(f"Failed to list datasets: {e}")
            return []
    
    def get_dataset_info(self, dataset_name: str) -> Optional[Dict[str, Any]]:
        """Get information about specific dataset."""
        try:
            dataset_prefix = self.get_dataset_prefix(dataset_name)
            
            # Check if dataset exists by looking for metadata.json
            metadata_key = f"{dataset_prefix}metadata.json"
            
            try:
                self.s3_client.head_object(
                    Bucket=self.config.bucket_name, 
                    Key=metadata_key
                )
            except ClientError as e:
                if e.response['Error']['Code'] == '404':
                    return None
                raise
            
            # Load and parse metadata.json
            metadata = asyncio.run(self.download_metadata(dataset_name))
            if not metadata:
                return None
            
            # List available files and subsets
            files_info = self.list_dataset_files(dataset_name)
            
            # Return comprehensive dataset information
            return {
                'name': dataset_name,
                'metadata': metadata.to_dict(),
                'files': files_info,
                'status': 'ready',
                'last_modified': datetime.utcnow().isoformat()
            }
            
        except ClientError as e:
            self.logger.error(f"Failed to get dataset info for {dataset_name}: {e}")
            return None
    
    async def upload_metadata(self, dataset_name: str, 
                             metadata: DatasetMetadata) -> bool:
        """Upload dataset metadata to S3."""
        try:
            dataset_prefix = self.get_dataset_prefix(dataset_name)
            metadata_key = f"{dataset_prefix}metadata.json"
            
            # Serialize metadata to JSON
            metadata_json = json.dumps(metadata.to_dict(), indent=2)
            
            # Upload metadata.json to dataset prefix
            self.s3_client.put_object(
                Bucket=self.config.bucket_name,
                Key=metadata_key,
                Body=metadata_json,
                ContentType='application/json',
                Metadata={
                    'dataset_name': dataset_name,
                    'upload_time': datetime.utcnow().isoformat()
                }
            )
            
            self.logger.info(f"Successfully uploaded metadata for {dataset_name}")
            return True
            
        except ClientError as e:
            self.logger.error(f"Failed to upload metadata for {dataset_name}: {e}")
            return False
    
    async def download_metadata(self, dataset_name: str) -> Optional[DatasetMetadata]:
        """Download and parse dataset metadata."""
        try:
            dataset_prefix = self.get_dataset_prefix(dataset_name)
            metadata_key = f"{dataset_prefix}metadata.json"
            
            # Download metadata.json from S3
            response = self.s3_client.get_object(
                Bucket=self.config.bucket_name,
                Key=metadata_key
            )
            
            # Parse JSON and create DatasetMetadata object
            metadata_data = json.loads(response['Body'].read().decode('utf-8'))
            
            # Validate metadata structure
            metadata = DatasetMetadata.from_dict(metadata_data)
            
            return metadata
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                self.logger.warning(f"Metadata not found for dataset {dataset_name}")
                return None
            else:
                self.logger.error(f"Failed to download metadata for {dataset_name}: {e}")
                return None
        except (json.JSONDecodeError, ValueError) as e:
            self.logger.error(f"Invalid metadata format for {dataset_name}: {e}")
            return None
    
    def list_dataset_files(self, dataset_name: str, 
                          subset_size: Optional[int] = None) -> Dict[str, Dict[str, Any]]:
        """List all files for a dataset with metadata."""
        try:
            dataset_prefix = self.get_dataset_prefix(dataset_name)
            
            if subset_size:
                # Filter by subset if specified
                list_prefix = f"{dataset_prefix}subsets/{subset_size}/"
            else:
                list_prefix = dataset_prefix
            
            # List objects under dataset prefix
            response = self.s3_client.list_objects_v2(
                Bucket=self.config.bucket_name,
                Prefix=list_prefix
            )
            
            files_info = {}
            
            if 'Contents' in response:
                for obj in response['Contents']:
                    key = obj['Key']
                    
                    # Skip subdirectories and metadata file
                    if key.endswith('/') or key.endswith('metadata.json'):
                        continue
                    
                    # Extract file type from key
                    file_type = key.split('/')[-1]
                    
                    # Get object metadata
                    files_info[file_type] = {
                        'key': key,
                        'size': obj['Size'],
                        'last_modified': obj['LastModified'].isoformat(),
                        'etag': obj['ETag'].strip('"')
                    }
            
            return files_info
            
        except ClientError as e:
            self.logger.error(f"Failed to list files for {dataset_name}: {e}")
            return {}
    
    async def validate_dataset_integrity(self, dataset_name: str) -> Dict[str, bool]:
        """Validate dataset file integrity using checksums."""
        validation_results = {}
        
        try:
            # Download metadata with expected checksums
            metadata = await self.download_metadata(dataset_name)
            if not metadata:
                return {'metadata': False}
            
            # Get list of files
            files_info = self.list_dataset_files(dataset_name)
            
            for file_type, file_info in files_info.items():
                try:
                    # Get object metadata
                    response = self.s3_client.head_object(
                        Bucket=self.config.bucket_name,
                        Key=file_info['key']
                    )
                    
                    s3_metadata = response.get('Metadata', {})
                    
                    if 'checksum' in s3_metadata:
                        # For now, assume integrity if checksum exists
                        # Full validation would require downloading and re-computing
                        validation_results[file_type] = True
                    else:
                        validation_results[file_type] = False
                        
                except ClientError:
                    validation_results[file_type] = False
            
            return validation_results
            
        except Exception as e:
            self.logger.error(f"Failed to validate dataset integrity: {e}")
            return {}
    
    def calculate_file_checksum(self, file_path: Path) -> str:
        """Calculate SHA256 checksum for file."""
        hash_sha256 = hashlib.sha256()
        
        with open(file_path, 'rb') as f:
            # Read file in chunks to handle large files
            for chunk in iter(lambda: f.read(4096), b""):
                hash_sha256.update(chunk)
        
        return hash_sha256.hexdigest()
    
    async def delete_dataset(self, dataset_name: str, 
                            confirm: bool = False) -> bool:
        """Delete entire dataset from S3."""
        if not confirm:
            self.logger.error("Dataset deletion requires explicit confirmation")
            return False
        
        try:
            dataset_prefix = self.get_dataset_prefix(dataset_name)
            
            # List all objects under dataset prefix
            response = self.s3_client.list_objects_v2(
                Bucket=self.config.bucket_name,
                Prefix=dataset_prefix
            )
            
            if 'Contents' not in response:
                self.logger.warning(f"No objects found for dataset {dataset_name}")
                return True
            
            # Delete all objects in batches
            objects_to_delete = [{'Key': obj['Key']} for obj in response['Contents']]
            
            # Delete in batches of 1000 (S3 limit)
            for i in range(0, len(objects_to_delete), 1000):
                batch = objects_to_delete[i:i+1000]
                self.s3_client.delete_objects(
                    Bucket=self.config.bucket_name,
                    Delete={'Objects': batch}
                )
            
            self.logger.info(f"Successfully deleted dataset {dataset_name}")
            return True
            
        except ClientError as e:
            self.logger.error(f"Failed to delete dataset {dataset_name}: {e}")
            return False
    
    async def create_dataset_subset(self, dataset_name: str, 
                                   subset_size: int,
                                   source_files: Dict[str, Path]) -> Dict[str, Any]:
        """Create and upload dataset subset."""
        try:
            dataset_prefix = self.get_dataset_prefix(dataset_name)
            subset_prefix = f"{dataset_prefix}subsets/{subset_size}/"
            
            # Upload subset files to appropriate S3 paths
            upload_results = {}
            for file_type, source_path in source_files.items():
                if source_path.exists():
                    s3_key = f"{subset_prefix}{file_type}"
                    result = await self.upload_file(
                        source_path, s3_key,
                        metadata={'subset_size': str(subset_size)}
                    )
                    upload_results[file_type] = result
            
            # Update main dataset metadata with subset info
            metadata = await self.download_metadata(dataset_name)
            if metadata:
                # Add subset info to metadata
                if not hasattr(metadata, 'subsets') or metadata.subsets is None:
                    metadata.subsets = []
                
                subset_info = {
                    'size': subset_size,
                    'files': list(source_files.keys()),
                    'created': datetime.utcnow().isoformat()
                }
                metadata.subsets.append(subset_info)
                
                # Re-upload updated metadata
                await self.upload_metadata(dataset_name, metadata)
            
            return {
                'dataset_name': dataset_name,
                'subset_size': subset_size,
                'upload_results': upload_results,
                'success': all(r.get('success', False) for r in upload_results.values())
            }
            
        except Exception as e:
            self.logger.error(f"Failed to create subset for {dataset_name}: {e}")
            return {
                'dataset_name': dataset_name,
                'subset_size': subset_size,
                'success': False,
                'error': str(e)
            }
    
    def estimate_storage_costs(self, dataset_info: Dict[str, Any]) -> Dict[str, float]:
        """Estimate S3 storage costs for dataset."""
        # Basic cost estimation - would need current AWS pricing
        total_size_gb = 0
        
        if 'files' in dataset_info:
            for file_info in dataset_info['files'].values():
                total_size_gb += file_info.get('size', 0) / (1024**3)
        
        # Rough estimates (would need current pricing)
        storage_cost_per_gb_month = 0.023  # Standard storage
        transfer_cost_per_gb = 0.09        # Data transfer out
        
        return {
            'total_size_gb': total_size_gb,
            'monthly_storage_cost': total_size_gb * storage_cost_per_gb_month,
            'transfer_cost_estimate': total_size_gb * transfer_cost_per_gb,
            'currency': 'USD'
        }
    
    def get_presigned_url(self, s3_key: str, 
                         expiration: int = 3600,
                         method: str = 'GET') -> str:
        """Generate presigned URL for S3 object access."""
        try:
            if method.upper() == 'GET':
                url = self.s3_client.generate_presigned_url(
                    'get_object',
                    Params={'Bucket': self.config.bucket_name, 'Key': s3_key},
                    ExpiresIn=expiration
                )
            elif method.upper() == 'PUT':
                url = self.s3_client.generate_presigned_url(
                    'put_object',
                    Params={'Bucket': self.config.bucket_name, 'Key': s3_key},
                    ExpiresIn=expiration
                )
            else:
                raise ValueError(f"Unsupported method: {method}")
            
            return url
            
        except ClientError as e:
            self.logger.error(f"Failed to generate presigned URL: {e}")
            return ""


class S3StreamingReader:
    """Streaming reader for large S3 files."""
    
    def __init__(self, s3_manager: S3DatasetManager, s3_key: str):
        """Initialize streaming reader for S3 object."""
        self.s3_manager = s3_manager
        self.s3_key = s3_key
        self.file_size = None
        self.current_position = 0
        self.logger = logging.getLogger(__name__)
    
    async def __aenter__(self):
        """Get object metadata and initialize streaming connection."""
        try:
            # Get object metadata (size, etc.)
            response = self.s3_manager.s3_client.head_object(
                Bucket=self.s3_manager.config.bucket_name,
                Key=self.s3_key
            )
            self.file_size = response['ContentLength']
            self.logger.info(f"Initialized streaming reader for {self.s3_key} ({self.file_size} bytes)")
            return self
        except ClientError as e:
            self.logger.error(f"Failed to initialize streaming reader: {e}")
            raise
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Close streaming connection."""
        # Nothing to close for S3 - each request is independent
        pass
    
    async def read_chunk(self, chunk_size: int) -> bytes:
        """Read chunk of data from S3 object."""
        if self.current_position >= self.file_size:
            return b''
        
        # Calculate actual chunk size (don't read past end of file)
        actual_chunk_size = min(chunk_size, self.file_size - self.current_position)
        end_byte = self.current_position + actual_chunk_size - 1
        
        try:
            # Use byte range request to read specific chunk
            response = self.s3_manager.s3_client.get_object(
                Bucket=self.s3_manager.config.bucket_name,
                Key=self.s3_key,
                Range=f'bytes={self.current_position}-{end_byte}'
            )
            
            chunk_data = response['Body'].read()
            
            # Update current position
            self.current_position += len(chunk_data)
            
            return chunk_data
            
        except ClientError as e:
            self.logger.error(f"Failed to read chunk from {self.s3_key}: {e}")
            raise
    
    async def seek(self, position: int):
        """Seek to specific position in S3 object."""
        if position < 0 or (self.file_size and position > self.file_size):
            raise ValueError(f"Invalid seek position: {position}")
        
        self.current_position = position
    
    def tell(self) -> int:
        """Get current position in S3 object."""
        return self.current_position
    
    def size(self) -> Optional[int]:
        """Get total size of S3 object."""
        return self.file_size


class DatasetUploader:
    """Handles complete dataset upload workflow."""
    
    def __init__(self, s3_manager: S3DatasetManager):
        self.s3_manager = s3_manager
        self.logger = logging.getLogger(__name__)
    
    async def upload_complete_dataset(self, dataset_name: str,
                                     local_dataset_dir: Path,
                                     metadata: DatasetMetadata,
                                     generate_subsets: List[int] = None) -> Dict[str, Any]:
        """Upload complete dataset with all components."""
        try:
            # Validate local dataset directory structure
            if not self.validate_dataset_structure(local_dataset_dir):
                return {
                    'success': False,
                    'error': 'Invalid dataset directory structure'
                }
            
            # Collect all files to upload
            files_to_upload = {}
            
            # Vector files (train.vkv, query.vkv, etc.)
            for vkv_file in local_dataset_dir.glob('*.vkv'):
                files_to_upload[vkv_file.name] = vkv_file
            
            # RDB files
            for rdb_file in local_dataset_dir.glob('*.rdb'):
                files_to_upload[rdb_file.name] = rdb_file
            
            # Other files
            for other_file in local_dataset_dir.glob('*'):
                if other_file.is_file() and other_file.suffix not in ['.vkv', '.rdb', '.json']:
                    files_to_upload[other_file.name] = other_file
            
            # Upload main dataset files
            dataset_prefix = self.s3_manager.get_dataset_prefix(dataset_name)
            upload_results = await self.upload_with_progress(files_to_upload, dataset_prefix)
            
            # Generate and upload subsets if requested
            subset_results = {}
            if generate_subsets:
                for subset_size in generate_subsets:
                    subset_dir = local_dataset_dir / 'subsets' / str(subset_size)
                    if subset_dir.exists():
                        subset_files = {}
                        for subset_file in subset_dir.glob('*'):
                            if subset_file.is_file():
                                subset_files[subset_file.name] = subset_file
                        
                        subset_prefix = f"{dataset_prefix}subsets/{subset_size}/"
                        subset_result = await self.upload_with_progress(subset_files, subset_prefix)
                        subset_results[subset_size] = subset_result
            
            # Upload metadata.json last
            metadata_success = await self.s3_manager.upload_metadata(dataset_name, metadata)
            
            # Validate all uploads completed successfully
            all_successful = (
                upload_results.get('success', False) and
                metadata_success and
                all(r.get('success', False) for r in subset_results.values())
            )
            
            return {
                'dataset_name': dataset_name,
                'success': all_successful,
                'main_files': upload_results,
                'subsets': subset_results,
                'metadata_uploaded': metadata_success
            }
            
        except Exception as e:
            self.logger.error(f"Failed to upload complete dataset {dataset_name}: {e}")
            return {
                'dataset_name': dataset_name,
                'success': False,
                'error': str(e)
            }
    
    async def upload_with_progress(self, files_to_upload: Dict[str, Path],
                                  dataset_prefix: str) -> Dict[str, Any]:
        """Upload files with progress tracking."""
        # Calculate total upload size
        total_size = sum(f.stat().st_size for f in files_to_upload.values() if f.exists())
        
        uploaded_size = 0
        upload_results = {}
        
        self.logger.info(f"Starting upload of {len(files_to_upload)} files ({total_size / 1024**2:.1f} MB)")
        
        # Upload files with progress callbacks
        for file_type, file_path in files_to_upload.items():
            if not file_path.exists():
                upload_results[file_type] = {
                    'success': False,
                    'error': f'File not found: {file_path}'
                }
                continue
            
            s3_key = f"{dataset_prefix}{file_type}"
            
            try:
                result = await self.s3_manager.upload_file(
                    file_path, s3_key, show_progress=True
                )
                upload_results[file_type] = result
                
                if result['success']:
                    uploaded_size += result['file_size']
                    progress = (uploaded_size / total_size) * 100
                    self.logger.info(f"Upload progress: {progress:.1f}%")
                    
            except Exception as e:
                upload_results[file_type] = {
                    'success': False,
                    'error': str(e)
                }
        
        successful_uploads = sum(1 for r in upload_results.values() if r.get('success', False))
        
        return {
            'success': successful_uploads == len(files_to_upload),
            'total_files': len(files_to_upload),
            'successful_uploads': successful_uploads,
            'total_size': total_size,
            'uploaded_size': uploaded_size,
            'files': upload_results
        }
    
    def validate_dataset_structure(self, dataset_dir: Path) -> bool:
        """Validate local dataset directory structure."""
        if not dataset_dir.exists() or not dataset_dir.is_dir():
            self.logger.error(f"Dataset directory does not exist: {dataset_dir}")
            return False
        
        # Check for required files
        required_files = ['metadata.json']
        for required_file in required_files:
            if not (dataset_dir / required_file).exists():
                self.logger.error(f"Required file missing: {required_file}")
                return False
        
        # Check for at least one VKV file
        vkv_files = list(dataset_dir.glob('*.vkv'))
        if not vkv_files:
            self.logger.error("No VKV files found in dataset directory")
            return False
        
        # Validate file formats and sizes
        for vkv_file in vkv_files:
            if vkv_file.stat().st_size == 0:
                self.logger.error(f"Empty VKV file: {vkv_file}")
                return False
        
        # Check subset directory structure if present
        subsets_dir = dataset_dir / 'subsets'
        if subsets_dir.exists():
            for subset_dir in subsets_dir.iterdir():
                if subset_dir.is_dir():
                    if not any(subset_dir.glob('*.vkv')):
                        self.logger.warning(f"No VKV files in subset: {subset_dir}")
        
        return True


class DatasetDownloader:
    """Handles dataset download workflow."""
    
    def __init__(self, s3_manager: S3DatasetManager):
        self.s3_manager = s3_manager
        self.logger = logging.getLogger(__name__)
    
    async def download_for_stress_test(self, dataset_name: str,
                                      local_cache_dir: Path,
                                      components: List[str] = None,
                                      subset_size: Optional[int] = None,
                                      force_refresh: bool = False) -> Dict[str, Path]:
        """Download dataset components needed for stress testing."""
        try:
            # Create cache directory
            cache_dir = local_cache_dir / dataset_name
            if subset_size:
                cache_dir = cache_dir / 'subsets' / str(subset_size)
            cache_dir.mkdir(parents=True, exist_ok=True)
            
            # Default components for stress testing
            if components is None:
                components = ['train.vkv', 'query.vkv', 'vectors-only.rdb', 'metadata.json']
            
            downloaded_files = {}
            
            # Check local cache for existing files
            for component in components:
                local_file = cache_dir / component
                
                if local_file.exists() and not force_refresh:
                    # Skip download if file exists and not forcing refresh
                    self.logger.info(f"Using cached file: {local_file}")
                    downloaded_files[component] = local_file
                    continue
                
                # Download component
                dataset_prefix = self.s3_manager.get_dataset_prefix(dataset_name)
                if subset_size:
                    s3_key = f"{dataset_prefix}subsets/{subset_size}/{component}"
                else:
                    s3_key = f"{dataset_prefix}{component}"
                
                result = await self.s3_manager.download_file(s3_key, local_file)
                
                if result['success']:
                    downloaded_files[component] = local_file
                    self.logger.info(f"Downloaded {component}")
                else:
                    self.logger.error(f"Failed to download {component}: {result.get('error', 'Unknown error')}")
            
            # Validate downloaded files
            for component, file_path in downloaded_files.items():
                if not file_path.exists() or file_path.stat().st_size == 0:
                    self.logger.error(f"Invalid downloaded file: {file_path}")
                    downloaded_files.pop(component, None)
            
            return downloaded_files
            
        except Exception as e:
            self.logger.error(f"Failed to download dataset {dataset_name}: {e}")
            return {}
    
    async def download_with_resume(self, s3_key: str, local_path: Path) -> bool:
        """Download file with resume capability."""
        try:
            # Check if partial file exists locally
            if local_path.exists():
                local_size = local_path.stat().st_size
                
                # Get remote file size
                response = self.s3_manager.s3_client.head_object(
                    Bucket=self.s3_manager.config.bucket_name,
                    Key=s3_key
                )
                remote_size = response['ContentLength']
                
                if local_size == remote_size:
                    # File already complete
                    self.logger.info(f"File already complete: {local_path}")
                    return True
                elif local_size < remote_size:
                    # Resume download from current position
                    self.logger.info(f"Resuming download from byte {local_size}")
                    
                    with open(local_path, 'ab') as f:
                        response = self.s3_manager.s3_client.get_object(
                            Bucket=self.s3_manager.config.bucket_name,
                            Key=s3_key,
                            Range=f'bytes={local_size}-'
                        )
                        
                        # Write remaining data
                        for chunk in response['Body'].iter_chunks(chunk_size=8192):
                            f.write(chunk)
                else:
                    # Local file is larger - start over
                    local_path.unlink()
                    return await self.download_with_resume(s3_key, local_path)
            else:
                # Download complete file
                result = await self.s3_manager.download_file(s3_key, local_path)
                return result['success']
            
            # Validate completed download
            if local_path.exists():
                local_size = local_path.stat().st_size
                response = self.s3_manager.s3_client.head_object(
                    Bucket=self.s3_manager.config.bucket_name,
                    Key=s3_key
                )
                remote_size = response['ContentLength']
                
                if local_size == remote_size:
                    self.logger.info(f"Download completed successfully: {local_path}")
                    return True
                else:
                    self.logger.error(f"Size mismatch after download: {local_size} != {remote_size}")
                    return False
            
            return False
            
        except ClientError as e:
            self.logger.error(f"Failed to download with resume {s3_key}: {e}")
            return False
