"""
Integration Guide: Updating Existing Code to Use New Dataset Class

This module provides utilities and examples for migrating existing code
to use the new Dataset class architecture while maintaining backward compatibility.
"""

from typing import Dict, Any, Optional, List
from pathlib import Path
import logging

from .dataset import Dataset, DatasetState, DatasetSource, DatasetRegistry
from .preparer import DatasetPreparer
from .metadata import DatasetMetadata

logger = logging.getLogger(__name__)


class DatasetMigrationHelper:
    """Helper class for migrating existing dataset management to new architecture."""
    
    @staticmethod
    def migrate_existing_metadata_to_dataset(
        metadata_dict: Dict[str, Any],
        s3_bucket: str,
        s3_prefix: str
    ) -> Dataset:
        """
        Migrate existing metadata dictionary to Dataset object.
        
        Args:
            metadata_dict: Existing metadata as dictionary
            s3_bucket: S3 bucket name
            s3_prefix: S3 prefix for dataset
            
        Returns:
            Dataset object with migrated data
        """
        # Extract name from metadata or prefix
        name = metadata_dict.get('dataset', {}).get('name', s3_prefix.split('/')[-1])
        
        # Create dataset
        dataset = Dataset(
            name=name,
            state=DatasetState.READY,  # Assume existing data is ready
            s3_bucket=s3_bucket,
            s3_prefix=s3_prefix
        )
        
        # Convert metadata dict to DatasetMetadata object if needed
        if isinstance(metadata_dict, dict):
            metadata = DatasetMetadata.from_dict(metadata_dict)
        else:
            metadata = metadata_dict
        
        # Update dataset from metadata
        dataset.update_from_metadata(metadata)
        
        return dataset
    
    @staticmethod
    def create_registry_from_existing_datasets(
        existing_datasets: List[Dict[str, Any]],
        s3_bucket: str
    ) -> DatasetRegistry:
        """
        Create a registry from existing dataset information.
        
        Args:
            existing_datasets: List of existing dataset info
            s3_bucket: S3 bucket name
            
        Returns:
            Populated DatasetRegistry
        """
        registry = DatasetRegistry()
        
        for dataset_info in existing_datasets:
            try:
                # Extract dataset information
                name = dataset_info.get('name')
                metadata = dataset_info.get('metadata', {})
                s3_prefix = dataset_info.get('s3_prefix', name)
                
                if not name:
                    logger.warning(f"Skipping dataset without name: {dataset_info}")
                    continue
                
                # Create dataset object
                dataset = DatasetMigrationHelper.migrate_existing_metadata_to_dataset(
                    metadata, s3_bucket, s3_prefix
                )
                
                # Add any additional information
                if 'tags' in dataset_info:
                    dataset.tags = dataset_info['tags']
                
                if 'state' in dataset_info:
                    try:
                        dataset.state = DatasetState(dataset_info['state'])
                    except ValueError:
                        logger.warning(f"Unknown state for {name}: {dataset_info['state']}")
                
                # Register dataset
                registry.register(dataset)
                logger.info(f"Migrated dataset: {name}")
                
            except Exception as e:
                logger.error(f"Failed to migrate dataset {dataset_info.get('name', 'unknown')}: {e}")
        
        return registry
    
    @staticmethod
    async def update_preparer_to_use_dataset_tracking(
        preparer: DatasetPreparer,
        dataset_name: str,
        source_path: Path,
        **kwargs
    ) -> Dataset:
        """
        Update existing preparer usage to use Dataset tracking.
        
        Args:
            preparer: Existing DatasetPreparer instance
            dataset_name: Name for the dataset
            source_path: Path to source data
            **kwargs: Additional preparation arguments
            
        Returns:
            Dataset object with full tracking
        """
        # Create dataset object
        dataset = Dataset(name=dataset_name)
        
        # Use built-in dataset tracking
        return await preparer.prepare_dataset_with_tracking(
            dataset=dataset,
            source_path=source_path,
            **kwargs
        )


class BackwardCompatibilityWrapper:
    """Wrapper to maintain backward compatibility while introducing new Dataset class."""
    
    def __init__(self, registry: Optional[DatasetRegistry] = None):
        self.registry = registry or DatasetRegistry()
    
    def get_dataset_metadata(self, dataset_name: str) -> Optional[DatasetMetadata]:
        """Get metadata in old format for backward compatibility."""
        dataset = self.registry.get(dataset_name)
        return dataset.dataset_metadata if dataset else None
    
    def list_datasets(self) -> List[Dict[str, Any]]:
        """List datasets in old format for backward compatibility."""
        datasets = self.registry.list()
        return [
            {
                'name': d.name,
                'state': d.state.value,
                's3_bucket': d.s3_bucket,
                's3_prefix': d.s3_prefix,
                'components': d.components,
                'subsets': list(d.subsets.keys()),
                'tags': d.tags,
                'created_at': d.created_at,
                'ready': d.is_ready()
            }
            for d in datasets
        ]
    
    def get_dataset_files(self, dataset_name: str) -> Dict[str, str]:
        """Get dataset files in old format."""
        dataset = self.registry.get(dataset_name)
        if not dataset:
            return {}
        
        files = dict(dataset.components)
        
        # Add RDB files
        for rdb_type, rdb_info in dataset.rdb_files.items():
            files[f'rdb_{rdb_type}'] = f"{dataset.s3_prefix}/rdb/{rdb_info.filename}"
        
        return files


# Example migration functions for specific components

def migrate_cli_commands():
    """Example of how to migrate CLI commands to use new Dataset class."""
    
    def old_prepare_command(dataset_name: str, source_path: Path, **kwargs):
        """Old CLI command implementation."""
        # This is how it might have been done before
        # preparer = DatasetPreparer(s3_config, rdb_config)
        # metadata = preparer.prepare_dataset(dataset_name, source_path, **kwargs)
        # return metadata
        pass
    
    def new_prepare_command(dataset_name: str, source_path: Path, **kwargs):
        """New CLI command implementation with Dataset tracking."""
        from .s3_manager import S3Config
        from .rdb_generator import RDBGenerationConfig
        
        # Create configurations (same as before)
        s3_config = S3Config(bucket_name=kwargs.get('s3_bucket', 'vss-datasets'))
        rdb_config = RDBGenerationConfig()
        
        # Create dataset object (new)
        dataset = Dataset(name=dataset_name)
        
        # Prepare with tracking (new)
        async def prepare():
            async with DatasetPreparer(
                s3_config, rdb_config, 
                dataset_registry=registry, 
                use_dataset_tracking=True
            ) as preparer:
                return await preparer.prepare_dataset_with_tracking(
                    dataset=dataset,
                    source_path=source_path,
                    **kwargs
                )
        
        import asyncio
        result_dataset = asyncio.run(prepare())
        
        # Save to registry (new)
        registry = DatasetRegistry()
        registry.register(result_dataset)
        registry.save(Path("./dataset_registry.json"))
        
        return result_dataset


def migrate_s3_operations():
    """Example of how to migrate S3 operations to use Dataset objects."""
    
    def old_s3_download(dataset_name: str, local_dir: Path):
        """Old S3 download implementation."""
        # This might have used raw S3 operations
        pass
    
    def new_s3_download(dataset_name: str, local_dir: Path):
        """New S3 download using Dataset object."""
        registry = DatasetRegistry.load(Path("./dataset_registry.json"))
        dataset = registry.get(dataset_name)
        
        if not dataset:
            raise ValueError(f"Dataset {dataset_name} not found in registry")
        
        if not dataset.is_ready():
            raise ValueError(f"Dataset {dataset_name} is not ready (state: {dataset.state})")
        
        # Use dataset manifest for efficient downloading
        manifest = dataset.get_s3_manifest()
        
        # Download files based on manifest
        # Implementation would use the S3 paths from the dataset object
        return manifest


# Integration checklist and status
INTEGRATION_CHECKLIST = {
    "core_dataset_class": "✅ Implemented",
    "dataset_registry": "✅ Implemented", 
    "integration_layer": "✅ Implemented & Cleaned",
    "unit_tests": "✅ Completed",
    "documentation": "✅ Completed",
    "cli_command_updates": "✅ Completed & Cleaned",
    "existing_preparer_integration": "✅ Completed & Cleaned", 
    "code_cleanup": "✅ Completed",
    
    # Still needed
    "s3_manager_updates": "🔄 Needs Implementation",
    "backward_compatibility": "🔄 Needs Implementation",
    "migration_scripts": "🔄 Needs Implementation",
    "production_deployment": "⏳ Future Phase"
}


def print_integration_status():
    """Print current integration status."""
    print("Dataset Class Integration Status")
    print("=" * 40)
    
    for item, status in INTEGRATION_CHECKLIST.items():
        print(f"{item.replace('_', ' ').title()}: {status}")
    
    print("\n" + "=" * 40)
    completed = sum(1 for status in INTEGRATION_CHECKLIST.values() if status.startswith("✅"))
    total = len(INTEGRATION_CHECKLIST)
    print(f"Progress: {completed}/{total} ({completed/total*100:.1f}%)")


if __name__ == "__main__":
    print_integration_status()