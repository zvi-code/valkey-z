"""
Dataset Preparation Module for S3-based Vector Dataset Management

This module provides comprehensive dataset preparation capabilities including:
- VKV format conversion for streaming-optimized vector storage  
- RDB generation (vectors-only and vectors+index)
- S3 bucket management and organization
- Subset generation for different scale testing
- Metadata management and validation
"""

from .formats import VKVFormat
from .converter import DatasetConverter
from .rdb_generator import RDBGenerator, IndexSpec  
from .s3_manager import S3DatasetManager, S3Config
from .metadata import DatasetMetadata, VectorStats
from .preparer import DatasetPreparer
from .streaming import VKVStreamReader, VKVStreamWriter
from .dataset import Dataset, DatasetState, DatasetSource, DatasetSubset, DatasetRegistry

__all__ = [
    'VKVFormat',
    'DatasetConverter',
    'RDBGenerator', 
    'IndexSpec',
    'S3DatasetManager',
    'S3Config',
    'DatasetMetadata',
    'VectorStats', 
    'DatasetPreparer',
    'VKVStreamReader',
    'VKVStreamWriter',
    'Dataset',
    'DatasetState',
    'DatasetSource',
    'DatasetSubset',
    'DatasetRegistry'
]