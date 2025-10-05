#!/usr/bin/env python3
"""Test configuration system with S3 and dataset preparation settings."""

import os
import tempfile
import yaml
from pathlib import Path
import pytest

# Add src to path for imports
import sys
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from core.config import (
    Config, S3Config, DatasetPrepConfig, ConfigValidator
)


class TestS3Config:
    """Test S3 configuration."""
    
    def test_default_s3_config(self):
        """Test default S3 configuration values."""
        config = S3Config()
        
        assert config.bucket_name == "vss-datasets"
        assert config.region == "us-east-1"
        assert config.access_key_id is None
        assert config.secret_access_key is None
        assert config.session_token is None
        assert config.multipart_threshold == 64 * 1024 * 1024
        assert config.max_concurrency == 10
        assert config.download_threads == 4
        assert config.max_retries == 3
        assert config.retry_delay == 1.0
    
    def test_s3_config_validation(self):
        """Test S3 configuration validation."""
        # Valid config should pass
        config = S3Config()
        config.validate()  # Should not raise
        
        # Invalid bucket name
        config.bucket_name = ""
        with pytest.raises(ValueError, match="bucket name cannot be empty"):
            config.validate()
        
        # Invalid region
        config.bucket_name = "test-bucket"
        config.region = ""
        with pytest.raises(ValueError, match="region cannot be empty"):
            config.validate()
        
        # Invalid multipart threshold
        config.region = "us-east-1"
        config.multipart_threshold = -1
        with pytest.raises(ValueError, match="multipart_threshold must be positive"):
            config.validate()


class TestDatasetPrepConfig:
    """Test dataset preparation configuration."""
    
    def test_default_datasets_config(self):
        """Test default dataset preparation configuration values."""
        config = DatasetPrepConfig()
        
        assert config.default_compression == "zstd"
        assert config.default_block_size == 1000
        assert config.valkey_host == "localhost"
        assert config.valkey_port == 6379
        assert config.valkey_password is None
        assert config.memory_limit_gb is None
        assert config.batch_size == 1000
        assert config.create_index_by_default is True
        assert config.default_index_algorithm == "HNSW"
        assert config.default_distance_metric == "COSINE"
        assert config.default_hnsw_m == 16
        assert config.default_hnsw_ef_construction == 200
        assert config.max_vector_dimensions == 4096
        assert config.max_dataset_size_gb == 1000.0
        assert config.processing_timeout_minutes == 480
    
    def test_datasets_config_validation(self):
        """Test dataset preparation configuration validation."""
        # Valid config should pass
        config = DatasetPrepConfig()
        config.validate()  # Should not raise
        
        # Invalid compression
        config.default_compression = "invalid"
        with pytest.raises(ValueError, match="Invalid compression type"):
            config.validate()
        
        # Invalid block size
        config.default_compression = "zstd"
        config.default_block_size = -1
        with pytest.raises(ValueError, match="default_block_size must be positive"):
            config.validate()
        
        # Invalid valkey host
        config.default_block_size = 1000
        config.valkey_host = ""
        with pytest.raises(ValueError, match="Valkey host cannot be empty"):
            config.validate()
        
        # Invalid valkey port
        config.valkey_host = "localhost"
        config.valkey_port = -1
        with pytest.raises(ValueError, match="Invalid Valkey port"):
            config.validate()
        
        # Invalid memory limit
        config.valkey_port = 6379
        config.memory_limit_gb = -5.0
        with pytest.raises(ValueError, match="memory_limit_gb must be positive"):
            config.validate()


class TestConfigIntegration:
    """Test full configuration integration."""
    
    def test_config_with_s3_and_datasets(self):
        """Test loading configuration with S3 and dataset prep sections."""
        config_data = {
            "s3": {
                "bucket_name": "test-bucket",
                "region": "us-west-2",
                "multipart_threshold": 128 * 1024 * 1024,
                "max_concurrency": 20,
                "download_threads": 8,
                "max_retries": 5,
            },
            "datasets": {
                "default_compression": "lz4",
                "default_block_size": 2000,
                "valkey_host": "valkey.example.com",
                "valkey_port": 6380,
                "valkey_password": "secret",
                "memory_limit_gb": 16.0,
                "batch_size": 500,
                "create_index_by_default": False,
                "default_index_algorithm": "FLAT",
                "default_distance_metric": "L2",
                "max_vector_dimensions": 2048,
                "max_dataset_size_gb": 500.0,
                "processing_timeout_minutes": 240,
            }
        }
        
        # Create temporary config file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(config_data, f)
            config_file = Path(f.name)
        
        try:
            # Load configuration
            config = Config(config_file)
            
            # Verify S3 config
            assert config.s3.bucket_name == "test-bucket"
            assert config.s3.region == "us-west-2"
            assert config.s3.multipart_threshold == 128 * 1024 * 1024
            assert config.s3.max_concurrency == 20
            assert config.s3.download_threads == 8
            assert config.s3.max_retries == 5
            
            # Verify dataset prep config
            assert config.datasets.default_compression == "lz4"
            assert config.datasets.default_block_size == 2000
            assert config.datasets.valkey_host == "valkey.example.com"
            assert config.datasets.valkey_port == 6380
            assert config.datasets.valkey_password == "secret"
            assert config.datasets.memory_limit_gb == 16.0
            assert config.datasets.batch_size == 500
            assert config.datasets.create_index_by_default is False
            assert config.datasets.default_index_algorithm == "FLAT"
            assert config.datasets.default_distance_metric == "L2"
            assert config.datasets.max_vector_dimensions == 2048
            assert config.datasets.max_dataset_size_gb == 500.0
            assert config.datasets.processing_timeout_minutes == 240
            
        finally:
            # Clean up
            config_file.unlink()
    
    def test_environment_variable_integration(self):
        """Test environment variable integration for S3 and dataset prep."""
        # Set environment variables
        env_vars = {
            "AWS_S3_BUCKET": "env-bucket",
            "AWS_DEFAULT_REGION": "eu-west-1",
            "AWS_ACCESS_KEY_ID": "AKIAIOSFODNN7EXAMPLE",
            "AWS_SECRET_ACCESS_KEY": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            "VST_S3_MAX_CONCURRENCY": "15",
            "VST_VALKEY_HOST": "valkey.example.com",
            "VST_VALKEY_PORT": "6380",
            "VST_VALKEY_PASSWORD": "env-password",
            "VST_MEMORY_LIMIT_GB": "32.0",
            "VST_DEFAULT_COMPRESSION": "lz4",
            "VST_DATASET_BATCH_SIZE": "2000",
        }
        
        # Apply environment variables
        for key, value in env_vars.items():
            os.environ[key] = value
        
        try:
            # Create config without file (should use defaults + env vars)
            config = Config()
            
            # Verify S3 config from environment
            assert config.s3.bucket_name == "env-bucket"
            assert config.s3.region == "eu-west-1"
            assert config.s3.access_key_id == "AKIAIOSFODNN7EXAMPLE"
            assert config.s3.secret_access_key == "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
            assert config.s3.max_concurrency == 15
            
            # Verify dataset prep config from environment
            assert config.datasets.valkey_host == "valkey.example.com"
            assert config.datasets.valkey_port == 6380
            assert config.datasets.valkey_password == "env-password"
            assert config.datasets.memory_limit_gb == 32.0
            assert config.datasets.default_compression == "lz4"
            assert config.datasets.batch_size == 2000
            
        finally:
            # Clean up environment variables
            for key in env_vars:
                if key in os.environ:
                    del os.environ[key]
    
    def test_config_to_dict_includes_new_sections(self):
        """Test that to_dict includes S3 and dataset prep sections."""
        config = Config()
        config_dict = config.to_dict()
        
        # Verify S3 section is included
        assert "s3" in config_dict
        s3_dict = config_dict["s3"]
        assert "bucket_name" in s3_dict
        assert "region" in s3_dict
        assert "access_key_id" in s3_dict
        assert "multipart_threshold" in s3_dict
        
        # Verify dataset prep section is included
        assert "datasets" in config_dict
        datasets_dict = config_dict["datasets"]
        assert "default_compression" in datasets_dict
        assert "valkey_host" in datasets_dict
        assert "default_index_algorithm" in datasets_dict
        assert "max_vector_dimensions" in datasets_dict
    
    def test_config_validator_with_new_sections(self):
        """Test ConfigValidator with S3 and dataset prep sections."""
        # Valid configuration
        valid_config = {
            "s3": {
                "bucket_name": "test-bucket",
                "region": "us-east-1",
            },
            "datasets": {
                "default_compression": "zstd",
                "valkey_host": "localhost",
            }
        }
        
        assert ConfigValidator.validate(valid_config) is True
        
        # Invalid S3 configuration
        invalid_s3_config = {
            "s3": {
                "bucket_name": "",  # Invalid empty bucket name
                "region": "us-east-1",
            }
        }
        
        assert ConfigValidator.validate(invalid_s3_config) is False
        
        # Invalid dataset prep configuration
        invalid_datasets_config = {
            "datasets": {
                "default_compression": "invalid",  # Invalid compression type
                "valkey_host": "localhost",
            }
        }
        
        assert ConfigValidator.validate(invalid_datasets_config) is False


def main():
    """Run configuration tests."""
    import logging
    logging.basicConfig(level=logging.INFO)
    
    print("Testing S3 configuration...")
    test_s3 = TestS3Config()
    test_s3.test_default_s3_config()
    test_s3.test_s3_config_validation()
    print("✓ S3 config tests passed")
    
    print("Testing dataset preparation configuration...")
    test_datasets = TestDatasetPrepConfig()
    test_datasets.test_default_datasets_config()
    test_datasets.test_datasets_config_validation()
    print("✓ Dataset prep config tests passed")
    
    print("Testing configuration integration...")
    test_integration = TestConfigIntegration()
    test_integration.test_config_with_s3_and_datasets()
    test_integration.test_environment_variable_integration()
    test_integration.test_config_to_dict_includes_new_sections()
    test_integration.test_config_validator_with_new_sections()
    print("✓ Configuration integration tests passed")
    
    print("\nAll configuration tests passed! ✓")


if __name__ == "__main__":
    main()
