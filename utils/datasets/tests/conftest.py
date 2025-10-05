# tests/conftest.py
"""Pytest configuration and shared fixtures."""

import pytest
import pytest_asyncio
import asyncio
import numpy as np
from pathlib import Path
import tempfile
import h5py
from unittest.mock import Mock

from tests.mocks import (
    MockValkeyClient,
    MockConnectionManager,
    MockAsyncValkeyPool,
    MockDataset,
)

from core import (
    ConnectionConfig,
    Config,
)


# Remove the event_loop fixture - pytest-asyncio provides one
# @pytest.fixture(scope="function")
# def event_loop():
#     """Create an instance of the default event loop for each test function."""
#     loop = asyncio.get_event_loop_policy().new_event_loop()
#     yield loop
#     loop.close()


@pytest.fixture
def mock_valkey_client():
    """Provide a mock Valkey client."""
    return MockValkeyClient()


@pytest_asyncio.fixture
async def mock_connection_pool():
    """Provide a mock connection pool."""
    config = Mock()
    pool = MockAsyncValkeyPool(config)
    await pool.initialize()
    yield pool
    await pool.close()


@pytest_asyncio.fixture
async def mock_connection_manager():
    """Provide a mock connection manager."""
    config = ConnectionConfig()
    manager = MockConnectionManager(config, n_pools=2)
    await manager.initialize()
    yield manager
    await manager.close_all()


@pytest.fixture
def mock_dataset():
    """Provide a mock dataset."""
    return MockDataset(n_vectors=1000, dimensions=1536)


@pytest.fixture
def small_mock_dataset():
    """Provide a small mock dataset for faster tests."""
    return MockDataset(n_vectors=100, dimensions=128)


@pytest.fixture
def sample_vectors():
    """Provide sample vectors for testing."""
    np.random.seed(42)
    return np.random.randn(10, 128).astype(np.float32)


@pytest.fixture
def sample_dataset_file(tmp_path):
    """Create a temporary HDF5 dataset file."""
    dataset_path = tmp_path / "test_dataset.h5"
    
    # Create HDF5 file with required structure
    with h5py.File(dataset_path, 'w') as f:
        # Training vectors
        train_vectors = np.random.randn(1000, 128).astype(np.float32)
        f.create_dataset('train', data=train_vectors)
        
        # Test vectors
        test_vectors = np.random.randn(100, 128).astype(np.float32)
        f.create_dataset('test', data=test_vectors)
        
        # Ground truth neighbors
        neighbors = np.random.randint(0, 1000, size=(100, 10))
        f.create_dataset('neighbors', data=neighbors)
    
    return dataset_path


@pytest.fixture
def sample_dataset_path(sample_dataset_file):
    """Provide the path to the sample dataset (alias for sample_dataset_file)."""
    return sample_dataset_file


@pytest.fixture
def config_file(tmp_path):
    """Create a temporary configuration file."""
    import yaml
    
    config_path = tmp_path / "test_config.yaml"
    
    config_data = {
        "valkey": {
            "host": "test-host",
            "port": 7379,
            "max_connections": 100,
        },
        "index": {
            "algorithm": "HNSW",
            "dimensions": 128,
            "m": 8,
            "ef_construction": 200,
        },
        "workload": {
            "n_threads": 4,
            "n_clients": 100,
            "batch_size": 500,
        },
        "monitoring": {
            "sampling_interval": 5.0,
        },
        "output": {
            "csv_path": str(tmp_path / "metrics.csv"),
            "log_level": "DEBUG",
        }
    }
    
    with open(config_path, 'w') as f:
        yaml.dump(config_data, f)
    
    return config_path


@pytest.fixture
def scenario_file(tmp_path):
    """Create a temporary scenario file."""
    import yaml
    
    scenario_path = tmp_path / "test_scenario.yaml"
    
    scenario_data = {
        "name": "test_scenario",
        "description": "Test scenario for unit tests",
        "dataset": "test_dataset",
        "global_config": {
            "n_threads": 2,
            "n_clients": 50,
        },
        "steps": [
            {
                "name": "ingest_phase",
                "type": "workload",
                "workload": "ingest",
                "parameters": {
                    "target_vectors": 1000,
                    "batch_size": 100,
                },
            },
            {
                "name": "query_phase",
                "type": "workload",
                "workload": "query",
                "duration_seconds": 10,
                "parameters": {
                    "queries_per_second": 100,
                },
            },
        ],
    }
    
    with open(scenario_path, 'w') as f:
        yaml.dump(scenario_data, f)
    
    return scenario_path


@pytest.fixture
def mock_metric_collector(mock_valkey_client):
    """Provide a mock metric collector."""
    from core import MetricCollector
    return MetricCollector(mock_valkey_client, sampling_interval=1.0)


@pytest.fixture
def mock_workload_config():
    """Provide a standard workload configuration."""
    return {
        "batch_size": 100,
        "n_threads": 2,
        "n_clients": 50,
        "index_name": "test_index",
        "dimensions": 128,
        "algorithm": "HNSW",
        "m": 8,
        "ef_construction": 200,
        "ef_runtime": 100,
        "query_k": 10,
    }


# Markers for different test categories
pytest.mark.unit = pytest.mark.unit
pytest.mark.integration = pytest.mark.integration
pytest.mark.slow = pytest.mark.slow


# Test utilities
class AsyncContextManager:
    """Helper for testing async context managers."""
    
    def __init__(self, target):
        self.target = target
        self.entered = False
        self.exited = False
        
    async def __aenter__(self):
        self.entered = True
        return self.target
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.exited = True
        return False


def assert_vectors_equal(v1: np.ndarray, v2: np.ndarray, rtol: float = 1e-5):
    """Assert two vectors are equal within tolerance."""
    assert v1.shape == v2.shape, f"Shape mismatch: {v1.shape} vs {v2.shape}"
    assert np.allclose(v1, v2, rtol=rtol), "Vectors not equal within tolerance"


def create_test_vectors(n: int, dim: int, seed: int = 42) -> np.ndarray:
    """Create reproducible test vectors."""
    np.random.seed(seed)
    return np.random.randn(n, dim).astype(np.float32)