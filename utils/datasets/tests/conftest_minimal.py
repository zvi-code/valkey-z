# tests/conftest_minimal.py
"""Minimal pytest configuration to avoid fixture conflicts."""

import pytest
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

# Don't define event_loop fixture - let pytest-asyncio handle it


@pytest.fixture
def mock_valkey_client():
    """Provide a mock Valkey client."""
    return MockValkeyClient()


@pytest.fixture
async def mock_connection_pool():
    """Provide a mock connection pool."""
    config = Mock()
    pool = MockAsyncValkeyPool(config)
    await pool.initialize()
    yield pool
    await pool.close()


@pytest.fixture
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