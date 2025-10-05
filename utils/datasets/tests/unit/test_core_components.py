# tests/unit/test_core_components.py
"""Unit tests for core components without Valkey dependency."""

import pytest
import numpy as np
from unittest.mock import Mock, patch, AsyncMock
import asyncio
from pathlib import Path

# Import components to test
from core import (
    VectorOperations,
    VectorExpansionConfig,
    VectorSampler,
    calculate_recall,
    ConnectionConfig,
    MetricCollector,
    MetricAggregator,
    MemoryMetrics,
    Config,
    ConfigValidator,
)

# Import mocks
from tests.mocks import MockValkeyClient, MockConnectionManager


@pytest.mark.unit
class TestVectorOperations:
    """Test vector operations without external dependencies."""
    
    def test_calculate_norm_single_vector(self):
        """Test L2 norm calculation for single vector."""
        vec_ops = VectorOperations(dimensions=3)
        vector = np.array([3.0, 4.0, 0.0])  # Known norm: 5.0
        
        norm = vec_ops.calculate_norm(vector)
        assert np.isclose(norm, 5.0)
        
    def test_calculate_norm_batch(self):
        """Test L2 norm calculation for batch of vectors."""
        vec_ops = VectorOperations(dimensions=3)
        vectors = np.array([
            [3.0, 4.0, 0.0],  # norm: 5.0
            [1.0, 0.0, 0.0],  # norm: 1.0
            [0.0, 0.0, 2.0],  # norm: 2.0
        ])
        
        norms = vec_ops.calculate_norm(vectors)
        expected = np.array([5.0, 1.0, 2.0])
        assert np.allclose(norms, expected)
        
    def test_find_max_norm_vector(self):
        """Test finding vector with maximum norm."""
        vec_ops = VectorOperations(dimensions=3)
        vectors = np.array([
            [1.0, 0.0, 0.0],
            [3.0, 4.0, 0.0],  # Max norm: 5.0
            [0.0, 2.0, 2.0],
        ])
        
        max_vector, max_norm = vec_ops.find_max_norm_vector(vectors)
        assert np.allclose(max_vector, [3.0, 4.0, 0.0])
        assert np.isclose(max_norm, 5.0)
        
    def test_generate_vector_with_norm(self):
        """Test generating vector with specific norm."""
        vec_ops = VectorOperations(dimensions=100)
        target_norm = 42.0
        
        vector = vec_ops.generate_vector_with_norm(target_norm, seed=123)
        actual_norm = np.linalg.norm(vector)
        
        assert np.isclose(actual_norm, target_norm, rtol=1e-5)
        assert vector.shape == (100,)
        
    def test_generate_expansion_vectors(self):
        """Test expansion vector generation."""
        vec_ops = VectorOperations(dimensions=10)
        max_norm = 10.0
        n_expansions = 3
        
        config = VectorExpansionConfig(
            target_norm_multiplier=1.5,
            seed=42
        )
        
        expansion_vectors = vec_ops.generate_expansion_vectors(
            max_norm=max_norm,
            n_expansions=n_expansions,
            config=config
        )
        
        # Check we got the right number of vectors
        assert len(expansion_vectors) == 3
        
        # Check all have norm > max_norm
        for i in range(1, n_expansions + 1):
            vector = expansion_vectors[i]
            norm = np.linalg.norm(vector)
            expected_norm = max_norm * config.target_norm_multiplier * i
            assert norm > max_norm
            assert np.isclose(norm, expected_norm, rtol=1e-5)
            
    def test_expand_dataset(self):
        """Test dataset expansion."""
        vec_ops = VectorOperations(dimensions=3)
        
        vectors = np.array([
            [1.0, 0.0, 0.0],
            [0.0, 1.0, 0.0],
        ])
        keys = ["vec1", "vec2"]
        expansion_vector = np.array([1.0, 1.0, 1.0])
        
        expanded_vectors, expanded_keys = vec_ops.expand_dataset(
            vectors=vectors,
            keys=keys,
            expansion_index=1,
            expansion_vector=expansion_vector
        )
        
        # Check expanded vectors
        expected_vectors = np.array([
            [2.0, 1.0, 1.0],
            [1.0, 2.0, 1.0],
        ])
        assert np.allclose(expanded_vectors, expected_vectors)
        
        # Check expanded keys
        assert expanded_keys == ["expand_1_vec1", "expand_1_vec2"]
        
    def test_validate_expansion(self):
        """Test expansion validation."""
        vec_ops = VectorOperations(dimensions=3)
        
        # Valid expansion vectors
        valid_vectors = {
            1: np.array([3.0, 4.0, 0.0]),  # norm: 5.0
            2: np.array([6.0, 8.0, 0.0]),  # norm: 10.0
        }
        assert vec_ops.validate_expansion(valid_vectors, max_norm=4.0) is True
        
        # Invalid expansion vectors
        invalid_vectors = {
            1: np.array([1.0, 0.0, 0.0]),  # norm: 1.0 <= 4.0
            2: np.array([6.0, 8.0, 0.0]),  # norm: 10.0
        }
        assert vec_ops.validate_expansion(invalid_vectors, max_norm=4.0) is False


@pytest.mark.unit
class TestVectorSampler:
    """Test vector sampling operations."""
    
    def test_sample_for_deletion(self):
        """Test sampling vectors for deletion."""
        sampler = VectorSampler(seed=42)
        
        total_vectors = 1000
        shrink_ratio = 0.3
        
        indices = sampler.sample_for_deletion(total_vectors, shrink_ratio)
        
        # Check correct number sampled
        assert len(indices) == int(total_vectors * shrink_ratio)
        
        # Check all indices are unique
        assert len(set(indices)) == len(indices)
        
        # Check all indices in range
        assert all(0 <= idx < total_vectors for idx in indices)
        
    def test_stratified_sample(self):
        """Test stratified sampling."""
        sampler = VectorSampler(seed=42)
        
        vector_groups = {
            "original": list(range(100)),
            "expanded_1": list(range(100, 200)),
            "expanded_2": list(range(200, 300)),
        }
        
        shrink_ratio = 0.5
        indices = sampler.stratified_sample(vector_groups, shrink_ratio)
        
        # Check total count
        total_vectors = sum(len(group) for group in vector_groups.values())
        expected_deletions = int(total_vectors * shrink_ratio)
        assert len(indices) == expected_deletions
        
        # Check proportional sampling
        # Each group should have roughly 50 deletions
        for group_indices in vector_groups.values():
            group_deletions = sum(1 for idx in indices if idx in group_indices)
            assert 40 <= group_deletions <= 60  # Allow some variance


@pytest.mark.unit
class TestRecallCalculation:
    """Test recall calculation."""
    
    def test_calculate_recall_perfect(self):
        """Test recall calculation with perfect retrieval."""
        retrieved = np.array([0, 1, 2, 3, 4])
        ground_truth = np.array([0, 1, 2, 3, 4])
        
        recall = calculate_recall(retrieved, ground_truth, k=5)
        assert recall == 1.0
        
    def test_calculate_recall_partial(self):
        """Test recall calculation with partial matches."""
        retrieved = np.array([0, 1, 2, 5, 6])
        ground_truth = np.array([0, 1, 2, 3, 4])
        
        recall = calculate_recall(retrieved, ground_truth, k=5)
        assert recall == 0.6  # 3 out of 5 correct
        
    def test_calculate_recall_batch(self):
        """Test recall calculation for multiple queries."""
        retrieved = np.array([
            [0, 1, 2, 3, 4],  # Perfect
            [0, 1, 5, 6, 7],  # 2/5 correct
        ])
        ground_truth = np.array([
            [0, 1, 2, 3, 4],
            [0, 1, 2, 3, 4],
        ])
        
        recall = calculate_recall(retrieved, ground_truth, k=5)
        assert recall == 0.7  # Average of 1.0 and 0.4


@pytest.mark.unit
class TestMetricCollection:
    """Test metric collection without Valkey."""
    
    @pytest.mark.asyncio
    async def test_memory_metrics_collection(self):
        """Test memory metric collection with mock client."""
        mock_client = MockValkeyClient()
        collector = MetricCollector(mock_client, sampling_interval=0.1)
        
        # Collect metrics
        metrics = await collector.collect_memory_metrics()
        
        assert isinstance(metrics, MemoryMetrics)
        assert metrics.rss_mb > 0
        assert metrics.fragmentation_ratio > 0
        assert mock_client.call_count["info"] == 1
        
    @pytest.mark.asyncio
    async def test_operation_latency_recording(self):
        """Test recording operation latencies."""
        mock_client = MockValkeyClient()
        collector = MetricCollector(mock_client)
        
        # Record some latencies
        collector.record_operation_latency("insert", 5.0)
        collector.record_operation_latency("insert", 7.0)
        collector.record_operation_latency("query", 2.0)
        
        # Check metrics
        insert_metrics = collector.get_operation_metrics("insert")
        assert insert_metrics.count == 2
        assert insert_metrics.success_count == 2
        assert len(insert_metrics.latencies) == 2
        
        query_metrics = collector.get_operation_metrics("query")
        assert query_metrics.count == 1


@pytest.mark.unit
class TestMetricAggregation:
    """Test metric aggregation."""
    
    def test_percentile_calculation(self):
        """Test percentile calculations."""
        aggregator = MetricAggregator(window_size=100)
        
        # Add latency samples
        for i in range(100):
            aggregator.add_latency_sample("query", float(i))
        
        p50, p95, p99 = aggregator.calculate_percentiles("query")
        
        assert np.isclose(p50, 49.5, atol=1)
        assert np.isclose(p95, 94.5, atol=1)
        assert np.isclose(p99, 98.5, atol=1)
        
    def test_operation_stats(self):
        """Test operation statistics calculation."""
        aggregator = MetricAggregator()
        
        # Add samples
        samples = [1.0, 2.0, 3.0, 4.0, 5.0]
        for s in samples:
            aggregator.add_latency_sample("insert", s)
        
        stats = aggregator.get_operation_stats("insert")
        
        assert stats["count"] == 5
        assert stats["mean"] == 3.0
        assert stats["min"] == 1.0
        assert stats["max"] == 5.0
        assert stats["p50"] == 3.0


@pytest.mark.unit
class TestConfiguration:
    """Test configuration handling."""
    
    def test_default_configuration(self):
        """Test default configuration values."""
        config = Config()
        
        # Check defaults
        assert config.valkey.host == "localhost"
        assert config.valkey.port == 6379
        assert config.index.algorithm == "HNSW"
        assert config.index.m == 16
        assert config.index.ef_construction == 356
        assert config.workload.n_threads == 8
        assert config.workload.n_clients == 1000
        
    @patch.dict('os.environ', {'VALKEY_HOST': 'test-host', 'VALKEY_PORT': '7000'})
    def test_environment_variable_override(self):
        """Test environment variable configuration."""
        config = Config()
        config._merge_env_vars()
        config._update_from_dict()
        
        assert config.valkey.host == "test-host"
        assert config.valkey.port == 7000
        
    def test_config_validation(self):
        """Test configuration validation."""
        # Valid config
        valid_config = {
            "valkey": {"host": "localhost", "port": 6379},
            "index": {"algorithm": "HNSW", "dimensions": 1536},
        }
        assert ConfigValidator.validate(valid_config) is True
        
        # Invalid config
        invalid_config = {
            "valkey": {"port": 99999},  # Invalid port
        }
        assert ConfigValidator.validate(invalid_config) is False


@pytest.mark.unit
class TestConnectionManagement:
    """Test connection management with mocks."""
    
    @pytest.mark.asyncio
    async def test_connection_pool_initialization(self):
        """Test connection pool initialization."""
        config = ConnectionConfig(host="localhost", max_connections=10)
        conn_manager = MockConnectionManager(config, n_pools=2)
        
        await conn_manager.initialize()
        
        assert conn_manager._initialized is True
        assert len(conn_manager.pools) == 2
        
        # Get pool and client
        pool = conn_manager.get_pool(0)
        client = await pool.get_client()
        
        assert isinstance(client, MockValkeyClient)
        await client.close()
        
    @pytest.mark.asyncio
    async def test_pipeline_execution(self):
        """Test pipeline execution with mocks."""
        config = ConnectionConfig()
        conn_manager = MockConnectionManager(config)
        await conn_manager.initialize()
        
        commands = [
            ("SET", ["key1", "value1"]),
            ("SET", ["key2", "value2"]),
            ("GET", ["key1"]),
        ]
        
        results = await conn_manager.execute_pipeline(commands)
        
        assert results[0] is True  # SET result
        assert results[1] is True  # SET result
        assert results[2] == "value1"  # GET result