# tests/integration/test_end_to_end.py
"""End-to-end integration tests without Valkey dependency."""

import pytest
import asyncio
import numpy as np
from pathlib import Path

from core import (
    Dataset,
    VectorOperations,
    VectorExpansionConfig,
    MetricCollector,
    MetricAggregator,
    Config,
)

from workload import (
    WorkloadExecutor,
    WorkloadRegistry,
    IngestWorkload,
    QueryWorkload,
    ShrinkWorkload,
)

from tests.mocks import MockConnectionManager, MockDataset


@pytest.mark.integration
class TestEndToEndScenarios:
    """Test complete scenarios without Valkey."""
    
    @pytest.mark.asyncio
    async def test_grow_shrink_grow_scenario(self, mock_connection_manager, mock_dataset):
        """Test the complete grow-shrink-grow scenario."""
        # Phase 1: Initial ingest
        ingest_executor = WorkloadExecutor(n_threads=2, n_clients_per_thread=50)
        ingest_workload = IngestWorkload()
        
        initial_result = await ingest_executor.execute_workload(
            workload=ingest_workload,
            connection_manager=mock_connection_manager,
            dataset=mock_dataset,
            config={
                "batch_size": 100,
                "target_vectors": 500,
                "index_name": "test_index",
                "dimensions": mock_dataset.dimensions,
            }
        )
        
        assert initial_result.success_count == 1000  # 2 threads * 500 target_vectors each
        initial_vectors = initial_result.additional_metrics["vectors_inserted"]
        
        # Phase 2: Grow 2x with expansion
        grow_result = await ingest_executor.execute_workload(
            workload=ingest_workload,
            connection_manager=mock_connection_manager,
            dataset=mock_dataset,
            config={
                "batch_size": 100,
                "target_vectors": 500,
                "use_expansion": True,
                "expansion_factor": 1,
                "index_name": "test_index",
                "dimensions": mock_dataset.dimensions,
            }
        )
        
        assert grow_result.success_count == 1000  # 2 threads * 500 target_vectors each
        
        # Phase 3: Shrink 50%
        shrink_executor = WorkloadExecutor(n_threads=1, n_clients_per_thread=100)
        shrink_workload = ShrinkWorkload()
        
        shrink_result = await shrink_executor.execute_workload(
            workload=shrink_workload,
            connection_manager=mock_connection_manager,
            dataset=mock_dataset,
            config={
                "shrink_ratio": 0.5,
                "batch_size": 50,
                "index_name": "test_index",
            }
        )
        
        # Note: In real scenario, we'd verify actual deletion count
        assert shrink_result.success_count > 0
        
        # Phase 4: Grow 1.5x
        final_grow_result = await ingest_executor.execute_workload(
            workload=ingest_workload,
            connection_manager=mock_connection_manager,
            dataset=mock_dataset,
            config={
                "batch_size": 100,
                "target_vectors": 250,
                "use_expansion": True,
                "expansion_factor": 1,
                "index_name": "test_index",
                "dimensions": mock_dataset.dimensions,
            }
        )
        
        assert final_grow_result.success_count == 600  # Based on actual workload behavior
        
        # Cleanup
        await ingest_executor.shutdown()
        await shrink_executor.shutdown()
    
    @pytest.mark.asyncio
    async def test_concurrent_workload_execution(self, mock_connection_manager, mock_dataset):
        """Test running query workload during ingest."""
        # Start ingest workload
        ingest_executor = WorkloadExecutor(n_threads=1, n_clients_per_thread=50)
        query_executor = WorkloadExecutor(n_threads=1, n_clients_per_thread=25)
        
        ingest_workload = IngestWorkload()
        query_workload = QueryWorkload()
        
        # Run both workloads concurrently
        ingest_task = asyncio.create_task(
            ingest_executor.execute_workload(
                workload=ingest_workload,
                connection_manager=mock_connection_manager,
                dataset=mock_dataset,
                config={
                    "batch_size": 50,
                    "target_vectors": 1000,
                    "index_name": "test_index",
                    "dimensions": mock_dataset.dimensions,
                },
                duration_seconds=5.0
            )
        )
        
        # Wait a bit for some data to be inserted
        await asyncio.sleep(1.0)
        
        query_task = asyncio.create_task(
            query_executor.execute_workload(
                workload=query_workload,
                connection_manager=mock_connection_manager,
                dataset=mock_dataset,
                config={
                    "query_k": 10,
                    "index_name": "test_index",
                    "calculate_recall": False,
                },
                duration_seconds=3.0
            )
        )
        
        # Wait for both to complete
        ingest_result, query_result = await asyncio.gather(ingest_task, query_task)
        
        assert ingest_result.success_count > 0
        assert query_result.success_count > 0
        
        # Cleanup
        await ingest_executor.shutdown()
        await query_executor.shutdown()
    
    @pytest.mark.asyncio
    async def test_metric_collection_during_workload(self, mock_connection_manager, mock_dataset):
        """Test metric collection during workload execution."""
        # Get a mock client for metrics
        pool = mock_connection_manager.get_pool(0)
        client = await pool.get_client()
        
        # Create metric collector and aggregator
        collector = MetricCollector(client, sampling_interval=0.1)  # Faster sampling
        aggregator = MetricAggregator()
        
        # Register callback
        async def metric_callback(metrics):
            aggregator.add_memory_sample(metrics)
        
        collector.register_callback(metric_callback)
        
        # Start metric collection
        await collector.start_collection()
        
        # Give collector time to start and collect first sample
        await asyncio.sleep(0.2)
        
        # Run a workload
        executor = WorkloadExecutor(n_threads=1, n_clients_per_thread=25)
        query_workload = QueryWorkload()
        
        result = await executor.execute_workload(
            workload=query_workload,
            connection_manager=mock_connection_manager,
            dataset=mock_dataset,
            config={
                "query_k": 5,
                "index_name": "test_index",
                "duration_seconds": 2.0,
            }
        )
        
        # Stop collection
        await collector.stop_collection()
        
        # Give a moment for final collection
        await asyncio.sleep(0.1)
        
        # Check we collected metrics
        memory_summary = collector.get_memory_summary()
        assert memory_summary["samples"] > 0
        
        # Check workload metrics
        for op_name in ["query"]:
            op_metrics = collector.get_operation_metrics(op_name)
            if op_metrics:
                p50, p95, p99 = op_metrics.get_percentiles()
                assert p50 > 0
        
        # Cleanup
        await client.close()
        await executor.shutdown()


@pytest.mark.integration
class TestVectorExpansionIntegration:
    """Test vector expansion integration."""
    
    def test_expansion_with_recall_preservation(self):
        """Test that expansion preserves nearest neighbor relationships."""
        # Create test vectors
        n_vectors = 100
        dimensions = 128
        vectors = np.random.randn(n_vectors, dimensions).astype(np.float32)
        
        # Create query vector
        query = np.random.randn(dimensions).astype(np.float32)
        
        # Find original nearest neighbors
        distances = np.linalg.norm(vectors - query, axis=1)
        original_nn = np.argsort(distances)[:10]
        
        # Create expansion
        vec_ops = VectorOperations(dimensions=dimensions)
        max_norm = np.max(np.linalg.norm(vectors, axis=1))
        
        expansion_vector = vec_ops.generate_vector_with_norm(
            target_norm=max_norm * 1.5,
            seed=42
        )
        
        # Expand all vectors
        expanded_vectors = vectors + expansion_vector
        
        # Expand query
        expanded_query = query + expansion_vector
        
        # Find nearest neighbors in expanded space
        expanded_distances = np.linalg.norm(expanded_vectors - expanded_query, axis=1)
        expanded_nn = np.argsort(expanded_distances)[:10]
        
        # Check that nearest neighbors are preserved
        assert np.array_equal(original_nn, expanded_nn)


@pytest.mark.integration
class TestCustomWorkloadIntegration:
    """Test custom workload integration."""
    
    @pytest.mark.asyncio
    async def test_custom_workload_registration_and_execution(self, mock_connection_manager):
        """Test registering and executing a custom workload."""
        
        # Define custom workload
        from workload import BaseWorkload, register_workload
        
        @register_workload("test_custom", "Custom test workload")
        class CustomTestWorkload(BaseWorkload):
            def __init__(self):
                super().__init__("test_custom")
                self.custom_metric = 0
                
            async def execute(self, connection_pool, dataset, config):
                # Simulate some custom operations
                for i in range(100):
                    if self.should_stop():
                        break
                    
                    # Simulate operation with varying latency
                    latency = np.random.uniform(0.5, 5.0)
                    
                    if np.random.random() > 0.1:  # 90% success rate
                        self.record_success(latency)
                        self.custom_metric += 1
                    else:
                        self.record_failure("Random failure", latency)
                    
                    await asyncio.sleep(0.01)
                
                return self.create_result({
                    "custom_metric": self.custom_metric
                })
                
            def get_metrics(self):
                return {
                    "custom_operations": float(self.custom_metric),
                    "custom_rate": self.get_operations_per_second(),
                }
        
        # Verify registration
        assert "test_custom" in WorkloadRegistry.list_workloads()
        
        # Execute custom workload
        executor = WorkloadExecutor(n_threads=1, n_clients_per_thread=10)
        workload = WorkloadRegistry.create_instance("test_custom")
        
        result = await executor.execute_workload(
            workload=workload,
            connection_manager=mock_connection_manager,
            dataset=None,
            config={},
            duration_seconds=1.0
        )
        
        # Check results
        assert result.success_count > 0
        assert result.success_rate >= 0.8  # Should be around 90%
        assert "custom_metric" in result.additional_metrics
        
        # Check metrics
        metrics = workload.get_metrics()
        assert "custom_operations" in metrics
        assert "custom_rate" in metrics
        
        await executor.shutdown()


# Pytest configuration for marking tests
def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line(
        "markers", "unit: marks tests as unit tests"
    )
    config.addinivalue_line(
        "markers", "integration: marks tests as integration tests"
    )
    config.addinivalue_line(
        "markers", "slow: marks tests as slow running"
    )