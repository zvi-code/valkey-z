# tests/unit/test_workload_components.py
"""Unit tests for workload components without Valkey dependency."""

import pytest
import asyncio
import numpy as np
from unittest.mock import Mock, patch, AsyncMock
import time

from workload import (
    BaseWorkload,
    WorkloadResult,
    BatchedWorkload,
    RateLimitedWorkload,
    WorkloadExecutor,
    IngestWorkload,
    QueryWorkload,
    ShrinkWorkload,
    WorkloadRegistry,
    register_workload,
)

from tests.mocks import (
    MockValkeyClient,
    MockConnectionManager,
    MockAsyncValkeyPool,
    MockDataset,
)


@pytest.mark.unit
class TestBaseWorkload:
    """Test base workload functionality."""
    
    class ConcreteWorkload(BaseWorkload):
        """Concrete implementation for testing."""
        
        async def execute(self, connection_pool, dataset, config):
            # Simple implementation that records 10 operations
            for i in range(10):
                if i < 8:
                    self.record_success(latency_ms=i + 1.0)
                else:
                    self.record_failure("Test error", latency_ms=10.0)
            
            return self.create_result()
        
        def get_metrics(self):
            return {"test_metric": 42.0}
    
    @pytest.mark.asyncio
    async def test_workload_execution(self):
        """Test basic workload execution and metrics."""
        workload = self.ConcreteWorkload("test")
        
        result = await workload.execute(None, None, {})
        
        assert result.success_count == 8
        assert result.failure_count == 2
        assert result.success_rate == 0.8
        assert len(result.latency_percentiles) == 3
        assert "p50" in result.latency_percentiles
        
    def test_stop_event_handling(self):
        """Test stop event handling."""
        workload = self.ConcreteWorkload("test")
        stop_event = asyncio.Event()
        
        workload.set_stop_event(stop_event)
        assert not workload.should_stop()
        
        stop_event.set()
        assert workload.should_stop()


@pytest.mark.unit
class TestBatchedWorkload:
    """Test batched workload functionality."""
    
    class ConcreteBatchedWorkload(BatchedWorkload):
        """Concrete implementation for testing."""
        async def execute(self, connection_pool, dataset, config):
            return self.create_result()
        
        def get_metrics(self):
            return super().get_metrics()
    
    def test_batch_success_recording(self):
        """Test recording batch operations."""
        workload = self.ConcreteBatchedWorkload("test_batch", batch_size=100)
        
        # Record a successful batch
        workload.record_batch_success(100, latency_ms=50.0)
        
        assert workload._success_count == 100
        assert workload._operation_count == 100
        assert workload._batches_processed == 1
        
        # Check latency sampling
        assert len(workload._latencies) == 10  # Sampled 10 items
        assert all(lat == 0.5 for lat in workload._latencies)  # 50ms / 100 items


@pytest.mark.unit
class TestRateLimitedWorkload:
    """Test rate-limited workload functionality."""
    
    class ConcreteRateLimitedWorkload(RateLimitedWorkload):
        """Concrete implementation for testing."""
        async def execute(self, connection_pool, dataset, config):
            return self.create_result()
        
        def get_metrics(self):
            return {}
    
    @pytest.mark.asyncio
    async def test_rate_limiting(self):
        """Test rate limiting functionality."""
        workload = self.ConcreteRateLimitedWorkload("test_rate", target_ops_per_second=100)
        
        start_time = time.time()
        
        # Execute 10 operations
        for _ in range(10):
            await workload.rate_limit()
        
        elapsed = time.time() - start_time
        
        # Should take approximately 0.1 seconds (10 ops at 100 ops/sec)
        assert 0.08 <= elapsed <= 0.15


@pytest.mark.unit
class TestWorkloadExecutor:
    """Test workload executor."""
    
    @pytest.mark.asyncio
    async def test_multi_threaded_execution(self):
        """Test multi-threaded workload execution."""
        # Create a simple test workload
        class TestWorkload(BaseWorkload):
            def __init__(self, name = "test"):
                """Initialize test workload."""
                super().__init__(name)
                
            async def execute(self, connection_pool, dataset, config):
                thread_id = config.get("thread_id", 0)
                # Each thread does 10 operations
                for i in range(10):
                    self.record_success(latency_ms=1.0)
                    await asyncio.sleep(0.001)
                
                return self.create_result({"thread_id": thread_id})
            
            def get_metrics(self):
                return {}
        
        # Setup
        workload = TestWorkload("test")
        config = Mock()
        conn_manager = MockConnectionManager(config, n_pools=2)
        await conn_manager.initialize()
        
        dataset = MockDataset()
        
        # Create executor with 2 threads
        executor = WorkloadExecutor(n_threads=2, n_clients_per_thread=10)
        
        # Execute workload
        result = await executor.execute_workload(
            workload=workload,
            connection_manager=conn_manager,
            dataset=dataset,
            config={},
            duration_seconds=1.0
        )
        
        # Check results
        assert result.success_count == 20  # 2 threads * 10 operations
        assert result.failure_count == 0
        assert result.additional_metrics["threads_used"] == 2
        
        await executor.shutdown()


@pytest.mark.unit
class TestIngestWorkload:
    """Test ingest workload."""
    
    @pytest.mark.asyncio
    async def test_vector_insertion(self):
        """Test vector insertion workload."""
        # Setup
        workload = IngestWorkload()
        pool = MockAsyncValkeyPool(Mock())
        await pool.initialize()
        
        dataset = MockDataset(n_vectors=100, dimensions=10)
        
        config = {
            "batch_size": 10,
            "target_vectors": 50,
            "index_name": "test_index",
            "dimensions": 10,
            "n_clients": 5,
        }
        
        # Execute
        result = await workload.execute(pool, dataset, config)
        
        # Check results
        assert result.success_count > 0
        assert result.additional_metrics["vectors_inserted"] == 50
        assert result.additional_metrics["batches_processed"] == 5
        
    def test_vector_serialization(self):
        """Test vector serialization to bytes."""
        workload = IngestWorkload()
        
        vector = np.array([1.0, 2.0, 3.0], dtype=np.float32)
        serialized = workload._serialize_vector(vector)
        
        # Check it's bytes
        assert isinstance(serialized, bytes)
        assert len(serialized) == 12  # 3 * 4 bytes
        
        # Deserialize and check
        import struct
        deserialized = struct.unpack('<3f', serialized)
        assert np.allclose(deserialized, vector)


@pytest.mark.unit
class TestQueryWorkload:
    """Test query workload."""
    
    @pytest.mark.asyncio
    async def test_query_execution(self):
        """Test query execution."""
         # Setup
        workload = IngestWorkload()
        pool = MockAsyncValkeyPool(Mock())
        await pool.initialize()
        
        dataset = MockDataset(n_vectors=100, dimensions=10)
        
        config = {
            "batch_size": 10,
            "target_vectors": 50,
            "index_name": "test_index",
            "dimensions": 10,
            "n_clients": 5,
        }
        
        # Execute
        result = await workload.execute(pool, dataset, config)
        
        # Check results
        assert result.success_count > 0
        assert result.additional_metrics["vectors_inserted"] == 50
        assert result.additional_metrics["batches_processed"] == 5
        workload = QueryWorkload()
                
        config = {
            "query_k": 5,
            "ef_runtime": 100,
            "index_name": "test_index",
            "n_clients": 5,
            "duration_seconds": 1,
            "calculate_recall": False,
        }
        
        # Execute
        result = await workload.execute(pool, dataset, config)
        
        # Check results
        assert result.success_count > 0
        assert result.additional_metrics["queries_executed"] > 0
        assert result.additional_metrics["k"] == 5
        
    def test_key_to_index_conversion(self):
        """Test converting keys to indices."""
        workload = QueryWorkload()
        
        # Test original keys
        keys = ["train_42", "train_100", "test_5"]
        indices = workload._keys_to_indices(keys)
        assert indices == [42, 100, 5]
        
        # Test expanded keys
        keys = ["expand_1_train_42", "expand_2_train_100"]
        indices = workload._keys_to_indices(keys)
        assert indices == [42, 100]
        
    def test_recall_calculation_integration(self):
        """Test recall calculation with retrieved results."""
        workload = QueryWorkload()
        
        # Add some recall scores
        workload.recall_scores = [1.0, 0.8, 0.6, 0.9, 1.0]
        
        avg_recall = workload.calculate_average_recall()
        assert np.isclose(avg_recall, 0.86)
        
        # Test metrics
        metrics = workload.get_metrics()
        assert "average_recall" in metrics
        assert "recall_min" in metrics
        assert "recall_max" in metrics


@pytest.mark.unit
class TestShrinkWorkload:
    """Test shrink workload."""
    
    @pytest.mark.asyncio
    async def test_deletion_execution(self):
        """Test deletion workload execution."""
        # Setup
        workload = IngestWorkload()
        pool = MockAsyncValkeyPool(Mock())
        await pool.initialize()
        
        dataset = MockDataset(n_vectors=100, dimensions=10)
        
        config = {
            "batch_size": 10,
            "target_vectors": 50,
            "index_name": "test_index",
            "dimensions": 10,
            "n_clients": 5,
        }
        
        # Execute
        result = await workload.execute(pool, dataset, config)
        
        # Check results
        assert result.success_count > 0
        assert result.additional_metrics["vectors_inserted"] == 50
        assert result.additional_metrics["batches_processed"] == 5
        workload = ShrinkWorkload()
        # pool = MockAsyncValkeyPool(Mock())
        # await pool.initialize()
        
        # Prepare mock client with keys
        client = await pool.get_client()
        # # Add keys to both data and hash_data to ensure they're found
        # for i in range(100):
        #     key = f"train_{i}"
        #     await client.hset(key, "vector", b"data")
        #     client.data[key] = b"data"  # Also add to regular data dict
        
        config = {
            "shrink_ratio": 0.3,
            "batch_size": 10,
            "index_name": "test_index",
            "n_clients": 5,
        }
        
        # Execute
        result = await workload.execute(pool, MockDataset(), config)
        
        # Check results - should have deleted some vectors
        assert result.success_count >= 10  # At least 10 deletions (allowing for some variance)
        assert "vectors_deleted" in result.additional_metrics
        assert result.additional_metrics["vectors_deleted"] >= 10

    def test_key_filtering(self):
        """Test key filtering with exclusion patterns."""
        workload = ShrinkWorkload()
        
        keys = [
            "train_1",
            "expand_1_train_2",
            "test_3",
            "expand_2_train_4"
        ]
        
        # Exclude expanded vectors
        filtered = workload._filter_keys(keys, ["expand_"])
        assert filtered == ["train_1", "test_3"]
        
        # Exclude test vectors
        filtered = workload._filter_keys(keys, ["test_"])
        assert len(filtered) == 3


@pytest.mark.unit
class TestWorkloadRegistry:
    """Test workload registry."""
    
    def test_workload_registration(self):
        """Test registering and retrieving workloads."""
        # Clear registry first
        WorkloadRegistry.clear()
        
        # Register a test workload
        @register_workload("test_workload", "A test workload")
        class TestWorkload(BaseWorkload):
            def __init__(self):
                super().__init__("test_workload")
                
            async def execute(self, connection_pool, dataset, config):
                return self.create_result()
            
            def get_metrics(self):
                return {}
        
        # Check registration
        assert "test_workload" in WorkloadRegistry.list_workloads()
        
        # Get workload class
        workload_class = WorkloadRegistry.get("test_workload")
        assert workload_class is TestWorkload
        
        # Create instance
        instance = WorkloadRegistry.create_instance("test_workload")
        assert isinstance(instance, TestWorkload)
        
    def test_invalid_workload_retrieval(self):
        """Test retrieving non-existent workload."""
        with pytest.raises(KeyError):
            WorkloadRegistry.get("non_existent_workload")
            
    def test_workload_info(self):
        """Test getting workload information."""
        # Register built-in workloads
        from workload import register_builtin_workloads
        register_builtin_workloads()
        
        info = WorkloadRegistry.get_workload_info()
        
        # Check we have info for built-in workloads
        workload_names = [w["name"] for w in info]
        assert "ingest" in workload_names
        assert "query" in workload_names
        assert "shrink" in workload_names


@pytest.mark.unit
class TestWorkloadIntegration:
    """Integration tests for workload components."""
    
    @pytest.mark.asyncio
    async def test_full_workload_lifecycle(self):
        """Test complete workload lifecycle with mocks."""
        # Setup
        config = Mock()
        conn_manager = MockConnectionManager(config, n_pools=2)
        await conn_manager.initialize()
        
        dataset = MockDataset(n_vectors=1000)
        
        # 1. Ingest workload
        ingest_executor = WorkloadExecutor(n_threads=2, n_clients_per_thread=50)
        ingest_workload = IngestWorkload()
        
        ingest_result = await ingest_executor.execute_workload(
            workload=ingest_workload,
            connection_manager=conn_manager,
            dataset=dataset,
            config={
                "batch_size": 100,
                "target_vectors": 500,
                "index_name": "test_index",
            },
            duration_seconds=2.0
        )
        
        assert ingest_result.success_count > 0
        
        # 2. Query workload
        query_executor = WorkloadExecutor(n_threads=2, n_clients_per_thread=25)
        query_workload = QueryWorkload()
        
        query_result = await query_executor.execute_workload(
            workload=query_workload,
            connection_manager=conn_manager,
            dataset=dataset,
            config={
                "query_k": 10,
                "duration_seconds": 1.0,
            }
        )
        
        assert query_result.success_count > 0
        
        # 3. Shrink workload
        shrink_executor = WorkloadExecutor(n_threads=1, n_clients_per_thread=50)
        shrink_workload = ShrinkWorkload()
        
        shrink_result = await shrink_executor.execute_workload(
            workload=shrink_workload,
            connection_manager=conn_manager,
            dataset=dataset,
            config={
                "shrink_ratio": 0.5,
                "batch_size": 50,
            }
        )
        
        assert shrink_result.success_count > 0
        
        # Cleanup
        await ingest_executor.shutdown()
        await query_executor.shutdown()
        await shrink_executor.shutdown()
        await conn_manager.close_all()