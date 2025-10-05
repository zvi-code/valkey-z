#!/usr/bin/env python3
"""
Example usage of workload execution components.

This demonstrates:
1. Registering custom workloads
2. Executing built-in workloads
3. Multi-threaded execution
4. Monitoring and metrics collection
"""

import asyncio
import logging
from pathlib import Path
from typing import Dict, Any

from core import Config, ConnectionManager, Dataset, generate_test_vectors

from workload import (
    BaseWorkload,
    WorkloadResult,
    WorkloadExecutor,
    WorkloadRegistry,
    register_workload,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# Example custom workload
@register_workload("mixed", "Mixed read/write workload (70% queries, 30% updates)")
class MixedWorkload(BaseWorkload):
    """Custom workload demonstrating mixed operations."""
    
    def __init__(self):
        super().__init__("mixed")
        self.query_ratio = 0.7
        self.update_ratio = 0.3
        
    async def execute(self, 
                     connection_pool: Any,
                     dataset: Any,
                     config: Dict[str, Any]) -> WorkloadResult:
        """Execute mixed workload."""
        logger.info(f"Executing mixed workload (thread {config.get('thread_id', 0)})")
        
        # Reset metrics
        self.reset_metrics()
        
        # Simulate workload for demonstration
        import random
        import time
        
        operations = 1000
        for i in range(operations):
            if self.should_stop():
                break
                
            # Randomly choose operation type
            if random.random() < self.query_ratio:
                # Simulate query
                latency = random.uniform(0.5, 5.0)
                self.record_success(latency)
            else:
                # Simulate update
                latency = random.uniform(1.0, 10.0)
                self.record_success(latency)
            
            # Small delay to simulate real operations
            await asyncio.sleep(0.001)
        
        return self.create_result({
            "query_ratio": self.query_ratio,
            "update_ratio": self.update_ratio,
        })
    
    def get_metrics(self) -> Dict[str, float]:
        """Return workload metrics."""
        return {
            "operations_per_second": self.get_operations_per_second(),
            "query_percentage": self.query_ratio * 100,
            "update_percentage": self.update_ratio * 100,
        }


async def example_ingest_workload():
    """Example of running the ingest workload."""
    logger.info("\n=== Ingest Workload Example ===")
    
    # Setup
    config = Config()
    conn_config = config.get_valkey_config()
    
    # Create connection manager
    conn_manager = ConnectionManager(conn_config, n_pools=2)
    await conn_manager.initialize()
    
    # Create or load dataset
    dataset_path = Path("test_dataset.h5")
    if not dataset_path.exists():
        logger.info("Creating test dataset...")
        # Generate test data
        from core import VectorOperations
        vec_ops = VectorOperations()
        
        # Create mock dataset
        class MockDataset:
            def __init__(self):
                self.vectors = generate_test_vectors(n_vectors=10000, dimensions=1536)
                self._max_norm = 100.0
                
            def iterate_batches(self, batch_size=1000, shuffle=False):
                for i in range(0, len(self.vectors), batch_size):
                    batch = self.vectors[i:i+batch_size]
                    keys = [f"test_{j}" for j in range(i, i+len(batch))]
                    yield batch, keys
                    
            def get_max_norm(self):
                return self._max_norm
        
        dataset = MockDataset()
    else:
        dataset = Dataset(dataset_path)
        dataset.load()
    
    # Create workload executor
    executor = WorkloadExecutor(n_threads=4, n_clients_per_thread=250)
    
    # Get ingest workload
    ingest_workload = WorkloadRegistry.create_instance("ingest")
    
    # Configuration for workload
    workload_config = {
        "batch_size": 1000,
        "target_vectors": 5000,
        "index_name": "test_index",
        "algorithm": "HNSW",
        "dimensions": 1536,
        "m": 16,
        "ef_construction": 356,
        "use_expansion": False,
    }
    
    # Execute workload
    try:
        result = await executor.execute_workload(
            workload=ingest_workload,
            connection_manager=conn_manager,
            dataset=dataset,
            config=workload_config,
            duration_seconds=30  # Run for 30 seconds max
        )
        
        logger.info(f"Ingest completed: {result.success_count} vectors inserted")
        logger.info(f"Performance: {result.operations_per_second:.2f} ops/sec")
        logger.info(f"Latencies - p50: {result.latency_percentiles['p50']:.2f}ms, "
                   f"p95: {result.latency_percentiles['p95']:.2f}ms, "
                   f"p99: {result.latency_percentiles['p99']:.2f}ms")
        
    finally:
        await executor.shutdown()
        await conn_manager.close_all()


async def example_query_workload():
    """Example of running the query workload."""
    logger.info("\n=== Query Workload Example ===")
    
    # Setup (assuming index already exists from ingest)
    config = Config()
    conn_config = config.get_valkey_config()
    
    conn_manager = ConnectionManager(conn_config, n_pools=2)
    await conn_manager.initialize()
    
    # Create mock dataset with test vectors
    class MockDatasetWithQueries:
        def __init__(self):
            self.test_vectors = generate_test_vectors(n_vectors=100, dimensions=1536)
            self._ground_truth = None  # No ground truth for this example
            
        def get_test_vectors(self):
            return self.test_vectors
    
    dataset = MockDatasetWithQueries()
    
    # Create executor
    executor = WorkloadExecutor(n_threads=2, n_clients_per_thread=50)
    
    # Get query workload
    query_workload = WorkloadRegistry.create_instance("query")
    
    # Configuration
    workload_config = {
        "query_k": 10,
        "ef_runtime": 200,
        "index_name": "test_index",
        "queries_per_second": 100,  # Rate limited to 100 QPS
        "duration_seconds": 10,
        "calculate_recall": False,  # No ground truth available
    }
    
    # Execute
    try:
        result = await executor.execute_workload(
            workload=query_workload,
            connection_manager=conn_manager,
            dataset=dataset,
            config=workload_config
        )
        
        logger.info(f"Query completed: {result.success_count} queries executed")
        logger.info(f"Performance: {result.operations_per_second:.2f} QPS")
        logger.info(f"Success rate: {result.success_rate:.2%}")
        
    finally:
        await executor.shutdown()
        await conn_manager.close_all()


async def example_shrink_workload():
    """Example of running the shrink workload."""
    logger.info("\n=== Shrink Workload Example ===")
    
    # Setup
    config = Config()
    conn_config = config.get_valkey_config()
    
    conn_manager = ConnectionManager(conn_config, n_pools=1)
    await conn_manager.initialize()
    
    # Shrink doesn't need dataset
    dataset = None
    
    # Create executor (single thread for deletion)
    executor = WorkloadExecutor(n_threads=1, n_clients_per_thread=100)
    
    # Get shrink workload
    shrink_workload = WorkloadRegistry.create_instance("shrink")
    
    # Configuration
    workload_config = {
        "shrink_ratio": 0.5,  # Delete 50% of vectors
        "batch_size": 100,
        "index_name": "test_index",
        "deletion_strategy": "random",
    }
    
    # Execute
    try:
        result = await executor.execute_workload(
            workload=shrink_workload,
            connection_manager=conn_manager,
            dataset=dataset,
            config=workload_config
        )
        
        logger.info(f"Shrink completed: {result.success_count} vectors deleted")
        logger.info(f"Deletion rate: {result.operations_per_second:.2f} DPS")
        
        if "initial_count" in result.additional_metrics:
            logger.info(f"Index size: {result.additional_metrics['initial_count']} -> "
                       f"{result.additional_metrics['final_count']}")
        
    finally:
        await executor.shutdown()
        await conn_manager.close_all()


async def example_custom_workload():
    """Example of running a custom workload."""
    logger.info("\n=== Custom Mixed Workload Example ===")
    
    # List all registered workloads
    logger.info("Available workloads:")
    for info in WorkloadRegistry.get_workload_info():
        logger.info(f"  - {info['name']}: {info['description']}")
    
    # Setup
    config = Config()
    conn_config = config.get_valkey_config()
    
    conn_manager = ConnectionManager(conn_config, n_pools=2)
    await conn_manager.initialize()
    
    # Create executor
    executor = WorkloadExecutor(n_threads=4, n_clients_per_thread=25)
    
    # Get custom workload
    mixed_workload = WorkloadRegistry.create_instance("mixed")
    
    # Execute
    try:
        result = await executor.execute_workload(
            workload=mixed_workload,
            connection_manager=conn_manager,
            dataset=None,
            config={},
            duration_seconds=5
        )
        
        logger.info(f"Mixed workload completed: {result.success_count} operations")
        logger.info(f"Performance: {result.operations_per_second:.2f} ops/sec")
        
    finally:
        await executor.shutdown()
        await conn_manager.close_all()


async def example_workload_with_metrics():
    """Example showing workload execution with metric collection."""
    logger.info("\n=== Workload with Metric Collection Example ===")
    
    # Setup
    config = Config()
    conn_config = config.get_valkey_config()
    
    conn_manager = ConnectionManager(conn_config, n_pools=1)
    await conn_manager.initialize()
    
    # Get a Valkey client for metric collection
    pool = conn_manager.get_pool(0)
    client = await pool.get_client()
    
    # Create metric collector
    metric_collector = MetricCollector(client, sampling_interval=2.0)
    
    # Start metric collection
    await metric_collector.start_collection()
    
    # Create executor
    executor = WorkloadExecutor(n_threads=2, n_clients_per_thread=50)
    
    # Run a workload
    query_workload = WorkloadRegistry.create_instance("query")
    
    # Create mock dataset
    class MockDataset:
        def get_test_vectors(self):
            return generate_test_vectors(n_vectors=50, dimensions=1536)
    
    try:
        result = await executor.execute_workload(
            workload=query_workload,
            connection_manager=conn_manager,
            dataset=MockDataset(),
            config={
                "query_k": 10,
                "index_name": "test_index",
                "duration_seconds": 10,
            }
        )
        
        # Get memory summary
        memory_summary = metric_collector.get_memory_summary()
        
        logger.info(f"Workload completed with metrics:")
        logger.info(f"  Operations: {result.success_count}")
        logger.info(f"  Memory RSS: {memory_summary.get('rss_mb', {}).get('avg', 0):.1f} MB avg")
        logger.info(f"  Fragmentation: {memory_summary.get('fragmentation', {}).get('avg', 1.0):.2f} avg")
        
    finally:
        await metric_collector.stop_collection()
        await client.close()
        await executor.shutdown()
        await conn_manager.close_all()


async def main():
    """Run all examples."""
    logger.info("Starting Workload Execution Examples\n")
    
    # Note: Comment out examples that require a running Valkey instance
    
    # Example 1: Ingest workload
    # await example_ingest_workload()
    
    # Example 2: Query workload
    # await example_query_workload()
    
    # Example 3: Shrink workload
    # await example_shrink_workload()
    
    # Example 4: Custom workload (can run without Valkey)
    await example_custom_workload()
    
    # Example 5: Workload with metrics
    # await example_workload_with_metrics()
    
    logger.info("\nAll examples completed!")


if __name__ == "__main__":
    asyncio.run(main())