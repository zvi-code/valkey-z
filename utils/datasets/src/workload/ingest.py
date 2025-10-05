# src/workload/ingest.py
"""Vector insertion workload."""

from __future__ import annotations

from typing import Dict, Any, List, Optional, Tuple
import asyncio
import logging
import time
import numpy as np
import struct

from .base import BatchedWorkload, WorkloadResult
from .executor import ConcurrentClientManager
from core import VectorOperations, VectorExpansionConfig

logger = logging.getLogger(__name__)


class IngestWorkload(BatchedWorkload):
    """Handles parallel vector insertion into Valkey."""
    
    def __init__(self):
        """Initialize ingest workload."""
        super().__init__("ingest", batch_size=1000)
        self.index_created = False
        self.vectors_inserted = 0
        self.use_expansion = False
        self.expansion_factor = 1
        
    async def execute(self, 
                     connection_pool: Any,
                     dataset: Any,
                     config: Dict[str, Any]) -> WorkloadResult:
        """Execute vector insertion workload."""
        logger.info(f"Starting ingest workload (thread {config.get('thread_id', 0)})")
        
        # Reset metrics
        self.reset_metrics()
        
        # Extract configuration
        self.batch_size = config.get("batch_size", 1000)
        self.use_expansion = config.get("use_expansion", False)
        self.expansion_factor = config.get("expansion_factor", 1)
        target_vectors = config.get("target_vectors", None)
        index_name = config.get("index_name", "vst_index")
        n_clients = config.get("n_clients", 100)
        
        # Create client manager
        client_manager = ConcurrentClientManager(connection_pool, n_clients)
        await client_manager.initialize()
        
        try:
            # Create index if needed (only thread 0)
            if config.get("thread_id", 0) == 0 and not self.index_created:
                await self._create_index(
                    client_manager.clients[0],
                    index_name,
                    config
                )
                self.index_created = True
            
            # Wait a bit for index creation if not thread 0
            if config.get("thread_id", 0) != 0:
                await asyncio.sleep(2.0)
            
            # Prepare expansion if needed
            expansion_vectors = None
            if self.use_expansion:
                vec_ops = VectorOperations(dimensions=config.get("dimensions", 1536))
                max_norm = dataset.get_max_norm()
                
                expansion_config = VectorExpansionConfig(
                    target_norm_multiplier=1.5,
                    seed=42
                )
                
                expansion_vectors = vec_ops.generate_expansion_vectors(
                    max_norm=max_norm,
                    n_expansions=self.expansion_factor,
                    config=expansion_config
                )
                
                logger.info(f"Generated {len(expansion_vectors)} expansion vectors")
            
            # Insert vectors in batches
            vectors_inserted = 0
            batch_count = 0
            
            for batch_vectors, batch_keys in dataset.iterate_batches(
                batch_size=self.batch_size,
                shuffle=True
            ):
                if self.should_stop():
                    logger.info("Stop event received, halting insertion")
                    break
                
                if target_vectors and vectors_inserted >= target_vectors:
                    logger.info(f"Reached target of {target_vectors} vectors")
                    break
                
                # Apply expansion if configured
                if self.use_expansion and expansion_vectors:
                    for exp_idx in range(1, self.expansion_factor + 1):
                        if exp_idx in expansion_vectors:
                            vec_ops = VectorOperations(dimensions=batch_vectors.shape[1])
                            exp_vectors, exp_keys = vec_ops.expand_dataset(
                                vectors=batch_vectors,
                                keys=batch_keys,
                                expansion_index=exp_idx,
                                expansion_vector=expansion_vectors[exp_idx]
                            )
                            
                            # Insert expanded vectors
                            await self._insert_batch_concurrent(
                                client_manager,
                                exp_vectors,
                                exp_keys,
                                index_name
                            )
                else:
                    # Insert original vectors
                    await self._insert_batch_concurrent(
                        client_manager,
                        batch_vectors,
                        batch_keys,
                        index_name
                    )
                
                vectors_inserted += len(batch_keys)
                batch_count += 1
                
                if batch_count % 10 == 0:
                    logger.info(f"Thread {config.get('thread_id', 0)}: "
                               f"Inserted {vectors_inserted} vectors, "
                               f"{self.get_operations_per_second():.2f} ops/sec")
            
            self.vectors_inserted = vectors_inserted
            
        finally:
            await client_manager.cleanup()
        
        # Create final result
        result = self.create_result({
            "vectors_inserted": self.vectors_inserted,
            "batches_processed": self._batches_processed,
            "expansion_used": self.use_expansion,
            "expansion_factor": self.expansion_factor if self.use_expansion else 1,
        })
        
        logger.info(f"Ingest workload completed: {result.success_count} vectors inserted")
        return result
        
    async def _create_index(self,
                          client: Any,
                          index_name: str,
                          config: Dict[str, Any]) -> None:
        """Create vector index if it doesn't exist."""
        try:
            # Check if index exists
            existing_indices = await client.execute_command("FT._LIST")
            if index_name.encode() in existing_indices:
                logger.info(f"Index {index_name} already exists")
                return
            
            # Build index creation command
            algorithm = config.get("algorithm", "HNSW")
            dimensions = config.get("dimensions", 1536)
            distance_metric = config.get("distance_metric", "L2")
            m = config.get("m", 16)
            ef_construction = config.get("ef_construction", 356)
            initial_cap = config.get("initial_cap", 10000)
            
            create_cmd = [
                "FT.CREATE", index_name,
                "ON", "HASH",
                "PREFIX", "1", "vec:",
                "SCHEMA",
                "vector", "VECTOR", algorithm, "10",
                "DIM", str(dimensions),
                "TYPE", "FLOAT32",
                "DISTANCE_METRIC", distance_metric,
                "INITIAL_CAP", str(initial_cap)
            ]
            
            if algorithm == "HNSW":
                create_cmd.extend([
                    "M", str(m),
                    "EF_CONSTRUCTION", str(ef_construction)
                ])
            
            # Create index
            await client.execute_command(*create_cmd)
            logger.info(f"Created index {index_name} with {algorithm} algorithm")
            
        except Exception as e:
            logger.error(f"Failed to create index: {e}")
            raise
        
    async def _insert_batch_concurrent(self,
                                     client_manager: ConcurrentClientManager,
                                     vectors: np.ndarray,
                                     keys: List[str],
                                     index_name: str) -> None:
        """Insert a batch of vectors using concurrent clients."""
        start_time = time.time()
        
        # Prepare operations for concurrent execution
        operations = []
        for i, (vector, key) in enumerate(zip(vectors, keys)):
            # Create operation function
            async def insert_op(client, v=vector, k=key):
                return await self._insert_single_vector(client, v, k)
            
            operations.append(insert_op)
        
        # Execute concurrently
        results = await client_manager.execute_batch(operations)
        
        # Count successes and failures
        success_count = 0
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                self.record_failure(str(result))
                logger.debug(f"Failed to insert {keys[i]}: {result}")
            else:
                success_count += 1
        
        # Record batch metrics
        elapsed_ms = (time.time() - start_time) * 1000
        self.record_batch_success(success_count, elapsed_ms)
        
        if success_count < len(vectors):
            logger.warning(f"Batch insertion: {success_count}/{len(vectors)} succeeded")
        
    async def _insert_single_vector(self,
                                  client: Any,
                                  vector: np.ndarray,
                                  key: str) -> bool:
        """Insert a single vector."""
        try:
            # Serialize vector to bytes
            vector_bytes = self._serialize_vector(vector)
            
            # Use HSET to store vector
            full_key = f"vec:{key}"
            await client.hset(full_key, "vector", vector_bytes)
            
            return True
            
        except Exception as e:
            logger.debug(f"Failed to insert vector {key}: {e}")
            raise
    
    def _serialize_vector(self, vector: np.ndarray) -> bytes:
        """Serialize numpy vector to bytes for Valkey storage."""
        # Ensure float32
        if vector.dtype != np.float32:
            vector = vector.astype(np.float32)
        
        # Convert to bytes (little-endian float32)
        return struct.pack(f"<{len(vector)}f", *vector)
    
    def get_metrics(self) -> Dict[str, float]:
        """Get insertion-specific metrics."""
        metrics = super().get_metrics()
        metrics.update({
            "vectors_inserted": float(self.vectors_inserted),
            "insertion_rate": self.get_operations_per_second(),
            "success_rate": float(self._success_count / self._operation_count) if self._operation_count > 0 else 0.0,
        })
        return metrics