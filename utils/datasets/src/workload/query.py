# src/workload/query.py
"""KNN query workload with recall calculation."""

from __future__ import annotations

from typing import Dict, Any, List, Tuple, Optional
import asyncio
import numpy as np
import logging
import time
import struct

from .base import RateLimitedWorkload, WorkloadResult
from .executor import ConcurrentClientManager
from core import calculate_recall

logger = logging.getLogger(__name__)


class QueryWorkload(RateLimitedWorkload):
    """Handles KNN query execution and recall measurement."""
    
    def __init__(self):
        """Initialize query workload."""
        super().__init__("query", target_ops_per_second=None)
        self.recall_scores: List[float] = []
        self.query_count = 0
        self.total_recall = 0.0
        
    async def execute(self, 
                     connection_pool: Any,
                     dataset: Any,
                     config: Dict[str, Any]) -> WorkloadResult:
        """Execute KNN query workload."""
        logger.info(f"Starting query workload (thread {config.get('thread_id', 0)})")
        
        # Reset metrics
        self.reset_metrics()
        
        # Extract configuration
        k = config.get("query_k", 10)
        ef_runtime = config.get("ef_runtime", 200)
        index_name = config.get("index_name", "vst_index")
        n_clients = config.get("n_clients", 100)
        queries_per_second = config.get("queries_per_second", None)
        duration_seconds = config.get("duration_seconds", None)
        calculate_recall_metric = config.get("calculate_recall", True)
        
        # Set rate limit if specified
        self.target_ops_per_second = queries_per_second
        
        # Get test vectors and ground truth
        test_vectors = dataset.get_test_vectors()
        ground_truth = None
        
        if calculate_recall_metric and dataset._ground_truth is not None:
            ground_truth = dataset.get_ground_truth(k=k)
            logger.info(f"Will calculate recall using {len(test_vectors)} query vectors")
        else:
            logger.info("Recall calculation disabled or ground truth not available")
        
        # Create client manager
        client_manager = ConcurrentClientManager(connection_pool, n_clients)
        await client_manager.initialize()
        
        try:
            # Execute queries
            query_idx = 0
            start_time = time.time()
            
            while True:
                if self.should_stop():
                    logger.info("Stop event received, halting queries")
                    break
                
                if duration_seconds and (time.time() - start_time) >= duration_seconds:
                    logger.info(f"Duration limit reached ({duration_seconds}s)")
                    break
                
                # Get next query vector (cycle through test set)
                vector = test_vectors[query_idx % len(test_vectors)]
                
                # Apply rate limiting
                await self.rate_limit()
                
                # Execute query
                result = await self._execute_knn_query_concurrent(
                    client_manager,
                    vector,
                    k,
                    ef_runtime,
                    index_name
                )
                
                if result is not None:
                    retrieved_keys, distances, latency_ms = result
                    self.record_success(latency_ms)
                    
                    # Calculate recall if ground truth available
                    if ground_truth is not None and query_idx < len(ground_truth):
                        # Convert keys to indices
                        retrieved_indices = self._keys_to_indices(retrieved_keys)
                        gt_indices = ground_truth[query_idx % len(ground_truth)]
                        
                        recall = calculate_recall(
                            np.array(retrieved_indices),
                            np.array(gt_indices),
                            k=k
                        )
                        
                        self.recall_scores.append(recall)
                        self.total_recall += recall
                else:
                    self.record_failure("Query failed")
                
                query_idx += 1
                self.query_count += 1
                
                # Log progress
                if query_idx % 100 == 0:
                    avg_recall = self.calculate_average_recall()
                    logger.info(f"Thread {config.get('thread_id', 0)}: "
                               f"Executed {query_idx} queries, "
                               f"{self.get_operations_per_second():.2f} QPS, "
                               f"Recall@{k}: {avg_recall:.3f}")
            
        finally:
            await client_manager.cleanup()
        
        # Create final result
        result = self.create_result({
            "queries_executed": self.query_count,
            "average_recall": self.calculate_average_recall(),
            "k": k,
            "ef_runtime": ef_runtime,
        })
        
        logger.info(f"Query workload completed: {result.success_count} queries, "
                   f"avg recall: {result.additional_metrics['average_recall']:.3f}")
        return result
        
    async def _execute_knn_query_concurrent(self,
                                          client_manager: ConcurrentClientManager,
                                          query_vector: np.ndarray,
                                          k: int,
                                          ef_runtime: int,
                                          index_name: str) -> Optional[Tuple[List[str], List[float], float]]:
        """Execute a single KNN query using concurrent client."""
        start_time = time.time()
        
        # Use first available client for single query
        client = client_manager.clients[0]
        
        try:
            result = await self._execute_knn_query(
                client,
                query_vector,
                k,
                ef_runtime,
                index_name
            )
            
            latency_ms = (time.time() - start_time) * 1000
            
            if result:
                keys, distances = result
                return keys, distances, latency_ms
            else:
                return None
                
        except Exception as e:
            logger.debug(f"Query execution failed: {e}")
            return None
        
    async def _execute_knn_query(self,
                               client: Any,
                               query_vector: np.ndarray,
                               k: int = 10,
                               ef_runtime: int = 200,
                               index_name: str = "vst_index") -> Optional[Tuple[List[str], List[float]]]:
        """Execute a single KNN query."""
        try:
            # Serialize query vector
            vector_bytes = self._serialize_vector(query_vector)
            
            # Build FT.SEARCH command
            # Format: FT.SEARCH index "*=>[KNN k @vector $vec EF_RUNTIME ef]" PARAMS 2 vec <blob> DIALECT 2
            search_cmd = [
                "FT.SEARCH", index_name,
                f"*=>[KNN {k} @vector $vec EF_RUNTIME {ef_runtime}]",
                "PARAMS", "2", "vec", vector_bytes,
                "RETURN", "1", "__vector_score",
                "LIMIT", "0", str(k),
                "DIALECT", "2"
            ]
            
            # Execute search
            result = await client.execute_command(*search_cmd)
            
            # Parse results
            # Result format: [total_results, key1, [score1], key2, [score2], ...]
            if not result or len(result) < 1:
                return None
            
            total_results = result[0]
            if total_results == 0:
                return [], []
            
            keys = []
            distances = []
            
            # Parse key-value pairs
            i = 1
            while i < len(result):
                if i + 1 < len(result):
                    key = result[i].decode() if isinstance(result[i], bytes) else str(result[i])
                    # Remove "vec:" prefix if present
                    if key.startswith("vec:"):
                        key = key[4:]
                    keys.append(key)
                    
                    # Extract score from field list
                    fields = result[i + 1]
                    if fields and len(fields) >= 2:
                        score_value = fields[1]
                        # Handle bytes decoding if needed
                        if isinstance(score_value, bytes):
                            score_value = score_value.decode()
                        score = float(score_value)
                        distances.append(score)
                    else:
                        distances.append(0.0)
                    
                    i += 2
                else:
                    break
            
            return keys, distances
            
        except Exception as e:
            logger.debug(f"Failed to execute KNN query: {e}")
            return None
    
    def _serialize_vector(self, vector: np.ndarray) -> bytes:
        """Serialize numpy vector to bytes for query."""
        # Ensure float32
        if vector.dtype != np.float32:
            vector = vector.astype(np.float32)
        
        # Convert to bytes (little-endian float32)
        return struct.pack(f"<{len(vector)}f", *vector)
    
    def _keys_to_indices(self, keys: List[str]) -> List[int]:
        """Convert vector keys to indices."""
        indices = []
        for key in keys:
            # Extract index from key format "train_123" or "expand_1_train_123"
            parts = key.split('_')
            if parts[0] == "expand":
                # Expanded vector: expand_n_train_idx
                if len(parts) >= 4:
                    try:
                        idx = int(parts[-1])
                        indices.append(idx)
                    except ValueError:
                        indices.append(-1)
            else:
                # Original vector: train_idx
                if len(parts) >= 2:
                    try:
                        idx = int(parts[-1])
                        indices.append(idx)
                    except ValueError:
                        indices.append(-1)
        
        return indices
        
    def calculate_average_recall(self) -> float:
        """Calculate average recall across all queries."""
        if not self.recall_scores:
            return 0.0
        return np.mean(self.recall_scores)
        
    def get_metrics(self) -> Dict[str, float]:
        """Get query-specific metrics including recall."""
        metrics = {
            "queries_executed": float(self.query_count),
            "queries_per_second": self.get_operations_per_second(),
            "average_recall": self.calculate_average_recall(),
            "success_rate": float(self._success_count / self._operation_count) if self._operation_count > 0 else 0.0,
        }
        
        # Add recall distribution if available
        if self.recall_scores:
            recall_array = np.array(self.recall_scores)
            metrics.update({
                "recall_min": float(np.min(recall_array)),
                "recall_max": float(np.max(recall_array)),
                "recall_std": float(np.std(recall_array)),
                "recall_p50": float(np.percentile(recall_array, 50)),
                "recall_p90": float(np.percentile(recall_array, 90)),
            })
        
        return metrics