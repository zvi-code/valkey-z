# src/workload/shrink.py
"""Random deletion workload for index shrinking."""

from __future__ import annotations

from typing import Dict, Any, List, Optional, Set
import asyncio
import logging
import time
import random

from .base import BatchedWorkload, WorkloadResult
from .executor import ConcurrentClientManager
from core import VectorSampler

logger = logging.getLogger(__name__)


class ShrinkWorkload(BatchedWorkload):
    """Handles random deletion of vectors to shrink the index."""
    
    def __init__(self):
        """Initialize shrink workload."""
        super().__init__("shrink", batch_size=100)
        self.vectors_deleted = 0
        self.initial_count = 0
        self.final_count = 0
        
    async def execute(self, 
                     connection_pool: Any,
                     dataset: Any,
                     config: Dict[str, Any]) -> WorkloadResult:
        """Execute vector deletion workload."""
        logger.info(f"Starting shrink workload (thread {config.get('thread_id', 0)})")
        
        # Reset metrics
        self.reset_metrics()
        
        # Extract configuration
        shrink_ratio = config.get("shrink_ratio", 0.5)
        self.batch_size = config.get("batch_size", 100)
        index_name = config.get("index_name", "vst_index")
        n_clients = config.get("n_clients", 100)
        exclude_patterns = config.get("exclude_patterns", [])
        deletion_strategy = config.get("deletion_strategy", "random")  # random or stratified
        
        # Validate shrink ratio
        if not 0.0 < shrink_ratio <= 1.0:
            raise ValueError(f"Shrink ratio must be between 0.0 and 1.0, got {shrink_ratio}")
        
        # Create client manager
        client_manager = ConcurrentClientManager(connection_pool, n_clients)
        await client_manager.initialize()
        
        try:
            # Get current index size
            self.initial_count = await self._get_index_size(
                client_manager.clients[0],
                index_name
            )
            logger.info(f"Current index size: {self.initial_count} vectors")
            
            if self.initial_count == 0:
                logger.warning("Index is empty, nothing to delete")
                return self.create_result({"vectors_deleted": 0})
            
            # Calculate number of vectors to delete
            target_deletions = int(self.initial_count * shrink_ratio)
            logger.info(f"Target deletions: {target_deletions} ({shrink_ratio:.1%} of {self.initial_count})")
            
            # Get all keys
            all_keys = await self._get_all_keys(
                client_manager.clients[0],
                pattern="*"
            )
            
            if not all_keys:
                logger.warning("No keys found matching pattern")
                return self.create_result({"vectors_deleted": 0})
            
            # Apply exclusion patterns
            if exclude_patterns:
                all_keys = self._filter_keys(all_keys, exclude_patterns)
                logger.info(f"Keys after filtering: {len(all_keys)}")
            
            # Select keys for deletion
            if deletion_strategy == "stratified":
                keys_to_delete = await self._stratified_selection(
                    all_keys,
                    target_deletions,
                    client_manager.clients[0]
                )
            else:
                # Random selection
                sampler = VectorSampler(seed=config.get("seed", None))
                indices = sampler.sample_for_deletion(
                    total_vectors=len(all_keys),
                    shrink_ratio=shrink_ratio
                )
                keys_to_delete = [all_keys[i] for i in indices]
            
            logger.info(f"Selected {len(keys_to_delete)} keys for deletion")
            
            # Delete in batches
            for i in range(0, len(keys_to_delete), self.batch_size):
                if self.should_stop():
                    logger.info("Stop event received, halting deletion")
                    break
                
                batch_keys = keys_to_delete[i:i + self.batch_size]
                
                deleted = await self._delete_batch_concurrent(
                    client_manager,
                    batch_keys
                )
                
                self.vectors_deleted += deleted
                
                # Log progress
                if (i // self.batch_size + 1) % 10 == 0:
                    logger.info(f"Thread {config.get('thread_id', 0)}: "
                               f"Deleted {self.vectors_deleted}/{target_deletions} vectors, "
                               f"{self.get_operations_per_second():.2f} DPS")
            
            # Get final index size
            self.final_count = await self._get_index_size(
                client_manager.clients[0],
                index_name
            )
            
            logger.info(f"Final index size: {self.final_count} vectors "
                       f"(deleted {self.initial_count - self.final_count})")
            
        finally:
            await client_manager.cleanup()
        
        # Create final result
        result = self.create_result({
            "vectors_deleted": self.vectors_deleted,
            "initial_count": self.initial_count,
            "final_count": self.final_count,
            "target_deletions": target_deletions,
            "actual_shrink_ratio": (self.initial_count - self.final_count) / self.initial_count if self.initial_count > 0 else 0.0,
        })
        
        logger.info(f"Shrink workload completed: {result.success_count} vectors deleted")
        return result
        
    async def _get_index_size(self, client: Any, index_name: str) -> int:
        """Get current number of vectors in the index."""
        try:
            # Use FT.INFO to get index statistics
            info = await client.execute_command("FT.INFO", index_name)
            
            # Parse info response (it's a flat list of key-value pairs)
            info_dict = {}
            for i in range(0, len(info), 2):
                if i + 1 < len(info):
                    key = info[i].decode() if isinstance(info[i], bytes) else str(info[i])
                    value = info[i + 1]
                    info_dict[key] = value
            
            # Get number of documents
            num_docs = info_dict.get("num_docs", 0)
            return int(num_docs)
            
        except Exception as e:
            logger.error(f"Failed to get index size: {e}")
            return 0
        
    async def _get_all_keys(self, 
                          client: Any,
                          pattern: str = "*") -> List[str]:
        """Get all keys matching pattern using SCAN."""
        all_keys = []
        cursor = 0
        
        try:
            while True:
                # Use SCAN to iterate through keys
                result = await client.scan(cursor, match=pattern, count=1000)
                cursor, keys = result
                
                # Add keys to list
                for key in keys:
                    if isinstance(key, bytes):
                        all_keys.append(key.decode())
                    else:
                        all_keys.append(str(key))
                
                # Check if we're done
                if cursor == 0:
                    break
                
                # Log progress for large datasets
                if len(all_keys) % 10000 == 0:
                    logger.debug(f"Scanned {len(all_keys)} keys so far...")
            
            logger.info(f"Found {len(all_keys)} keys matching pattern '{pattern}'")
            return all_keys
            
        except Exception as e:
            logger.error(f"Failed to scan keys: {e}")
            return []
    
    def _filter_keys(self, keys: List[str], exclude_patterns: List[str]) -> List[str]:
        """Filter out keys matching exclusion patterns."""
        filtered_keys = []
        
        for key in keys:
            exclude = False
            for pattern in exclude_patterns:
                if pattern in key:
                    exclude = True
                    break
            
            if not exclude:
                filtered_keys.append(key)
        
        logger.info(f"Filtered {len(keys) - len(filtered_keys)} keys based on exclusion patterns")
        return filtered_keys
    
    async def _stratified_selection(self,
                                  all_keys: List[str],
                                  target_deletions: int,
                                  client: Any) -> List[str]:
        """Select keys for deletion using stratified sampling."""
        # Group keys by type (original vs expanded)
        groups = {
            "original": [],
            "expanded": {}
        }
        
        for key in all_keys:
            # Remove "vec:" prefix for analysis
            clean_key = key[4:] if key.startswith("vec:") else key
            
            if clean_key.startswith("expand_"):
                # Extract expansion index
                parts = clean_key.split("_")
                if len(parts) >= 3:
                    try:
                        exp_idx = int(parts[1])
                        if exp_idx not in groups["expanded"]:
                            groups["expanded"][exp_idx] = []
                        groups["expanded"][exp_idx].append(key)
                    except ValueError:
                        groups["original"].append(key)
            else:
                groups["original"].append(key)
        
        # Calculate proportional deletions
        total_keys = len(all_keys)
        keys_to_delete = []
        
        # Delete from original vectors
        if groups["original"]:
            original_ratio = len(groups["original"]) / total_keys
            original_deletions = int(target_deletions * original_ratio)
            
            sampler = VectorSampler()
            indices = sampler.sample_for_deletion(
                total_vectors=len(groups["original"]),
                shrink_ratio=original_deletions / len(groups["original"])
            )
            
            for idx in indices:
                keys_to_delete.append(groups["original"][idx])
        
        # Delete from each expansion group
        for exp_idx, exp_keys in groups["expanded"].items():
            exp_ratio = len(exp_keys) / total_keys
            exp_deletions = int(target_deletions * exp_ratio)
            
            if exp_deletions > 0:
                sampler = VectorSampler()
                indices = sampler.sample_for_deletion(
                    total_vectors=len(exp_keys),
                    shrink_ratio=exp_deletions / len(exp_keys)
                )
                
                for idx in indices:
                    keys_to_delete.append(exp_keys[idx])
        
        logger.info(f"Stratified selection: {len(keys_to_delete)} keys from "
                   f"{len(groups['original'])} original and "
                   f"{sum(len(v) for v in groups['expanded'].values())} expanded vectors")
        
        return keys_to_delete
        
    async def _delete_batch_concurrent(self,
                                     client_manager: ConcurrentClientManager,
                                     keys: List[str]) -> int:
        """Delete a batch of vectors using concurrent clients."""
        start_time = time.time()
        
        # Prepare delete operations
        operations = []
        for key in keys:
            async def delete_op(client, k=key):
                return await client.delete(k)
            
            operations.append(delete_op)
        
        # Execute concurrently
        results = await client_manager.execute_batch(operations)
        
        # Count successes
        success_count = 0
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                self.record_failure(f"Failed to delete {keys[i]}: {result}")
            elif result == 1:  # Valkey DELETE returns 1 for success
                success_count += 1
            else:
                self.record_failure(f"Key not found: {keys[i]}")
        
        # Record batch metrics
        elapsed_ms = (time.time() - start_time) * 1000
        self.record_batch_success(success_count, elapsed_ms)
        
        return success_count
        
    def get_metrics(self) -> Dict[str, float]:
        """Get deletion-specific metrics."""
        metrics = super().get_metrics()
        metrics.update({
            "vectors_deleted": float(self.vectors_deleted),
            "deletion_rate": self.get_operations_per_second(),
            "success_rate": float(self._success_count / self._operation_count) if self._operation_count > 0 else 0.0,
        })
        
        if self.initial_count > 0:
            metrics["shrink_percentage"] = float(self.vectors_deleted / self.initial_count) * 100
        
        return metrics