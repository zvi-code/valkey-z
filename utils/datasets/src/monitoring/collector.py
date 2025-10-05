# src/monitoring/collector.py
"""Async memory metric collector."""

from __future__ import annotations

import asyncio
from typing import Dict, Any, Callable, Optional, List
import logging
from datetime import datetime
import time
from dataclasses import dataclass, field

from core import MemoryMetrics

logger = logging.getLogger(__name__)


@dataclass
class CollectorStats:
    """Statistics about the collector itself."""
    collections_total: int = 0
    collections_failed: int = 0
    last_collection_time: Optional[float] = None
    average_collection_duration: float = 0.0


class AsyncMemoryCollector:
    """Collects memory metrics asynchronously at regular intervals."""
    
    def __init__(self, 
                valkey_client: Any,
                interval_seconds: float = 10.0,
                callback: Optional[Callable] = None,
                max_history: int = 10000):
        """
        Initialize collector.
        
        Args:
            valkey_client: Valkey client for executing INFO MEMORY
            interval_seconds: Collection interval
            callback: Optional async callback function
            max_history: Maximum number of samples to keep in history
        """
        self.valkey_client = valkey_client
        self.interval_seconds = interval_seconds
        self.callback = callback
        self.max_history = max_history
        
        self._task: Optional[asyncio.Task] = None
        self._running = False
        self._metric_history: List[MemoryMetrics] = []
        self._callbacks: List[Callable] = []
        self._stats = CollectorStats()
        
        if callback:
            self._callbacks.append(callback)
            
        logger.info(f"Initialized memory collector with {interval_seconds}s interval")
        
    async def start(self) -> None:
        """Start the collection loop."""
        if self._running:
            logger.warning("Collector already running")
            return
            
        self._running = True
        self._task = asyncio.create_task(self._collection_loop())
        logger.info("Started memory metric collection")
        
    async def stop(self) -> None:
        """Stop the collection loop."""
        if not self._running:
            return
            
        self._running = False
        
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
            
        logger.info(f"Stopped memory metric collection (collected {self._stats.collections_total} samples)")
        
    async def _collection_loop(self) -> None:
        """Main collection loop."""
        logger.debug("Collection loop started")
        
        while self._running:
            try:
                start_time = time.time()
                
                # Collect metrics
                metrics = await self._collect_info_memory()
                
                # Update stats
                collection_duration = time.time() - start_time
                self._update_stats(collection_duration)
                
                # Store in history
                self._metric_history.append(metrics)
                if len(self._metric_history) > self.max_history:
                    self._metric_history.pop(0)
                
                # Call callbacks
                for callback in self._callbacks:
                    try:
                        if asyncio.iscoroutinefunction(callback):
                            await callback(metrics)
                        else:
                            callback(metrics)
                    except Exception as e:
                        logger.error(f"Callback error: {e}")
                
                # Sleep until next collection
                sleep_time = max(0, self.interval_seconds - collection_duration)
                await asyncio.sleep(sleep_time)
                
            except asyncio.CancelledError:
                logger.debug("Collection loop cancelled")
                break
            except Exception as e:
                logger.error(f"Error in collection loop: {e}")
                self._stats.collections_failed += 1
                await asyncio.sleep(self.interval_seconds)
                
        logger.debug("Collection loop ended")
        
    async def _collect_info_memory(self) -> MemoryMetrics:
        """Execute INFO MEMORY and parse results."""
        try:
            # Execute INFO MEMORY command
            info = await self.valkey_client.info("memory")
            
            # Convert bytes to MB
            bytes_to_mb = 1024 * 1024
            
            # Extract metrics with defaults for missing fields
            used_memory = info.get("used_memory", 0) / bytes_to_mb
            used_memory_rss = info.get("used_memory_rss", 0) / bytes_to_mb
            
            # Some fields might not be available in all Valkey versions
            allocator_allocated = info.get("allocator_allocated", used_memory * bytes_to_mb) / bytes_to_mb
            allocator_active = info.get("allocator_active", allocator_allocated * bytes_to_mb) / bytes_to_mb
            allocator_resident = info.get("allocator_resident", used_memory_rss * bytes_to_mb) / bytes_to_mb
            
            # Calculate fragmentation ratio
            if used_memory > 0:
                fragmentation_ratio = used_memory_rss / used_memory
            else:
                fragmentation_ratio = 1.0
            
            metrics = MemoryMetrics(
                timestamp=time.time(),
                rss_mb=used_memory_rss,
                active_mb=allocator_active,
                resident_mb=allocator_resident,
                allocated_mb=allocator_allocated,
                fragmentation_ratio=fragmentation_ratio
            )
            
            logger.debug(f"Collected memory metrics: RSS={metrics.rss_mb:.1f}MB, "
                        f"Fragmentation={metrics.fragmentation_ratio:.2f}")
            
            return metrics
            
        except Exception as e:
            logger.error(f"Failed to collect memory metrics: {e}")
            # Return default metrics on error
            return MemoryMetrics(
                timestamp=time.time(),
                rss_mb=0.0,
                active_mb=0.0,
                resident_mb=0.0,
                allocated_mb=0.0,
                fragmentation_ratio=1.0
            )
    
    def _update_stats(self, collection_duration: float) -> None:
        """Update collector statistics."""
        self._stats.collections_total += 1
        self._stats.last_collection_time = time.time()
        
        # Update rolling average of collection duration
        if self._stats.average_collection_duration == 0:
            self._stats.average_collection_duration = collection_duration
        else:
            # Exponential moving average
            alpha = 0.1
            self._stats.average_collection_duration = (
                alpha * collection_duration + 
                (1 - alpha) * self._stats.average_collection_duration
            )
    
    def register_callback(self, callback: Callable) -> None:
        """Register a callback to be called with new memory metrics."""
        self._callbacks.append(callback)
        logger.debug(f"Registered callback: {callback}")
        
    def unregister_callback(self, callback: Callable) -> None:
        """Unregister a callback."""
        if callback in self._callbacks:
            self._callbacks.remove(callback)
            logger.debug(f"Unregistered callback: {callback}")
    
    def get_collected_metrics(self) -> List[MemoryMetrics]:
        """Return all collected metrics."""
        return self._metric_history.copy()
    
    def get_latest_metric(self) -> Optional[MemoryMetrics]:
        """Get the most recent metric."""
        if self._metric_history:
            return self._metric_history[-1]
        return None
    
    def get_collector_stats(self) -> CollectorStats:
        """Get statistics about the collector."""
        return self._stats
    
    def clear_history(self) -> None:
        """Clear collected metric history."""
        self._metric_history.clear()
        logger.info("Cleared metric history")
    
    async def collect_once(self) -> MemoryMetrics:
        """Collect metrics once without starting the loop."""
        metrics = await self._collect_info_memory()
        self._metric_history.append(metrics)
        return metrics
    
    def get_memory_summary(self) -> Dict[str, Any]:
        """Get summary statistics of collected memory metrics."""
        if not self._metric_history:
            return {
                "samples": 0,
                "duration_seconds": 0,
                "collection_interval": self.interval_seconds,
            }
        
        import numpy as np
        
        # Extract time series
        timestamps = [m.timestamp for m in self._metric_history]
        rss_values = [m.rss_mb for m in self._metric_history]
        active_values = [m.active_mb for m in self._metric_history]
        fragmentation_values = [m.fragmentation_ratio for m in self._metric_history]
        
        duration = timestamps[-1] - timestamps[0] if len(timestamps) > 1 else 0
        
        return {
            "samples": len(self._metric_history),
            "duration_seconds": duration,
            "collection_interval": self.interval_seconds,
            "rss_mb": {
                "min": float(np.min(rss_values)),
                "max": float(np.max(rss_values)),
                "avg": float(np.mean(rss_values)),
                "std": float(np.std(rss_values)),
                "current": rss_values[-1],
                "growth": rss_values[-1] - rss_values[0] if len(rss_values) > 1 else 0,
            },
            "active_mb": {
                "min": float(np.min(active_values)),
                "max": float(np.max(active_values)),
                "avg": float(np.mean(active_values)),
                "current": active_values[-1],
            },
            "fragmentation": {
                "min": float(np.min(fragmentation_values)),
                "max": float(np.max(fragmentation_values)),
                "avg": float(np.mean(fragmentation_values)),
                "current": fragmentation_values[-1],
            },
            "collector_stats": {
                "total_collections": self._stats.collections_total,
                "failed_collections": self._stats.collections_failed,
                "success_rate": (
                    (self._stats.collections_total - self._stats.collections_failed) / 
                    self._stats.collections_total if self._stats.collections_total > 0 else 0
                ),
                "avg_collection_time_ms": self._stats.average_collection_duration * 1000,
            }
        }
    
    async def __aenter__(self):
        """Async context manager entry."""
        await self.start()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.stop()