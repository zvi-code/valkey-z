# src/core/metrics.py
"""Metric collection and aggregation."""

from __future__ import annotations

import time
import asyncio
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from datetime import datetime
from collections import defaultdict, deque
import numpy as np
import logging

logger = logging.getLogger(__name__)


@dataclass
class MemoryMetrics:
    """Container for memory metrics."""
    timestamp: float
    rss_mb: float
    active_mb: float
    resident_mb: float
    allocated_mb: float
    fragmentation_ratio: float
    
    def to_dict(self) -> Dict[str, float]:
        """Convert to dictionary for CSV export."""
        return {
            "timestamp": self.timestamp,
            "rss_mb": self.rss_mb,
            "active_mb": self.active_mb,
            "resident_mb": self.resident_mb,
            "allocated_mb": self.allocated_mb,
            "fragmentation_ratio": self.fragmentation_ratio,
        }


@dataclass
class PerformanceMetrics:
    """Container for performance metrics."""
    timestamp: float
    phase: str
    qps: float  # Queries per second
    ips: float  # Inserts per second
    dps: float  # Deletes per second
    p50_ms: float
    p95_ms: float
    p99_ms: float
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for CSV export."""
        return {
            "timestamp": self.timestamp,
            "phase": self.phase,
            "qps": self.qps,
            "ips": self.ips,
            "dps": self.dps,
            "p50_ms": self.p50_ms,
            "p95_ms": self.p95_ms,
            "p99_ms": self.p99_ms,
        }


@dataclass
class OperationMetrics:
    """Metrics for a specific operation type."""
    operation: str
    count: int = 0
    success_count: int = 0
    error_count: int = 0
    latencies: List[float] = field(default_factory=list)
    start_time: float = field(default_factory=time.time)
    
    def add_latency(self, latency_ms: float, success: bool = True) -> None:
        """Add a latency sample."""
        self.latencies.append(latency_ms)
        self.count += 1
        if success:
            self.success_count += 1
        else:
            self.error_count += 1
    
    def get_throughput(self) -> float:
        """Calculate operations per second."""
        elapsed = time.time() - self.start_time
        if elapsed > 0:
            return self.success_count / elapsed
        return 0.0
    
    def get_percentiles(self) -> Tuple[float, float, float]:
        """Calculate p50, p95, p99."""
        if not self.latencies:
            return 0.0, 0.0, 0.0
        
        sorted_latencies = np.sort(self.latencies)
        p50 = np.percentile(sorted_latencies, 50)
        p95 = np.percentile(sorted_latencies, 95)
        p99 = np.percentile(sorted_latencies, 99)
        
        return float(p50), float(p95), float(p99)


class MetricCollector:
    """Collects metrics from Valkey instance."""
    
    def __init__(self, valkey_client: Any, sampling_interval: float = 10.0):
        """Initialize metric collector."""
        self.valkey_client = valkey_client
        self.sampling_interval = sampling_interval
        self._running = False
        self._collection_task: Optional[asyncio.Task] = None
        
        # Storage
        self.memory_metrics: List[MemoryMetrics] = []
        self.operation_metrics: Dict[str, OperationMetrics] = {}
        self._metric_callbacks: List[Any] = []
        
    async def start_collection(self) -> None:
        """Start async metric collection loop."""
        if self._running:
            logger.warning("Metric collection already running")
            return
            
        self._running = True
        self._collection_task = asyncio.create_task(self._collection_loop())
        logger.info(f"Started metric collection (interval={self.sampling_interval}s)")
        
    async def stop_collection(self) -> None:
        """Stop metric collection."""
        self._running = False
        
        if self._collection_task:
            self._collection_task.cancel()
            try:
                await self._collection_task
            except asyncio.CancelledError:
                pass
            self._collection_task = None
            
        logger.info("Stopped metric collection")
        
    async def _collection_loop(self) -> None:
        """Main collection loop."""
        while self._running:
            try:
                # Collect memory metrics
                metrics = await self.collect_memory_metrics()
                self.memory_metrics.append(metrics)
                
                # Call registered callbacks
                for callback in self._metric_callbacks:
                    await callback(metrics)
                
                # Sleep until next collection
                await asyncio.sleep(self.sampling_interval)
                
            except Exception as e:
                logger.error(f"Error in metric collection: {e}")
                await asyncio.sleep(self.sampling_interval)
        
    async def collect_memory_metrics(self) -> MemoryMetrics:
        """Collect current memory metrics."""
        try:
            # Execute INFO MEMORY command
            info = await self.valkey_client.info("memory")
            
            # Convert bytes to MB
            bytes_to_mb = 1024 * 1024
            
            # Extract metrics
            used_memory = info.get("used_memory", 0) / bytes_to_mb
            used_memory_rss = info.get("used_memory_rss", 0) / bytes_to_mb
            
            # Some metrics might not be available in all Valkey versions
            active_defrag_running = info.get("active_defrag_running", 0)
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
        
    def record_operation_latency(self, operation: str, latency_ms: float, success: bool = True) -> None:
        """Record latency for an operation."""
        if operation not in self.operation_metrics:
            self.operation_metrics[operation] = OperationMetrics(operation=operation)
        
        self.operation_metrics[operation].add_latency(latency_ms, success)
    
    def get_operation_metrics(self, operation: str) -> Optional[OperationMetrics]:
        """Get metrics for a specific operation."""
        return self.operation_metrics.get(operation)
    
    def register_callback(self, callback: Any) -> None:
        """Register a callback to be called with new memory metrics."""
        self._metric_callbacks.append(callback)
    
    def get_memory_summary(self) -> Dict[str, Any]:
        """Get summary of memory metrics."""
        if not self.memory_metrics:
            return {
                "samples": 0,
                "duration_seconds": 0,
            }
        
        # Extract time series
        timestamps = [m.timestamp for m in self.memory_metrics]
        rss_values = [m.rss_mb for m in self.memory_metrics]
        
        # Calculate statistics
        return {
            "samples": len(self.memory_metrics),
            "duration_seconds": timestamps[-1] - timestamps[0] if len(timestamps) > 1 else 0,
            "rss_mb": {
                "min": min(rss_values),
                "max": max(rss_values),
                "avg": np.mean(rss_values),
                "current": rss_values[-1],
            },
            "fragmentation": {
                "min": min(m.fragmentation_ratio for m in self.memory_metrics),
                "max": max(m.fragmentation_ratio for m in self.memory_metrics),
                "avg": np.mean([m.fragmentation_ratio for m in self.memory_metrics]),
            }
        }


class MetricAggregator:
    """Aggregates and calculates statistics from collected metrics."""
    
    def __init__(self, window_size: int = 1000):
        """Initialize aggregator."""
        self.window_size = window_size
        self.latency_windows: Dict[str, deque] = defaultdict(lambda: deque(maxlen=window_size))
        self.memory_samples: deque = deque(maxlen=window_size)
        self.performance_history: List[PerformanceMetrics] = []
        
    def add_latency_sample(self, 
                         operation: str,
                         latency_ms: float) -> None:
        """Add a latency sample for an operation."""
        self.latency_windows[operation].append(latency_ms)
        
    def add_memory_sample(self, sample: MemoryMetrics) -> None:
        """Add a memory metric sample."""
        self.memory_samples.append(sample)
        
    def calculate_percentiles(self, 
                            operation: str) -> Tuple[float, float, float]:
        """Calculate p50, p95, p99 for an operation."""
        samples = list(self.latency_windows.get(operation, []))
        
        if not samples:
            return 0.0, 0.0, 0.0
        
        sorted_samples = np.sort(samples)
        p50 = float(np.percentile(sorted_samples, 50))
        p95 = float(np.percentile(sorted_samples, 95))
        p99 = float(np.percentile(sorted_samples, 99))
        
        return p50, p95, p99
        
    def get_operation_stats(self, operation: str) -> Dict[str, float]:
        """Get comprehensive stats for an operation."""
        samples = list(self.latency_windows.get(operation, []))
        
        if not samples:
            return {
                "count": 0,
                "mean": 0.0,
                "std": 0.0,
                "min": 0.0,
                "max": 0.0,
                "p50": 0.0,
                "p95": 0.0,
                "p99": 0.0,
            }
        
        p50, p95, p99 = self.calculate_percentiles(operation)
        
        return {
            "count": len(samples),
            "mean": float(np.mean(samples)),
            "std": float(np.std(samples)),
            "min": float(np.min(samples)),
            "max": float(np.max(samples)),
            "p50": p50,
            "p95": p95,
            "p99": p99,
        }
        
    def get_memory_trend(self) -> Dict[str, List[float]]:
        """Get memory usage trend over time."""
        if not self.memory_samples:
            return {}
        
        samples = list(self.memory_samples)
        
        return {
            "timestamps": [s.timestamp for s in samples],
            "rss_mb": [s.rss_mb for s in samples],
            "active_mb": [s.active_mb for s in samples],
            "resident_mb": [s.resident_mb for s in samples],
            "allocated_mb": [s.allocated_mb for s in samples],
            "fragmentation_ratio": [s.fragmentation_ratio for s in samples],
        }
    
    def create_performance_snapshot(self, 
                                  phase: str,
                                  operation_metrics: Dict[str, OperationMetrics]) -> PerformanceMetrics:
        """Create a performance metrics snapshot."""
        # Calculate throughput for each operation type
        qps = operation_metrics.get("query", OperationMetrics("query")).get_throughput()
        ips = operation_metrics.get("insert", OperationMetrics("insert")).get_throughput()
        dps = operation_metrics.get("delete", OperationMetrics("delete")).get_throughput()
        
        # Get percentiles (combine all operations for overall latency)
        all_latencies = []
        for metrics in operation_metrics.values():
            all_latencies.extend(metrics.latencies)
        
        if all_latencies:
            sorted_latencies = np.sort(all_latencies)
            p50 = float(np.percentile(sorted_latencies, 50))
            p95 = float(np.percentile(sorted_latencies, 95))
            p99 = float(np.percentile(sorted_latencies, 99))
        else:
            p50 = p95 = p99 = 0.0
        
        snapshot = PerformanceMetrics(
            timestamp=time.time(),
            phase=phase,
            qps=qps,
            ips=ips,
            dps=dps,
            p50_ms=p50,
            p95_ms=p95,
            p99_ms=p99
        )
        
        self.performance_history.append(snapshot)
        return snapshot
    
    def get_summary_statistics(self) -> Dict[str, Any]:
        """Get overall summary statistics."""
        summary = {
            "operations": {},
            "memory": {},
            "performance_phases": [],
        }
        
        # Operation statistics
        for operation in self.latency_windows:
            summary["operations"][operation] = self.get_operation_stats(operation)
        
        # Memory statistics
        if self.memory_samples:
            memory_values = {
                "rss_mb": [s.rss_mb for s in self.memory_samples],
                "fragmentation": [s.fragmentation_ratio for s in self.memory_samples],
            }
            
            for metric, values in memory_values.items():
                summary["memory"][metric] = {
                    "min": float(np.min(values)),
                    "max": float(np.max(values)),
                    "avg": float(np.mean(values)),
                    "std": float(np.std(values)),
                }
        
        # Performance phases
        for perf in self.performance_history:
            summary["performance_phases"].append(perf.to_dict())
        
        return summary