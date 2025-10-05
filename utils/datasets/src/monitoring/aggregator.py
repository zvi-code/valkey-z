# src/monitoring/aggregator.py
"""Metric aggregation and statistics calculation."""

from __future__ import annotations

import numpy as np
from typing import Dict, List, Any, Tuple, Optional
from collections import defaultdict, deque
from dataclasses import dataclass, field
import time
import logging

from core import MemoryMetrics, PerformanceMetrics

logger = logging.getLogger(__name__)


@dataclass
class AggregatedStats:
    """Container for aggregated statistics."""
    count: int
    mean: float
    std: float
    min: float
    max: float
    p50: float
    p90: float
    p95: float
    p99: float
    
    def to_dict(self) -> Dict[str, float]:
        """Convert to dictionary."""
        return {
            "count": self.count,
            "mean": self.mean,
            "std": self.std,
            "min": self.min,
            "max": self.max,
            "p50": self.p50,
            "p90": self.p90,
            "p95": self.p95,
            "p99": self.p99,
        }


@dataclass
class TimeSeriesStats:
    """Statistics for time series data."""
    timestamps: List[float] = field(default_factory=list)
    values: List[float] = field(default_factory=list)
    
    def add_point(self, timestamp: float, value: float) -> None:
        """Add a data point."""
        self.timestamps.append(timestamp)
        self.values.append(value)
    
    def get_rate_of_change(self) -> Optional[float]:
        """Calculate rate of change (per second)."""
        if len(self.values) < 2:
            return None
        
        time_diff = self.timestamps[-1] - self.timestamps[0]
        if time_diff <= 0:
            return None
            
        value_diff = self.values[-1] - self.values[0]
        return value_diff / time_diff


class MetricAggregator:
    """Aggregates metrics and calculates statistics."""
    
    def __init__(self, window_size: int = 1000):
        """
        Initialize aggregator.
        
        Args:
            window_size: Size of sliding window for latency samples
        """
        self.window_size = window_size
        
        # Latency windows by operation type
        self.latency_windows: Dict[str, deque] = defaultdict(
            lambda: deque(maxlen=window_size)
        )
        
        # Memory samples
        self.memory_samples: deque = deque(maxlen=window_size)
        
        # Performance history
        self.performance_history: List[PerformanceMetrics] = []
        
        # Time series tracking
        self.time_series: Dict[str, TimeSeriesStats] = {
            "rss_mb": TimeSeriesStats(),
            "active_mb": TimeSeriesStats(),
            "fragmentation_ratio": TimeSeriesStats(),
            "qps": TimeSeriesStats(),
            "ips": TimeSeriesStats(),
            "dps": TimeSeriesStats(),
        }
        
        # Operation counts for throughput calculation
        self.operation_counts: Dict[str, int] = defaultdict(int)
        self.operation_start_times: Dict[str, float] = defaultdict(time.time)
        
        logger.info(f"Initialized metric aggregator with window size {window_size}")
        
    def add_latency_sample(self, 
                         operation: str,
                         latency_ms: float) -> None:
        """Add a latency sample for an operation."""
        self.latency_windows[operation].append(latency_ms)
        self.operation_counts[operation] += 1
        
        # Initialize start time if not set
        if operation not in self.operation_start_times:
            self.operation_start_times[operation] = time.time()
            
    def add_memory_sample(self, sample: MemoryMetrics) -> None:
        """Add a memory metric sample."""
        self.memory_samples.append(sample)
        
        # Update time series
        self.time_series["rss_mb"].add_point(sample.timestamp, sample.rss_mb)
        self.time_series["active_mb"].add_point(sample.timestamp, sample.active_mb)
        self.time_series["fragmentation_ratio"].add_point(
            sample.timestamp, sample.fragmentation_ratio
        )
        
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
    
    def calculate_all_percentiles(self, 
                                 operation: str) -> Dict[str, float]:
        """Calculate comprehensive percentiles for an operation."""
        samples = list(self.latency_windows.get(operation, []))
        
        if not samples:
            return {
                "p50": 0.0, "p90": 0.0, "p95": 0.0, 
                "p99": 0.0, "p99.9": 0.0
            }
        
        sorted_samples = np.sort(samples)
        
        return {
            "p50": float(np.percentile(sorted_samples, 50)),
            "p90": float(np.percentile(sorted_samples, 90)),
            "p95": float(np.percentile(sorted_samples, 95)),
            "p99": float(np.percentile(sorted_samples, 99)),
            "p99.9": float(np.percentile(sorted_samples, 99.9)),
        }
        
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
                "p90": 0.0,
                "p95": 0.0,
                "p99": 0.0,
                "throughput": 0.0,
            }
        
        # Calculate basic stats
        percentiles = self.calculate_all_percentiles(operation)
        
        # Calculate throughput
        elapsed = time.time() - self.operation_start_times.get(operation, time.time())
        throughput = self.operation_counts[operation] / elapsed if elapsed > 0 else 0.0
        
        return {
            "count": len(samples),
            "mean": float(np.mean(samples)),
            "std": float(np.std(samples)),
            "min": float(np.min(samples)),
            "max": float(np.max(samples)),
            **percentiles,
            "throughput": throughput,
        }
    
    def get_aggregated_stats(self, values: List[float]) -> AggregatedStats:
        """Calculate aggregated statistics for a list of values."""
        if not values:
            return AggregatedStats(
                count=0, mean=0.0, std=0.0, min=0.0, max=0.0,
                p50=0.0, p90=0.0, p95=0.0, p99=0.0
            )
        
        sorted_values = np.sort(values)
        
        return AggregatedStats(
            count=len(values),
            mean=float(np.mean(values)),
            std=float(np.std(values)),
            min=float(np.min(values)),
            max=float(np.max(values)),
            p50=float(np.percentile(sorted_values, 50)),
            p90=float(np.percentile(sorted_values, 90)),
            p95=float(np.percentile(sorted_values, 95)),
            p99=float(np.percentile(sorted_values, 99)),
        )
        
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
    
    def get_memory_growth_rate(self) -> Dict[str, Optional[float]]:
        """Calculate memory growth rates (MB/second)."""
        return {
            "rss_mb_per_sec": self.time_series["rss_mb"].get_rate_of_change(),
            "active_mb_per_sec": self.time_series["active_mb"].get_rate_of_change(),
        }
    
    def add_performance_snapshot(self, snapshot: PerformanceMetrics) -> None:
        """Add a performance snapshot."""
        self.performance_history.append(snapshot)
        
        # Update time series
        self.time_series["qps"].add_point(snapshot.timestamp, snapshot.qps)
        self.time_series["ips"].add_point(snapshot.timestamp, snapshot.ips)
        self.time_series["dps"].add_point(snapshot.timestamp, snapshot.dps)
    
    def get_performance_trend(self) -> Dict[str, List[float]]:
        """Get performance metrics trend over time."""
        if not self.performance_history:
            return {}
        
        return {
            "timestamps": [p.timestamp for p in self.performance_history],
            "phases": [p.phase for p in self.performance_history],
            "qps": [p.qps for p in self.performance_history],
            "ips": [p.ips for p in self.performance_history],
            "dps": [p.dps for p in self.performance_history],
            "p50_ms": [p.p50_ms for p in self.performance_history],
            "p95_ms": [p.p95_ms for p in self.performance_history],
            "p99_ms": [p.p99_ms for p in self.performance_history],
        }
    
    def get_summary_statistics(self) -> Dict[str, Any]:
        """Get overall summary statistics."""
        summary = {
            "operations": {},
            "memory": {},
            "performance": {},
            "growth_rates": {},
        }
        
        # Operation statistics
        for operation in self.latency_windows:
            summary["operations"][operation] = self.get_operation_stats(operation)
        
        # Memory statistics
        if self.memory_samples:
            memory_values = {
                "rss_mb": [s.rss_mb for s in self.memory_samples],
                "active_mb": [s.active_mb for s in self.memory_samples],
                "fragmentation": [s.fragmentation_ratio for s in self.memory_samples],
            }
            
            for metric, values in memory_values.items():
                summary["memory"][metric] = self.get_aggregated_stats(values).to_dict()
        
        # Performance statistics
        if self.performance_history:
            perf_values = {
                "qps": [p.qps for p in self.performance_history],
                "ips": [p.ips for p in self.performance_history],
                "dps": [p.dps for p in self.performance_history],
                "p50_ms": [p.p50_ms for p in self.performance_history],
                "p95_ms": [p.p95_ms for p in self.performance_history],
                "p99_ms": [p.p99_ms for p in self.performance_history],
            }
            
            for metric, values in perf_values.items():
                if values:  # Skip empty lists
                    summary["performance"][metric] = self.get_aggregated_stats(values).to_dict()
        
        # Growth rates
        summary["growth_rates"] = self.get_memory_growth_rate()
        
        return summary
    
    def reset(self) -> None:
        """Reset all aggregated data."""
        self.latency_windows.clear()
        self.memory_samples.clear()
        self.performance_history.clear()
        self.operation_counts.clear()
        self.operation_start_times.clear()
        
        # Reset time series
        for ts in self.time_series.values():
            ts.timestamps.clear()
            ts.values.clear()
            
        logger.info("Reset all aggregated metrics")
    
    def merge(self, other: 'MetricAggregator') -> None:
        """Merge data from another aggregator."""
        # Merge latency windows
        for operation, samples in other.latency_windows.items():
            self.latency_windows[operation].extend(samples)
        
        # Merge memory samples
        self.memory_samples.extend(other.memory_samples)
        
        # Merge performance history
        self.performance_history.extend(other.performance_history)
        
        # Merge operation counts
        for operation, count in other.operation_counts.items():
            self.operation_counts[operation] += count
            
        logger.info("Merged data from another aggregator")