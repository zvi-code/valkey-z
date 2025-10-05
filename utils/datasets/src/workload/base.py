# src/workload/base.py
"""Base classes for workload implementation."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field
import asyncio
import time
import logging

logger = logging.getLogger(__name__)


@dataclass
class WorkloadResult:
    """Results from workload execution."""
    success_count: int
    failure_count: int
    total_time_seconds: float
    operations_per_second: float
    latency_percentiles: Dict[str, float]
    errors: List[str] = field(default_factory=list)
    additional_metrics: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate."""
        total = self.success_count + self.failure_count
        if total == 0:
            return 0.0
        return self.success_count / total
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for reporting."""
        return {
            "success_count": self.success_count,
            "failure_count": self.failure_count,
            "total_time_seconds": self.total_time_seconds,
            "operations_per_second": self.operations_per_second,
            "success_rate": self.success_rate,
            "latency_p50": self.latency_percentiles.get("p50", 0.0),
            "latency_p95": self.latency_percentiles.get("p95", 0.0),
            "latency_p99": self.latency_percentiles.get("p99", 0.0),
            "error_count": len(self.errors),
            **self.additional_metrics
        }


class BaseWorkload(ABC):
    """Abstract base class for all workloads."""
    
    def __init__(self, name: str):
        """Initialize base workload."""
        self.name = name
        self.metrics: Dict[str, Any] = {}
        self._start_time: Optional[float] = None
        self._stop_event: Optional[asyncio.Event] = None
        self._operation_count = 0
        self._success_count = 0
        self._failure_count = 0
        self._errors: List[str] = []
        self._latencies: List[float] = []
        
    @abstractmethod
    async def execute(self, 
                     connection_pool: Any,
                     dataset: Any,
                     config: Dict[str, Any]) -> WorkloadResult:
        """
        Execute the workload.
        
        Args:
            connection_pool: Valkey connection pool
            dataset: Dataset instance
            config: Workload configuration
            
        Returns:
            WorkloadResult with execution metrics
        """
        pass
        
    @abstractmethod
    def get_metrics(self) -> Dict[str, float]:
        """
        Get workload-specific metrics.
        
        Returns:
            Dictionary of metric name to value
        """
        pass
    
    async def warm_up(self, connection_pool: Any) -> None:
        """
        Optional warm-up phase before execution.
        
        Args:
            connection_pool: Valkey connection pool
        """
        logger.info(f"Running warm-up for {self.name} workload")
        
        # Default warm-up: test connectivity
        try:
            client = await connection_pool.get_client()
            await client.ping()
            await client.close()
            logger.info("Warm-up completed successfully")
        except Exception as e:
            logger.error(f"Warm-up failed: {e}")
            raise
    
    def set_stop_event(self, stop_event: asyncio.Event) -> None:
        """Set stop event for graceful shutdown."""
        self._stop_event = stop_event
    
    def should_stop(self) -> bool:
        """Check if workload should stop."""
        if self._stop_event:
            return self._stop_event.is_set()
        return False
    
    def record_success(self, latency_ms: Optional[float] = None) -> None:
        """Record a successful operation."""
        self._success_count += 1
        self._operation_count += 1
        if latency_ms is not None:
            self._latencies.append(latency_ms)
    
    def record_failure(self, error: str, latency_ms: Optional[float] = None) -> None:
        """Record a failed operation."""
        self._failure_count += 1
        self._operation_count += 1
        self._errors.append(error)
        if latency_ms is not None:
            self._latencies.append(latency_ms)
    
    def calculate_percentiles(self) -> Dict[str, float]:
        """Calculate latency percentiles."""
        if not self._latencies:
            return {"p50": 0.0, "p95": 0.0, "p99": 0.0}
        
        import numpy as np
        sorted_latencies = np.sort(self._latencies)
        
        return {
            "p50": float(np.percentile(sorted_latencies, 50)),
            "p95": float(np.percentile(sorted_latencies, 95)),
            "p99": float(np.percentile(sorted_latencies, 99)),
        }
    
    def get_elapsed_time(self) -> float:
        """Get elapsed time since start."""
        if self._start_time is None:
            return 0.0
        return time.time() - self._start_time
    
    def get_operations_per_second(self) -> float:
        """Calculate operations per second."""
        elapsed = self.get_elapsed_time()
        if elapsed > 0:
            return self._operation_count / elapsed
        return 0.0
    
    def create_result(self, additional_metrics: Optional[Dict[str, Any]] = None) -> WorkloadResult:
        """Create workload result from current metrics."""
        elapsed_time = self.get_elapsed_time()
        
        result = WorkloadResult(
            success_count=self._success_count,
            failure_count=self._failure_count,
            total_time_seconds=elapsed_time,
            operations_per_second=self.get_operations_per_second(),
            latency_percentiles=self.calculate_percentiles(),
            errors=self._errors[:100],  # Limit error list size
            additional_metrics=additional_metrics or {}
        )
        
        return result
    
    def reset_metrics(self) -> None:
        """Reset all metrics for a new run."""
        self._start_time = time.time()
        self._operation_count = 0
        self._success_count = 0
        self._failure_count = 0
        self._errors = []
        self._latencies = []
        self.metrics = {}


class RateLimitedWorkload(BaseWorkload):
    """Base class for workloads with rate limiting."""
    
    def __init__(self, name: str, target_ops_per_second: Optional[float] = None):
        """
        Initialize rate-limited workload.
        
        Args:
            name: Workload name
            target_ops_per_second: Target operations per second (None for unlimited)
        """
        super().__init__(name)
        self.target_ops_per_second = target_ops_per_second
        self._last_operation_time = 0.0
        
    async def rate_limit(self) -> None:
        """Apply rate limiting if configured."""
        if self.target_ops_per_second is None or self.target_ops_per_second <= 0:
            return
        
        # Calculate required delay
        min_interval = 1.0 / self.target_ops_per_second
        current_time = time.time()
        time_since_last = current_time - self._last_operation_time
        
        if time_since_last < min_interval:
            delay = min_interval - time_since_last
            await asyncio.sleep(delay)
        
        self._last_operation_time = time.time()


class BatchedWorkload(BaseWorkload):
    """Base class for workloads that process data in batches."""
    
    def __init__(self, name: str, batch_size: int = 1000):
        """
        Initialize batched workload.
        
        Args:
            name: Workload name
            batch_size: Number of items to process per batch
        """
        super().__init__(name)
        self.batch_size = batch_size
        self._batches_processed = 0
        
    def record_batch_success(self, batch_size: int, latency_ms: Optional[float] = None) -> None:
        """Record successful batch processing."""
        if batch_size <= 0:
            logger.warning(f"Invalid batch size: {batch_size}. Skipping batch record.")
            return
            
        self._success_count += batch_size
        self._operation_count += batch_size
        self._batches_processed += 1
        
        if latency_ms is not None:
            # Record average latency per item
            per_item_latency = latency_ms / batch_size
            for _ in range(min(batch_size, 10)):  # Sample up to 10 items per batch
                self._latencies.append(per_item_latency)
    
    def get_metrics(self) -> Dict[str, float]:
        """Get batch-specific metrics."""
        return {
            "batches_processed": float(self._batches_processed),
            "items_per_batch": float(self.batch_size),
            "items_per_second": self.get_operations_per_second(),
        }