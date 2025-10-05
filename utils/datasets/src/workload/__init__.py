# src/workload/__init__.py
"""Workload execution components."""

from .base import (
    BaseWorkload, 
    WorkloadResult,
    RateLimitedWorkload,
    BatchedWorkload
)
from .executor import (
    WorkloadExecutor,
    ConcurrentClientManager,
    ThreadResult
)
from .ingest import IngestWorkload
from .query import QueryWorkload
from .shrink import ShrinkWorkload
from .registry import (
    WorkloadRegistry, 
    register_workload,
    register_builtin_workloads
)

__all__ = [
    # Base classes
    "BaseWorkload",
    "WorkloadResult",
    "RateLimitedWorkload",
    "BatchedWorkload",
    
    # Executor
    "WorkloadExecutor",
    "ConcurrentClientManager",
    "ThreadResult",
    
    # Built-in workloads
    "IngestWorkload",
    "QueryWorkload",
    "ShrinkWorkload",
    
    # Registry
    "WorkloadRegistry",
    "register_workload",
    "register_builtin_workloads",
]

# Ensure built-in workloads are registered
register_builtin_workloads()