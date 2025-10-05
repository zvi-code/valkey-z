# src/workload/executor.py
"""Multi-threaded workload execution engine."""

from __future__ import annotations

import asyncio
import concurrent.futures
from typing import List, Dict, Any, Optional, Callable
import logging
import time
from dataclasses import dataclass
import multiprocessing
from threading import Thread

from .base import BaseWorkload, WorkloadResult

logger = logging.getLogger(__name__)


@dataclass
class ThreadResult:
    """Results from a single thread execution."""
    thread_id: int
    success_count: int
    failure_count: int
    latencies: List[float]
    errors: List[str]
    elapsed_time: float
    additional_metrics: Dict[str, Any]


class WorkloadExecutor:
    """Orchestrates multi-threaded workload execution."""
    
    def __init__(self, n_threads: int, n_clients_per_thread: int):
        """
        Initialize executor.
        
        Args:
            n_threads: Number of threads to use
            n_clients_per_thread: Number of Valkey clients per thread
        """
        self.n_threads = min(n_threads, multiprocessing.cpu_count())
        self.n_clients_per_thread = n_clients_per_thread
        self._executor: Optional[concurrent.futures.ThreadPoolExecutor] = None
        self._stop_event = asyncio.Event()
        
        logger.info(f"Initialized executor with {self.n_threads} threads, "
                   f"{self.n_clients_per_thread} clients per thread")
        
    async def execute_workload(self,
                             workload: BaseWorkload,
                             connection_manager: Any,
                             dataset: Any,
                             config: Dict[str, Any],
                             duration_seconds: Optional[float] = None,
                             target_operations: Optional[int] = None) -> WorkloadResult:
        """
        Execute a workload across multiple threads.
        
        Args:
            workload: Workload instance to execute
            connection_manager: Connection manager for Valkey pools
            dataset: Dataset instance
            config: Workload configuration
            duration_seconds: Max duration (None for unlimited)
            target_operations: Target number of operations (None for unlimited)
            
        Returns:
            Aggregated WorkloadResult
        """
        logger.info(f"Starting execution of {workload.name} workload")
        
        # Reset workload metrics
        workload.reset_metrics()
        workload.set_stop_event(self._stop_event)
        
        # Create thread pool
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.n_threads)
        
        # Start time
        start_time = time.time()
        
        try:
            # Create futures for each thread
            futures = []
            for thread_id in range(self.n_threads):
                # Get a connection pool for this thread
                pool_index = thread_id % connection_manager.n_pools
                pool = connection_manager.get_pool(pool_index)
                
                # Submit thread work
                future = self._executor.submit(
                    self._thread_worker,
                    thread_id=thread_id,
                    workload=workload,
                    pool=pool,
                    dataset=dataset,
                    config=config,
                    stop_event=self._stop_event,
                    target_operations_per_thread=target_operations // self.n_threads if target_operations else None
                )
                futures.append(future)
            
            # Monitor execution
            if duration_seconds:
                # Wait for duration or completion
                await asyncio.sleep(duration_seconds)
                logger.info(f"Duration limit reached ({duration_seconds}s), stopping workload")
                self._stop_event.set()
            
            # Wait for all threads to complete
            thread_results = []
            for future in concurrent.futures.as_completed(futures):
                try:
                    result = future.result()
                    thread_results.append(result)
                except Exception as e:
                    logger.error(f"Thread execution failed: {e}")
            
            # Aggregate results
            aggregated_result = self._aggregate_results(thread_results, start_time)
            
            logger.info(f"Workload execution completed: {aggregated_result.success_count} operations, "
                       f"{aggregated_result.operations_per_second:.2f} ops/sec")
            
            return aggregated_result
            
        finally:
            # Cleanup
            self._stop_event.clear()
            if self._executor:
                self._executor.shutdown(wait=True)
                self._executor = None
    
    def _thread_worker(self,
                      thread_id: int,
                      workload: BaseWorkload,
                      pool: Any,
                      dataset: Any,
                      config: Dict[str, Any],
                      stop_event: asyncio.Event,
                      target_operations_per_thread: Optional[int] = None) -> ThreadResult:
        """
        Worker function for each thread.
        
        Args:
            thread_id: Thread identifier
            workload: Workload instance
            pool: Valkey connection pool
            dataset: Dataset instance
            config: Workload configuration
            stop_event: Event to signal stop
            target_operations_per_thread: Target operations for this thread
            
        Returns:
            ThreadResult with execution metrics
        """
        # Create new event loop for this thread
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        logger.info(f"Thread {thread_id} starting")
        
        try:
            # Run the async worker
            result = loop.run_until_complete(
                self._async_thread_worker(
                    thread_id=thread_id,
                    workload=workload,
                    pool=pool,
                    dataset=dataset,
                    config=config,
                    stop_event=stop_event,
                    target_operations=target_operations_per_thread
                )
            )
            
            logger.info(f"Thread {thread_id} completed: {result.success_count} operations")
            return result
            
        except Exception as e:
            logger.error(f"Thread {thread_id} failed: {e}")
            # Return empty result on failure
            return ThreadResult(
                thread_id=thread_id,
                success_count=0,
                failure_count=0,
                latencies=[],
                errors=[str(e)],
                elapsed_time=0.0,
                additional_metrics={}
            )
        finally:
            loop.close()
    
    async def _async_thread_worker(self,
                                 thread_id: int,
                                 workload: BaseWorkload,
                                 pool: Any,
                                 dataset: Any,
                                 config: Dict[str, Any],
                                 stop_event: asyncio.Event,
                                 target_operations: Optional[int] = None) -> ThreadResult:
        """
        Async worker function for thread execution.
        
        Args:
            thread_id: Thread identifier
            workload: Workload instance
            pool: Valkey connection pool
            dataset: Dataset instance
            config: Workload configuration
            stop_event: Event to signal stop
            target_operations: Target number of operations
            
        Returns:
            ThreadResult with execution metrics
        """
        start_time = time.time()
        success_count = 0
        failure_count = 0
        latencies = []
        errors = []
        operations_completed = 0
        
        # Create thread-specific configuration
        thread_config = config.copy()
        thread_config["thread_id"] = thread_id
        thread_config["n_clients"] = self.n_clients_per_thread
        
        # Create a thread-local copy of the workload
        # Use the class directly without passing name since built-in workloads don't take name as argument
        thread_workload = workload.__class__()
        thread_workload.set_stop_event(stop_event)
        
        try:
            # Execute workload
            result = await thread_workload.execute(
                connection_pool=pool,
                dataset=dataset,
                config=thread_config
            )
            
            # Extract metrics
            success_count = result.success_count
            failure_count = result.failure_count
            latencies = thread_workload._latencies[:1000]  # Limit size
            errors = result.errors[:100]  # Limit size
            
        except Exception as e:
            logger.error(f"Thread {thread_id} workload execution failed: {e}")
            errors.append(str(e))
            failure_count += 1
        
        elapsed_time = time.time() - start_time
        
        return ThreadResult(
            thread_id=thread_id,
            success_count=success_count,
            failure_count=failure_count,
            latencies=latencies,
            errors=errors,
            elapsed_time=elapsed_time,
            additional_metrics=result.additional_metrics
        )
    
    def _aggregate_results(self, 
                         thread_results: List[ThreadResult],
                         start_time: float) -> WorkloadResult:
        """
        Aggregate results from all threads.
        
        Args:
            thread_results: List of results from each thread
            start_time: Overall start time
            
        Returns:
            Aggregated WorkloadResult
        """
        total_success = sum(r.success_count for r in thread_results)
        total_failure = sum(r.failure_count for r in thread_results)
        all_latencies = []
        all_errors = []
        
        for result in thread_results:
            all_latencies.extend(result.latencies)
            all_errors.extend(result.errors)
        
        # Calculate overall elapsed time
        elapsed_time = time.time() - start_time
        
        # Calculate operations per second
        total_operations = total_success + total_failure
        ops_per_second = total_operations / elapsed_time if elapsed_time > 0 else 0.0
        
        # Calculate percentiles
        percentiles = {"p50": 0.0, "p95": 0.0, "p99": 0.0}
        if all_latencies:
            import numpy as np
            sorted_latencies = np.sort(all_latencies)
            percentiles = {
                "p50": float(np.percentile(sorted_latencies, 50)),
                "p95": float(np.percentile(sorted_latencies, 95)),
                "p99": float(np.percentile(sorted_latencies, 99)),
            }
        
        # Merge additional metrics from all threads
        all_additional_metrics = {}
        for result in thread_results:
            for key, value in result.additional_metrics.items():
                if key in all_additional_metrics:
                    # Sum numeric values, concatenate strings
                    if isinstance(value, (int, float)) and isinstance(all_additional_metrics[key], (int, float)):
                        all_additional_metrics[key] += value
                    else:
                        all_additional_metrics[key] = value  # Keep last value for non-numeric
                else:
                    all_additional_metrics[key] = value
        
        # Additional standard metrics
        additional_metrics = {
            "threads_used": len(thread_results),
            "avg_ops_per_thread": total_operations / len(thread_results) if thread_results else 0,
        }
        
        # Merge with thread-specific metrics
        additional_metrics.update(all_additional_metrics)
        
        return WorkloadResult(
            success_count=total_success,
            failure_count=total_failure,
            total_time_seconds=elapsed_time,
            operations_per_second=ops_per_second,
            latency_percentiles=percentiles,
            errors=all_errors[:1000],  # Limit total errors
            additional_metrics=additional_metrics
        )
    
    async def shutdown(self) -> None:
        """Gracefully shutdown the executor."""
        logger.info("Shutting down workload executor")
        
        # Signal stop to all threads
        self._stop_event.set()
        
        # Shutdown thread pool
        if self._executor:
            self._executor.shutdown(wait=True, cancel_futures=True)
            self._executor = None
        
        logger.info("Workload executor shutdown complete")


class ConcurrentClientManager:
    """Manages concurrent client execution within a thread."""
    
    def __init__(self, pool: Any, n_clients: int):
        """
        Initialize concurrent client manager.
        
        Args:
            pool: Valkey connection pool
            n_clients: Number of concurrent clients
        """
        self.pool = pool
        self.n_clients = n_clients
        self.clients: List[Any] = []
        
    async def initialize(self) -> None:
        """Initialize all clients."""
        for _ in range(self.n_clients):
            client = await self.pool.get_client()
            self.clients.append(client)
        
        logger.debug(f"Initialized {self.n_clients} concurrent clients")
    
    async def execute_batch(self,
                          operations: List[Callable],
                          rate_limit: Optional[float] = None) -> List[Any]:
        """
        Execute operations across concurrent clients.
        
        Args:
            operations: List of async operations to execute
            rate_limit: Optional rate limit in operations per second
            
        Returns:
            List of operation results
        """
        if not self.clients:
            await self.initialize()
        
        # Distribute operations across clients
        tasks = []
        for i, operation in enumerate(operations):
            client_idx = i % self.n_clients
            client = self.clients[client_idx]
            
            # Apply rate limiting if specified
            if rate_limit and i > 0:
                delay = i / rate_limit
                task = self._delayed_operation(operation, client, delay)
            else:
                task = operation(client)
            
            tasks.append(task)
        
        # Execute all operations concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        return results
    
    async def _delayed_operation(self, operation: Callable, client: Any, delay: float) -> Any:
        """Execute operation after delay."""
        await asyncio.sleep(delay)
        return await operation(client)
    
    async def cleanup(self) -> None:
        """Close all clients."""
        for client in self.clients:
            try:
                await client.close()
            except Exception as e:
                logger.error(f"Error closing client: {e}")
        
        self.clients.clear()