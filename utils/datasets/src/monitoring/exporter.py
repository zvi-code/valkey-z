# src/monitoring/exporter.py
"""Metric export to CSV and Prometheus."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Dict, List, Any, Optional, Union
import logging
from datetime import datetime
import time
import aiofiles
import asyncio
from contextlib import asynccontextmanager

from prometheus_client import CollectorRegistry, Gauge, Counter, Histogram, push_to_gateway
from prometheus_client.exposition import basic_auth_handler

from core import MemoryMetrics, PerformanceMetrics

logger = logging.getLogger(__name__)


class CSVExporter:
    """Exports metrics to CSV format."""
    
    def __init__(self, 
                output_path: Path,
                buffer_size: int = 100,
                auto_flush: bool = True):
        """
        Initialize CSV exporter.
        
        Args:
            output_path: Path to output CSV file
            buffer_size: Number of rows to buffer before writing
            auto_flush: Whether to flush after each write
        """
        self.output_path = Path(output_path)
        self.buffer_size = buffer_size
        self.auto_flush = auto_flush
        
        self._file_handle = None
        self._csv_writer = None
        self._buffer: List[Dict[str, Any]] = []
        self._headers_written = False
        self._write_count = 0
        
        # Define CSV columns
        self.columns = [
            "timestamp",
            "phase",
            "rss_mb",
            "active_mb",
            "resident_mb",
            "allocated_mb",
            "fragmentation_ratio",
            "qps",
            "ips",
            "dps",
            "p50_ms",
            "p95_ms",
            "p99_ms",
        ]
        
        # Ensure output directory exists
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Initialized CSV exporter: {self.output_path}")
        
    def open(self) -> None:
        """Open CSV file and write header."""
        if self._file_handle is not None:
            logger.warning("CSV file already open")
            return
            
        try:
            self._file_handle = open(self.output_path, 'w', newline='', buffering=1)
            self._csv_writer = csv.DictWriter(
                self._file_handle,
                fieldnames=self.columns,
                extrasaction='ignore'
            )
            
            # Write header
            self._csv_writer.writeheader()
            self._headers_written = True
            
            if self.auto_flush:
                self._file_handle.flush()
                
            logger.info(f"Opened CSV file: {self.output_path}")
            
        except Exception as e:
            logger.error(f"Failed to open CSV file: {e}")
            if self._file_handle:
                self._file_handle.close()
                self._file_handle = None
            raise
            
    def write_sample(self, sample: Dict[str, Any]) -> None:
        """Write a single metric sample."""
        if self._csv_writer is None:
            self.open()
            
        # Add to buffer
        self._buffer.append(sample)
        
        # Write if buffer is full
        if len(self._buffer) >= self.buffer_size:
            self._flush_buffer()
            
    def write_memory_metrics(self, 
                           metrics: MemoryMetrics,
                           phase: str = "unknown") -> None:
        """Write memory metrics to CSV."""
        sample = {
            "timestamp": metrics.timestamp,
            "phase": phase,
            "rss_mb": metrics.rss_mb,
            "active_mb": metrics.active_mb,
            "resident_mb": metrics.resident_mb,
            "allocated_mb": metrics.allocated_mb,
            "fragmentation_ratio": metrics.fragmentation_ratio,
        }
        self.write_sample(sample)
        
    def write_performance_metrics(self, metrics: PerformanceMetrics) -> None:
        """Write performance metrics to CSV."""
        sample = {
            "timestamp": metrics.timestamp,
            "phase": metrics.phase,
            "qps": metrics.qps,
            "ips": metrics.ips,
            "dps": metrics.dps,
            "p50_ms": metrics.p50_ms,
            "p95_ms": metrics.p95_ms,
            "p99_ms": metrics.p99_ms,
        }
        self.write_sample(sample)
        
    def write_combined_metrics(self,
                             memory: Optional[MemoryMetrics] = None,
                             performance: Optional[PerformanceMetrics] = None,
                             phase: str = "unknown") -> None:
        """Write combined memory and performance metrics."""
        sample = {
            "timestamp": time.time(),
            "phase": phase,
        }
        
        if memory:
            sample.update({
                "rss_mb": memory.rss_mb,
                "active_mb": memory.active_mb,
                "resident_mb": memory.resident_mb,
                "allocated_mb": memory.allocated_mb,
                "fragmentation_ratio": memory.fragmentation_ratio,
            })
            
        if performance:
            sample.update({
                "qps": performance.qps,
                "ips": performance.ips,
                "dps": performance.dps,
                "p50_ms": performance.p50_ms,
                "p95_ms": performance.p95_ms,
                "p99_ms": performance.p99_ms,
            })
            
        self.write_sample(sample)
        
    def _flush_buffer(self) -> None:
        """Flush buffer to file."""
        if not self._buffer or self._csv_writer is None:
            return
            
        try:
            for sample in self._buffer:
                self._csv_writer.writerow(sample)
                self._write_count += 1
                
            if self.auto_flush and self._file_handle:
                self._file_handle.flush()
                
            logger.debug(f"Flushed {len(self._buffer)} rows to CSV")
            self._buffer.clear()
            
        except Exception as e:
            logger.error(f"Failed to flush CSV buffer: {e}")
            
    def close(self) -> None:
        """Close the CSV file."""
        if self._file_handle is None:
            return
            
        # Flush any remaining data
        self._flush_buffer()
        
        # Close file
        self._file_handle.close()
        self._file_handle = None
        self._csv_writer = None
        
        logger.info(f"Closed CSV file: {self.output_path} ({self._write_count} rows written)")
        
    def export_summary(self, 
                      scenario_results: Dict[str, Any],
                      output_path: Optional[Path] = None) -> None:
        """Export scenario summary to separate CSV."""
        if output_path is None:
            output_path = self.output_path.parent / f"{self.output_path.stem}_summary.csv"
            
        try:
            # Extract summary data
            summary_data = []
            
            # Memory summary
            if "memory" in scenario_results:
                for metric, stats in scenario_results["memory"].items():
                    summary_data.append({
                        "category": "memory",
                        "metric": metric,
                        **stats
                    })
                    
            # Operation summary
            if "operations" in scenario_results:
                for operation, stats in scenario_results["operations"].items():
                    summary_data.append({
                        "category": "operation",
                        "metric": operation,
                        **stats
                    })
                    
            # Write summary CSV
            if summary_data:
                fieldnames = list(summary_data[0].keys())
                
                with open(output_path, 'w', newline='') as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(summary_data)
                    
                logger.info(f"Exported summary to: {output_path}")
                
        except Exception as e:
            logger.error(f"Failed to export summary: {e}")
            
    def __enter__(self):
        """Context manager entry."""
        self.open()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()


class AsyncCSVExporter:
    """Async version of CSV exporter."""
    
    def __init__(self, output_path: Path):
        """Initialize async CSV exporter."""
        self.output_path = Path(output_path)
        self._file_handle = None
        self._lock = asyncio.Lock()
        self.columns = CSVExporter("dummy").columns  # Reuse column definitions
        
    async def open(self) -> None:
        """Open CSV file for async writing."""
        async with self._lock:
            if self._file_handle is not None:
                return
                
            self.output_path.parent.mkdir(parents=True, exist_ok=True)
            self._file_handle = await aiofiles.open(self.output_path, 'w')
            
            # Write header
            header = ','.join(self.columns) + '\n'
            await self._file_handle.write(header)
            
    async def write_sample(self, sample: Dict[str, Any]) -> None:
        """Write a sample asynchronously."""
        if self._file_handle is None:
            await self.open()
            
        async with self._lock:
            # Convert to CSV row
            row = []
            for col in self.columns:
                value = sample.get(col, '')
                if isinstance(value, float):
                    value = f"{value:.3f}"
                row.append(str(value))
                
            line = ','.join(row) + '\n'
            await self._file_handle.write(line)
            
    async def close(self) -> None:
        """Close the file."""
        async with self._lock:
            if self._file_handle:
                await self._file_handle.close()
                self._file_handle = None
                
    async def export_summary(self, 
                           scenario_results: Dict[str, Any],
                           output_path: Optional[Path] = None) -> None:
        """Export scenario summary to separate CSV."""
        if output_path is None:
            output_path = self.output_path.parent / f"{self.output_path.stem}_summary.csv"
            
        try:
            # Extract summary data
            summary_data = []
            
            # Memory summary
            if "memory" in scenario_results:
                for metric, stats in scenario_results["memory"].items():
                    summary_data.append({
                        "category": "memory",
                        "metric": metric,
                        **stats
                    })
                    
            # Operation summary
            if "operations" in scenario_results:
                for operation, stats in scenario_results["operations"].items():
                    summary_data.append({
                        "category": "operation",
                        "metric": operation,
                        **stats
                    })
                    
            # Write summary CSV
            if summary_data:
                fieldnames = list(summary_data[0].keys())
                
                async with aiofiles.open(output_path, 'w', newline='') as f:
                    # Write header
                    header = ','.join(fieldnames) + '\n'
                    await f.write(header)
                    
                    # Write rows
                    for row in summary_data:
                        values = []
                        for field in fieldnames:
                            value = row.get(field, '')
                            if isinstance(value, float):
                                value = f"{value:.3f}"
                            values.append(str(value))
                        line = ','.join(values) + '\n'
                        await f.write(line)
                    
                logger.info(f"Exported summary to: {output_path}")
                
        except Exception as e:
            logger.error(f"Failed to export summary: {e}")
        
    @asynccontextmanager
    async def __aenter__(self):
        await self.open()
        yield self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()


class PrometheusExporter:
    """Exports metrics to Prometheus pushgateway."""
    
    def __init__(self, 
                pushgateway_url: str,
                job_name: str,
                instance: Optional[str] = None,
                username: Optional[str] = None,
                password: Optional[str] = None):
        """
        Initialize Prometheus exporter.
        
        Args:
            pushgateway_url: URL of Prometheus pushgateway
            job_name: Job name for grouping metrics
            instance: Instance label (defaults to hostname)
            username: Basic auth username
            password: Basic auth password
        """
        self.pushgateway_url = pushgateway_url
        self.job_name = job_name
        self.instance = instance or "search_delete_test"
        
        # Authentication
        self.auth_handler = None
        if username and password:
            self.auth_handler = basic_auth_handler(username, password)
            
        # Create registry
        self.registry = CollectorRegistry()
        
        # Define metrics
        self._define_metrics()
        
        logger.info(f"Initialized Prometheus exporter: {pushgateway_url}")
        
    def _define_metrics(self) -> None:
        """Define Prometheus metrics."""
        # Memory metrics
        self.memory_rss = Gauge(
            'valkey_memory_rss_mb',
            'RSS memory in MB',
            registry=self.registry
        )
        self.memory_active = Gauge(
            'valkey_memory_active_mb',
            'Active memory in MB',
            registry=self.registry
        )
        self.memory_fragmentation = Gauge(
            'valkey_memory_fragmentation_ratio',
            'Memory fragmentation ratio',
            registry=self.registry
        )
        
        # Performance metrics
        self.queries_per_second = Gauge(
            'valkey_queries_per_second',
            'Queries per second',
            registry=self.registry
        )
        self.inserts_per_second = Gauge(
            'valkey_inserts_per_second',
            'Inserts per second',
            registry=self.registry
        )
        self.deletes_per_second = Gauge(
            'valkey_deletes_per_second',
            'Deletes per second',
            registry=self.registry
        )
        
        # Latency metrics
        self.latency_p50 = Gauge(
            'valkey_latency_p50_ms',
            '50th percentile latency in ms',
            registry=self.registry
        )
        self.latency_p95 = Gauge(
            'valkey_latency_p95_ms',
            '95th percentile latency in ms',
            registry=self.registry
        )
        self.latency_p99 = Gauge(
            'valkey_latency_p99_ms',
            '99th percentile latency in ms',
            registry=self.registry
        )
        
        # Counters
        self.operations_total = Counter(
            'valkey_operations_total',
            'Total operations performed',
            ['operation'],
            registry=self.registry
        )
        
        # Histograms
        self.operation_duration = Histogram(
            'valkey_operation_duration_seconds',
            'Operation duration in seconds',
            ['operation'],
            buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
            registry=self.registry
        )
        
    def update_memory_metrics(self, metrics: MemoryMetrics) -> None:
        """Update memory metrics."""
        self.memory_rss.set(metrics.rss_mb)
        self.memory_active.set(metrics.active_mb)
        self.memory_fragmentation.set(metrics.fragmentation_ratio)
        
    def update_performance_metrics(self, metrics: PerformanceMetrics) -> None:
        """Update performance metrics."""
        self.queries_per_second.set(metrics.qps)
        self.inserts_per_second.set(metrics.ips)
        self.deletes_per_second.set(metrics.dps)
        self.latency_p50.set(metrics.p50_ms)
        self.latency_p95.set(metrics.p95_ms)
        self.latency_p99.set(metrics.p99_ms)
        
    def record_operation(self, operation: str, duration_seconds: float) -> None:
        """Record an operation."""
        self.operations_total.labels(operation=operation).inc()
        self.operation_duration.labels(operation=operation).observe(duration_seconds)
        
    def push_metrics(self, metrics: Optional[Dict[str, float]] = None) -> None:
        """Push metrics to Prometheus pushgateway."""
        try:
            # Update custom metrics if provided
            if metrics:
                for name, value in metrics.items():
                    # Create gauge if it doesn't exist
                    if not hasattr(self, f"custom_{name}"):
                        gauge = Gauge(
                            f'valkey_custom_{name}',
                            f'Custom metric: {name}',
                            registry=self.registry
                        )
                        setattr(self, f"custom_{name}", gauge)
                    
                    # Set value
                    getattr(self, f"custom_{name}").set(value)
            
            # Push to gateway
            grouping_key = {'instance': self.instance}
            
            push_to_gateway(
                self.pushgateway_url,
                job=self.job_name,
                registry=self.registry,
                grouping_key=grouping_key,
                handler=self.auth_handler
            )
            
            logger.debug(f"Pushed metrics to Prometheus pushgateway")
            
        except Exception as e:
            logger.error(f"Failed to push metrics to Prometheus: {e}")
            
    def clear_metrics(self) -> None:
        """Clear all metrics from pushgateway."""
        try:
            from prometheus_client import delete_from_gateway
            
            grouping_key = {'instance': self.instance}
            
            delete_from_gateway(
                self.pushgateway_url,
                job=self.job_name,
                grouping_key=grouping_key,
                handler=self.auth_handler
            )
            
            logger.info("Cleared metrics from Prometheus pushgateway")
            
        except Exception as e:
            logger.error(f"Failed to clear metrics: {e}")
            
#     """Exports metrics to CSV format."""
    
#     def __init__(self, output_path: Path):
#         """Initialize CSV exporter."""
#         self.output_path = output_path
#         self._file_handle = None
#         self._csv_writer = None
#         # TODO: Define CSV columns
        
#     def open(self) -> None:
#         """Open CSV file and write header."""
#         # TODO: Open file for writing
#         # TODO: Create CSV writer
#         # TODO: Write header row
#         raise NotImplementedError()
        
#     def write_sample(self, sample: Dict[str, Any]) -> None:
#         """Write a single metric sample."""
#         # TODO: Format sample data
#         # TODO: Write row to CSV
#         # TODO: Flush for real-time updates
#         raise NotImplementedError()
        
#     def close(self) -> None:
#         """Close the CSV file."""
#         # TODO: Close file handle
#         raise NotImplementedError()
        
#     def export_summary(self, 
#                       scenario_results: Dict[str, Any],
#                       output_path: Optional[Path] = None) -> None:
#         """Export scenario summary to separate CSV."""
#         # TODO: Create summary CSV
#         # TODO: Write aggregated metrics
#         raise NotImplementedError()


# class PrometheusExporter:
#     """Exports metrics to Prometheus pushgateway (future implementation)."""
    
#     def __init__(self, pushgateway_url: str, job_name: str):
#         """Initialize Prometheus exporter."""
#         self.pushgateway_url = pushgateway_url
#         self.job_name = job_name
#         # TODO: Initialize Prometheus client
        
#     def push_metrics(self, metrics: Dict[str, float]) -> None:
#         """Push metrics to Prometheus pushgateway."""
#         # TODO: Convert metrics to Prometheus format
#         # TODO: Push to gateway
#         raise NotImplementedError()
