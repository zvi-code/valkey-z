"""Scenario execution orchestrator."""

from __future__ import annotations

import asyncio
from typing import Dict, Any, List, Optional
import logging
import time
from pathlib import Path

from .models import Scenario, ScenarioStep, StepType
from core import Dataset
from workload import WorkloadExecutor, WorkloadRegistry, WorkloadResult
from monitoring import AsyncMemoryCollector, MetricAggregator, CSVExporter

logger = logging.getLogger(__name__)


class ScenarioExecutionError(Exception):
    """Exception raised during scenario execution."""
    pass


class ScenarioRunner:
    """Executes scenarios by orchestrating workloads."""
    
    def __init__(self, 
                connection_manager: Any,
                dataset_manager: Any,
                metric_collector: Optional[AsyncMemoryCollector] = None,
                output_dir: Optional[Path] = None):
        """Initialize scenario runner."""
        self.connection_manager = connection_manager
        self.dataset_manager = dataset_manager
        self.metric_collector = metric_collector
        self.output_dir = output_dir or Path("./output")
        
        # Create output directory
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Execution state
        self.current_scenario: Optional[Scenario] = None
        self.step_results: List[Dict[str, Any]] = []
        self.scenario_start_time: Optional[float] = None
        self.metric_aggregator = MetricAggregator()
        
        logger.info(f"Initialized scenario runner with output dir: {self.output_dir}")
        
    async def run_scenario(self, scenario: Scenario) -> Dict[str, Any]:
        """Execute a complete scenario."""
        logger.info(f"Starting execution of scenario: {scenario.name}")
        
        self.current_scenario = scenario
        self.step_results = []
        self.scenario_start_time = time.time()
        self.metric_aggregator.reset()
        
        # Setup metric collection
        csv_exporter = None
        if self.output_dir:
            csv_path = self.output_dir / f"{scenario.name}_{int(time.time())}_metrics.csv"
            csv_exporter = CSVExporter(csv_path)
            csv_exporter.open()
            
        try:
            # Start memory collection if available
            if self.metric_collector:
                await self.metric_collector.start()
                
            # Execute each step
            for i, step in enumerate(scenario.steps):
                logger.info(f"Executing step {i+1}/{len(scenario.steps)}: {step.name}")
                
                step_start_time = time.time()
                
                try:
                    step_result = await self._execute_step(step, scenario.global_config)
                    step_result["step_index"] = i
                    step_result["step_name"] = step.name
                    step_result["execution_time"] = time.time() - step_start_time
                    step_result["status"] = "success"
                    
                    self.step_results.append(step_result)
                    
                    # Record metrics
                    if csv_exporter and self.metric_collector:
                        try:
                            latest_memory = self.metric_collector.get_latest_metric()
                            if latest_memory:
                                csv_exporter.write_memory_metrics(latest_memory, f"step_{i+1}_{step.name}")
                        except Exception as e:
                            logger.warning(f"Failed to record metrics for step {step.name}: {e}")
                            
                    logger.info(f"Step {step.name} completed successfully")
                    
                except Exception as e:
                    logger.error(f"Step {step.name} failed: {e}")
                    
                    step_result = {
                        "step_index": i,
                        "step_name": step.name,
                        "execution_time": time.time() - step_start_time,
                        "status": "failed",
                        "error": str(e)
                    }
                    self.step_results.append(step_result)
                    
                    # Decide whether to continue or abort
                    if step.type == StepType.WORKLOAD:
                        # For now, continue execution even if a step fails
                        logger.warning(f"Continuing scenario execution after step failure: {step.name}")
                    else:
                        # Could make this configurable
                        logger.warning(f"Non-critical step failed, continuing: {step.name}")
                        
            # Generate final results
            total_time = time.time() - self.scenario_start_time
            results = await self._generate_results(scenario, total_time)
            
            # Export summary
            if csv_exporter:
                csv_exporter.export_summary(results)
                
            logger.info(f"Scenario {scenario.name} completed in {total_time:.2f} seconds")
            return results
            
        finally:
            # Cleanup
            if self.metric_collector:
                await self.metric_collector.stop()
                
            if csv_exporter:
                csv_exporter.close()
                
    async def _execute_step(self, step: ScenarioStep, global_config: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a single scenario step."""
        if step.type == StepType.WORKLOAD:
            return await self._execute_workload_step(step, global_config)
        elif step.type == StepType.WAIT:
            return await self._execute_wait_step(step)
        elif step.type == StepType.CHECKPOINT:
            return await self._execute_checkpoint_step(step)
        else:
            raise ScenarioExecutionError(f"Unknown step type: {step.type}")
            
    async def _execute_workload_step(self, step: ScenarioStep, global_config: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a workload step."""
        if not step.workload:
            raise ScenarioExecutionError(f"Workload step {step.name} missing workload specification")
            
        # Get workload instance
        try:
            workload = WorkloadRegistry.create_instance(step.workload)
        except KeyError as e:
            raise ScenarioExecutionError(f"Unknown workload '{step.workload}': {e}")
            
        # Merge global config with step parameters
        workload_config = global_config.copy()
        workload_config.update(step.parameters)
        
        # Create workload executor
        n_threads = workload_config.get("n_threads", 4)
        n_clients = workload_config.get("n_clients", 100)
        n_clients_per_thread = max(1, n_clients // n_threads)
        
        executor = WorkloadExecutor(n_threads=n_threads, n_clients_per_thread=n_clients_per_thread)
        
        # Get dataset if needed
        dataset = None
        if step.workload in ["ingest", "query"]:
            dataset_name = workload_config.get("dataset", self.current_scenario.dataset if self.current_scenario else "default")
            dataset = self.dataset_manager.get_dataset(dataset_name)
            
        try:
            # Execute workload
            result = await executor.execute_workload(
                workload=workload,
                connection_manager=self.connection_manager,
                dataset=dataset,
                config=workload_config,
                duration_seconds=step.duration_seconds,
                target_operations=workload_config.get("target_operations")
            )
            
            # Record metrics in aggregator
            if hasattr(workload, '_latencies'):
                for latency in workload._latencies:
                    self.metric_aggregator.add_latency_sample(step.workload, latency)
                    
            return {
                "type": "workload",
                "workload": step.workload,
                "success_count": result.success_count,
                "failure_count": result.failure_count,
                "operations_per_second": result.operations_per_second,
                "latency_percentiles": result.latency_percentiles,
                "additional_metrics": result.additional_metrics
            }
            
        finally:
            await executor.shutdown()
            
    async def _execute_wait_step(self, step: ScenarioStep) -> Dict[str, Any]:
        """Execute a wait step."""
        if step.duration_seconds:
            logger.info(f"Waiting for {step.duration_seconds} seconds")
            await asyncio.sleep(step.duration_seconds)
            return {
                "type": "wait",
                "wait_type": "duration",
                "duration_seconds": step.duration_seconds
            }
        elif step.wait_condition:
            wait_result = await self._wait_for_condition(step.wait_condition)
            return {
                "type": "wait",
                "wait_type": "condition",
                "condition": step.wait_condition,
                "result": wait_result
            }
        else:
            raise ScenarioExecutionError(f"Wait step {step.name} has no duration or condition specified")
            
    async def _execute_checkpoint_step(self, step: ScenarioStep) -> Dict[str, Any]:
        """Execute a checkpoint step."""
        logger.info(f"Executing checkpoint: {step.name}")
        
        checkpoint_data = {
            "type": "checkpoint",
            "timestamp": time.time()
        }
        
        # Collect current metrics if requested
        if step.parameters.get("collect_full_metrics", False):
            if self.metric_collector:
                memory_summary = self.metric_collector.get_memory_summary()
                checkpoint_data["memory_summary"] = memory_summary
                
            # Get aggregated metrics
            aggregated_stats = self.metric_aggregator.get_summary_statistics()
            checkpoint_data["performance_summary"] = aggregated_stats
            
        # Custom checkpoint actions
        checkpoint_actions = step.parameters.get("actions", [])
        action_results = []
        
        for action in checkpoint_actions:
            if action == "collect_memory_info":
                if self.metric_collector:
                    memory_metric = await self.metric_collector.collect_once()
                    action_results.append({
                        "action": "collect_memory_info",
                        "result": memory_metric.__dict__
                    })
            # Add more checkpoint actions as needed
            
        if action_results:
            checkpoint_data["action_results"] = action_results
            
        return checkpoint_data
        
    async def _wait_for_condition(self, condition: Dict[str, Any]) -> Dict[str, Any]:
        """Wait for a condition before proceeding."""
        condition_type = condition.get("type")
        
        if condition_type == "duration":
            duration = condition.get("seconds", 60)
            logger.info(f"Waiting for condition: duration {duration}s")
            await asyncio.sleep(duration)
            return {"waited_seconds": duration}
            
        elif condition_type == "memory_stable":
            # Wait for memory to stabilize
            threshold_mb = condition.get("threshold_mb", 100)
            window_seconds = condition.get("window_seconds", 30)
            max_wait_seconds = condition.get("max_wait_seconds", 300)
            
            logger.info(f"Waiting for memory to stabilize (threshold: {threshold_mb}MB, window: {window_seconds}s)")
            
            if not self.metric_collector:
                logger.warning("No metric collector available for memory stability check")
                await asyncio.sleep(window_seconds)
                return {"result": "no_collector_available"}
                
            start_time = time.time()
            stable_start = None
            
            while time.time() - start_time < max_wait_seconds:
                await asyncio.sleep(5)  # Check every 5 seconds
                
                # Get recent memory metrics
                recent_metrics = self.metric_collector.get_collected_metrics()
                if len(recent_metrics) < 2:
                    continue
                    
                # Check if memory is stable in the window
                window_start = time.time() - window_seconds
                window_metrics = [m for m in recent_metrics if m.timestamp >= window_start]
                
                if len(window_metrics) >= 2:
                    rss_values = [m.rss_mb for m in window_metrics]
                    memory_variation = max(rss_values) - min(rss_values)
                    
                    if memory_variation <= threshold_mb:
                        if stable_start is None:
                            stable_start = time.time()
                        elif time.time() - stable_start >= window_seconds:
                            # Memory has been stable for the required window
                            total_wait = time.time() - start_time
                            logger.info(f"Memory stabilized after {total_wait:.1f} seconds")
                            return {
                                "result": "stable",
                                "wait_time_seconds": total_wait,
                                "final_variation_mb": memory_variation
                            }
                    else:
                        stable_start = None
                        
            # Timeout
            total_wait = time.time() - start_time
            logger.warning(f"Memory stability timeout after {total_wait:.1f} seconds")
            return {
                "result": "timeout",
                "wait_time_seconds": total_wait
            }
            
        else:
            raise ScenarioExecutionError(f"Unknown wait condition type: {condition_type}")
            
    async def _generate_results(self, scenario: Scenario, total_time: float) -> Dict[str, Any]:
        """Generate final scenario results."""
        results = {
            "scenario_name": scenario.name,
            "scenario_description": scenario.description,
            "total_execution_time": total_time,
            "steps_executed": len(self.step_results),
            "steps_successful": len([r for r in self.step_results if r.get("status") == "success"]),
            "steps_failed": len([r for r in self.step_results if r.get("status") == "failed"]),
            "step_results": self.step_results,
            "timestamp": time.time()
        }
        
        # Add aggregated metrics
        if self.metric_aggregator:
            aggregated_stats = self.metric_aggregator.get_summary_statistics()
            results["aggregated_metrics"] = aggregated_stats
            
        # Add memory collection summary
        if self.metric_collector:
            memory_summary = self.metric_collector.get_memory_summary()
            results["memory_collection_summary"] = memory_summary
            
        return results
        
    def generate_report(self, results: Dict[str, Any]) -> str:
        """Generate a summary report of scenario execution."""
        lines = []
        lines.append("=" * 80)
        lines.append(f"SCENARIO EXECUTION REPORT: {results['scenario_name']}")
        lines.append("=" * 80)
        lines.append("")
        
        lines.append(f"Description: {results['scenario_description']}")
        lines.append(f"Total Execution Time: {results['total_execution_time']:.2f} seconds")
        lines.append(f"Steps Executed: {results['steps_executed']}")
        lines.append(f"Steps Successful: {results['steps_successful']}")
        lines.append(f"Steps Failed: {results['steps_failed']}")
        lines.append("")
        
        # Step summary
        lines.append("STEP RESULTS:")
        lines.append("-" * 40)
        for step_result in results['step_results']:
            status = step_result['status'].upper()
            name = step_result['step_name']
            exec_time = step_result['execution_time']
            lines.append(f"  {status:<8} {name:<20} ({exec_time:.2f}s)")
            
            if step_result.get('type') == 'workload':
                ops = step_result.get('operations_per_second', 0)
                success = step_result.get('success_count', 0)
                lines.append(f"           → {success} operations, {ops:.1f} ops/sec")
            
            # Include error message for failed steps
            if step_result.get('status') == 'failed' and 'error' in step_result:
                lines.append(f"           → Error: {step_result['error']}")
                
        lines.append("")
        
        # Performance summary
        if 'aggregated_metrics' in results:
            metrics = results['aggregated_metrics']
            lines.append("PERFORMANCE SUMMARY:")
            lines.append("-" * 40)
            
            if 'operations' in metrics:
                for op_type, op_stats in metrics['operations'].items():
                    lines.append(f"  {op_type.upper()}:")
                    lines.append(f"    Count: {op_stats.get('count', 0)}")
                    lines.append(f"    Throughput: {op_stats.get('throughput', 0):.1f} ops/sec")
                    lines.append(f"    Latency P50/P95/P99: {op_stats.get('p50', 0):.1f}/"
                                f"{op_stats.get('p95', 0):.1f}/"
                                f"{op_stats.get('p99', 0):.1f} ms")
                    
        # Memory summary
        if 'memory_collection_summary' in results:
            memory = results['memory_collection_summary']
            lines.append("")
            lines.append("MEMORY SUMMARY:")
            lines.append("-" * 40)
            
            if 'rss_mb' in memory:
                rss = memory['rss_mb']
                lines.append(f"  RSS Memory: {rss.get('min', 0):.1f} - {rss.get('max', 0):.1f} MB "
                            f"(avg: {rss.get('avg', 0):.1f} MB)")
                lines.append(f"  Growth: {rss.get('growth', 0):.1f} MB")
                
            if 'fragmentation' in memory:
                frag = memory['fragmentation']
                lines.append(f"  Fragmentation: {frag.get('min', 0):.2f} - {frag.get('max', 0):.2f} "
                            f"(avg: {frag.get('avg', 0):.2f})")
                
        lines.append("")
        lines.append("=" * 80)
        
        return "\n".join(lines)
