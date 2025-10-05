"""Integration tests for scenario execution."""

import pytest
import asyncio
import tempfile
from pathlib import Path
from unittest.mock import Mock, AsyncMock, patch

from scenarios import ScenarioLoader, ScenarioRunner
from scenarios.models import Scenario, ScenarioStep, StepType


@pytest.mark.integration
class TestScenarioExecution:
    """Test end-to-end scenario execution."""
    
    @pytest.mark.asyncio
    async def test_simple_scenario_execution(self):
        """Test execution of a simple scenario with mocked components."""
        # Create a simple test scenario
        simple_scenario = Scenario(
            name="test_integration",
            description="Integration test scenario",
            dataset="test-dataset",
            steps=[
                ScenarioStep(
                    name="wait_step",
                    type=StepType.WAIT,
                    duration_seconds=0.1  # Very short for testing
                ),
                ScenarioStep(
                    name="checkpoint",
                    type=StepType.CHECKPOINT,
                    parameters={"collect_full_metrics": False}
                )
            ],
            global_config={"n_threads": 2, "n_clients": 10}
        )
        
        # Mock dependencies
        mock_connection_manager = Mock()
        mock_dataset_manager = Mock()
        mock_metric_collector = AsyncMock()
        mock_metric_collector.start = AsyncMock()
        mock_metric_collector.stop = AsyncMock()
        mock_metric_collector.get_memory_summary.return_value = {"test": "data"}
        mock_metric_collector.get_latest_metric.return_value = None
        
        # Create temp output directory
        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir)
            
            # Create scenario runner
            runner = ScenarioRunner(
                connection_manager=mock_connection_manager,
                dataset_manager=mock_dataset_manager,
                metric_collector=mock_metric_collector,
                output_dir=output_dir
            )
            
            # Execute scenario
            results = await runner.run_scenario(simple_scenario)
            
            # Verify results
            assert results["scenario_name"] == "test_integration"
            assert results["steps_executed"] == 2
            assert results["steps_successful"] == 2
            assert results["steps_failed"] == 0
            assert len(results["step_results"]) == 2
            
            # Verify step results
            wait_step_result = results["step_results"][0]
            assert wait_step_result["step_name"] == "wait_step"
            assert wait_step_result["status"] == "success"
            assert wait_step_result["type"] == "wait"
            
            checkpoint_result = results["step_results"][1]
            assert checkpoint_result["step_name"] == "checkpoint"
            assert checkpoint_result["status"] == "success"
            assert checkpoint_result["type"] == "checkpoint"
            
            # Verify mock calls
            mock_metric_collector.start.assert_called_once()
            mock_metric_collector.stop.assert_called_once()
    
    @pytest.mark.asyncio 
    async def test_scenario_with_workload_step(self):
        """Test scenario execution with a mocked workload step."""
        # Create scenario with workload step
        workload_scenario = Scenario(
            name="workload_test",
            description="Test scenario with workload",
            dataset="test-dataset",
            steps=[
                ScenarioStep(
                    name="test_workload",
                    type=StepType.WORKLOAD,
                    workload="ingest",
                    parameters={"target_vectors": 100},
                    duration_seconds=0.1
                )
            ],
            global_config={"n_threads": 1, "n_clients": 5}
        )
        
        # Mock all dependencies
        mock_connection_manager = Mock()
        mock_dataset_manager = Mock()
        mock_dataset = Mock()
        mock_dataset_manager.get_dataset.return_value = mock_dataset
        
        mock_metric_collector = AsyncMock()
        mock_metric_collector.start = AsyncMock()
        mock_metric_collector.stop = AsyncMock()
        mock_metric_collector.get_memory_summary.return_value = {"test": "data"}
        mock_metric_collector.get_latest_metric.return_value = None
        
        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir)
            
            runner = ScenarioRunner(
                connection_manager=mock_connection_manager,
                dataset_manager=mock_dataset_manager,
                metric_collector=mock_metric_collector,
                output_dir=output_dir
            )
            
            # Mock workload execution
            with patch('search_delete_test.scenarios.runner.WorkloadRegistry') as mock_registry, \
                 patch('search_delete_test.scenarios.runner.WorkloadExecutor') as mock_executor_class:
                
                # Setup mocks
                mock_workload = Mock()
                mock_workload._latencies = [5.0, 10.0, 15.0]
                mock_registry.create_instance.return_value = mock_workload
                
                mock_executor = AsyncMock()
                mock_result = Mock()
                mock_result.success_count = 50
                mock_result.failure_count = 0
                mock_result.operations_per_second = 25.0
                mock_result.latency_percentiles = {"p50": 10.0, "p95": 15.0, "p99": 20.0}
                mock_result.additional_metrics = {}
                
                mock_executor.execute_workload.return_value = mock_result
                mock_executor.shutdown = AsyncMock()
                mock_executor_class.return_value = mock_executor
                
                # Execute scenario
                results = await runner.run_scenario(workload_scenario)
                
                # Verify results
                assert results["scenario_name"] == "workload_test"
                assert results["steps_executed"] == 1
                assert results["steps_successful"] == 1
                assert results["steps_failed"] == 0
                
                workload_result = results["step_results"][0]
                assert workload_result["step_name"] == "test_workload"
                assert workload_result["status"] == "success"
                assert workload_result["type"] == "workload"
                assert workload_result["workload"] == "ingest"
                assert workload_result["success_count"] == 50
                assert workload_result["operations_per_second"] == 25.0
                
                # Verify workload was executed
                mock_registry.create_instance.assert_called_once_with("ingest")
                mock_executor.execute_workload.assert_called_once()
                mock_executor.shutdown.assert_called_once()
    
    def test_builtin_scenario_loading(self):
        """Test loading built-in scenarios."""
        loader = ScenarioLoader()
        
        # Test listing built-in scenarios
        scenarios = loader.list_builtin_scenarios()
        assert "continuous_growth" in scenarios
        assert "grow_shrink_grow" in scenarios
        
        # Test loading a built-in scenario
        scenario = loader.load_builtin_scenario("continuous_growth")
        assert scenario.name == "continuous_growth"
        assert scenario.description
        assert len(scenario.steps) > 0
        assert scenario.validate()
    
    def test_scenario_report_generation(self):
        """Test scenario report generation."""
        # Mock scenario results
        results = {
            "scenario_name": "test_report",
            "scenario_description": "Test report generation",
            "total_execution_time": 45.5,
            "steps_executed": 2,
            "steps_successful": 1,
            "steps_failed": 1,
            "step_results": [
                {
                    "step_name": "successful_step",
                    "status": "success",
                    "execution_time": 30.0,
                    "type": "workload",
                    "operations_per_second": 100.0,
                    "success_count": 500
                },
                {
                    "step_name": "failed_step",
                    "status": "failed",
                    "execution_time": 15.5,
                    "error": "Test error message"
                }
            ]
        }
        
        # Create mock runner
        mock_connection_manager = Mock()
        mock_dataset_manager = Mock()
        
        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir)
            
            runner = ScenarioRunner(
                connection_manager=mock_connection_manager,
                dataset_manager=mock_dataset_manager,
                output_dir=output_dir
            )
            
            # Generate report
            report = runner.generate_report(results)
            
            # Verify report content
            assert "SCENARIO EXECUTION REPORT: test_report" in report
            assert "Test report generation" in report
            assert "Total Execution Time: 45.50 seconds" in report
            assert "Steps Executed: 2" in report
            assert "Steps Successful: 1" in report
            assert "Steps Failed: 1" in report
            assert "successful_step" in report
            assert "failed_step" in report
            assert "Test error message" in report
