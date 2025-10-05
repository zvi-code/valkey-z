# tests/unit/test_scenarios.py
"""Unit tests for scenario components."""

import pytest
import asyncio
import tempfile
from pathlib import Path
from unittest.mock import Mock, AsyncMock, patch

# Import scenario components
from scenarios.models import Scenario, ScenarioStep, StepType
from scenarios.loader import ScenarioLoader
from scenarios.runner import ScenarioRunner, ScenarioExecutionError


@pytest.mark.unit
class TestScenarioModels:
    """Test scenario data models."""
    
    def test_step_type_enum(self):
        """Test step type enumeration."""
        assert StepType.WORKLOAD.value == "workload"
        assert StepType.WAIT.value == "wait"
        assert StepType.CHECKPOINT.value == "checkpoint"
    
    def test_scenario_step_validation(self):
        """Test scenario step validation."""
        # Valid workload step
        workload_step = ScenarioStep(
            name="test_step",
            type=StepType.WORKLOAD,
            workload="ingest",
            parameters={"target_vectors": 1000}
        )
        assert workload_step.validate() is True
        
        # Invalid workload step (missing workload)
        invalid_workload_step = ScenarioStep(
            name="test_step",
            type=StepType.WORKLOAD
        )
        assert invalid_workload_step.validate() is False
        
        # Valid wait step with duration
        wait_step = ScenarioStep(
            name="wait_step",
            type=StepType.WAIT,
            duration_seconds=30
        )
        assert wait_step.validate() is True
        
        # Valid wait step with condition
        wait_step_condition = ScenarioStep(
            name="wait_step",
            type=StepType.WAIT,
            wait_condition={"type": "memory_stable"}
        )
        assert wait_step_condition.validate() is True
        
        # Invalid wait step (no duration or condition)
        invalid_wait_step = ScenarioStep(
            name="wait_step",
            type=StepType.WAIT
        )
        assert invalid_wait_step.validate() is False
        
        # Valid checkpoint step
        checkpoint_step = ScenarioStep(
            name="checkpoint",
            type=StepType.CHECKPOINT
        )
        assert checkpoint_step.validate() is True
    
    def test_scenario_validation(self):
        """Test scenario validation."""
        steps = [
            ScenarioStep(
                name="ingest_data",
                type=StepType.WORKLOAD,
                workload="ingest",
                parameters={"target_vectors": 1000}
            ),
            ScenarioStep(
                name="wait",
                type=StepType.WAIT,
                duration_seconds=30
            )
        ]
        
        # Valid scenario
        scenario = Scenario(
            name="test_scenario",
            description="Test scenario",
            dataset="test-dataset",
            steps=steps
        )
        assert scenario.validate() is True
        
        # Invalid scenario (no steps)
        empty_scenario = Scenario(
            name="test_scenario",
            description="Test scenario",
            dataset="test-dataset",
            steps=[]
        )
        assert empty_scenario.validate() is False
        
        # Invalid scenario (no name)
        no_name_scenario = Scenario(
            name="",
            description="Test scenario",
            dataset="test-dataset",
            steps=steps
        )
        assert no_name_scenario.validate() is False
    
    def test_scenario_duration_calculation(self):
        """Test total duration calculation."""
        steps_with_duration = [
            ScenarioStep(name="step1", type=StepType.WAIT, duration_seconds=30),
            ScenarioStep(name="step2", type=StepType.WAIT, duration_seconds=60),
        ]
        
        scenario = Scenario(
            name="test_scenario",
            description="Test scenario",
            dataset="test-dataset",
            steps=steps_with_duration
        )
        assert scenario.get_total_duration() == 90.0
        
        # Scenario with undetermined duration
        steps_mixed = [
            ScenarioStep(name="step1", type=StepType.WAIT, duration_seconds=30),
            ScenarioStep(name="step2", type=StepType.WAIT),  # No duration
        ]
        
        scenario_mixed = Scenario(
            name="test_scenario",
            description="Test scenario",
            dataset="test-dataset",
            steps=steps_mixed
        )
        assert scenario_mixed.get_total_duration() is None


@pytest.mark.unit
class TestScenarioLoader:
    """Test scenario loading functionality."""
    
    def test_scenario_loader_initialization(self):
        """Test scenario loader initialization."""
        loader = ScenarioLoader()
        assert loader.builtin_scenarios_dir.exists()
    
    def test_valid_scenario_validation(self):
        """Test validation of valid scenario data."""
        loader = ScenarioLoader()
        
        valid_scenario = {
            "name": "test_scenario",
            "description": "Test scenario description",
            "dataset": "test-dataset",
            "steps": [
                {
                    "name": "ingest_step",
                    "type": "workload",
                    "workload": "ingest",
                    "parameters": {"target_vectors": 1000}
                },
                {
                    "name": "wait_step",
                    "type": "wait",
                    "duration_seconds": 30
                }
            ],
            "global_config": {
                "n_threads": 4,
                "n_clients": 100
            }
        }
        
        assert loader.validate_scenario(valid_scenario) is True
    
    def test_invalid_scenario_validation(self):
        """Test validation of invalid scenario data."""
        loader = ScenarioLoader()
        
        # Missing required fields
        missing_name = {
            "description": "Test scenario description",
            "dataset": "test-dataset",
            "steps": []
        }
        assert loader.validate_scenario(missing_name) is False
        
        # Empty steps
        empty_steps = {
            "name": "test_scenario",
            "description": "Test scenario description",
            "dataset": "test-dataset",
            "steps": []
        }
        assert loader.validate_scenario(empty_steps) is False
        
        # Invalid step type
        invalid_step_type = {
            "name": "test_scenario",
            "description": "Test scenario description",
            "dataset": "test-dataset",
            "steps": [
                {
                    "name": "invalid_step",
                    "type": "invalid_type"
                }
            ]
        }
        assert loader.validate_scenario(invalid_step_type) is False
        
        # Workload step missing workload field
        missing_workload = {
            "name": "test_scenario",
            "description": "Test scenario description",
            "dataset": "test-dataset",
            "steps": [
                {
                    "name": "workload_step",
                    "type": "workload"
                }
            ]
        }
        assert loader.validate_scenario(missing_workload) is False
    
    @patch('workload.WorkloadRegistry')
    def test_workload_validation(self, mock_registry):
        """Test workload existence validation."""
        loader = ScenarioLoader()
        
        # Mock registry to have 'ingest' workload available
        mock_registry.get.side_effect = lambda name: name if name == "ingest" else (_ for _ in ()).throw(KeyError(f"Unknown workload: {name}"))
        
        # Valid workload
        valid_step = {
            "name": "ingest_step",
            "type": "workload",
            "workload": "ingest"
        }
        assert loader._validate_step(valid_step, 0) is True
        
        # Invalid workload
        invalid_step = {
            "name": "invalid_step",
            "type": "workload",
            "workload": "nonexistent"
        }
        assert loader._validate_step(invalid_step, 0) is False
    
    def test_yaml_loading(self):
        """Test loading scenario from YAML file."""
        loader = ScenarioLoader()
        
        scenario_yaml = """
name: test_scenario
description: Test scenario from YAML
dataset: test-dataset
global_config:
  n_threads: 4
  n_clients: 100
steps:
  - name: ingest_step
    type: workload
    workload: ingest
    parameters:
      target_vectors: 1000
  - name: wait_step
    type: wait
    duration_seconds: 30
"""
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write(scenario_yaml)
            temp_path = Path(f.name)
        
        try:
            with patch('workload.WorkloadRegistry'):
                scenario = loader.load_scenario(temp_path)
                
                assert scenario.name == "test_scenario"
                assert scenario.description == "Test scenario from YAML"
                assert scenario.dataset == "test-dataset"
                assert len(scenario.steps) == 2
                assert scenario.global_config["n_threads"] == 4
                
        finally:
            temp_path.unlink()
    
    def test_builtin_scenarios_listing(self):
        """Test listing built-in scenarios."""
        loader = ScenarioLoader()
        builtin_scenarios = loader.list_builtin_scenarios()
        
        # Should contain the known built-in scenarios
        assert "continuous_growth" in builtin_scenarios
        assert "grow_shrink_grow" in builtin_scenarios


@pytest.mark.unit 
class TestScenarioRunner:
    """Test scenario execution."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.mock_connection_manager = Mock()
        self.mock_dataset_manager = Mock()
        self.mock_metric_collector = AsyncMock()
        
        # Create temp output directory
        self.temp_dir = Path(tempfile.mkdtemp())
        
        self.runner = ScenarioRunner(
            connection_manager=self.mock_connection_manager,
            dataset_manager=self.mock_dataset_manager,
            metric_collector=self.mock_metric_collector,
            output_dir=self.temp_dir
        )
    
    def teardown_method(self):
        """Clean up test fixtures."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_runner_initialization(self):
        """Test runner initialization."""
        assert self.runner.connection_manager == self.mock_connection_manager
        assert self.runner.dataset_manager == self.mock_dataset_manager
        assert self.runner.metric_collector == self.mock_metric_collector
        assert self.runner.output_dir == self.temp_dir
        assert self.temp_dir.exists()
    
    @pytest.mark.asyncio
    async def test_wait_step_execution(self):
        """Test wait step execution."""
        # Duration-based wait
        wait_step = ScenarioStep(
            name="wait_duration",
            type=StepType.WAIT,
            duration_seconds=0.1  # Short duration for testing
        )
        
        result = await self.runner._execute_wait_step(wait_step)
        
        assert result["type"] == "wait"
        assert result["wait_type"] == "duration"
        assert result["duration_seconds"] == 0.1
        
        # Condition-based wait
        wait_condition_step = ScenarioStep(
            name="wait_condition",
            type=StepType.WAIT,
            wait_condition={"type": "duration", "seconds": 0.1}
        )
        
        result = await self.runner._execute_wait_step(wait_condition_step)
        
        assert result["type"] == "wait"
        assert result["wait_type"] == "condition"
    
    @pytest.mark.asyncio
    async def test_checkpoint_step_execution(self):
        """Test checkpoint step execution."""
        checkpoint_step = ScenarioStep(
            name="checkpoint",
            type=StepType.CHECKPOINT,
            parameters={"collect_full_metrics": False}
        )
        
        result = await self.runner._execute_checkpoint_step(checkpoint_step)
        
        assert result["type"] == "checkpoint"
        assert "timestamp" in result
    
    @pytest.mark.asyncio
    async def test_invalid_step_execution(self):
        """Test handling of invalid step types."""
        # Create a step with invalid type (this is a bit artificial since we're bypassing enum validation)
        invalid_step = ScenarioStep(
            name="invalid",
            type=StepType.WORKLOAD,  # We'll mock this to be invalid
        )
        
        # Directly modify the type to something invalid
        invalid_step.type = "invalid_type"
        
        with pytest.raises(ScenarioExecutionError):
            await self.runner._execute_step(invalid_step, {})
    
    @patch('scenarios.runner.WorkloadRegistry')
    @patch('scenarios.runner.WorkloadExecutor')
    @pytest.mark.asyncio
    async def test_workload_step_execution(self, mock_executor_class, mock_registry):
        """Test workload step execution."""
        # Mock workload and executor
        mock_workload = Mock()
        mock_workload._latencies = [10.0, 15.0, 20.0]  # Mock latency data
        mock_registry.create_instance.return_value = mock_workload
        
        mock_executor = AsyncMock()
        mock_result = Mock()
        mock_result.success_count = 100
        mock_result.failure_count = 0
        mock_result.operations_per_second = 50.0
        mock_result.latency_percentiles = {"p50": 10.0, "p95": 20.0, "p99": 30.0}
        mock_result.additional_metrics = {}
        
        mock_executor.execute_workload.return_value = mock_result
        mock_executor_class.return_value = mock_executor
        
        # Mock dataset manager
        mock_dataset = Mock()
        self.mock_dataset_manager.get_dataset.return_value = mock_dataset
        
        workload_step = ScenarioStep(
            name="ingest_workload",
            type=StepType.WORKLOAD,
            workload="ingest",
            parameters={"target_vectors": 1000},
            duration_seconds=60
        )
        
        # Create a simple scenario for context
        scenario = Scenario(
            name="test_scenario",
            description="Test",
            dataset="test-dataset",
            steps=[workload_step]
        )
        self.runner.current_scenario = scenario
        
        result = await self.runner._execute_workload_step(workload_step, {"n_threads": 4})
        
        assert result["type"] == "workload"
        assert result["workload"] == "ingest"
        assert result["success_count"] == 100
        assert result["failure_count"] == 0
        assert result["operations_per_second"] == 50.0
        
        # Verify method calls
        mock_registry.create_instance.assert_called_once_with("ingest")
        mock_executor.execute_workload.assert_called_once()
        mock_executor.shutdown.assert_called_once()
    
    def test_report_generation(self):
        """Test scenario report generation."""
        # Mock scenario results
        results = {
            "scenario_name": "test_scenario",
            "scenario_description": "Test scenario description",
            "total_execution_time": 120.5,
            "steps_executed": 3,
            "steps_successful": 2,
            "steps_failed": 1,
            "step_results": [
                {
                    "step_name": "step1",
                    "status": "success",
                    "execution_time": 30.0,
                    "type": "workload",
                    "operations_per_second": 100.0,
                    "success_count": 500
                },
                {
                    "step_name": "step2",
                    "status": "success",
                    "execution_time": 60.0,
                    "type": "wait"
                },
                {
                    "step_name": "step3",
                    "status": "failed",
                    "execution_time": 30.5,
                    "error": "Test error"
                }
            ]
        }
        
        report = self.runner.generate_report(results)
        
        assert "SCENARIO EXECUTION REPORT: test_scenario" in report
        assert "Test scenario description" in report
        assert "Total Execution Time: 120.50 seconds" in report
        assert "Steps Executed: 3" in report
        assert "Steps Successful: 2" in report
        assert "Steps Failed: 1" in report
        assert "step1" in report
        assert "step2" in report
        assert "step3" in report
