"""YAML scenario file parser."""

from __future__ import annotations

import yaml
from pathlib import Path
from typing import Dict, Any, List
import logging
from .models import Scenario, ScenarioStep, StepType

logger = logging.getLogger(__name__)


class ScenarioLoader:
    """Loads and parses scenario definitions from YAML files."""
    
    def __init__(self):
        """Initialize scenario loader."""
        self.builtin_scenarios_dir = Path(__file__).parent.parent.parent.parent / "config" / "scenarios"
        
    def load_scenario(self, scenario_path: Path) -> Scenario:
        """Load a scenario from YAML file."""
        logger.info(f"Loading scenario from: {scenario_path}")
        
        if not scenario_path.exists():
            raise FileNotFoundError(f"Scenario file not found: {scenario_path}")
            
        try:
            with open(scenario_path, 'r') as f:
                scenario_data = yaml.safe_load(f)
                
            if not self.validate_scenario(scenario_data):
                raise ValueError(f"Invalid scenario structure in: {scenario_path}")
                
            return self._create_scenario_from_data(scenario_data)
            
        except yaml.YAMLError as e:
            raise ValueError(f"Invalid YAML in scenario file {scenario_path}: {e}")
        except Exception as e:
            logger.error(f"Failed to load scenario {scenario_path}: {e}")
            raise
        
    def validate_scenario(self, scenario_data: Dict[str, Any]) -> bool:
        """Validate scenario structure and values."""
        try:
            # Check required top-level fields
            required_fields = ["name", "description", "dataset", "steps"]
            for field in required_fields:
                if field not in scenario_data:
                    logger.error(f"Missing required field: {field}")
                    return False
                    
            # Validate steps
            steps = scenario_data.get("steps", [])
            if not isinstance(steps, list) or len(steps) == 0:
                logger.error("Steps must be a non-empty list")
                return False
                
            # Validate each step
            for i, step_data in enumerate(steps):
                if not self._validate_step(step_data, i):
                    return False
                    
            # Validate global config if present
            if "global_config" in scenario_data:
                if not isinstance(scenario_data["global_config"], dict):
                    logger.error("global_config must be a dictionary")
                    return False
                    
            logger.debug(f"Scenario validation passed for: {scenario_data.get('name', 'unknown')}")
            return True
            
        except Exception as e:
            logger.error(f"Scenario validation error: {e}")
            return False
            
    def _validate_step(self, step_data: Dict[str, Any], step_index: int) -> bool:
        """Validate a single step."""
        # Required fields
        if "name" not in step_data:
            logger.error(f"Step {step_index}: missing 'name' field")
            return False
            
        if "type" not in step_data:
            logger.error(f"Step {step_index}: missing 'type' field")
            return False
            
        # Validate step type
        step_type = step_data["type"]
        try:
            step_type_enum = StepType(step_type)
        except ValueError:
            logger.error(f"Step {step_index}: invalid step type '{step_type}'")
            return False
            
        # Type-specific validation
        if step_type_enum == StepType.WORKLOAD:
            if "workload" not in step_data:
                logger.error(f"Step {step_index}: workload steps must specify 'workload' field")
                return False
                
            # Check if workload exists
            from workload import WorkloadRegistry
            workload_name = step_data["workload"]
            try:
                WorkloadRegistry.get(workload_name)
            except KeyError:
                logger.error(f"Step {step_index}: unknown workload '{workload_name}'")
                return False
                
        elif step_type_enum == StepType.WAIT:
            # Wait steps need either duration_seconds or wait_condition
            if "duration_seconds" not in step_data and "wait_condition" not in step_data:
                logger.error(f"Step {step_index}: wait steps must specify either 'duration_seconds' or 'wait_condition'")
                return False
                
        return True
        
    def _create_scenario_from_data(self, scenario_data: Dict[str, Any]) -> Scenario:
        """Create Scenario object from validated data."""
        # Create steps
        steps = []
        for step_data in scenario_data["steps"]:
            step = ScenarioStep(
                name=step_data["name"],
                type=StepType(step_data["type"]),
                workload=step_data.get("workload"),
                parameters=step_data.get("parameters", {}),
                duration_seconds=step_data.get("duration_seconds"),
                wait_condition=step_data.get("wait_condition")
            )
            steps.append(step)
            
        # Create scenario
        scenario = Scenario(
            name=scenario_data["name"],
            description=scenario_data["description"],
            dataset=scenario_data["dataset"],
            steps=steps,
            global_config=scenario_data.get("global_config", {})
        )
        
        # Final validation
        if not scenario.validate():
            raise ValueError(f"Invalid scenario after creation: {scenario.name}")
            
        logger.info(f"Successfully loaded scenario: {scenario.name} with {len(scenario.steps)} steps")
        return scenario
        
    def load_builtin_scenario(self, name: str) -> Scenario:
        """Load a built-in scenario by name."""
        # Map scenario names to files
        scenario_files = {
            "continuous_growth": "continuous_growth.yaml",
            "grow_shrink_grow": "grow_shrink_grow.yaml"
        }
        
        if name not in scenario_files:
            available = ", ".join(scenario_files.keys())
            raise ValueError(f"Unknown built-in scenario '{name}'. Available: {available}")
            
        scenario_path = self.builtin_scenarios_dir / scenario_files[name]
        return self.load_scenario(scenario_path)
        
    def list_builtin_scenarios(self) -> List[str]:
        """List available built-in scenarios."""
        scenarios = []
        if self.builtin_scenarios_dir.exists():
            for file_path in self.builtin_scenarios_dir.glob("*.yaml"):
                scenario_name = file_path.stem
                scenarios.append(scenario_name)
        return sorted(scenarios)
