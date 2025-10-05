"""Data models for scenario definitions."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional
from enum import Enum


class StepType(Enum):
    """Types of scenario steps."""
    WORKLOAD = "workload"
    WAIT = "wait"
    CHECKPOINT = "checkpoint"


@dataclass
class ScenarioStep:
    """Single step in a scenario."""
    name: str
    type: StepType
    workload: Optional[str] = None
    parameters: Dict[str, Any] = field(default_factory=dict)
    duration_seconds: Optional[float] = None
    wait_condition: Optional[Dict[str, Any]] = None
    
    def validate(self) -> bool:
        """Validate step configuration."""
        if self.type == StepType.WORKLOAD:
            if not self.workload:
                return False
            # Check if it's a valid workload type
            valid_workloads = ["ingest", "query", "shrink"]
            if self.workload not in valid_workloads:
                return False
        elif self.type == StepType.WAIT:
            if not self.wait_condition and not self.duration_seconds:
                return False
        elif self.type == StepType.CHECKPOINT:
            # Checkpoints are always valid with any parameters
            pass
        
        return True


@dataclass
class Scenario:
    """Complete scenario definition."""
    name: str
    description: str
    dataset: str
    steps: List[ScenarioStep]
    global_config: Dict[str, Any] = field(default_factory=dict)
    
    def validate(self) -> bool:
        """Validate scenario configuration."""
        if not self.name or not self.description:
            return False
        
        if not self.steps:
            return False
        
        # Validate all steps
        for step in self.steps:
            if not step.validate():
                return False
        
        return True
        
    def get_total_duration(self) -> Optional[float]:
        """Calculate total scenario duration if deterministic."""
        total = 0.0
        for step in self.steps:
            if step.duration_seconds is None:
                # Can't calculate total if any step has no duration
                return None
            total += step.duration_seconds
        
        return total
