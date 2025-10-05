"""Example custom workload implementation."""

from workload import BaseWorkload, WorkloadResult, register_workload
from typing import Dict, Any
import asyncio


@register_workload("custom_mixed")
class CustomMixedWorkload(BaseWorkload):
    """Custom workload with 70% queries and 30% updates."""
    
    def __init__(self):
        super().__init__("custom_mixed")
        self.query_ratio = 0.7
        self.update_ratio = 0.3
        
    async def execute(self, 
                     connection_pool: Any,
                     dataset: Any,
                     config: Dict[str, Any]) -> WorkloadResult:
        """Execute the mixed workload."""
        # TODO: Implement mixed workload logic
        # TODO: 70% of operations should be queries
        # TODO: 30% should be updates
        raise NotImplementedError("Custom workload not implemented")
        
    def get_metrics(self) -> Dict[str, float]:
        """Return workload-specific metrics."""
        return {
            "query_ratio": self.query_ratio,
            "update_ratio": self.update_ratio,
        }
