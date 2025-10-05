# Workload Development Guide

## Creating a Custom Workload

1. Inherit from `BaseWorkload`
2. Implement `execute()` method
3. Implement `get_metrics()` method
4. Register with decorator

## Example

```python
from workload import BaseWorkload, register_workload

@register_workload("my_workload")
class MyWorkload(BaseWorkload):
    async def execute(self, connection_pool, dataset, config):
        # Implementation here
        pass
```

## Best Practices

- Use async/await for Valkey operations
- Track latencies for percentile calculations
- Handle errors gracefully
- Return meaningful metrics
