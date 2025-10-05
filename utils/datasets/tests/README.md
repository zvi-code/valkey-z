# Testing Guide for search_delete_test

This directory contains comprehensive unit and integration tests that **do not require a running Valkey instance**.

## Overview

All tests use mock implementations to simulate Valkey behavior, allowing for:
- Fast test execution
- Consistent test results
- CI/CD pipeline compatibility
- Development without Valkey installation

## Test Structure

```
tests/
├── mocks.py              # Mock implementations of Valkey client and related components
├── conftest.py           # Pytest configuration and shared fixtures
├── unit/
│   ├── test_core_components.py    # Unit tests for core modules
│   ├── test_workload_components.py # Unit tests for workload modules
│   └── ...
└── integration/
    ├── test_end_to_end.py          # Integration tests for complete scenarios
    └── ...
```

## Mock Components

### MockValkeyClient
- Simulates Valkey commands (SET, GET, HSET, etc.)
- Tracks command execution counts
- Configurable latency simulation
- Supports FT.* commands for vector operations

### MockConnectionManager
- Simulates connection pooling
- Multiple pool support
- Async operation support

### MockDataset
- Generates random vectors for testing
- Configurable dimensions and size
- Includes ground truth for recall testing

## Running Tests

### Install Test Dependencies
```bash
pip install pytest pytest-asyncio pytest-cov pytest-mock
```

### Run All Tests
```bash
pytest
```

### Run Specific Test Types

**Unit tests only:**
```bash
pytest -m unit
```

**Integration tests only:**
```bash
pytest -m integration
```

**Quick tests (exclude slow tests):**
```bash
pytest -m "not slow"
```

### With Coverage Report
```bash
pytest --cov=search_delete_test --cov-report=html
```

### Using the Test Runner Script
```bash
# Run all tests with coverage
python run_tests.py --coverage

# Run only unit tests
python run_tests.py --type unit

# Run tests with verbose output and stop on first failure
python run_tests.py --verbose --failfast
```

## Writing New Tests

### Unit Test Example
```python
import pytest
from tests.mocks import MockValkeyClient

class TestMyComponent:
    @pytest.mark.unit
    async def test_my_feature(self, mock_valkey_client):
        # Your test logic here
        result = await mock_valkey_client.set("key", "value")
        assert result is True
```

### Integration Test Example
```python
@pytest.mark.integration
@pytest.mark.asyncio
async def test_complete_scenario(mock_connection_manager, mock_dataset):
    # Test complete workflow
    executor = WorkloadExecutor(n_threads=2, n_clients_per_thread=50)
    # ... test logic ...
```

## Key Testing Patterns

### 1. Testing Async Code
```python
@pytest.mark.asyncio
async def test_async_operation():
    result = await some_async_function()
    assert result == expected
```

### 2. Testing with Mock Valkey
```python
def test_valkey_operation(mock_valkey_client):
    # Mock client tracks all operations
    await mock_valkey_client.set("key", "value")
    assert mock_valkey_client.call_count["set"] == 1
```

### 3. Testing Workloads
```python
async def test_workload(mock_connection_manager, mock_dataset):
    workload = IngestWorkload()
    result = await workload.execute(
        connection_pool=mock_connection_manager.get_pool(0),
        dataset=mock_dataset,
        config={"batch_size": 100}
    )
    assert result.success_count > 0
```

### 4. Testing Vector Operations
```python
def test_vector_expansion():
    vec_ops = VectorOperations(dimensions=128)
    expansion_vectors = vec_ops.generate_expansion_vectors(
        max_norm=10.0,
        n_expansions=3,
        config=VectorExpansionConfig(seed=42)
    )
    assert all(np.linalg.norm(v) > 10.0 for v in expansion_vectors.values())
```

## Continuous Integration

The test suite is designed to run in CI/CD pipelines without external dependencies:

```yaml
# Example GitHub Actions workflow
name: Tests
on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - uses: actions/setup-python@v2
        with:
          python-version: '3.10'
      - run: pip install -e .[dev]
      - run: pytest --cov=search_delete_test
```

## Best Practices

1. **Use Fixtures**: Leverage pytest fixtures for common setup
2. **Mark Tests**: Use appropriate markers (unit, integration, slow)
3. **Mock External Dependencies**: Never require real Valkey
4. **Test Edge Cases**: Include tests for error conditions
5. **Keep Tests Fast**: Mock time-consuming operations
6. **Assert Behavior**: Test behavior, not implementation details

## Debugging Tests

### Run specific test
```bash
pytest tests/unit/test_core_components.py::TestVectorOperations::test_calculate_norm_single_vector -v
```

### Show print statements
```bash
pytest -s
```

### Drop into debugger on failure
```bash
pytest --pdb
```

## Performance Testing

While these tests don't measure real Valkey performance, they can still test:
- Algorithm correctness
- Metric calculation accuracy
- Concurrent execution logic
- Resource management
- Error handling

For actual performance testing, use the real implementation against a Valkey instance.