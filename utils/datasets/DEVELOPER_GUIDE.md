# Developer Guide

Technical documentation for developers working on the Valkey Search Delete Stress Test tool, including architecture, design patterns, testing, and contribution guidelines.

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Project Structure](#project-structure)
- [Design Patterns](#design-patterns)
- [Development Setup](#development-setup)
- [Testing Framework](#testing-framework)
- [Code Style and Standards](#code-style-and-standards)
- [Contributing Guidelines](#contributing-guidelines)
- [Advanced Features](#advanced-features)
- [Performance Optimization](#performance-optimization)
- [Troubleshooting Development Issues](#troubleshooting-development-issues)

## Architecture Overview

The Valkey Search Delete Stress Test tool is built with a modular architecture designed for extensibility, maintainability, and performance.

### Core Components

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   CLI Layer     │    │  Configuration  │    │   Validation    │
│                 │    │    Management   │    │     Layer       │
│ - Commands      │    │                 │    │                 │
│ - Argument      │    │ - YAML Config   │    │ - Schema        │
│   Parsing       │    │ - Environment   │    │   Validation    │
│ - Help System   │    │   Variables     │    │ - Data Types    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
┌─────────────────────────────────┼─────────────────────────────────┐
│                    Core Engine                                    │
├─────────────────┬─────────────────┬─────────────────┬─────────────┤
│   Connection    │    Workload     │   Monitoring    │   Dataset   │
│   Management    │    Execution    │   & Metrics     │ Management  │
│                 │                 │                 │             │
│ - Valkey Pool   │ - Ingest        │ - Memory        │ - Download  │
│ - Health Check  │ - Query         │ - Performance   │ - Validate  │
│ - Retry Logic   │ - Mixed         │ - Export        │ - Convert   │
│ - Failover      │ - Shrink        │ - Visualization │ - S3 Sync   │
└─────────────────┴─────────────────┴─────────────────┴─────────────┘
```

### Key Design Principles

1. **Modularity**: Each component has a single responsibility
2. **Extensibility**: Easy to add new workloads, datasets, and metrics
3. **Configuration-Driven**: Behavior controlled through YAML configuration
4. **Async/Concurrent**: High-performance concurrent execution
5. **Observability**: Comprehensive metrics and logging
6. **Testability**: Extensive unit and integration test coverage

## Project Structure

```
search_delete_test/
├── src/                          # Main source code
│   ├── cli/                      # Command-line interface
│   │   ├── main.py              # CLI entry point
│   │   ├── utils.py             # CLI utilities
│   │   └── commands/            # Command implementations
│   │       ├── dataset.py       # Dataset management commands
│   │       ├── info.py          # System information commands
│   │       ├── prep.py          # Dataset preparation commands
│   │       ├── run.py           # Test execution commands
│   │       ├── validate.py      # Validation commands
│   │       └── visualize.py     # Visualization commands
│   │
│   ├── core/                    # Core engine components
│   │   ├── config.py           # Configuration management
│   │   ├── connection.py       # Valkey connection handling
│   │   ├── dataset.py          # Dataset abstraction
│   │   ├── metrics.py          # Metrics collection
│   │   └── vector_ops.py       # Vector operations
│   │
│   ├── datasets/               # Dataset management
│   │   ├── converter.py        # Format conversion
│   │   ├── dataset.py          # Dataset classes
│   │   ├── formats.py          # Format definitions
│   │   ├── metadata.py         # Dataset metadata
│   │   ├── preparer.py         # Dataset preparation
│   │   ├── rdb_generator.py    # RDB file generation
│   │   ├── s3_manager.py       # S3 integration
│   │   └── streaming.py        # Streaming operations
│   │
│   ├── monitoring/             # Monitoring and metrics
│   │   ├── aggregator.py       # Metrics aggregation
│   │   ├── collector.py        # Data collection
│   │   └── exporter.py         # Export functionality
│   │
│   ├── scenarios/              # Scenario management
│   │   ├── loader.py           # Scenario loading
│   │   ├── models.py           # Scenario data models
│   │   └── runner.py           # Scenario execution
│   │
│   ├── visualization/          # Data visualization
│   │   └── advanced_visualizer.py # Chart generation
│   │
│   └── workload/               # Workload implementations
│       ├── base.py             # Base workload class
│       ├── executor.py         # Workload execution engine
│       ├── ingest.py           # Data ingestion workload
│       ├── query.py            # Query workload
│       ├── registry.py         # Workload registry
│       └── shrink.py           # Data shrinking workload
│
├── config/                     # Configuration files
│   ├── default.yaml           # Default configuration
│   └── scenarios/             # Built-in scenarios
│       ├── continuous_growth.yaml
│       ├── memory_leak_detection.yaml
│       └── production_simulation.yaml
│
├── docs/                      # Documentation (merged into guides)
├── tests/                     # Test suite
│   ├── unit/                  # Unit tests
│   ├── integration/           # Integration tests
│   └── conftest.py           # Test configuration
│
├── examples/                  # Usage examples
├── scripts/                   # Utility scripts
└── requirements.txt          # Python dependencies
```

## Design Patterns

### 1. Factory Pattern - Workload Creation

```python
# src/workload/registry.py
class WorkloadRegistry:
    """Registry for workload types using Factory pattern."""
    
    _workloads = {}
    
    @classmethod
    def register(cls, name: str, workload_class: Type[BaseWorkload]):
        """Register a workload type."""
        cls._workloads[name] = workload_class
    
    @classmethod
    def create(cls, name: str, config: Dict) -> BaseWorkload:
        """Create workload instance."""
        if name not in cls._workloads:
            raise ValueError(f"Unknown workload: {name}")
        return cls._workloads[name](config)

# Usage in workload implementations
@WorkloadRegistry.register("ingest")
class IngestWorkload(BaseWorkload):
    def execute(self):
        # Implementation
        pass
```

### 2. Strategy Pattern - Dataset Formats

```python
# src/datasets/formats.py
class DatasetFormat(ABC):
    """Abstract base class for dataset formats."""
    
    @abstractmethod
    def load(self, path: str) -> np.ndarray:
        """Load dataset from file."""
        pass
    
    @abstractmethod
    def save(self, data: np.ndarray, path: str):
        """Save dataset to file."""
        pass

class HDF5Format(DatasetFormat):
    def load(self, path: str) -> np.ndarray:
        with h5py.File(path, 'r') as f:
            return f['vectors'][:]
    
    def save(self, data: np.ndarray, path: str):
        with h5py.File(path, 'w') as f:
            f.create_dataset('vectors', data=data)

# Format registry
FORMAT_REGISTRY = {
    '.h5': HDF5Format(),
    '.hdf5': HDF5Format(),
    '.npy': NumpyFormat(),
    '.parquet': ParquetFormat(),
}
```

### 3. Observer Pattern - Metrics Collection

```python
# src/monitoring/collector.py
class MetricsCollector:
    """Collects metrics using Observer pattern."""
    
    def __init__(self):
        self._observers = []
    
    def add_observer(self, observer: MetricsObserver):
        self._observers.append(observer)
    
    def notify_observers(self, metric: Metric):
        for observer in self._observers:
            observer.on_metric(metric)

class MemoryObserver(MetricsObserver):
    def on_metric(self, metric: Metric):
        if metric.type == 'memory':
            self.process_memory_metric(metric)
```

### 4. Command Pattern - CLI Commands

```python
# src/cli/commands/base.py
class Command(ABC):
    """Base class for CLI commands using Command pattern."""
    
    @abstractmethod
    def execute(self, args: argparse.Namespace) -> int:
        """Execute the command."""
        pass

class RunCommand(Command):
    def execute(self, args: argparse.Namespace) -> int:
        # Command implementation
        return 0
```

### 5. Builder Pattern - Configuration

```python
# src/core/config.py
class ConfigBuilder:
    """Builds configuration using Builder pattern."""
    
    def __init__(self):
        self._config = {}
    
    def with_valkey(self, host: str, port: int) -> 'ConfigBuilder':
        self._config['valkey'] = {'host': host, 'port': port}
        return self
    
    def with_workload(self, threads: int, clients: int) -> 'ConfigBuilder':
        self._config['workload'] = {'n_threads': threads, 'n_clients': clients}
        return self
    
    def build(self) -> Config:
        return Config(self._config)

# Usage
config = (ConfigBuilder()
    .with_valkey('localhost', 6379)
    .with_workload(8, 1000)
    .build())
```

## Development Setup

### Prerequisites

- Python 3.10+
- Git
- Virtual environment tool (venv, conda, or poetry)
- Docker (for integration tests)

### Development Installation

```bash
# Clone the repository
git clone https://github.com/zvi-code/search_delete_test.git
cd search_delete_test

# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install in development mode with all dependencies
pip install -e ".[dev,test,visualization]"

# Install pre-commit hooks
pre-commit install

# Verify installation
python -m pytest tests/ -v
```

### Development Dependencies

```bash
# Core development tools
pip install black ruff mypy pytest pytest-cov

# Optional visualization dependencies
pip install matplotlib seaborn plotly

# Documentation tools
pip install sphinx sphinx-rtd-theme

# Performance profiling
pip install py-spy memory-profiler
```

### IDE Configuration

#### VS Code Settings

```json
{
    "python.defaultInterpreterPath": "./venv/bin/python",
    "python.linting.enabled": true,
    "python.linting.ruffEnabled": true,
    "python.formatting.provider": "black",
    "python.testing.pytestEnabled": true,
    "python.testing.pytestArgs": ["tests/"],
    "files.exclude": {
        "**/__pycache__": true,
        "**/*.pyc": true,
        ".pytest_cache": true,
        ".mypy_cache": true
    }
}
```

#### PyCharm Configuration

1. Set Python interpreter to `./venv/bin/python`
2. Enable pytest as test runner
3. Configure Black as code formatter
4. Enable Ruff for linting

## Testing Framework

### Test Structure

```
tests/
├── unit/                      # Unit tests
│   ├── test_cli_commands.py   # CLI command tests
│   ├── test_core_components.py # Core component tests
│   ├── test_dataset.py        # Dataset handling tests
│   ├── test_metrics.py        # Metrics collection tests
│   ├── test_scenarios.py      # Scenario tests
│   ├── test_vector_ops.py     # Vector operation tests
│   └── test_workload_components.py # Workload tests
│
├── integration/               # Integration tests
│   ├── test_cli_integration.py # End-to-end CLI tests
│   ├── test_end_to_end.py     # Full system tests
│   └── test_scenarios.py      # Scenario integration tests
│
├── conftest.py               # Pytest configuration
├── mocks.py                  # Test mocks and fixtures
└── README.md                 # Testing documentation
```

### Running Tests

```bash
# Run all tests
python run_tests.py

# Run with coverage
python run_tests.py --coverage

# Run specific test categories
pytest tests/unit/ -v                    # Unit tests only
pytest tests/integration/ -v             # Integration tests only
pytest tests/unit/test_dataset.py -v     # Specific test file

# Run tests with markers
pytest -m "not slow" -v                  # Skip slow tests
pytest -m "integration" -v               # Integration tests only

# Run tests in parallel
pytest -n auto tests/                    # Use all CPU cores
```

### Test Configuration

```python
# tests/conftest.py
import pytest
from unittest.mock import Mock
from src.core.config import Config
from src.core.connection import ValkeyConnection

@pytest.fixture
def mock_config():
    """Mock configuration for testing."""
    return Config({
        'valkey': {'host': 'localhost', 'port': 6379},
        'workload': {'n_threads': 2, 'batch_size': 100}
    })

@pytest.fixture
def mock_valkey_connection():
    """Mock Valkey connection for testing."""
    mock = Mock(spec=ValkeyConnection)
    mock.ping.return_value = True
    mock.info.return_value = {'valkey_version': '7.0.0'}
    return mock

@pytest.fixture(scope="session")
def docker_valkey():
    """Start Valkey container for integration tests."""
    import docker
    client = docker.from_env()
    
    container = client.containers.run(
        "valkey/valkey-stack-server:latest",
        ports={'6379/tcp': 6379},
        detach=True,
        remove=True
    )
    
    # Wait for Valkey to be ready
    import time
    time.sleep(5)
    
    yield container
    
    container.stop()
```

### Writing Tests

#### Unit Test Example

```python
# tests/unit/test_dataset.py
import pytest
import numpy as np
from src.datasets.dataset import Dataset
from src.datasets.formats import HDF5Format

class TestDataset:
    def test_load_hdf5_dataset(self, tmp_path):
        """Test loading HDF5 dataset."""
        # Create test data
        data = np.random.rand(1000, 128).astype(np.float32)
        
        # Save test file
        test_file = tmp_path / "test.h5"
        format_handler = HDF5Format()
        format_handler.save(data, str(test_file))
        
        # Load and verify
        dataset = Dataset.load(str(test_file))
        assert dataset.vectors.shape == (1000, 128)
        np.testing.assert_array_equal(dataset.vectors, data)
    
    def test_dataset_validation(self):
        """Test dataset validation."""
        # Test invalid dimensions
        with pytest.raises(ValueError, match="Invalid dimensions"):
            Dataset(vectors=np.array([1, 2, 3]))  # 1D array
        
        # Test empty dataset
        with pytest.raises(ValueError, match="Empty dataset"):
            Dataset(vectors=np.array([]).reshape(0, 128))
```

#### Integration Test Example

```python
# tests/integration/test_end_to_end.py
import pytest
from src.cli.main import main
from src.core.config import Config

class TestEndToEnd:
    def test_quick_run_command(self, docker_valkey, tmp_path):
        """Test complete quick run workflow."""
        # Setup
        output_dir = tmp_path / "output"
        
        # Run command
        result = main([
            'run', 'quick',
            '--duration', '30',
            '--output', str(output_dir),
            '--dataset', 'test-small'
        ])
        
        # Verify results
        assert result == 0
        assert (output_dir / "metrics.csv").exists()
        assert (output_dir / "summary.csv").exists()
        
        # Verify metrics content
        import pandas as pd
        metrics = pd.read_csv(output_dir / "metrics.csv")
        assert len(metrics) > 0
        assert 'timestamp' in metrics.columns
        assert 'rss_mb' in metrics.columns
```

### Test Utilities

```python
# tests/mocks.py
from unittest.mock import Mock, MagicMock
import numpy as np

class MockDataset:
    """Mock dataset for testing."""
    
    def __init__(self, size=1000, dimensions=128):
        self.vectors = np.random.rand(size, dimensions).astype(np.float32)
        self.size = size
        self.dimensions = dimensions
    
    def sample(self, n):
        indices = np.random.choice(self.size, n, replace=False)
        return self.vectors[indices]

class MockValkeyConnection:
    """Mock Valkey connection for testing."""
    
    def __init__(self):
        self.data = {}
        self.indexes = {}
    
    def ft_create(self, index_name, schema):
        self.indexes[index_name] = schema
        return "OK"
    
    def hset(self, key, mapping):
        self.data[key] = mapping
        return len(mapping)
    
    def ft_search(self, index_name, query):
        # Simple mock search
        return Mock(docs=[Mock(id=f"doc:{i}") for i in range(10)])
```

## Code Style and Standards

### Code Formatting

The project uses Black for code formatting and Ruff for linting.

```bash
# Format code
black src/ tests/

# Check formatting
black --check src/ tests/

# Lint code
ruff check src/ tests/

# Fix linting issues
ruff check --fix src/ tests/
```

### Type Hints

All code should include comprehensive type hints:

```python
from typing import Dict, List, Optional, Union, Any
from pathlib import Path
import numpy as np

def load_dataset(
    path: Union[str, Path],
    format_type: Optional[str] = None,
    max_vectors: Optional[int] = None
) -> Dict[str, Any]:
    """Load dataset from file.
    
    Args:
        path: Path to dataset file
        format_type: Dataset format (auto-detected if None)
        max_vectors: Maximum vectors to load (all if None)
    
    Returns:
        Dictionary containing vectors and metadata
    
    Raises:
        FileNotFoundError: If dataset file doesn't exist
        ValueError: If dataset format is unsupported
    """
    # Implementation
    pass
```

### Documentation Standards

#### Docstring Format

Use Google-style docstrings:

```python
class DatasetManager:
    """Manages dataset operations including loading, validation, and conversion.
    
    This class provides a unified interface for working with various dataset
    formats and handles format detection, validation, and conversion between
    different vector dataset formats.
    
    Attributes:
        supported_formats: List of supported dataset formats
        cache_enabled: Whether dataset caching is enabled
    
    Example:
        >>> manager = DatasetManager()
        >>> dataset = manager.load("path/to/dataset.h5")
        >>> manager.validate(dataset)
    """
    
    def __init__(self, cache_enabled: bool = True):
        """Initialize dataset manager.
        
        Args:
            cache_enabled: Enable dataset caching for performance
        """
        self.cache_enabled = cache_enabled
        self.supported_formats = ['.h5', '.hdf5', '.npy', '.parquet']
    
    def load(
        self,
        path: Union[str, Path],
        validate: bool = True
    ) -> Dataset:
        """Load dataset from file.
        
        Args:
            path: Path to dataset file
            validate: Whether to validate dataset after loading
        
        Returns:
            Loaded dataset object
        
        Raises:
            FileNotFoundError: If dataset file doesn't exist
            ValueError: If dataset is invalid
        """
        # Implementation
        pass
```

### Error Handling

Use specific exception types and provide helpful error messages:

```python
class DatasetError(Exception):
    """Base exception for dataset operations."""
    pass

class DatasetFormatError(DatasetError):
    """Raised when dataset format is unsupported or invalid."""
    pass

class DatasetValidationError(DatasetError):
    """Raised when dataset validation fails."""
    pass

def validate_dataset(dataset: np.ndarray) -> None:
    """Validate dataset format and content.
    
    Args:
        dataset: Dataset array to validate
    
    Raises:
        DatasetValidationError: If dataset is invalid
    """
    if dataset.ndim != 2:
        raise DatasetValidationError(
            f"Dataset must be 2D array, got {dataset.ndim}D"
        )
    
    if dataset.size == 0:
        raise DatasetValidationError("Dataset cannot be empty")
    
    if not np.isfinite(dataset).all():
        raise DatasetValidationError("Dataset contains non-finite values")
```

### Logging

Use structured logging throughout the codebase:

```python
import logging
from typing import Dict, Any

# Configure logger
logger = logging.getLogger(__name__)

class WorkloadExecutor:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
    
    def execute(self) -> None:
        """Execute workload with comprehensive logging."""
        self.logger.info(
            "Starting workload execution",
            extra={
                'workload_type': self.config.get('type'),
                'threads': self.config.get('n_threads'),
                'clients': self.config.get('n_clients')
            }
        )
        
        try:
            self._run_workload()
            self.logger.info("Workload execution completed successfully")
        except Exception as e:
            self.logger.error(
                "Workload execution failed",
                extra={'error': str(e), 'error_type': type(e).__name__},
                exc_info=True
            )
            raise
```

## Contributing Guidelines

### Development Workflow

1. **Fork and Clone**
   ```bash
   git clone https://github.com/your-username/search_delete_test.git
   cd search_delete_test
   ```

2. **Create Feature Branch**
   ```bash
   git checkout -b feature/your-feature-name
   ```

3. **Make Changes**
   - Follow code style guidelines
   - Add tests for new functionality
   - Update documentation

4. **Test Changes**
   ```bash
   python run_tests.py
   black --check src/ tests/
   ruff check src/ tests/
   mypy src/ --ignore-missing-imports
   ```

5. **Commit Changes**
   ```bash
   git add .
   git commit -m "feat: add new dataset format support"
   ```

6. **Push and Create PR**
   ```bash
   git push origin feature/your-feature-name
   # Create pull request on GitHub
   ```

### Commit Message Format

Use conventional commit format:

```
<type>(<scope>): <description>

[optional body]

[optional footer]
```

Types:
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `style`: Code style changes
- `refactor`: Code refactoring
- `test`: Test additions/changes
- `chore`: Maintenance tasks

Examples:
```
feat(dataset): add support for Parquet format
fix(cli): handle missing configuration file gracefully
docs(api): update CLI command documentation
test(workload): add integration tests for query workload
```

### Code Review Guidelines

#### For Contributors

- Keep PRs focused and small
- Include tests for new functionality
- Update documentation as needed
- Ensure all CI checks pass
- Respond to review feedback promptly

#### For Reviewers

- Review for correctness, performance, and maintainability
- Check test coverage and quality
- Verify documentation updates
- Ensure code follows project standards
- Be constructive and helpful in feedback

### Adding New Features

#### Adding a New Workload

1. **Create Workload Class**
   ```python
   # src/workload/my_workload.py
   from .base import BaseWorkload
   from .registry import WorkloadRegistry
   
   @WorkloadRegistry.register("my_workload")
   class MyWorkload(BaseWorkload):
       def __init__(self, config):
           super().__init__(config)
           # Initialize workload-specific settings
       
       def execute(self):
           # Implement workload logic
           pass
   ```

2. **Add Tests**
   ```python
   # tests/unit/test_my_workload.py
   from src.workload.my_workload import MyWorkload
   
   class TestMyWorkload:
       def test_workload_execution(self):
           # Test workload functionality
           pass
   ```

3. **Update Documentation**
   - Add workload description to user guide
   - Update API reference
   - Include usage examples

#### Adding a New Dataset Format

1. **Implement Format Handler**
   ```python
   # src/datasets/formats.py
   class MyFormat(DatasetFormat):
       def load(self, path: str) -> np.ndarray:
           # Implement loading logic
           pass
       
       def save(self, data: np.ndarray, path: str):
           # Implement saving logic
           pass
   
   # Register format
   FORMAT_REGISTRY['.myformat'] = MyFormat()
   ```

2. **Add Format Tests**
   ```python
   # tests/unit/test_formats.py
   def test_my_format_load_save(tmp_path):
       # Test format loading and saving
       pass
   ```

3. **Update Documentation**
   - Add format to supported formats list
   - Include usage examples
   - Document any format-specific requirements

## Advanced Features

### Custom Metrics Collection

```python
# src/monitoring/custom_metrics.py
from .collector import MetricsCollector, Metric
from typing import Dict, Any

class CustomMetricsCollector(MetricsCollector):
    """Custom metrics collector for specialized monitoring."""
    
    def __init__(self):
        super().__init__()
        self.custom_metrics = {}
    
    def collect_custom_metric(self, name: str, value: Any, tags: Dict[str, str] = None):
        """Collect custom metric."""
        metric = Metric(
            name=name,
            value=value,
            timestamp=time.time(),
            tags=tags or {}
        )
        self.notify_observers(metric)
    
    def collect_valkey_info(self, connection):
        """Collect Valkey-specific metrics."""
        info = connection.info()
        
        # Memory metrics
        self.collect_custom_metric(
            'valkey_memory_used',
            info.get('used_memory', 0),
            tags={'type': 'memory'}
        )
        
        # Performance metrics
        self.collect_custom_metric(
            'valkey_ops_per_sec',
            info.get('instantaneous_ops_per_sec', 0),
            tags={'type': 'performance'}
        )
```

### Plugin System

```python
# src/plugins/base.py
from abc import ABC, abstractmethod
from typing import Dict, Any

class Plugin(ABC):
    """Base class for plugins."""
    
    @abstractmethod
    def initialize(self, config: Dict[str, Any]) -> None:
        """Initialize plugin with configuration."""
        pass
    
    @abstractmethod
    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Execute plugin logic."""
        pass

# src/plugins/registry.py
class PluginRegistry:
    """Registry for plugins."""
    
    _plugins = {}
    
    @classmethod
    def register(cls, name: str, plugin_class: type):
        cls._plugins[name] = plugin_class
    
    @classmethod
    def get_plugin(cls, name: str) -> Plugin:
        if name not in cls._plugins:
            raise ValueError(f"Unknown plugin: {name}")
        return cls._plugins[name]()

# Example plugin
@PluginRegistry.register("custom_analyzer")
class CustomAnalyzerPlugin(Plugin):
    def initialize(self, config: Dict[str, Any]) -> None:
        self.config = config
    
    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        # Custom analysis logic
        return {"analysis_result": "custom_data"}
```

### Performance Profiling

```python
# src/profiling/profiler.py
import cProfile
import pstats
from functools import wraps
from typing import Callable, Any

def profile_function(func: Callable) -> Callable:
    """Decorator to profile function execution."""
    
    @wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        profiler = cProfile.Profile()
        profiler.enable()
        
        try:
            result = func(*args, **kwargs)
        finally:
            profiler.disable()
            
            # Save profile stats
            stats = pstats.Stats(profiler)
            stats.sort_stats('cumulative')
            stats.dump_stats(f"{func.__name__}_profile.stats")
        
        return result
    
    return wrapper

# Usage
@profile_function
def expensive_operation():
    # Function to profile
    pass
```

## Performance Optimization

### Memory Management

```python
# src/core/memory.py
import gc
import psutil
from typing import Optional

class MemoryManager:
    """Manages memory usage and optimization."""
    
    def __init__(self, max_memory_gb: Optional[float] = None):
        self.max_memory_gb = max_memory_gb
        self.process = psutil.Process()
    
    def get_memory_usage(self) -> Dict[str, float]:
        """Get current memory usage."""
        memory_info = self.process.memory_info()
        return {
            'rss_mb': memory_info.rss / 1024 / 1024,
            'vms_mb': memory_info.vms / 1024 / 1024,
            'percent': self.process.memory_percent()
        }
    
    def check_memory_limit(self) -> bool:
        """Check if memory usage exceeds limit."""
        if not self.max_memory_gb:
            return False
        
        current_gb = self.get_memory_usage()['rss_mb'] / 1024
        return current_gb > self.max_memory_gb
    
    def optimize_memory(self):
        """Optimize memory usage."""
        # Force garbage collection
        gc.collect()
        
        # Additional optimization strategies
        if self.check_memory_limit():
            self._reduce_cache_sizes()
    
    def _reduce_cache_sizes(self):
        """Reduce cache sizes to free memory."""
        # Implementation specific to application caches
        pass
```

### Concurrent Execution

```python
# src/workload/concurrent.py
import asyncio
import concurrent.futures
from
