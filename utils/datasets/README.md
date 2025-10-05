# Search Module Delete Test Testing Tool

A comprehensive memory stress testing tool for Valkey-Search with vector operations. This tool helps you test memory usage patterns, performance characteristics, and stability of Valkey instances under various vector workloads.

## 🚀 Features

- **🔄 Multiple Workload Types**: Built-in support for ingestion, querying, and data shrinking workloads
- **📊 Memory Monitoring**: Real-time memory usage tracking with detailed metrics
- **🎯 Scenario-Based Testing**: Define complex test scenarios with multiple phases
- **📈 Performance Metrics**: Comprehensive performance and latency measurements
- **⚙️ Configurable**: Flexible configuration for different testing needs
- **📦 Dataset Management**: Built-in dataset download and management
- **🛡️ Validation**: Configuration and scenario validation before execution
- **🌐 Multi-Platform**: Support for Linux, macOS, Windows (WSL), and cloud platforms
- **☁️ S3 Integration**: AWS S3 support for dataset storage and management
- **📊 Visualization**: Generate charts and comparisons from test results

## 📖 Documentation

| Document | Description |
|----------|-------------|
| **[📥 Installation Guide](INSTALL.md)** | Complete installation instructions for all platforms |
| **[⌨️ Commands Reference](COMMANDS.md)** | Comprehensive CLI commands and usage examples |
| **[🚀 Getting Started](docs/GETTING_STARTED.md)** | Quick start tutorials and examples |
| **[⚙️ Configuration](docs/CONFIGURATION.md)** | Detailed configuration options |
| **[🎯 Scenarios](docs/SCENARIOS.md)** | Pre-built scenarios and custom scenario development |

## 🎯 Quick Start

> **Prerequisites**: Python 3.9+, Git, and optionally Docker for Valkey

### 1. Installation

> **Note**: This package is in development. Install from source for latest features.

```bash
# Quick system check
git clone https://github.com/zvi-code/search_delete_test.git
cd search_delete_test
python3 setup_check.py

# Install (requires Python 3.9+)
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
pip install -e .

# Verify installation
python verify_installation.py
vst --help
```

📋 **Need help with installation?** See the **[Complete Installation Guide](INSTALL.md)** for detailed instructions, troubleshooting, and platform-specific guidance.

### 2. Check System Compatibility

```bash
# Check system compatibility
vst info system

# Test Valkey connection
vst info valkey --host localhost --port 6379

# List workload types
vst info workloads

# Show version information
vst version
```

### 3. Setup Valkey

```bash
# Using Docker (recommended)
docker run -d --name valkey-stack \
  -p 6379:6379 \
  valkey/valkey-stack-server:latest

# Verify connection
vst info valkey
```

### 4. Run Your First Test

```bash
# Quick 5-minute stress test
vst run quick --duration 300

# With specific dataset
vst dataset quick-setup small
vst run quick --dataset sift-128d --duration 600

# Check results
ls ./output/
```

## 📊 Dataset Management

Access to comprehensive dataset management:

```bash
# List all available datasets
vst dataset list

# Quick setup by scale
vst dataset quick-setup small    # Testing datasets
vst dataset quick-setup medium   # Production evaluation
vst dataset quick-setup large    # Large-scale testing

# Download specific datasets
vst dataset download openai-5m --output ./_datasets_downloads
vst dataset download sift-128d --force

# Dataset information
vst dataset info openai-5m
vst dataset validate ./_datasets_downloads/openai-5m.h5
```

📋 **Want to explore all dataset options?** See the **[Commands Reference](COMMANDS.md#dataset-commands)** for comprehensive dataset management features.

## 🎯 Scenario Testing

Pre-built scenarios for various testing patterns:

> **Note**: Currently scenarios must be referenced by file path. Built-in scenario discovery is under development.

```bash
# List available scenarios
vst run list-scenarios

# Run built-in scenarios (specify file path)
vst run scenario ./config/scenarios/continuous_growth.yaml
vst run scenario ./config/scenarios/memory_leak_detection.yaml
vst run scenario ./config/scenarios/production_simulation.yaml

# Validate before running
vst run validate ./config/scenarios/my-scenario.yaml
```

**Available scenarios:**
- `continuous_growth` - Continuous memory growth testing
- `grow_shrink_grow` - Oscillating memory patterns
- `high_velocity_ingestion` - High-speed data ingestion
- `memory_leak_detection` - Memory leak simulation and detection
- `index_rebuild_memory_pattern` - Index rebuild impact testing
- `production_simulation` - Real-world production workload patterns
- `cluster_memory_distribution` - Memory distribution across cluster nodes
- `memory_fragmentation_analysis` - Memory fragmentation pattern analysis
- `vector_expansion_memory_impact` - Vector expansion memory impact testing

## ☁️ S3 Dataset Management

Advanced S3 integration for large-scale datasets:

```bash
# List S3 datasets
vst prep list

# Download from S3
vst prep download my-dataset

# Prepare datasets for S3 with RDB generation
vst prep prepare ./dataset.h5 --compress zstd

# Get detailed dataset info
vst prep info my-dataset

# Validate S3 dataset
vst prep validate ./dataset
```

## 📊 Visualization and Analysis

Generate insights from your test results:

```bash
# Generate visualizations from test results
vst visualize generate ./output/scenario-results

# Compare multiple test runs
vst visualize compare ./test1 ./test2 ./test3

# Export data for external analysis
vst visualize export-data ./results ./analysis/data.csv

# List scenarios with available data
vst visualize list-scenarios
```

## ⚙️ Configuration

Quick configuration examples:

```yaml
# Basic configuration
valkey:
  host: localhost
  port: 6379
  
workload:
  type: mixed
  duration: 300
  batch_size: 1000
  
monitoring:
  memory_tracking: true
  performance_metrics: true

# S3 configuration
s3:
  bucket: my-datasets
  region: us-west-2
  multipart_threshold: 100MB
```

📋 **Need detailed configuration options?** See the **[Configuration Guide](docs/CONFIGURATION.md)** for complete configuration reference.

## 🧪 Testing and Validation

```bash
# Run unit tests
python run_tests.py

# Run with coverage
python run_tests.py --coverage

# Quick validation test
vst run quick --duration 30

# Validate configuration files
vst validate scenario ./config/scenarios/my-scenario.yaml
vst validate config ./config/my-config.yaml
```

## 💻 Development

For contributors and developers:

```bash
# Development installation
git clone https://github.com/zvi-code/search_delete_test.git
cd search_delete_test

# Create virtual environment and install dependencies
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
pip install -e .

# Run tests and checks
python run_tests.py
python run_tests.py --coverage

# Optional: Install development tools
pip install black ruff mypy pytest

# Format and check code
black .
ruff check .
mypy src/ --ignore-missing-imports
```

## 🗂️ Project Structure

```
search_delete_test/
├── src/                     # Main source code
│   ├── cli/                 # Command-line interface
│   ├── core/                # Core testing engine
│   ├── datasets/            # Dataset management and S3 integration
│   ├── monitoring/          # Performance monitoring
│   ├── scenarios/           # Scenario definitions
│   ├── visualization/       # Visualization and reporting
│   └── workload/            # Workload implementations
├── config/                  # Configuration files
│   ├── scenarios/           # Pre-built scenarios
│   └── default.yaml         # Default configuration
├── docs/                    # Documentation
├── tests/                   # Test suite
├── examples/                # Usage examples
└── _datasets_downloads/               # Dataset storage (created on first use)
```

## 🤝 Contributing

We welcome contributions! For contribution guidelines:

- Follow Python code style standards (Black, Ruff)
- Include tests for new features
- Update documentation as needed
- Report issues via GitHub Issues
- Submit pull requests for review

## 📄 License

This project is open source. See the repository for license information.

## 🆘 Support and Help

### Documentation
- **[📥 Installation Guide](INSTALL.md)** - Complete installation instructions
- **[⌨️ Commands Reference](COMMANDS.md)** - All CLI commands and examples
- **[🚀 Getting Started](docs/GETTING_STARTED.md)** - Tutorials and quick start
- **[⚙️ Configuration](docs/CONFIGURATION.md)** - Configuration reference
- **[🎯 Scenarios](docs/SCENARIOS.md)** - Scenario development guide

### Troubleshooting
- **[Installation Issues](INSTALL.md#troubleshooting)** - Common installation problems
- **[Commands Help](COMMANDS.md#common-workflows)** - Usage examples and workflows
- **[System Requirements](INSTALL.md#prerequisites)** - Platform requirements

### Community
- **[GitHub Issues](https://github.com/zvi-code/search_delete_test/issues)** - Bug reports and feature requests
- **[Discussions](https://github.com/zvi-code/search_delete_test/discussions)** - Community discussions

---

## 🚀 Ready to Get Started?

1. **[📥 Install the tool](INSTALL.md)** - Follow the installation guide for your platform
2. **[⌨️ Learn the commands](COMMANDS.md)** - Explore CLI capabilities and dataset management  
3. **[🚀 Run your first test](docs/GETTING_STARTED.md)** - Follow the getting started tutorial
4. **[⚙️ Configure for your needs](docs/CONFIGURATION.md)** - Customize for your testing requirements

**Quick validation:**
```bash
# Check system compatibility and run a quick test
vst info system
vst run quick --duration 60
```
