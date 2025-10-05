# Testing Guide

This document explains how to run tests in the Valkey Search Delete Stress Test project.

## Quick Start

### Run All Unit Tests (Default)
```bash
# Python version (recommended)
python run_tests.py

# Shell script version
./run_tests.sh
```

### Common Test Commands
```bash
# Run with coverage report
python run_tests.py --coverage

# Run integration tests
python run_tests.py --type integration

# Run quick tests only (exclude slow tests)
python run_tests.py --quick

# Verbose output with fail-fast
python run_tests.py --verbose --failfast

# Run specific test type
python run_tests.py --type unit    # Unit tests only
python run_tests.py --type all     # All tests
```

## Test Runners

### Main Python Runner: `run_tests.py`
- **Comprehensive**: Full-featured test runner with all options
- **Smart Environment Setup**: Automatically configures Python paths
- **Multiple Test Types**: Unit, integration, and all tests
- **Coverage Reports**: HTML and terminal coverage reports
- **Flexible Options**: Verbose, quick, fail-fast modes

### Shell Script Alternative: `run_tests.sh`
- **Simple**: Easy-to-use shell script wrapper
- **Convenient**: Good for quick testing
- **Cross-platform**: Works on macOS, Linux
- **Colorized Output**: Green/red status indicators

## Available Options

| Option | Short | Description |
|--------|-------|-------------|
| `--type {all,unit,integration}` | | Type of tests to run (default: unit) |
| `--coverage` | | Generate HTML and terminal coverage reports |
| `--verbose` | `-v` | Verbose test output |
| `--quick` | `-q` | Run quick tests only (exclude slow tests) |
| `--failfast` | `-x` | Stop on first test failure |
| `--warnings` | `-w` | Show warnings (default: suppressed) |

## Test Organization

### Unit Tests (`tests/unit/`)
- **No External Dependencies**: Run without Valkey
- **Fast Execution**: Complete in seconds
- **Core Logic Testing**: Test business logic and algorithms
- **Mocked Dependencies**: Use mock objects for external services

### Integration Tests (`tests/integration/`)
- **Real Dependencies**: Require running Valkey instance
- **End-to-End Testing**: Test complete workflows
- **Slower Execution**: May take longer to complete
- **System Integration**: Test component interactions

## Examples

### Development Workflow
```bash
# Quick check during development
python run_tests.py --quick --failfast

# Full unit test suite
python run_tests.py --verbose

# Generate coverage report
python run_tests.py --coverage
open htmlcov/index.html  # View coverage report
```

### CI/CD Pipeline
```bash
# Fast feedback for pull requests
python run_tests.py --type unit --failfast

# Full test suite for main branch
python run_tests.py --type all --coverage
```

### Shell Script Examples
```bash
# Quick unit tests
./run_tests.sh --quick

# Integration tests with coverage
./run_tests.sh --integration --coverage

# Verbose all tests
./run_tests.sh --all --verbose
```

## Troubleshooting

### Import Errors
The test runners automatically set up Python paths. If you encounter import errors:
1. Make sure you're running from the project root directory
2. Check that `src/` directory exists and contains the package
3. Verify `tests/` directory structure

### Test Failures
- Use `--verbose` for detailed output
- Use `--failfast` to stop on first failure
- Check test output for specific error messages

### Coverage Issues
- Ensure all source files are in `src/`
- Check that `__init__.py` files exist in package directories
- Use `--coverage` flag to generate reports

## Environment Setup

The test runners automatically:
- Add `src/` to Python path
- Add project root to Python path  
- Create missing `__init__.py` files
- Set up proper environment variables

No manual setup is required - just run the tests!
