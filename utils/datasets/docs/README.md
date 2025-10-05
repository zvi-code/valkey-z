# Documentation Index

Welcome to the Search Module Delete Test Testing Tool documentation!

## Getting Started

- **[Installation Guide](../INSTALL.md)**: Complete installation instructions with troubleshooting
- **[Getting Started Guide](GETTING_STARTED.md)**: Quick setup and first test
- **[CLI Reference](CLI_REFERENCE.md)**: Complete command reference

## User Guides

- **[Installation Guide](../INSTALL.md)**: Detailed installation instructions and troubleshooting
- **[Configuration Guide](CONFIGURATION.md)**: Complete configuration reference
- **[Scenario Guide](SCENARIOS.md)**: Creating and managing test scenarios
- **[Dataset Guide](DATASETS.md)**: Working with datasets and public data

## Technical Documentation

- **[Architecture](architecture.md)**: System architecture and design
- **[Workload Development](workload_development.md)**: Creating custom workloads
- **[API Documentation](api/)**: Code API documentation

## Examples

- **[Example Scenarios](../examples/)**: Sample scenario configurations
- **[Example Configs](../config/)**: Sample configuration files

## Quick Links

### For Users
1. [Install the tool](../INSTALL.md)
2. [Run your first test](GETTING_STARTED.md#step-4-run-your-first-test)
3. [Create custom scenarios](GETTING_STARTED.md#step-6-try-a-custom-scenario)

### For Developers
1. [Development setup](../README.md#development)
2. [Architecture overview](architecture.md)
3. [Creating workloads](workload_development.md)

### For System Administrators
1. [Performance tuning](../README.md#performance-tips)
2. [Troubleshooting](../README.md#troubleshooting)
3. [Monitoring setup](monitoring.md) (TODO)

## Command Quick Reference

```bash
# Basic commands
vst info system              # Check system info
vst info valkey              # Check Valkey connection
vst run quick               # Quick test
vst run list-scenarios      # List scenarios

# Dataset management
vst dataset list            # List available datasets
vst dataset download NAME   # Download dataset
vst dataset info FILE       # Dataset information

# Scenario execution
vst validate scenario FILE  # Validate scenario
vst run scenario FILE       # Run scenario
vst run scenario FILE --dry-run  # Validate without running

# Configuration validation
vst validate config FILE    # Validate config file
```

## FAQ

### Common Questions

**Q: What's the difference between quick and scenario tests?**
A: Quick tests use predefined settings for immediate testing. Scenario tests allow complex, multi-phase testing with custom configurations.

**Q: How much memory do I need?**
A: Minimum 4GB RAM recommended. For large datasets (millions of vectors), 16GB+ is recommended.

**Q: Can I test against production Valkey?**
A: Yes, but use caution. Start with small loads and non-production data. The tool can generate significant load.

**Q: What datasets are available?**
A: Run `vst dataset list` to see available datasets. Includes OpenAI embeddings, SIFT features, and GIST descriptors.

**Q: How do I create custom workloads?**
A: See the [Workload Development Guide](workload_development.md) for detailed instructions.

### Troubleshooting

**Problem: "Connection refused"**
```bash
# Check if Valkey is running
valkey-cli ping
# Start Valkey Stack with Docker
docker run -d -p 6379:6379 valkey/valkey-stack-server:latest
```

**Problem: "Search module not found"**
```bash
# Use Valkey Stack (includes all modules)
docker run -d -p 6379:6379 valkey/valkey-stack-server:latest
```

**Problem: "Out of memory"**
```bash
# Reduce test parameters in scenario:
# - Lower target_vectors
# - Reduce n_clients
# - Smaller batch_size
```

**Problem: Tests are slow**
```bash
# Check system resources
vst info system
# Tune configuration:
# - Match n_threads to CPU cores
# - Increase batch_size
# - Optimize Valkey configuration
```

## Contributing

We welcome contributions! See the main [README](../README.md#development) for development setup.

### Documentation

- Documentation is written in Markdown
- Examples should be tested and working
- Include both basic and advanced usage
- Add troubleshooting sections for common issues

### Code

- Follow the existing code style
- Add tests for new features
- Update documentation for changes
- Ensure all tests pass

## Support

- **Issues**: Report bugs and request features via GitHub Issues
- **Discussions**: Ask questions and share experiences
- **Documentation**: Improve these docs via pull requests

## Version History

- **v0.1.0**: Initial release with basic stress testing functionality
- More versions to come...

---

For the most up-to-date information, see the [main README](../README.md).
