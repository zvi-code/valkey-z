# Installation Guide: Amazon Linux 2 ARM64 (aarch64)

This guide covers installing the Valkey Search Delete Stress Test tool on Amazon Linux 2 with ARM64 architecture, including conda installation and troubleshooting common issues.

## System Information

- **OS**: Amazon Linux 2 (AL2)
- **Architecture**: ARM64 (aarch64)
- **Shell**: zsh
- **Original Python**: 3.9.21 (system Python)
- **Target Python**: 3.10+ (required by the project)

## Prerequisites Check

First, verify your system information:

```bash
# Check OS version
cat /etc/os-release

# Check architecture
uname -m

# Check available Python versions
which python3
python3 --version
which python3.10
python3.10 --version  # If available
```

## Installation Methods

### Method 1: Conda Installation (Recommended)

#### Step 1: Install Miniforge for ARM64

Miniforge is the recommended conda distribution for ARM64 systems as it provides native ARM64 support and includes conda-forge by default.

```bash
# Download Miniforge for ARM64
wget https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-Linux-aarch64.sh

# Install Miniforge (batch mode)
bash Miniforge3-Linux-aarch64.sh -b -p $HOME/miniforge3

# Initialize conda for zsh
$HOME/miniforge3/bin/conda init zsh

# Reload shell configuration
source ~/.zshrc
```

#### Step 2: Create and Activate Environment

```bash
# Create environment with Python 3.10
conda create -n valkey-stress python=3.10 -y

# Activate environment
conda activate valkey-stress

# Verify Python version
python --version  # Should show Python 3.10.x
```

#### Step 3: Install Project Dependencies

```bash
# Navigate to project directory
cd /path/to/search_delete_test

# Install Python packages
pip install -r requirements.txt

# Install project in editable mode
pip install -e .

# Fix typer compatibility issue (if needed)
pip install "typer>=0.12.0"
```

#### Step 4: Verify Installation

```bash
# Test the CLI
vst --help
vst version
vst validate config config/default.yaml
```

### Method 2: Direct Python Installation (Alternative)

If conda is not available or preferred, you can use the system Python with some modifications.

#### Step 1: Use Available Python 3.10

```bash
# Check if Python 3.10 is available
which python3.10
python3.10 --version
```

#### Step 2: Install Dependencies

```bash
# Install requirements with Python 3.9 (if 3.10 not available)
pip3 install --user -r requirements.txt

# Set up environment variable for Python path
export PYTHONPATH="/path/to/search_delete_test/src:$PYTHONPATH"
```

#### Step 3: Use Local VST Script

```bash
# Make vst script executable
chmod +x vst

# Test with local script
./vst --help

# Add to PATH (optional)
echo 'export PATH="/path/to/search_delete_test:$PATH"' >> ~/.zshrc
source ~/.zshrc
```

## Common Issues and Solutions

### Issue 1: Conda Command Not Found

**Problem**: `zsh: command not found: conda`

**Solution**: 
```bash
# Conda not installed - install Miniforge
wget https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-Linux-aarch64.sh
bash Miniforge3-Linux-aarch64.sh -b -p $HOME/miniforge3
$HOME/miniforge3/bin/conda init zsh
source ~/.zshrc
```

### Issue 2: Python Version Incompatibility

**Problem**: `Python 3.9.21 - Requires Python 3.10+`

**Solutions**:
1. **Use conda** (recommended):
   ```bash
   conda create -n valkey-stress python=3.10
   conda activate valkey-stress
   ```

2. **Check for system Python 3.10**:
   ```bash
   which python3.10
   # If available, use python3.10 instead of python3
   ```

### Issue 3: Virtual Environment Creation Failed

**Problem**: `Error: Command '[...ensurepip...] returned non-zero exit status 127`

**Root Cause**: System Python installation issues or missing libraries.

**Solution**: Use conda instead of venv:
```bash
# Instead of python3 -m venv
conda create -n valkey-stress python=3.10
conda activate valkey-stress
```

### Issue 4: Typer Compatibility Error

**Problem**: `TypeError: Parameter.make_metavar() missing 1 required positional argument: 'ctx'`

**Solution**: Upgrade typer to a compatible version:
```bash
pip install "typer>=0.12.0"
```

### Issue 5: SSL/TLS Module Not Available

**Problem**: `pip is configured with locations that require TLS/SSL, however the ssl module in Python is not available`

**Solution**: Use conda which includes properly compiled Python with SSL support:
```bash
conda activate valkey-stress
pip install -r requirements.txt
```

### Issue 6: Module Import Errors

**Problem**: `ModuleNotFoundError: No module named 'datasets'`

**Solution**: Ensure correct Python path setup:
```bash
# For local installation
export PYTHONPATH="/path/to/search_delete_test/src:$PYTHONPATH"

# Or install in editable mode
pip install -e .
```

## Verification Steps

After installation, verify everything works:

```bash
# 1. Check Python version
python --version

# 2. Test CLI availability
vst --help

# 3. Check version info
vst version

# 4. Validate configuration
vst validate config config/default.yaml

# 5. Test system info
vst info system

# 6. List available workloads
vst info workloads
```

## Architecture-Specific Notes

### ARM64 Amazon Linux 2 Considerations

1. **Package Availability**: Some Python packages may not have pre-built ARM64 wheels
2. **Compilation**: Some packages may need to be compiled from source
3. **Performance**: ARM64 builds may have different performance characteristics
4. **Memory**: ARM instances may have different memory layouts

### Why Miniforge over Anaconda

1. **Native ARM64 Support**: Built specifically for ARM architecture
2. **Conda-forge Default**: Better package availability for ARM64
3. **Smaller Size**: More lightweight than full Anaconda
4. **Faster**: Optimized for ARM64 architecture
5. **Community Support**: Active community maintaining ARM64 packages

## Environment Management

### Useful Conda Commands

```bash
# List environments
conda env list

# Activate environment
conda activate valkey-stress

# Deactivate environment
conda deactivate

# Remove environment
conda env remove -n valkey-stress

# Export environment
conda env export > environment.yml

# Create from exported environment
conda env create -f environment.yml
```

### Shell Configuration

Add to your `~/.zshrc` for persistent setup:

```bash
# Conda initialization (added by conda init)
# ... conda init block ...

# Project-specific aliases
alias vst-activate='conda activate valkey-stress'
alias vst-test='cd /path/to/search_delete_test && vst validate config config/default.yaml'

# Add project to PATH (if using local vst script)
export PATH="/path/to/search_delete_test:$PATH"
```

## Performance Optimization

### For ARM64 Systems

1. **Use conda-forge packages** when possible (better ARM64 optimization)
2. **Set appropriate thread counts** based on ARM core count
3. **Monitor memory usage** (ARM instances may have different memory patterns)
4. **Consider native ARM64 Redis/Valkey** builds for better performance

### Environment Variables

```bash
# Optimize for ARM64
export OPENBLAS_NUM_THREADS=4  # Adjust based on CPU cores
export OMP_NUM_THREADS=4       # Adjust based on CPU cores

# For numpy/scipy performance
export NPY_NUM_BUILD_JOBS=4    # Parallel compilation
```

## Troubleshooting Resources

- **Miniforge Issues**: https://github.com/conda-forge/miniforge/issues
- **ARM64 Package Status**: https://conda-forge.org/status/
- **Amazon Linux 2 Documentation**: https://docs.aws.amazon.com/linux/
- **Project Issues**: Check the project's GitHub issues page

## Alternative Installation Methods

### Using Docker (if available)

```bash
# Use ARM64 Python container
docker run -it --rm -v $(pwd):/workspace python:3.10-slim bash
cd /workspace
pip install -r requirements.txt
pip install -e .
```

### Using pyenv (if preferred)

```bash
# Install pyenv
curl https://pyenv.run | bash

# Add to shell profile
echo 'export PATH="$HOME/.pyenv/bin:$PATH"' >> ~/.zshrc
echo 'eval "$(pyenv init --path)"' >> ~/.zshrc
echo 'eval "$(pyenv init -)"' >> ~/.zshrc
source ~/.zshrc

# Install Python 3.10
pyenv install 3.10.14
pyenv local 3.10.14
```

---

This guide should help you successfully install and run the Valkey Search Delete Stress Test tool on Amazon Linux 2 ARM64 systems. If you encounter issues not covered here, please check the project documentation or open an issue.
