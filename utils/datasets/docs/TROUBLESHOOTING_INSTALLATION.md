# Troubleshooting Guide: Installation Issues and Solutions

This document covers common installation issues encountered when setting up the Valkey Search Delete Stress Test tool, particularly on ARM Amazon Linux 2 systems.

## Quick Diagnosis

Run this command to quickly identify your system and potential issues:

```bash
echo "=== System Information ==="
echo "OS: $(cat /etc/os-release | grep PRETTY_NAME)"
echo "Architecture: $(uname -m)"
echo "Python3: $(python3 --version 2>/dev/null || echo 'Not found')"
echo "Python3.10: $(python3.10 --version 2>/dev/null || echo 'Not found')"
echo "Conda: $(conda --version 2>/dev/null || echo 'Not found')"
echo "Current directory: $(pwd)"
echo "=========================="
```

## Installation Issues Matrix

| Issue | Symptoms | Platform | Solution |
|-------|----------|----------|----------|
| Conda not found | `zsh: command not found: conda` | ARM64 AL2 | Install Miniforge |
| Python version | `Python 3.9.x - Requires Python 3.10+` | Any | Use conda or find Python 3.10 |
| venv creation fails | `ensurepip returned non-zero exit status` | ARM64 AL2 | Use conda instead |
| SSL module missing | `ssl module in Python is not available` | ARM64 | Use conda Python |
| Typer errors | `Parameter.make_metavar() missing argument` | Any | Upgrade typer |
| Import errors | `ModuleNotFoundError: No module named 'x'` | Any | Set PYTHONPATH or pip install -e |

## Detailed Issue Resolution

### 1. Conda Installation Issues

#### Issue: Command Not Found
```bash
# Error
conda create -n valkey-stress python=3.10
# zsh: command not found: conda
```

#### Root Cause
- Conda not installed
- Conda not in PATH
- Shell not initialized

#### Solutions

**Option A: Install Miniforge (Recommended for ARM64)**
```bash
# Download and install
wget https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-Linux-aarch64.sh
bash Miniforge3-Linux-aarch64.sh -b -p $HOME/miniforge3

# Initialize
$HOME/miniforge3/bin/conda init zsh
source ~/.zshrc

# Test
conda --version
```

**Option B: Install Miniconda**
```bash
# For x86_64 systems
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
bash Miniconda3-latest-Linux-x86_64.sh

# For ARM64 systems (use Miniforge instead)
# Miniconda ARM64 support is limited
```

**Option C: Manual PATH Setup**
```bash
# If conda is installed but not in PATH
export PATH="$HOME/miniforge3/bin:$PATH"
echo 'export PATH="$HOME/miniforge3/bin:$PATH"' >> ~/.zshrc
```

### 2. Python Version Compatibility

#### Issue: Incompatible Python Version
```bash
# Error from verify_installation.py
✗ Python 3.9.21 - Requires Python 3.10+
```

#### Root Cause
- Project requires Python 3.10+
- System has older Python version
- No Python 3.10 available

#### Solutions

**Option A: Use Conda (Recommended)**
```bash
# Create environment with correct Python version
conda create -n valkey-stress python=3.10 -y
conda activate valkey-stress
python --version  # Should show 3.10.x
```

**Option B: Check for System Python 3.10**
```bash
# Look for installed Python 3.10
which python3.10
ls /usr/bin/python3.*

# If found, use it directly
python3.10 --version
python3.10 -m venv valkey-stress-env
```

**Option C: Install Python 3.10 (if root access)**
```bash
# Amazon Linux 2
sudo yum install python3.10 python3.10-pip

# Ubuntu/Debian
sudo apt update
sudo apt install python3.10 python3.10-venv python3.10-pip
```

**Option D: Use pyenv**
```bash
# Install pyenv
curl https://pyenv.run | bash

# Add to shell
echo 'export PATH="$HOME/.pyenv/bin:$PATH"' >> ~/.zshrc
echo 'eval "$(pyenv init --path)"' >> ~/.zshrc
source ~/.zshrc

# Install Python 3.10
pyenv install 3.10.14
pyenv local 3.10.14
```

### 3. Virtual Environment Issues

#### Issue: venv Creation Fails
```bash
# Error
python3 -m venv valkey-stress-env
# Error: Command '[...ensurepip...] returned non-zero exit status 127
```

#### Root Cause
- Missing ensurepip module
- Broken Python installation
- Missing system libraries
- SSL/TLS issues

#### Solutions

**Option A: Use Conda Instead**
```bash
# Conda manages its own Python environment
conda create -n valkey-stress python=3.10
conda activate valkey-stress
# No venv needed
```

**Option B: Install Missing Components**
```bash
# Amazon Linux 2
sudo yum install python3-pip python3-setuptools

# Ubuntu/Debian
sudo apt install python3-venv python3-pip
```

**Option C: Use --without-pip**
```bash
# Create venv without pip, install pip separately
python3 -m venv --without-pip valkey-stress-env
source valkey-stress-env/bin/activate
curl https://bootstrap.pypa.io/get-pip.py | python
```

### 4. SSL/TLS Issues

#### Issue: SSL Module Not Available
```bash
# Error
pip install -r requirements.txt
# WARNING: pip is configured with locations that require TLS/SSL, 
# however the ssl module in Python is not available
```

#### Root Cause
- Python compiled without SSL support
- Missing SSL libraries
- Incomplete Python installation

#### Solutions

**Option A: Use Conda Python**
```bash
# Conda includes properly compiled Python with SSL
conda activate valkey-stress
pip install -r requirements.txt
```

**Option B: Install SSL Libraries**
```bash
# Amazon Linux 2
sudo yum install openssl-devel

# Ubuntu/Debian
sudo apt install libssl-dev
```

**Option C: Compile Python with SSL**
```bash
# Using pyenv (automatically includes SSL)
pyenv install 3.10.14
```

### 5. Package Installation Issues

#### Issue: Typer Compatibility Error
```bash
# Error when running vst command
TypeError: Parameter.make_metavar() missing 1 required positional argument: 'ctx'
```

#### Root Cause
- Incompatible typer version
- Click version mismatch
- Package dependency conflicts

#### Solutions

**Option A: Upgrade Typer**
```bash
pip install "typer>=0.12.0"
```

**Option B: Reinstall with Compatible Versions**
```bash
pip uninstall typer click
pip install "typer>=0.12.0" "click>=8.0.0"
```

**Option C: Use Specific Versions**
```bash
pip install typer==0.16.0 click==8.2.1
```

#### Issue: Module Import Errors
```bash
# Error
ModuleNotFoundError: No module named 'datasets'
```

#### Root Cause
- Module not in Python path
- Relative import issues
- Package not installed in editable mode

#### Solutions

**Option A: Install in Editable Mode**
```bash
cd /path/to/search_delete_test
pip install -e .
```

**Option B: Set Python Path**
```bash
export PYTHONPATH="/path/to/search_delete_test/src:$PYTHONPATH"
echo 'export PYTHONPATH="/path/to/search_delete_test/src:$PYTHONPATH"' >> ~/.zshrc
```

**Option C: Run from Correct Directory**
```bash
cd /path/to/search_delete_test
PYTHONPATH=/path/to/search_delete_test/src python3 -m cli.main --help
```

### 6. Architecture-Specific Issues

#### Issue: Package Not Available for ARM64
```bash
# Error
ERROR: Could not find a version that satisfies the requirement package_name
```

#### Root Cause
- No pre-built wheels for ARM64
- Package not compiled for ARM architecture

#### Solutions

**Option A: Use Conda-Forge**
```bash
# Conda-forge has better ARM64 support
conda install -c conda-forge package_name
```

**Option B: Install from Source**
```bash
# Allow pip to compile from source
pip install --no-binary=all package_name
```

**Option C: Use Alternative Packages**
```bash
# Find ARM64-compatible alternatives
pip install package_name_alternative
```

## Verification and Testing

### Complete Installation Check

```bash
#!/bin/bash
echo "=== Installation Verification ==="

# Check environment
if [[ $CONDA_DEFAULT_ENV ]]; then
    echo "✓ Conda environment: $CONDA_DEFAULT_ENV"
else
    echo "⚠ No conda environment active"
fi

# Check Python
python_version=$(python --version 2>/dev/null)
if [[ $python_version == *"3.10"* ]] || [[ $python_version == *"3.11"* ]] || [[ $python_version == *"3.12"* ]]; then
    echo "✓ Python version: $python_version"
else
    echo "✗ Python version: $python_version (requires 3.10+)"
fi

# Check vst command
if command -v vst &> /dev/null; then
    echo "✓ vst command available"
    vst_version=$(vst version 2>/dev/null | head -1)
    echo "  Version: $vst_version"
else
    echo "✗ vst command not found"
fi

# Check key packages
for pkg in numpy redis psutil typer; do
    if python -c "import $pkg" 2>/dev/null; then
        version=$(python -c "import $pkg; print($pkg.__version__)" 2>/dev/null)
        echo "✓ $pkg: $version"
    else
        echo "✗ $pkg: not found"
    fi
done

echo "=========================="
```

### Manual Testing Steps

```bash
# 1. Basic CLI test
vst --help

# 2. Version information
vst version

# 3. Configuration validation
vst validate config config/default.yaml

# 4. System information
vst info system

# 5. Available workloads
vst info workloads

# 6. Dataset commands
vst dataset --help
```

## Recovery Procedures

### Complete Reset

If installation is completely broken:

```bash
# 1. Remove conda environment
conda env remove -n valkey-stress

# 2. Clean pip cache
pip cache purge

# 3. Remove any existing installations
pip uninstall search_delete_test -y

# 4. Start fresh
conda create -n valkey-stress python=3.10 -y
conda activate valkey-stress
cd /path/to/search_delete_test
pip install -r requirements.txt
pip install -e .
```

### Partial Reset

If only some components are broken:

```bash
# Reset just the Python packages
pip freeze > installed_packages.txt
pip uninstall -r installed_packages.txt -y
pip install -r requirements.txt
pip install -e .
```

## Getting Help

### Log Collection

When asking for help, include this information:

```bash
# System info
uname -a
cat /etc/os-release

# Python environment
which python
python --version
pip list

# Conda info (if using conda)
conda info
conda list

# Error messages
vst --help 2>&1 | head -50
```

### Common Support Resources

- **Project Issues**: GitHub repository issues section
- **Conda Issues**: https://github.com/conda-forge/miniforge/issues
- **Python Issues**: Python.org documentation
- **Amazon Linux**: AWS documentation and forums

---

This troubleshooting guide should help resolve most installation issues. Keep it handy during setup and refer to specific sections based on the errors you encounter.
