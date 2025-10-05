# Installation Issues Summary and Solutions

This document summarizes the specific issues we encountered during installation and their solutions, based on our real experience with ARM Amazon Linux 2.

## Issues Encountered and Resolved

### 1. Initial Conda Unavailability

**Issue**: `zsh: command not found: conda`

**Context**: User attempted to create conda environment but conda was not installed on the system.

**Root Cause**: Conda not installed on fresh Amazon Linux 2 ARM64 system.

**Solution Applied**:
```bash
# Downloaded and installed Miniforge for ARM64
wget https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-Linux-aarch64.sh
bash Miniforge3-Linux-aarch64.sh -b -p $HOME/miniforge3
$HOME/miniforge3/bin/conda init zsh
source ~/.zshrc
```

**Why This Solution**: Miniforge provides native ARM64 support with conda-forge as default channel, ensuring better package availability for ARM architecture.

### 2. Python Version Incompatibility

**Issue**: Project requires Python 3.10+ but system had Python 3.9.21

**Error Message**: 
```
✗ Python 3.9.21 - Requires Python 3.10+
```

**Context**: The project's verification script detected incompatible Python version.

**Solutions Applied**:

**Primary Solution**: Use conda to create environment with Python 3.10
```bash
conda create -n valkey-stress python=3.10 -y
conda activate valkey-stress
```

**Alternative Solution**: Found that Python 3.10 was available on system
```bash
which python3.10  # Returned /usr/local/bin/python3.10
python3.10 --version  # Showed Python 3.10.14
```

**Why These Solutions**: Conda environments provide clean, isolated Python installations, while the system Python 3.10 was available but not the default.

### 3. Virtual Environment Creation Failure

**Issue**: Standard venv creation failed with SSL/library errors

**Error**: 
```
Error: Command '[...ensurepip...] returned non-zero exit status 127
```

**Root Cause**: System Python installation had missing or broken components, particularly SSL libraries.

**Solution Applied**: Used conda environment instead of venv
```bash
# Instead of: python3 -m venv valkey-stress-env
conda create -n valkey-stress python=3.10 -y
conda activate valkey-stress
```

**Why This Solution**: Conda provides a complete, self-contained Python environment with all necessary libraries and dependencies.

### 4. Package Installation Issues

**Issue**: Initial pip install failed with SSL warnings

**Error**: 
```
WARNING: pip is configured with locations that require TLS/SSL, 
however the ssl module in Python is not available
```

**Solution Applied**: Used conda environment's Python and pip
```bash
conda activate valkey-stress
pip install -r requirements.txt  # Worked without SSL issues
```

**Why This Solution**: Conda's Python is compiled with proper SSL support and includes all necessary certificates.

### 5. Project Installation Failure

**Issue**: `pip install -e .` failed initially due to running in wrong directory

**Error**: 
```
ERROR: file:///local/home/ezvisch/workplace/search-delete-test does not appear to be a Python project
```

**Root Cause**: Command was run from a different directory than the project root.

**Solution Applied**: 
```bash
cd /home/ezvisch/workplace/search_delete_test  # Correct project directory
pip install -e .
```

**Why This Solution**: The `-e` flag requires running from the directory containing `pyproject.toml` or `setup.py`.

### 6. Typer Compatibility Error

**Issue**: CLI failed with typer-related error after installation

**Error**: 
```
TypeError: Parameter.make_metavar() missing 1 required positional argument: 'ctx'
```

**Root Cause**: Project's dependency specification allowed an older typer version (0.9.4) that had compatibility issues with other installed packages.

**Solution Applied**: 
```bash
pip install "typer>=0.12.0"  # Upgraded to 0.16.0
```

**Why This Solution**: Newer typer versions fixed the compatibility issues with click and other CLI dependencies.

### 7. Module Import Issues

**Issue**: Initial attempts to run the CLI failed with import errors

**Error**: 
```
ModuleNotFoundError: No module named 'datasets'
```

**Root Cause**: Python was not finding the project's modules due to incorrect path setup.

**Solutions Applied**:

**Primary**: Install in editable mode
```bash
pip install -e .
```

**Alternative**: Set PYTHONPATH manually
```bash
export PYTHONPATH="/home/ezvisch/workplace/search_delete_test/src:$PYTHONPATH"
```

**Why These Solutions**: Editable installation adds the project to Python's site-packages, while PYTHONPATH modification allows direct module imports.

## Key Lessons Learned

### 1. ARM64 Considerations

- **Use Miniforge instead of standard conda/miniconda** for ARM64 systems
- **Conda-forge packages** generally have better ARM64 support
- **Some packages may need compilation** from source on ARM64

### 2. Amazon Linux 2 Specifics

- **System Python may be incomplete** (missing SSL, ensurepip issues)
- **Multiple Python versions** may be available but not default
- **Package managers** (yum) may have limited Python package support

### 3. Environment Isolation Importance

- **Always use virtual environments** or conda environments
- **System Python modifications** can cause issues
- **Isolated environments** prevent dependency conflicts

### 4. Dependency Management

- **Version pinning** in requirements can cause compatibility issues
- **Flexible version ranges** may be better for different architectures
- **Testing on target architecture** is crucial

## Recommended Installation Flow

Based on our experience, here's the recommended installation sequence:

### Step 1: System Check
```bash
# Check system information
uname -m                    # Verify ARM64
cat /etc/os-release        # Verify Amazon Linux 2
which python3.10          # Check for Python 3.10
```

### Step 2: Install Conda (if needed)
```bash
# Download Miniforge for ARM64
wget https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-Linux-aarch64.sh

# Install Miniforge
bash Miniforge3-Linux-aarch64.sh -b -p $HOME/miniforge3

# Initialize shell
$HOME/miniforge3/bin/conda init zsh
source ~/.zshrc
```

### Step 3: Create Environment
```bash
# Create conda environment with Python 3.10
conda create -n valkey-stress python=3.10 -y
conda activate valkey-stress
```

### Step 4: Install Project
```bash
# Navigate to project
cd /path/to/search_delete_test

# Install dependencies
pip install -r requirements.txt

# Install project
pip install -e .

# Fix typer if needed
pip install "typer>=0.12.0"
```

### Step 5: Verify Installation
```bash
# Test CLI
vst --help
vst version
vst validate config config/default.yaml
```

## Prevention Strategies

### For Future Installations

1. **Document system requirements** clearly (Python 3.10+, architecture notes)
2. **Provide architecture-specific instructions** (ARM64 vs x86_64)
3. **Test on target platforms** before release
4. **Use flexible dependency versions** where possible
5. **Provide multiple installation methods** (conda, pip, docker)

### For Users

1. **Check system compatibility** before starting
2. **Use recommended tools** (Miniforge for ARM64)
3. **Follow platform-specific guides** when available
4. **Keep installation logs** for troubleshooting
5. **Test basic functionality** after each major step

## Quick Reference Commands

### Diagnostic Commands
```bash
# System info
uname -a && cat /etc/os-release

# Python availability
which python3 && python3 --version
which python3.10 && python3.10 --version

# Conda status
conda --version && conda info

# Project verification
cd /path/to/project && python verify_installation.py
```

### Recovery Commands
```bash
# Clean conda installation
conda env remove -n valkey-stress -y
conda clean --all

# Fresh installation
conda create -n valkey-stress python=3.10 -y
conda activate valkey-stress
pip install -r requirements.txt
pip install -e .
```

---

This summary captures the real issues we encountered and their practical solutions. It should help future users avoid similar problems and provide quick resolution paths when issues arise.
