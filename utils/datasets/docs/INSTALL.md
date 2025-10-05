# Installation Guide

This comprehensive guide provides step-by-step instructions for installing the Valkey Search Delete Stress Test tool on any platform.

> **⚠️ Important**: This package is currently in development and not available on PyPI. You must install from source using this guide.

## Prerequisites

- **Python 3.10 or higher** - Check with `python3 --version`
- **Git** - For cloning the repository
- **Valkey instance** - With Search module enabled (optional for installation)

## Quick System Check

Before installation, check your system compatibility:

```bash
# Clone the repository first
git clone https://github.com/zvi-code/search_delete_test.git
cd search_delete_test

# Run system check
python3 setup_check.py
```

This script will analyze your system and recommend the best installation method.

## Installation Methods

### Method 1: Simple pip Installation (Recommended)

**Use if you have Python 3.10+**

```bash
# 1. Clone the repository
git clone https://github.com/zvi-code/search_delete_test.git
cd search_delete_test

# 2. Create virtual environment (recommended)
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Install the package in development mode
pip install -e .

# 5. Verify installation
python verify_installation.py
```

**Expected output:**
```
✓ Python 3.10+ - OK
✓ search_delete_test package - OK
✓ Dependencies - OK
✓ vst command - OK
✓ Configuration files - OK
🎉 ALL CHECKS PASSED!
```

### Method 2: AWS EC2 / Cloud Server Installation

**For Amazon Linux, Ubuntu, CentOS, and other cloud servers**

#### Amazon Linux 2/2023 (Automated)

```bash
# Quick setup with automated script
curl -s https://raw.githubusercontent.com/zvi-code/search_delete_test/main/setup_ec2.sh | bash
```

#### Manual Installation (All Cloud Servers)

```bash
# 1. Update system packages
sudo yum update -y  # Amazon Linux
# or: sudo apt update && sudo apt upgrade -y  # Ubuntu

# 2. Install basic dependencies
sudo yum install git gcc openssl-devel libffi-devel bzip2-devel -y  # Amazon Linux
# or: sudo apt install git build-essential libssl-dev libffi-dev libbz2-dev -y  # Ubuntu

# 3. Install Python 3.10+ (if not available)
# For Amazon Linux (usually has Python 3.9):
sudo yum groupinstall "Development Tools" -y
curl https://pyenv.run | bash
echo 'export PATH="$HOME/.pyenv/bin:$PATH"' >> ~/.bashrc
echo 'eval "$(pyenv init -)"' >> ~/.bashrc
source ~/.bashrc
pyenv install 3.10.14
pyenv global 3.10.14

# For Ubuntu 20.04+ (may need Python 3.10):
sudo apt install software-properties-common -y
sudo add-apt-repository ppa:deadsnakes/ppa -y
sudo apt update
sudo apt install python3.10 python3.10-venv python3.10-pip -y

# 4. Create working directory
mkdir -p ~/search_delete_test
cd ~/search_delete_test

# 5. Clone and install
git clone https://github.com/zvi-code/search_delete_test.git
cd search_delete_test

# 6. Create virtual environment
python3.10 -m venv venv  # Use python3.10 specifically
source venv/bin/activate

# 7. Install dependencies and package
pip install -r requirements.txt
pip install -e .

# 8. Verify installation
python verify_installation.py
```

### Method 3: Poetry Installation (For Developers)

```bash
# 1. Install Poetry (if not already installed)
curl -sSL https://install.python-poetry.org | python3 -

# 2. Clone and setup
git clone https://github.com/zvi-code/search_delete_test.git
cd search_delete_test

# 3. Install dependencies and package
poetry install

# Optional: Install with visualization dependencies
poetry install --extras visualization

# 4. Activate the environment
poetry shell

# 5. Verify installation
vst --help
```

### Method 4: Docker Installation

```bash
# Option A: Run in Docker container
docker run -it --rm -v $(pwd):/workspace python:3.10-slim bash
cd /workspace
git clone https://github.com/zvi-code/search_delete_test.git
cd search_delete_test
pip install -r requirements.txt
pip install -e .
python verify_installation.py

# Option B: Build custom Docker image
cat > Dockerfile << 'EOF'
FROM python:3.10-slim
WORKDIR /app
RUN apt-get update && apt-get install -y git
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
RUN pip install -e .
CMD ["vst", "--help"]
EOF

docker build -t search_delete_test .
docker run -it search_delete_test
```

### Method 5: pyenv Installation (Version Management)

**Use when you need to manage multiple Python versions**

```bash
# 1. Install pyenv
curl https://pyenv.run | bash

# 2. Add to your shell profile
echo 'export PATH="$HOME/.pyenv/bin:$PATH"' >> ~/.bashrc  # or ~/.zshrc
echo 'eval "$(pyenv init -)"' >> ~/.bashrc
echo 'eval "$(pyenv virtualenv-init -)"' >> ~/.bashrc

# 3. Reload shell
source ~/.bashrc  # or restart terminal

# 4. Install Python 3.10+
pyenv install 3.10.14
pyenv global 3.10.14

# 5. Verify Python version
python --version  # Should show 3.10.14

# 6. Continue with Method 1 installation
git clone https://github.com/zvi-code/search_delete_test.git
cd search_delete_test
pip install -r requirements.txt
pip install -e .
python verify_installation.py
```

### Method 6: conda Installation (Data Science Users)

```bash
# 1. Create conda environment with Python 3.10
conda create -n search_delete_test python=3.10
conda activate search_delete_test

# 2. Clone and install
git clone https://github.com/zvi-code/search_delete_test.git
cd search_delete_test
pip install -r requirements.txt
pip install -e .

# 3. Verify installation
python verify_installation.py
```

## Python Version Upgrade

If you have Python < 3.10, here are your options:

### Option A: Using pyenv (Recommended - Works on all platforms)

```bash
# Install pyenv
curl https://pyenv.run | bash

# Add to your shell profile
echo 'export PATH="$HOME/.pyenv/bin:$PATH"' >> ~/.bashrc  # or ~/.zshrc
echo 'eval "$(pyenv init -)"' >> ~/.bashrc
echo 'eval "$(pyenv virtualenv-init -)"' >> ~/.bashrc

# Reload shell
source ~/.bashrc  # or restart terminal

# Install Python 3.10+
pyenv install 3.10.14
pyenv local 3.10.14  # For this project only
# or: pyenv global 3.10.14  # For all projects
```

### Option B: Using conda

```bash
# Install miniconda if not already installed
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
bash Miniconda3-latest-Linux-x86_64.sh

# Create environment with Python 3.10
conda create -n search_delete_test python=3.10
conda activate search_delete_test
```

### Option C: Using Docker

```bash
# Use Docker with Python 3.10
docker run -it --rm -v $(pwd):/workspace python:3.10-slim bash
cd /workspace
# Continue with installation...
```

### Option D: System Package Manager

**Ubuntu 20.04+:**
```bash
sudo apt install software-properties-common
sudo add-apt-repository ppa:deadsnakes/ppa
sudo apt update
sudo apt install python3.10 python3.10-venv python3.10-pip
```

**CentOS/RHEL 8+:**
```bash
sudo dnf install python3.10 python3.10-pip python3.10-venv
```

**macOS (using Homebrew):**
```bash
brew install python@3.10
```

## Platform-Specific Installation

### Amazon Linux 2 (ARM/x86_64)

**Recommended approach for Amazon Linux 2, especially on ARM EC2 instances:**

```bash
# 1. Update system
sudo yum update -y

# 2. Install build dependencies (if using pyenv)
sudo yum install -y gcc gcc-c++ make git curl bzip2-devel libffi-devel openssl-devel readline-devel sqlite-devel zlib-devel tk-devel

# 3. Option A: Use conda/miniforge (Recommended for ARM)
# Check if conda is available
which conda

# If conda is available, create Python 3.10 environment
conda create -n search_delete_test_py310 python=3.10 -y
conda activate search_delete_test_py310

# Verify Python version
python --version  # Should show Python 3.10.x

# 4. Clone and install the project
git clone https://github.com/zvi-code/search_delete_test.git
cd search_delete_test

# 5. Install package
pip install -e .

# 6. Verify installation
vst --version

# Option B: Use pyenv (if conda not available)
# Install pyenv
curl https://pyenv.run | bash

# Add to shell profile (~/.zshrc or ~/.bashrc)
echo 'export PYENV_ROOT="$HOME/.pyenv"' >> ~/.zshrc
echo 'export PATH="$HOME/.pyenv/bin:$PATH"' >> ~/.zshrc
echo 'eval "$(pyenv init --path)"' >> ~/.zshrc
echo 'eval "$(pyenv init -)"' >> ~/.zshrc

# Reload shell
source ~/.zshrc  # or restart terminal

# Install Python 3.10 with SSL support
CONFIGURE_OPTS="--with-openssl=/usr" pyenv install 3.10.14
pyenv local 3.10.14

# Continue with installation
git clone https://github.com/zvi-code/search_delete_test.git
cd search_delete_test
pip install -e .
```

### macOS

```bash
# 1. Install Homebrew (if not already installed)
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

# 2. Install Python 3.10+
brew install python@3.10

# 3. Install Git (if needed)
brew install git

# 4. Continue with Method 1
git clone https://github.com/zvi-code/search_delete_test.git
cd search_delete_test
python3.10 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
pip install -e .
python verify_installation.py
```

### Windows (WSL Recommended)

```bash
# 1. Enable WSL and install Ubuntu
wsl --install -d Ubuntu

# 2. Update Ubuntu
sudo apt update && sudo apt upgrade -y

# 3. Install Python 3.10
sudo apt install python3.10 python3.10-venv python3.10-pip git -y

# 4. Continue with Method 1
git clone https://github.com/zvi-code/search_delete_test.git
cd search_delete_test
python3.10 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
pip install -e .
python verify_installation.py
```

### Windows (Native)

```powershell
# 1. Install Python 3.10+ from python.org
# Download and install from: https://www.python.org/downloads/

# 2. Install Git
# Download from: https://git-scm.com/download/win

# 3. Open Command Prompt or PowerShell
git clone https://github.com/zvi-code/search_delete_test.git
cd search_delete_test
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
pip install -e .
python verify_installation.py
```

## Verification and Testing

### Installation Verification

```bash
# Run the verification script
python verify_installation.py

# Check CLI command
vst --help

# Test basic functionality
vst info system
```

### Test Installation

```bash
# Run unit tests (no external dependencies)
python run_tests.py

# Run with coverage report
python run_tests.py --coverage

# Quick shell script version
./run_tests.sh

# Test with dry run
vst run quick --duration 30 --dry-run
```

## Setting Up Valkey

The stress test tool requires a Valkey or Valkey instance with the Search module.

### Option 1: Docker (Recommended)

```bash
# Valkey Stack (includes Search module)
docker run -d --name valkey-stack \
  -p 6379:6379 \
  valkey/valkey-stack-server:latest

# Verify connection
vst info valkey
```

### Option 2: Manual Installation

- **Valkey Stack**: Follow [Valkey Stack installation guide](https://valkey.io/docs/stack/get-started/install/)
- **Valkey**: Follow [Valkey installation guide](https://valkey.io/docs/intro/)

## Troubleshooting

### Common Issues

#### `vst` command not found

**Cause**: Package not installed in editable mode or PATH issues.

**Solutions:**
```bash
# 1. Reinstall in editable mode
pip install -e .

# 2. Check if ~/.local/bin is in PATH
echo $PATH | grep ~/.local/bin

# 3. Add to PATH if missing (add to ~/.bashrc or ~/.zshrc)
export PATH="$HOME/.local/bin:$PATH"

# 4. Restart terminal or source profile
source ~/.bashrc
```

#### `No module named 'search_delete_test'`

**Cause**: Package not properly installed.

**Solutions:**
```bash
# 1. Check current directory
pwd  # Should be in search_delete_test directory

# 2. Reinstall dependencies and package
pip install -r requirements.txt
pip install -e .

# 3. Check installation
pip list | grep search-delete-test
```

#### Python Version Issues

**Cause**: Python version < 3.10 not supported.

**Solutions:**
```bash
# 1. Check Python version
python3 --version

# 2. Use pyenv to install Python 3.10+
curl https://pyenv.run | bash
pyenv install 3.10.14
pyenv local 3.10.14

# 3. Or use conda
conda create -n search_delete_test python=3.10
conda activate search_delete_test
```

#### pip PATH Warning Issues (Amazon Linux 2 ARM)

**Issue**: `WARNING: The scripts pip3 and pip3.10 are installed in '/home/user/.pyenv/versions/3.10.14/bin' which is not on PATH`

**Cause**: This warning appears when pip attempts to install to a pyenv directory that doesn't exist or when there are multiple Python installations.

**Solutions for Amazon Linux 2 ARM:**

```bash
# Option 1: Use conda (Recommended for Amazon Linux 2 ARM)
# 1. Check if conda is available
which conda

# 2. Create Python 3.10 environment
conda create -n python310 python=3.10 -y
conda activate python310

# 3. Verify Python version
python --version  # Should show Python 3.10.x

# 4. Install your project
pip install -e .

# Option 2: Fix pyenv installation (if using pyenv)
# 1. Install build dependencies
sudo yum install -y gcc gcc-c++ make git curl bzip2-devel libffi-devel openssl-devel readline-devel sqlite-devel zlib-devel tk-devel

# 2. Install Python 3.10 with proper SSL configuration
CONFIGURE_OPTS="--with-openssl=/usr" pyenv install 3.10.14

# 3. Set as local version
pyenv local 3.10.14

# Option 3: Use existing Python and virtual environment
# 1. Check current Python
python3 --version

# 2. If >= 3.9, create virtual environment
python3 -m venv venv310
source venv310/bin/activate

# 3. Upgrade pip and install
pip install --upgrade pip
pip install -e .
```

**Valkey Version Issues:**

**Issue**: `ERROR: No matching distribution found for valkey<6.0.0,>=5.0.0`

**Cause**: Valkey 5.x stable versions don't exist - only beta versions.

**Solution:**
```bash
# Update pyproject.toml to use valkey 6.x
# Change: valkey = "^5.0.0"
# To:     valkey = "^6.0.0"

# Then reinstall
pip install -e .
```

**Typer Compatibility Issues:**

**Issue**: `TypeError: Parameter.make_metavar() missing 1 required positional argument: 'ctx'`

**Cause**: Version conflict between typer and click.

**Solution:**
```bash
# Update typer to newer version
pip install "typer>=0.12.0"

# Update pyproject.toml
# Change: typer = "^0.9.0"
# To:     typer = ">=0.9.0"

# Reinstall package
pip install -e .
```

#### Permission Errors

**Cause**: Insufficient permissions for installation.

**Solutions:**
```bash
# 1. Use virtual environment (recommended)
python3 -m venv venv
source venv/bin/activate

# 2. Or install for user only
pip install --user -r requirements.txt
pip install --user -e .

# 3. Check directory permissions
ls -la ~/.local/bin/
```

#### AWS EC2 Installation Failures

**Specific Issues and Solutions:**

**Issue**: `ensurepip` module not found
```bash
# Solution: Install python3-pip
sudo yum install python3-pip -y  # Amazon Linux
sudo apt install python3-pip -y  # Ubuntu
```

**Issue**: Compilation errors
```bash
# Solution: Install development tools
sudo yum groupinstall "Development Tools" -y  # Amazon Linux
sudo apt install build-essential -y  # Ubuntu
```

**Issue**: SSL/TLS errors
```bash
# Solution: Update certificates
sudo yum update ca-certificates -y  # Amazon Linux
sudo apt update && sudo apt install ca-certificates -y  # Ubuntu
```

#### Memory/Resource Issues

**Issue**: Installation fails due to memory constraints

**Solutions:**
```bash
# 1. Use smaller installation
pip install -r requirements.txt --no-cache-dir

# 2. Install dependencies one by one
pip install numpy
pip install valkey
pip install pyyaml
# ... continue with each dependency

# 3. Use swap file (Linux)
sudo fallocate -l 2G /swapfile
sudo chmod 600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile
```

### Environment Variables

You can set these environment variables to customize installation:

```bash
# Custom installation paths
export VST_CONFIG_FILE="/path/to/config.yaml"
export VST_OUTPUT_DIR="/path/to/output"

# Valkey connection defaults
export VST_VALKEY_HOST="localhost"
export VST_VALKEY_PORT="6379"

# Installation options
export PIP_CACHE_DIR="/tmp/pip-cache"
export PYTHONPATH="/path/to/additional/modules"
```

## Quick Reference: Common Solutions

### "pip3 not on PATH" Warning
```bash
# Use conda (Amazon Linux 2 ARM recommended)
conda create -n myproject python=3.10 -y
conda activate myproject
```

### "No matching distribution found for valkey<6.0.0"
```bash
# Update pyproject.toml: valkey = "^6.0.0"
pip install -e .
```

### "TypeError: Parameter.make_metavar() missing argument"
```bash
# Update typer version
pip install "typer>=0.12.0"
```

### SSL Module Issues (pyenv)
```bash
# Install with SSL support
CONFIGURE_OPTS="--with-openssl=/usr" pyenv install 3.10.14
```

## Installation Summary

Choose the method that best fits your environment:

| Method | Best For | Difficulty | Requirements |
|--------|----------|------------|--------------|
| **Simple pip** | Most users | Easy | Python 3.10+ |
| **Cloud/EC2** | Servers | Medium | Any (auto-upgrade) |
| **Poetry** | Developers | Medium | Python 3.10+ |
| **Docker** | Isolation | Easy | Docker installed |
| **pyenv** | Version management | Medium | Any |
| **conda** | Data science | Easy | Anaconda/Miniconda |

After successful installation, proceed to the [Quick Start](README.md#quick-start) section or explore the [Commands Guide](COMMANDS.md) for detailed CLI usage.
