# Quick Installation Guide

## Step 1: Check Your System

```bash
git clone https://github.com/zvi-code/search_delete_test.git
cd src
python3 setup_check.py
```

## Step 2: Follow the Recommended Path

### ✅ If you have Python 3.10+

```bash
# Create virtual environment (recommended)
python3 -m venv venv
source venv/bin/activate

# Install
pip install -r requirements.txt
pip install -e .

# Verify
python verify_installation.py
```

### ❌ If you have Python < 3.10

**Option A: Upgrade Python with pyenv (Recommended)**
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

# Then install the package
pip install -r requirements.txt
pip install -e .
```

**Option B: Use conda**
```bash
# Create environment with Python 3.10
conda create -n search_delete_test python=3.10
conda activate search_delete_test

# Install package
pip install -r requirements.txt
pip install -e .
```

**Option C: Use Docker**
```bash
# Run in Docker container
docker run -it --rm -v $(pwd):/workspace python:3.10-slim bash
cd /workspace
pip install -r requirements.txt
pip install -e .
```

## Step 3: Test Installation

```bash
# Check if CLI works
vst --help

# Run verification script
python verify_installation.py

# Test with a simple scenario
vst run examples/config/development.yaml
```

## Common Issues

### `vst` command not found
```bash
pip install -e .  # Reinstall in editable mode
```

### Missing dependencies
```bash
pip install -r requirements.txt  # Reinstall dependencies
```

### AWS EC2 / Cloud Server Issues
See [INSTALL.md](INSTALL.md#aws-ec2-installation) for cloud-specific instructions.

---

For complete documentation: [INSTALL.md](INSTALL.md)
