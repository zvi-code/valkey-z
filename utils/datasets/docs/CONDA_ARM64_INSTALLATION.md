# Conda Installation Guide for Amazon Linux 2 ARM64

This guide provides detailed instructions for installing conda on Amazon Linux 2 with ARM64 architecture (aarch64), specifically focusing on the best practices and options available.

## Overview

Amazon Linux 2 on ARM64 requires special consideration for conda installation since not all conda distributions provide full ARM64 support. This guide covers the recommended approaches and troubleshooting steps.

## System Requirements

- **OS**: Amazon Linux 2
- **Architecture**: ARM64 (aarch64)
- **Memory**: Minimum 2GB RAM recommended
- **Disk Space**: At least 3GB free space
- **Network**: Internet connection for downloads

## Pre-Installation Check

Verify your system before starting:

```bash
# Check OS version
cat /etc/os-release
# Should show Amazon Linux 2

# Check architecture
uname -m
# Should show aarch64

# Check available space
df -h /home
# Ensure sufficient space

# Check internet connectivity
curl -I https://github.com/conda-forge/miniforge/releases/latest
# Should return HTTP 200
```

## Installation Options

### Option 1: Miniforge (Recommended)

Miniforge is the best choice for ARM64 systems as it's specifically built for this architecture and includes conda-forge by default.

#### Why Miniforge?

- ✅ **Native ARM64 support**
- ✅ **Conda-forge by default** (better package availability)
- ✅ **Smaller download size**
- ✅ **Active ARM64 maintenance**
- ✅ **Community-driven**
- ✅ **Compatible with conda commands**

#### Installation Steps

```bash
# 1. Download Miniforge for ARM64
wget https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-Linux-aarch64.sh

# 2. Verify download (optional but recommended)
sha256sum Miniforge3-Linux-aarch64.sh
# Compare with published checksums at: https://github.com/conda-forge/miniforge/releases/latest

# 3. Make executable
chmod +x Miniforge3-Linux-aarch64.sh

# 4. Run installer
bash Miniforge3-Linux-aarch64.sh
```

#### Interactive Installation

The installer will prompt you for:

1. **License Agreement**: Press ENTER to scroll, type 'yes' to accept
2. **Installation Location**: Default is `$HOME/miniforge3` (recommended)
3. **Initialize conda**: Choose 'yes' to modify your shell profile

#### Batch Installation (Non-Interactive)

For automated installations:

```bash
# Install in batch mode
bash Miniforge3-Linux-aarch64.sh -b -p $HOME/miniforge3

# Initialize conda manually
$HOME/miniforge3/bin/conda init zsh  # or bash

# Reload shell
source ~/.zshrc  # or ~/.bashrc
```

#### Post-Installation Verification

```bash
# Check conda installation
conda --version
# Should show: conda 25.3.0 (or later)

# Check default channels
conda config --show channels
# Should include conda-forge

# Test conda functionality
conda create -n test python=3.10 -y
conda activate test
python --version
conda deactivate
conda env remove -n test -y
```

### Option 2: Mambaforge (Alternative)

Mambaforge includes mamba (faster conda) and is also ARM64-compatible:

```bash
# Download Mambaforge
wget https://github.com/conda-forge/miniforge/releases/latest/download/Mambaforge-Linux-aarch64.sh

# Install
bash Mambaforge-Linux-aarch64.sh -b -p $HOME/mambaforge

# Initialize
$HOME/mambaforge/bin/conda init zsh
source ~/.zshrc
```

### Option 3: Miniconda (Limited ARM64 Support)

⚠️ **Not recommended for ARM64** - Limited package availability

```bash
# Only if Miniforge is not suitable
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-aarch64.sh
bash Miniconda3-latest-Linux-aarch64.sh
```

## Shell Configuration

### For Zsh (Default on newer systems)

```bash
# Add to ~/.zshrc (conda init does this automatically)
# >>> conda initialize >>>
# !! Contents within this block are managed by 'conda init' !!
__conda_setup="$('/home/username/miniforge3/bin/conda' 'shell.zsh' 'hook' 2> /dev/null)"
if [ $? -eq 0 ]; then
    eval "$__conda_setup"
else
    if [ -f "/home/username/miniforge3/etc/profile.d/conda.sh" ]; then
        . "/home/username/miniforge3/etc/profile.d/conda.sh"
    else
        export PATH="/home/username/miniforge3/bin:$PATH"
    fi
fi
unset __conda_setup
# <<< conda initialize <<<
```

### For Bash

```bash
# Add to ~/.bashrc (conda init does this automatically)
# Similar block but for bash shell
```

### Manual PATH Configuration

If you prefer not to auto-activate base environment:

```bash
# Add to shell profile
export PATH="$HOME/miniforge3/bin:$PATH"

# Disable auto-activation of base environment
conda config --set auto_activate_base false
```

## Configuration and Optimization

### Essential Configuration

```bash
# Set conda-forge as priority channel (usually default in Miniforge)
conda config --add channels conda-forge
conda config --set channel_priority strict

# Disable automatic base environment activation (optional)
conda config --set auto_activate_base false

# Set solver to libmamba for faster operations (if available)
conda config --set solver libmamba
```

### Performance Optimization for ARM64

```bash
# Set optimal thread count based on CPU cores
export CONDA_THREADS=$(nproc)

# Configure package cache
conda config --set pkgs_dirs ~/.conda/pkgs

# Enable package caching
conda config --set use_pip true
```

## Common Installation Issues

### Issue 1: Download Fails

```bash
# Error: Connection timeout or 404
wget https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-Linux-aarch64.sh

# Solution: Use alternative download methods
curl -L -O https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-Linux-aarch64.sh

# Or use specific version
wget https://github.com/conda-forge/miniforge/releases/download/25.3.0-3/Miniforge3-Linux-aarch64.sh
```

### Issue 2: Permission Denied

```bash
# Error: Permission denied during installation

# Solution: Ensure proper permissions
chmod +x Miniforge3-Linux-aarch64.sh

# Install to user directory (no sudo needed)
bash Miniforge3-Linux-aarch64.sh -b -p $HOME/miniforge3
```

### Issue 3: Shell Not Initialized

```bash
# Error: conda command not found after installation

# Solution: Manual initialization
$HOME/miniforge3/bin/conda init zsh
source ~/.zshrc

# Or add to PATH manually
echo 'export PATH="$HOME/miniforge3/bin:$PATH"' >> ~/.zshrc
source ~/.zshrc
```

### Issue 4: Conflicts with Existing Python

```bash
# Error: Conda python conflicts with system python

# Solution: Use environments to isolate
conda create -n myproject python=3.10
conda activate myproject
# Now python refers to conda python
```

## Environment Management

### Creating Environments

```bash
# Create environment with specific Python version
conda create -n myenv python=3.10

# Create with multiple packages
conda create -n myenv python=3.10 numpy pandas

# Create from environment.yml file
conda env create -f environment.yml
```

### Managing Environments

```bash
# List environments
conda env list
conda info --envs

# Activate environment
conda activate myenv

# Deactivate environment
conda deactivate

# Remove environment
conda env remove -n myenv

# Clone environment
conda create --clone myenv --name myenv_backup
```

### Exporting Environments

```bash
# Export current environment
conda env export > environment.yml

# Export with specific name
conda env export -n myenv > myenv.yml

# Export only non-default packages
conda env export --from-history > environment.yml
```

## Package Management

### Installing Packages

```bash
# Install from conda-forge (preferred for ARM64)
conda install -c conda-forge package_name

# Install specific version
conda install package_name=1.2.3

# Install multiple packages
conda install numpy pandas matplotlib

# Use pip when conda package not available
pip install package_name
```

### Package Channels for ARM64

```bash
# Recommended channel priority for ARM64
conda config --add channels conda-forge
conda config --add channels defaults
conda config --set channel_priority strict

# Check available packages for ARM64
conda search package_name

# Force specific architecture (if needed)
conda install package_name[build=*aarch64*]
```

## Maintenance and Updates

### Updating Conda

```bash
# Update conda itself
conda update conda

# Update all packages in current environment
conda update --all

# Update specific package
conda update package_name
```

### Cleaning Up

```bash
# Clean package cache
conda clean --all

# Remove unused packages
conda clean --packages

# Remove index cache
conda clean --index-cache
```

### Health Check

```bash
# Verify conda installation
conda info

# Check for inconsistencies
conda info --envs
conda list --show-channel-urls
```

## Uninstallation

If you need to remove conda completely:

```bash
# 1. Remove conda initialization from shell profile
# Edit ~/.zshrc and remove conda init block

# 2. Remove conda directory
rm -rf $HOME/miniforge3

# 3. Remove conda configuration
rm -rf ~/.conda

# 4. Remove conda from PATH
# Edit shell profile and remove PATH exports

# 5. Restart shell
exec zsh  # or exec bash
```

## Troubleshooting Tips

### Debug Information

```bash
# Get detailed conda information
conda info
conda info --envs
conda config --show

# Check package sources
conda list --show-channel-urls

# Verbose installation for debugging
conda install -v package_name
```

### Common Commands Reference

```bash
# Installation and setup
conda --version                    # Check version
conda info                        # System info
conda config --show               # Show configuration

# Environment management
conda env list                    # List environments
conda create -n NAME python=3.10  # Create environment
conda activate NAME               # Activate environment
conda deactivate                  # Deactivate environment
conda env remove -n NAME          # Remove environment

# Package management
conda list                        # List packages
conda search PACKAGE              # Search package
conda install PACKAGE             # Install package
conda update PACKAGE              # Update package
conda remove PACKAGE              # Remove package

# Maintenance
conda update conda               # Update conda
conda clean --all                # Clean cache
```

## Best Practices

1. **Always use environments** - Don't install packages in base environment
2. **Use conda-forge** - Better ARM64 package availability
3. **Pin important packages** - Prevent unwanted updates
4. **Regular cleanup** - Remove unused packages and cache
5. **Export environments** - Keep backup of working environments
6. **Use specific versions** - For reproducible environments

## Integration with Development Tools

### VS Code Integration

```bash
# Install VS Code Python extension
# conda environments will be automatically detected

# Set Python interpreter to conda environment
# Ctrl+Shift+P -> "Python: Select Interpreter"
# Choose: ~/miniforge3/envs/myenv/bin/python
```

### Jupyter Integration

```bash
# Install Jupyter in environment
conda activate myenv
conda install jupyter ipykernel

# Add environment to Jupyter
python -m ipykernel install --user --name myenv --display-name "Python (myenv)"
```

---

This comprehensive guide should help you successfully install and configure conda on Amazon Linux 2 ARM64 systems. The key is using Miniforge for the best ARM64 compatibility and following the recommended practices for environment management.
