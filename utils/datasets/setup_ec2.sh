#!/bin/bash

# Amazon Linux EC2 Setup Script for Valkey Search Delete Stress Test
# This script installs Python 3.10+ and the search_delete_test package

set -e

echo "🚀 Setting up Valkey Search Delete Stress Test on Amazon Linux EC2..."

# Check if we're on Amazon Linux
if ! grep -q "Amazon Linux" /etc/os-release 2>/dev/null; then
    echo "⚠️  This script is designed for Amazon Linux. Use INSTALL.md for other systems."
    exit 1
fi

# Check current Python version
current_python=$(python3 --version 2>/dev/null | cut -d' ' -f2 || echo "not found")
echo "Current Python version: $current_python"

# Function to install via pyenv (recommended)
install_with_pyenv() {
    echo "📦 Installing Python 3.10+ with pyenv..."
    
    # Install dependencies
    sudo yum groupinstall "Development Tools" -y
    sudo yum install git gcc openssl-devel libffi-devel bzip2-devel readline-devel sqlite-devel -y
    
    # Install pyenv
    if ! command -v pyenv &> /dev/null; then
        curl https://pyenv.run | bash
        
        # Add to bashrc
        echo 'export PATH="$HOME/.pyenv/bin:$PATH"' >> ~/.bashrc
        echo 'eval "$(pyenv init -)"' >> ~/.bashrc
        echo 'eval "$(pyenv virtualenv-init -)"' >> ~/.bashrc
        
        # Load pyenv for current session
        export PATH="$HOME/.pyenv/bin:$PATH"
        eval "$(pyenv init -)"
        eval "$(pyenv virtualenv-init -)"
    fi
    
    # Install Python 3.10.14
    pyenv install 3.10.14
    pyenv global 3.10.14
    
    echo "✓ Python 3.10.14 installed via pyenv"
}

# Function to install via compilation
install_with_compile() {
    echo "📦 Installing Python 3.10+ from source..."
    
    # Install dependencies
    sudo yum groupinstall "Development Tools" -y
    sudo yum install openssl-devel libffi-devel bzip2-devel readline-devel sqlite-devel -y
    
    # Download and compile Python 3.10
    cd /tmp
    wget https://www.python.org/ftp/python/3.10.14/Python-3.10.14.tgz
    tar xvf Python-3.10.14.tgz
    cd Python-3.10.14
    ./configure --enable-optimizations --prefix=/usr/local
    make -j $(nproc)
    sudo make altinstall
    
    # Create symlinks for easy access
    sudo ln -sf /usr/local/bin/python3.10 /usr/local/bin/python3
    sudo ln -sf /usr/local/bin/pip3.10 /usr/local/bin/pip3
    
    echo "✓ Python 3.10.14 compiled and installed"
}

# Check if Python 3.10+ is already available
if python3 -c "import sys; exit(0 if sys.version_info >= (3, 10) else 1)" 2>/dev/null; then
    echo "✓ Python 3.10+ already available"
    python_cmd="python3"
else
    echo "❌ Python 3.10+ required. Current version: $current_python"
    echo ""
    echo "Choose installation method:"
    echo "1) pyenv (recommended, easy to manage multiple versions)"
    echo "2) compile from source (system-wide installation)"
    echo "3) exit and use Docker instead"
    echo ""
    read -p "Enter choice (1-3): " choice
    
    case $choice in
        1)
            install_with_pyenv
            python_cmd="python"
            ;;
        2)
            install_with_compile
            python_cmd="python3"
            ;;
        3)
            echo "To use Docker instead:"
            echo "docker run -it --rm -v \$(pwd):/workspace python:3.10-slim bash"
            exit 0
            ;;
        *)
            echo "Invalid choice. Exiting."
            exit 1
            ;;
    esac
fi

# Verify Python version
new_version=$($python_cmd --version 2>/dev/null | cut -d' ' -f2)
echo "✓ Python version: $new_version"

# Install git if not present
if ! command -v git &> /dev/null; then
    echo "📦 Installing git..."
    sudo yum install git -y
fi

# Install the package
echo "📦 Installing Valkey Search Delete Stress Test package..."

# Create virtual environment (recommended)
$python_cmd -m venv venv
source venv/bin/activate

# Upgrade pip
pip install --upgrade pip

# Install dependencies and package
pip install -r requirements.txt
pip install -e .

# Verify installation
echo "🧪 Verifying installation..."
if command -v vst &> /dev/null; then
    echo "✓ vst command available"
    vst --help > /dev/null && echo "✓ vst command works"
else
    echo "❌ vst command not found"
    echo "Try: source venv/bin/activate"
fi

# Run verification script
if [ -f "verify_installation.py" ]; then
    echo "🔍 Running installation verification..."
    python verify_installation.py
fi

echo ""
echo "🎉 Installation complete!"
echo ""
echo "To use the tool:"
echo "  source venv/bin/activate  # Activate virtual environment"
echo "  vst --help                # Show available commands"
echo ""
echo "To deactivate virtual environment:"
echo "  deactivate"
