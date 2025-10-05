#!/usr/bin/env python3
"""
Quick setup script for Valkey Search Delete Stress Test tool.

This script checks your system and provides guidance for installation.
"""

import sys
import subprocess
import shutil
from pathlib import Path

def check_python_version():
    """Check if Python version meets requirements."""
    version = sys.version_info
    print(f"Current Python version: {version.major}.{version.minor}.{version.micro}")
    
    if version.major == 3 and version.minor >= 10:
        print("✓ Python version is compatible")
        return True
    else:
        print("✗ Python 3.10+ required")
        print("\nSolutions:")
        print("1. Ubuntu/Debian: sudo apt install python3.10")
        print("2. Use pyenv: curl https://pyenv.run | bash && pyenv install 3.10.14")
        print("3. Use conda: conda create -n search_delete_test python=3.10")
        print("4. Use Docker: docker run -it python:3.10-slim")
        return False

def check_git():
    """Check if git is available."""
    if shutil.which("git"):
        print("✓ Git is available")
        return True
    else:
        print("✗ Git not found")
        print("Install git: sudo apt install git (Ubuntu) or brew install git (macOS)")
        return False

def check_pip():
    """Check if pip is available."""
    if shutil.which("pip") or shutil.which("pip3"):
        print("✓ pip is available")
        return True
    else:
        print("✗ pip not found")
        print("Install pip: sudo apt install python3-pip")
        return False

def check_requirements_file():
    """Check if requirements.txt exists."""
    if Path("requirements.txt").exists():
        print("✓ requirements.txt found")
        return True
    else:
        print("✗ requirements.txt not found")
        print("Make sure you're in the search_delete_test directory")
        return False

def suggest_installation_method():
    """Suggest the best installation method based on system."""
    print("\n" + "="*50)
    print("RECOMMENDED INSTALLATION METHOD")
    print("="*50)
    
    version = sys.version_info
    
    if version.major == 3 and version.minor >= 10:
        print("✅ Your Python version is compatible!")
        print("\nRecommended steps:")
        print("1. Create virtual environment: python3 -m venv venv")
        print("2. Activate environment: source venv/bin/activate")
        print("3. Install dependencies: pip install -r requirements.txt")
        print("4. Install package: pip install -e .")
        print("5. Verify: python verify_installation.py")
        
    else:
        print("⚠️  Python version upgrade needed")
        print("\nOption 1 - pyenv (recommended):")
        print("curl https://pyenv.run | bash")
        print("pyenv install 3.10.14")
        print("pyenv global 3.10.14")
        print("python -m venv venv")
        print("source venv/bin/activate")
        print("pip install -r requirements.txt")
        print("pip install -e .")
        
        print("\nOption 2 - conda:")
        print("conda create -n search_delete_test python=3.10")
        print("conda activate search_delete_test")
        print("pip install -r requirements.txt")
        print("pip install -e .")
        
        print("\nOption 3 - Docker:")
        print("docker run -it --rm -v $(pwd):/workspace python:3.10-slim bash")
        print("# Inside container:")
        print("cd /workspace")
        print("apt update && apt install git")
        print("pip install -r requirements.txt")
        print("pip install -e .")

def main():
    """Run system check and provide installation guidance."""
    print("Valkey Search Delete Stress Test - Installation Check")
    print("="*50)
    
    checks = [
        ("Python Version", check_python_version),
        ("Git", check_git),
        ("pip", check_pip),
        ("Requirements File", check_requirements_file),
    ]
    
    all_passed = True
    
    for check_name, check_func in checks:
        print(f"\n{check_name}:")
        try:
            passed = check_func()
            if not passed:
                all_passed = False
        except Exception as e:
            print(f"✗ {check_name} - ERROR: {e}")
            all_passed = False
    
    suggest_installation_method()
    
    print(f"\n{'='*50}")
    if all_passed:
        print("🎉 System ready for installation!")
    else:
        print("⚠️  System needs setup before installation")
    
    print("\nFor detailed instructions, see: INSTALL.md")
    
    return 0 if all_passed else 1

if __name__ == "__main__":
    sys.exit(main())
