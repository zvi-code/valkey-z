#!/usr/bin/env python3
"""
Installation verification script for Valkey Search Delete Stress Test tool.

Run this script after installation to verify everything is working correctly.
"""

import sys
import subprocess
import importlib.util
from pathlib import Path

def check_python_version():
    """Check Python version requirement."""
    version = sys.version_info
    if version.major == 3 and version.minor >= 10:
        print(f"✓ Python {version.major}.{version.minor}.{version.micro} - OK")
        return True
    else:
        print(f"✗ Python {version.major}.{version.minor}.{version.micro} - Requires Python 3.10+")
        print("  Solutions:")
        print("  1. Install Python 3.10+: see INSTALL.md for platform-specific instructions")
        print("  2. Use pyenv: curl https://pyenv.run | bash && pyenv install 3.10.14")
        print("  3. Use conda: conda create -n search_delete_test python=3.10")
        print("  4. Use Docker: docker run -it python:3.10-slim")
        return False

def check_package_installed():
    """Check if search_delete_test package is installed."""
    try:
        spec = importlib.util.find_spec("search_delete_test")
        if spec is not None:
            print(f"✓ search_delete_test package - OK")
            print(f"  Location: {spec.origin}")
            return True
        else:
            print("✗ search_delete_test package - NOT FOUND")
            return False
    except Exception as e:
        print(f"✗ search_delete_test package - ERROR: {e}")
        return False

def check_dependencies():
    """Check required dependencies."""
    required_packages = [
        "numpy", "valkey", "psutil", "prometheus_client", 
        "typer", "yaml", "h5py", "aiofiles", "pandas", "rich"
    ]
    
    missing = []
    for package in required_packages:
        try:
            importlib.import_module(package)
            print(f"✓ {package} - OK")
        except ImportError:
            print(f"✗ {package} - MISSING")
            missing.append(package)
    
    return len(missing) == 0

def check_vst_command():
    """Check if vst command is available."""
    try:
        result = subprocess.run(
            ["vst", "--help"], 
            capture_output=True, 
            text=True, 
            timeout=30
        )
        if result.returncode == 0:
            print("✓ vst command - OK")
            return True
        else:
            print(f"✗ vst command - FAILED (exit code {result.returncode})")
            print(f"  stderr: {result.stderr}")
            return False
    except FileNotFoundError:
        print("✗ vst command - NOT FOUND")
        print("  Make sure you ran: pip install -e .")
        return False
    except subprocess.TimeoutExpired:
        print("✗ vst command - TIMEOUT")
        return False
    except Exception as e:
        print(f"✗ vst command - ERROR: {e}")
        return False

def check_config_files():
    """Check if configuration files exist."""
    config_files = [
        "config/default.yaml",
        "requirements.txt",
        "pyproject.toml"
    ]
    
    all_exist = True
    for config_file in config_files:
        if Path(config_file).exists():
            print(f"✓ {config_file} - OK")
        else:
            print(f"✗ {config_file} - MISSING")
            all_exist = False
    
    return all_exist

def main():
    """Run all verification checks."""
    print("Valkey Search Delete Stress Test - Installation Verification")
    print("=" * 50)
    
    checks = [
        ("Python Version", check_python_version),
        ("Package Installation", check_package_installed),
        ("Dependencies", check_dependencies),
        ("VST Command", check_vst_command),
        ("Configuration Files", check_config_files),
    ]
    
    all_passed = True
    
    for check_name, check_func in checks:
        print(f"\n{check_name}:")
        try:
            passed = check_func()
            if not passed:
                all_passed = False
        except Exception as e:
            print(f"✗ {check_name} - EXCEPTION: {e}")
            all_passed = False
    
    print("\n" + "=" * 50)
    if all_passed:
        print("🎉 ALL CHECKS PASSED!")
        print("\nNext steps:")
        print("1. Start Valkey: docker run -d -p 6379:6379 valkey/valkey-stack-server:latest")
        print("2. Test connection: vst info valkey")
        print("3. Run quick test: vst run quick --duration 60")
        print("4. Read docs: docs/GETTING_STARTED.md")
    else:
        print("❌ SOME CHECKS FAILED!")
        print("\nTroubleshooting:")
        print("1. Check the installation guide: INSTALL.md")
        
        # Check if Python version is the issue
        if sys.version_info.minor < 10:
            print("2. PYTHON VERSION ISSUE - Install Python 3.10+:")
            print("   • Ubuntu: sudo apt install python3.10")
            print("   • pyenv: pyenv install 3.10.14 && pyenv global 3.10.14")
            print("   • conda: conda create -n search_delete_test python=3.10")
            print("   • Docker: docker run -it python:3.10-slim")
        else:
            print("2. Reinstall: pip install -r requirements.txt && pip install -e .")
            print("3. Use virtual environment: python3 -m venv venv && source venv/bin/activate")
        
    return 0 if all_passed else 1

if __name__ == "__main__":
    sys.exit(main())
