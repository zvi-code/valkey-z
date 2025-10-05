# tests/__init__.py
"""Test package for search_delete_test."""

# Ensure the package can be imported
import sys
from pathlib import Path

# Add the src directory to Python path for test imports
src_path = Path(__file__).parent.parent / "src"
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))