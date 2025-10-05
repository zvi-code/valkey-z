#!/usr/bin/env python3
"""
Setup file for search_delete_test package.

Install in development mode:
    pip install -e .

Install with test dependencies:
    pip install -e ".[test]"
"""

from setuptools import setup, find_packages

setup(
    name="search_delete_test",
    version="0.1.0",
    description="Memory stress testing tool for Valkey-Search with vector operations",
    author="Your Name",
    author_email="your.email@example.com",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    python_requires=">=3.10",
    install_requires=[
        "numpy>=1.24.0",
        "valkey>=6.0.0",
        "psutil>=5.9.0",
        "prometheus-client>=0.19.0",
        "typer>=0.9.0",
        "pyyaml>=6.0",
        "h5py>=3.10.0",
        "aiofiles>=23.0",
        "pandas>=2.0.0",
        "rich>=13.0.0",
    ],
    extras_require={
        "test": [
            "pytest>=7.4.0",
            "pytest-asyncio>=0.21.0",
            "pytest-cov>=4.1.0",
            "pytest-mock>=3.14.0",
        ],
        "dev": [
            "black>=23.0.0",
            "ruff>=0.1.0",
            "mypy>=1.7.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "vst=search_delete_test.cli.main:app",
        ],
    },
)