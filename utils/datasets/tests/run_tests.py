#!/usr/bin/env python3
"""
Comprehensive test runner for search_delete_test.

This is the main test runner that consolidates all testing functionality.
It properly configures the Python path and provides options for different test types.

Usage:
    python run_tests.py                    # Run all unit tests
    python run_tests.py --type unit        # Run only unit tests
    python run_tests.py --type integration # Run only integration tests
    python run_tests.py --coverage         # Run with coverage report
    python run_tests.py --verbose          # Verbose output
    python run_tests.py --quick            # Quick tests only (no slow tests)
"""

import subprocess
import sys
import os
import argparse
from pathlib import Path


def setup_environment():
    """Set up the Python environment for testing."""
    project_root = Path(__file__).parent.absolute()
    src_path = project_root / "src"
    
    # Add paths to Python path
    paths_to_add = [str(src_path), str(project_root)]
    for path in paths_to_add:
        if path not in sys.path:
            sys.path.insert(0, path)
    
    # Set environment variable for subprocess
    env = os.environ.copy()
    existing_path = env.get('PYTHONPATH', '')
    env['PYTHONPATH'] = os.pathsep.join(paths_to_add + ([existing_path] if existing_path else []))
    
    # Ensure __init__.py files exist
    init_files = [
        project_root / "tests" / "__init__.py",
        project_root / "tests" / "unit" / "__init__.py",
        project_root / "tests" / "integration" / "__init__.py",
    ]
    
    for init_file in init_files:
        init_file.parent.mkdir(parents=True, exist_ok=True)
        if not init_file.exists():
            init_file.touch()
    
    return env


def run_command(cmd: list, env: dict) -> int:
    """Run a command and return the exit code."""
    print(f"\nRunning: {' '.join(cmd)}")
    print("-" * 80)
    return subprocess.call(cmd, env=env)


def main():
    parser = argparse.ArgumentParser(
        description="Run tests for search_delete_test",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                      # Run all unit tests (default)
  %(prog)s --type integration   # Run integration tests
  %(prog)s --coverage           # Run with coverage report
  %(prog)s --verbose --quick    # Quick verbose unit tests
  %(prog)s --type unit -x       # Stop on first unit test failure
        """
    )
    
    parser.add_argument(
        "--type",
        choices=["all", "unit", "integration"],
        default="unit",
        help="Type of tests to run (default: unit)"
    )
    parser.add_argument(
        "--coverage",
        action="store_true",
        help="Generate coverage report (HTML and terminal)"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Verbose output"
    )
    parser.add_argument(
        "--quick", "-q",
        action="store_true",
        help="Run quick tests only (exclude slow tests)"
    )
    parser.add_argument(
        "--failfast", "-x",
        action="store_true",
        help="Stop on first failure"
    )
    parser.add_argument(
        "--warnings", "-w",
        action="store_true",
        help="Show warnings (default: warnings are suppressed)"
    )
    
    args = parser.parse_args()
    
    # Set up environment
    env = setup_environment()
    
    # Build pytest command
    cmd = [sys.executable, "-m", "pytest"]
    
    # Add test type markers
    if args.type == "unit":
        cmd.extend(["-m", "unit"])
        print("🔧 Running unit tests...")
    elif args.type == "integration":
        cmd.extend(["-m", "integration"])
        print("🔗 Running integration tests...")
    else:
        print("🚀 Running all tests...")
    
    # Add quick test filter
    if args.quick:
        if args.type == "unit":
            cmd.extend(["-m", "unit and not slow"])
        else:
            cmd.extend(["-m", "not slow"])
        print("⚡ Quick tests only (excluding slow tests)")
    
    # Add verbosity
    if args.verbose:
        cmd.append("-vv")
    else:
        cmd.append("-v")
    
    # Add fail fast
    if args.failfast:
        cmd.append("-x")
    
    # Handle warnings
    if not args.warnings:
        cmd.extend(["-W", "ignore::DeprecationWarning"])
    
    # Add coverage
    if args.coverage:
        cmd.extend([
            "--cov=search_delete_test",
            "--cov-report=html",
            "--cov-report=term-missing"
        ])
        print("📊 Coverage reporting enabled")
    
    # Add nice output formatting
    cmd.extend(["--tb=short"])
    
    # Run the tests
    print(f"\nPython path: {env.get('PYTHONPATH', 'Not set')}")
    exit_code = run_command(cmd, env)
    
    # Print results
    print("\n" + "=" * 80)
    if exit_code == 0:
        print("✅ ALL TESTS PASSED! 🎉")
        if args.coverage:
            print("📊 Coverage report generated in htmlcov/index.html")
        if args.type == "unit":
            print("🔧 All unit tests are working correctly without Valkey!")
    else:
        print("❌ Some tests failed.")
        print("Check the output above for details.")
    print("=" * 80)
    
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
