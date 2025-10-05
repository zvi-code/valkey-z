#!/bin/bash
# Simple test runner shell script for search_delete_test
# This is a convenience wrapper around the main Python test runner

# Get the directory of this script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default to unit tests
TEST_TYPE="unit"
EXTRA_ARGS=""

# Parse simple arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  -h, --help        Show this help message"
            echo "  -u, --unit        Run unit tests (default)"
            echo "  -i, --integration Run integration tests"
            echo "  -a, --all         Run all tests"
            echo "  -c, --coverage    Generate coverage report"
            echo "  -v, --verbose     Verbose output"
            echo "  -q, --quick       Quick tests only"
            echo "  -x, --failfast    Stop on first failure"
            echo ""
            echo "Examples:"
            echo "  $0                # Run unit tests"
            echo "  $0 --integration  # Run integration tests"
            echo "  $0 --coverage     # Run with coverage"
            echo "  $0 --verbose --quick  # Quick verbose tests"
            echo ""
            echo "For more options, use: python run_tests.py --help"
            exit 0
            ;;
        -u|--unit)
            TEST_TYPE="unit"
            shift
            ;;
        -i|--integration)
            TEST_TYPE="integration"
            shift
            ;;
        -a|--all)
            TEST_TYPE="all"
            shift
            ;;
        -c|--coverage)
            EXTRA_ARGS="$EXTRA_ARGS --coverage"
            shift
            ;;
        -v|--verbose)
            EXTRA_ARGS="$EXTRA_ARGS --verbose"
            shift
            ;;
        -q|--quick)
            EXTRA_ARGS="$EXTRA_ARGS --quick"
            shift
            ;;
        -x|--failfast)
            EXTRA_ARGS="$EXTRA_ARGS --failfast"
            shift
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            echo "Use -h or --help for usage information"
            exit 1
            ;;
    esac
done

# Run the Python test runner
echo -e "${BLUE}Valkey Search Delete Stress Test Runner${NC}"
echo -e "${BLUE}=========================${NC}"
echo ""

python tests/run_tests.py --type "$TEST_TYPE" $EXTRA_ARGS

# Capture exit code
EXIT_CODE=$?

# Final message
echo ""
if [ $EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}Test run completed successfully!${NC}"
else
    echo -e "${RED}Test run failed with exit code $EXIT_CODE${NC}"
fi

exit $EXIT_CODE
