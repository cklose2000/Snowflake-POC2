# Dashboard Factory Test Suite Makefile
# Run tests with: make test

.PHONY: all test test-python test-sql test-ui clean report help setup

# Default target
all: test

# Run all tests
test:
	@echo "Running complete test suite..."
	@./scripts/run-tests.sh

# Run only Python unit tests
test-python:
	@echo "Running Python unit tests..."
	@python3 tests/test_plan_runner.py
	@python3 tests/test_nl_compiler.py
	@python3 tests/test_schedule_flow.py

# Run only SQL procedure tests
test-sql:
	@echo "Running SQL procedure tests..."
	@~/bin/sf exec-file scripts/test-comprehensive-procs.sql

# Run real integration tests (live Snowflake)
test-real:
	@echo "Running REAL integration tests..."
	@SF_PK_PATH=./claude_code_rsa_key.p8 python3 tests/test_integration_real.py

# Run real API tests (requires server running)
test-api:
	@echo "Running REAL API tests..."
	@SF_PK_PATH=./claude_code_rsa_key.p8 python3 tests/test_api_real.py

# Run only UI smoke tests
test-ui:
	@echo "Running UI smoke tests..."
	@python3 tests/test_ui_smoke.py

# Run ALL real tests (not mocks)
test-all-real:
	@echo "Running ALL REAL tests (live Snowflake + API)..."
	@SF_PK_PATH=./claude_code_rsa_key.p8 python3 tests/test_integration_real.py
	@SF_PK_PATH=./claude_code_rsa_key.p8 python3 tests/test_api_real.py

# Run specific test file
test-file:
	@if [ -z "$(FILE)" ]; then \
		echo "Usage: make test-file FILE=tests/test_plan_runner.py"; \
		exit 1; \
	fi
	@echo "Running $(FILE)..."
	@python3 $(FILE)

# Clean test artifacts
clean:
	@echo "Cleaning test artifacts..."
	@rm -rf reports/*.txt reports/*.json
	@rm -rf __pycache__ tests/__pycache__ tests/helpers/__pycache__
	@find . -name "*.pyc" -delete

# Generate test report
report:
	@echo "Generating test report..."
	@if [ -f reports/test_summary.json ]; then \
		cat reports/test_summary.json | python3 -m json.tool; \
	else \
		echo "No test summary found. Run 'make test' first."; \
	fi

# Setup test environment
setup:
	@echo "Setting up test environment..."
	@mkdir -p reports
	@mkdir -p tests/fixtures
	@echo "Test environment ready"

# Install test dependencies
install-deps:
	@echo "Installing test dependencies..."
	@pip3 install pytest pandas pytz

# Quick test (fast subset)
test-quick:
	@echo "Running quick test subset..."
	@python3 tests/test_plan_runner.py
	@python3 tests/test_nl_compiler.py

# Verbose test run
test-verbose:
	@echo "Running tests with verbose output..."
	@VERBOSE=1 ./scripts/run-tests.sh

# CI test run (for GitHub Actions)
test-ci:
	@echo "Running CI test suite..."
	@./scripts/run-tests.sh || (cat reports/test_report_*.txt && exit 1)

# Help target
help:
	@echo "Dashboard Factory Test Suite"
	@echo ""
	@echo "Available targets:"
	@echo "  make test          - Run all tests"
	@echo "  make test-python   - Run Python unit tests only"
	@echo "  make test-sql      - Run SQL procedure tests only"
	@echo "  make test-ui       - Run UI smoke tests only"
	@echo "  make test-quick    - Run quick test subset"
	@echo "  make test-verbose  - Run tests with verbose output"
	@echo "  make test-ci       - Run tests for CI pipeline"
	@echo "  make test-file FILE=<path> - Run specific test file"
	@echo "  make clean         - Clean test artifacts"
	@echo "  make report        - Display test report"
	@echo "  make setup         - Setup test environment"
	@echo "  make install-deps  - Install test dependencies"
	@echo "  make help          - Show this help message"
	@echo ""
	@echo "Examples:"
	@echo "  make test"
	@echo "  make test-file FILE=tests/test_plan_runner.py"
	@echo "  make test-verbose"