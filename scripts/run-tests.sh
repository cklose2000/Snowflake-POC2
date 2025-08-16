#!/bin/bash

# Test orchestration script for Dashboard Factory
# Runs all test suites and generates reports

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Test configuration
REPORT_DIR="reports"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
REPORT_FILE="${REPORT_DIR}/test_report_${TIMESTAMP}.txt"

# Create reports directory
mkdir -p ${REPORT_DIR}

# Function to run a test suite
run_test_suite() {
    local test_name=$1
    local test_file=$2
    
    echo -e "${YELLOW}Running ${test_name}...${NC}"
    
    if python3 ${test_file} > ${REPORT_DIR}/${test_name}_output.txt 2>&1; then
        echo -e "${GREEN}✓ ${test_name} passed${NC}"
        echo "✓ ${test_name} passed" >> ${REPORT_FILE}
        return 0
    else
        echo -e "${RED}✗ ${test_name} failed${NC}"
        echo "✗ ${test_name} failed" >> ${REPORT_FILE}
        echo "  See ${REPORT_DIR}/${test_name}_output.txt for details" >> ${REPORT_FILE}
        return 1
    fi
}

# Function to run SQL tests
run_sql_tests() {
    local test_name="SQL_Procedures"
    
    echo -e "${YELLOW}Running ${test_name}...${NC}"
    
    if ~/bin/sf exec-file /Users/chandler/claude7/GrowthZone/SnowflakePOC2/scripts/test-comprehensive-procs.sql > ${REPORT_DIR}/${test_name}_output.txt 2>&1; then
        echo -e "${GREEN}✓ ${test_name} passed${NC}"
        echo "✓ ${test_name} passed" >> ${REPORT_FILE}
        return 0
    else
        echo -e "${RED}✗ ${test_name} failed${NC}"
        echo "✗ ${test_name} failed" >> ${REPORT_FILE}
        echo "  See ${REPORT_DIR}/${test_name}_output.txt for details" >> ${REPORT_FILE}
        return 1
    fi
}

# Start test run
echo "========================================="
echo "Dashboard Factory Test Suite"
echo "Timestamp: ${TIMESTAMP}"
echo "========================================="
echo ""

# Initialize counters
TOTAL_TESTS=0
PASSED_TESTS=0
FAILED_TESTS=0

# Write report header
echo "Dashboard Factory Test Report" > ${REPORT_FILE}
echo "Generated: $(date)" >> ${REPORT_FILE}
echo "=========================================" >> ${REPORT_FILE}
echo "" >> ${REPORT_FILE}

# Run Python unit tests
echo -e "${YELLOW}=== Python Unit Tests ===${NC}"
echo "=== Python Unit Tests ===" >> ${REPORT_FILE}

# Test: Mock Session Helper
if run_test_suite "Mock_Session" "tests/helpers/sf_session_stub.py"; then
    ((PASSED_TESTS++))
else
    ((FAILED_TESTS++))
fi
((TOTAL_TESTS++))

# Test: Plan Runner
if run_test_suite "Plan_Runner" "tests/test_plan_runner.py"; then
    ((PASSED_TESTS++))
else
    ((FAILED_TESTS++))
fi
((TOTAL_TESTS++))

# Test: NL Compiler
if run_test_suite "NL_Compiler" "tests/test_nl_compiler.py"; then
    ((PASSED_TESTS++))
else
    ((FAILED_TESTS++))
fi
((TOTAL_TESTS++))

# Test: Schedule Flow
if run_test_suite "Schedule_Flow" "tests/test_schedule_flow.py"; then
    ((PASSED_TESTS++))
else
    ((FAILED_TESTS++))
fi
((TOTAL_TESTS++))

# Test: UI Smoke Tests
if run_test_suite "UI_Smoke" "tests/test_ui_smoke.py"; then
    ((PASSED_TESTS++))
else
    ((FAILED_TESTS++))
fi
((TOTAL_TESTS++))

echo ""
echo -e "${YELLOW}=== SQL Procedure Tests ===${NC}"
echo "" >> ${REPORT_FILE}
echo "=== SQL Procedure Tests ===" >> ${REPORT_FILE}

# Run SQL tests
if run_sql_tests; then
    ((PASSED_TESTS++))
else
    ((FAILED_TESTS++))
fi
((TOTAL_TESTS++))

echo ""
echo "========================================="
echo "Test Summary"
echo "========================================="
echo "Total Tests: ${TOTAL_TESTS}"
echo "Passed: ${PASSED_TESTS}"
echo "Failed: ${FAILED_TESTS}"

# Write summary to report
echo "" >> ${REPORT_FILE}
echo "=========================================" >> ${REPORT_FILE}
echo "Summary" >> ${REPORT_FILE}
echo "=========================================" >> ${REPORT_FILE}
echo "Total Tests: ${TOTAL_TESTS}" >> ${REPORT_FILE}
echo "Passed: ${PASSED_TESTS}" >> ${REPORT_FILE}
echo "Failed: ${FAILED_TESTS}" >> ${REPORT_FILE}

# Generate JSON report for CI
cat > ${REPORT_DIR}/test_summary.json <<EOF
{
    "timestamp": "${TIMESTAMP}",
    "total": ${TOTAL_TESTS},
    "passed": ${PASSED_TESTS},
    "failed": ${FAILED_TESTS},
    "report_file": "${REPORT_FILE}"
}
EOF

# Exit with appropriate code
if [ ${FAILED_TESTS} -eq 0 ]; then
    echo -e "${GREEN}All tests passed!${NC}"
    echo ""
    echo "Full report: ${REPORT_FILE}"
    exit 0
else
    echo -e "${RED}Some tests failed!${NC}"
    echo ""
    echo "Full report: ${REPORT_FILE}"
    exit 1
fi