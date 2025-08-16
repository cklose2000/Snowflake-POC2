# Test Reports Directory

This directory contains test execution reports from the Dashboard Factory test suite.

## Report Types

### Test Execution Reports
- `test_report_YYYYMMDD_HHMMSS.txt` - Main test execution report
- `test_summary.json` - JSON summary of test results

### Individual Test Output
- `Plan_Runner_output.txt` - Output from plan runner tests
- `NL_Compiler_output.txt` - Output from NL compiler tests
- `Schedule_Flow_output.txt` - Output from schedule flow tests
- `UI_Smoke_output.txt` - Output from UI smoke tests
- `SQL_Procedures_output.txt` - Output from SQL procedure tests

## Running Tests

To generate reports, run:
```bash
make test
```

To view the latest report:
```bash
make report
```

## Report Structure

Each test report includes:
- Timestamp of execution
- Test suite name
- Pass/fail status for each test
- Detailed error messages for failures
- Summary statistics

## CI/CD Integration

Reports are automatically generated and uploaded as artifacts in GitHub Actions.

## Cleaning Reports

To clean old reports:
```bash
make clean
```

This will remove all `.txt` and `.json` files from this directory.