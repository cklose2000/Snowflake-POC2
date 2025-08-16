# Dashboard Factory Test Suite

Comprehensive test suite for the Dashboard Factory system with Claude Code integration.

## Test Structure

```
tests/
├── helpers/
│   └── sf_session_stub.py      # Mock Snowpark session for testing
├── fixtures/
│   └── presets.json            # Test data fixtures
├── test_plan_runner.py         # Tests for plan execution with PARSE_JSON(?)
├── test_nl_compiler.py         # Tests for NL to plan compilation
├── test_schedule_flow.py       # Tests for schedule creation/execution
├── test_ui_smoke.py           # Headless UI smoke tests
└── README.md                   # This file
```

## Running Tests

### Run All Tests
```bash
make test
```

### Run Specific Test Suite
```bash
# Python unit tests only
make test-python

# SQL procedure tests only
make test-sql

# UI smoke tests only
make test-ui

# Quick subset of tests
make test-quick
```

### Run Individual Test File
```bash
make test-file FILE=tests/test_plan_runner.py
```

## Test Categories

### 1. Plan Runner Tests (`test_plan_runner.py`)
- **PROC-01**: Validates correct `CALL MCP.PROC(PARSE_JSON(?))` pattern
- **PROC-02**: Ensures only whitelisted procedures allowed
- **GUARD-01**: Tests interval clamping to valid values
- **GUARD-02**: Tests limit/n capping at maximum values
- **PLAN-01**: Validates ISO8601 timestamps with timezone
- **PLAN-02**: Ensures all JSON keys are lowercase

### 2. NL Compiler Tests (`test_nl_compiler.py`)
- **NL-01**: Extract user email from natural language
- **NL-02**: Parse "top actions today" queries
- **NL-03**: Parse cohort URLs from text
- **NL-04**: Correct procedure selection based on query type
- **NL-05**: Parse time intervals correctly
- **NL-06**: Filters always present as object
- **NL-07**: Dimension values from allowed set

### 3. Schedule Flow Tests (`test_schedule_flow.py`)
- **SCHED-01**: Create daily schedule with timezone
- **SCHED-02**: Weekday schedule skips weekends
- **SCHED-03**: Correct handling around DST transitions
- **SCHED-04**: Execute schedule and generate snapshot
- **SCHED-05**: Snapshots are pointers, not table writes
- **SCHED-06**: Multiple delivery channels supported
- **SCHED-07**: Only valid Olson timezone IDs accepted
- **AUDIT-01**: Schedule events have complete attributes

### 4. UI Smoke Tests (`test_ui_smoke.py`)
- **UI-01**: Claude Code is prominently displayed
- **UI-02**: Agent Console shows Claude's thinking
- **UI-03**: Natural language input field present
- **UI-04**: Quick preset buttons available
- **UI-05**: Errors shown with Claude context
- **UI-06**: Dashboard selector in sidebar
- **UI-07**: Charts render with proper types
- **UI-08**: Claude status shows state transitions
- **UI-09**: Layout uses columns for responsive design
- **UI-10**: Results show "via Claude Code"
- **AUDIT-02**: Audit trail shows Claude's actions
- **COO-UX**: Zero-click to first insight

### 5. SQL Procedure Tests (`test-comprehensive-procs.sql`)
- Two-Table Law validation
- All procedures with correct CALL pattern
- Parameter validation and capping
- ISO timestamp handling
- Cohort filtering
- Empty results handling
- SQL injection prevention
- Boundary conditions

## Key Testing Principles

### 1. Two-Table Law Enforcement
All tests validate that only two tables exist:
- `CLAUDE_BI.APP.LANDING.RAW_EVENTS`
- `CLAUDE_BI.APP.ACTIVITY.EVENTS`

### 2. Correct Procedure Calls
All procedures use single VARIANT parameter:
```sql
CALL MCP.PROC(PARSE_JSON(?))
```
Never:
- Multiple named parameters
- SELECT wrapper
- Named arguments with `=>`

### 3. Claude Code Visibility
Tests ensure Claude Code is:
- Visible in UI (not hidden)
- Attributed in query tags
- Shown in audit trails
- Present in status indicators

### 4. ISO8601 Timestamps
All timestamps must be ISO8601 with timezone:
- ✅ `2025-01-16T00:00:00Z`
- ✅ `2025-01-16T00:00:00+00:00`
- ❌ `DATEADD('day', -7, CURRENT_TIMESTAMP())`

## Test Fixtures

Test data is stored in `tests/fixtures/presets.json`:
- Valid presets and expected results
- Test actors, actions, sources
- Invalid SQL patterns to detect
- Sample events and dashboards
- Timezone and schedule test data

## CI/CD Integration

Tests run automatically on:
- Push to `main` or `develop` branches
- Pull requests to `main`
- Changes to `src/`, `stage/`, `scripts/`, or `tests/`

GitHub Actions workflow: `.github/workflows/test-dashboard-factory.yml`

## Reports

Test reports are generated in `reports/`:
- `test_report_YYYYMMDD_HHMMSS.txt` - Full report
- `test_summary.json` - JSON summary
- Individual test output files

View latest report:
```bash
make report
```

## Development

### Adding New Tests
1. Create test file in `tests/`
2. Follow naming convention: `test_*.py`
3. Use test ID format: `CATEGORY-NN` (e.g., `UI-01`)
4. Add to `scripts/run-tests.sh`
5. Update this README

### Mock Objects
Use `MockSession` from `helpers/sf_session_stub.py` for Snowpark testing.

### Running Tests Locally
```bash
# Install dependencies
make install-deps

# Setup test environment
make setup

# Run tests
make test
```

## Troubleshooting

### Tests Failing Locally
1. Check Python version (3.9+)
2. Install dependencies: `pip install pytest pandas pytz`
3. Verify Snowflake CLI: `~/bin/sf --version`

### SQL Tests Not Running
Ensure Snowflake CLI is configured:
```bash
~/bin/sf connection test
```

### Clean Test Artifacts
```bash
make clean
```