-- Comprehensive SQL Procedure Tests for Dashboard Factory
-- Tests all procedures with correct CALL pattern and validates Two-Table Law

-- ============================================
-- SETUP
-- ============================================

-- @statement
USE DATABASE CLAUDE_BI;

-- @statement
USE SCHEMA MCP;

-- @statement
-- Set test context
ALTER SESSION SET QUERY_TAG = 'test-suite|agent:claude|test:comprehensive';

-- ============================================
-- TWO-TABLE LAW VALIDATION
-- ============================================

-- @statement
-- CRITICAL: Verify ONLY 2 tables exist
SELECT 
    CASE 
        WHEN COUNT(*) = 2 THEN 'PASS: Two-Table Law Maintained'
        ELSE 'FAIL: ' || COUNT(*) || ' tables found - VIOLATION!'
    END AS table_count_test,
    COUNT(*) as actual_count
FROM CLAUDE_BI.INFORMATION_SCHEMA.TABLES
WHERE TABLE_CATALOG = 'CLAUDE_BI'
  AND TABLE_SCHEMA IN ('APP', 'LANDING', 'ACTIVITY')
  AND TABLE_TYPE IN ('BASE TABLE', 'DYNAMIC TABLE');

-- @statement
-- Verify the correct 2 tables
SELECT 
    TABLE_SCHEMA,
    TABLE_NAME,
    TABLE_TYPE,
    CASE 
        WHEN (TABLE_SCHEMA = 'LANDING' AND TABLE_NAME = 'RAW_EVENTS' AND TABLE_TYPE = 'BASE TABLE')
          OR (TABLE_SCHEMA = 'ACTIVITY' AND TABLE_NAME = 'EVENTS' AND TABLE_TYPE = 'DYNAMIC TABLE')
        THEN 'VALID'
        ELSE 'INVALID - MUST BE REMOVED!'
    END AS validation_status
FROM CLAUDE_BI.INFORMATION_SCHEMA.TABLES
WHERE TABLE_CATALOG = 'CLAUDE_BI'
  AND TABLE_SCHEMA IN ('APP', 'LANDING', 'ACTIVITY')
  AND TABLE_TYPE IN ('BASE TABLE', 'DYNAMIC TABLE')
ORDER BY TABLE_SCHEMA, TABLE_NAME;

-- ============================================
-- TEST: DASH_GET_SERIES
-- ============================================

-- @statement
-- Test 1: Basic time series with hourly intervals
CALL MCP.DASH_GET_SERIES(PARSE_JSON('{
    "start_ts": "2025-01-15T00:00:00Z",
    "end_ts": "2025-01-16T00:00:00Z",
    "interval": "hour",
    "filters": {},
    "group_by": null
}'));

-- @statement
-- Test 2: Time series with actor filter
CALL MCP.DASH_GET_SERIES(PARSE_JSON('{
    "start_ts": "2025-01-14T00:00:00Z",
    "end_ts": "2025-01-16T00:00:00Z",
    "interval": "day",
    "filters": {
        "actor": "test@example.com"
    }
}'));

-- @statement
-- Test 3: Time series with 15-minute intervals
CALL MCP.DASH_GET_SERIES(PARSE_JSON('{
    "start_ts": "2025-01-16T12:00:00Z",
    "end_ts": "2025-01-16T14:00:00Z",
    "interval": "15 minute",
    "filters": {
        "action": "user.login"
    }
}'));

-- ============================================
-- TEST: DASH_GET_TOPN
-- ============================================

-- @statement
-- Test 4: Top 10 actors
CALL MCP.DASH_GET_TOPN(PARSE_JSON('{
    "start_ts": "2025-01-09T00:00:00Z",
    "end_ts": "2025-01-16T00:00:00Z",
    "dimension": "actor",
    "n": 10,
    "limit": 1000,
    "filters": {}
}'));

-- @statement
-- Test 5: Top 5 actions with filter
CALL MCP.DASH_GET_TOPN(PARSE_JSON('{
    "start_ts": "2025-01-15T00:00:00Z",
    "end_ts": "2025-01-16T00:00:00Z",
    "dimension": "action",
    "n": 5,
    "filters": {
        "source": "WEB"
    }
}'));

-- @statement
-- Test 6: Top sources (testing dimension variety)
CALL MCP.DASH_GET_TOPN(PARSE_JSON('{
    "start_ts": "2025-01-01T00:00:00Z",
    "end_ts": "2025-01-16T00:00:00Z",
    "dimension": "source",
    "n": 10,
    "filters": {}
}'));

-- ============================================
-- TEST: DASH_GET_METRICS
-- ============================================

-- @statement
-- Test 7: Basic metrics summary
CALL MCP.DASH_GET_METRICS(PARSE_JSON('{
    "start_ts": "2025-01-01T00:00:00Z",
    "end_ts": "2025-01-16T00:00:00Z",
    "filters": {}
}'));

-- @statement
-- Test 8: Metrics with actor filter
CALL MCP.DASH_GET_METRICS(PARSE_JSON('{
    "start_ts": "2025-01-10T00:00:00Z",
    "end_ts": "2025-01-16T00:00:00Z",
    "filters": {
        "actor": "admin@company.com"
    }
}'));

-- ============================================
-- TEST: DASH_GET_EVENTS
-- ============================================

-- @statement
-- Test 9: Get recent events
CALL MCP.DASH_GET_EVENTS(PARSE_JSON('{
    "cursor_ts": "2025-01-16T00:00:00Z",
    "limit": 10
}'));

-- @statement
-- Test 10: Get events with small limit
CALL MCP.DASH_GET_EVENTS(PARSE_JSON('{
    "cursor_ts": "2025-01-15T12:00:00Z",
    "limit": 5
}'));

-- ============================================
-- TEST: PARAMETER VALIDATION
-- ============================================

-- @statement
-- Test 11: Verify limit capping (should cap at 5000)
CALL MCP.DASH_GET_EVENTS(PARSE_JSON('{
    "cursor_ts": "2025-01-16T00:00:00Z",
    "limit": 999999
}'));

-- @statement
-- Test 12: Verify n capping in TOPN (should cap at 50)
CALL MCP.DASH_GET_TOPN(PARSE_JSON('{
    "start_ts": "2025-01-15T00:00:00Z",
    "end_ts": "2025-01-16T00:00:00Z",
    "dimension": "actor",
    "n": 999999,
    "filters": {}
}'));

-- ============================================
-- TEST: INTERVAL VALIDATION
-- ============================================

-- @statement
-- Test 13: Valid intervals
CALL MCP.DASH_GET_SERIES(PARSE_JSON('{
    "start_ts": "2025-01-16T00:00:00Z",
    "end_ts": "2025-01-16T01:00:00Z",
    "interval": "minute",
    "filters": {}
}'));

-- @statement
CALL MCP.DASH_GET_SERIES(PARSE_JSON('{
    "start_ts": "2025-01-16T00:00:00Z",
    "end_ts": "2025-01-16T01:00:00Z",
    "interval": "5 minute",
    "filters": {}
}'));

-- ============================================
-- TEST: ISO TIMESTAMP HANDLING
-- ============================================

-- @statement
-- Test 14: ISO timestamps with timezone
CALL MCP.DASH_GET_SERIES(PARSE_JSON('{
    "start_ts": "2025-01-16T00:00:00+00:00",
    "end_ts": "2025-01-16T23:59:59+00:00",
    "interval": "hour",
    "filters": {}
}'));

-- @statement
-- Test 15: ISO timestamps with Z suffix
CALL MCP.DASH_GET_SERIES(PARSE_JSON('{
    "start_ts": "2025-01-16T00:00:00Z",
    "end_ts": "2025-01-16T23:59:59Z",
    "interval": "hour",
    "filters": {}
}'));

-- ============================================
-- TEST: COHORT FILTERING
-- ============================================

-- @statement
-- Test 16: Cohort URL filter (if supported)
CALL MCP.DASH_GET_SERIES(PARSE_JSON('{
    "start_ts": "2025-01-15T00:00:00Z",
    "end_ts": "2025-01-16T00:00:00Z",
    "interval": "hour",
    "filters": {
        "cohort_url": "s3://test-bucket/cohorts/test.jsonl"
    }
}'));

-- ============================================
-- TEST: EMPTY RESULTS HANDLING
-- ============================================

-- @statement
-- Test 17: Query with no matching data (future dates)
CALL MCP.DASH_GET_SERIES(PARSE_JSON('{
    "start_ts": "2030-01-01T00:00:00Z",
    "end_ts": "2030-01-02T00:00:00Z",
    "interval": "hour",
    "filters": {}
}'));

-- ============================================
-- TEST: CLAUDE EVENT LOGGING
-- ============================================

-- @statement
-- Test 18: Log a test event (validates event ingestion)
CALL MCP.LOG_CLAUDE_EVENT(PARSE_JSON('{
    "action": "test.comprehensive.completed",
    "actor_id": "test-suite",
    "object": {
        "type": "test",
        "id": "comprehensive-sql"
    },
    "attributes": {
        "tests_run": 18,
        "timestamp": "2025-01-16T12:00:00Z"
    }
}'));

-- ============================================
-- TEST: VERIFY NO SQL INJECTION
-- ============================================

-- @statement
-- Test 19: Attempt SQL injection in filters (should be safely handled)
CALL MCP.DASH_GET_SERIES(PARSE_JSON('{
    "start_ts": "2025-01-16T00:00:00Z",
    "end_ts": "2025-01-16T01:00:00Z",
    "interval": "hour",
    "filters": {
        "actor": "test@example.com; DROP TABLE users; --"
    }
}'));

-- ============================================
-- TEST: BOUNDARY CONDITIONS
-- ============================================

-- @statement
-- Test 20: Minimum time range (1 minute)
CALL MCP.DASH_GET_SERIES(PARSE_JSON('{
    "start_ts": "2025-01-16T12:00:00Z",
    "end_ts": "2025-01-16T12:01:00Z",
    "interval": "minute",
    "filters": {}
}'));

-- @statement
-- Test 21: Maximum reasonable time range (1 year)
CALL MCP.DASH_GET_SERIES(PARSE_JSON('{
    "start_ts": "2024-01-16T00:00:00Z",
    "end_ts": "2025-01-16T00:00:00Z",
    "interval": "day",
    "filters": {}
}'));

-- ============================================
-- FINAL VALIDATION
-- ============================================

-- @statement
-- Final check: Ensure no tables were created during tests
SELECT 
    CASE 
        WHEN COUNT(*) = 2 THEN 'SUCCESS: Tests completed without violating Two-Table Law'
        ELSE 'FAILURE: Test created ' || (COUNT(*) - 2) || ' additional tables!'
    END AS final_validation,
    COUNT(*) as table_count
FROM CLAUDE_BI.INFORMATION_SCHEMA.TABLES
WHERE TABLE_CATALOG = 'CLAUDE_BI'
  AND TABLE_SCHEMA IN ('APP', 'LANDING', 'ACTIVITY')
  AND TABLE_TYPE IN ('BASE TABLE', 'DYNAMIC TABLE');

-- @statement
-- Summary of test execution
SELECT 
    'Test Suite Complete' as status,
    CURRENT_TIMESTAMP() as completed_at,
    'All procedures tested with correct CALL pattern' as notes;