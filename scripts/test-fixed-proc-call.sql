-- Test the fixed procedure call pattern
-- This demonstrates the CORRECT way to call procedures with single VARIANT parameter

-- @statement
USE DATABASE CLAUDE_BI;

-- @statement
USE SCHEMA MCP;

-- @statement
-- Test DASH_GET_TOPN with single JSON parameter
CALL MCP.DASH_GET_TOPN(PARSE_JSON('{
    "start_ts": "2025-01-09T00:00:00Z",
    "end_ts": "2025-01-16T00:00:00Z",
    "dimension": "actor",
    "n": 10,
    "limit": 1000,
    "filters": {}
}'));

-- @statement
-- Test DASH_GET_SERIES with single JSON parameter
CALL MCP.DASH_GET_SERIES(PARSE_JSON('{
    "start_ts": "2025-01-15T00:00:00Z",
    "end_ts": "2025-01-16T00:00:00Z",
    "interval": "hour",
    "filters": {},
    "group_by": null
}'));

-- @statement
-- Test DASH_GET_METRICS with single JSON parameter
CALL MCP.DASH_GET_METRICS(PARSE_JSON('{
    "start_ts": "2025-01-01T00:00:00Z",
    "end_ts": "2025-01-16T00:00:00Z",
    "filters": {}
}'));

-- @statement
-- Test DASH_GET_EVENTS with single JSON parameter
CALL MCP.DASH_GET_EVENTS(PARSE_JSON('{
    "cursor_ts": "2025-01-16T00:00:00Z",
    "limit": 10
}'));