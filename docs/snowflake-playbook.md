# Snowflake Playbook for Claude Code

This playbook contains canonical patterns and best practices for working with Snowflake in this codebase. Use these patterns as templates for consistent, performant SQL generation.

## Table of Contents
1. [Stored Procedures](#stored-procedures)
2. [Dynamic Tables](#dynamic-tables)
3. [Time Series Patterns](#time-series-patterns)
4. [Top-N Patterns](#top-n-patterns)
5. [Statement Splitting](#statement-splitting)
6. [Performance Optimizations](#performance-optimizations)
7. [Security Patterns](#security-patterns)

## Stored Procedures

### Basic SQL Procedure Template
```sql
-- @statement
CREATE OR REPLACE PROCEDURE PROC_NAME(
  param1 TYPE,
  param2 TYPE
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
BEGIN
  -- Procedure logic here
  LET result VARIANT;
  
  result := (SELECT ...);
  
  RETURN OBJECT_CONSTRUCT(
    'ok', TRUE,
    'data', result
  );
  
EXCEPTION
  WHEN OTHER THEN
    RETURN OBJECT_CONSTRUCT('ok', FALSE, 'error', SQLERRM);
END;
$$;
```

### JavaScript Procedure Template
```sql
-- @statement
CREATE OR REPLACE PROCEDURE PROC_NAME_JS(
  param1 TYPE
)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
  try {
    // JavaScript logic here
    var result = snowflake.execute({
      sqlText: `SELECT * FROM table WHERE col = ?`,
      binds: [PARAM1]
    });
    
    return {ok: true, data: result};
  } catch (err) {
    return {ok: false, error: err.message};
  }
$$;
```

### Role Guards (Simplified)
Since we use `EXECUTE AS OWNER`, the procedure runs with the owner's privileges. For access control, grant EXECUTE privileges on the procedure to specific roles:
```sql
GRANT EXECUTE ON PROCEDURE PROC_NAME TO ROLE R_APP_READ;
```

## Dynamic Tables

### Activity Events Pattern
```sql
-- @statement
CREATE OR REPLACE DYNAMIC TABLE ACTIVITY.EVENTS
TARGET_LAG = '1 minute'
WAREHOUSE = CLAUDE_AGENT_WH
AS
SELECT 
  event_data:event_id::STRING AS event_id,
  event_data:action::STRING AS action,
  event_data:actor_id::STRING AS actor_id,
  event_data:object AS object,
  event_data:attributes AS attributes,
  event_data:occurred_at::TIMESTAMP_TZ AS occurred_at,
  ingested_at,
  source_lane
FROM LANDING.RAW_EVENTS
WHERE event_data IS NOT NULL;
```

## Time Series Patterns

### Time Binning with Generator
```sql
-- Generate complete time series with zero-fill
WITH time_bins AS (
  SELECT 
    DATEADD('hour', SEQ4(), :start_ts) AS time_bucket
  FROM TABLE(GENERATOR(ROWCOUNT => 
    TIMESTAMPDIFF('hour', :start_ts, :end_ts) + 1
  ))
),
event_counts AS (
  SELECT 
    DATE_TRUNC('hour', occurred_at) AS time_bucket,
    COUNT(*) AS event_count
  FROM ACTIVITY.EVENTS
  WHERE occurred_at BETWEEN :start_ts AND :end_ts
  GROUP BY 1
)
SELECT 
  tb.time_bucket,
  COALESCE(ec.event_count, 0) AS event_count
FROM time_bins tb
LEFT JOIN event_counts ec ON tb.time_bucket = ec.time_bucket
ORDER BY tb.time_bucket;
```

### Time Slice Alternative
```sql
-- Using TIME_SLICE for simpler binning
SELECT 
  TIME_SLICE(occurred_at, 1, 'hour') AS time_bucket,
  COUNT(*) AS event_count
FROM ACTIVITY.EVENTS
WHERE occurred_at BETWEEN :start_ts AND :end_ts
GROUP BY 1
ORDER BY 1;
```

## Top-N Patterns

### Using QUALIFY with ROW_NUMBER
```sql
-- Get top N items by count
SELECT 
  dimension_column,
  COUNT(*) AS item_count
FROM ACTIVITY.EVENTS
WHERE occurred_at BETWEEN :start_ts AND :end_ts
GROUP BY dimension_column
QUALIFY ROW_NUMBER() OVER (ORDER BY item_count DESC) <= :n
ORDER BY item_count DESC;
```

### Using LIMIT (simpler but less flexible)
```sql
SELECT 
  dimension_column,
  COUNT(*) AS item_count
FROM ACTIVITY.EVENTS
WHERE occurred_at BETWEEN :start_ts AND :end_ts
GROUP BY dimension_column
ORDER BY item_count DESC
LIMIT :n;
```

## Statement Splitting

### Using Statement Markers (Preferred)
Always add `-- @statement` markers before each DDL/DML statement in SQL files:
```sql
-- @statement
USE DATABASE CLAUDE_BI;

-- @statement
USE SCHEMA MCP;

-- @statement
CREATE OR REPLACE PROCEDURE ...
$$;

-- @statement
CREATE TABLE IF NOT EXISTS ...;
```

## Performance Optimizations

### Session Settings
```sql
-- Set at session start for optimal performance
ALTER SESSION SET 
  AUTOCOMMIT = TRUE,
  USE_CACHED_RESULT = TRUE,
  STATEMENT_TIMEOUT_IN_SECONDS = 120,
  QUERY_TAG = 'cc-cli|session:xyz';
```

### Result Caching
- Keep SQL text identical across runs
- Use bind parameters instead of string concatenation
- Set stable QUERY_TAG for cache hits

### Warehouse Management
```sql
-- Use appropriate warehouse sizes
USE WAREHOUSE CLAUDE_AGENT_WH;  -- Small for queries
USE WAREHOUSE ETL_WH;           -- Larger for heavy processing
```

## Security Patterns

### Parameter Binding (Prevent SQL Injection)
```sql
-- GOOD: Use bind parameters
EXECUTE IMMEDIATE 'SELECT * FROM events WHERE action = ?'
  USING (user_input);

-- BAD: String concatenation
EXECUTE IMMEDIATE 'SELECT * FROM events WHERE action = ''' || user_input || '''';
```

### VARIANT Data Access
```sql
-- Safe JSON path access with null handling
SELECT 
  event_data:action::STRING AS action,
  COALESCE(event_data:attributes:count::NUMBER, 0) AS count,
  TRY_PARSE_JSON(event_data:metadata) AS metadata
FROM ACTIVITY.EVENTS;
```

### Timestamp Handling
```sql
-- Always use TIMESTAMP_TZ for consistency
CAST(value AS TIMESTAMP_TZ)
CURRENT_TIMESTAMP()  -- Returns TIMESTAMP_TZ
TO_TIMESTAMP_TZ(string_value, 'YYYY-MM-DD HH24:MI:SS')
```

## Common Patterns

### Latest State per Entity
```sql
-- Get most recent state for each entity
WITH latest_events AS (
  SELECT 
    *,
    ROW_NUMBER() OVER (
      PARTITION BY object:id 
      ORDER BY occurred_at DESC
    ) AS rn
  FROM ACTIVITY.EVENTS
  WHERE action = 'state.updated'
)
SELECT * FROM latest_events WHERE rn = 1;
```

### Event Aggregation
```sql
-- Aggregate events with multiple dimensions
SELECT 
  action,
  actor_id,
  DATE(occurred_at) AS event_date,
  COUNT(*) AS event_count,
  COUNT(DISTINCT session_id) AS unique_sessions
FROM ACTIVITY.EVENTS
WHERE occurred_at >= DATEADD('day', -30, CURRENT_DATE())
GROUP BY 1, 2, 3;
```

### Search Optimization
For large ACTIVITY.EVENTS tables, consider:
```sql
-- Add search optimization
ALTER TABLE ACTIVITY.EVENTS 
ADD SEARCH OPTIMIZATION ON (occurred_at, action, actor_id);
```

## Error Handling

### Procedure Error Handling
```sql
BEGIN
  -- Main logic
  RETURN OBJECT_CONSTRUCT('ok', TRUE, 'data', result);
EXCEPTION
  WHEN STATEMENT_ERROR THEN
    RETURN OBJECT_CONSTRUCT('ok', FALSE, 'error', 'Invalid SQL');
  WHEN OTHER THEN
    RETURN OBJECT_CONSTRUCT('ok', FALSE, 'error', SQLERRM);
END;
```

### Safe Type Casting
```sql
-- Use TRY_ functions to avoid errors
TRY_CAST(value AS NUMBER)
TRY_TO_TIMESTAMP(date_string)
TRY_PARSE_JSON(json_string)
```

## Testing Patterns

### Smoke Test Query
```sql
-- Quick connectivity and permission check
SELECT 
  CURRENT_USER() AS user,
  CURRENT_ROLE() AS role,
  CURRENT_WAREHOUSE() AS warehouse,
  CURRENT_DATABASE() AS database,
  COUNT(*) AS event_count
FROM ACTIVITY.EVENTS
WHERE occurred_at > DATEADD('hour', -1, CURRENT_TIMESTAMP());
```

### Procedure Testing
```sql
-- Test procedure with sample data
CALL MCP.DASH_GET_METRICS(
  DATEADD('day', -7, CURRENT_TIMESTAMP()),
  CURRENT_TIMESTAMP(),
  OBJECT_CONSTRUCT('action', 'test.event')
);
```

## Best Practices Summary

1. **Always use statement markers** (`-- @statement`) in SQL files
2. **Use bind parameters** instead of string concatenation
3. **Handle NULLs explicitly** with COALESCE or TRY_ functions
4. **Use TIMESTAMP_TZ** for all timestamp columns
5. **Keep SQL text stable** for result cache benefits
6. **Use appropriate warehouses** for different workloads
7. **Return consistent VARIANT structures** from procedures
8. **Add exception handling** to all procedures
9. **Test with result caching enabled** for performance
10. **Document expected parameters** in procedure comments