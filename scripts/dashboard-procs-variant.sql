-- Dashboard Stored Procedures with Single VARIANT Parameter
-- This is the CORRECT pattern: CALL MCP.PROC(PARSE_JSON(?))
-- All procedures accept a single VARIANT parameter and extract fields from JSON

-- @statement
USE DATABASE CLAUDE_BI;

-- @statement
USE SCHEMA MCP;

-- =====================================================
-- 1. DASH_GET_SERIES - Time series data (Simplified)
-- =====================================================
-- @statement
CREATE OR REPLACE PROCEDURE MCP.DASH_GET_SERIES(PARAMS VARIANT)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
BEGIN
  -- Direct query with parameter extraction inline
  RETURN (
    SELECT OBJECT_CONSTRUCT(
      'ok', TRUE,
      'data', ARRAY_AGG(
        OBJECT_CONSTRUCT(
          'time_bucket', time_bucket,
          'event_count', event_count,
          'unique_actors', unique_actors
        )
      ),
      'metadata', OBJECT_CONSTRUCT(
        'start_ts', PARAMS:start_ts::STRING,
        'end_ts', PARAMS:end_ts::STRING,
        'interval', COALESCE(PARAMS:interval::STRING, 'hour'),
        'row_count', COUNT(*)
      )
    )
    FROM (
      SELECT 
        DATE_TRUNC(
          CASE 
            WHEN PARAMS:interval::STRING IN ('minute', '5 minute', '15 minute', 'hour', 'day') 
            THEN PARAMS:interval::STRING 
            ELSE 'hour' 
          END, 
          occurred_at
        ) AS time_bucket,
        COUNT(*) AS event_count,
        COUNT(DISTINCT actor_id) AS unique_actors
      FROM ACTIVITY.EVENTS
      WHERE occurred_at BETWEEN PARAMS:start_ts::TIMESTAMP_TZ AND PARAMS:end_ts::TIMESTAMP_TZ
        AND (PARAMS:filters:actor::STRING IS NULL OR actor_id = PARAMS:filters:actor::STRING)
        AND (PARAMS:filters:action::STRING IS NULL OR action = PARAMS:filters:action::STRING)
        AND (PARAMS:filters:source::STRING IS NULL OR source = PARAMS:filters:source::STRING)
      GROUP BY time_bucket
      ORDER BY time_bucket
    )
  );
END;

-- =====================================================
-- 2. DASH_GET_TOPN - Top N dimension values (Simplified)
-- =====================================================
-- @statement
CREATE OR REPLACE PROCEDURE MCP.DASH_GET_TOPN(PARAMS VARIANT)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
BEGIN
  -- For simplicity, handle only action dimension initially
  -- Can be extended with dynamic SQL if needed
  LET res RESULTSET := (
    SELECT 
      CASE 
        WHEN COALESCE(PARAMS:dimension::STRING, 'action') = 'action' THEN action
        WHEN PARAMS:dimension::STRING IN ('actor', 'actor_id') THEN actor_id
        WHEN PARAMS:dimension::STRING = 'source' THEN source
        WHEN PARAMS:dimension::STRING = 'object_type' THEN object_type
        ELSE action
      END AS dimension_value,
      COUNT(*) AS cnt
    FROM ACTIVITY.EVENTS
    WHERE occurred_at BETWEEN PARAMS:start_ts::TIMESTAMP_TZ AND PARAMS:end_ts::TIMESTAMP_TZ
      AND (PARAMS:filters:actor::STRING IS NULL OR actor_id = PARAMS:filters:actor::STRING)
      AND (PARAMS:filters:action::STRING IS NULL OR action = PARAMS:filters:action::STRING)
      AND (PARAMS:filters:source::STRING IS NULL OR source = PARAMS:filters:source::STRING)
    GROUP BY dimension_value
    ORDER BY cnt DESC
    LIMIT 50
  );
  
  RETURN (
    SELECT OBJECT_CONSTRUCT(
      'ok', TRUE,
      'data', ARRAY_AGG(
        OBJECT_CONSTRUCT(
          'dimension', dimension_value,
          'count', cnt
        )
      ),
      'metadata', OBJECT_CONSTRUCT(
        'dimension', COALESCE(PARAMS:dimension::STRING, 'action'),
        'n', LEAST(COALESCE(PARAMS:n::NUMBER, 10), 50),
        'start_ts', PARAMS:start_ts::STRING,
        'end_ts', PARAMS:end_ts::STRING
      )
    )
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
  );
END;

-- =====================================================
-- 3. DASH_GET_EVENTS - Recent events (Simplified)
-- =====================================================
-- @statement
CREATE OR REPLACE PROCEDURE MCP.DASH_GET_EVENTS(PARAMS VARIANT)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
BEGIN
  RETURN (
    SELECT OBJECT_CONSTRUCT(
      'ok', TRUE,
      'data', ARRAY_AGG(
        OBJECT_CONSTRUCT(
          'event_id', event_id,
          'action', action,
          'actor_id', actor_id,
          'object_type', object_type,
          'object_id', object_id,
          'occurred_at', occurred_at,
          'source', source
        )
      ),
      'metadata', OBJECT_CONSTRUCT(
        'cursor_ts', PARAMS:cursor_ts::STRING,
        'limit', LEAST(COALESCE(PARAMS:limit::NUMBER, 100), 5000),
        'row_count', COUNT(*)
      )
    )
    FROM (
      SELECT *
      FROM ACTIVITY.EVENTS
      WHERE occurred_at <= PARAMS:cursor_ts::TIMESTAMP_TZ
      ORDER BY occurred_at DESC
      LIMIT 5000
    )
  );
END;

-- =====================================================
-- 4. DASH_GET_METRICS - Summary metrics (Simplified)
-- =====================================================
-- @statement
CREATE OR REPLACE PROCEDURE MCP.DASH_GET_METRICS(PARAMS VARIANT)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
BEGIN
  RETURN (
    SELECT OBJECT_CONSTRUCT(
      'ok', TRUE,
      'data', OBJECT_CONSTRUCT(
        'total_events', COUNT(*),
        'unique_actors', COUNT(DISTINCT actor_id),
        'unique_actions', COUNT(DISTINCT action),
        'unique_objects', COUNT(DISTINCT object_id),
        'avg_events_per_hour', 
          COUNT(*) / NULLIF(TIMESTAMPDIFF(hour, PARAMS:start_ts::TIMESTAMP_TZ, PARAMS:end_ts::TIMESTAMP_TZ), 0),
        'most_common_action', MODE(action),
        'most_active_actor', MODE(actor_id),
        'time_range_hours', TIMESTAMPDIFF(hour, PARAMS:start_ts::TIMESTAMP_TZ, PARAMS:end_ts::TIMESTAMP_TZ)
      ),
      'metadata', OBJECT_CONSTRUCT(
        'start_ts', PARAMS:start_ts::STRING,
        'end_ts', PARAMS:end_ts::STRING,
        'filters', COALESCE(PARAMS:filters, OBJECT_CONSTRUCT())
      )
    )
    FROM ACTIVITY.EVENTS
    WHERE occurred_at BETWEEN PARAMS:start_ts::TIMESTAMP_TZ AND PARAMS:end_ts::TIMESTAMP_TZ
      AND (PARAMS:filters:actor::STRING IS NULL OR actor_id = PARAMS:filters:actor::STRING)
      AND (PARAMS:filters:action::STRING IS NULL OR action = PARAMS:filters:action::STRING)
      AND (PARAMS:filters:source::STRING IS NULL OR source = PARAMS:filters:source::STRING)
  );
END;

-- =====================================================
-- 5. LOG_CLAUDE_EVENT - Log events from Claude Code
-- =====================================================
-- @statement
CREATE OR REPLACE PROCEDURE MCP.LOG_CLAUDE_EVENT(EVENT_DATA VARIANT)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
BEGIN
  -- Insert event into RAW_EVENTS
  INSERT INTO LANDING.RAW_EVENTS (raw_event, source, loaded_at)
  VALUES (
    OBJECT_INSERT(EVENT_DATA, 'event_id', COALESCE(EVENT_DATA:event_id::STRING, UUID_STRING())),
    'CLAUDE_CODE',
    CURRENT_TIMESTAMP()
  );
  
  RETURN OBJECT_CONSTRUCT(
    'ok', TRUE,
    'event_id', COALESCE(EVENT_DATA:event_id::STRING, UUID_STRING()),
    'message', 'Event logged successfully'
  );
END;

-- =====================================================
-- Grant permissions
-- =====================================================
-- @statement
GRANT USAGE ON PROCEDURE MCP.DASH_GET_SERIES(VARIANT) TO ROLE CLAUDE_BI_ROLE;

-- @statement
GRANT USAGE ON PROCEDURE MCP.DASH_GET_TOPN(VARIANT) TO ROLE CLAUDE_BI_ROLE;

-- @statement
GRANT USAGE ON PROCEDURE MCP.DASH_GET_EVENTS(VARIANT) TO ROLE CLAUDE_BI_ROLE;

-- @statement
GRANT USAGE ON PROCEDURE MCP.DASH_GET_METRICS(VARIANT) TO ROLE CLAUDE_BI_ROLE;

-- @statement
GRANT USAGE ON PROCEDURE MCP.LOG_CLAUDE_EVENT(VARIANT) TO ROLE CLAUDE_BI_ROLE;