-- ===================================================================
-- ESSENTIAL NATIVE PROCEDURES DEPLOYMENT
-- Minimal deployment for all-Snowflake native architecture
-- ===================================================================

USE DATABASE CLAUDE_BI;
USE SCHEMA MCP;

-- ===================================================================
-- 1. SIMPLIFIED RUN_PLAN (no External Access required)
-- ===================================================================

CREATE OR REPLACE PROCEDURE MCP.RUN_PLAN(PLAN VARIANT)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
BEGIN
  -- Set session context
  ALTER SESSION SET QUERY_TAG = 'dash-api|proc:RUN_PLAN|agent:claude';
  
  -- Extract procedure name
  LET proc_name := :PLAN:proc::STRING;
  LET params := :PLAN:params;
  
  -- Validate procedure whitelist
  IF (:proc_name NOT IN ('DASH_GET_SERIES', 'DASH_GET_TOPN', 'DASH_GET_EVENTS', 'DASH_GET_METRICS')) THEN
    RETURN OBJECT_CONSTRUCT('ok', FALSE, 'error', 'Disallowed procedure: ' || :proc_name);
  END IF;
  
  -- Execute the procedure
  CASE :proc_name
    WHEN 'DASH_GET_SERIES' THEN
      RETURN (CALL MCP.DASH_GET_SERIES(:params));
    WHEN 'DASH_GET_TOPN' THEN
      RETURN (CALL MCP.DASH_GET_TOPN(:params));
    WHEN 'DASH_GET_EVENTS' THEN
      RETURN (CALL MCP.DASH_GET_EVENTS(:params));
    WHEN 'DASH_GET_METRICS' THEN
      RETURN (CALL MCP.DASH_GET_METRICS(:params));
    ELSE
      RETURN OBJECT_CONSTRUCT('ok', FALSE, 'error', 'Unknown procedure');
  END CASE;
END;

-- ===================================================================
-- 2. SIMPLIFIED COMPILE_NL_PLAN (fallback mode without Claude API)
-- ===================================================================

CREATE OR REPLACE PROCEDURE MCP.COMPILE_NL_PLAN(INTENT VARIANT)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
BEGIN
  -- Extract text from intent
  LET text := LOWER(COALESCE(:INTENT:text::STRING, ''));
  
  -- Default time range
  LET end_ts := CURRENT_TIMESTAMP();
  LET start_ts := DATEADD('hour', -24, :end_ts);
  
  -- Simple pattern matching
  LET proc := CASE
    WHEN CONTAINS(:text, 'series') OR CONTAINS(:text, 'trend') OR CONTAINS(:text, 'hour') THEN 'DASH_GET_SERIES'
    WHEN CONTAINS(:text, 'top') OR CONTAINS(:text, 'ranking') THEN 'DASH_GET_TOPN'
    WHEN CONTAINS(:text, 'event') OR CONTAINS(:text, 'recent') THEN 'DASH_GET_EVENTS'
    WHEN CONTAINS(:text, 'metric') OR CONTAINS(:text, 'kpi') THEN 'DASH_GET_METRICS'
    ELSE 'DASH_GET_TOPN'
  END;
  
  -- Build plan based on procedure type
  LET plan := CASE :proc
    WHEN 'DASH_GET_SERIES' THEN
      OBJECT_CONSTRUCT(
        'proc', :proc,
        'params', OBJECT_CONSTRUCT(
          'start_ts', :start_ts,
          'end_ts', :end_ts,
          'interval', 'hour',
          'filters', OBJECT_CONSTRUCT()
        )
      )
    WHEN 'DASH_GET_TOPN' THEN
      OBJECT_CONSTRUCT(
        'proc', :proc,
        'params', OBJECT_CONSTRUCT(
          'start_ts', :start_ts,
          'end_ts', :end_ts,
          'dimension', 'action',
          'n', 10,
          'filters', OBJECT_CONSTRUCT()
        )
      )
    WHEN 'DASH_GET_EVENTS' THEN
      OBJECT_CONSTRUCT(
        'proc', :proc,
        'params', OBJECT_CONSTRUCT(
          'cursor_ts', :end_ts,
          'limit', 100
        )
      )
    WHEN 'DASH_GET_METRICS' THEN
      OBJECT_CONSTRUCT(
        'proc', :proc,
        'params', OBJECT_CONSTRUCT(
          'start_ts', :start_ts,
          'end_ts', :end_ts,
          'filters', OBJECT_CONSTRUCT()
        )
      )
    ELSE
      OBJECT_CONSTRUCT(
        'proc', 'DASH_GET_METRICS',
        'params', OBJECT_CONSTRUCT(
          'start_ts', :start_ts,
          'end_ts', :end_ts,
          'filters', OBJECT_CONSTRUCT()
        )
      )
  END;
  
  RETURN OBJECT_CONSTRUCT(
    'ok', TRUE,
    'plan', :plan,
    'source', 'fallback',
    'original_text', :text
  );
END;

-- ===================================================================
-- 3. SAVE_DASHBOARD_SPEC
-- ===================================================================

CREATE OR REPLACE PROCEDURE MCP.SAVE_DASHBOARD_SPEC(SPEC VARIANT)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
BEGIN
  -- Generate dashboard ID
  LET dashboard_id := 'dash_' || UUID_STRING();
  
  -- Log dashboard creation event
  INSERT INTO LANDING.RAW_EVENTS (raw_event, source, loaded_at)
  VALUES (
    OBJECT_CONSTRUCT(
      'event_id', UUID_STRING(),
      'action', 'dashboard.created',
      'actor_id', 'CLAUDE_CODE_AI_AGENT',
      'object', OBJECT_CONSTRUCT('type', 'dashboard', 'id', :dashboard_id),
      'attributes', OBJECT_CONSTRUCT(
        'title', COALESCE(:SPEC:title::STRING, 'Untitled Dashboard'),
        'panels', :SPEC:panels,
        'stage_path', '@MCP.DASH_SPECS/' || :dashboard_id || '.json',
        'panel_count', ARRAY_SIZE(COALESCE(:SPEC:panels, ARRAY_CONSTRUCT())),
        'status', 'success'
      ),
      'occurred_at', CURRENT_TIMESTAMP()
    ),
    'CLAUDE_CODE',
    CURRENT_TIMESTAMP()
  );
  
  RETURN OBJECT_CONSTRUCT(
    'ok', TRUE,
    'dashboard_id', :dashboard_id,
    'stage_path', '@MCP.DASH_SPECS/' || :dashboard_id || '.json'
  );
END;

-- ===================================================================
-- 4. LIST_DASHBOARDS
-- ===================================================================

CREATE OR REPLACE PROCEDURE MCP.LIST_DASHBOARDS()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
BEGIN
  RETURN OBJECT_CONSTRUCT(
    'ok', TRUE,
    'dashboards', (
      SELECT ARRAY_AGG(
        OBJECT_CONSTRUCT(
          'dashboard_id', object_id,
          'title', attributes:title::string,
          'panel_count', attributes:panel_count::int,
          'created_at', occurred_at
        )
      )
      FROM (
        SELECT object_id, attributes, occurred_at,
               ROW_NUMBER() OVER (PARTITION BY object_id ORDER BY occurred_at DESC) as rn
        FROM ACTIVITY.EVENTS
        WHERE action = 'dashboard.created'
        QUALIFY rn = 1
        ORDER BY occurred_at DESC
        LIMIT 100
      )
    )
  );
END;

-- ===================================================================
-- 5. CREATE_DASHBOARD_SCHEDULE
-- ===================================================================

CREATE OR REPLACE PROCEDURE MCP.CREATE_DASHBOARD_SCHEDULE(SCHEDULE VARIANT)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
BEGIN
  -- Generate schedule ID
  LET schedule_id := 'sched_' || UUID_STRING();
  
  -- Log schedule creation event
  INSERT INTO LANDING.RAW_EVENTS (raw_event, source, loaded_at)
  VALUES (
    OBJECT_CONSTRUCT(
      'event_id', UUID_STRING(),
      'action', 'dashboard.schedule_created',
      'actor_id', 'CLAUDE_CODE_AI_AGENT',
      'object', OBJECT_CONSTRUCT('type', 'schedule', 'id', :schedule_id),
      'attributes', OBJECT_CONSTRUCT(
        'dashboard_id', :SCHEDULE:dashboard_id::STRING,
        'frequency', COALESCE(:SCHEDULE:frequency::STRING, 'DAILY'),
        'time', COALESCE(:SCHEDULE:time::STRING, '09:00'),
        'timezone', COALESCE(:SCHEDULE:timezone::STRING, 'UTC'),
        'deliveries', COALESCE(:SCHEDULE:deliveries, ARRAY_CONSTRUCT('email')),
        'recipients', COALESCE(:SCHEDULE:recipients, ARRAY_CONSTRUCT()),
        'stage_path', '@MCP.DASH_COHORTS/' || :schedule_id || '.json',
        'status', 'success'
      ),
      'occurred_at', CURRENT_TIMESTAMP()
    ),
    'CLAUDE_CODE',
    CURRENT_TIMESTAMP()
  );
  
  RETURN OBJECT_CONSTRUCT(
    'ok', TRUE,
    'schedule_id', :schedule_id,
    'stage_path', '@MCP.DASH_COHORTS/' || :schedule_id || '.json'
  );
END;

-- ===================================================================
-- 6. GRANTS
-- ===================================================================

GRANT EXECUTE ON PROCEDURE MCP.RUN_PLAN(VARIANT) TO ROLE R_CLAUDE_AGENT;
GRANT EXECUTE ON PROCEDURE MCP.COMPILE_NL_PLAN(VARIANT) TO ROLE R_CLAUDE_AGENT;
GRANT EXECUTE ON PROCEDURE MCP.SAVE_DASHBOARD_SPEC(VARIANT) TO ROLE R_CLAUDE_AGENT;
GRANT EXECUTE ON PROCEDURE MCP.LIST_DASHBOARDS() TO ROLE R_CLAUDE_AGENT;
GRANT EXECUTE ON PROCEDURE MCP.CREATE_DASHBOARD_SCHEDULE(VARIANT) TO ROLE R_CLAUDE_AGENT;

-- ===================================================================
-- 7. VALIDATION
-- ===================================================================

-- Test RUN_PLAN
CALL MCP.RUN_PLAN(PARSE_JSON('{
  "proc": "DASH_GET_METRICS",
  "params": {
    "start_ts": "2025-01-15T00:00:00Z",
    "end_ts": "2025-01-16T00:00:00Z",
    "filters": {}
  }
}'));

-- Test COMPILE_NL_PLAN
CALL MCP.COMPILE_NL_PLAN(PARSE_JSON('{"text": "show top 10 actions"}'));

-- Test LIST_DASHBOARDS
CALL MCP.LIST_DASHBOARDS();