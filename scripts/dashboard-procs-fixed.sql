-- Dashboard Stored Procedures (Fixed)
-- Using simpler return types and correct syntax

-- @statement
USE DATABASE CLAUDE_BI;

-- @statement
USE SCHEMA MCP;

-- =====================================================
-- Dashboard View (reads from EVENTS table) 
-- =====================================================
-- @statement
CREATE OR REPLACE VIEW MCP.VW_DASHBOARDS AS
WITH latest_dashboards AS (
  SELECT 
    object_id AS dashboard_id,
    attributes:title::STRING AS title,
    attributes:spec AS spec,
    occurred_at AS created_at,
    actor_id AS created_by,
    COALESCE(attributes:refresh_interval_sec::NUMBER, 300) AS refresh_interval_sec,
    COALESCE(attributes:is_active::BOOLEAN, TRUE) AS is_active,
    ROW_NUMBER() OVER (PARTITION BY object_id ORDER BY occurred_at DESC) AS rn
  FROM ACTIVITY.EVENTS
  WHERE action IN ('dashboard.created', 'dashboard.updated')
    AND object_type = 'dashboard'
)
SELECT 
  dashboard_id,
  title,
  spec,
  created_at,
  created_by,
  refresh_interval_sec,
  is_active
FROM latest_dashboards
WHERE rn = 1 AND is_active = TRUE;

-- =====================================================
-- 1. DASH_GET_SERIES - Time series data (Simplified)
-- =====================================================
-- @statement
CREATE OR REPLACE PROCEDURE MCP.DASH_GET_SERIES(
  start_ts TIMESTAMP_TZ,
  end_ts TIMESTAMP_TZ,
  interval_str STRING,
  filters STRING,
  group_by STRING
)
RETURNS TABLE(time_bucket TIMESTAMP_TZ, event_count NUMBER, dimension STRING)
LANGUAGE SQL
AS
$$
DECLARE
  res RESULTSET;
BEGIN
  -- Set query tag
  ALTER SESSION SET QUERY_TAG = CONCAT('dash:series|interval:', interval_str);
  
  -- Execute query
  IF (group_by IS NOT NULL) THEN
    res := (
      SELECT 
        TIME_SLICE(occurred_at, 1, interval_str) AS time_bucket,
        COUNT(*) AS event_count,
        ANY_VALUE(group_by) AS dimension
      FROM ACTIVITY.EVENTS 
      WHERE occurred_at BETWEEN start_ts AND end_ts
      GROUP BY time_bucket, dimension
      ORDER BY time_bucket
    );
  ELSE
    res := (
      SELECT 
        TIME_SLICE(occurred_at, 1, interval_str) AS time_bucket,
        COUNT(*) AS event_count,
        NULL AS dimension
      FROM ACTIVITY.EVENTS 
      WHERE occurred_at BETWEEN start_ts AND end_ts
      GROUP BY time_bucket
      ORDER BY time_bucket
    );
  END IF;
  
  RETURN TABLE(res);
END;
$$;

-- =====================================================
-- 2. DASH_GET_TOPN - Ranking/Top-N data (Simplified)
-- =====================================================
-- @statement
CREATE OR REPLACE PROCEDURE MCP.DASH_GET_TOPN(
  start_ts TIMESTAMP_TZ,
  end_ts TIMESTAMP_TZ,
  dimension STRING,
  filters STRING,
  n NUMBER
)
RETURNS TABLE(dimension_value STRING, cnt NUMBER)
LANGUAGE SQL
AS
$$
DECLARE
  res RESULTSET;
BEGIN
  -- Set query tag
  ALTER SESSION SET QUERY_TAG = CONCAT('dash:topn|dim:', dimension);
  
  -- Execute based on dimension
  IF (dimension = 'action') THEN
    res := (
      SELECT action AS dimension_value, COUNT(*) AS cnt
      FROM ACTIVITY.EVENTS
      WHERE occurred_at BETWEEN start_ts AND end_ts
      GROUP BY action
      ORDER BY cnt DESC
      LIMIT n
    );
  ELSEIF (dimension = 'actor_id') THEN
    res := (
      SELECT actor_id AS dimension_value, COUNT(*) AS cnt
      FROM ACTIVITY.EVENTS
      WHERE occurred_at BETWEEN start_ts AND end_ts
      GROUP BY actor_id
      ORDER BY cnt DESC
      LIMIT n
    );
  ELSE
    res := (
      SELECT object_type AS dimension_value, COUNT(*) AS cnt
      FROM ACTIVITY.EVENTS
      WHERE occurred_at BETWEEN start_ts AND end_ts
      GROUP BY object_type
      ORDER BY cnt DESC
      LIMIT n
    );
  END IF;
  
  RETURN TABLE(res);
END;
$$;

-- =====================================================
-- 3. DASH_GET_EVENTS - Recent events stream (Simplified)
-- =====================================================
-- @statement
CREATE OR REPLACE PROCEDURE MCP.DASH_GET_EVENTS(
  cursor_ts TIMESTAMP_TZ,
  limit_rows NUMBER
)
RETURNS TABLE(
  event_id STRING,
  occurred_at TIMESTAMP_TZ,
  action STRING,
  actor_id STRING,
  object_type STRING,
  object_id STRING
)
LANGUAGE SQL
AS
$$
DECLARE
  res RESULTSET;
BEGIN
  -- Set query tag
  ALTER SESSION SET QUERY_TAG = 'dash:events|stream';
  
  res := (
    SELECT 
      event_id,
      occurred_at,
      action,
      actor_id,
      object_type,
      object_id
    FROM ACTIVITY.EVENTS
    WHERE occurred_at >= cursor_ts
    ORDER BY occurred_at DESC
    LIMIT limit_rows
  );
  
  RETURN TABLE(res);
END;
$$;

-- =====================================================
-- 4. DASH_GET_METRICS - Summary metrics (Simplified)
-- =====================================================
-- @statement
CREATE OR REPLACE PROCEDURE MCP.DASH_GET_METRICS(
  start_ts TIMESTAMP_TZ,
  end_ts TIMESTAMP_TZ,
  filters STRING
)
RETURNS TABLE(
  metric STRING,
  value NUMBER,
  label STRING
)
LANGUAGE SQL
AS
$$
DECLARE
  res RESULTSET;
BEGIN
  -- Set query tag
  ALTER SESSION SET QUERY_TAG = 'dash:metrics|summary';
  
  res := (
    SELECT 
      'total_events' AS metric,
      COUNT(*) AS value,
      'Total Events' AS label
    FROM ACTIVITY.EVENTS
    WHERE occurred_at BETWEEN start_ts AND end_ts
    UNION ALL
    SELECT 
      'unique_actors' AS metric,
      COUNT(DISTINCT actor_id) AS value,
      'Unique Actors' AS label
    FROM ACTIVITY.EVENTS
    WHERE occurred_at BETWEEN start_ts AND end_ts
    UNION ALL
    SELECT 
      'unique_actions' AS metric,
      COUNT(DISTINCT action) AS value,
      'Unique Actions' AS label
    FROM ACTIVITY.EVENTS
    WHERE occurred_at BETWEEN start_ts AND end_ts
  );
  
  RETURN TABLE(res);
END;
$$;

-- =====================================================
-- Save Dashboard Procedure (using EVENTS)
-- =====================================================
-- @statement
CREATE OR REPLACE PROCEDURE MCP.SAVE_DASHBOARD(
  dashboard_id STRING,
  title STRING,
  spec STRING,
  refresh_interval_sec NUMBER
)
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
  -- Save dashboard as an event
  INSERT INTO LANDING.RAW_EVENTS (event_payload, source_system, ingested_at)
  VALUES (
    OBJECT_CONSTRUCT(
      'event_id', UUID_STRING(),
      'action', 'dashboard.created',
      'actor_id', CURRENT_USER(),
      'object', OBJECT_CONSTRUCT(
        'type', 'dashboard',
        'id', dashboard_id
      ),
      'attributes', OBJECT_CONSTRUCT(
        'title', title,
        'spec', PARSE_JSON(spec),
        'refresh_interval_sec', refresh_interval_sec,
        'is_active', TRUE
      ),
      'occurred_at', CURRENT_TIMESTAMP()
    ),
    'DASHBOARD_SYSTEM',
    CURRENT_TIMESTAMP()
  );
  
  -- Log the event
  CALL LOG_CLAUDE_EVENT(OBJECT_CONSTRUCT(
    'action', 'dashboard.saved',
    'object', OBJECT_CONSTRUCT('type', 'dashboard', 'id', dashboard_id),
    'attributes', OBJECT_CONSTRUCT(
      'title', title,
      'refresh_sec', refresh_interval_sec
    )
  ), 'DASHBOARD');
  
  RETURN dashboard_id;
END;
$$;

-- =====================================================
-- Get Dashboard Procedure
-- =====================================================
-- @statement
CREATE OR REPLACE PROCEDURE MCP.GET_DASHBOARD(dashboard_id STRING)
RETURNS TABLE(
  dashboard_id STRING,
  title STRING,
  spec VARIANT,
  refresh_interval_sec NUMBER,
  created_at TIMESTAMP_TZ,
  created_by STRING
)
LANGUAGE SQL
AS
$$
DECLARE
  res RESULTSET;
BEGIN
  res := (
    SELECT 
      dashboard_id,
      title,
      spec,
      refresh_interval_sec,
      created_at,
      created_by
    FROM MCP.VW_DASHBOARDS
    WHERE dashboard_id = :dashboard_id
  );
  
  RETURN TABLE(res);
END;
$$;