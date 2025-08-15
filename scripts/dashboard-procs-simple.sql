-- Simplified Dashboard Stored Procedures
-- Testing basic syntax first

-- @statement
USE DATABASE CLAUDE_BI;

-- @statement
USE SCHEMA MCP;

-- @statement
CREATE OR REPLACE PROCEDURE DASH_GET_SERIES(
  start_ts TIMESTAMP_TZ,
  end_ts TIMESTAMP_TZ,
  interval_str STRING,
  filters VARIANT,
  group_by STRING
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
BEGIN
  -- Simple implementation for testing
  LET result_data VARIANT;
  
  -- Basic query without dynamic SQL first
  result_data := (
    SELECT ARRAY_AGG(
      OBJECT_CONSTRUCT(
        'time_bucket', TIME_SLICE(occurred_at, 1, :interval_str),
        'event_count', COUNT(*)
      )
    )
    FROM ACTIVITY.EVENTS 
    WHERE occurred_at BETWEEN :start_ts AND :end_ts
    GROUP BY TIME_SLICE(occurred_at, 1, :interval_str)
    ORDER BY TIME_SLICE(occurred_at, 1, :interval_str)
  );
  
  RETURN OBJECT_CONSTRUCT(
    'ok', TRUE,
    'data', result_data,
    'query_id', LAST_QUERY_ID()
  );
  
EXCEPTION
  WHEN OTHER THEN
    RETURN OBJECT_CONSTRUCT('ok', FALSE, 'error', SQLERRM);
END;
$$;

-- @statement
CREATE OR REPLACE PROCEDURE DASH_GET_TOPN(
  start_ts TIMESTAMP_TZ,
  end_ts TIMESTAMP_TZ,
  dimension STRING,
  filters VARIANT,
  n NUMBER
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
BEGIN
  LET result_data VARIANT;
  
  -- Simple top-N without dynamic dimension
  result_data := (
    SELECT ARRAY_AGG(
      OBJECT_CONSTRUCT(
        'item', action,
        'count', event_count
      )
    )
    FROM (
      SELECT action, COUNT(*) as event_count
      FROM ACTIVITY.EVENTS
      WHERE occurred_at BETWEEN :start_ts AND :end_ts
      GROUP BY action
      ORDER BY event_count DESC
      LIMIT :n
    )
  );
  
  RETURN OBJECT_CONSTRUCT(
    'ok', TRUE,
    'data', result_data,
    'query_id', LAST_QUERY_ID()
  );
  
EXCEPTION
  WHEN OTHER THEN
    RETURN OBJECT_CONSTRUCT('ok', FALSE, 'error', SQLERRM);
END;
$$;

-- @statement
CREATE OR REPLACE PROCEDURE DASH_GET_EVENTS(
  cursor_ts TIMESTAMP_TZ,
  limit_rows NUMBER
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
BEGIN
  LET result_data VARIANT;
  
  result_data := (
    SELECT ARRAY_AGG(
      OBJECT_CONSTRUCT(
        'event_id', event_id,
        'occurred_at', occurred_at,
        'action', action,
        'actor_id', actor_id
      )
    )
    FROM (
      SELECT event_id, occurred_at, action, actor_id
      FROM ACTIVITY.EVENTS
      WHERE occurred_at > :cursor_ts
      ORDER BY occurred_at DESC
      LIMIT :limit_rows
    )
  );
  
  RETURN OBJECT_CONSTRUCT(
    'ok', TRUE,
    'data', result_data,
    'cursor', (SELECT MAX(occurred_at) FROM ACTIVITY.EVENTS WHERE occurred_at > :cursor_ts)
  );
  
EXCEPTION
  WHEN OTHER THEN
    RETURN OBJECT_CONSTRUCT('ok', FALSE, 'error', SQLERRM);
END;
$$;

-- @statement
CREATE OR REPLACE PROCEDURE DASH_GET_METRICS(
  start_ts TIMESTAMP_TZ,
  end_ts TIMESTAMP_TZ,
  filters VARIANT
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
BEGIN
  LET total_events NUMBER;
  LET unique_actors NUMBER;
  LET top_action STRING;
  
  -- Get metrics
  SELECT COUNT(*) INTO total_events
  FROM ACTIVITY.EVENTS
  WHERE occurred_at BETWEEN :start_ts AND :end_ts;
  
  SELECT COUNT(DISTINCT actor_id) INTO unique_actors
  FROM ACTIVITY.EVENTS
  WHERE occurred_at BETWEEN :start_ts AND :end_ts;
  
  SELECT action INTO top_action
  FROM (
    SELECT action, COUNT(*) as cnt
    FROM ACTIVITY.EVENTS
    WHERE occurred_at BETWEEN :start_ts AND :end_ts
    GROUP BY action
    ORDER BY cnt DESC
    LIMIT 1
  );
  
  RETURN OBJECT_CONSTRUCT(
    'ok', TRUE,
    'data', OBJECT_CONSTRUCT(
      'total_events', total_events,
      'unique_actors', unique_actors,
      'top_action', top_action,
      'period_start', start_ts,
      'period_end', end_ts
    )
  );
  
EXCEPTION
  WHEN OTHER THEN
    RETURN OBJECT_CONSTRUCT('ok', FALSE, 'error', SQLERRM);
END;
$$;