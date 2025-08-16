-- Fix DASH_GET_METRICS procedure
-- The procedure has undefined variable issues

-- @statement
USE DATABASE CLAUDE_BI;

-- @statement
USE SCHEMA MCP;

-- @statement
CREATE OR REPLACE PROCEDURE MCP.DASH_GET_METRICS(
  p_start_ts TIMESTAMP_TZ,
  p_end_ts TIMESTAMP_TZ,
  p_filters VARIANT
)
RETURNS VARIANT
LANGUAGE SQL
AS
'BEGIN
  LET total_events NUMBER;
  LET unique_actors NUMBER;
  LET unique_actions NUMBER;
  
  SELECT 
    COUNT(*) AS total,
    COUNT(DISTINCT actor_id) AS actors,
    COUNT(DISTINCT action) AS actions
  INTO 
    total_events,
    unique_actors,
    unique_actions
  FROM ACTIVITY.EVENTS
  WHERE occurred_at BETWEEN :p_start_ts AND :p_end_ts;
  
  RETURN OBJECT_CONSTRUCT(
    ''ok'', TRUE,
    ''data'', OBJECT_CONSTRUCT(
      ''total_events'', total_events,
      ''unique_actors'', unique_actors,
      ''unique_actions'', unique_actions,
      ''period_start'', p_start_ts,
      ''period_end'', p_end_ts
    )
  );
END;';