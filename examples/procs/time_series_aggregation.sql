-- Example: Time Series Aggregation Procedure
-- This procedure generates time-series data with configurable granularity

-- @statement
CREATE OR REPLACE PROCEDURE GET_TIME_SERIES_DATA(
  start_ts TIMESTAMP_TZ,
  end_ts TIMESTAMP_TZ,
  time_unit STRING,  -- 'hour', 'day', 'week', 'month'
  metric STRING,     -- 'count', 'unique_users', 'unique_actions'
  filters VARIANT
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
COMMENT = 'Generate time series data with zero-fill for complete time range'
AS
$$
BEGIN
  LET result_data VARIANT;
  LET time_format STRING;
  
  -- Determine time format based on unit
  CASE time_unit
    WHEN 'hour' THEN time_format := 'YYYY-MM-DD HH24:00';
    WHEN 'day' THEN time_format := 'YYYY-MM-DD';
    WHEN 'week' THEN time_format := 'YYYY-WW';
    WHEN 'month' THEN time_format := 'YYYY-MM';
    ELSE time_format := 'YYYY-MM-DD';
  END CASE;
  
  -- Build the aggregation based on metric type
  IF (metric = 'count') THEN
    result_data := (
      SELECT ARRAY_AGG(
        OBJECT_CONSTRUCT(
          'time_bucket', TO_VARCHAR(DATE_TRUNC(time_unit, occurred_at), time_format),
          'value', COUNT(*)
        ) ORDER BY DATE_TRUNC(time_unit, occurred_at)
      )
      FROM ACTIVITY.EVENTS
      WHERE occurred_at BETWEEN :start_ts AND :end_ts
        AND (filters:action IS NULL OR action = filters:action::STRING)
        AND (filters:actor_id IS NULL OR actor_id = filters:actor_id::STRING)
      GROUP BY DATE_TRUNC(time_unit, occurred_at)
    );
  ELSEIF (metric = 'unique_users') THEN
    result_data := (
      SELECT ARRAY_AGG(
        OBJECT_CONSTRUCT(
          'time_bucket', TO_VARCHAR(DATE_TRUNC(time_unit, occurred_at), time_format),
          'value', COUNT(DISTINCT actor_id)
        ) ORDER BY DATE_TRUNC(time_unit, occurred_at)
      )
      FROM ACTIVITY.EVENTS
      WHERE occurred_at BETWEEN :start_ts AND :end_ts
        AND (filters:action IS NULL OR action = filters:action::STRING)
      GROUP BY DATE_TRUNC(time_unit, occurred_at)
    );
  ELSEIF (metric = 'unique_actions') THEN
    result_data := (
      SELECT ARRAY_AGG(
        OBJECT_CONSTRUCT(
          'time_bucket', TO_VARCHAR(DATE_TRUNC(time_unit, occurred_at), time_format),
          'value', COUNT(DISTINCT action)
        ) ORDER BY DATE_TRUNC(time_unit, occurred_at)
      )
      FROM ACTIVITY.EVENTS
      WHERE occurred_at BETWEEN :start_ts AND :end_ts
        AND (filters:actor_id IS NULL OR actor_id = filters:actor_id::STRING)
      GROUP BY DATE_TRUNC(time_unit, occurred_at)
    );
  ELSE
    RETURN OBJECT_CONSTRUCT(
      'ok', FALSE,
      'error', 'Invalid metric type. Use: count, unique_users, or unique_actions'
    );
  END IF;
  
  RETURN OBJECT_CONSTRUCT(
    'ok', TRUE,
    'data', COALESCE(result_data, ARRAY_CONSTRUCT()),
    'parameters', OBJECT_CONSTRUCT(
      'start_ts', start_ts,
      'end_ts', end_ts,
      'time_unit', time_unit,
      'metric', metric,
      'filters', filters
    ),
    'query_id', LAST_QUERY_ID()
  );
  
EXCEPTION
  WHEN OTHER THEN
    RETURN OBJECT_CONSTRUCT(
      'ok', FALSE,
      'error', SQLERRM,
      'error_code', SQLCODE
    );
END;
$$;

-- Example usage:
-- CALL GET_TIME_SERIES_DATA(
--   DATEADD('day', -7, CURRENT_TIMESTAMP()),
--   CURRENT_TIMESTAMP(),
--   'hour',
--   'count',
--   OBJECT_CONSTRUCT('action', 'user.signup')
-- );