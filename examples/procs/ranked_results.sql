-- Example: Ranked Results with Window Functions
-- This procedure returns top items with rankings and percentiles

-- @statement
CREATE OR REPLACE PROCEDURE GET_RANKED_RESULTS(
  start_ts TIMESTAMP_TZ,
  end_ts TIMESTAMP_TZ,
  dimension STRING,      -- Column to rank by (e.g., 'action', 'actor_id')
  metric STRING,        -- Metric to calculate ('event_count', 'unique_sessions')
  top_n NUMBER,
  include_percentiles BOOLEAN
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
COMMENT = 'Returns ranked results with optional percentile calculations'
AS
$$
BEGIN
  LET result_data VARIANT;
  
  -- Different metrics require different aggregations
  IF (metric = 'event_count') THEN
    IF (include_percentiles) THEN
      result_data := (
        SELECT ARRAY_AGG(
          OBJECT_CONSTRUCT(
            'rank', rank_num,
            'item', item_name,
            'count', item_count,
            'percentage', ROUND(100.0 * item_count / SUM(item_count) OVER(), 2),
            'cumulative_percentage', ROUND(100.0 * SUM(item_count) OVER (ORDER BY rank_num) / SUM(item_count) OVER(), 2),
            'percentile', ROUND(PERCENT_RANK() OVER (ORDER BY item_count DESC) * 100, 2)
          ) ORDER BY rank_num
        )
        FROM (
          SELECT 
            ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) AS rank_num,
            GET(SPLIT(:dimension, '.'), 0) AS item_name,
            COUNT(*) AS item_count
          FROM ACTIVITY.EVENTS
          WHERE occurred_at BETWEEN :start_ts AND :end_ts
          GROUP BY item_name
          QUALIFY rank_num <= :top_n
        )
      );
    ELSE
      result_data := (
        SELECT ARRAY_AGG(
          OBJECT_CONSTRUCT(
            'rank', rank_num,
            'item', item_name,
            'count', item_count,
            'percentage', ROUND(100.0 * item_count / SUM(item_count) OVER(), 2)
          ) ORDER BY rank_num
        )
        FROM (
          SELECT 
            ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) AS rank_num,
            CASE 
              WHEN :dimension = 'action' THEN action
              WHEN :dimension = 'actor_id' THEN actor_id
              WHEN :dimension = 'object_type' THEN object:type::STRING
              ELSE 'unknown'
            END AS item_name,
            COUNT(*) AS item_count
          FROM ACTIVITY.EVENTS
          WHERE occurred_at BETWEEN :start_ts AND :end_ts
          GROUP BY item_name
          QUALIFY rank_num <= :top_n
        )
      );
    END IF;
  ELSEIF (metric = 'unique_sessions') THEN
    result_data := (
      SELECT ARRAY_AGG(
        OBJECT_CONSTRUCT(
          'rank', rank_num,
          'item', item_name,
          'unique_sessions', session_count,
          'percentage', ROUND(100.0 * session_count / SUM(session_count) OVER(), 2)
        ) ORDER BY rank_num
      )
      FROM (
        SELECT 
          ROW_NUMBER() OVER (ORDER BY COUNT(DISTINCT session_id) DESC) AS rank_num,
          CASE 
            WHEN :dimension = 'action' THEN action
            WHEN :dimension = 'actor_id' THEN actor_id
            ELSE 'unknown'
          END AS item_name,
          COUNT(DISTINCT session_id) AS session_count
        FROM ACTIVITY.EVENTS
        WHERE occurred_at BETWEEN :start_ts AND :end_ts
          AND session_id IS NOT NULL
        GROUP BY item_name
        QUALIFY rank_num <= :top_n
      )
    );
  ELSE
    RETURN OBJECT_CONSTRUCT(
      'ok', FALSE,
      'error', 'Invalid metric. Use: event_count or unique_sessions'
    );
  END IF;
  
  -- Calculate summary statistics
  LET total_items NUMBER;
  LET total_events NUMBER;
  
  SELECT COUNT(DISTINCT 
    CASE 
      WHEN :dimension = 'action' THEN action
      WHEN :dimension = 'actor_id' THEN actor_id
      WHEN :dimension = 'object_type' THEN object:type::STRING
    END
  ) INTO total_items
  FROM ACTIVITY.EVENTS
  WHERE occurred_at BETWEEN :start_ts AND :end_ts;
  
  SELECT COUNT(*) INTO total_events
  FROM ACTIVITY.EVENTS
  WHERE occurred_at BETWEEN :start_ts AND :end_ts;
  
  RETURN OBJECT_CONSTRUCT(
    'ok', TRUE,
    'data', COALESCE(result_data, ARRAY_CONSTRUCT()),
    'summary', OBJECT_CONSTRUCT(
      'total_unique_items', total_items,
      'total_events', total_events,
      'items_shown', ARRAY_SIZE(result_data),
      'dimension', dimension,
      'metric', metric
    ),
    'parameters', OBJECT_CONSTRUCT(
      'start_ts', start_ts,
      'end_ts', end_ts,
      'top_n', top_n,
      'include_percentiles', include_percentiles
    )
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
-- CALL GET_RANKED_RESULTS(
--   DATEADD('day', -30, CURRENT_TIMESTAMP()),
--   CURRENT_TIMESTAMP(),
--   'action',
--   'event_count',
--   10,
--   TRUE
-- );