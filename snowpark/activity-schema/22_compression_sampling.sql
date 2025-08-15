-- ============================================================================
-- 22_compression_sampling.sql
-- Event compression and sampling procedures for high-volume logging
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE CLAUDE_BI;
USE SCHEMA MCP;

-- ============================================================================
-- Create compression control procedures
-- ============================================================================

CREATE OR REPLACE PROCEDURE MCP.COMPRESS_HIGH_VOLUME_EVENTS(
  session_id STRING,
  lookback_minutes INTEGER DEFAULT 5
)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
AS
$$
  // Query for high-volume event patterns
  const sql = `
    WITH event_patterns AS (
      SELECT 
        action,
        attributes:path::STRING AS path,
        COUNT(*) AS event_count,
        MIN(occurred_at) AS window_start,
        MAX(occurred_at) AS window_end,
        ANY_VALUE(attributes) AS sample_attributes
      FROM CLAUDE_BI.ACTIVITY.EVENTS
      WHERE session_id = ?
        AND occurred_at >= DATEADD('minute', -?, CURRENT_TIMESTAMP())
        AND action IN ('ccode.file.read', 'ccode.file.edited', 'ccode.bash.executed')
      GROUP BY action, path
      HAVING event_count > 10
    )
    SELECT * FROM event_patterns
  `;
  
  const statement = snowflake.createStatement({
    sqlText: sql,
    binds: [SESSION_ID, LOOKBACK_MINUTES]
  });
  
  const results = statement.execute();
  let compressed_count = 0;
  let original_count = 0;
  
  while (results.next()) {
    const action = results.getColumnValue('ACTION');
    const path = results.getColumnValue('PATH');
    const count = results.getColumnValue('EVENT_COUNT');
    const window_start = results.getColumnValue('WINDOW_START');
    const window_end = results.getColumnValue('WINDOW_END');
    const sample = results.getColumnValue('SAMPLE_ATTRIBUTES');
    
    original_count += count;
    
    // Log compressed event
    const compressProc = snowflake.createStatement({
      sqlText: `CALL MCP.LOG_COMPRESSED_EVENT(?, ?, ?, ?, ?, ?, ?)`,
      binds: [SESSION_ID, action, path, count, window_start, window_end, sample]
    });
    
    compressProc.execute();
    compressed_count++;
  }
  
  return {
    success: true,
    original_events: original_count,
    compressed_to: compressed_count,
    compression_ratio: original_count > 0 ? 
      Math.round((1 - compressed_count / original_count) * 100) : 0
  };
$$;

-- ============================================================================
-- Create sampling configuration table (as events)
-- ============================================================================

-- Procedure to set sampling rate for specific actions
CREATE OR REPLACE PROCEDURE MCP.SET_SAMPLING_RATE(
  action_pattern STRING,
  sample_rate FLOAT,
  reason STRING DEFAULT NULL
)
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
  -- Validate sample rate
  IF (sample_rate < 0 OR sample_rate > 1) THEN
    RETURN 'Error: Sample rate must be between 0 and 1';
  END IF;
  
  -- Log configuration change as event
  INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS VALUES (
    OBJECT_CONSTRUCT(
      'event_id', SHA2(CONCAT_WS('|', 'sampling_config', action_pattern, CURRENT_TIMESTAMP()::STRING), 256),
      'action', 'system.config.sampling',
      'occurred_at', CURRENT_TIMESTAMP(),
      'actor_id', CURRENT_USER(),
      'source', 'system',
      'attributes', OBJECT_CONSTRUCT(
        'action_pattern', action_pattern,
        'sample_rate', sample_rate,
        'reason', reason,
        'previous_rate', (
          SELECT attributes:sample_rate
          FROM CLAUDE_BI.ACTIVITY.EVENTS
          WHERE action = 'system.config.sampling'
            AND attributes:action_pattern = action_pattern
          ORDER BY occurred_at DESC
          LIMIT 1
        )
      )
    ),
    'SYSTEM',
    CURRENT_TIMESTAMP()
  );
  
  RETURN 'Sampling rate set to ' || (sample_rate * 100) || '% for ' || action_pattern;
END;
$$;

-- ============================================================================
-- Create intelligent sampling function
-- ============================================================================

CREATE OR REPLACE FUNCTION MCP.SHOULD_SAMPLE_EVENT(
  action STRING,
  session_id STRING,
  attributes VARIANT
)
RETURNS BOOLEAN
LANGUAGE SQL
AS
$$
  WITH sampling_config AS (
    -- Get current sampling configuration
    SELECT 
      attributes:action_pattern::STRING AS pattern,
      attributes:sample_rate::FLOAT AS rate
    FROM CLAUDE_BI.ACTIVITY.EVENTS
    WHERE action = 'system.config.sampling'
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY attributes:action_pattern 
      ORDER BY occurred_at DESC
    ) = 1
  ),
  event_frequency AS (
    -- Check recent frequency for this session
    SELECT COUNT(*) AS recent_count
    FROM CLAUDE_BI.ACTIVITY.EVENTS
    WHERE session_id = session_id
      AND action = action
      AND occurred_at >= DATEADD('minute', -1, CURRENT_TIMESTAMP())
  )
  SELECT 
    CASE
      -- Always sample errors and important events
      WHEN action LIKE 'quality.%' OR action LIKE 'system.%' THEN TRUE
      
      -- Apply configured sampling rate if exists
      WHEN EXISTS (
        SELECT 1 FROM sampling_config 
        WHERE action LIKE pattern
      ) THEN 
        UNIFORM(0::FLOAT, 1::FLOAT, RANDOM()) <= (
          SELECT MAX(rate) FROM sampling_config 
          WHERE action LIKE pattern
        )
      
      -- Adaptive sampling based on frequency
      WHEN (SELECT recent_count FROM event_frequency) > 100 THEN 
        UNIFORM(0::FLOAT, 1::FLOAT, RANDOM()) <= 0.1  -- Sample 10% if > 100/min
      WHEN (SELECT recent_count FROM event_frequency) > 50 THEN 
        UNIFORM(0::FLOAT, 1::FLOAT, RANDOM()) <= 0.5  -- Sample 50% if > 50/min
      
      -- Default: sample everything
      ELSE TRUE
    END
$$;

-- ============================================================================
-- Create event aggregation procedures
-- ============================================================================

CREATE OR REPLACE PROCEDURE MCP.AGGREGATE_METRICS(
  session_id STRING,
  window_minutes INTEGER DEFAULT 5
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
  result VARIANT;
BEGIN
  -- Create aggregated metrics event
  INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS
  SELECT 
    OBJECT_CONSTRUCT(
      'event_id', SHA2(CONCAT_WS('|', 'metrics', session_id, DATE_TRUNC('minute', CURRENT_TIMESTAMP())::STRING), 256),
      'action', 'ccode.metrics.aggregated',
      'session_id', session_id,
      'occurred_at', CURRENT_TIMESTAMP(),
      'source', 'aggregation',
      'attributes', OBJECT_CONSTRUCT(
        'window_minutes', window_minutes,
        'window_end', CURRENT_TIMESTAMP(),
        'total_events', COUNT(*),
        'unique_actions', COUNT(DISTINCT action),
        'tool_calls', COUNT(CASE WHEN action = 'ccode.tool.called' THEN 1 END),
        'file_operations', COUNT(CASE WHEN action LIKE 'ccode.file.%' THEN 1 END),
        'bash_commands', COUNT(CASE WHEN action = 'ccode.bash.executed' THEN 1 END),
        'errors', COUNT(CASE WHEN attributes:error IS NOT NULL THEN 1 END),
        'avg_execution_ms', AVG(execution_ms),
        'p95_execution_ms', PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY execution_ms),
        'top_tools', ARRAY_AGG(DISTINCT tool_name) WITHIN GROUP (ORDER BY tool_name),
        'top_files', ARRAY_SLICE(
          ARRAY_AGG(DISTINCT attributes:file_path::STRING) WITHIN GROUP (ORDER BY attributes:file_path::STRING),
          0, 10
        )
      )
    ),
    'CLAUDE_CODE',
    CURRENT_TIMESTAMP()
  FROM CLAUDE_BI.ACTIVITY.EVENTS
  WHERE session_id = :session_id
    AND occurred_at >= DATEADD('minute', -:window_minutes, CURRENT_TIMESTAMP());
  
  SELECT OBJECT_CONSTRUCT(
    'success', TRUE,
    'session_id', :session_id,
    'metrics_logged', TRUE
  ) INTO result;
  
  RETURN result;
END;
$$;

-- ============================================================================
-- Create deduplication view for compressed events
-- ============================================================================

CREATE OR REPLACE VIEW CLAUDE_BI.LOGGING.DECOMPRESSED_EVENTS AS
WITH compressed_events AS (
  SELECT 
    event_id,
    action,
    occurred_at,
    session_id,
    attributes,
    compression_metadata,
    compression_metadata:original_count::NUMBER AS original_count,
    attributes:compression_metadata:window_start::TIMESTAMP_TZ AS window_start,
    attributes:compression_metadata:window_end::TIMESTAMP_TZ AS window_end
  FROM CLAUDE_BI.ACTIVITY.EVENTS
  WHERE compression_metadata IS NOT NULL
),
expanded AS (
  SELECT 
    event_id,
    REPLACE(action, '.compressed', '') AS original_action,
    DATEADD(
      'second',
      FLOOR(seq.value * DATEDIFF('second', window_start, window_end) / original_count),
      window_start
    ) AS interpolated_time,
    session_id,
    attributes,
    'interpolated' AS event_source
  FROM compressed_events,
    LATERAL FLATTEN(ARRAY_GENERATE_RANGE(0, original_count)) seq
)
SELECT * FROM expanded

UNION ALL

-- Include non-compressed events
SELECT 
  event_id,
  action AS original_action,
  occurred_at AS interpolated_time,
  session_id,
  attributes,
  'original' AS event_source
FROM CLAUDE_BI.ACTIVITY.EVENTS
WHERE compression_metadata IS NULL;

-- ============================================================================
-- Create sampling statistics view
-- ============================================================================

CREATE OR REPLACE VIEW CLAUDE_BI.LOGGING.SAMPLING_STATS AS
WITH sampling_configs AS (
  SELECT 
    attributes:action_pattern::STRING AS action_pattern,
    attributes:sample_rate::FLOAT AS sample_rate,
    occurred_at AS configured_at,
    actor_id AS configured_by
  FROM CLAUDE_BI.ACTIVITY.EVENTS
  WHERE action = 'system.config.sampling'
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY attributes:action_pattern 
    ORDER BY occurred_at DESC
  ) = 1
),
actual_sampling AS (
  SELECT 
    action,
    DATE_TRUNC('hour', occurred_at) AS hour,
    COUNT(*) AS total_events,
    COUNT(CASE WHEN attributes:sampled = TRUE THEN 1 END) AS sampled_events,
    COUNT(CASE WHEN attributes:sampled = FALSE THEN 1 END) AS dropped_events
  FROM CLAUDE_BI.ACTIVITY.EVENTS
  WHERE occurred_at >= DATEADD('day', -7, CURRENT_TIMESTAMP())
  GROUP BY action, hour
)
SELECT 
  a.action,
  a.hour,
  a.total_events,
  a.sampled_events,
  a.dropped_events,
  ROUND(a.sampled_events * 100.0 / NULLIF(a.total_events, 0), 2) AS actual_sample_rate,
  c.sample_rate * 100 AS configured_rate,
  c.configured_at,
  c.configured_by
FROM actual_sampling a
LEFT JOIN sampling_configs c ON a.action LIKE c.action_pattern
ORDER BY a.hour DESC, a.total_events DESC;

-- ============================================================================
-- Create adaptive sampling procedure
-- ============================================================================

CREATE OR REPLACE PROCEDURE MCP.APPLY_ADAPTIVE_SAMPLING()
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
  adjustments INTEGER DEFAULT 0;
BEGIN
  -- Find high-volume actions and adjust sampling
  FOR action_record IN (
    SELECT 
      action,
      COUNT(*) AS event_count,
      AVG(execution_ms) AS avg_execution_ms
    FROM CLAUDE_BI.ACTIVITY.EVENTS
    WHERE occurred_at >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
      AND action LIKE 'ccode.%'
    GROUP BY action
    HAVING event_count > 1000  -- High volume threshold
  ) DO
    -- Set aggressive sampling for high-volume, low-value events
    IF (action_record.action IN ('ccode.file.read', 'ccode.cursor.moved') 
        AND action_record.avg_execution_ms < 10) THEN
      CALL MCP.SET_SAMPLING_RATE(
        action_record.action,
        0.05,  -- Sample only 5%
        'Adaptive sampling for high-volume event'
      );
      adjustments := adjustments + 1;
    ELSEIF (action_record.event_count > 5000) THEN
      CALL MCP.SET_SAMPLING_RATE(
        action_record.action,
        0.1,  -- Sample 10%
        'Adaptive sampling for very high-volume event'
      );
      adjustments := adjustments + 1;
    END IF;
  END FOR;
  
  RETURN OBJECT_CONSTRUCT(
    'success', TRUE,
    'adjustments_made', adjustments,
    'timestamp', CURRENT_TIMESTAMP()
  );
END;
$$;

-- ============================================================================
-- Grant permissions
-- ============================================================================

GRANT EXECUTE ON PROCEDURE MCP.COMPRESS_HIGH_VOLUME_EVENTS(STRING, INTEGER) TO ROLE MCP_LOGGER_ROLE;
GRANT EXECUTE ON PROCEDURE MCP.SET_SAMPLING_RATE(STRING, FLOAT, STRING) TO ROLE MCP_ADMIN_ROLE;
GRANT EXECUTE ON FUNCTION MCP.SHOULD_SAMPLE_EVENT(STRING, STRING, VARIANT) TO ROLE MCP_LOGGER_ROLE;
GRANT EXECUTE ON PROCEDURE MCP.AGGREGATE_METRICS(STRING, INTEGER) TO ROLE MCP_LOGGER_ROLE;
GRANT EXECUTE ON PROCEDURE MCP.APPLY_ADAPTIVE_SAMPLING() TO ROLE MCP_ADMIN_ROLE;

GRANT SELECT ON VIEW CLAUDE_BI.LOGGING.DECOMPRESSED_EVENTS TO ROLE MCP_ADMIN_ROLE;
GRANT SELECT ON VIEW CLAUDE_BI.LOGGING.SAMPLING_STATS TO ROLE MCP_ADMIN_ROLE;

-- ============================================================================
-- Create scheduled task for adaptive sampling
-- ============================================================================

CREATE OR REPLACE TASK CLAUDE_BI.MCP.ADAPTIVE_SAMPLING_TASK
  WAREHOUSE = ALERT_WH
  SCHEDULE = '30 MINUTE'
  COMMENT = 'Adjust sampling rates based on volume patterns'
AS
  CALL MCP.APPLY_ADAPTIVE_SAMPLING();

ALTER TASK CLAUDE_BI.MCP.ADAPTIVE_SAMPLING_TASK RESUME;

-- ============================================================================
-- Test compression and sampling
-- ============================================================================

-- Set sampling rate for file reads
CALL MCP.SET_SAMPLING_RATE('ccode.file.%', 0.2, 'Initial configuration');

-- Test compression for a mock session
CALL MCP.COMPRESS_HIGH_VOLUME_EVENTS('test_session_compression', 10);

-- Check sampling configuration
SELECT * FROM CLAUDE_BI.LOGGING.SAMPLING_STATS LIMIT 10;