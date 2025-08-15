-- ============================================================================
-- 21_monitoring_dlq.sql
-- Monitoring views and dead letter queue management
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE CLAUDE_BI;
USE SCHEMA LOGGING;

-- ============================================================================
-- Real-time monitoring dashboard view
-- ============================================================================

CREATE OR REPLACE VIEW CLAUDE_BI.LOGGING.REALTIME_DASHBOARD AS
WITH last_hour AS (
  SELECT 
    DATE_TRUNC('minute', occurred_at) AS minute,
    event_category,
    action,
    COUNT(*) AS event_count,
    AVG(execution_ms) AS avg_execution_ms,
    MAX(execution_ms) AS max_execution_ms,
    COUNT(DISTINCT session_id) AS unique_sessions
  FROM CLAUDE_BI.ACTIVITY.EVENTS
  WHERE occurred_at >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
  GROUP BY minute, event_category, action
),
error_summary AS (
  SELECT 
    DATE_TRUNC('minute', occurred_at) AS minute,
    COUNT(*) AS error_count,
    COUNT(DISTINCT session_id) AS sessions_with_errors
  FROM CLAUDE_BI.ACTIVITY.EVENTS
  WHERE occurred_at >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
    AND (event_category = 'quality' OR _source_lane IN ('QUALITY_REJECT', 'DEAD_LETTER'))
  GROUP BY minute
)
SELECT 
  l.minute,
  l.event_category,
  l.action,
  l.event_count,
  l.avg_execution_ms,
  l.max_execution_ms,
  l.unique_sessions,
  COALESCE(e.error_count, 0) AS error_count,
  COALESCE(e.sessions_with_errors, 0) AS sessions_with_errors,
  ROUND(COALESCE(e.error_count, 0) * 100.0 / NULLIF(l.event_count, 0), 2) AS error_rate_percent
FROM last_hour l
LEFT JOIN error_summary e ON l.minute = e.minute
ORDER BY l.minute DESC, l.event_count DESC;

-- ============================================================================
-- Tool usage analytics
-- ============================================================================

CREATE OR REPLACE VIEW CLAUDE_BI.LOGGING.TOOL_USAGE_ANALYTICS AS
WITH tool_metrics AS (
  SELECT 
    DATE_TRUNC('hour', occurred_at) AS hour,
    tool_name,
    COUNT(*) AS invocation_count,
    COUNT(DISTINCT session_id) AS unique_sessions,
    AVG(execution_ms) AS avg_execution_ms,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY execution_ms) AS median_execution_ms,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY execution_ms) AS p95_execution_ms,
    MAX(execution_ms) AS max_execution_ms,
    COUNT(CASE WHEN attributes:error IS NOT NULL THEN 1 END) AS error_count
  FROM CLAUDE_BI.ACTIVITY.EVENTS
  WHERE action = 'ccode.tool.called'
    AND tool_name IS NOT NULL
    AND occurred_at >= DATEADD('day', -7, CURRENT_TIMESTAMP())
  GROUP BY hour, tool_name
)
SELECT 
  hour,
  tool_name,
  invocation_count,
  unique_sessions,
  avg_execution_ms,
  median_execution_ms,
  p95_execution_ms,
  max_execution_ms,
  error_count,
  ROUND(error_count * 100.0 / NULLIF(invocation_count, 0), 2) AS error_rate_percent,
  CASE 
    WHEN p95_execution_ms > 5000 THEN 'SLOW'
    WHEN p95_execution_ms > 2000 THEN 'MODERATE'
    ELSE 'FAST'
  END AS performance_category
FROM tool_metrics
ORDER BY hour DESC, invocation_count DESC;

-- ============================================================================
-- Session timeline view
-- ============================================================================

CREATE OR REPLACE VIEW CLAUDE_BI.LOGGING.SESSION_TIMELINE AS
SELECT 
  session_id,
  occurred_at,
  action,
  tool_name,
  execution_ms,
  event_category,
  CASE 
    WHEN attributes:error IS NOT NULL THEN 'ERROR'
    WHEN execution_ms > 5000 THEN 'SLOW'
    ELSE 'OK'
  END AS event_status,
  attributes:error::STRING AS error_message,
  attributes:file_path::STRING AS file_path,
  attributes:command::STRING AS command,
  compression_metadata
FROM CLAUDE_BI.ACTIVITY.EVENTS
WHERE session_id IS NOT NULL
  AND occurred_at >= DATEADD('day', -1, CURRENT_TIMESTAMP())
ORDER BY session_id, occurred_at;

-- ============================================================================
-- Dead Letter Queue management procedures
-- ============================================================================

-- Procedure to retry dead letter events
CREATE OR REPLACE PROCEDURE CLAUDE_BI.MCP.RETRY_DEAD_LETTER_EVENTS(
  max_events_to_retry INTEGER DEFAULT 100,
  max_age_hours INTEGER DEFAULT 24
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
  retry_count INTEGER DEFAULT 0;
  success_count INTEGER DEFAULT 0;
  failure_count INTEGER DEFAULT 0;
  event_cursor CURSOR FOR
    SELECT 
      event_id,
      attributes AS original_event
    FROM CLAUDE_BI.ACTIVITY.EVENTS
    WHERE _source_lane = 'DEAD_LETTER'
      AND DATEDIFF('hour', _recv_at, CURRENT_TIMESTAMP()) <= max_age_hours
    ORDER BY _recv_at
    LIMIT max_events_to_retry;
  event_record RECORD;
BEGIN
  -- Process each dead letter event
  FOR event_record IN event_cursor DO
    retry_count := retry_count + 1;
    
    BEGIN
      -- Attempt to reinsert the event
      INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS
      VALUES (
        OBJECT_INSERT(
          event_record.original_event,
          'retry_metadata',
          OBJECT_CONSTRUCT(
            'retry_at', CURRENT_TIMESTAMP(),
            'original_dlq_id', event_record.event_id
          )
        ),
        'RETRY',
        CURRENT_TIMESTAMP()
      );
      
      success_count := success_count + 1;
      
      -- Mark original DLQ event as processed
      INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS
      VALUES (
        OBJECT_CONSTRUCT(
          'event_id', SHA2(CONCAT('dlq_processed_', event_record.event_id), 256),
          'action', 'system.dlq.processed',
          'occurred_at', CURRENT_TIMESTAMP(),
          'attributes', OBJECT_CONSTRUCT(
            'original_event_id', event_record.event_id,
            'retry_successful', TRUE
          )
        ),
        'SYSTEM',
        CURRENT_TIMESTAMP()
      );
      
    EXCEPTION
      WHEN OTHER THEN
        failure_count := failure_count + 1;
        
        -- Log retry failure
        INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS
        VALUES (
          OBJECT_CONSTRUCT(
            'event_id', SHA2(CONCAT('dlq_retry_failed_', event_record.event_id), 256),
            'action', 'system.dlq.retry_failed',
            'occurred_at', CURRENT_TIMESTAMP(),
            'attributes', OBJECT_CONSTRUCT(
              'original_event_id', event_record.event_id,
              'error', SQLERRM
            )
          ),
          'SYSTEM',
          CURRENT_TIMESTAMP()
        );
    END;
  END FOR;
  
  RETURN OBJECT_CONSTRUCT(
    'processed', retry_count,
    'succeeded', success_count,
    'failed', failure_count,
    'timestamp', CURRENT_TIMESTAMP()
  );
END;
$$;

-- Procedure to purge old dead letter events
CREATE OR REPLACE PROCEDURE CLAUDE_BI.MCP.PURGE_OLD_DLQ_EVENTS(
  retention_days INTEGER DEFAULT 7
)
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
  -- Log purge action
  INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS
  VALUES (
    OBJECT_CONSTRUCT(
      'event_id', SHA2(CONCAT('dlq_purge_', CURRENT_TIMESTAMP()::STRING), 256),
      'action', 'system.dlq.purged',
      'occurred_at', CURRENT_TIMESTAMP(),
      'attributes', OBJECT_CONSTRUCT(
        'retention_days', retention_days,
        'purged_before', DATEADD('day', -retention_days, CURRENT_TIMESTAMP())
      )
    ),
    'SYSTEM',
    CURRENT_TIMESTAMP()
  );
  
  RETURN 'DLQ purge logged. Old events will be excluded from next Dynamic Table refresh.';
END;
$$;

-- ============================================================================
-- Alert monitoring views
-- ============================================================================

CREATE OR REPLACE VIEW CLAUDE_BI.LOGGING.ALERT_CONDITIONS AS
WITH circuit_breaker_status AS (
  SELECT 
    session_id,
    action,
    COUNT(*) AS events_per_minute
  FROM CLAUDE_BI.ACTIVITY.EVENTS
  WHERE occurred_at >= DATEADD('minute', -1, CURRENT_TIMESTAMP())
  GROUP BY session_id, action
  HAVING events_per_minute > 800  -- Warning threshold
),
dlq_status AS (
  SELECT 
    COUNT(*) AS dlq_count,
    MIN(_recv_at) AS oldest_dlq_event
  FROM CLAUDE_BI.ACTIVITY.EVENTS
  WHERE _source_lane = 'DEAD_LETTER'
),
error_rate AS (
  SELECT 
    COUNT(CASE WHEN event_category = 'quality' THEN 1 END) * 100.0 / 
      NULLIF(COUNT(*), 0) AS overall_error_rate
  FROM CLAUDE_BI.ACTIVITY.EVENTS
  WHERE occurred_at >= DATEADD('minute', -5, CURRENT_TIMESTAMP())
)
SELECT 
  'CIRCUIT_BREAKER' AS alert_type,
  'Session approaching rate limit' AS alert_message,
  OBJECT_CONSTRUCT(
    'session_id', session_id,
    'action', action,
    'events_per_minute', events_per_minute
  ) AS alert_details
FROM circuit_breaker_status

UNION ALL

SELECT 
  'DEAD_LETTER_QUEUE' AS alert_type,
  'Dead letter queue has ' || dlq_count || ' events' AS alert_message,
  OBJECT_CONSTRUCT(
    'count', dlq_count,
    'oldest_event_age_hours', DATEDIFF('hour', oldest_dlq_event, CURRENT_TIMESTAMP())
  ) AS alert_details
FROM dlq_status
WHERE dlq_count > 100  -- Alert if more than 100 events in DLQ

UNION ALL

SELECT 
  'HIGH_ERROR_RATE' AS alert_type,
  'Error rate is ' || ROUND(overall_error_rate, 2) || '%' AS alert_message,
  OBJECT_CONSTRUCT(
    'error_rate_percent', ROUND(overall_error_rate, 2)
  ) AS alert_details
FROM error_rate
WHERE overall_error_rate > 5;  -- Alert if error rate > 5%

-- ============================================================================
-- Performance benchmarking view
-- ============================================================================

CREATE OR REPLACE VIEW CLAUDE_BI.LOGGING.PERFORMANCE_BENCHMARKS AS
WITH tool_benchmarks AS (
  SELECT 
    tool_name,
    DATE_TRUNC('day', occurred_at) AS day,
    COUNT(*) AS daily_calls,
    AVG(execution_ms) AS avg_ms,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY execution_ms) AS p50_ms,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY execution_ms) AS p75_ms,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY execution_ms) AS p95_ms,
    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY execution_ms) AS p99_ms,
    MAX(execution_ms) AS max_ms
  FROM CLAUDE_BI.ACTIVITY.EVENTS
  WHERE action = 'ccode.tool.called'
    AND tool_name IS NOT NULL
    AND execution_ms IS NOT NULL
    AND occurred_at >= DATEADD('day', -30, CURRENT_TIMESTAMP())
  GROUP BY tool_name, day
)
SELECT 
  tool_name,
  day,
  daily_calls,
  ROUND(avg_ms, 2) AS avg_ms,
  p50_ms,
  p75_ms,
  p95_ms,
  p99_ms,
  max_ms,
  -- Performance grade based on P95
  CASE 
    WHEN p95_ms <= 100 THEN 'A+'
    WHEN p95_ms <= 500 THEN 'A'
    WHEN p95_ms <= 1000 THEN 'B'
    WHEN p95_ms <= 2000 THEN 'C'
    WHEN p95_ms <= 5000 THEN 'D'
    ELSE 'F'
  END AS performance_grade
FROM tool_benchmarks
ORDER BY day DESC, tool_name;

-- ============================================================================
-- Event volume forecasting
-- ============================================================================

CREATE OR REPLACE VIEW CLAUDE_BI.LOGGING.EVENT_VOLUME_FORECAST AS
WITH daily_volumes AS (
  SELECT 
    DATE_TRUNC('day', occurred_at) AS day,
    COUNT(*) AS event_count,
    COUNT(DISTINCT session_id) AS session_count,
    SUM(BYTE_LENGTH(TO_JSON(attributes))) / POW(1024, 3) AS data_volume_gb
  FROM CLAUDE_BI.ACTIVITY.EVENTS
  WHERE occurred_at >= DATEADD('day', -30, CURRENT_TIMESTAMP())
  GROUP BY day
),
trends AS (
  SELECT 
    day,
    event_count,
    session_count,
    data_volume_gb,
    AVG(event_count) OVER (ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS avg_7day_events,
    AVG(session_count) OVER (ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS avg_7day_sessions,
    REGR_SLOPE(event_count, DATE_PART('epoch', day)) OVER (ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS event_trend
  FROM daily_volumes
)
SELECT 
  day,
  event_count,
  session_count,
  ROUND(data_volume_gb, 3) AS data_volume_gb,
  ROUND(avg_7day_events, 0) AS avg_7day_events,
  ROUND(avg_7day_sessions, 0) AS avg_7day_sessions,
  CASE 
    WHEN event_trend > 0 THEN 'INCREASING'
    WHEN event_trend < 0 THEN 'DECREASING'
    ELSE 'STABLE'
  END AS volume_trend,
  -- Simple linear forecast for next day
  ROUND(avg_7day_events + (event_trend * 86400), 0) AS next_day_forecast,
  -- Monthly projection based on 7-day average
  ROUND(avg_7day_events * 30, 0) AS monthly_projection
FROM trends
ORDER BY day DESC;

-- ============================================================================
-- Grant permissions
-- ============================================================================

-- Admin role gets full access
GRANT SELECT ON ALL VIEWS IN SCHEMA CLAUDE_BI.LOGGING TO ROLE MCP_ADMIN_ROLE;
GRANT EXECUTE ON PROCEDURE CLAUDE_BI.MCP.RETRY_DEAD_LETTER_EVENTS(INTEGER, INTEGER) TO ROLE MCP_ADMIN_ROLE;
GRANT EXECUTE ON PROCEDURE CLAUDE_BI.MCP.PURGE_OLD_DLQ_EVENTS(INTEGER) TO ROLE MCP_ADMIN_ROLE;

-- Logger role gets limited monitoring access
GRANT SELECT ON VIEW CLAUDE_BI.LOGGING.REALTIME_DASHBOARD TO ROLE MCP_LOGGER_ROLE;
GRANT SELECT ON VIEW CLAUDE_BI.LOGGING.ALERT_CONDITIONS TO ROLE MCP_LOGGER_ROLE;

-- ============================================================================
-- Create scheduled task for DLQ management
-- ============================================================================

-- Create task to retry DLQ events every hour
CREATE OR REPLACE TASK CLAUDE_BI.MCP.RETRY_DLQ_TASK
  WAREHOUSE = ALERT_WH
  SCHEDULE = '60 MINUTE'
  COMMENT = 'Hourly retry of dead letter queue events'
AS
  CALL CLAUDE_BI.MCP.RETRY_DEAD_LETTER_EVENTS(50, 12);  -- Retry up to 50 events less than 12 hours old

-- Create task to purge old DLQ events weekly
CREATE OR REPLACE TASK CLAUDE_BI.MCP.PURGE_DLQ_TASK
  WAREHOUSE = ALERT_WH
  SCHEDULE = 'USING CRON 0 2 * * 0 UTC'  -- Sunday at 2 AM UTC
  COMMENT = 'Weekly purge of old dead letter queue events'
AS
  CALL CLAUDE_BI.MCP.PURGE_OLD_DLQ_EVENTS(7);  -- Keep 7 days of DLQ events

-- Resume tasks
ALTER TASK CLAUDE_BI.MCP.RETRY_DLQ_TASK RESUME;
ALTER TASK CLAUDE_BI.MCP.PURGE_DLQ_TASK RESUME;

-- ============================================================================
-- Summary
-- ============================================================================

SELECT 
  'Monitoring and DLQ management created' AS status,
  (SELECT COUNT(*) FROM CLAUDE_BI.INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA = 'LOGGING') AS monitoring_views,
  (SELECT COUNT(*) FROM CLAUDE_BI.INFORMATION_SCHEMA.PROCEDURES WHERE PROCEDURE_SCHEMA = 'MCP' AND PROCEDURE_NAME LIKE '%DLQ%') AS dlq_procedures,
  (SELECT COUNT(*) FROM CLAUDE_BI.INFORMATION_SCHEMA.TASKS WHERE TASK_SCHEMA = 'MCP' AND TASK_NAME LIKE '%DLQ%') AS scheduled_tasks;