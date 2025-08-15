-- ============================================================================
-- 20_logging_dynamic_table.sql
-- Enhanced Dynamic Table for Claude Code logging with compression windows
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE CLAUDE_BI;
USE SCHEMA ACTIVITY;

-- ============================================================================
-- Drop existing EVENTS Dynamic Table to recreate with logging optimizations
-- NOTE: This maintains the Two-Table Law - we're just enhancing the existing DT
-- ============================================================================

-- First, preserve any critical views that depend on EVENTS
CREATE OR REPLACE VIEW TEMP_EVENTS_BACKUP AS
SELECT * FROM CLAUDE_BI.ACTIVITY.EVENTS;

-- Drop the existing Dynamic Table
ALTER DYNAMIC TABLE CLAUDE_BI.ACTIVITY.EVENTS SUSPEND;
DROP DYNAMIC TABLE IF EXISTS CLAUDE_BI.ACTIVITY.EVENTS;

-- ============================================================================
-- Create Enhanced Dynamic Table with Claude Code optimizations
-- ============================================================================

CREATE OR REPLACE DYNAMIC TABLE CLAUDE_BI.ACTIVITY.EVENTS
  TARGET_LAG = '1 minute'
  WAREHOUSE = DT_XS_WH
  CLUSTER BY (action, DATE_TRUNC('hour', occurred_at))
  COMMENT = 'Enhanced Dynamic Table with Claude Code logging optimizations'
AS
WITH raw_with_ids AS (
  -- Add stable IDs and classification
  SELECT 
    payload,
    _source_lane,
    _recv_at,
    -- Generate stable event_id if not provided
    COALESCE(
      payload:event_id::STRING,
      payload:idempotency_key::STRING,
      SHA2(CONCAT_WS('|',
        payload:action::STRING,
        payload:session_id::STRING,
        payload:occurred_at::STRING,
        TO_JSON(payload:attributes)
      ), 256)
    ) AS event_id,
    -- Classify event type for routing
    CASE 
      WHEN _source_lane = 'DEAD_LETTER' THEN 'dead_letter'
      WHEN _source_lane = 'QUALITY_REJECT' THEN 'quality'
      WHEN payload:action::STRING LIKE 'quality.%' THEN 'quality'
      WHEN payload:action::STRING LIKE 'ccode.%' THEN 'claude_code'
      WHEN payload:action::STRING LIKE 'system.%' THEN 'system'
      WHEN payload:action::STRING LIKE 'mcp.%' THEN 'mcp'
      WHEN payload:action::STRING LIKE 'security.%' THEN 'security'
      ELSE 'other'
    END AS event_category,
    -- Add compression window for high-volume events
    CASE 
      WHEN payload:action::STRING IN (
        'ccode.file.read',
        'ccode.file.edited',
        'ccode.bash.executed',
        'ccode.tool.called'
      ) THEN DATE_TRUNC('minute', _recv_at)
      ELSE NULL
    END AS compression_window
  FROM CLAUDE_BI.LANDING.RAW_EVENTS
  WHERE _recv_at >= DATEADD('day', -30, CURRENT_TIMESTAMP())  -- 30-day retention
),
-- Incremental-safe deduplication using GROUP BY
first_seen AS (
  SELECT 
    event_id,
    MIN(_recv_at) AS first_recv_at,
    ANY_VALUE(event_category) AS event_category,
    ANY_VALUE(compression_window) AS compression_window
  FROM raw_with_ids
  GROUP BY event_id
),
deduplicated AS (
  SELECT 
    r.payload,
    r._source_lane,
    r._recv_at,
    r.event_id,
    r.event_category,
    r.compression_window
  FROM raw_with_ids r
  INNER JOIN first_seen f 
    ON r.event_id = f.event_id 
    AND r._recv_at = f.first_recv_at
),
-- Apply compression for high-volume events
compressed AS (
  SELECT 
    event_id,
    payload,
    _source_lane,
    _recv_at,
    event_category,
    compression_window,
    -- Count occurrences within compression window
    COUNT(*) OVER (
      PARTITION BY 
        payload:action::STRING,
        payload:session_id::STRING,
        compression_window
    ) AS window_event_count,
    -- Get first event in window for sampling
    ROW_NUMBER() OVER (
      PARTITION BY 
        payload:action::STRING,
        payload:session_id::STRING,
        compression_window
      ORDER BY _recv_at
    ) AS window_event_rank
  FROM deduplicated
  WHERE event_category = 'claude_code'
    AND compression_window IS NOT NULL
),
-- Final selection with compression logic
final_events AS (
  -- Non-compressed events (all non-Claude Code and non-compressible)
  SELECT 
    event_id,
    payload:action::STRING AS action,
    COALESCE(payload:occurred_at::TIMESTAMP_TZ, _recv_at) AS occurred_at,
    payload:actor_id::STRING AS actor_id,
    payload:object_type::STRING AS object_type,
    payload:object_id::STRING AS object_id,
    payload:attributes::VARIANT AS attributes,
    payload:source::STRING AS source,
    event_category,
    _source_lane,
    _recv_at,
    NULL AS compression_metadata
  FROM deduplicated
  WHERE event_category != 'claude_code' 
    OR compression_window IS NULL
  
  UNION ALL
  
  -- Compressed Claude Code events (keep first of each window + metadata)
  SELECT 
    event_id,
    CASE 
      WHEN window_event_count > 10 AND window_event_rank = 1 
      THEN payload:action::STRING || '.compressed'
      ELSE payload:action::STRING
    END AS action,
    COALESCE(payload:occurred_at::TIMESTAMP_TZ, _recv_at) AS occurred_at,
    payload:actor_id::STRING AS actor_id,
    payload:object_type::STRING AS object_type,
    payload:object_id::STRING AS object_id,
    CASE 
      WHEN window_event_count > 10 AND window_event_rank = 1
      THEN OBJECT_INSERT(
        payload:attributes::VARIANT,
        'compression_metadata',
        OBJECT_CONSTRUCT(
          'original_count', window_event_count,
          'window_start', compression_window,
          'window_end', DATEADD('minute', 1, compression_window)
        )
      )
      ELSE payload:attributes::VARIANT
    END AS attributes,
    payload:source::STRING AS source,
    event_category,
    _source_lane,
    _recv_at,
    CASE 
      WHEN window_event_count > 10 AND window_event_rank = 1
      THEN OBJECT_CONSTRUCT(
        'compressed', TRUE,
        'original_count', window_event_count
      )
      ELSE NULL
    END AS compression_metadata
  FROM compressed
  WHERE window_event_rank = 1  -- Only keep first event per compression window
    OR window_event_count <= 10  -- Keep all if not enough to compress
)
SELECT 
  event_id,
  action,
  occurred_at,
  actor_id,
  object_type,
  object_id,
  attributes,
  source,
  event_category,
  _source_lane,
  _recv_at,
  compression_metadata,
  -- Add derived fields for easier querying
  attributes:session_id::STRING AS session_id,
  attributes:tool_name::STRING AS tool_name,
  attributes:execution_ms::NUMBER AS execution_ms,
  DATE_TRUNC('hour', occurred_at) AS occurred_hour,
  DATE_TRUNC('day', occurred_at) AS occurred_date
FROM final_events;

-- ============================================================================
-- Create specialized views for different event categories
-- ============================================================================

-- Claude Code events view
CREATE OR REPLACE VIEW CLAUDE_BI.LOGGING.CLAUDE_CODE_EVENTS AS
SELECT * FROM CLAUDE_BI.ACTIVITY.EVENTS
WHERE event_category = 'claude_code'
  AND occurred_at >= DATEADD('day', -7, CURRENT_TIMESTAMP());

-- Quality events view (validation failures, circuit breaks, etc.)
CREATE OR REPLACE VIEW CLAUDE_BI.LOGGING.QUALITY_EVENTS AS
SELECT * FROM CLAUDE_BI.ACTIVITY.EVENTS
WHERE event_category = 'quality'
  OR _source_lane IN ('QUALITY_REJECT', 'DEAD_LETTER')
ORDER BY occurred_at DESC;

-- System events view (config changes, maintenance)
CREATE OR REPLACE VIEW CLAUDE_BI.LOGGING.SYSTEM_EVENTS AS
SELECT * FROM CLAUDE_BI.ACTIVITY.EVENTS
WHERE event_category = 'system'
ORDER BY occurred_at DESC;

-- ============================================================================
-- Create dead letter queue view
-- ============================================================================

CREATE OR REPLACE VIEW CLAUDE_BI.LOGGING.DEAD_LETTER_QUEUE AS
SELECT 
  event_id,
  action,
  occurred_at,
  _recv_at,
  attributes:error::STRING AS error_message,
  attributes:event_index::NUMBER AS original_index,
  attributes:original_action::STRING AS original_action,
  attributes:session_id::STRING AS session_id,
  DATEDIFF('hour', _recv_at, CURRENT_TIMESTAMP()) AS hours_in_dlq
FROM CLAUDE_BI.ACTIVITY.EVENTS
WHERE _source_lane = 'DEAD_LETTER'
  OR event_category = 'dead_letter'
ORDER BY _recv_at DESC;

-- ============================================================================
-- Create compression statistics view
-- ============================================================================

CREATE OR REPLACE VIEW CLAUDE_BI.LOGGING.COMPRESSION_STATS AS
WITH compression_summary AS (
  SELECT 
    DATE_TRUNC('hour', occurred_at) AS hour,
    action,
    session_id,
    COUNT(*) AS event_count,
    COUNT(CASE WHEN compression_metadata IS NOT NULL THEN 1 END) AS compressed_count,
    SUM(compression_metadata:original_count::NUMBER) AS original_event_count
  FROM CLAUDE_BI.ACTIVITY.EVENTS
  WHERE event_category = 'claude_code'
    AND occurred_at >= DATEADD('day', -1, CURRENT_TIMESTAMP())
  GROUP BY hour, action, session_id
)
SELECT 
  hour,
  action,
  session_id,
  event_count AS stored_events,
  COALESCE(original_event_count, event_count) AS original_events,
  compressed_count AS compression_groups,
  CASE 
    WHEN original_event_count > event_count 
    THEN ROUND((1 - (event_count::FLOAT / original_event_count)) * 100, 2)
    ELSE 0
  END AS compression_ratio_percent
FROM compression_summary
WHERE original_events > 100  -- Only show significant compressions
ORDER BY hour DESC, compression_ratio_percent DESC;

-- ============================================================================
-- Create session quality metrics
-- ============================================================================

CREATE OR REPLACE VIEW CLAUDE_BI.LOGGING.SESSION_QUALITY AS
WITH session_metrics AS (
  SELECT 
    session_id,
    MIN(occurred_at) AS session_start,
    MAX(occurred_at) AS session_end,
    COUNT(*) AS total_events,
    COUNT(CASE WHEN event_category = 'quality' THEN 1 END) AS quality_events,
    COUNT(CASE WHEN _source_lane = 'DEAD_LETTER' THEN 1 END) AS dead_letter_events,
    COUNT(CASE WHEN action LIKE '%.compressed' THEN 1 END) AS compressed_events,
    AVG(execution_ms) AS avg_execution_ms,
    MAX(execution_ms) AS max_execution_ms
  FROM CLAUDE_BI.ACTIVITY.EVENTS
  WHERE session_id IS NOT NULL
    AND occurred_at >= DATEADD('day', -7, CURRENT_TIMESTAMP())
  GROUP BY session_id
)
SELECT 
  session_id,
  session_start,
  session_end,
  DATEDIFF('minute', session_start, session_end) AS duration_minutes,
  total_events,
  quality_events,
  dead_letter_events,
  compressed_events,
  ROUND(quality_events * 100.0 / NULLIF(total_events, 0), 2) AS error_rate_percent,
  avg_execution_ms,
  max_execution_ms,
  CASE 
    WHEN quality_events > 10 OR error_rate_percent > 5 THEN 'POOR'
    WHEN quality_events > 5 OR error_rate_percent > 2 THEN 'FAIR'
    ELSE 'GOOD'
  END AS session_quality
FROM session_metrics
ORDER BY session_start DESC;

-- ============================================================================
-- Grant permissions
-- ============================================================================

-- Admin can see all logging views
GRANT SELECT ON VIEW CLAUDE_BI.LOGGING.CLAUDE_CODE_EVENTS TO ROLE MCP_ADMIN_ROLE;
GRANT SELECT ON VIEW CLAUDE_BI.LOGGING.QUALITY_EVENTS TO ROLE MCP_ADMIN_ROLE;
GRANT SELECT ON VIEW CLAUDE_BI.LOGGING.SYSTEM_EVENTS TO ROLE MCP_ADMIN_ROLE;
GRANT SELECT ON VIEW CLAUDE_BI.LOGGING.DEAD_LETTER_QUEUE TO ROLE MCP_ADMIN_ROLE;
GRANT SELECT ON VIEW CLAUDE_BI.LOGGING.COMPRESSION_STATS TO ROLE MCP_ADMIN_ROLE;
GRANT SELECT ON VIEW CLAUDE_BI.LOGGING.SESSION_QUALITY TO ROLE MCP_ADMIN_ROLE;

-- Logger role needs read access to EVENTS for circuit breaker checks
GRANT SELECT ON DYNAMIC TABLE CLAUDE_BI.ACTIVITY.EVENTS TO ROLE MCP_LOGGER_ROLE;

-- ============================================================================
-- Resume Dynamic Table
-- ============================================================================

ALTER DYNAMIC TABLE CLAUDE_BI.ACTIVITY.EVENTS RESUME;

-- ============================================================================
-- Verify Dynamic Table health
-- ============================================================================

-- Check Dynamic Table status
SHOW DYNAMIC TABLES LIKE 'EVENTS' IN SCHEMA CLAUDE_BI.ACTIVITY;

-- Wait for initial refresh
CALL SYSTEM$WAIT(5);

-- Verify data flow
SELECT 
  'Dynamic Table Status' AS check_type,
  COUNT(*) AS total_events,
  COUNT(DISTINCT event_category) AS categories,
  COUNT(DISTINCT session_id) AS unique_sessions,
  MIN(occurred_at) AS earliest_event,
  MAX(occurred_at) AS latest_event
FROM CLAUDE_BI.ACTIVITY.EVENTS
WHERE occurred_at >= DATEADD('hour', -1, CURRENT_TIMESTAMP());