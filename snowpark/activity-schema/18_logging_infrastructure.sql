-- ============================================================================
-- 18_logging_infrastructure.sql
-- Dedicated infrastructure for Claude Code logging MCP
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE CLAUDE_BI;

-- ============================================================================
-- Create dedicated role for logging service
-- ============================================================================

CREATE ROLE IF NOT EXISTS MCP_LOGGER_ROLE
  COMMENT = 'Role for Claude Code logging service - write only to RAW_EVENTS';

-- Grant minimal permissions
GRANT USAGE ON DATABASE CLAUDE_BI TO ROLE MCP_LOGGER_ROLE;
GRANT USAGE ON SCHEMA CLAUDE_BI.MCP TO ROLE MCP_LOGGER_ROLE;
GRANT USAGE ON SCHEMA CLAUDE_BI.LANDING TO ROLE MCP_LOGGER_ROLE;
GRANT USAGE ON SCHEMA CLAUDE_BI.ACTIVITY TO ROLE MCP_LOGGER_ROLE;

-- Only INSERT permission on RAW_EVENTS
GRANT INSERT ON TABLE CLAUDE_BI.LANDING.RAW_EVENTS TO ROLE MCP_LOGGER_ROLE;

-- Grant execute on logging procedures (to be created)
-- GRANT EXECUTE ON PROCEDURE MCP.LOG_DEV_EVENT TO ROLE MCP_LOGGER_ROLE;
-- GRANT EXECUTE ON PROCEDURE MCP.LOG_DEV_BATCH TO ROLE MCP_LOGGER_ROLE;

-- ============================================================================
-- Create dedicated warehouse for logging
-- ============================================================================

CREATE WAREHOUSE IF NOT EXISTS LOG_XS_WH
  WAREHOUSE_SIZE = 'X-SMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Dedicated warehouse for Claude Code event logging - minimal cost';

-- Grant usage to logger role
GRANT USAGE ON WAREHOUSE LOG_XS_WH TO ROLE MCP_LOGGER_ROLE;

-- ============================================================================
-- Create logging service user
-- ============================================================================

-- Create user for the logging MCP service
CREATE USER IF NOT EXISTS MCP_LOGGER_SERVICE
  PASSWORD = 'LoggerServicePassword123!'
  DEFAULT_ROLE = MCP_LOGGER_ROLE
  DEFAULT_WAREHOUSE = LOG_XS_WH
  DEFAULT_NAMESPACE = CLAUDE_BI.MCP
  MUST_CHANGE_PASSWORD = FALSE
  COMMENT = 'Service account for Claude Code logging MCP';

-- Grant role to user
GRANT ROLE MCP_LOGGER_ROLE TO USER MCP_LOGGER_SERVICE;

-- ============================================================================
-- Create context for logging configuration
-- ============================================================================

CREATE OR REPLACE CONTEXT MCP_LOGGING_CTX
  COMMENT = 'Context for logging configuration and settings';

-- Set default configuration
ALTER CONTEXT MCP_LOGGING_CTX SET
  max_batch_size = 500,
  max_event_size_bytes = 102400,  -- 100KB
  default_sample_rate = 1.0,
  circuit_breaker_threshold = 1000,
  circuit_breaker_window_seconds = 60,
  spool_max_size_mb = 50,
  compression_window_ms = 10000;

-- ============================================================================
-- Create schema for logging-specific objects
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS CLAUDE_BI.LOGGING
  COMMENT = 'Schema for Claude Code logging infrastructure';

USE SCHEMA CLAUDE_BI.LOGGING;

-- ============================================================================
-- Create configuration table (uses events pattern)
-- ============================================================================

-- Note: Configuration is stored as events in RAW_EVENTS/EVENTS
-- This view reads the latest configuration
CREATE OR REPLACE VIEW CLAUDE_BI.LOGGING.CURRENT_CONFIG AS
WITH config_events AS (
  SELECT 
    attributes:setting_name::STRING AS setting_name,
    attributes:setting_value::VARIANT AS setting_value,
    attributes:setting_type::STRING AS setting_type,
    occurred_at,
    actor_id AS changed_by,
    ROW_NUMBER() OVER (
      PARTITION BY attributes:setting_name
      ORDER BY occurred_at DESC
    ) AS rn
  FROM CLAUDE_BI.ACTIVITY.EVENTS
  WHERE action = 'system.config.logging'
)
SELECT 
  setting_name,
  setting_value,
  setting_type,
  occurred_at AS last_updated,
  changed_by
FROM config_events
WHERE rn = 1;

-- ============================================================================
-- Create session tracking view
-- ============================================================================

CREATE OR REPLACE VIEW CLAUDE_BI.LOGGING.ACTIVE_SESSIONS AS
WITH session_events AS (
  SELECT 
    attributes:session_id::STRING AS session_id,
    action,
    occurred_at,
    attributes
  FROM CLAUDE_BI.ACTIVITY.EVENTS
  WHERE action IN ('ccode.session.started', 'ccode.session.ended')
    AND occurred_at >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
),
session_status AS (
  SELECT 
    session_id,
    MAX(CASE WHEN action = 'ccode.session.started' THEN occurred_at END) AS started_at,
    MAX(CASE WHEN action = 'ccode.session.ended' THEN occurred_at END) AS ended_at,
    ANY_VALUE(CASE WHEN action = 'ccode.session.started' THEN attributes END) AS start_attributes
  FROM session_events
  GROUP BY session_id
)
SELECT 
  session_id,
  started_at,
  ended_at,
  CASE 
    WHEN ended_at IS NULL THEN 'ACTIVE'
    ELSE 'ENDED'
  END AS status,
  DATEDIFF('minute', started_at, COALESCE(ended_at, CURRENT_TIMESTAMP())) AS duration_minutes,
  start_attributes:version::STRING AS claude_code_version,
  start_attributes:platform::STRING AS platform,
  start_attributes:node_version::STRING AS node_version
FROM session_status
ORDER BY started_at DESC;

-- ============================================================================
-- Create rate limiting tracking
-- ============================================================================

CREATE OR REPLACE VIEW CLAUDE_BI.LOGGING.RATE_LIMITS AS
WITH event_counts AS (
  SELECT 
    attributes:session_id::STRING AS session_id,
    action,
    DATE_TRUNC('minute', occurred_at) AS minute,
    COUNT(*) AS event_count
  FROM CLAUDE_BI.ACTIVITY.EVENTS
  WHERE action LIKE 'ccode.%'
    AND occurred_at >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
  GROUP BY session_id, action, minute
)
SELECT 
  session_id,
  action,
  minute,
  event_count,
  CASE 
    WHEN event_count > 1000 THEN 'EXCEEDED'
    WHEN event_count > 800 THEN 'WARNING'
    ELSE 'OK'
  END AS rate_status
FROM event_counts
WHERE event_count > 100  -- Only show high-volume combinations
ORDER BY event_count DESC;

-- ============================================================================
-- Create quality tracking for logging pipeline
-- ============================================================================

CREATE OR REPLACE VIEW CLAUDE_BI.LOGGING.PIPELINE_QUALITY AS
SELECT 
  DATE_TRUNC('hour', _recv_at) AS hour,
  _source_lane,
  COUNT(*) AS total_events,
  COUNT(CASE WHEN _source_lane = 'CLAUDE_CODE' THEN 1 END) AS claude_code_events,
  COUNT(CASE WHEN _source_lane = 'QUALITY_REJECT' THEN 1 END) AS rejected_events,
  COUNT(CASE WHEN _source_lane = 'DEAD_LETTER' THEN 1 END) AS dead_letter_events,
  AVG(BYTE_LENGTH(TO_JSON(payload))) AS avg_payload_bytes,
  MAX(BYTE_LENGTH(TO_JSON(payload))) AS max_payload_bytes,
  COUNT(DISTINCT payload:session_id::STRING) AS unique_sessions
FROM CLAUDE_BI.LANDING.RAW_EVENTS
WHERE _recv_at >= DATEADD('day', -7, CURRENT_TIMESTAMP())
  AND _source_lane IN ('CLAUDE_CODE', 'QUALITY_REJECT', 'DEAD_LETTER')
GROUP BY hour, _source_lane
ORDER BY hour DESC;

-- ============================================================================
-- Grant permissions to roles
-- ============================================================================

-- Admin role can view all logging infrastructure
GRANT SELECT ON VIEW CLAUDE_BI.LOGGING.CURRENT_CONFIG TO ROLE MCP_ADMIN_ROLE;
GRANT SELECT ON VIEW CLAUDE_BI.LOGGING.ACTIVE_SESSIONS TO ROLE MCP_ADMIN_ROLE;
GRANT SELECT ON VIEW CLAUDE_BI.LOGGING.RATE_LIMITS TO ROLE MCP_ADMIN_ROLE;
GRANT SELECT ON VIEW CLAUDE_BI.LOGGING.PIPELINE_QUALITY TO ROLE MCP_ADMIN_ROLE;

-- Logger role needs access to context
GRANT USAGE ON CONTEXT MCP_LOGGING_CTX TO ROLE MCP_LOGGER_ROLE;

-- ============================================================================
-- Create resource monitor for logging
-- ============================================================================

CREATE OR REPLACE RESOURCE MONITOR LOGGING_MONITOR
  WITH 
    CREDIT_QUOTA = 10  -- 10 credits per month for logging
    FREQUENCY = MONTHLY
    START_TIMESTAMP = IMMEDIATELY
  TRIGGERS
    ON 75 PERCENT DO NOTIFY
    ON 90 PERCENT DO NOTIFY
    ON 100 PERCENT DO SUSPEND;

-- Assign to logging warehouse
ALTER WAREHOUSE LOG_XS_WH SET RESOURCE_MONITOR = LOGGING_MONITOR;

-- ============================================================================
-- Summary
-- ============================================================================

SELECT 'Logging infrastructure created successfully!' AS status,
       'MCP_LOGGER_ROLE' AS role_name,
       'LOG_XS_WH' AS warehouse_name,
       'MCP_LOGGER_SERVICE' AS service_user;