-- ============================================================================
-- 15_enhanced_dynamic_table.sql  
-- Incremental-safe Dynamic Table with dead letter handling
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE CLAUDE_BI;

-- ============================================================================
-- Create dedicated warehouse for Dynamic Table refresh
-- ============================================================================

CREATE WAREHOUSE IF NOT EXISTS DT_XS_WH
  WAREHOUSE_SIZE = 'X-SMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Dedicated warehouse for Dynamic Table refresh - minimal cost';

-- ============================================================================
-- Drop and recreate Dynamic Table with incremental-safe pattern
-- ============================================================================

-- Suspend and drop existing Dynamic Table if exists
ALTER DYNAMIC TABLE IF EXISTS CLAUDE_BI.ACTIVITY.EVENTS SUSPEND;
DROP DYNAMIC TABLE IF EXISTS CLAUDE_BI.ACTIVITY.EVENTS;

-- Create the Dynamic Table with all production improvements
CREATE OR REPLACE DYNAMIC TABLE CLAUDE_BI.ACTIVITY.EVENTS
TARGET_LAG = '1 minute'
WAREHOUSE = DT_XS_WH
CLUSTER BY (DATE_TRUNC('hour', occurred_at), action)
AS
WITH src AS (
  -- Extract and validate payload with size guards
  SELECT 
    payload,
    _source_lane,
    _recv_at,
    -- Size and format validation
    CASE 
      WHEN BYTE_LENGTH(TO_JSON(payload)) > 1000000 THEN 'oversized'
      WHEN TRY_PARSE_JSON(payload::STRING) IS NULL THEN 'malformed'
      WHEN COALESCE(payload:action::STRING, '') = '' THEN 'missing_action'
      WHEN TRY_TO_TIMESTAMP_TZ(payload:occurred_at) IS NULL 
        AND _recv_at IS NULL THEN 'missing_timestamp'
      ELSE 'valid'
    END AS quality_check,
    -- Extract core fields safely
    COALESCE(payload:action::STRING, 'unknown') AS action,
    COALESCE(payload:actor_id::STRING, 'system') AS actor_id,
    COALESCE(payload:source::STRING, _source_lane) AS source,
    COALESCE(payload:object:type::STRING, '') AS object_type,
    COALESCE(payload:object:id::STRING, '') AS object_id,
    COALESCE(
      TRY_TO_TIMESTAMP_TZ(payload:occurred_at),
      _recv_at
    ) AS occurred_at,
    payload:attributes AS attributes,
    COALESCE(payload:schema_version::STRING, '2.1.0') AS schema_version
  FROM CLAUDE_BI.LANDING.RAW_EVENTS
),
valid_events AS (
  -- Keep only valid events
  SELECT 
    payload,
    _source_lane,
    _recv_at,
    action,
    actor_id,
    source,
    object_type,
    object_id,
    occurred_at,
    attributes,
    schema_version
  FROM src 
  WHERE quality_check = 'valid'
),
quality_events AS (
  -- Route bad events to quality lane
  SELECT 
    OBJECT_CONSTRUCT(
      'event_id', SHA2(CONCAT('quality_', payload::STRING, _recv_at::STRING), 256),
      'action', 'quality.' || LOWER(REPLACE(quality_check, '_', '.')),
      'occurred_at', _recv_at,
      'actor_id', 'system',
      'source', 'quality',
      'schema_version', '2.1.0',
      'object', OBJECT_CONSTRUCT(
        'type', 'rejected_event',
        'id', SHA2(payload::STRING, 256)
      ),
      'attributes', OBJECT_CONSTRUCT(
        'reason', quality_check,
        'source_lane', _source_lane,
        'payload_size', BYTE_LENGTH(TO_JSON(payload)),
        'truncated_payload', SUBSTR(payload::STRING, 1, 1000),
        'detected_at', _recv_at
      )
    ) AS payload,
    'QUALITY' AS _source_lane,
    _recv_at,
    'quality.' || LOWER(REPLACE(quality_check, '_', '.')) AS action,
    'system' AS actor_id,
    'quality' AS source,
    'rejected_event' AS object_type,
    SHA2(payload::STRING, 256) AS object_id,
    _recv_at AS occurred_at,
    OBJECT_CONSTRUCT(
      'reason', quality_check,
      'original_source', _source_lane
    ) AS attributes,
    '2.1.0' AS schema_version
  FROM src 
  WHERE quality_check != 'valid'
),
all_events AS (
  -- Combine valid and quality events
  SELECT * FROM valid_events
  UNION ALL
  SELECT * FROM quality_events
),
with_ids AS (
  -- Generate deterministic event IDs using canonical formula
  SELECT
    SHA2(
      CONCAT_WS('|',
        'v2',  -- Version prefix
        COALESCE(action, ''),
        COALESCE(actor_id, ''),
        COALESCE(object_type, ''),
        COALESCE(object_id, ''),
        TO_VARCHAR(occurred_at, 'YYYY-MM-DD"T"HH24:MI:SS.FF3TZH:TZM'),
        COALESCE(_source_lane, ''),
        TO_VARCHAR(_recv_at, 'YYYY-MM-DD"T"HH24:MI:SS.FF3')
      ),
      256
    ) AS event_id,
    action,
    actor_id,
    source,
    object_type,
    object_id,
    occurred_at,
    attributes,
    schema_version,
    _source_lane,
    _recv_at
  FROM all_events
),
first_seen AS (
  -- Incremental-safe deduplication using GROUP BY
  -- This pattern allows incremental refresh unlike ROW_NUMBER()
  SELECT 
    event_id,
    MIN(_recv_at) AS first_recv_at
  FROM with_ids
  GROUP BY event_id
),
deduped AS (
  -- Join back to get full event for first occurrence
  SELECT 
    w.event_id,
    w.occurred_at,
    w.actor_id,
    w.action,
    w.object_type,
    w.object_id,
    w.source,
    w.schema_version,
    w.attributes,
    w._source_lane,
    w._recv_at
  FROM with_ids w
  INNER JOIN first_seen f 
    ON w.event_id = f.event_id 
    AND w._recv_at = f.first_recv_at
)
SELECT * FROM deduped;

-- ============================================================================
-- Add search optimization for common lookups
-- ============================================================================

ALTER TABLE CLAUDE_BI.ACTIVITY.EVENTS 
ADD SEARCH OPTIMIZATION ON EQUALITY(
  event_id,
  action,
  actor_id,
  object_type,
  object_id
);

-- ============================================================================
-- Create quality monitoring view
-- ============================================================================

CREATE OR REPLACE VIEW CLAUDE_BI.MCP.QUALITY_EVENTS AS
SELECT 
  event_id,
  occurred_at,
  action,
  attributes:reason::STRING AS rejection_reason,
  attributes:source_lane::STRING AS original_source,
  attributes:payload_size::NUMBER AS payload_size_bytes,
  attributes:truncated_payload::STRING AS sample_payload
FROM CLAUDE_BI.ACTIVITY.EVENTS
WHERE action LIKE 'quality.%'
ORDER BY occurred_at DESC;

-- ============================================================================
-- Create DT health monitoring view
-- ============================================================================

CREATE OR REPLACE VIEW CLAUDE_BI.MCP.DT_HEALTH AS
WITH refresh_history AS (
  SELECT 
    name,
    state,
    phase,
    phase_end_time,
    details,
    ROW_NUMBER() OVER (ORDER BY phase_end_time DESC) AS rn
  FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(
    NAME => 'CLAUDE_BI.ACTIVITY.EVENTS'
  ))
  WHERE phase = 'COMPLETED'
),
dt_info AS (
  SELECT
    name,
    target_lag,
    warehouse,
    refresh_mode,
    last_suspended_on
  FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLES(
    DATABASE_NAME => 'CLAUDE_BI',
    SCHEMA_NAME => 'ACTIVITY'
  ))
  WHERE name = 'EVENTS'
)
SELECT 
  dt.name,
  dt.target_lag,
  dt.warehouse,
  rh.state AS last_refresh_state,
  rh.phase_end_time AS last_refresh_time,
  DATEDIFF('second', rh.phase_end_time, CURRENT_TIMESTAMP()) AS seconds_since_refresh,
  PARSE_JSON(rh.details):rows_inserted::NUMBER AS rows_inserted,
  PARSE_JSON(rh.details):rows_updated::NUMBER AS rows_updated,
  CASE 
    WHEN dt.last_suspended_on IS NOT NULL THEN 'SUSPENDED'
    WHEN seconds_since_refresh > 120 THEN 'LAG_WARNING'  -- 2+ minutes
    WHEN seconds_since_refresh > 300 THEN 'LAG_CRITICAL' -- 5+ minutes
    ELSE 'HEALTHY'
  END AS health_status,
  CASE
    WHEN dt.last_suspended_on IS NOT NULL THEN 
      'Dynamic Table is suspended'
    WHEN seconds_since_refresh > 120 THEN 
      'Refresh lag exceeds target'
    ELSE 'Operating normally'
  END AS health_message
FROM dt_info dt
LEFT JOIN refresh_history rh ON dt.name = rh.name AND rh.rn = 1;

-- ============================================================================
-- Create alert for DT lag
-- ============================================================================

CREATE WAREHOUSE IF NOT EXISTS ALERT_WH
  WAREHOUSE_SIZE = 'X-SMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Warehouse for monitoring alerts';

CREATE OR REPLACE ALERT CLAUDE_BI.MCP.DT_LAG_ALERT
  WAREHOUSE = ALERT_WH
  SCHEDULE = '5 MINUTE'
  IF (EXISTS (
    SELECT 1 FROM CLAUDE_BI.MCP.DT_HEALTH
    WHERE health_status IN ('LAG_WARNING', 'LAG_CRITICAL', 'SUSPENDED')
  ))
  THEN CALL SYSTEM$SEND_EMAIL(
    'admin@company.com',
    'Dynamic Table Alert',
    'The EVENTS Dynamic Table is experiencing issues. Check DT_HEALTH view for details.'
  );

-- Resume the alert
ALTER ALERT CLAUDE_BI.MCP.DT_LAG_ALERT RESUME;

-- ============================================================================
-- Grant permissions
-- ============================================================================

GRANT SELECT ON DYNAMIC TABLE CLAUDE_BI.ACTIVITY.EVENTS TO ROLE MCP_SERVICE_ROLE;
GRANT SELECT ON DYNAMIC TABLE CLAUDE_BI.ACTIVITY.EVENTS TO ROLE MCP_USER_ROLE;
GRANT SELECT ON VIEW CLAUDE_BI.MCP.QUALITY_EVENTS TO ROLE MCP_ADMIN_ROLE;
GRANT SELECT ON VIEW CLAUDE_BI.MCP.DT_HEALTH TO ROLE MCP_ADMIN_ROLE;

-- ============================================================================
-- Verify setup
-- ============================================================================

-- Check Dynamic Table status
SHOW DYNAMIC TABLES LIKE 'EVENTS' IN SCHEMA CLAUDE_BI.ACTIVITY;

-- Check recent quality events
SELECT * FROM CLAUDE_BI.MCP.QUALITY_EVENTS LIMIT 10;

-- Check DT health
SELECT * FROM CLAUDE_BI.MCP.DT_HEALTH;