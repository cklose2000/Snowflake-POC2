-- ============================================================================
-- 05_dynamic_tables.sql
-- Dynamic table pipeline and future grants
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE CLAUDE_BI;

-- ============================================================================
-- DYNAMIC TABLE: RAW_EVENTS → EVENTS
-- Explicit pipeline for canonical event structure
-- ============================================================================

-- Ensure ACTIVITY schema exists
CREATE SCHEMA IF NOT EXISTS CLAUDE_BI.ACTIVITY;

-- Create the dynamic table
CREATE OR REPLACE DYNAMIC TABLE CLAUDE_BI.ACTIVITY.EVENTS
TARGET_LAG = '1 minute'
WAREHOUSE = CLAUDE_WAREHOUSE
AS
SELECT
  -- Core event fields
  COALESCE(payload:event_id::STRING, SYSTEM$UUID()) AS event_id,
  payload:action::STRING AS action,
  COALESCE(payload:actor_id::STRING, payload:user_id::STRING, 'system') AS actor_id,
  
  -- Timestamps
  COALESCE(
    TRY_TO_TIMESTAMP(payload:occurred_at::STRING),
    ingested_at,
    CURRENT_TIMESTAMP()
  ) AS occurred_at,
  
  -- Object reference (optional)
  payload:object AS object,
  
  -- Flexible attributes
  payload:attributes AS attributes,
  
  -- Metadata
  OBJECT_CONSTRUCT(
    'source_lane', source_lane,
    'ingested_at', ingested_at,
    'claude_meta', payload:_claude_meta,
    'audit', payload:_audit
  ) AS metadata,
  
  -- Session tracking
  COALESCE(
    payload:session_id::STRING,
    payload:_claude_meta:session::STRING
  ) AS session_id,
  
  -- Processing metadata
  ingested_at,
  source_lane,
  CURRENT_TIMESTAMP() AS processed_at
  
FROM CLAUDE_BI.LANDING.RAW_EVENTS
WHERE payload IS NOT NULL
  AND payload:action IS NOT NULL;

-- ============================================================================
-- GRANT-ON-FUTURE PATTERNS
-- Automatic permission inheritance
-- ============================================================================

-- Future schemas in database
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE CLAUDE_BI TO ROLE R_APP_READ;
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE CLAUDE_BI TO ROLE R_APP_WRITE;

-- Future views in MCP schema
GRANT SELECT ON FUTURE VIEWS IN SCHEMA CLAUDE_BI.MCP TO ROLE R_APP_READ;

-- Future tables in LANDING schema (for write operations)
GRANT INSERT ON FUTURE TABLES IN SCHEMA CLAUDE_BI.LANDING TO ROLE R_APP_WRITE;

-- Future procedures in MCP schema
GRANT USAGE ON FUTURE PROCEDURES IN SCHEMA CLAUDE_BI.MCP TO ROLE R_APP_READ;
GRANT USAGE ON FUTURE PROCEDURES IN SCHEMA CLAUDE_BI.MCP TO ROLE R_APP_WRITE;

-- Future functions in MCP schema
GRANT USAGE ON FUTURE FUNCTIONS IN SCHEMA CLAUDE_BI.MCP TO ROLE R_APP_READ;

-- ============================================================================
-- RESOURCE MONITORS
-- Cost control and guardrails
-- ============================================================================

-- Create monitor for Claude agent warehouse
CREATE OR REPLACE RESOURCE MONITOR CLAUDE_AGENT_MONITOR
  WITH 
    CREDIT_QUOTA = 10  -- 10 credits per month
    FREQUENCY = MONTHLY
    START_TIMESTAMP = IMMEDIATELY
    TRIGGERS
      ON 75 PERCENT DO NOTIFY
      ON 90 PERCENT DO NOTIFY
      ON 100 PERCENT DO SUSPEND;

-- Create monitor for analyst warehouse
CREATE OR REPLACE RESOURCE MONITOR CLAUDE_ANALYST_MONITOR
  WITH
    CREDIT_QUOTA = 50  -- 50 credits per month
    FREQUENCY = MONTHLY
    START_TIMESTAMP = IMMEDIATELY
    TRIGGERS
      ON 75 PERCENT DO NOTIFY
      ON 90 PERCENT DO NOTIFY
      ON 100 PERCENT DO SUSPEND;

-- ============================================================================
-- DEDICATED WAREHOUSES WITH MONITORS
-- ============================================================================

-- Agent warehouse (XS, auto-suspend quickly)
CREATE OR REPLACE WAREHOUSE CLAUDE_AGENT_WH
  WITH
    WAREHOUSE_SIZE = 'X-SMALL'
    MAX_CLUSTER_COUNT = 1
    MIN_CLUSTER_COUNT = 1
    SCALING_POLICY = 'STANDARD'
    AUTO_SUSPEND = 60  -- 1 minute
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    RESOURCE_MONITOR = 'CLAUDE_AGENT_MONITOR'
    COMMENT = 'Dedicated warehouse for Claude Code agent operations';

-- Optional: Separate logging warehouse (for isolation)
CREATE OR REPLACE WAREHOUSE LOG_XS_WH
  WITH
    WAREHOUSE_SIZE = 'X-SMALL'
    MAX_CLUSTER_COUNT = 1
    MIN_CLUSTER_COUNT = 1
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Isolated warehouse for logging operations';

-- Grant warehouse usage
GRANT USAGE ON WAREHOUSE CLAUDE_AGENT_WH TO ROLE R_APP_WRITE;
GRANT USAGE ON WAREHOUSE CLAUDE_AGENT_WH TO ROLE R_APP_READ;
GRANT USAGE ON WAREHOUSE LOG_XS_WH TO ROLE R_APP_WRITE;

-- ============================================================================
-- GRANT DYNAMIC TABLE ACCESS
-- ============================================================================

-- Grant access to the dynamic table
GRANT SELECT ON DYNAMIC TABLE CLAUDE_BI.ACTIVITY.EVENTS TO ROLE R_APP_READ;

-- Grant schema usage
GRANT USAGE ON SCHEMA CLAUDE_BI.ACTIVITY TO ROLE R_APP_READ;
GRANT USAGE ON SCHEMA CLAUDE_BI.ACTIVITY TO ROLE R_APP_WRITE;

-- ============================================================================
-- DATA RETENTION POLICY (Optional)
-- ============================================================================

-- Set data retention for raw events (90 days)
ALTER TABLE CLAUDE_BI.LANDING.RAW_EVENTS SET DATA_RETENTION_TIME_IN_DAYS = 90;

-- ============================================================================
-- QUERY TAG DEFAULTS
-- Set default tags for observability
-- ============================================================================

-- Create a function to generate default query tags
CREATE OR REPLACE FUNCTION CLAUDE_BI.MCP.GENERATE_QUERY_TAG(
  operation STRING,
  agent STRING DEFAULT 'claude-code'
)
RETURNS STRING
LANGUAGE SQL
IMMUTABLE
AS $$
  OBJECT_CONSTRUCT(
    'agent', agent,
    'operation', operation,
    'timestamp', CURRENT_TIMESTAMP()::STRING,
    'warehouse', CURRENT_WAREHOUSE(),
    'user', CURRENT_USER()
  )::STRING
$$;

GRANT USAGE ON FUNCTION CLAUDE_BI.MCP.GENERATE_QUERY_TAG(STRING, STRING) TO ROLE R_APP_READ;
GRANT USAGE ON FUNCTION CLAUDE_BI.MCP.GENERATE_QUERY_TAG(STRING, STRING) TO ROLE R_APP_WRITE;

-- ============================================================================
-- COMPRESSION OPTIMIZATION (for large-scale events)
-- ============================================================================

-- Enable auto-compression on the raw events table
ALTER TABLE CLAUDE_BI.LANDING.RAW_EVENTS 
  SET CHANGE_TRACKING = TRUE
  COMMENT = 'Raw event ingestion with auto-compression and change tracking';

-- ============================================================================
-- Success message
-- ============================================================================
SELECT 'Dynamic tables and resource controls created!' AS status,
       'Warehouses configured with monitors' AS detail,
       'Next: Run 06_monitoring_views.sql' AS next_action;