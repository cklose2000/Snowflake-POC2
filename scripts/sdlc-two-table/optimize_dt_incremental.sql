-- ============================================================================
-- optimize_dt_incremental.sql
-- Convert ACTIVITY.EVENTS Dynamic Table to INCREMENTAL refresh mode
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE CLAUDE_BI;
USE SCHEMA ACTIVITY;

-- ============================================================================
-- OPTIMIZED DYNAMIC TABLE - INCREMENTAL REFRESH MODE
-- ============================================================================
-- Key Change: Replace CURRENT_TIMESTAMP with fixed date to enable INCREMENTAL mode
-- This allows Snowflake to track changes incrementally instead of full refresh

CREATE OR REPLACE DYNAMIC TABLE CLAUDE_BI.ACTIVITY.EVENTS
  TARGET_LAG = '1 minute'
  REFRESH_MODE = 'INCREMENTAL'  -- Explicit INCREMENTAL mode
  WAREHOUSE = CLAUDE_AGENT_WH
AS
WITH base AS (
  SELECT
    -- Use top-level columns first, then fall back to payload
    COALESCE(
      occurred_at,
      TRY_TO_TIMESTAMP_TZ(payload:occurred_at::STRING),
      _recv_at
    ) AS occurred_at,
    
    COALESCE(
      action,
      payload:action::STRING,
      'unknown'
    ) AS action,
    
    COALESCE(
      actor,
      payload:actor_id::STRING,
      payload:actor::STRING,
      'system'
    ) AS actor_id,
    
    -- Extract object fields from payload
    COALESCE(
      payload:object:type::STRING,
      payload:object_type::STRING,
      ''
    ) AS object_type,
    
    COALESCE(
      payload:object:id::STRING,
      payload:object_id::STRING,
      ''
    ) AS object_id,
    
    -- Event ID for uniqueness
    COALESCE(
      payload:event_id::STRING,
      dedupe_key,
      SHA2(CONCAT(action, actor, occurred_at::STRING), 256)
    ) AS event_id,
    
    -- Keep full payload as attributes
    payload AS attributes,
    
    -- Source tracking
    _source_lane AS source,
    _recv_at AS ingested_at,
    
    -- Dedupe key
    COALESCE(
      dedupe_key,
      payload:event_id::STRING,
      SHA2(TO_VARCHAR(payload), 256)
    ) AS dkey
    
  FROM CLAUDE_BI.LANDING.RAW_EVENTS
  -- OPTIMIZATION: Replace CURRENT_TIMESTAMP with fixed date for INCREMENTAL mode
  -- This allows Snowflake to process only new/changed data instead of full table
  WHERE _recv_at >= '2024-01-01'::TIMESTAMP_TZ  -- Fixed date instead of CURRENT_TIMESTAMP
),
dedup AS (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY dkey ORDER BY ingested_at DESC) AS rn
  FROM base
)
SELECT 
  event_id,
  occurred_at,
  action,
  actor_id,
  object_type,
  object_id,
  attributes,
  source,
  ingested_at
FROM dedup
WHERE rn = 1
  AND occurred_at IS NOT NULL
  AND action IS NOT NULL
  AND actor_id IS NOT NULL;

-- ============================================================================
-- Verification Query
-- ============================================================================
-- Check that the Dynamic Table is now in INCREMENTAL mode
SELECT 
  'After Optimization' AS status,
  NAME,
  REFRESH_MODE,
  REFRESH_MODE_REASON,
  TARGET_LAG,
  SCHEDULING_STATE
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_GRAPH_HISTORY())
WHERE QUALIFIED_NAME = 'CLAUDE_BI.ACTIVITY.EVENTS'
ORDER BY DATA_TIMESTAMP DESC
LIMIT 1;