-- Fix Dynamic Table Projection
-- Makes it more forgiving and uses top-level columns

-- @statement
USE DATABASE CLAUDE_BI;

-- @statement
USE SCHEMA ACTIVITY;

-- =====================================================
-- Recreate EVENTS Dynamic Table with better projection
-- =====================================================
-- @statement
CREATE OR REPLACE DYNAMIC TABLE CLAUDE_BI.ACTIVITY.EVENTS
TARGET_LAG = '1 minute'
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
  WHERE _recv_at >= DATEADD('day', -30, CURRENT_TIMESTAMP()) -- Only last 30 days
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