-- ============================================================================
-- 03_create_dynamic_table.sql
-- Dynamic Table with deduplication, validation, and dependency checking
-- This is the single source of truth that auto-refreshes every minute
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE CLAUDE_BI;
USE SCHEMA ACTIVITY;

-- Drop if exists for clean setup
DROP DYNAMIC TABLE IF EXISTS CLAUDE_BI.ACTIVITY.EVENTS;

-- Create the Dynamic Table (auto-refreshes from RAW_EVENTS)
CREATE DYNAMIC TABLE CLAUDE_BI.ACTIVITY.EVENTS
TARGET_LAG = '1 minute'
WAREHOUSE = DT_XS_WH
AS
WITH src AS (
  -- Extract and validate core fields
  SELECT
    payload,
    _source_lane,
    _recv_at,
    payload:event_id::string            AS event_id_raw,
    payload:occurred_at::timestamp_tz   AS occurred_at,
    payload:actor_id::string            AS actor_id,
    payload:action::string              AS action,
    payload:object.type::string         AS object_type,
    payload:object.id::string           AS object_id,
    payload:source::string              AS source,
    payload:schema_version::string      AS schema_version,
    payload:attributes                  AS attributes,
    payload:depends_on_event_id::string AS depends_on_event_id,
    -- Poison-pill guard: reject payloads > 1MB
    (LENGTH(payload::string) < 1000000) AS size_ok
  FROM CLAUDE_BI.LANDING.RAW_EVENTS
),
filtered AS (
  -- Apply validation rules
  SELECT *
  FROM src
  WHERE action IS NOT NULL
    AND occurred_at IS NOT NULL
    AND size_ok = TRUE
    -- Enforce reserved namespace rules
    AND NOT (action LIKE 'system.%' AND source <> 'system')
    AND NOT (action LIKE 'mcp.%' AND source <> 'mcp')
    AND NOT (action LIKE 'quality.%' AND source <> 'quality')
),
evolved AS (
  -- Generate synthetic IDs and evolve schema versions
  SELECT
    COALESCE(event_id_raw, 'sys_' || UUID_STRING()) AS event_id,
    occurred_at,
    actor_id,
    action,
    object_type,
    object_id,
    source,
    -- Auto-evolve 2.0.x to 2.1.0
    CASE 
      WHEN schema_version LIKE '2.0%' THEN '2.1.0' 
      ELSE schema_version 
    END AS schema_version,
    attributes,
    depends_on_event_id,
    _source_lane,
    _recv_at,
    -- Hash for deduplication
    HASH(source, event_id_raw, occurred_at) AS event_hash
  FROM filtered
),
sequenced AS (
  -- Add deterministic sequence number for events at same timestamp
  SELECT
    e.*,
    ROW_NUMBER() OVER (
      PARTITION BY occurred_at 
      ORDER BY _recv_at, event_id
    ) AS event_sequence
  FROM evolved e
),
dedup AS (
  -- Remove duplicates based on event hash
  SELECT *
  FROM (
    SELECT 
      s.*, 
      ROW_NUMBER() OVER (
        PARTITION BY event_hash 
        ORDER BY _recv_at
      ) AS rn
    FROM sequenced s
  ) 
  WHERE rn = 1
),
ready AS (
  -- Check dependencies exist
  SELECT 
    d.*,
    (depends_on_event_id IS NULL OR EXISTS (
      SELECT 1 FROM dedup p 
      WHERE p.event_id = d.depends_on_event_id
    )) AS dependency_ok
  FROM dedup d
)
SELECT
  event_id,
  occurred_at,
  actor_id,
  action,
  object_type,
  object_id,
  source,
  schema_version,
  -- Embed sequence into attributes.meta for auditability
  OBJECT_INSERT(
    attributes, 
    'meta',
    OBJECT_INSERT(
      COALESCE(attributes:meta, OBJECT_CONSTRUCT()), 
      'sequence', 
      TO_VARIANT(event_sequence), 
      TRUE
    ),
    TRUE
  ) AS attributes,
  depends_on_event_id,
  _source_lane,
  _recv_at
FROM ready
WHERE dependency_ok = TRUE;

-- Verify Dynamic Table
SHOW DYNAMIC TABLES LIKE 'EVENTS' IN SCHEMA CLAUDE_BI.ACTIVITY;