-- ============================================================================
-- Fix Dynamic Table - Backup existing data, recreate as Dynamic Table
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE CLAUDE_BI;
USE SCHEMA ACTIVITY;

-- Step 1: Create backup of existing EVENTS table
CREATE TABLE IF NOT EXISTS CLAUDE_BI.ACTIVITY.EVENTS_BAK AS
SELECT * FROM CLAUDE_BI.ACTIVITY.EVENTS;

-- Show how many rows we're backing up
SELECT COUNT(*) as rows_backed_up FROM CLAUDE_BI.ACTIVITY.EVENTS_BAK;

-- Step 2: Drop the existing regular table
DROP TABLE IF EXISTS CLAUDE_BI.ACTIVITY.EVENTS;

-- Step 3: Create the Dynamic Table
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

-- Step 4: Insert backed up data into RAW_EVENTS so it flows through Dynamic Table
-- Convert backed up events to payload format
INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS
SELECT 
  OBJECT_CONSTRUCT(
    'event_id', event_id,
    'occurred_at', occurred_at,
    'actor_id', actor_id,
    'action', action,
    'object', OBJECT_CONSTRUCT(
      'type', object_type,
      'id', object_id
    ),
    'source', source,
    'schema_version', schema_version,
    'attributes', attributes,
    'depends_on_event_id', depends_on_event_id
  ) AS payload,
  _source_lane,
  _recv_at
FROM CLAUDE_BI.ACTIVITY.EVENTS_BAK;

-- Step 5: Verify restoration
SELECT 
  'Backup Table' as table_name,
  COUNT(*) as row_count 
FROM CLAUDE_BI.ACTIVITY.EVENTS_BAK
UNION ALL
SELECT 
  'RAW_EVENTS' as table_name,
  COUNT(*) as row_count 
FROM CLAUDE_BI.LANDING.RAW_EVENTS
UNION ALL
SELECT 
  'Dynamic Table (after refresh)' as table_name,
  COUNT(*) as row_count 
FROM CLAUDE_BI.ACTIVITY.EVENTS;

-- Show Dynamic Table status
SHOW DYNAMIC TABLES LIKE 'EVENTS' IN SCHEMA CLAUDE_BI.ACTIVITY;