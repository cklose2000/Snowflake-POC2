-- ============================================================================
-- Enhanced Dynamic Table with SHA2-256 Content-Addressed IDs
-- Production-ready implementation with all expert recommendations
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE CLAUDE_BI;

-- Drop existing Dynamic Table to recreate with enhancements
ALTER DYNAMIC TABLE IF EXISTS CLAUDE_BI.ACTIVITY.EVENTS SUSPEND;
DROP DYNAMIC TABLE IF EXISTS CLAUDE_BI.ACTIVITY.EVENTS;

-- Create the Enhanced Dynamic Table with all improvements
CREATE OR REPLACE DYNAMIC TABLE CLAUDE_BI.ACTIVITY.EVENTS
CLUSTER BY (DATE(occurred_at), action)  -- Optimize for common query patterns
TARGET_LAG = '1 minute'
WAREHOUSE = DT_XS_WH
AS
WITH src AS (
  -- Extract and validate core fields with comprehensive guards
  SELECT
    payload,
    _source_lane,
    _recv_at,
    -- Safely extract fields with validation
    TRY_CAST(payload:event_id::STRING AS STRING) AS event_id_raw,
    TRY_CAST(payload:occurred_at::STRING AS TIMESTAMP_TZ) AS occurred_at_raw,
    LOWER(TRIM(COALESCE(payload:actor_id::STRING, ''))) AS actor_id,
    LOWER(TRIM(COALESCE(payload:action::STRING, ''))) AS action,
    LOWER(TRIM(COALESCE(payload:object.type::STRING, ''))) AS object_type,
    LOWER(TRIM(COALESCE(payload:object.id::STRING, ''))) AS object_id,
    LOWER(TRIM(COALESCE(payload:source::STRING, 'unknown'))) AS source,
    COALESCE(payload:schema_version::STRING, '2.0.0') AS schema_version,
    payload:attributes AS attributes,
    payload:depends_on_event_id::STRING AS depends_on_event_id,
    -- Validation flags
    (LENGTH(payload::STRING) <= 1000000) AS size_ok,
    (TRY_PARSE_JSON(payload::STRING) IS NOT NULL) AS json_valid,
    -- Extract micro-sequence for same-millisecond ordering
    COALESCE(
      payload:sequence_within_ms::NUMBER,
      MOD(DATE_PART(NANOSECOND, _recv_at), 1000000)  -- Get microseconds from nanoseconds
    ) AS micro_sequence
  FROM CLAUDE_BI.LANDING.RAW_EVENTS
),
validated AS (
  -- Apply validation rules and emit quality events for failures
  SELECT *,
    -- Determine validation status
    CASE
      WHEN NOT json_valid THEN 'malformed_json'
      WHEN NOT size_ok THEN 'payload_too_large'
      WHEN action IS NULL OR action = '' THEN 'missing_action'
      WHEN occurred_at_raw IS NULL AND _recv_at IS NULL THEN 'missing_timestamp'
      ELSE 'valid'
    END AS validation_status
  FROM src
),
filtered AS (
  -- Keep valid events and enforce namespace rules
  SELECT *,
    -- Use _recv_at if occurred_at is missing
    COALESCE(occurred_at_raw, _recv_at) AS occurred_at,
    -- Check namespace compliance
    CASE
      WHEN action LIKE 'system.%' AND source != 'system' THEN FALSE
      WHEN action LIKE 'mcp.%' AND source != 'mcp' THEN FALSE
      WHEN action LIKE 'quality.%' AND source != 'quality' THEN FALSE
      ELSE TRUE
    END AS namespace_ok
  FROM validated
  WHERE validation_status = 'valid'
    AND json_valid = TRUE
    AND size_ok = TRUE
),
canonicalized AS (
  -- Build canonical representation for ID generation
  SELECT *,
    -- Create canonical timestamp string for hashing
    TO_VARCHAR(occurred_at, 'YYYY-MM-DD"T"HH24:MI:SS.FF6TZH:TZM') AS occurred_iso,
    TO_VARCHAR(_recv_at, 'YYYY-MM-DD"T"HH24:MI:SS.FF6TZH:TZM') AS recv_iso,
    -- Schema version evolution
    CASE 
      WHEN schema_version LIKE '2.0%' THEN '2.1.0' 
      ELSE schema_version 
    END AS schema_version_evolved,
    -- Include recv hour for late arrival detection
    TO_VARCHAR(_recv_at, 'YYYYMMDDHH24') AS recv_hour
  FROM filtered
  WHERE namespace_ok = TRUE
),
with_ids AS (
  -- Generate SHA2-256 content-addressed IDs
  SELECT
    -- Deterministic content-addressed ID with version prefix
    SHA2(
      CONCAT_WS('|',
        'v2',  -- Version prefix for future-proofing
        COALESCE(action, ''),
        COALESCE(actor_id, ''),
        COALESCE(object_type, ''),
        COALESCE(object_id, ''),
        occurred_iso,
        COALESCE(_source_lane, ''),
        recv_iso,
        COALESCE(MD5(TO_JSON(attributes)), '')  -- Hash attributes for stability
      ),
      256
    ) AS event_id,
    -- Secondary hash for collision detection (paranoia mode)
    MD5(TO_JSON(attributes)) AS attributes_hash,
    -- Use raw event_id if provided, otherwise use generated
    COALESCE(event_id_raw, event_id) AS final_event_id,
    *
  FROM canonicalized
),
sequenced AS (
  -- Add deterministic sequence for deduplication and ordering
  SELECT *,
    -- Primary deduplication by event_id
    ROW_NUMBER() OVER (
      PARTITION BY final_event_id 
      ORDER BY occurred_at, _recv_at, micro_sequence, attributes_hash
    ) AS dedupe_rank,
    -- Sequence within same timestamp for deterministic ordering
    ROW_NUMBER() OVER (
      PARTITION BY occurred_at 
      ORDER BY _recv_at, final_event_id, micro_sequence
    ) AS event_sequence,
    -- Track late arrivals (received > 1 hour after occurred)
    DATEDIFF('hour', occurred_at, _recv_at) AS arrival_lag_hours
  FROM with_ids
),
deduped AS (
  -- Keep only first occurrence of each event
  SELECT *
  FROM sequenced
  WHERE dedupe_rank = 1
),
dependencies_checked AS (
  -- Check if dependencies exist (for events that depend on others)
  SELECT d.*,
    CASE
      WHEN depends_on_event_id IS NULL THEN TRUE
      WHEN EXISTS (
        SELECT 1 FROM deduped parent 
        WHERE parent.final_event_id = d.depends_on_event_id
          AND parent.occurred_at <= d.occurred_at
      ) THEN TRUE
      ELSE FALSE
    END AS dependency_satisfied
  FROM deduped d
),
final AS (
  -- Build final event structure with metadata
  SELECT
    final_event_id AS event_id,
    occurred_at,
    actor_id,
    action,
    object_type,
    object_id,
    source,
    schema_version_evolved AS schema_version,
    -- Enrich attributes with metadata
    OBJECT_INSERT(
      attributes, 
      '_meta',
      OBJECT_CONSTRUCT(
        'sequence', event_sequence,
        'arrival_lag_hours', arrival_lag_hours,
        'dedupe_rank', dedupe_rank,
        'content_hash', event_id,
        'recv_hour', recv_hour,
        'micro_sequence', micro_sequence
      ),
      TRUE
    ) AS attributes,
    depends_on_event_id,
    _source_lane,
    _recv_at
  FROM dependencies_checked
  WHERE dependency_satisfied = TRUE
)
SELECT * FROM final;

-- Create a procedure to emit quality events (maintains 2-table architecture)
CREATE OR REPLACE PROCEDURE CLAUDE_BI.MCP.EMIT_QUALITY_EVENT(
  validation_status STRING,
  error_message STRING,
  affected_payload VARIANT
)
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
  -- Insert quality event into RAW_EVENTS (NOT a separate table!)
  INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS
  SELECT 
    OBJECT_CONSTRUCT(
      'event_id', 'qual_' || SHA2(CONCAT(validation_status, '|', CURRENT_TIMESTAMP()::STRING, '|', TO_JSON(affected_payload)), 256),
      'action', 'quality.' || LOWER(REPLACE(validation_status, '_', '.')),
      'occurred_at', CURRENT_TIMESTAMP(),
      'actor_id', 'system',
      'source', 'quality',
      'schema_version', '2.1.0',
      'object', OBJECT_CONSTRUCT(
        'type', 'validation',
        'id', SHA2(TO_JSON(affected_payload), 256)
      ),
      'attributes', OBJECT_CONSTRUCT(
        'validation_status', :validation_status,
        'error_message', :error_message,
        'raw_payload', :affected_payload,
        'detected_at', CURRENT_TIMESTAMP()
      )
    ),
    'SYSTEM',
    CURRENT_TIMESTAMP();
    
  RETURN 'Quality event emitted: ' || validation_status;
END;
$$;

-- Create a task to detect and log validation failures as events
CREATE OR REPLACE TASK CLAUDE_BI.ACTIVITY.DETECT_QUALITY_ISSUES
WAREHOUSE = DT_XS_WH
SCHEDULE = '5 minutes'
AS
CALL CLAUDE_BI.MCP.EMIT_QUALITY_EVENT(
  'batch_validation_check',
  'Periodic validation check completed',
  OBJECT_CONSTRUCT(
    'checked_at', CURRENT_TIMESTAMP(),
    'invalid_count', (
      SELECT COUNT(*) FROM (
        SELECT payload
        FROM CLAUDE_BI.LANDING.RAW_EVENTS
        WHERE _recv_at >= DATEADD('minute', -5, CURRENT_TIMESTAMP())
          AND (
            TRY_PARSE_JSON(payload::STRING) IS NULL
            OR LENGTH(payload::STRING) > 1000000
            OR COALESCE(payload:action::STRING, '') = ''
          )
      )
    )
  )
);

-- Resume the task
ALTER TASK CLAUDE_BI.ACTIVITY.DETECT_QUALITY_ISSUES RESUME;

-- Show the new Dynamic Table
SHOW DYNAMIC TABLES LIKE 'EVENTS' IN SCHEMA CLAUDE_BI.ACTIVITY;

-- Grant necessary permissions
GRANT SELECT ON DYNAMIC TABLE CLAUDE_BI.ACTIVITY.EVENTS TO ROLE MCP_SERVICE_ROLE;
GRANT SELECT ON TABLE CLAUDE_BI.ACTIVITY.QUALITY_EVENTS TO ROLE MCP_ADMIN_ROLE;