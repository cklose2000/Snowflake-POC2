-- Quick deploy script for schema awareness views
-- Simplified single statements to avoid splitter issues

USE DATABASE CLAUDE_BI;
USE SCHEMA MCP;

-- VW_LATEST_SCHEMA: Latest schema state from events
CREATE OR REPLACE SECURE VIEW VW_LATEST_SCHEMA AS
WITH schema_events AS (
  SELECT
    occurred_at,
    event_id,
    actor_id,
    action,
    attributes:version::string AS schema_version,
    attributes:source::string AS source,
    obj.value AS object_def
  FROM CLAUDE_BI.ACTIVITY.EVENTS
  ,LATERAL FLATTEN(input => attributes:objects) obj
  WHERE action IN ('mcp.schema.published', 'mcp.schema.updated', 'mcp.schema.retired')
    AND attributes:objects IS NOT NULL
),
latest_per_object AS (
  SELECT
    object_def:object_name::string AS object_name,
    object_def:object_type::string AS object_type,
    object_def:db::string AS database_name,
    object_def:schema::string AS schema_name,
    object_def:definition AS definition,
    object_def:ddl::string AS ddl_text,
    object_def:status::string AS status,
    object_def:category::string AS category,
    object_def:description::string AS description,
    object_def:signature::string AS signature,
    object_def:example::string AS example_call,
    object_def:comment::string AS comment_text,
    schema_version,
    source,
    occurred_at,
    event_id,
    actor_id,
    ROW_NUMBER() OVER (
      PARTITION BY 
        object_def:object_name::string,
        object_def:object_type::string,
        object_def:db::string,
        object_def:schema::string
      ORDER BY occurred_at DESC, event_id DESC
    ) AS rn
  FROM schema_events
)
SELECT
  database_name,
  schema_name,
  object_name,
  object_type,
  COALESCE(status, 'active') AS status,
  category,
  description,
  signature,
  example_call,
  comment_text,
  definition,
  ddl_text,
  schema_version,
  source,
  occurred_at AS last_updated,
  event_id AS last_event_id,
  actor_id AS last_updated_by
FROM latest_per_object
WHERE rn = 1;