-- ============================================================================
-- Schema Awareness Infrastructure - Events Only (No Tables!)
-- Part of WORK-00004: Implement Events-Only Schema Awareness for SF CLI Wrapper
-- ============================================================================

USE DATABASE CLAUDE_BI;

-- @statement

USE SCHEMA MCP;

-- @statement
-- ============================================================================
-- VW_LATEST_SCHEMA: Latest schema state from events (canonical truth)
-- ============================================================================
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

-- @statement
-- ============================================================================
-- VW_IS_PROCEDURES: Agent-friendly procedure catalog
-- ============================================================================
CREATE OR REPLACE SECURE VIEW VW_IS_PROCEDURES AS
SELECT
  database_name,
  schema_name,
  object_name AS procedure_name,
  signature,
  category,
  description,
  example_call,
  comment_text,
  definition,
  last_updated,
  last_updated_by,
  CASE 
    WHEN category IN ('mcp', 'agent', 'query') THEN true
    ELSE false
  END AS is_mcp_compatible,
  CASE
    WHEN signature LIKE '%VARIANT%' THEN 'json'
    WHEN signature LIKE '%STRING%' THEN 'text'
    ELSE 'mixed'
  END AS parameter_type
FROM VW_LATEST_SCHEMA
WHERE object_type = 'PROCEDURE'
  AND status = 'active'
ORDER BY category, procedure_name;

-- @statement
-- ============================================================================
-- VW_IS_OBJECTS: Tables and views catalog
-- ============================================================================
CREATE OR REPLACE SECURE VIEW VW_IS_OBJECTS AS
SELECT
  database_name,
  schema_name,
  object_name,
  object_type,
  description,
  comment_text,
  last_updated,
  last_updated_by,
  definition:row_count::number AS estimated_rows,
  definition:size_mb::number AS size_mb,
  CASE
    WHEN object_type = 'VIEW' AND schema_name = 'MCP' THEN 'agent_accessible'
    WHEN object_type = 'DYNAMIC TABLE' THEN 'event_stream'
    WHEN object_type = 'TABLE' THEN 'base_table'
    ELSE 'other'
  END AS access_category
FROM VW_LATEST_SCHEMA
WHERE object_type IN ('TABLE', 'VIEW', 'DYNAMIC TABLE', 'MATERIALIZED VIEW')
  AND status = 'active'
ORDER BY database_name, schema_name, object_name;

-- @statement
-- ============================================================================
-- VW_IS_COLUMNS: Column metadata from schema events
-- ============================================================================
CREATE OR REPLACE SECURE VIEW VW_IS_COLUMNS AS
WITH columns_flattened AS (
  SELECT
    database_name,
    schema_name,
    object_name,
    object_type,
    col.value:name::string AS column_name,
    col.value:type::string AS data_type,
    col.value:nullable::boolean AS is_nullable,
    col.value:comment::string AS column_comment,
    col.value:default::string AS default_value,
    last_updated,
    last_updated_by
  FROM VW_LATEST_SCHEMA
  ,LATERAL FLATTEN(input => definition:columns) col
  WHERE status = 'active'
    AND definition:columns IS NOT NULL
)
SELECT
  database_name,
  schema_name,
  object_name,
  object_type,
  column_name,
  data_type,
  COALESCE(is_nullable, true) AS is_nullable,
  column_comment,
  default_value,
  last_updated,
  last_updated_by,
  -- Convenience flags
  CASE
    WHEN data_type LIKE '%VARIANT%' THEN 'json'
    WHEN data_type LIKE '%TIMESTAMP%' THEN 'datetime'
    WHEN data_type LIKE '%NUMBER%' OR data_type LIKE '%INT%' THEN 'numeric'
    WHEN data_type LIKE '%VARCHAR%' OR data_type LIKE '%STRING%' THEN 'text'
    WHEN data_type LIKE '%BOOLEAN%' THEN 'boolean'
    ELSE 'other'
  END AS type_category
FROM columns_flattened
ORDER BY database_name, schema_name, object_name, column_name;

-- @statement
-- ============================================================================
-- PUBLISH_SCHEMA_SNAPSHOT: Capture and publish current schema as events
-- ============================================================================
CREATE OR REPLACE PROCEDURE PUBLISH_SCHEMA_SNAPSHOT(
  include_ddl BOOLEAN DEFAULT FALSE,
  schema_filter STRING DEFAULT 'MCP'
)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS $$
const SF = snowflake;

try {
  const includeFullDDL = INCLUDE_DDL || false;
  const schemaFilter = SCHEMA_FILTER || 'MCP';
  
  // Get current procedures
  const procSQL = `
    SELECT 
      procedure_catalog as db,
      procedure_schema as schema,
      procedure_name as name,
      'PROCEDURE' as object_type,
      argument_signature as signature,
      CASE 
        WHEN procedure_name LIKE '%MCP%' OR procedure_name LIKE '%EXECUTE%' THEN 'mcp'
        WHEN procedure_name LIKE '%SDLC%' THEN 'sdlc'
        ELSE 'utility'
      END as category,
      COALESCE(procedure_comment, 'No description available') as description,
      'CALL ' || procedure_schema || '.' || procedure_name || '(...)' as example_call,
      procedure_comment as comment_text,
      'active' as status
    FROM INFORMATION_SCHEMA.PROCEDURES
    WHERE procedure_schema = ?
      AND is_builtin = 'N'
  `;
  
  const procStmt = SF.createStatement({
    sqlText: procSQL,
    binds: [schemaFilter]
  });
  
  const procRS = procStmt.execute();
  const procedures = [];
  
  while (procRS.next()) {
    const proc = {
      object_type: procRS.getColumnValue('OBJECT_TYPE'),
      object_name: procRS.getColumnValue('NAME'),
      db: procRS.getColumnValue('DB'),
      schema: procRS.getColumnValue('SCHEMA'),
      definition: {
        signature: procRS.getColumnValue('SIGNATURE'),
        category: procRS.getColumnValue('CATEGORY'),
        description: procRS.getColumnValue('DESCRIPTION'),
        example_call: procRS.getColumnValue('EXAMPLE_CALL')
      },
      status: 'active',
      comment: procRS.getColumnValue('COMMENT_TEXT')
    };
    
    // Optionally include DDL
    if (includeFullDDL) {
      try {
        const ddlSQL = `SELECT GET_DDL('PROCEDURE', ?) as ddl`;
        const ddlStmt = SF.createStatement({
          sqlText: ddlSQL,
          binds: [procRS.getColumnValue('SCHEMA') + '.' + procRS.getColumnValue('NAME')]
        });
        const ddlRS = ddlStmt.execute();
        if (ddlRS.next()) {
          proc.ddl = ddlRS.getColumnValue('DDL');
        }
      } catch (e) {
        // DDL might fail due to permissions, continue without it
        proc.ddl = 'DDL unavailable: ' + e.toString();
      }
    }
    
    procedures.push(proc);
  }
  
  // Get current views
  const viewSQL = `
    SELECT
      table_catalog as db,
      table_schema as schema,
      table_name as name,
      'VIEW' as object_type,
      comment as comment_text,
      'active' as status
    FROM INFORMATION_SCHEMA.VIEWS
    WHERE table_schema = ?
  `;
  
  const viewStmt = SF.createStatement({
    sqlText: viewSQL,
    binds: [schemaFilter]
  });
  
  const viewRS = viewStmt.execute();
  const views = [];
  
  while (viewRS.next()) {
    const view = {
      object_type: viewRS.getColumnValue('OBJECT_TYPE'),
      object_name: viewRS.getColumnValue('NAME'),
      db: viewRS.getColumnValue('DB'),
      schema: viewRS.getColumnValue('SCHEMA'),
      definition: {
        category: 'view',
        description: viewRS.getColumnValue('COMMENT_TEXT') || 'Data view'
      },
      status: 'active',
      comment: viewRS.getColumnValue('COMMENT_TEXT')
    };
    
    views.push(view);
  }
  
  // Combine all objects
  const allObjects = procedures.concat(views);
  
  if (allObjects.length === 0) {
    return {
      result: 'warning',
      message: 'No objects found in schema: ' + schemaFilter
    };
  }
  
  // Create schema published event
  const eventPayload = {
    action: 'mcp.schema.published',
    actor_id: 'system_schema_publisher',
    attributes: {
      version: new Date().toISOString(),
      source: 'automated_snapshot',
      schema_filter: schemaFilter,
      include_ddl: includeFullDDL,
      object_count: allObjects.length,
      objects: allObjects
    },
    schema_version: '2.1.0'
  };
  
  // Write the event
  const writerSQL = `CALL CLAUDE_BI.MCP.SDLC_UPSERT_EVENT_IDEMPOTENT(?)`;
  const writerStmt = SF.createStatement({
    sqlText: writerSQL,
    binds: [eventPayload]
  });
  
  const writerRS = writerStmt.execute();
  writerRS.next();
  const result = writerRS.getColumnValue(1);
  
  return {
    result: 'ok',
    schema_version: eventPayload.attributes.version,
    objects_published: allObjects.length,
    procedures: procedures.length,
    views: views.length,
    event_result: result
  };
  
} catch (err) {
  return {
    result: 'error',
    error: err.toString()
  };
}
$$;

-- @statement
-- ============================================================================
-- UPDATE_SCHEMA_OBJECT: Publish individual object updates
-- ============================================================================
CREATE OR REPLACE PROCEDURE UPDATE_SCHEMA_OBJECT(
  object_name STRING,
  object_type STRING DEFAULT 'PROCEDURE',
  schema_name STRING DEFAULT 'MCP'
)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS $$
const SF = snowflake;

try {
  const objName = OBJECT_NAME;
  const objType = OBJECT_TYPE || 'PROCEDURE';
  const schemaName = SCHEMA_NAME || 'MCP';
  
  let objectDef = {};
  
  if (objType.toUpperCase() === 'PROCEDURE') {
    // Get procedure metadata
    const procSQL = `
      SELECT 
        procedure_catalog as db,
        procedure_schema as schema,
        procedure_name as name,
        argument_signature as signature,
        procedure_comment as comment_text,
        CASE 
          WHEN procedure_name LIKE '%MCP%' OR procedure_name LIKE '%EXECUTE%' THEN 'mcp'
          WHEN procedure_name LIKE '%SDLC%' THEN 'sdlc'
          ELSE 'utility'
        END as category
      FROM INFORMATION_SCHEMA.PROCEDURES
      WHERE procedure_schema = ?
        AND procedure_name = ?
        AND is_builtin = 'N'
    `;
    
    const procStmt = SF.createStatement({
      sqlText: procSQL,
      binds: [schemaName, objName]
    });
    
    const procRS = procStmt.execute();
    
    if (!procRS.next()) {
      return {
        result: 'error',
        error: 'Procedure not found: ' + schemaName + '.' + objName
      };
    }
    
    objectDef = {
      object_type: 'PROCEDURE',
      object_name: procRS.getColumnValue('NAME'),
      db: procRS.getColumnValue('DB'),
      schema: procRS.getColumnValue('SCHEMA'),
      definition: {
        signature: procRS.getColumnValue('SIGNATURE'),
        category: procRS.getColumnValue('CATEGORY'),
        description: procRS.getColumnValue('COMMENT_TEXT') || 'No description available',
        example_call: 'CALL ' + procRS.getColumnValue('SCHEMA') + '.' + procRS.getColumnValue('NAME') + '(...)'
      },
      status: 'active',
      comment: procRS.getColumnValue('COMMENT_TEXT')
    };
    
  } else if (objType.toUpperCase() === 'VIEW') {
    // Get view metadata
    const viewSQL = `
      SELECT
        table_catalog as db,
        table_schema as schema,
        table_name as name,
        comment as comment_text
      FROM INFORMATION_SCHEMA.VIEWS
      WHERE table_schema = ?
        AND table_name = ?
    `;
    
    const viewStmt = SF.createStatement({
      sqlText: viewSQL,
      binds: [schemaName, objName]
    });
    
    const viewRS = viewStmt.execute();
    
    if (!viewRS.next()) {
      return {
        result: 'error',
        error: 'View not found: ' + schemaName + '.' + objName
      };
    }
    
    objectDef = {
      object_type: 'VIEW',
      object_name: viewRS.getColumnValue('NAME'),
      db: viewRS.getColumnValue('DB'),
      schema: viewRS.getColumnValue('SCHEMA'),
      definition: {
        category: 'view',
        description: viewRS.getColumnValue('COMMENT_TEXT') || 'Data view'
      },
      status: 'active',
      comment: viewRS.getColumnValue('COMMENT_TEXT')
    };
  } else {
    return {
      result: 'error',
      error: 'Unsupported object type: ' + objType
    };
  }
  
  // Create schema update event
  const eventPayload = {
    action: 'mcp.schema.updated',
    actor_id: 'system_schema_updater',
    attributes: {
      version: new Date().toISOString(),
      source: 'individual_update',
      objects: [objectDef]
    },
    schema_version: '2.1.0'
  };
  
  // Write the event
  const writerSQL = `CALL CLAUDE_BI.MCP.SDLC_UPSERT_EVENT_IDEMPOTENT(?)`;
  const writerStmt = SF.createStatement({
    sqlText: writerSQL,
    binds: [eventPayload]
  });
  
  const writerRS = writerStmt.execute();
  writerRS.next();
  const result = writerRS.getColumnValue(1);
  
  return {
    result: 'ok',
    object_name: objName,
    object_type: objType,
    schema_version: eventPayload.attributes.version,
    event_result: result
  };
  
} catch (err) {
  return {
    result: 'error',
    error: err.toString()
  };
}
$$;

-- @statement
-- ============================================================================
-- Grants for Claude agents
-- ============================================================================
GRANT SELECT ON VIEW VW_LATEST_SCHEMA TO ROLE CLAUDE_AGENT_ROLE;
GRANT SELECT ON VIEW VW_IS_PROCEDURES TO ROLE CLAUDE_AGENT_ROLE;
GRANT SELECT ON VIEW VW_IS_OBJECTS TO ROLE CLAUDE_AGENT_ROLE;
GRANT SELECT ON VIEW VW_IS_COLUMNS TO ROLE CLAUDE_AGENT_ROLE;

GRANT EXECUTE ON PROCEDURE PUBLISH_SCHEMA_SNAPSHOT(BOOLEAN, STRING) TO ROLE CLAUDE_AGENT_ROLE;
GRANT EXECUTE ON PROCEDURE UPDATE_SCHEMA_OBJECT(STRING, STRING, STRING) TO ROLE CLAUDE_AGENT_ROLE;

-- @statement
-- ============================================================================
-- Comments for documentation
-- ============================================================================
ALTER VIEW VW_LATEST_SCHEMA SET COMMENT = 'Latest schema state from events - canonical truth for agent discovery';
ALTER VIEW VW_IS_PROCEDURES SET COMMENT = 'Agent-friendly procedure catalog with MCP compatibility flags';
ALTER VIEW VW_IS_OBJECTS SET COMMENT = 'Tables and views catalog for agent object discovery';
ALTER VIEW VW_IS_COLUMNS SET COMMENT = 'Column metadata derived from schema events';

ALTER PROCEDURE PUBLISH_SCHEMA_SNAPSHOT(BOOLEAN, STRING) SET COMMENT = 'Capture current schema state and publish as mcp.schema.published event';
ALTER PROCEDURE UPDATE_SCHEMA_OBJECT(STRING, STRING, STRING) SET COMMENT = 'Publish individual object updates as mcp.schema.updated events';