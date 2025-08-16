-- ============================================================================
-- 03_concurrency_procedures_v3_sequential.sql
-- SDLC Procedures with Sequential Ticket Numbering using Snowflake Sequence
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE CLAUDE_BI;
USE SCHEMA MCP;

-- ============================================================================
-- Create sequence for ticket numbering if not exists
-- ============================================================================
CREATE SEQUENCE IF NOT EXISTS SDLC_TICKET_SEQ 
  START 1 
  INCREMENT 1 
  COMMENT = 'Sequential ticket numbering for SDLC work items';

-- ============================================================================
-- Helper: Central idempotent event writer (unchanged)
-- ============================================================================
CREATE OR REPLACE PROCEDURE SDLC_UPSERT_EVENT_IDEMPOTENT(payload_json VARIANT)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
const SF = snowflake;

try {
  const payload = PAYLOAD_JSON;
  const idempotencyKey = payload.idempotency_key;
  
  if (!idempotencyKey) {
    throw new Error('idempotency_key is required in payload');
  }
  
  // Check if event with this idempotency key already exists
  const checkSQL = `
    SELECT event_id, action, occurred_at
    FROM CLAUDE_BI.ACTIVITY.EVENTS
    WHERE attributes:idempotency_key::string = ?
    LIMIT 1
  `;
  
  const checkStmt = SF.createStatement({
    sqlText: checkSQL,
    binds: [idempotencyKey]
  });
  const checkRS = checkStmt.execute();
  
  if (checkRS.next()) {
    // Already exists - return existing event info
    return {
      result: 'idempotent_return',
      existing_event_id: checkRS.getColumnValue('EVENT_ID'),
      existing_action: checkRS.getColumnValue('ACTION'),
      existing_occurred_at: checkRS.getColumnValue('OCCURRED_AT')
    };
  }
  
  // Insert new event
  const insertSQL = `
    INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS
    SELECT 
      OBJECT_CONSTRUCT(
        'event_id', UUID_STRING(),
        'action', ?,
        'occurred_at', CURRENT_TIMESTAMP(),
        'actor_id', ?,
        'source', 'sdlc',
        'schema_version', '2.1.0',
        'object', OBJECT_CONSTRUCT(
          'type', 'work_item',
          'id', ?
        ),
        'attributes', ?
      ),
      'SDLC',
      CURRENT_TIMESTAMP()
  `;
  
  const insertStmt = SF.createStatement({
    sqlText: insertSQL,
    binds: [
      payload.action,
      payload.actor_id,
      payload.work_id,
      payload
    ]
  });
  insertStmt.execute();
  
  return {
    result: 'ok',
    action: payload.action,
    work_id: payload.work_id,
    idempotency_key: idempotencyKey
  };
  
} catch (err) {
  return {
    result: 'error',
    error: err.toString()
  };
}
$$;

-- ============================================================================
-- UPDATED: Create new work item with SEQUENCE-based numbering
-- ============================================================================
CREATE OR REPLACE PROCEDURE SDLC_CREATE_WORK(
  title STRING,
  work_type STRING,
  severity STRING,
  description STRING,
  reporter_id STRING,
  business_value NUMBER DEFAULT 5,
  customer_impact BOOLEAN DEFAULT FALSE,
  idempotency_key STRING DEFAULT NULL
)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
const SF = snowflake;

try {
  // Generate deterministic idempotency key if not provided
  const idemKey = IDEMPOTENCY_KEY || 
    'create_' + HASH(TITLE + ':' + REPORTER_ID + ':' + new Date().toISOString().split('T')[0]);
  
  // Check if this exact request was already processed
  const existingSQL = `
    SELECT 
      attributes:work_id::string as work_id,
      attributes:display_id::string as display_id,
      event_id
    FROM CLAUDE_BI.ACTIVITY.EVENTS
    WHERE attributes:idempotency_key::string = ?
      AND action = 'sdlc.work.create'
    LIMIT 1
  `;
  
  const existingStmt = SF.createStatement({
    sqlText: existingSQL,
    binds: [idemKey]
  });
  const existingRS = existingStmt.execute();
  
  if (existingRS.next()) {
    // Return existing work item
    return {
      result: 'idempotent_return',
      work_id: existingRS.getColumnValue('WORK_ID'),
      display_id: existingRS.getColumnValue('DISPLAY_ID'),
      last_event_id: existingRS.getColumnValue('EVENT_ID'),
      status: 'new'
    };
  }
  
  // Generate internal work ID (ULID-style for sorting)
  const workId = 'WORK_' + Date.now() + '_' + Math.random().toString(36).substring(2, 9).toUpperCase();
  
  // Generate human-readable display ID using SEQUENCE
  // This guarantees unique, sequential numbering even with concurrent requests
  const seqSQL = `SELECT CLAUDE_BI.MCP.SDLC_TICKET_SEQ.NEXTVAL as ticket_num`;
  
  const seqStmt = SF.createStatement({ sqlText: seqSQL });
  const seqRS = seqStmt.execute();
  seqRS.next();
  
  const displayId = 'WORK-' + String(seqRS.getColumnValue('TICKET_NUM')).padStart(5, '0');
  
  // Create the work item event
  const createPayload = {
    action: 'sdlc.work.create',
    work_id: workId,
    display_id: displayId,
    title: TITLE,
    type: WORK_TYPE,
    severity: SEVERITY,
    description: DESCRIPTION,
    reporter_id: REPORTER_ID,
    business_value: BUSINESS_VALUE,
    customer_impact: CUSTOMER_IMPACT,
    status: 'new',
    actor_id: REPORTER_ID,
    idempotency_key: idemKey,
    tenant_id: 'default',
    schema_version: '1.0.0'
  };
  
  // Write create event
  const writerSQL = `CALL CLAUDE_BI.MCP.SDLC_UPSERT_EVENT_IDEMPOTENT(?)`;
  const writerStmt = SF.createStatement({
    sqlText: writerSQL,
    binds: [createPayload]
  });
  const writerRS = writerStmt.execute();
  writerRS.next();
  
  const createResult = writerRS.getColumnValue(1);
  
  if (createResult.result !== 'ok' && createResult.result !== 'idempotent_return') {
    return createResult;
  }
  
  // Emit display number event for permanent record
  const numberPayload = {
    action: 'sdlc.work.number',
    work_id: workId,
    display_id: displayId,
    actor_id: 'system',
    idempotency_key: 'number_' + workId,
    schema_version: '1.0.0'
  };
  
  SF.createStatement({
    sqlText: writerSQL,
    binds: [numberPayload]
  }).execute();
  
  // Get the event ID for optimistic concurrency
  const eventSQL = `
    SELECT event_id
    FROM CLAUDE_BI.ACTIVITY.EVENTS
    WHERE attributes:work_id::string = ?
    ORDER BY occurred_at DESC, event_id DESC
    LIMIT 1
  `;
  
  const eventStmt = SF.createStatement({
    sqlText: eventSQL,
    binds: [workId]
  });
  const eventRS = eventStmt.execute();
  eventRS.next();
  
  return {
    result: 'ok',
    work_id: workId,
    display_id: displayId,
    last_event_id: eventRS.getColumnValue('EVENT_ID'),
    status: 'new',
    title: TITLE
  };
  
} catch (err) {
  return {
    result: 'error',
    error: err.toString()
  };
}
$$;

-- ============================================================================
-- Other procedures remain unchanged - copy from v2
-- (SDLC_UPDATE_WORK, SDLC_ADD_COMMENT, SDLC_LINK_WORK, etc.)
-- ============================================================================

GRANT USAGE ON SEQUENCE SDLC_TICKET_SEQ TO ROLE CLAUDE_AGENT_ROLE;
GRANT EXECUTE ON PROCEDURE SDLC_CREATE_WORK(STRING, STRING, STRING, STRING, STRING, NUMBER, BOOLEAN, STRING) TO ROLE CLAUDE_AGENT_ROLE;
GRANT EXECUTE ON PROCEDURE SDLC_UPSERT_EVENT_IDEMPOTENT(VARIANT) TO ROLE CLAUDE_AGENT_ROLE;