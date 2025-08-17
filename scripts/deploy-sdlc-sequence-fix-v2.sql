-- Deploy SDLC Sequence Fix V2
-- This script properly deploys the sequence-based SDLC_CREATE_WORK procedure

USE ROLE ACCOUNTADMIN;
USE DATABASE CLAUDE_BI;
USE SCHEMA MCP;

-- Ensure sequence exists
CREATE SEQUENCE IF NOT EXISTS SDLC_TICKET_SEQ 
  START 1 
  INCREMENT 1 
  COMMENT = 'Sequential ticket numbering for SDLC work items';

-- Drop any existing procedure
DROP PROCEDURE IF EXISTS SDLC_CREATE_WORK(STRING, STRING, STRING, STRING, STRING, NUMBER, BOOLEAN, STRING);
DROP PROCEDURE IF EXISTS SDLC_CREATE_WORK(VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, FLOAT, BOOLEAN, VARCHAR);

-- Create the sequence-based procedure
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

-- Grant permissions
GRANT USAGE ON SEQUENCE SDLC_TICKET_SEQ TO ROLE CLAUDE_AGENT_ROLE;
GRANT EXECUTE ON PROCEDURE SDLC_CREATE_WORK(STRING, STRING, STRING, STRING, STRING, NUMBER, BOOLEAN, STRING) TO ROLE CLAUDE_AGENT_ROLE;

-- Show what we deployed
SELECT 'Procedure deployed with sequence-based numbering' as status;