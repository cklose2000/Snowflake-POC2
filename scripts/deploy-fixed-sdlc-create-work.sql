-- Deploy Fixed SDLC_CREATE_WORK Procedure
-- This script deploys the corrected procedure with proper INSERT syntax

USE ROLE ACCOUNTADMIN;
USE DATABASE CLAUDE_BI;
USE SCHEMA MCP;

-- Drop any existing procedure first
DROP PROCEDURE IF EXISTS SDLC_CREATE_WORK(STRING, STRING, STRING, STRING, STRING, NUMBER, BOOLEAN, STRING);

-- Create the fixed procedure
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
    'create_' + TITLE.substring(0, 20) + '_' + REPORTER_ID + '_' + new Date().toISOString().split('T')[0];
  
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
  const seqSQL = `SELECT CLAUDE_BI.MCP.SDLC_TICKET_SEQ.NEXTVAL as ticket_num`;
  
  const seqStmt = SF.createStatement({ sqlText: seqSQL });
  const seqRS = seqStmt.execute();
  seqRS.next();
  
  const displayId = 'WORK-' + String(seqRS.getColumnValue('TICKET_NUM')).padStart(5, '0');
  
  // Create the work item payload
  const createPayload = {
    event_id: workId + '_CREATE',
    action: 'sdlc.work.create',
    occurred_at: new Date().toISOString(),
    actor_id: REPORTER_ID,
    source: 'sdlc',
    schema_version: '2.1.0',
    object: {
      type: 'work_item',
      id: workId
    },
    attributes: {
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
      idempotency_key: idemKey,
      tenant_id: 'default',
      schema_version: '1.0.0'
    }
  };
  
  // FIXED: Proper INSERT with explicit columns and PARSE_JSON
  const insertSQL = `
    INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS (PAYLOAD, _SOURCE_LANE, _RECV_AT)
    VALUES (PARSE_JSON(?), ?, CURRENT_TIMESTAMP())
  `;
  
  const insertStmt = SF.createStatement({
    sqlText: insertSQL,
    binds: [JSON.stringify(createPayload), 'SDLC']
  });
  insertStmt.execute();
  
  // Get the event ID for optimistic concurrency
  const eventSQL = `
    SELECT event_id
    FROM CLAUDE_BI.ACTIVITY.EVENTS
    WHERE attributes:work_id::string = ?
    ORDER BY occurred_at DESC, event_id DESC
    LIMIT 1
  `;
  
  // Retry logic for Dynamic Table lag
  let eventId = null;
  for (let i = 0; i < 3; i++) {
    const eventStmt = SF.createStatement({
      sqlText: eventSQL,
      binds: [workId]
    });
    const eventRS = eventStmt.execute();
    
    if (eventRS.next()) {
      eventId = eventRS.getColumnValue('EVENT_ID');
      break;
    }
    
    // Brief wait for Dynamic Table processing (if i < 2)
    if (i < 2) {
      const sleepStmt = SF.createStatement({
        sqlText: "CALL SYSTEM$WAIT(1, 'SECONDS')"
      });
      sleepStmt.execute();
    }
  }
  
  return {
    result: 'ok',
    work_id: workId,
    display_id: displayId,
    last_event_id: eventId || (workId + '_CREATE'),
    status: 'new',
    title: TITLE,
    sequence_number: seqRS.getColumnValue('TICKET_NUM')
  };
  
} catch (err) {
  return {
    result: 'error',
    error: err.toString(),
    error_type: 'procedure_execution',
    procedure: 'SDLC_CREATE_WORK'
  };
}
$$;

-- Grant permissions
GRANT USAGE ON PROCEDURE SDLC_CREATE_WORK(STRING, STRING, STRING, STRING, STRING, NUMBER, BOOLEAN, STRING) TO ROLE MCP_USER_ROLE;
GRANT USAGE ON PROCEDURE SDLC_CREATE_WORK(STRING, STRING, STRING, STRING, STRING, NUMBER, BOOLEAN, STRING) TO ROLE MCP_AGENT_ROLE;

-- Test that the procedure was created successfully
SELECT 'Fixed SDLC_CREATE_WORK procedure deployed successfully' as status;