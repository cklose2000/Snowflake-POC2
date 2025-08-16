-- Deploy SDLC Helper Procedure
USE ROLE ACCOUNTADMIN;
USE DATABASE CLAUDE_BI;
USE SCHEMA MCP;

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
    return {
      result: 'idempotent_return',
      existing_event_id: checkRS.getColumnValue('EVENT_ID'),
      existing_action: checkRS.getColumnValue('ACTION'),
      existing_occurred_at: checkRS.getColumnValue('OCCURRED_AT')
    };
  }
  
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