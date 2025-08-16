-- ============================================================================
-- 03_concurrency_procedures.sql
-- SDLC Procedures with Optimistic Concurrency Control - Two-Table Law Compliant
-- All procedures use expected_last_event_id to prevent conflicts
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE CLAUDE_BI;
USE SCHEMA MCP;

-- ============================================================================
-- Helper: Central idempotent event writer
-- All other procedures call this to ensure consistency
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
-- Create new work item
-- ============================================================================
CREATE OR REPLACE PROCEDURE SDLC_CREATE_WORK(
  work_id STRING,
  title STRING,
  work_type STRING,
  severity STRING,
  description STRING,
  reporter_id STRING,
  idempotency_key STRING,
  business_value NUMBER DEFAULT NULL,
  customer_impact BOOLEAN DEFAULT FALSE
)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
const SF = snowflake;

try {
  // Check if work_id already exists
  const existsSQL = `
    SELECT COUNT(*) as count
    FROM CLAUDE_BI.MCP.VW_WORK_ITEMS
    WHERE work_id = ?
  `;
  
  const existsStmt = SF.createStatement({
    sqlText: existsSQL,
    binds: [WORK_ID]
  });
  const existsRS = existsStmt.execute();
  existsRS.next();
  
  if (existsRS.getColumnValue('COUNT') > 0) {
    return {
      result: 'error',
      error: 'work_id_already_exists',
      work_id: WORK_ID
    };
  }
  
  // Create payload
  const payload = {
    action: 'sdlc.work.create',
    work_id: WORK_ID,
    title: TITLE,
    type: WORK_TYPE,
    severity: SEVERITY, 
    description: DESCRIPTION,
    reporter_id: REPORTER_ID,
    business_value: BUSINESS_VALUE,
    customer_impact: CUSTOMER_IMPACT,
    actor_id: REPORTER_ID,
    idempotency_key: IDEMPOTENCY_KEY,
    tenant_id: 'default',
    schema_version: '1.0.0'
  };
  
  // Call central writer
  const writerSQL = `CALL CLAUDE_BI.MCP.SDLC_UPSERT_EVENT_IDEMPOTENT(?)`;
  const writerStmt = SF.createStatement({
    sqlText: writerSQL,
    binds: [payload]
  });
  const writerRS = writerStmt.execute();
  writerRS.next();
  
  const result = writerRS.getColumnValue(1);
  
  if (result.result === 'ok' || result.result === 'idempotent_return') {
    return {
      result: 'ok',
      work_id: WORK_ID,
      title: TITLE,
      initial_status: 'new'
    };
  } else {
    return result;
  }
  
} catch (err) {
  return {
    result: 'error',
    error: err.toString()
  };
}
$$;

-- ============================================================================
-- Update work status with optimistic concurrency control
-- ============================================================================
CREATE OR REPLACE PROCEDURE SDLC_STATUS(
  work_id STRING,
  new_status STRING,
  expected_last_event_id STRING,
  idempotency_key STRING,
  actor_id STRING,
  status_reason STRING DEFAULT NULL
)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
const SF = snowflake;

try {
  // Get current state
  const currentSQL = `
    SELECT 
      last_event_id,
      status as current_status,
      title
    FROM CLAUDE_BI.MCP.VW_WORK_ITEMS
    WHERE work_id = ?
  `;
  
  const currentStmt = SF.createStatement({
    sqlText: currentSQL,
    binds: [WORK_ID]
  });
  const currentRS = currentStmt.execute();
  
  if (!currentRS.next()) {
    return {
      result: 'error',
      error: 'work_not_found',
      work_id: WORK_ID
    };
  }
  
  const currentLastEventId = currentRS.getColumnValue('LAST_EVENT_ID');
  const currentStatus = currentRS.getColumnValue('CURRENT_STATUS');
  
  // Check optimistic concurrency
  if (currentLastEventId !== EXPECTED_LAST_EVENT_ID) {
    // Emit conflict event for audit trail
    const conflictPayload = {
      action: 'sdlc.agent.error',
      work_id: WORK_ID,
      agent_id: ACTOR_ID,
      error_type: 'conflict',
      error_message: 'Work item was modified by another process',
      expected_last_event_id: EXPECTED_LAST_EVENT_ID,
      actual_last_event_id: currentLastEventId,
      actor_id: ACTOR_ID,
      idempotency_key: IDEMPOTENCY_KEY + '_conflict',
      schema_version: '1.0.0'
    };
    
    const conflictSQL = `CALL CLAUDE_BI.MCP.SDLC_UPSERT_EVENT_IDEMPOTENT(?)`;
    SF.createStatement({
      sqlText: conflictSQL,
      binds: [conflictPayload]
    }).execute();
    
    return {
      result: 'error',
      error: 'conflict',
      expected: EXPECTED_LAST_EVENT_ID,
      actual: currentLastEventId,
      current_status: currentStatus
    };
  }
  
  // Validate status transition
  const validTransitions = {
    'new': ['backlog', 'ready', 'cancelled'],
    'backlog': ['ready', 'cancelled'], 
    'ready': ['in_progress', 'cancelled'],
    'in_progress': ['review', 'done', 'blocked', 'cancelled'],
    'review': ['in_progress', 'done', 'cancelled'],
    'blocked': ['ready', 'in_progress', 'cancelled'],
    'done': ['review'],  // Allow reopening if needed
    'cancelled': ['backlog']  // Allow uncancelling
  };
  
  if (validTransitions[currentStatus] && !validTransitions[currentStatus].includes(NEW_STATUS)) {
    return {
      result: 'error',
      error: 'invalid_transition',
      from_status: currentStatus,
      to_status: NEW_STATUS,
      valid_transitions: validTransitions[currentStatus]
    };
  }
  
  // Create status change event
  const payload = {
    action: 'sdlc.work.status',
    work_id: WORK_ID,
    status: NEW_STATUS,
    from_status: currentStatus,
    status_reason: STATUS_REASON,
    expected_last_event_id: EXPECTED_LAST_EVENT_ID,
    actor_id: ACTOR_ID,
    idempotency_key: IDEMPOTENCY_KEY,
    schema_version: '1.0.0'
  };
  
  const writerSQL = `CALL CLAUDE_BI.MCP.SDLC_UPSERT_EVENT_IDEMPOTENT(?)`;
  const writerStmt = SF.createStatement({
    sqlText: writerSQL,
    binds: [payload]
  });
  const writerRS = writerStmt.execute();
  writerRS.next();
  
  const result = writerRS.getColumnValue(1);
  
  if (result.result === 'ok' || result.result === 'idempotent_return') {
    return {
      result: 'ok',
      work_id: WORK_ID,
      from_status: currentStatus,
      to_status: NEW_STATUS
    };
  } else {
    return result;
  }
  
} catch (err) {
  return {
    result: 'error',
    error: err.toString()
  };
}
$$;

-- ============================================================================
-- Assign work with optimistic concurrency control
-- ============================================================================
CREATE OR REPLACE PROCEDURE SDLC_ASSIGN(
  work_id STRING,
  assignee_id STRING,
  assignee_type STRING,
  expected_last_event_id STRING,
  idempotency_key STRING,
  actor_id STRING,
  assignment_reason STRING DEFAULT NULL
)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
const SF = snowflake;

try {
  // Get current state
  const currentSQL = `
    SELECT 
      last_event_id,
      assignee_id as current_assignee,
      status,
      title
    FROM CLAUDE_BI.MCP.VW_WORK_ITEMS
    WHERE work_id = ?
  `;
  
  const currentStmt = SF.createStatement({
    sqlText: currentSQL,
    binds: [WORK_ID]
  });
  const currentRS = currentStmt.execute();
  
  if (!currentRS.next()) {
    return {
      result: 'error',
      error: 'work_not_found',
      work_id: WORK_ID
    };
  }
  
  const currentLastEventId = currentRS.getColumnValue('LAST_EVENT_ID');
  const currentAssignee = currentRS.getColumnValue('CURRENT_ASSIGNEE');
  const status = currentRS.getColumnValue('STATUS');
  
  // Check optimistic concurrency
  if (currentLastEventId !== EXPECTED_LAST_EVENT_ID) {
    return {
      result: 'error',
      error: 'conflict',
      expected: EXPECTED_LAST_EVENT_ID,
      actual: currentLastEventId
    };
  }
  
  // Check if work is in assignable state
  if (['done', 'cancelled'].includes(status)) {
    return {
      result: 'error',
      error: 'work_not_assignable',
      status: status
    };
  }
  
  // Create assignment event
  const payload = {
    action: 'sdlc.work.assign',
    work_id: WORK_ID,
    assignee_id: ASSIGNEE_ID,
    assignee_type: ASSIGNEE_TYPE,
    assigned_by: ACTOR_ID,
    reason: ASSIGNMENT_REASON,
    previous_assignee: currentAssignee,
    expected_last_event_id: EXPECTED_LAST_EVENT_ID,
    actor_id: ACTOR_ID,
    idempotency_key: IDEMPOTENCY_KEY,
    schema_version: '1.0.0'
  };
  
  const writerSQL = `CALL CLAUDE_BI.MCP.SDLC_UPSERT_EVENT_IDEMPOTENT(?)`;
  const writerStmt = SF.createStatement({
    sqlText: writerSQL,
    binds: [payload]
  });
  const writerRS = writerStmt.execute();
  writerRS.next();
  
  const result = writerRS.getColumnValue(1);
  
  if (result.result === 'ok' || result.result === 'idempotent_return') {
    return {
      result: 'ok',
      work_id: WORK_ID,
      assignee_id: ASSIGNEE_ID,
      previous_assignee: currentAssignee
    };
  } else {
    return result;
  }
  
} catch (err) {
  return {
    result: 'error',
    error: err.toString()
  };
}
$$;

-- ============================================================================
-- Add work dependency with cycle detection
-- ============================================================================
CREATE OR REPLACE PROCEDURE SDLC_ADD_DEPENDENCY(
  work_id STRING,
  depends_on_id STRING,
  dependency_type STRING,
  expected_last_event_id STRING,
  idempotency_key STRING,
  actor_id STRING,
  dependency_reason STRING DEFAULT NULL
)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
const SF = snowflake;

try {
  // Check if both work items exist
  const checkSQL = `
    SELECT work_id, title FROM CLAUDE_BI.MCP.VW_WORK_ITEMS
    WHERE work_id IN (?, ?)
  `;
  
  const checkStmt = SF.createStatement({
    sqlText: checkSQL,
    binds: [WORK_ID, DEPENDS_ON_ID]
  });
  const checkRS = checkStmt.execute();
  
  const foundItems = [];
  while (checkRS.next()) {
    foundItems.push(checkRS.getColumnValue('WORK_ID'));
  }
  
  if (foundItems.length !== 2) {
    return {
      result: 'error',
      error: 'work_items_not_found',
      requested: [WORK_ID, DEPENDS_ON_ID],
      found: foundItems
    };
  }
  
  // Check for cycle using recursive CTE (simplified check)
  const cycleSQL = `
    WITH RECURSIVE dep_chain AS (
      -- Base case: direct dependencies
      SELECT 
        e.attributes:work_id::string as work_id,
        e.attributes:depends_on_id::string as depends_on_id,
        1 as depth
      FROM CLAUDE_BI.ACTIVITY.EVENTS e
      WHERE e.action = 'sdlc.work.depends'
      
      UNION ALL
      
      -- Recursive case: transitive dependencies  
      SELECT 
        dc.work_id,
        e.attributes:depends_on_id::string,
        dc.depth + 1
      FROM dep_chain dc
      JOIN CLAUDE_BI.ACTIVITY.EVENTS e ON e.attributes:work_id::string = dc.depends_on_id
      WHERE e.action = 'sdlc.work.depends'
        AND dc.depth < 10  -- Prevent infinite recursion
    )
    SELECT COUNT(*) as cycle_count
    FROM dep_chain
    WHERE work_id = ? AND depends_on_id = ?
  `;
  
  const cycleStmt = SF.createStatement({
    sqlText: cycleSQL,
    binds: [DEPENDS_ON_ID, WORK_ID]  -- Would creating this dependency create a cycle?
  });
  const cycleRS = cycleStmt.execute();
  cycleRS.next();
  
  if (cycleRS.getColumnValue('CYCLE_COUNT') > 0) {
    return {
      result: 'error',
      error: 'dependency_cycle_detected',
      work_id: WORK_ID,
      depends_on_id: DEPENDS_ON_ID
    };
  }
  
  // Get current state for concurrency check
  const currentSQL = `
    SELECT last_event_id FROM CLAUDE_BI.MCP.VW_WORK_ITEMS WHERE work_id = ?
  `;
  
  const currentStmt = SF.createStatement({
    sqlText: currentSQL,
    binds: [WORK_ID]
  });
  const currentRS = currentStmt.execute();
  currentRS.next();
  
  const currentLastEventId = currentRS.getColumnValue('LAST_EVENT_ID');
  
  if (currentLastEventId !== EXPECTED_LAST_EVENT_ID) {
    return {
      result: 'error',
      error: 'conflict',
      expected: EXPECTED_LAST_EVENT_ID,
      actual: currentLastEventId
    };
  }
  
  // Create dependency event
  const payload = {
    action: 'sdlc.work.depends',
    work_id: WORK_ID,
    depends_on_id: DEPENDS_ON_ID,
    dependency_type: DEPENDENCY_TYPE,
    dependency_reason: DEPENDENCY_REASON,
    expected_last_event_id: EXPECTED_LAST_EVENT_ID,
    actor_id: ACTOR_ID,
    idempotency_key: IDEMPOTENCY_KEY,
    schema_version: '1.0.0'
  };
  
  const writerSQL = `CALL CLAUDE_BI.MCP.SDLC_UPSERT_EVENT_IDEMPOTENT(?)`;
  const writerStmt = SF.createStatement({
    sqlText: writerSQL,
    binds: [payload]
  });
  const writerRS = writerStmt.execute();
  writerRS.next();
  
  const result = writerRS.getColumnValue(1);
  
  if (result.result === 'ok' || result.result === 'idempotent_return') {
    return {
      result: 'ok',
      work_id: WORK_ID,
      depends_on_id: DEPENDS_ON_ID,
      dependency_type: DEPENDENCY_TYPE
    };
  } else {
    return result;
  }
  
} catch (err) {
  return {
    result: 'error',
    error: err.toString()
  };
}
$$;

-- ============================================================================
-- Estimate or re-estimate work
-- ============================================================================
CREATE OR REPLACE PROCEDURE SDLC_ESTIMATE(
  work_id STRING,
  points NUMBER,
  expected_last_event_id STRING,
  idempotency_key STRING,
  actor_id STRING,
  estimation_reason STRING DEFAULT NULL
)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
const SF = snowflake;

try {
  // Get current state
  const currentSQL = `
    SELECT 
      last_event_id,
      points as current_points,
      status
    FROM CLAUDE_BI.MCP.VW_WORK_ITEMS
    WHERE work_id = ?
  `;
  
  const currentStmt = SF.createStatement({
    sqlText: currentSQL,
    binds: [WORK_ID]
  });
  const currentRS = currentStmt.execute();
  
  if (!currentRS.next()) {
    return {
      result: 'error',
      error: 'work_not_found',
      work_id: WORK_ID
    };
  }
  
  const currentLastEventId = currentRS.getColumnValue('LAST_EVENT_ID');
  const currentPoints = currentRS.getColumnValue('CURRENT_POINTS');
  const status = currentRS.getColumnValue('STATUS');
  
  // Check optimistic concurrency
  if (currentLastEventId !== EXPECTED_LAST_EVENT_ID) {
    return {
      result: 'error',
      error: 'conflict',
      expected: EXPECTED_LAST_EVENT_ID,
      actual: currentLastEventId
    };
  }
  
  // Determine if this is initial estimate or re-estimate
  const actionType = currentPoints ? 'sdlc.work.reestimate' : 'sdlc.work.estimate';
  
  // Create estimation event
  const payload = {
    action: actionType,
    work_id: WORK_ID,
    points: POINTS,
    previous_points: currentPoints,
    estimator: ACTOR_ID,
    estimation_reason: ESTIMATION_REASON,
    expected_last_event_id: EXPECTED_LAST_EVENT_ID,
    actor_id: ACTOR_ID,
    idempotency_key: IDEMPOTENCY_KEY,
    schema_version: '1.0.0'
  };
  
  const writerSQL = `CALL CLAUDE_BI.MCP.SDLC_UPSERT_EVENT_IDEMPOTENT(?)`;
  const writerStmt = SF.createStatement({
    sqlText: writerSQL,
    binds: [payload]
  });
  const writerRS = writerStmt.execute();
  writerRS.next();
  
  const result = writerRS.getColumnValue(1);
  
  if (result.result === 'ok' || result.result === 'idempotent_return') {
    return {
      result: 'ok',
      work_id: WORK_ID,
      points: POINTS,
      previous_points: currentPoints,
      action_type: actionType
    };
  } else {
    return result;
  }
  
} catch (err) {
  return {
    result: 'error',
    error: err.toString()
  };
}
$$;

-- ============================================================================
-- Grant procedure permissions
-- ============================================================================
GRANT USAGE ON PROCEDURE SDLC_UPSERT_EVENT_IDEMPOTENT(VARIANT) TO ROLE MCP_USER_ROLE;
GRANT USAGE ON PROCEDURE SDLC_CREATE_WORK(STRING, STRING, STRING, STRING, STRING, STRING, STRING, NUMBER, BOOLEAN) TO ROLE MCP_USER_ROLE;
GRANT USAGE ON PROCEDURE SDLC_STATUS(STRING, STRING, STRING, STRING, STRING, STRING) TO ROLE MCP_USER_ROLE;
GRANT USAGE ON PROCEDURE SDLC_ASSIGN(STRING, STRING, STRING, STRING, STRING, STRING, STRING) TO ROLE MCP_USER_ROLE;
GRANT USAGE ON PROCEDURE SDLC_ADD_DEPENDENCY(STRING, STRING, STRING, STRING, STRING, STRING, STRING) TO ROLE MCP_USER_ROLE;
GRANT USAGE ON PROCEDURE SDLC_ESTIMATE(STRING, NUMBER, STRING, STRING, STRING, STRING) TO ROLE MCP_USER_ROLE;

-- ============================================================================
-- END OF CONCURRENCY PROCEDURES
-- 
-- Next: 04_agent_integration.sql - Agent-specific procedures (SDLC_CLAIM_NEXT)
-- ============================================================================