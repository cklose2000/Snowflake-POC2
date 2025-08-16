-- ============================================================================
-- 03_concurrency_procedures_v2.sql
-- HARDENED SDLC Procedures with Full Concurrency Control - Two-Table Law Compliant
-- All procedures use expected_last_event_id and generate IDs internally
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE CLAUDE_BI;
USE SCHEMA MCP;

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
-- HARDENED: Create new work item with internal ID generation
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
  
  // Generate human-readable display ID
  // Count existing work items to determine sequence number
  const countSQL = `
    SELECT COUNT(DISTINCT attributes:work_id::string) + 1 as next_num
    FROM CLAUDE_BI.ACTIVITY.EVENTS
    WHERE action = 'sdlc.work.create'
  `;
  
  const countStmt = SF.createStatement({ sqlText: countSQL });
  const countRS = countStmt.execute();
  countRS.next();
  
  const displayId = 'WORK-' + String(countRS.getColumnValue('NEXT_NUM')).padStart(5, '0');
  
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
-- HARDENED: Update work status with mandatory optimistic concurrency
-- ============================================================================
CREATE OR REPLACE PROCEDURE SDLC_STATUS(
  work_id STRING,
  new_status STRING,
  expected_last_event_id STRING,  -- Now mandatory
  actor_id STRING,
  status_reason STRING DEFAULT NULL,
  idempotency_key STRING DEFAULT NULL
)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
const SF = snowflake;

try {
  if (!EXPECTED_LAST_EVENT_ID) {
    return {
      result: 'error',
      error: 'expected_last_event_id is required for concurrency control'
    };
  }
  
  const idemKey = IDEMPOTENCY_KEY || 'status_' + WORK_ID + '_' + NEW_STATUS + '_' + Date.now();
  
  // Get current state from consistency view
  const currentSQL = `
    SELECT 
      last_event_id,
      status as current_status,
      title,
      display_id
    FROM CLAUDE_BI.MCP.VW_WORK_ITEMS_CONSISTENCY
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
  const displayId = currentRS.getColumnValue('DISPLAY_ID');
  
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
      idempotency_key: idemKey + '_conflict',
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
      current_status: currentStatus,
      work_id: WORK_ID,
      display_id: displayId
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
      valid_transitions: validTransitions[currentStatus],
      work_id: WORK_ID,
      display_id: displayId
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
    idempotency_key: idemKey,
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
    // Get new last event ID
    const newEventSQL = `
      SELECT event_id
      FROM CLAUDE_BI.ACTIVITY.EVENTS
      WHERE attributes:work_id::string = ?
      ORDER BY occurred_at DESC, event_id DESC
      LIMIT 1
    `;
    
    const newEventStmt = SF.createStatement({
      sqlText: newEventSQL,
      binds: [WORK_ID]
    });
    const newEventRS = newEventStmt.execute();
    newEventRS.next();
    
    return {
      result: 'ok',
      work_id: WORK_ID,
      display_id: displayId,
      from_status: currentStatus,
      to_status: NEW_STATUS,
      last_event_id: newEventRS.getColumnValue('EVENT_ID'),
      status: NEW_STATUS
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
-- HARDENED: Assign work with supersession rules
-- ============================================================================
CREATE OR REPLACE PROCEDURE SDLC_ASSIGN(
  work_id STRING,
  assignee_id STRING,
  assignee_type STRING,
  expected_last_event_id STRING,  -- Mandatory
  actor_id STRING,
  assignment_reason STRING DEFAULT NULL,
  idempotency_key STRING DEFAULT NULL
)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
const SF = snowflake;

try {
  if (!EXPECTED_LAST_EVENT_ID) {
    return {
      result: 'error',
      error: 'expected_last_event_id is required for concurrency control'
    };
  }
  
  const idemKey = IDEMPOTENCY_KEY || 'assign_' + WORK_ID + '_' + ASSIGNEE_ID + '_' + Date.now();
  
  // Get current state from consistency view
  const currentSQL = `
    SELECT 
      last_event_id,
      assignee_id as current_assignee,
      assignee_type as current_assignee_type,
      status,
      title,
      display_id
    FROM CLAUDE_BI.MCP.VW_WORK_ITEMS_CONSISTENCY
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
  const currentAssigneeType = currentRS.getColumnValue('CURRENT_ASSIGNEE_TYPE');
  const status = currentRS.getColumnValue('STATUS');
  const displayId = currentRS.getColumnValue('DISPLAY_ID');
  
  // Check optimistic concurrency
  if (currentLastEventId !== EXPECTED_LAST_EVENT_ID) {
    return {
      result: 'error',
      error: 'conflict',
      expected: EXPECTED_LAST_EVENT_ID,
      actual: currentLastEventId,
      work_id: WORK_ID,
      display_id: displayId
    };
  }
  
  // Check if work is in assignable state
  if (['done', 'cancelled'].includes(status)) {
    return {
      result: 'error',
      error: 'work_not_assignable',
      status: status,
      work_id: WORK_ID,
      display_id: displayId
    };
  }
  
  // Check for supersession (human assignment overrides agent claim)
  if (currentAssignee && currentAssigneeType === 'ai_agent' && ASSIGNEE_TYPE === 'human') {
    // Emit supersession event
    const supersessionPayload = {
      action: 'sdlc.agent.superseded',
      work_id: WORK_ID,
      previous_assignee: currentAssignee,
      new_assignee: ASSIGNEE_ID,
      reason: 'Human assignment overrides agent claim',
      actor_id: ACTOR_ID,
      idempotency_key: idemKey + '_supersede',
      schema_version: '1.0.0'
    };
    
    const supersessionSQL = `CALL CLAUDE_BI.MCP.SDLC_UPSERT_EVENT_IDEMPOTENT(?)`;
    SF.createStatement({
      sqlText: supersessionSQL,
      binds: [supersessionPayload]
    }).execute();
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
    idempotency_key: idemKey,
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
    // Get new last event ID
    const newEventSQL = `
      SELECT event_id
      FROM CLAUDE_BI.ACTIVITY.EVENTS
      WHERE attributes:work_id::string = ?
      ORDER BY occurred_at DESC, event_id DESC
      LIMIT 1
    `;
    
    const newEventStmt = SF.createStatement({
      sqlText: newEventSQL,
      binds: [WORK_ID]
    });
    const newEventRS = newEventStmt.execute();
    newEventRS.next();
    
    return {
      result: 'ok',
      work_id: WORK_ID,
      display_id: displayId,
      assignee_id: ASSIGNEE_ID,
      previous_assignee: currentAssignee,
      last_event_id: newEventRS.getColumnValue('EVENT_ID'),
      status: status
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
-- HARDENED: Complete work (unified semantics)
-- ============================================================================
CREATE OR REPLACE PROCEDURE SDLC_COMPLETE_WORK(
  work_id STRING,
  expected_last_event_id STRING,  -- Mandatory
  actor_id STRING,
  completion_notes STRING DEFAULT NULL,
  deliverables ARRAY DEFAULT NULL,
  tests_passing BOOLEAN DEFAULT TRUE,
  idempotency_key STRING DEFAULT NULL
)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
const SF = snowflake;

try {
  if (!EXPECTED_LAST_EVENT_ID) {
    return {
      result: 'error',
      error: 'expected_last_event_id is required for concurrency control'
    };
  }
  
  const idemKey = IDEMPOTENCY_KEY || 'complete_' + WORK_ID + '_' + Date.now();
  
  // Get current work state from consistency view
  const currentSQL = `
    SELECT 
      last_event_id,
      status,
      assignee_id,
      title,
      display_id,
      created_at
    FROM CLAUDE_BI.MCP.VW_WORK_ITEMS_CONSISTENCY
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
  const status = currentRS.getColumnValue('STATUS');
  const assigneeId = currentRS.getColumnValue('ASSIGNEE_ID');
  const displayId = currentRS.getColumnValue('DISPLAY_ID');
  const createdAt = currentRS.getColumnValue('CREATED_AT');
  
  // Check optimistic concurrency
  if (currentLastEventId !== EXPECTED_LAST_EVENT_ID) {
    return {
      result: 'error',
      error: 'conflict',
      expected: EXPECTED_LAST_EVENT_ID,
      actual: currentLastEventId,
      work_id: WORK_ID,
      display_id: displayId
    };
  }
  
  // Verify actor is authorized to complete (must be assignee or admin)
  if (assigneeId && assigneeId !== ACTOR_ID && !ACTOR_ID.includes('admin')) {
    return {
      result: 'error',
      error: 'not_authorized',
      assignee_id: assigneeId,
      actor_id: ACTOR_ID,
      work_id: WORK_ID,
      display_id: displayId
    };
  }
  
  // Check if work can be completed
  if (['done', 'cancelled'].includes(status)) {
    return {
      result: 'error',
      error: 'already_completed',
      status: status,
      work_id: WORK_ID,
      display_id: displayId
    };
  }
  
  // Calculate completion time
  const completionTimeMs = new Date().getTime() - new Date(createdAt).getTime();
  
  // Create unified done event (no separate status change)
  const payload = {
    action: 'sdlc.work.done',
    work_id: WORK_ID,
    completion_time_ms: completionTimeMs,
    completion_notes: COMPLETION_NOTES,
    deliverables: DELIVERABLES,
    tests_passing: TESTS_PASSING,
    completed_by: ACTOR_ID,
    expected_last_event_id: EXPECTED_LAST_EVENT_ID,
    actor_id: ACTOR_ID,
    idempotency_key: idemKey,
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
    // Get new last event ID
    const newEventSQL = `
      SELECT event_id
      FROM CLAUDE_BI.ACTIVITY.EVENTS
      WHERE attributes:work_id::string = ?
      ORDER BY occurred_at DESC, event_id DESC
      LIMIT 1
    `;
    
    const newEventStmt = SF.createStatement({
      sqlText: newEventSQL,
      binds: [WORK_ID]
    });
    const newEventRS = newEventStmt.execute();
    newEventRS.next();
    
    return {
      result: 'ok',
      work_id: WORK_ID,
      display_id: displayId,
      completion_time_ms: completionTimeMs,
      completion_time_hours: Math.round(completionTimeMs / 3600000 * 100) / 100,
      tests_passing: TESTS_PASSING,
      last_event_id: newEventRS.getColumnValue('EVENT_ID'),
      status: 'done'
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
-- Other procedures remain mostly the same but with mandatory expected_last_event_id
-- ============================================================================

-- Update ESTIMATE procedure signature
CREATE OR REPLACE PROCEDURE SDLC_ESTIMATE(
  work_id STRING,
  points NUMBER,
  expected_last_event_id STRING,  -- Now mandatory
  actor_id STRING,
  estimation_reason STRING DEFAULT NULL,
  idempotency_key STRING DEFAULT NULL
)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
const SF = snowflake;

try {
  if (!EXPECTED_LAST_EVENT_ID) {
    return {
      result: 'error',
      error: 'expected_last_event_id is required for concurrency control'
    };
  }
  
  const idemKey = IDEMPOTENCY_KEY || 'estimate_' + WORK_ID + '_' + POINTS + '_' + Date.now();
  
  // Get current state from consistency view
  const currentSQL = `
    SELECT 
      last_event_id,
      points as current_points,
      status,
      display_id
    FROM CLAUDE_BI.MCP.VW_WORK_ITEMS_CONSISTENCY
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
  const displayId = currentRS.getColumnValue('DISPLAY_ID');
  
  // Check optimistic concurrency
  if (currentLastEventId !== EXPECTED_LAST_EVENT_ID) {
    return {
      result: 'error',
      error: 'conflict',
      expected: EXPECTED_LAST_EVENT_ID,
      actual: currentLastEventId,
      work_id: WORK_ID,
      display_id: displayId
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
    idempotency_key: idemKey,
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
    // Get new last event ID
    const newEventSQL = `
      SELECT event_id
      FROM CLAUDE_BI.ACTIVITY.EVENTS
      WHERE attributes:work_id::string = ?
      ORDER BY occurred_at DESC, event_id DESC
      LIMIT 1
    `;
    
    const newEventStmt = SF.createStatement({
      sqlText: newEventSQL,
      binds: [WORK_ID]
    });
    const newEventRS = newEventStmt.execute();
    newEventRS.next();
    
    return {
      result: 'ok',
      work_id: WORK_ID,
      display_id: displayId,
      points: POINTS,
      previous_points: currentPoints,
      action_type: actionType,
      last_event_id: newEventRS.getColumnValue('EVENT_ID'),
      status: status
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
GRANT USAGE ON PROCEDURE SDLC_CREATE_WORK(STRING, STRING, STRING, STRING, STRING, NUMBER, BOOLEAN, STRING) TO ROLE MCP_USER_ROLE;
GRANT USAGE ON PROCEDURE SDLC_STATUS(STRING, STRING, STRING, STRING, STRING, STRING) TO ROLE MCP_USER_ROLE;
GRANT USAGE ON PROCEDURE SDLC_ASSIGN(STRING, STRING, STRING, STRING, STRING, STRING, STRING) TO ROLE MCP_USER_ROLE;
GRANT USAGE ON PROCEDURE SDLC_COMPLETE_WORK(STRING, STRING, STRING, STRING, ARRAY, BOOLEAN, STRING) TO ROLE MCP_USER_ROLE;
GRANT USAGE ON PROCEDURE SDLC_ESTIMATE(STRING, NUMBER, STRING, STRING, STRING, STRING) TO ROLE MCP_USER_ROLE;

-- ============================================================================
-- END OF HARDENED CONCURRENCY PROCEDURES
-- 
-- Next: Update views with consistency layer and missing columns
-- ============================================================================