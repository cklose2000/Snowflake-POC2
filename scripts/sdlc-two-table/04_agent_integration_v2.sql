-- ============================================================================
-- 04_agent_integration_v2.sql
-- HARDENED Agent-Specific SDLC Procedures - Two-Table Law Compliant  
-- Smart work claiming with version checking and automatic retries
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE CLAUDE_BI;
USE SCHEMA MCP;

-- ============================================================================
-- HARDENED: Smart work claiming with concurrency control and retries
-- ============================================================================
CREATE OR REPLACE PROCEDURE SDLC_CLAIM_NEXT(
  agent_id STRING,
  agent_type STRING,
  agent_capabilities ARRAY DEFAULT NULL,
  max_retry_attempts NUMBER DEFAULT 3
)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
const SF = snowflake;

try {
  let attempt = 0;
  const maxAttempts = MAX_RETRY_ATTEMPTS || 3;
  
  while (attempt < maxAttempts) {
    attempt++;
    
    // Get next available work item with skill matching
    const findWorkSQL = `
      WITH candidate_work AS (
        SELECT 
          work_id,
          display_id,
          title,
          type,
          severity,
          priority_score,
          age_hours,
          last_event_id,
          -- Skill matching score (simple keyword matching)
          CASE 
            WHEN ? IS NULL THEN 1  -- No capabilities specified, any work is fine
            WHEN title ILIKE ANY (SELECT '%' || value::string || '%' FROM TABLE(FLATTEN(?))) THEN 3
            WHEN description ILIKE ANY (SELECT '%' || value::string || '%' FROM TABLE(FLATTEN(?))) THEN 2  
            ELSE 1
          END AS skill_match_score
        FROM CLAUDE_BI.MCP.VW_PRIORITY_QUEUE
        WHERE is_available = TRUE
        ORDER BY 
          skill_match_score DESC,
          priority_score DESC,
          age_hours DESC
        LIMIT 5  -- Consider top 5 candidates
      )
      SELECT 
        work_id,
        display_id,
        title,
        type,
        severity,
        last_event_id,
        skill_match_score,
        priority_score
      FROM candidate_work
      ORDER BY skill_match_score DESC, priority_score DESC
      LIMIT 1
    `;
    
    const findStmt = SF.createStatement({
      sqlText: findWorkSQL,
      binds: [AGENT_CAPABILITIES, AGENT_CAPABILITIES, AGENT_CAPABILITIES]
    });
    const findRS = findStmt.execute();
    
    if (!findRS.next()) {
      return {
        result: 'no_work_available',
        claimed: false,
        agent_id: AGENT_ID,
        attempts: attempt
      };
    }
    
    const workId = findRS.getColumnValue('WORK_ID');
    const displayId = findRS.getColumnValue('DISPLAY_ID');
    const title = findRS.getColumnValue('TITLE');
    const lastEventId = findRS.getColumnValue('LAST_EVENT_ID');
    const skillScore = findRS.getColumnValue('SKILL_MATCH_SCORE');
    const priorityScore = findRS.getColumnValue('PRIORITY_SCORE');
    
    // Re-verify the work item is still available (double-check for race conditions)
    const verifySQL = `
      SELECT 
        last_event_id,
        status,
        assignee_id
      FROM CLAUDE_BI.MCP.VW_WORK_ITEMS
      WHERE work_id = ?
    `;
    
    const verifyStmt = SF.createStatement({
      sqlText: verifySQL,
      binds: [workId]
    });
    const verifyRS = verifyStmt.execute();
    
    if (!verifyRS.next()) {
      // Work item disappeared, retry
      continue;
    }
    
    const currentLastEventId = verifyRS.getColumnValue('LAST_EVENT_ID');
    const currentStatus = verifyRS.getColumnValue('STATUS');
    const currentAssignee = verifyRS.getColumnValue('ASSIGNEE_ID');
    
    // Check if work item state changed
    if (currentLastEventId !== lastEventId) {
      // State changed, retry with next item
      continue;
    }
    
    // Check if already assigned
    if (currentAssignee) {
      // Already assigned, retry with next item
      continue;
    }
    
    // Check if status is still claimable
    if (!['new', 'ready', 'backlog'].includes(currentStatus)) {
      // Not claimable, retry
      continue;
    }
    
    // Generate idempotency keys
    const claimIdempotencyKey = AGENT_ID + '_claim_' + workId + '_' + Date.now();
    const assignIdempotencyKey = AGENT_ID + '_assign_' + workId + '_' + Date.now();
    
    // First emit claim event with version check
    const claimPayload = {
      action: 'sdlc.agent.claim',
      work_id: workId,
      agent_id: AGENT_ID,
      agent_type: AGENT_TYPE,
      agent_capabilities: AGENT_CAPABILITIES,
      claim_reason: 'Automatic assignment by SDLC_CLAIM_NEXT',
      skill_match_score: skillScore,
      priority_score: priorityScore,
      expected_last_event_id: currentLastEventId,
      actor_id: AGENT_ID,
      idempotency_key: claimIdempotencyKey,
      schema_version: '1.0.0'
    };
    
    const claimSQL = `CALL CLAUDE_BI.MCP.SDLC_UPSERT_EVENT_IDEMPOTENT(?)`;
    const claimStmt = SF.createStatement({
      sqlText: claimSQL,
      binds: [claimPayload]
    });
    const claimRS = claimStmt.execute();
    claimRS.next();
    
    const claimResult = claimRS.getColumnValue(1);
    
    if (claimResult.result === 'error') {
      // Claim failed, emit conflict event and retry
      const conflictPayload = {
        action: 'sdlc.agent.conflict',
        work_id: workId,
        agent_id: AGENT_ID,
        conflict_type: 'claim_failed',
        attempt_number: attempt,
        actor_id: AGENT_ID,
        idempotency_key: claimIdempotencyKey + '_conflict',
        schema_version: '1.0.0'
      };
      
      SF.createStatement({
        sqlText: claimSQL,
        binds: [conflictPayload]
      }).execute();
      
      continue;
    }
    
    // Get the new last_event_id after claim
    const getNewEventSQL = `
      SELECT event_id
      FROM CLAUDE_BI.ACTIVITY.EVENTS
      WHERE attributes:work_id::string = ?
      ORDER BY occurred_at DESC, event_id DESC
      LIMIT 1
    `;
    
    const getNewEventStmt = SF.createStatement({
      sqlText: getNewEventSQL,
      binds: [workId]
    });
    const getNewEventRS = getNewEventStmt.execute();
    getNewEventRS.next();
    
    const newLastEventId = getNewEventRS.getColumnValue('EVENT_ID');
    
    // Now try to assign the work to the agent with version check
    const assignSQL = `
      CALL CLAUDE_BI.MCP.SDLC_ASSIGN(?, ?, ?, ?, ?, ?, ?)
    `;
    
    const assignStmt = SF.createStatement({
      sqlText: assignSQL,
      binds: [
        workId,
        AGENT_ID,
        AGENT_TYPE,
        newLastEventId,  // Use the event ID after claim
        AGENT_ID,
        'Claimed by agent ' + AGENT_ID,
        assignIdempotencyKey
      ]
    });
    const assignRS = assignStmt.execute();
    assignRS.next();
    
    const assignResult = assignRS.getColumnValue(1);
    
    if (assignResult.result === 'ok') {
      // Success! Also update status to in_progress if it's not already
      const finalLastEventId = assignResult.last_event_id;
      const currentStatus = assignResult.status;
      
      if (currentStatus !== 'in_progress' && currentStatus !== 'done') {
        const statusIdempotencyKey = AGENT_ID + '_status_' + workId + '_' + Date.now();
        
        const statusSQL = `
          CALL CLAUDE_BI.MCP.SDLC_STATUS(?, ?, ?, ?, ?, ?)
        `;
        
        try {
          SF.createStatement({
            sqlText: statusSQL,
            binds: [
              workId,
              'in_progress',
              finalLastEventId,
              AGENT_ID,
              'Started by agent ' + AGENT_ID,
              statusIdempotencyKey
            ]
          }).execute();
        } catch (statusErr) {
          // Status update failed, but assignment succeeded - that's OK
        }
      }
      
      return {
        result: 'ok',
        claimed: true,
        work_id: workId,
        display_id: displayId,
        title: title,
        agent_id: AGENT_ID,
        skill_match_score: skillScore,
        priority_score: priorityScore,
        last_event_id: finalLastEventId,
        attempts: attempt
      };
      
    } else if (assignResult.error === 'conflict') {
      // Conflict occurred during assignment
      const conflictPayload = {
        action: 'sdlc.agent.conflict',
        work_id: workId,
        agent_id: AGENT_ID,
        conflict_type: 'assign_conflict',
        expected: newLastEventId,
        actual: assignResult.actual,
        attempt_number: attempt,
        actor_id: AGENT_ID,
        idempotency_key: assignIdempotencyKey + '_conflict',
        schema_version: '1.0.0'
      };
      
      SF.createStatement({
        sqlText: claimSQL,
        binds: [conflictPayload]
      }).execute();
      
      // Retry with next work item
      continue;
    } else {
      // Other error during assignment
      return {
        result: 'error',
        claimed: false,
        error: assignResult.error,
        work_id: workId,
        display_id: displayId,
        attempts: attempt
      };
    }
  }
  
  // Max attempts reached
  return {
    result: 'max_attempts_reached',
    claimed: false,
    error: 'max_retry_attempts_reached',
    max_attempts: maxAttempts,
    agent_id: AGENT_ID
  };
  
} catch (err) {
  return {
    result: 'error',
    claimed: false,
    error: err.toString(),
    agent_id: AGENT_ID
  };
}
$$;

-- ============================================================================
-- HARDENED: Complete work item with version checking
-- ============================================================================
CREATE OR REPLACE PROCEDURE SDLC_AGENT_COMPLETE_WORK(
  work_id STRING,
  agent_id STRING,
  completion_notes STRING DEFAULT NULL,
  deliverables ARRAY DEFAULT NULL,
  tests_passing BOOLEAN DEFAULT TRUE
)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
const SF = snowflake;

try {
  // Get current work state
  const currentSQL = `
    SELECT 
      last_event_id,
      status,
      assignee_id,
      display_id,
      title,
      created_at
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
  
  const lastEventId = currentRS.getColumnValue('LAST_EVENT_ID');
  const status = currentRS.getColumnValue('STATUS');
  const assigneeId = currentRS.getColumnValue('ASSIGNEE_ID');
  const displayId = currentRS.getColumnValue('DISPLAY_ID');
  const title = currentRS.getColumnValue('TITLE');
  
  // Verify agent is assigned to this work
  if (assigneeId !== AGENT_ID) {
    return {
      result: 'error',
      error: 'not_assigned',
      assignee_id: assigneeId,
      agent_id: AGENT_ID,
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
  
  // Complete the work with version check
  const completeSQL = `
    CALL CLAUDE_BI.MCP.SDLC_COMPLETE_WORK(?, ?, ?, ?, ?, ?, ?)
  `;
  
  const completeStmt = SF.createStatement({
    sqlText: completeSQL,
    binds: [
      WORK_ID,
      lastEventId,  // expected_last_event_id
      AGENT_ID,     // actor_id
      COMPLETION_NOTES,
      DELIVERABLES,
      TESTS_PASSING,
      'agent_complete_' + WORK_ID + '_' + Date.now()  // idempotency_key
    ]
  });
  const completeRS = completeStmt.execute();
  completeRS.next();
  
  const result = completeRS.getColumnValue(1);
  
  if (result.result === 'ok') {
    return {
      result: 'ok',
      work_id: WORK_ID,
      display_id: displayId,
      title: title,
      completion_time_ms: result.completion_time_ms,
      completion_time_hours: result.completion_time_hours,
      tests_passing: TESTS_PASSING,
      last_event_id: result.last_event_id,
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
-- Handle agent errors with automatic retry logic (updated)
-- ============================================================================
CREATE OR REPLACE PROCEDURE SDLC_HANDLE_ERROR(
  work_id STRING,
  agent_id STRING,
  error_type STRING,
  error_message STRING,
  will_retry BOOLEAN DEFAULT TRUE,
  retry_after_ms NUMBER DEFAULT 5000
)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
const SF = snowflake;

try {
  // Get display_id for the work item
  const workSQL = `
    SELECT display_id, status, assignee_id
    FROM CLAUDE_BI.MCP.VW_WORK_ITEMS
    WHERE work_id = ?
  `;
  
  const workStmt = SF.createStatement({
    sqlText: workSQL,
    binds: [WORK_ID]
  });
  const workRS = workStmt.execute();
  
  let displayId = null;
  let currentStatus = null;
  let assigneeId = null;
  
  if (workRS.next()) {
    displayId = workRS.getColumnValue('DISPLAY_ID');
    currentStatus = workRS.getColumnValue('STATUS');
    assigneeId = workRS.getColumnValue('ASSIGNEE_ID');
  }
  
  // Get current retry count for this work item and agent
  const retrySQL = `
    SELECT COUNT(*) as retry_count
    FROM CLAUDE_BI.ACTIVITY.EVENTS
    WHERE action = 'sdlc.agent.error'
      AND attributes:work_id::string = ?
      AND attributes:agent_id::string = ?
      AND attributes:error_type::string != 'conflict'  -- Don't count conflicts as retries
      AND occurred_at >= DATEADD('hour', -1, CURRENT_TIMESTAMP())  -- Only count recent errors
  `;
  
  const retryStmt = SF.createStatement({
    sqlText: retrySQL,
    binds: [WORK_ID, AGENT_ID]
  });
  const retryRS = retryStmt.execute();
  retryRS.next();
  
  const retryCount = retryRS.getColumnValue('RETRY_COUNT');
  const maxRetries = 3;
  
  // Determine if we should retry
  const shouldRetry = WILL_RETRY && retryCount < maxRetries;
  
  // Create error event
  const payload = {
    action: 'sdlc.agent.error',
    work_id: WORK_ID,
    agent_id: AGENT_ID,
    error_type: ERROR_TYPE,
    error_message: ERROR_MESSAGE,
    retry_count: retryCount + 1,
    will_retry: shouldRetry,
    retry_after_ms: RETRY_AFTER_MS,
    max_retries: maxRetries,
    actor_id: AGENT_ID,
    idempotency_key: AGENT_ID + '_error_' + WORK_ID + '_' + Date.now(),
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
    // If max retries exceeded and agent is assigned, mark work as blocked
    if (!shouldRetry && retryCount >= maxRetries && assigneeId === AGENT_ID) {
      const currentWorkSQL = `
        SELECT last_event_id FROM CLAUDE_BI.MCP.VW_WORK_ITEMS WHERE work_id = ?
      `;
      const currentStmt = SF.createStatement({
        sqlText: currentWorkSQL,
        binds: [WORK_ID]
      });
      const currentRS = currentStmt.execute();
      
      if (currentRS.next()) {
        const lastEventId = currentRS.getColumnValue('LAST_EVENT_ID');
        
        // Update status to blocked
        const statusSQL = `
          CALL CLAUDE_BI.MCP.SDLC_STATUS(?, ?, ?, ?, ?, ?)
        `;
        
        try {
          SF.createStatement({
            sqlText: statusSQL,
            binds: [
              WORK_ID,
              'blocked',
              lastEventId,
              'system',
              'Max retry attempts exceeded for agent ' + AGENT_ID,
              AGENT_ID + '_block_' + WORK_ID + '_' + Date.now()
            ]
          }).execute();
        } catch (statusErr) {
          // Status update failed, but error was logged
        }
      }
    }
    
    return {
      result: 'ok',
      work_id: WORK_ID,
      display_id: displayId,
      agent_id: AGENT_ID,
      error_type: ERROR_TYPE,
      retry_count: retryCount + 1,
      will_retry: shouldRetry,
      max_retries_exceeded: !shouldRetry && retryCount >= maxRetries
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
-- Release/unclaim work (updated with display_id)
-- ============================================================================
CREATE OR REPLACE PROCEDURE SDLC_RELEASE_WORK(
  work_id STRING,
  agent_id STRING,
  release_reason STRING DEFAULT NULL
)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
const SF = snowflake;

try {
  // Get current work state
  const currentSQL = `
    SELECT 
      last_event_id,
      status,
      assignee_id,
      display_id
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
  
  const lastEventId = currentRS.getColumnValue('LAST_EVENT_ID');
  const status = currentRS.getColumnValue('STATUS');
  const assigneeId = currentRS.getColumnValue('ASSIGNEE_ID');
  const displayId = currentRS.getColumnValue('DISPLAY_ID');
  
  // Verify agent is assigned to this work
  if (assigneeId !== AGENT_ID) {
    return {
      result: 'error',
      error: 'not_assigned',
      assignee_id: assigneeId,
      agent_id: AGENT_ID,
      work_id: WORK_ID,
      display_id: displayId
    };
  }
  
  // Create release event (unassign)
  const assignSQL = `
    CALL CLAUDE_BI.MCP.SDLC_ASSIGN(?, ?, ?, ?, ?, ?, ?)
  `;
  
  const assignStmt = SF.createStatement({
    sqlText: assignSQL,
    binds: [
      WORK_ID,
      null,  // Unassign
      'unassigned',
      lastEventId,
      AGENT_ID,
      RELEASE_REASON || 'Released by agent ' + AGENT_ID,
      AGENT_ID + '_release_' + WORK_ID + '_' + Date.now()
    ]
  });
  const assignRS = assignStmt.execute();
  assignRS.next();
  
  const assignResult = assignRS.getColumnValue(1);
  
  if (assignResult.result === 'ok') {
    // Also reset status to ready if it was in_progress
    if (status === 'in_progress') {
      const newLastEventId = assignResult.last_event_id;
      
      const statusSQL = `
        CALL CLAUDE_BI.MCP.SDLC_STATUS(?, ?, ?, ?, ?, ?)
      `;
      
      try {
        SF.createStatement({
          sqlText: statusSQL,
          binds: [
            WORK_ID,
            'ready',
            newLastEventId,
            AGENT_ID,
            'Reset to ready after release by ' + AGENT_ID,
            AGENT_ID + '_ready_' + WORK_ID + '_' + Date.now()
          ]
        }).execute();
      } catch (statusErr) {
        // Status update failed, but release succeeded
      }
    }
    
    return {
      result: 'ok',
      work_id: WORK_ID,
      display_id: displayId,
      agent_id: AGENT_ID,
      released: true,
      last_event_id: assignResult.last_event_id
    };
  } else {
    return assignResult;
  }
  
} catch (err) {
  return {
    result: 'error',
    error: err.toString()
  };
}
$$;

-- ============================================================================
-- Get work recommendations for an agent (updated with display_id)
-- ============================================================================
CREATE OR REPLACE VIEW VW_AGENT_WORK_RECOMMENDATIONS AS
WITH recent_agent_work AS (
  -- Get what each agent has worked on recently
  SELECT 
    e.attributes:agent_id::string AS agent_id,
    w.type,
    w.severity,
    COUNT(*) AS work_count,
    AVG(CASE WHEN w.status = 'done' THEN 1 ELSE 0 END) AS success_rate
  FROM CLAUDE_BI.ACTIVITY.EVENTS e
  JOIN CLAUDE_BI.MCP.VW_WORK_ITEMS w ON w.work_id = e.attributes:work_id::string
  WHERE e.action = 'sdlc.agent.claim'
    AND e.occurred_at >= DATEADD('day', -30, CURRENT_TIMESTAMP())
  GROUP BY e.attributes:agent_id::string, w.type, w.severity
),
agent_preferences AS (
  -- Calculate agent preferences based on historical performance
  SELECT 
    agent_id,
    type,
    severity,
    work_count,
    success_rate,
    -- Preference score: more work done + higher success rate = better fit
    (work_count * success_rate) AS preference_score
  FROM recent_agent_work
  WHERE success_rate > 0.5  -- Only consider types/severities where agent was >50% successful
)
SELECT 
  w.work_id,
  w.display_id,
  w.title,
  w.type,
  w.severity,
  w.priority_score,
  w.age_hours,
  ap.agent_id,
  COALESCE(ap.preference_score, 0) AS agent_fit_score,
  -- Combined recommendation score
  w.priority_score + COALESCE(ap.preference_score * 10, 0) AS recommendation_score
FROM CLAUDE_BI.MCP.VW_PRIORITY_QUEUE w
CROSS JOIN (SELECT DISTINCT agent_id FROM agent_preferences) agents
LEFT JOIN agent_preferences ap ON ap.agent_id = agents.agent_id 
  AND ap.type = w.type 
  AND ap.severity = w.severity
WHERE w.is_available = TRUE
ORDER BY agents.agent_id, recommendation_score DESC;

-- ============================================================================
-- Grant procedure permissions
-- ============================================================================
GRANT USAGE ON PROCEDURE SDLC_CLAIM_NEXT(STRING, STRING, ARRAY, NUMBER) TO ROLE MCP_USER_ROLE;
GRANT USAGE ON PROCEDURE SDLC_AGENT_COMPLETE_WORK(STRING, STRING, STRING, ARRAY, BOOLEAN) TO ROLE MCP_USER_ROLE;
GRANT USAGE ON PROCEDURE SDLC_HANDLE_ERROR(STRING, STRING, STRING, STRING, BOOLEAN, NUMBER) TO ROLE MCP_USER_ROLE;
GRANT USAGE ON PROCEDURE SDLC_RELEASE_WORK(STRING, STRING, STRING) TO ROLE MCP_USER_ROLE;

GRANT SELECT ON VIEW VW_AGENT_WORK_RECOMMENDATIONS TO ROLE MCP_USER_ROLE;

-- ============================================================================
-- END OF HARDENED AGENT INTEGRATION
-- 
-- Next: Update event taxonomy with new event types
-- ============================================================================