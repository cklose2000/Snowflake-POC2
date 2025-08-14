-- ============================================================================
-- 05_mcp_procedures.sql
-- Core MCP procedures with event-based permission checking
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE CLAUDE_BI;
USE SCHEMA MCP;

-- ============================================================================
-- Main MCP execution procedure with event-based permission checking
-- Returns query_id for RESULT_SCAN pattern
-- ============================================================================
CREATE OR REPLACE PROCEDURE EXECUTE_QUERY_PLAN(plan VARIANT)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER  -- Runs with MCP_SERVICE_ROLE privileges
AS
$$
const SF = snowflake;

// Get the calling user
const who = (() => {
  const rs = SF.createStatement({sqlText: 'SELECT CURRENT_USER()'}).execute();
  rs.next(); 
  return rs.getColumnValue(1);
})();

const startTime = Date.now();

try {
  // 1) Pull latest permission event for user
  const permSQL = `
    WITH perms AS (
      SELECT 
        object_id, 
        action, 
        attributes, 
        occurred_at,
        ROW_NUMBER() OVER (PARTITION BY object_id ORDER BY occurred_at DESC) rn
      FROM CLAUDE_BI.ACTIVITY.EVENTS
      WHERE object_type = 'user'
        AND action IN ('system.permission.granted', 'system.permission.revoked')
        AND object_id = :1
    )
    SELECT
      attributes:allowed_actions::array AS allowed_actions,
      attributes:max_rows::number AS max_rows,
      attributes:daily_runtime_budget_s::number AS daily_runtime_budget_s,
      attributes:can_export::boolean AS can_export,
      attributes:expires_at::timestamp_tz AS expires_at,
      action
    FROM perms 
    WHERE rn = 1
  `;
  
  const permStmt = SF.createStatement({
    sqlText: permSQL, 
    binds: [who]
  });
  const permRS = permStmt.execute();
  
  if (!permRS.next()) {
    throw new Error(`User ${who} has no permission events`);
  }
  
  if (permRS.getColumnValue('ACTION') !== 'system.permission.granted') {
    throw new Error(`User ${who} permission was revoked`);
  }
  
  const expires = permRS.getColumnValue('EXPIRES_AT');
  if (expires && new Date(expires) <= new Date()) {
    throw new Error(`Permission expired for user ${who}`);
  }
  
  const allowedActions = permRS.getColumnValue('ALLOWED_ACTIONS') || [];
  const maxRows = permRS.getColumnValue('MAX_ROWS') || 10000;
  const budgetS = permRS.getColumnValue('DAILY_RUNTIME_BUDGET_S') || 120;
  const canExport = permRS.getColumnValue('CAN_EXPORT') || false;
  
  // 2) Validate plan
  const p = PLAN || {};
  const reqLimit = Math.min(Number(p.limit || 100), maxRows);
  const reqWindow = p.window || 'last_30d';
  
  if (!Array.isArray(p.actions) || p.actions.length === 0) {
    throw new Error('Plan must specify actions array');
  }
  
  // Check each requested action is allowed
  for (const a of p.actions) {
    if (!allowedActions.includes(a)) {
      throw new Error(`Action not allowed: ${a}`);
    }
  }
  
  // 3) Rate-limit by runtime (sum of last 24h)
  const usageSQL = `
    SELECT COALESCE(SUM(attributes:execution_time_ms::number), 0)/1000 AS seconds_used
    FROM CLAUDE_BI.ACTIVITY.EVENTS
    WHERE action = 'mcp.query.executed'
      AND actor_id = :1
      AND occurred_at >= DATEADD('day', -1, CURRENT_TIMESTAMP())
  `;
  
  const usedStmt = SF.createStatement({
    sqlText: usageSQL, 
    binds: [who]
  });
  const usedRS = usedStmt.execute();
  usedRS.next();
  
  if (Number(usedRS.getColumnValue(1)) >= budgetS) {
    throw new Error('Daily runtime budget exceeded');
  }
  
  // 4) Build safe SQL with parameterized window
  const windowExpr = (() => {
    switch(reqWindow) {
      case 'last_7d':  return "DATEADD('day', -7, CURRENT_TIMESTAMP())";
      case 'last_30d': return "DATEADD('day', -30, CURRENT_TIMESTAMP())";
      case 'last_90d': return "DATEADD('day', -90, CURRENT_TIMESTAMP())";
      default: throw new Error('Unsupported window: ' + reqWindow);
    }
  })();
  
  const sql = `
    SELECT 
      event_id, 
      occurred_at, 
      actor_id, 
      action, 
      object_type, 
      object_id, 
      attributes
    FROM CLAUDE_BI.ACTIVITY.EVENTS
    WHERE occurred_at >= ${windowExpr}
      AND action IN (SELECT VALUE::string FROM TABLE(FLATTEN(INPUT => PARSE_JSON(:1))))
    ORDER BY occurred_at DESC
    LIMIT :2
  `;
  
  // 5) Tag query and execute
  SF.execute({
    sqlText: "ALTER SESSION SET QUERY_TAG = :1",
    binds: [`mcp:user=${who};plan=${HASH(JSON.stringify(p))}`]
  });
  
  const stmt = SF.createStatement({
    sqlText: sql, 
    binds: [JSON.stringify(p.actions), reqLimit]
  });
  const rs = stmt.execute();
  const qid = stmt.getQueryId();
  
  // 6) Log execution as an event
  const logSQL = `
    INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS
    SELECT OBJECT_CONSTRUCT(
      'event_id', UUID_STRING(),
      'action', 'mcp.query.executed',
      'occurred_at', CURRENT_TIMESTAMP(),
      'actor_id', :1,
      'source', 'mcp',
      'schema_version', '2.1.0',
      'object', OBJECT_CONSTRUCT(
        'type', 'query',
        'id', :2
      ),
      'attributes', OBJECT_CONSTRUCT(
        'plan', PARSE_JSON(:3),
        'rows_requested', :4,
        'window', :5,
        'execution_time_ms', :6
      )
    ), 'MCP', CURRENT_TIMESTAMP()
  `;
  
  SF.execute({
    sqlText: logSQL,
    binds: [
      who, 
      qid, 
      JSON.stringify(p), 
      reqLimit, 
      reqWindow,
      Date.now() - startTime
    ]
  });
  
  // 7) Return query_id for RESULT_SCAN
  return {
    query_id: qid,
    limit: reqLimit,
    window: reqWindow,
    actions: p.actions.length,
    user: who
  };
  
} catch (err) {
  // Log failure as an event
  const errorSQL = `
    INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS
    SELECT OBJECT_CONSTRUCT(
      'event_id', UUID_STRING(),
      'action', 'mcp.query.rejected',
      'occurred_at', CURRENT_TIMESTAMP(),
      'actor_id', :1,
      'source', 'mcp',
      'schema_version', '2.1.0',
      'object', OBJECT_CONSTRUCT(
        'type', 'query',
        'id', UUID_STRING()
      ),
      'attributes', OBJECT_CONSTRUCT(
        'plan', PARSE_JSON(:2),
        'error', :3,
        'execution_time_ms', :4
      )
    ), 'MCP', CURRENT_TIMESTAMP()
  `;
  
  try {
    SF.execute({
      sqlText: errorSQL,
      binds: [
        who,
        JSON.stringify(PLAN || {}),
        err.toString(),
        Date.now() - startTime
      ]
    });
  } catch (logErr) {
    // Logging failed, but still throw original error
  }
  
  throw err;
}
$$;

-- ============================================================================
-- Procedure to grant permissions (creates an event)
-- ============================================================================
CREATE OR REPLACE PROCEDURE GRANT_USER_PERMISSION(
  username STRING,
  allowed_actions ARRAY,
  max_rows NUMBER,
  daily_runtime_budget_s NUMBER,
  can_export BOOLEAN,
  expires_at TIMESTAMP_TZ
)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
const SF = snowflake;

// Get granting user
const grantingUser = (() => {
  const rs = SF.createStatement({sqlText: 'SELECT CURRENT_USER()'}).execute();
  rs.next();
  return rs.getColumnValue(1);
})();

// Insert permission grant event
const grantSQL = `
  INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS
  SELECT OBJECT_CONSTRUCT(
    'event_id', UUID_STRING(),
    'action', 'system.permission.granted',
    'occurred_at', CURRENT_TIMESTAMP(),
    'actor_id', :1,
    'source', 'system',
    'schema_version', '2.1.0',
    'object', OBJECT_CONSTRUCT(
      'type', 'user',
      'id', :2
    ),
    'attributes', OBJECT_CONSTRUCT(
      'allowed_actions', :3,
      'max_rows', :4,
      'daily_runtime_budget_s', :5,
      'can_export', :6,
      'expires_at', :7,
      'granted_by', :1
    )
  ), 'ADMIN', CURRENT_TIMESTAMP()
`;

SF.execute({
  sqlText: grantSQL,
  binds: [
    grantingUser, 
    USERNAME, 
    ALLOWED_ACTIONS, 
    MAX_ROWS,
    DAILY_RUNTIME_BUDGET_S, 
    CAN_EXPORT, 
    EXPIRES_AT
  ]
});

return `Permission granted to ${USERNAME} by ${grantingUser}`;
$$;

-- ============================================================================
-- Procedure to revoke permissions (creates a revocation event)
-- ============================================================================
CREATE OR REPLACE PROCEDURE REVOKE_USER_PERMISSION(username STRING, reason STRING)
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
const SF = snowflake;

// Get revoking user
const revokingUser = (() => {
  const rs = SF.createStatement({sqlText: 'SELECT CURRENT_USER()'}).execute();
  rs.next();
  return rs.getColumnValue(1);
})();

// Insert permission revoke event
const revokeSQL = `
  INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS
  SELECT OBJECT_CONSTRUCT(
    'event_id', UUID_STRING(),
    'action', 'system.permission.revoked',
    'occurred_at', CURRENT_TIMESTAMP(),
    'actor_id', :1,
    'source', 'system',
    'schema_version', '2.1.0',
    'object', OBJECT_CONSTRUCT(
      'type', 'user',
      'id', :2
    ),
    'attributes', OBJECT_CONSTRUCT(
      'reason', :3,
      'revoked_by', :1
    )
  ), 'ADMIN', CURRENT_TIMESTAMP()
`;

SF.execute({
  sqlText: revokeSQL,
  binds: [revokingUser, USERNAME, REASON]
});

return `Permission revoked for ${USERNAME} by ${revokingUser}`;
$$;

-- ============================================================================
-- Grant procedure permissions
-- ============================================================================
GRANT USAGE ON PROCEDURE EXECUTE_QUERY_PLAN(VARIANT) TO ROLE MCP_USER_ROLE;
GRANT USAGE ON PROCEDURE GRANT_USER_PERMISSION(STRING, ARRAY, NUMBER, NUMBER, BOOLEAN, TIMESTAMP_TZ) TO ROLE MCP_ADMIN_ROLE;
GRANT USAGE ON PROCEDURE REVOKE_USER_PERMISSION(STRING, STRING) TO ROLE MCP_ADMIN_ROLE;