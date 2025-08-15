-- ============================================================================
-- 13_mcp_handler.sql
-- Main MCP request handler with secure token validation and replay protection
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE CLAUDE_BI;
USE SCHEMA MCP;

-- ============================================================================
-- Main MCP Handler - Single entry point for all MCP requests
-- ============================================================================

CREATE OR REPLACE PROCEDURE MCP.HANDLE_REQUEST(
  endpoint STRING,
  payload VARIANT,
  auth_token STRING
)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER  -- Runs with MCP_SERVICE_ROLE privileges
AS
$$
const SF = snowflake;

try {
  const startTime = Date.now();
  
  // 1. Hash the provided token
  const pepperRS = SF.createStatement({
    sqlText: "SELECT SYSTEM$GET_CONTEXT('MCP_SECURITY_CTX', 'server_pepper')"
  }).execute();
  pepperRS.next();
  const pepper = pepperRS.getColumnValue(1);
  
  const tokenHash = SF.createStatement({
    sqlText: "SELECT SHA2(? || ?, 256)",
    binds: [AUTH_TOKEN, pepper]
  }).execute().next() ? 
    SF.createStatement({
      sqlText: "SELECT SHA2(? || ?, 256)",
      binds: [AUTH_TOKEN, pepper]
    }).execute().getColumnValue(1) : null;
  
  if (!tokenHash) {
    throw new Error('Failed to hash token');
  }
  
  // 2. Validate nonce for replay protection
  const nonce = PAYLOAD.nonce;
  if (!nonce) {
    throw new Error('Missing nonce in request');
  }
  
  const nonceCheckSQL = `
    SELECT COUNT(*) AS used_count
    FROM CLAUDE_BI.ACTIVITY.EVENTS
    WHERE action = 'mcp.request.processed'
      AND attributes:nonce::STRING = :1
      AND occurred_at >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
  `;
  
  const nonceRS = SF.createStatement({
    sqlText: nonceCheckSQL,
    binds: [nonce]
  }).execute();
  nonceRS.next();
  
  if (nonceRS.getColumnValue('USED_COUNT') > 0) {
    // Log replay attempt
    SF.createStatement({
      sqlText: `
        INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS VALUES (
          OBJECT_CONSTRUCT(
            'event_id', SHA2(CONCAT_WS('|', 'security.replay', :1, :2), 256),
            'action', 'security.replay_detected',
            'occurred_at', CURRENT_TIMESTAMP(),
            'actor_id', 'unknown',
            'source', 'security',
            'schema_version', '2.1.0',
            'attributes', OBJECT_CONSTRUCT(
              'nonce', :2,
              'token_hash_prefix', SUBSTR(:1, 1, 8)
            )
          ),
          'SECURITY',
          CURRENT_TIMESTAMP()
        )
      `,
      binds: [tokenHash, nonce]
    }).execute();
    
    throw new Error('Replay detected: nonce already used');
  }
  
  // 3. Get user permissions with tie-breaker rules
  const permSQL = `
    WITH latest_perm AS (
      SELECT
        object_id AS username,
        action,
        attributes:allowed_tools::ARRAY AS allowed_tools,
        attributes:max_rows::NUMBER AS max_rows,
        attributes:daily_runtime_seconds::NUMBER AS runtime_budget,
        attributes:expires_at::TIMESTAMP_TZ AS expires_at,
        occurred_at,
        _recv_at,
        event_id
      FROM CLAUDE_BI.ACTIVITY.EVENTS
      WHERE object_type = 'user'
        AND action IN ('system.permission.granted', 'system.permission.revoked')
        AND attributes:token_hash::STRING = :1
      QUALIFY ROW_NUMBER() OVER (
        PARTITION BY attributes:token_hash
        ORDER BY 
          occurred_at DESC,
          _recv_at DESC,
          CASE 
            WHEN action = 'system.permission.revoked' THEN 0
            ELSE 1
          END,
          event_id DESC
      ) = 1
    )
    SELECT * FROM latest_perm
  `;
  
  const permRS = SF.createStatement({
    sqlText: permSQL,
    binds: [tokenHash]
  }).execute();
  
  if (!permRS.next()) {
    throw new Error('Invalid token - no permissions found');
  }
  
  const userPerms = {
    username: permRS.getColumnValue('USERNAME'),
    action: permRS.getColumnValue('ACTION'),
    allowedTools: permRS.getColumnValue('ALLOWED_TOOLS'),
    maxRows: permRS.getColumnValue('MAX_ROWS') || 1000,
    runtimeBudget: permRS.getColumnValue('RUNTIME_BUDGET') || 3600,
    expiresAt: permRS.getColumnValue('EXPIRES_AT')
  };
  
  // 4. Validate permission state
  if (userPerms.action === 'system.permission.revoked') {
    throw new Error('Access revoked for user: ' + userPerms.username);
  }
  
  if (userPerms.expiresAt && new Date(userPerms.expiresAt) < new Date()) {
    throw new Error('Token expired for user: ' + userPerms.username);
  }
  
  // 5. Check runtime budget
  const budgetSQL = `
    SELECT COALESCE(SUM(attributes:execution_ms::NUMBER), 0) / 1000 AS seconds_used
    FROM CLAUDE_BI.ACTIVITY.EVENTS
    WHERE action = 'mcp.request.processed'
      AND actor_id = :1
      AND occurred_at >= DATEADD('day', -1, CURRENT_TIMESTAMP())
  `;
  
  const budgetRS = SF.createStatement({
    sqlText: budgetSQL,
    binds: [userPerms.username]
  }).execute();
  budgetRS.next();
  
  const secondsUsed = budgetRS.getColumnValue('SECONDS_USED');
  if (secondsUsed >= userPerms.runtimeBudget) {
    throw new Error('Daily runtime budget exceeded: ' + secondsUsed + '/' + userPerms.runtimeBudget + ' seconds');
  }
  
  // 6. Set session parameters for resource governance
  SF.createStatement({
    sqlText: "ALTER SESSION SET QUERY_TAG = :1",
    binds: [JSON.stringify({
      mcp_user: userPerms.username,
      mcp_endpoint: ENDPOINT,
      mcp_tool: PAYLOAD.name || 'unknown'
    })]
  }).execute();
  
  SF.createStatement({
    sqlText: "ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 30"
  }).execute();
  
  // 7. Route to appropriate handler
  let result;
  
  switch(ENDPOINT) {
    case 'tools/call':
      result = handleToolCall(PAYLOAD, userPerms);
      break;
      
    case 'tools/list':
      result = handleToolList(userPerms);
      break;
      
    case 'health':
      result = { status: 'healthy', user: userPerms.username };
      break;
      
    default:
      throw new Error('Unknown endpoint: ' + ENDPOINT);
  }
  
  // 8. Audit successful request (to RAW_EVENTS, not EVENTS!)
  const executionMs = Date.now() - startTime;
  
  SF.createStatement({
    sqlText: `
      INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS VALUES (
        OBJECT_CONSTRUCT(
          'event_id', SHA2(CONCAT_WS('|', 'mcp.exec', :1, :2), 256),
          'action', 'mcp.request.processed',
          'occurred_at', CURRENT_TIMESTAMP(),
          'actor_id', :1,
          'source', 'mcp',
          'schema_version', '2.1.0',
          'object', OBJECT_CONSTRUCT(
            'type', 'request',
            'id', :2
          ),
          'attributes', OBJECT_CONSTRUCT(
            'endpoint', :3,
            'tool', :4,
            'nonce', :2,
            'execution_ms', :5,
            'rows_returned', :6
          )
        ),
        'MCP',
        CURRENT_TIMESTAMP()
      )
    `,
    binds: [
      userPerms.username,
      nonce,
      ENDPOINT,
      PAYLOAD.name || null,
      executionMs,
      result.row_count || 0
    ]
  }).execute();
  
  return result;
  
} catch (err) {
  // Audit failed request
  try {
    SF.createStatement({
      sqlText: `
        INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS VALUES (
          OBJECT_CONSTRUCT(
            'event_id', SHA2(CONCAT_WS('|', 'mcp.fail', CURRENT_TIMESTAMP()::STRING), 256),
            'action', 'mcp.request.failed',
            'occurred_at', CURRENT_TIMESTAMP(),
            'actor_id', 'unknown',
            'source', 'mcp',
            'schema_version', '2.1.0',
            'attributes', OBJECT_CONSTRUCT(
              'endpoint', :1,
              'error', :2,
              'nonce', :3
            )
          ),
          'MCP',
          CURRENT_TIMESTAMP()
        )
      `,
      binds: [ENDPOINT, err.toString(), PAYLOAD.nonce || null]
    }).execute();
  } catch (auditErr) {
    // Audit failed, but continue with error response
  }
  
  return {
    success: false,
    error: err.toString()
  };
}

// Handler for tool calls
function handleToolCall(payload, userPerms) {
  const toolName = payload.name;
  const args = payload.arguments || {};
  
  // Check if tool is allowed
  if (!userPerms.allowedTools || !userPerms.allowedTools.includes(toolName)) {
    throw new Error('Tool not allowed: ' + toolName);
  }
  
  // Route to specific tool
  switch(toolName) {
    case 'compose_query':
      return composeQuery(args, userPerms);
      
    case 'list_sources':
      return listSources(args, userPerms);
      
    case 'export_data':
      return exportData(args, userPerms);
      
    case 'create_dashboard':
      return createDashboard(args, userPerms);
      
    default:
      throw new Error('Unknown tool: ' + toolName);
  }
}

// Handler for listing available tools
function handleToolList(userPerms) {
  const allTools = [
    {
      name: 'compose_query',
      description: 'Compose and execute a query from natural language',
      category: 'query'
    },
    {
      name: 'list_sources',
      description: 'List available event types and sources',
      category: 'discovery'
    },
    {
      name: 'export_data',
      description: 'Export query results to downloadable format',
      category: 'export'
    },
    {
      name: 'create_dashboard',
      description: 'Create a dashboard from specifications',
      category: 'visualization'
    }
  ];
  
  // Filter to allowed tools
  const allowedTools = allTools.filter(tool => 
    userPerms.allowedTools && userPerms.allowedTools.includes(tool.name)
  );
  
  return {
    success: true,
    tools: allowedTools,
    user: userPerms.username,
    tool_count: allowedTools.length
  };
}

// Tool: Compose Query
function composeQuery(args, userPerms) {
  const query = args.query || args.natural_language || '';
  const limit = Math.min(args.limit || 100, userPerms.maxRows);
  
  // Simple natural language to SQL mapping (in production, use LLM)
  let sql;
  
  if (query.toLowerCase().includes('signup')) {
    sql = `
      SELECT COUNT(*) as signups, DATE(occurred_at) as signup_date
      FROM CLAUDE_BI.ACTIVITY.EVENTS
      WHERE action = 'user.signup'
        AND occurred_at >= DATEADD('week', -1, CURRENT_DATE())
      GROUP BY signup_date
      ORDER BY signup_date DESC
      LIMIT ${limit}
    `;
  } else if (query.toLowerCase().includes('permission')) {
    sql = `
      SELECT object_id as username, action, occurred_at
      FROM CLAUDE_BI.ACTIVITY.EVENTS
      WHERE action LIKE 'system.permission.%'
      ORDER BY occurred_at DESC
      LIMIT ${limit}
    `;
  } else {
    // Default: recent events
    sql = `
      SELECT event_id, action, actor_id, occurred_at
      FROM CLAUDE_BI.ACTIVITY.EVENTS
      ORDER BY occurred_at DESC
      LIMIT ${limit}
    `;
  }
  
  // Execute query
  const rs = SF.createStatement({ sqlText: sql }).execute();
  const rows = [];
  
  while (rs.next() && rows.length < limit) {
    const row = {};
    for (let i = 1; i <= rs.getColumnCount(); i++) {
      row[rs.getColumnName(i)] = rs.getColumnValue(i);
    }
    rows.push(row);
  }
  
  return {
    success: true,
    query: query,
    sql: sql,
    rows: rows,
    row_count: rows.length,
    limit_applied: limit
  };
}

// Tool: List Sources
function listSources(args, userPerms) {
  const sql = `
    SELECT DISTINCT 
      action,
      COUNT(*) as event_count,
      MIN(occurred_at) as first_seen,
      MAX(occurred_at) as last_seen
    FROM CLAUDE_BI.ACTIVITY.EVENTS
    GROUP BY action
    ORDER BY event_count DESC
    LIMIT 100
  `;
  
  const rs = SF.createStatement({ sqlText: sql }).execute();
  const sources = [];
  
  while (rs.next()) {
    sources.push({
      action: rs.getColumnValue('ACTION'),
      count: rs.getColumnValue('EVENT_COUNT'),
      first_seen: rs.getColumnValue('FIRST_SEEN'),
      last_seen: rs.getColumnValue('LAST_SEEN')
    });
  }
  
  return {
    success: true,
    sources: sources,
    source_count: sources.length
  };
}

// Tool: Export Data (placeholder)
function exportData(args, userPerms) {
  // In production, would create a stage and return presigned URL
  return {
    success: true,
    message: 'Export functionality not yet implemented',
    placeholder_url: 'https://export.example.com/data.csv'
  };
}

// Tool: Create Dashboard (placeholder)
function createDashboard(args, userPerms) {
  // In production, would generate Streamlit app
  return {
    success: true,
    message: 'Dashboard creation not yet implemented',
    dashboard_id: 'dash_' + Date.now()
  };
}
$$;

-- ============================================================================
-- Grant permissions
-- ============================================================================

GRANT EXECUTE ON PROCEDURE MCP.HANDLE_REQUEST(STRING, VARIANT, STRING) TO ROLE MCP_SERVICE_ROLE;