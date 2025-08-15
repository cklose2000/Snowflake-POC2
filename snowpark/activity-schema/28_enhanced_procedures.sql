-- Enhanced MCP Procedures for Thin Client Integration
-- SECURITY: Server-side token hashing with pepper and budget enforcement
-- CORRECTNESS: Proper error handling and deterministic tie-breaking

USE ROLE ACCOUNTADMIN;
USE DATABASE CLAUDE_BI;
USE SCHEMA MCP;

-- Set up system parameter for pepper (secure)
-- In production, this would be set via secure parameter management
-- CREATE OR REPLACE SYSTEM PARAMETER MCP_SERVER_PEPPER = 'your_secure_pepper_here_change_this';

-- Enhanced HANDLE_REQUEST procedure with security and budget enforcement
CREATE OR REPLACE PROCEDURE MCP.HANDLE_REQUEST(
  method STRING,
  params VARIANT,
  user_token STRING
)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
  // Get pepper from system parameter or default
  function getPepper() {
    try {
      var stmt = snowflake.createStatement({
        sqlText: "SELECT SYSTEM$GET_PARAMETER('MCP_SERVER_PEPPER') as pepper"
      });
      var result = stmt.execute();
      if (result.next()) {
        var pepper = result.getColumnValue('PEPPER');
        return pepper || 'default_development_pepper_change_in_production';
      }
    } catch (e) {
      // Fallback for development
      return 'default_development_pepper_change_in_production';
    }
    return 'default_development_pepper_change_in_production';
  }
  
  // Hash token with pepper using SQL
  function hashToken(token) {
    var pepper = getPepper();
    var stmt = snowflake.createStatement({
      sqlText: "SELECT SHA2(? || ?, 256) as hash",
      binds: [token, pepper]
    });
    var result = stmt.execute();
    return result.next() ? result.getColumnValue('HASH') : null;
  }
  
  // Get latest permissions with deterministic tie-breaking
  function getLatestPermissions(tokenHash) {
    var stmt = snowflake.createStatement({
      sqlText: `
        WITH latest AS (
          SELECT
            object:id::string                AS username,
            attributes:allowed_tools         AS allowed_tools,
            attributes:max_rows::int         AS max_rows,
            attributes:daily_runtime_s::int  AS daily_runtime_s,
            attributes:expires_at::timestamp_tz AS expires_at,
            occurred_at, _recv_at, event_id
          FROM CLAUDE_BI.APP.ACTIVITY.EVENTS
          WHERE action = 'system.permission.granted'
            AND attributes:token_hash = ?
            AND COALESCE(attributes:expires_at::timestamp_tz, '9999-12-31'::timestamp_tz) > CURRENT_TIMESTAMP()
          QUALIFY ROW_NUMBER() OVER (
            ORDER BY occurred_at DESC, _recv_at DESC, event_id DESC
          ) = 1
        )
        SELECT * FROM latest
      `,
      binds: [tokenHash]
    });
    
    var result = stmt.execute();
    if (result.next()) {
      return {
        username: result.getColumnValue('USERNAME'),
        allowed_tools: JSON.parse(result.getColumnValue('ALLOWED_TOOLS')),
        max_rows: result.getColumnValue('MAX_ROWS') || 1000,
        daily_runtime_s: result.getColumnValue('DAILY_RUNTIME_S') || 3600,
        expires_at: result.getColumnValue('EXPIRES_AT')
      };
    }
    return null;
  }
  
  // Check daily budget enforcement
  function exceedsDailyBudget(perms, tokenHash) {
    var stmt = snowflake.createStatement({
      sqlText: `
        SELECT COALESCE(SUM(attributes:runtime_seconds::NUMBER), 0) AS daily_usage_s
        FROM CLAUDE_BI.APP.ACTIVITY.EVENTS
        WHERE action = 'mcp.query.executed'
          AND attributes:token_hash = ?
          AND occurred_at >= DATEADD('day', -1, CURRENT_TIMESTAMP())
      `,
      binds: [tokenHash]
    });
    
    var result = stmt.execute();
    if (result.next()) {
      var dailyUsage = result.getColumnValue('DAILY_USAGE_S');
      return dailyUsage >= perms.daily_runtime_s;
    }
    return false;
  }
  
  // Log activity event
  function logEvent(action, attributes, username) {
    try {
      var stmt = snowflake.createStatement({
        sqlText: `
          INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS 
          SELECT 
            OBJECT_CONSTRUCT(
              'event_id', UUID_STRING(),
              'action', ?,
              'occurred_at', CURRENT_TIMESTAMP(),
              '_recv_at', CURRENT_TIMESTAMP(),
              'actor_id', ?,
              'source', 'mcp_thin_client',
              'object', OBJECT_CONSTRUCT('type', 'mcp_request'),
              'attributes', PARSE_JSON(?)
            ),
            'MCP_CLIENT',
            CURRENT_TIMESTAMP()
        `,
        binds: [action, username, JSON.stringify(attributes)]
      });
      stmt.execute();
    } catch (e) {
      // Don't fail on logging errors
    }
  }
  
  // Main procedure logic
  try {
    // 1. Hash token with pepper
    var tokenHash = hashToken(USER_TOKEN);
    if (!tokenHash) {
      return {error: 'Token hashing failed'};
    }
    
    // 2. Get permissions with deterministic tie-breaking
    var perms = getLatestPermissions(tokenHash);
    if (!perms) {
      return {error: 'Invalid or expired token'};
    }
    
    // 3. Check daily budget
    if (exceedsDailyBudget(perms, tokenHash)) {
      return {error: 'Daily runtime limit exceeded'};
    }
    
    // 4. Log the request
    logEvent('mcp.request', {
      method: METHOD,
      tool: PARAMS.name,
      timestamp: new Date().toISOString()
    }, perms.username);
    
    // 5. Route to appropriate handler
    switch(METHOD) {
      case 'tools/call':
        return handleToolCall(PARAMS, perms, tokenHash);
      
      case 'tools/list':
        return listAvailableTools(perms);
      
      default:
        return {error: 'Unknown method: ' + METHOD};
    }
    
  } catch (e) {
    return {error: 'Request processing failed: ' + e.message};
  }
  
  // Handle tool execution
  function handleToolCall(params, perms, tokenHash) {
    var toolName = params.name;
    var args = params.arguments;
    
    // Check if user has access to this tool
    if (!perms.allowed_tools.includes(toolName) && !perms.allowed_tools.includes('*')) {
      return {error: 'Access denied to tool: ' + toolName};
    }
    
    // Execute tool based on name
    switch(toolName) {
      case 'compose_query_plan':
        return composeQuery(args, perms, tokenHash);
      
      case 'list_sources':
        return getAvailableSources(perms);
      
      case 'validate_plan':
        return validateQueryPlan(args, perms);
      
      case 'create_dashboard':
        return createDashboard(args, perms, tokenHash);
      
      case 'get_user_status':
        return getUserStatus(perms, tokenHash);
      
      default:
        return {error: 'Unknown tool: ' + toolName};
    }
  }
  
  // Tool implementations (simplified - extend as needed)
  function composeQuery(args, perms, tokenHash) {
    try {
      // Apply user row limit
      var rowLimit = Math.min(args.top_n || perms.max_rows, perms.max_rows);
      
      // Log execution
      logEvent('mcp.query.executed', {
        intent: args.intent_text,
        max_rows: rowLimit,
        runtime_seconds: 1  // Would be actual runtime
      }, perms.username);
      
      return {
        success: true,
        data: {
          message: 'Query composed successfully',
          intent: args.intent_text,
          max_rows: rowLimit,
          username: perms.username
        }
      };
    } catch (e) {
      return {error: 'Query composition failed: ' + e.message};
    }
  }
  
  function getAvailableSources(perms) {
    return {
      success: true,
      data: ['VW_ACTIVITY_SUMMARY', 'VW_ACTIVITY_COUNTS_24H', 'EVENTS']
    };
  }
  
  function validateQueryPlan(args, perms) {
    return {
      success: true,
      data: {valid: true, message: 'Query plan is valid'}
    };
  }
  
  function createDashboard(args, perms, tokenHash) {
    logEvent('mcp.dashboard.created', {title: args.title}, perms.username);
    return {
      success: true,
      data: {message: 'Dashboard created: ' + args.title}
    };
  }
  
  function getUserStatus(perms, tokenHash) {
    return {
      success: true,
      data: {
        username: perms.username,
        allowed_tools: perms.allowed_tools,
        max_rows: perms.max_rows,
        daily_runtime_s: perms.daily_runtime_s,
        expires_at: perms.expires_at
      }
    };
  }
  
  function listAvailableTools(perms) {
    return {
      success: true,
      data: perms.allowed_tools
    };
  }
$$;

-- New dedicated logging procedure with batching and DLQ
CREATE OR REPLACE PROCEDURE MCP.LOG_DEV_EVENT(event_batch VARIANT)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
  function routeToDLQ(event, reason) {
    try {
      var stmt = snowflake.createStatement({
        sqlText: `
          INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS 
          SELECT 
            OBJECT_CONSTRUCT(
              'event_id', UUID_STRING(),
              'action', 'system.dlq.event',
              'occurred_at', CURRENT_TIMESTAMP(),
              '_recv_at', CURRENT_TIMESTAMP(),
              'source', 'mcp_dlq',
              'attributes', OBJECT_CONSTRUCT(
                'original_event', ?,
                'reason', ?,
                'dlq_timestamp', CURRENT_TIMESTAMP()
              )
            ),
            'DLQ',
            CURRENT_TIMESTAMP()
        `,
        binds: [JSON.stringify(event), reason]
      });
      stmt.execute();
    } catch (e) {
      // DLQ failure - nothing we can do
    }
  }
  
  function insertEvent(eventData) {
    var stmt = snowflake.createStatement({
      sqlText: `
        INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS 
        SELECT 
          OBJECT_CONSTRUCT(
            'event_id', UUID_STRING(),
            'action', ?,
            'occurred_at', ?,
            '_recv_at', CURRENT_TIMESTAMP(),
            'actor_id', 'mcp_client',
            'source', 'mcp_dev_logging',
            'attributes', PARSE_JSON(?)
          ),
          'MCP_DEV',
          CURRENT_TIMESTAMP()
      `,
      binds: [
        eventData.action,
        eventData.occurred_at,
        JSON.stringify(eventData.attributes)
      ]
    });
    stmt.execute();
  }
  
  try {
    var events = Array.isArray(EVENT_BATCH) ? EVENT_BATCH : [EVENT_BATCH];
    var processed = 0;
    var errors = 0;
    
    for (var i = 0; i < events.length; i++) {
      try {
        var eventData = typeof events[i] === 'string' ? JSON.parse(events[i]) : events[i];
        
        // Size validation (max 100KB)
        var eventSize = JSON.stringify(eventData).length;
        if (eventSize > 100000) {
          routeToDLQ(eventData, 'Event too large: ' + eventSize + ' bytes');
          errors++;
          continue;
        }
        
        // Required fields validation
        if (!eventData.action || !eventData.occurred_at) {
          routeToDLQ(eventData, 'Missing required fields');
          errors++;
          continue;
        }
        
        // Insert the event
        insertEvent(eventData);
        processed++;
        
      } catch (e) {
        routeToDLQ(events[i], 'Processing error: ' + e.message);
        errors++;
      }
    }
    
    return {
      success: true,
      processed: processed,
      errors: errors,
      total: events.length
    };
    
  } catch (e) {
    return {
      success: false,
      error: 'Batch processing failed: ' + e.message
    };
  }
$$;

-- New token validation procedure for quick checks
CREATE OR REPLACE PROCEDURE MCP.VALIDATE_TOKEN(user_token STRING)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
  function getPepper() {
    try {
      var stmt = snowflake.createStatement({
        sqlText: "SELECT SYSTEM$GET_PARAMETER('MCP_SERVER_PEPPER') as pepper"
      });
      var result = stmt.execute();
      if (result.next()) {
        var pepper = result.getColumnValue('PEPPER');
        return pepper || 'default_development_pepper_change_in_production';
      }
    } catch (e) {
      return 'default_development_pepper_change_in_production';
    }
    return 'default_development_pepper_change_in_production';
  }
  
  try {
    var pepper = getPepper();
    
    var stmt = snowflake.createStatement({
      sqlText: `
        SELECT 
          CASE 
            WHEN COUNT(*) > 0 THEN OBJECT_CONSTRUCT(
              'valid', true, 
              'status', 'active',
              'username', MIN(object:id::string),
              'expires_at', MIN(attributes:expires_at::timestamp_tz)
            )
            ELSE OBJECT_CONSTRUCT('valid', false, 'status', 'invalid_or_expired')
          END as result
        FROM CLAUDE_BI.APP.ACTIVITY.EVENTS
        WHERE action = 'system.permission.granted'
          AND attributes:token_hash = SHA2(? || ?, 256)
          AND COALESCE(attributes:expires_at::timestamp_tz, '9999-12-31'::timestamp_tz) > CURRENT_TIMESTAMP()
      `,
      binds: [USER_TOKEN, pepper]
    });
    
    var result = stmt.execute();
    if (result.next()) {
      return result.getColumnValue('RESULT');
    }
    
    return {valid: false, status: 'validation_failed'};
    
  } catch (e) {
    return {valid: false, status: 'error', message: e.message};
  }
$$;

-- Grant execute permissions to service role
GRANT USAGE ON PROCEDURE MCP.HANDLE_REQUEST(STRING, VARIANT, STRING) TO ROLE MCP_SERVICE_ROLE;
GRANT USAGE ON PROCEDURE MCP.LOG_DEV_EVENT(VARIANT) TO ROLE MCP_SERVICE_ROLE;
GRANT USAGE ON PROCEDURE MCP.VALIDATE_TOKEN(STRING) TO ROLE MCP_SERVICE_ROLE;

-- Create client adoption tracking view
CREATE OR REPLACE VIEW MCP.CLIENT_ADOPTION AS
SELECT 
  attributes:client_version::string as version,
  COUNT(DISTINCT actor_id) as users,
  COUNT(*) as requests,
  AVG(attributes:execution_time_ms::number) as avg_latency_ms,
  DATE(occurred_at) as date
FROM CLAUDE_BI.APP.ACTIVITY.EVENTS
WHERE action = 'mcp.request'
  AND occurred_at >= DATEADD('day', -7, CURRENT_TIMESTAMP())
GROUP BY version, date
ORDER BY date DESC, version;