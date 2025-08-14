-- ============================================================================
-- Enhanced MCP Procedures with Security & Monitoring
-- Implements SafeSQL templates, cost prediction, circuit breaker, and tracking
-- ============================================================================

USE DATABASE CLAUDE_BI;
USE SCHEMA MCP;

-- ============================================================================
-- 1. SafeSQL Template Rendering Procedure
-- ============================================================================

CREATE OR REPLACE PROCEDURE MCP.RENDER_SAFE_SQL(template_name VARCHAR, params VARIANT)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
  // SafeSQL templates - only these patterns are allowed
  var SAFE_TEMPLATES = {
    'activity_summary': {
      sql: `SELECT 
              COUNT(*) as total_events,
              COUNT(DISTINCT customer) as unique_customers,
              COUNT(DISTINCT activity) as unique_activities,
              MAX(ts) as last_event
            FROM CLAUDE_BI.ACTIVITY.EVENTS
            WHERE ts >= DATEADD('hour', ?, CURRENT_TIMESTAMP())`,
      params: ['hours_back']
    },
    
    'activity_time_series': {
      sql: `SELECT 
              DATE_TRUNC(?, ts) as period,
              activity,
              COUNT(*) as event_count,
              COUNT(DISTINCT customer) as unique_customers
            FROM CLAUDE_BI.ACTIVITY.EVENTS
            WHERE ts >= DATEADD('day', ?, CURRENT_TIMESTAMP())
              AND activity LIKE ?
            GROUP BY 1, 2
            ORDER BY 1 DESC
            LIMIT ?`,
      params: ['grain', 'days_back', 'activity_pattern', 'limit']
    },
    
    'recent_events': {
      sql: `SELECT 
              activity_id,
              ts,
              customer,
              activity,
              feature_json
            FROM CLAUDE_BI.ACTIVITY.EVENTS
            WHERE ts >= DATEADD('hour', ?, CURRENT_TIMESTAMP())
            ORDER BY ts DESC
            LIMIT ?`,
      params: ['hours_back', 'limit']
    },
    
    'top_activities': {
      sql: `SELECT 
              activity,
              COUNT(*) as event_count,
              COUNT(DISTINCT customer) as unique_customers
            FROM CLAUDE_BI.ACTIVITY.EVENTS
            WHERE ts >= DATEADD('day', ?, CURRENT_TIMESTAMP())
            GROUP BY 1
            ORDER BY 2 DESC
            LIMIT ?`,
      params: ['days_back', 'limit']
    },
    
    'customer_activity': {
      sql: `SELECT 
              customer,
              activity,
              COUNT(*) as event_count,
              MIN(ts) as first_seen,
              MAX(ts) as last_seen
            FROM CLAUDE_BI.ACTIVITY.EVENTS
            WHERE customer = ?
              AND ts >= DATEADD('day', ?, CURRENT_TIMESTAMP())
            GROUP BY 1, 2
            ORDER BY 3 DESC
            LIMIT ?`,
      params: ['customer_id', 'days_back', 'limit']
    }
  };
  
  // Get template
  var template = SAFE_TEMPLATES[TEMPLATE_NAME];
  if (!template) {
    return {
      success: false,
      error: 'Unknown template: ' + TEMPLATE_NAME
    };
  }
  
  // Validate parameters
  var binds = [];
  for (var i = 0; i < template.params.length; i++) {
    var paramName = template.params[i];
    var paramValue = PARAMS[paramName];
    
    if (paramValue === undefined || paramValue === null) {
      return {
        success: false,
        error: 'Missing parameter: ' + paramName
      };
    }
    
    // Apply safety limits
    if (paramName === 'limit' && paramValue > 10000) {
      paramValue = 10000;
    }
    if (paramName === 'days_back' && paramValue > 365) {
      paramValue = 365;
    }
    if (paramName === 'hours_back' && paramValue > 720) {
      paramValue = 720;
    }
    
    binds.push(paramValue);
  }
  
  return {
    success: true,
    sql: template.sql,
    binds: binds,
    template: TEMPLATE_NAME
  };
$$;

-- ============================================================================
-- 2. Query Cost Prediction Procedure
-- ============================================================================

CREATE OR REPLACE PROCEDURE MCP.ESTIMATE_QUERY_COST(sql_text VARCHAR)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
  try {
    // Get execution plan without running query
    var explainStmt = snowflake.createStatement({
      sqlText: 'EXPLAIN USING JSON ' + SQL_TEXT
    });
    
    var planResult = explainStmt.execute();
    planResult.next();
    var plan = JSON.parse(planResult.getColumnValue(1));
    
    // Extract metrics from plan
    var totalBytes = 0;
    var partitionsTotal = 0;
    var partitionsPruned = 0;
    
    // Parse the execution plan tree
    if (plan && plan.Operations) {
      for (var i = 0; i < plan.Operations.length; i++) {
        var op = plan.Operations[i];
        if (op['Bytes Scanned']) {
          totalBytes += parseInt(op['Bytes Scanned']) || 0;
        }
        if (op['Partitions Total']) {
          partitionsTotal += parseInt(op['Partitions Total']) || 0;
        }
        if (op['Partitions Scanned']) {
          var scanned = parseInt(op['Partitions Scanned']) || 0;
          partitionsPruned += (partitionsTotal - scanned);
        }
      }
    }
    
    // Calculate estimated cost
    // Snowflake charges ~$0.00123 per TB scanned
    var tbScanned = totalBytes / (1024 * 1024 * 1024 * 1024);
    var estimatedCredits = tbScanned * 0.00123;
    var estimatedCost = estimatedCredits * 4; // Assuming $4 per credit
    
    // Check against limits
    var maxCreditsPerQuery = 0.1; // 0.1 credits max per query
    var wouldExceedLimit = estimatedCredits > maxCreditsPerQuery;
    
    return {
      success: true,
      estimated_bytes: totalBytes,
      estimated_tb: tbScanned.toFixed(6),
      estimated_credits: estimatedCredits.toFixed(6),
      estimated_cost_usd: estimatedCost.toFixed(4),
      partitions_total: partitionsTotal,
      partitions_pruned: partitionsPruned,
      pruning_efficiency: partitionsTotal > 0 ? (partitionsPruned / partitionsTotal * 100).toFixed(1) + '%' : 'N/A',
      would_exceed_limit: wouldExceedLimit,
      max_credits_allowed: maxCreditsPerQuery
    };
    
  } catch (err) {
    return {
      success: false,
      error: err.toString()
    };
  }
$$;

-- ============================================================================
-- 3. Circuit Breaker Check Procedure
-- ============================================================================

CREATE OR REPLACE PROCEDURE MCP.CHECK_CIRCUIT_BREAKER()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
BEGIN
  -- Check failure rate and queue depth from recent activity
  LET result VARIANT;
  
  SELECT OBJECT_CONSTRUCT(
    'circuit_status', 
      CASE 
        WHEN failure_rate > 0.5 THEN 'OPEN'
        WHEN failure_rate > 0.3 THEN 'HALF_OPEN'
        WHEN pending_queries > 100 THEN 'THROTTLED'
        ELSE 'CLOSED'
      END,
    'failure_rate', failure_rate,
    'pending_queries', pending_queries,
    'recent_errors', recent_errors,
    'can_proceed', 
      CASE 
        WHEN failure_rate > 0.5 OR pending_queries > 100 THEN FALSE
        ELSE TRUE
      END,
    'message',
      CASE
        WHEN failure_rate > 0.5 THEN 'Circuit open due to high failure rate'
        WHEN pending_queries > 100 THEN 'Too many pending queries'
        ELSE 'System healthy'
      END
  ) INTO :result
  FROM (
    SELECT 
      COALESCE(
        SUM(CASE WHEN feature_json:status = 'failed' THEN 1 ELSE 0 END) / 
        NULLIF(COUNT(*), 0), 
        0
      ) as failure_rate,
      SUM(CASE WHEN feature_json:status = 'pending' THEN 1 ELSE 0 END) as pending_queries,
      SUM(CASE WHEN feature_json:status = 'failed' THEN 1 ELSE 0 END) as recent_errors
    FROM CLAUDE_BI.ACTIVITY.EVENTS
    WHERE activity LIKE 'ccode.mcp.%'
      AND ts > DATEADD('minute', -5, CURRENT_TIMESTAMP())
  );
  
  RETURN :result;
END;

-- ============================================================================
-- 4. Enhanced Execute Query Plan with Tracking
-- ============================================================================

CREATE OR REPLACE PROCEDURE MCP.EXECUTE_QUERY_PLAN_V2(plan VARIANT)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
  var startTime = Date.now();
  var queryId = 'mcp_' + Date.now() + '_' + Math.random().toString(36).substr(2, 9);
  
  try {
    // Set query tag for attribution
    snowflake.execute({
      sqlText: 'ALTER SESSION SET QUERY_TAG = ?',
      binds: [JSON.stringify({
        mcp_version: '2.0',
        query_id: queryId,
        plan_source: PLAN.source || 'unknown',
        template: PLAN.template || 'direct',
        user: snowflake.getCurrentUser(),
        timestamp: new Date().toISOString()
      })]
    });
    
    // Check circuit breaker
    var cbCheck = snowflake.execute({
      sqlText: 'CALL MCP.CHECK_CIRCUIT_BREAKER()'
    });
    cbCheck.next();
    var circuitStatus = cbCheck.getColumnValue(1);
    
    if (!circuitStatus.can_proceed) {
      // Log circuit breaker rejection
      snowflake.execute({
        sqlText: `INSERT INTO CLAUDE_BI.ACTIVITY.EVENTS 
                  (activity_id, ts, customer, activity, feature_json, _source_system)
                  SELECT ?, CURRENT_TIMESTAMP(), CURRENT_USER(), ?, PARSE_JSON(?), ?`,
        binds: [
          'act_' + queryId,
          'ccode.mcp.query_rejected',
          JSON.stringify({
            reason: circuitStatus.message,
            circuit_status: circuitStatus.circuit_status,
            plan: PLAN
          }),
          'mcp_v2'
        ]
      });
      
      return {
        success: false,
        error: 'Circuit breaker: ' + circuitStatus.message,
        circuit_status: circuitStatus
      };
    }
    
    // Determine SQL to execute
    var sqlText = '';
    var binds = [];
    
    if (PLAN.template) {
      // Use SafeSQL template
      var renderResult = snowflake.execute({
        sqlText: 'CALL MCP.RENDER_SAFE_SQL(?, ?)',
        binds: [PLAN.template, PLAN.params || {}]
      });
      renderResult.next();
      var rendered = renderResult.getColumnValue(1);
      
      if (!rendered.success) {
        throw new Error('Template error: ' + rendered.error);
      }
      
      sqlText = rendered.sql;
      binds = rendered.binds;
      
    } else {
      // Legacy direct source mode (backward compatibility)
      var source = PLAN.source || 'VW_ACTIVITY_SUMMARY';
      var limit = Math.min(PLAN.top_n || 100, 10000);
      
      if (source === 'VW_ACTIVITY_SUMMARY') {
        sqlText = 'SELECT * FROM CLAUDE_BI.ACTIVITY_CCODE.VW_ACTIVITY_SUMMARY';
      } else if (source === 'VW_ACTIVITY_COUNTS_24H') {
        sqlText = 'SELECT * FROM CLAUDE_BI.ACTIVITY_CCODE.VW_ACTIVITY_COUNTS_24H LIMIT ?';
        binds = [limit];
      } else if (source === 'EVENTS') {
        sqlText = 'SELECT * FROM CLAUDE_BI.ACTIVITY.EVENTS ORDER BY ts DESC LIMIT ?';
        binds = [limit];
      } else {
        throw new Error('Unknown source: ' + source);
      }
    }
    
    // Estimate cost before execution
    var costCheck = snowflake.execute({
      sqlText: 'CALL MCP.ESTIMATE_QUERY_COST(?)',
      binds: [sqlText]
    });
    costCheck.next();
    var costEstimate = costCheck.getColumnValue(1);
    
    if (costEstimate.would_exceed_limit) {
      throw new Error('Query would exceed cost limit: ' + costEstimate.estimated_credits + ' credits');
    }
    
    // Execute the query
    var stmt = snowflake.createStatement({
      sqlText: sqlText,
      binds: binds
    });
    
    var result = stmt.execute();
    var rows = [];
    var columnCount = result.getColumnCount();
    var columnNames = [];
    
    // Get column names
    for (var i = 1; i <= columnCount; i++) {
      columnNames.push(result.getColumnName(i));
    }
    
    // Collect rows (limit to 1000 for response size)
    var rowCount = 0;
    while (result.next() && rowCount < 1000) {
      var row = {};
      for (var j = 1; j <= columnCount; j++) {
        row[columnNames[j-1]] = result.getColumnValue(j);
      }
      rows.push(row);
      rowCount++;
    }
    
    var executionTime = Date.now() - startTime;
    
    // Log successful execution
    snowflake.execute({
      sqlText: `INSERT INTO CLAUDE_BI.ACTIVITY.EVENTS 
                (activity_id, ts, customer, activity, feature_json, _source_system, _query_tag)
                SELECT ?, CURRENT_TIMESTAMP(), CURRENT_USER(), ?, PARSE_JSON(?), ?, ?`,
      binds: [
        'act_' + queryId,
        'ccode.mcp.query_executed',
        JSON.stringify({
          status: 'success',
          plan: PLAN,
          template: PLAN.template || null,
          rows_returned: rowCount,
          execution_time_ms: executionTime,
          cost_estimate: costEstimate,
          query_id: queryId
        }),
        'mcp_v2',
        queryId
      ]
    });
    
    return {
      success: true,
      query_id: queryId,
      rows: rows,
      row_count: rowCount,
      columns: columnNames,
      execution_time_ms: executionTime,
      cost_estimate: costEstimate
    };
    
  } catch (err) {
    // Log error
    snowflake.execute({
      sqlText: `INSERT INTO CLAUDE_BI.ACTIVITY.EVENTS 
                (activity_id, ts, customer, activity, feature_json, _source_system)
                SELECT ?, CURRENT_TIMESTAMP(), CURRENT_USER(), ?, PARSE_JSON(?), ?`,
      binds: [
        'act_' + queryId,
        'ccode.mcp.query_failed',
        JSON.stringify({
          status: 'failed',
          error: err.toString(),
          plan: PLAN,
          execution_time_ms: Date.now() - startTime
        }),
        'mcp_v2'
      ]
    });
    
    return {
      success: false,
      error: err.toString(),
      query_id: queryId
    };
  }
$$;

-- ============================================================================
-- 5. Grant Permissions
-- ============================================================================

GRANT USAGE ON PROCEDURE MCP.RENDER_SAFE_SQL(VARCHAR, VARIANT) TO ROLE CLAUDE_BI_ROLE;
GRANT USAGE ON PROCEDURE MCP.ESTIMATE_QUERY_COST(VARCHAR) TO ROLE CLAUDE_BI_ROLE;
GRANT USAGE ON PROCEDURE MCP.CHECK_CIRCUIT_BREAKER() TO ROLE CLAUDE_BI_ROLE;
GRANT USAGE ON PROCEDURE MCP.EXECUTE_QUERY_PLAN_V2(VARIANT) TO ROLE CLAUDE_BI_ROLE;

-- ============================================================================
-- 6. Test the Enhanced Procedures
-- ============================================================================

-- Test SafeSQL template rendering
CALL MCP.RENDER_SAFE_SQL('activity_summary', OBJECT_CONSTRUCT('hours_back', -24));

-- Test cost estimation
CALL MCP.ESTIMATE_QUERY_COST('SELECT * FROM CLAUDE_BI.ACTIVITY.EVENTS LIMIT 100');

-- Test circuit breaker
CALL MCP.CHECK_CIRCUIT_BREAKER();

-- Test enhanced execution with template
CALL MCP.EXECUTE_QUERY_PLAN_V2(OBJECT_CONSTRUCT(
  'template', 'recent_events',
  'params', OBJECT_CONSTRUCT(
    'hours_back', -24,
    'limit', 10
  )
));

-- Test backward compatibility
CALL MCP.EXECUTE_QUERY_PLAN_V2(OBJECT_CONSTRUCT(
  'source', 'VW_ACTIVITY_SUMMARY'
));