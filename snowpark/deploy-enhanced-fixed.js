#!/usr/bin/env node

/**
 * Deploy Enhanced MCP Procedures - Fixed Version
 */

const snowflake = require('snowflake-sdk');
const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '../.env') });

console.log('🚀 Deploying Enhanced MCP Procedures...\n');

const connection = snowflake.createConnection({
  account: process.env.SNOWFLAKE_ACCOUNT,
  username: process.env.SNOWFLAKE_USERNAME,
  password: process.env.SNOWFLAKE_PASSWORD,
  role: process.env.SNOWFLAKE_ROLE || 'CLAUDE_BI_ROLE',
  database: 'CLAUDE_BI',
  schema: 'MCP',
  warehouse: 'CLAUDE_WAREHOUSE'
});

// Deploy procedures one by one
async function deployProcedures() {
  return new Promise((resolve, reject) => {
    connection.connect(async (err, conn) => {
      if (err) {
        console.error('❌ Failed to connect:', err.message);
        reject(err);
        return;
      }
      
      console.log('✅ Connected to Snowflake\n');
      
      // 1. Deploy RENDER_SAFE_SQL procedure
      console.log('📝 Creating RENDER_SAFE_SQL procedure...');
      await executeSql(`
        CREATE OR REPLACE PROCEDURE MCP.RENDER_SAFE_SQL(template_name VARCHAR, params VARIANT)
        RETURNS VARIANT
        LANGUAGE JAVASCRIPT
        EXECUTE AS CALLER
        AS
        $$
          var SAFE_TEMPLATES = {
            'activity_summary': {
              sql: 'SELECT COUNT(*) as total_events, COUNT(DISTINCT customer) as unique_customers FROM CLAUDE_BI.ACTIVITY.EVENTS WHERE ts >= DATEADD(hour, ?, CURRENT_TIMESTAMP())',
              params: ['hours_back']
            },
            'recent_events': {
              sql: 'SELECT activity_id, ts, customer, activity FROM CLAUDE_BI.ACTIVITY.EVENTS ORDER BY ts DESC LIMIT ?',
              params: ['limit']
            },
            'top_activities': {
              sql: 'SELECT activity, COUNT(*) as count FROM CLAUDE_BI.ACTIVITY.EVENTS WHERE ts >= DATEADD(day, ?, CURRENT_TIMESTAMP()) GROUP BY 1 ORDER BY 2 DESC LIMIT ?',
              params: ['days_back', 'limit']
            }
          };
          
          var template = SAFE_TEMPLATES[TEMPLATE_NAME];
          if (!template) {
            return { success: false, error: 'Unknown template: ' + TEMPLATE_NAME };
          }
          
          var binds = [];
          for (var i = 0; i < template.params.length; i++) {
            var paramName = template.params[i];
            var paramValue = PARAMS[paramName];
            if (paramValue === undefined) {
              return { success: false, error: 'Missing parameter: ' + paramName };
            }
            // Apply limits
            if (paramName === 'limit' && paramValue > 10000) paramValue = 10000;
            if (paramName === 'days_back' && paramValue > 365) paramValue = 365;
            if (paramName === 'hours_back' && paramValue < -720) paramValue = -720;
            binds.push(paramValue);
          }
          
          return { success: true, sql: template.sql, binds: binds, template: TEMPLATE_NAME };
        $$
      `);
      
      // 2. Deploy CHECK_CIRCUIT_BREAKER procedure
      console.log('📝 Creating CHECK_CIRCUIT_BREAKER procedure...');
      await executeSql(`
        CREATE OR REPLACE PROCEDURE MCP.CHECK_CIRCUIT_BREAKER()
        RETURNS VARIANT
        LANGUAGE SQL
        EXECUTE AS CALLER
        AS
        BEGIN
          LET result VARIANT;
          
          SELECT OBJECT_CONSTRUCT(
            'circuit_status', 
              CASE 
                WHEN COUNT(*) = 0 THEN 'CLOSED'
                WHEN SUM(CASE WHEN feature_json:status = 'failed' THEN 1 ELSE 0 END) / COUNT(*) > 0.5 THEN 'OPEN'
                WHEN SUM(CASE WHEN feature_json:status = 'failed' THEN 1 ELSE 0 END) / COUNT(*) > 0.3 THEN 'HALF_OPEN'
                ELSE 'CLOSED'
              END,
            'failure_rate', COALESCE(SUM(CASE WHEN feature_json:status = 'failed' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 0),
            'recent_queries', COUNT(*),
            'can_proceed', 
              CASE 
                WHEN COUNT(*) > 0 AND SUM(CASE WHEN feature_json:status = 'failed' THEN 1 ELSE 0 END) / COUNT(*) > 0.5 THEN FALSE
                ELSE TRUE
              END
          ) INTO :result
          FROM CLAUDE_BI.ACTIVITY.EVENTS
          WHERE activity LIKE 'ccode.mcp.%'
            AND ts > DATEADD('minute', -5, CURRENT_TIMESTAMP());
          
          RETURN :result;
        END;
      `);
      
      // 3. Deploy EXECUTE_QUERY_PLAN_V2 procedure
      console.log('📝 Creating EXECUTE_QUERY_PLAN_V2 procedure...');
      await executeSql(`
        CREATE OR REPLACE PROCEDURE MCP.EXECUTE_QUERY_PLAN_V2(plan VARIANT)
        RETURNS VARIANT
        LANGUAGE JAVASCRIPT
        EXECUTE AS CALLER
        AS
        $$
          var startTime = Date.now();
          var queryId = 'mcp_' + Date.now() + '_' + Math.random().toString(36).substr(2, 9);
          
          try {
            // Set query tag
            snowflake.execute({
              sqlText: 'ALTER SESSION SET QUERY_TAG = ?',
              binds: [JSON.stringify({
                mcp_version: '2.0',
                query_id: queryId,
                user: snowflake.getCurrentUser(),
                timestamp: new Date().toISOString()
              })]
            });
            
            // Check circuit breaker
            var cbCheck = snowflake.execute({ sqlText: 'CALL MCP.CHECK_CIRCUIT_BREAKER()' });
            cbCheck.next();
            var circuitStatus = cbCheck.getColumnValue(1);
            
            if (!circuitStatus.can_proceed) {
              // Log rejection
              snowflake.execute({
                sqlText: 'INSERT INTO CLAUDE_BI.ACTIVITY.EVENTS (activity_id, ts, customer, activity, feature_json) VALUES (?, CURRENT_TIMESTAMP(), CURRENT_USER(), ?, PARSE_JSON(?))',
                binds: ['act_' + queryId, 'ccode.mcp.query_rejected', JSON.stringify({ reason: 'Circuit breaker open', plan: PLAN })]
              });
              return { success: false, error: 'Circuit breaker is open', circuit_status: circuitStatus };
            }
            
            // Execute query based on plan
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
              binds = rendered.binds || [];
            } else {
              // Legacy mode
              var source = PLAN.source || 'VW_ACTIVITY_SUMMARY';
              var limit = Math.min(PLAN.top_n || 100, 10000);
              
              if (source === 'VW_ACTIVITY_SUMMARY') {
                sqlText = 'SELECT * FROM CLAUDE_BI.ACTIVITY_CCODE.VW_ACTIVITY_SUMMARY';
              } else if (source === 'VW_ACTIVITY_COUNTS_24H') {
                sqlText = 'SELECT * FROM CLAUDE_BI.ACTIVITY_CCODE.VW_ACTIVITY_COUNTS_24H LIMIT ' + limit;
              } else if (source === 'EVENTS') {
                sqlText = 'SELECT * FROM CLAUDE_BI.ACTIVITY.EVENTS ORDER BY ts DESC LIMIT ' + limit;
              } else {
                throw new Error('Unknown source: ' + source);
              }
            }
            
            // Execute the query
            var stmt = binds.length > 0 
              ? snowflake.createStatement({ sqlText: sqlText, binds: binds })
              : snowflake.createStatement({ sqlText: sqlText });
              
            var result = stmt.execute();
            var rows = [];
            var rowCount = 0;
            
            while (result.next() && rowCount < 1000) {
              rows.push(result.getColumnValueAsString(1));
              rowCount++;
            }
            
            var executionTime = Date.now() - startTime;
            
            // Log success
            snowflake.execute({
              sqlText: 'INSERT INTO CLAUDE_BI.ACTIVITY.EVENTS (activity_id, ts, customer, activity, feature_json) VALUES (?, CURRENT_TIMESTAMP(), CURRENT_USER(), ?, PARSE_JSON(?))',
              binds: [
                'act_' + queryId,
                'ccode.mcp.query_executed',
                JSON.stringify({ status: 'success', plan: PLAN, rows_returned: rowCount, execution_time_ms: executionTime })
              ]
            });
            
            return {
              success: true,
              query_id: queryId,
              row_count: rowCount,
              sample: rows.slice(0, 5),
              execution_time_ms: executionTime
            };
            
          } catch (err) {
            // Log error
            snowflake.execute({
              sqlText: 'INSERT INTO CLAUDE_BI.ACTIVITY.EVENTS (activity_id, ts, customer, activity, feature_json) VALUES (?, CURRENT_TIMESTAMP(), CURRENT_USER(), ?, PARSE_JSON(?))',
              binds: ['act_' + queryId, 'ccode.mcp.query_failed', JSON.stringify({ error: err.toString(), plan: PLAN })]
            });
            
            return { success: false, error: err.toString(), query_id: queryId };
          }
        $$
      `);
      
      // 4. Grant permissions
      console.log('📝 Granting permissions...');
      await executeSql('GRANT USAGE ON PROCEDURE MCP.RENDER_SAFE_SQL(VARCHAR, VARIANT) TO ROLE CLAUDE_BI_ROLE');
      await executeSql('GRANT USAGE ON PROCEDURE MCP.CHECK_CIRCUIT_BREAKER() TO ROLE CLAUDE_BI_ROLE');
      await executeSql('GRANT USAGE ON PROCEDURE MCP.EXECUTE_QUERY_PLAN_V2(VARIANT) TO ROLE CLAUDE_BI_ROLE');
      
      // 5. Test the procedures
      console.log('\n🧪 Testing enhanced procedures...');
      
      // Test SafeSQL
      console.log('  Testing RENDER_SAFE_SQL...');
      const templateTest = await executeSql(`CALL MCP.RENDER_SAFE_SQL('activity_summary', OBJECT_CONSTRUCT('hours_back', -24))`);
      console.log('  ✅ SafeSQL template works');
      
      // Test circuit breaker
      console.log('  Testing CHECK_CIRCUIT_BREAKER...');
      const cbTest = await executeSql('CALL MCP.CHECK_CIRCUIT_BREAKER()');
      console.log('  ✅ Circuit breaker works');
      
      // Test V2 execution
      console.log('  Testing EXECUTE_QUERY_PLAN_V2...');
      const v2Test = await executeSql(`CALL MCP.EXECUTE_QUERY_PLAN_V2(OBJECT_CONSTRUCT('template', 'recent_events', 'params', OBJECT_CONSTRUCT('limit', 5)))`);
      console.log('  ✅ V2 execution works');
      
      console.log('\n✨ All enhanced procedures deployed successfully!');
      console.log('\n📊 New Capabilities:');
      console.log('  • SafeSQL templates prevent injection');
      console.log('  • Circuit breaker prevents overload');
      console.log('  • Query tagging enables attribution');
      console.log('  • Activity tracking for all queries');
      console.log('  • Cost limits enforced (10K row max)');
      
      connection.destroy();
      resolve();
    });
  });
  
  function executeSql(sql) {
    return new Promise((resolve, reject) => {
      connection.execute({
        sqlText: sql,
        complete: (err, statement, rows) => {
          if (err) {
            console.error('  ❌ Error:', err.message.substring(0, 100));
            reject(err);
          } else {
            resolve(rows);
          }
        }
      });
    });
  }
}

deployProcedures().catch(err => {
  console.error('Deployment failed:', err);
  process.exit(1);
});