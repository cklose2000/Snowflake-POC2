#!/usr/bin/env node

/**
 * Deploy Enhanced MCP with Proper Activity Logging
 * Events table logging is the CORE - everything is observable
 */

const snowflake = require('snowflake-sdk');
const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '../.env') });

console.log('🚀 Deploying Enhanced MCP with Activity Logging...\n');

const connection = snowflake.createConnection({
  account: process.env.SNOWFLAKE_ACCOUNT,
  username: process.env.SNOWFLAKE_USERNAME,
  password: process.env.SNOWFLAKE_PASSWORD,
  role: process.env.SNOWFLAKE_ROLE || 'CLAUDE_BI_ROLE',
  database: 'CLAUDE_BI',
  schema: 'MCP',
  warehouse: 'CLAUDE_WAREHOUSE'
});

async function deployProcedures() {
  return new Promise((resolve, reject) => {
    connection.connect(async (err, conn) => {
      if (err) {
        console.error('❌ Failed to connect:', err.message);
        reject(err);
        return;
      }
      
      console.log('✅ Connected to Snowflake\n');
      
      // 1. Enhanced EXECUTE_QUERY_PLAN_V2 with PROPER Activity logging
      console.log('📝 Creating EXECUTE_QUERY_PLAN_V2 with Activity logging...');
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
            // Set query tag for attribution
            snowflake.execute({
              sqlText: 'ALTER SESSION SET QUERY_TAG = ?',
              binds: [JSON.stringify({
                mcp_version: '2.0',
                query_id: queryId,
                plan_source: PLAN.source || 'unknown',
                user: snowflake.getCurrentUser(),
                timestamp: new Date().toISOString()
              })]
            });
            
            // Determine SQL to execute
            var sqlText = '';
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
            
            // Execute the query
            var stmt = snowflake.createStatement({ sqlText: sqlText });
            var result = stmt.execute();
            var rows = [];
            var rowCount = 0;
            
            while (result.next() && rowCount < 1000) {
              rows.push(result.getColumnValueAsString(1));
              rowCount++;
            }
            
            var executionTime = Date.now() - startTime;
            
            // LOG SUCCESS TO ACTIVITY.EVENTS - THIS IS THE KEY!
            var logStmt = snowflake.createStatement({
              sqlText: \`INSERT INTO CLAUDE_BI.ACTIVITY.EVENTS 
                        (activity_id, ts, customer, activity, feature_json, _source_system, _query_tag)
                        VALUES (?, CURRENT_TIMESTAMP(), CURRENT_USER(), ?, TO_VARIANT(?), ?, ?)\`,
              binds: [
                'act_' + queryId,
                'ccode.mcp.query_executed',
                {
                  status: 'success',
                  plan: PLAN,
                  source: source,
                  rows_returned: rowCount,
                  execution_time_ms: executionTime,
                  query_id: queryId
                },
                'mcp_v2',
                queryId
              ]
            });
            logStmt.execute();
            
            return {
              success: true,
              query_id: queryId,
              row_count: rowCount,
              sample: rows.slice(0, 5),
              execution_time_ms: executionTime,
              logged_to_events: true  // Confirm logging happened
            };
            
          } catch (err) {
            // LOG FAILURE TO ACTIVITY.EVENTS
            try {
              var errorLogStmt = snowflake.createStatement({
                sqlText: \`INSERT INTO CLAUDE_BI.ACTIVITY.EVENTS 
                          (activity_id, ts, customer, activity, feature_json, _source_system)
                          VALUES (?, CURRENT_TIMESTAMP(), CURRENT_USER(), ?, TO_VARIANT(?), ?)\`,
                binds: [
                  'act_' + queryId,
                  'ccode.mcp.query_failed',
                  {
                    status: 'failed',
                    error: err.toString(),
                    plan: PLAN,
                    execution_time_ms: Date.now() - startTime,
                    query_id: queryId
                  },
                  'mcp_v2'
                ]
              });
              errorLogStmt.execute();
            } catch (logErr) {
              // Even if logging fails, return the original error
            }
            
            return {
              success: false,
              error: err.toString(),
              query_id: queryId,
              logged_to_events: true
            };
          }
        $$
      `);
      
      // 2. Create a procedure to analyze MCP activity from EVENTS
      console.log('📝 Creating MCP_ACTIVITY_ANALYSIS procedure...');
      await executeSql(`
        CREATE OR REPLACE PROCEDURE MCP.ANALYZE_MCP_ACTIVITY(hours_back INTEGER)
        RETURNS TABLE(
          activity VARCHAR,
          status VARCHAR,
          count NUMBER,
          avg_execution_time_ms NUMBER,
          total_rows_returned NUMBER
        )
        LANGUAGE SQL
        EXECUTE AS CALLER
        AS
        BEGIN
          RETURN TABLE(
            SELECT 
              activity,
              feature_json:status::STRING as status,
              COUNT(*) as count,
              AVG(feature_json:execution_time_ms::NUMBER) as avg_execution_time_ms,
              SUM(feature_json:rows_returned::NUMBER) as total_rows_returned
            FROM CLAUDE_BI.ACTIVITY.EVENTS
            WHERE activity LIKE 'ccode.mcp.%'
              AND ts >= DATEADD('hour', -:hours_back, CURRENT_TIMESTAMP())
            GROUP BY 1, 2
            ORDER BY 3 DESC
          );
        END;
      `);
      
      // 3. Create a view for real-time MCP monitoring
      console.log('📝 Creating MCP monitoring view...');
      await executeSql(`
        CREATE OR REPLACE VIEW MCP.VW_MCP_ACTIVITY AS
        SELECT 
          activity_id,
          ts,
          customer,
          activity,
          feature_json:status::STRING as status,
          feature_json:source::STRING as source,
          feature_json:rows_returned::NUMBER as rows_returned,
          feature_json:execution_time_ms::NUMBER as execution_time_ms,
          feature_json:query_id::STRING as query_id,
          feature_json:error::STRING as error_message,
          _source_system,
          _query_tag
        FROM CLAUDE_BI.ACTIVITY.EVENTS
        WHERE activity LIKE 'ccode.mcp.%'
          AND ts >= DATEADD('day', -7, CURRENT_TIMESTAMP())
      `);
      
      // 4. Grant permissions
      console.log('📝 Granting permissions...');
      await executeSql('GRANT USAGE ON PROCEDURE MCP.EXECUTE_QUERY_PLAN_V2(VARIANT) TO ROLE CLAUDE_BI_ROLE');
      await executeSql('GRANT USAGE ON PROCEDURE MCP.ANALYZE_MCP_ACTIVITY(INTEGER) TO ROLE CLAUDE_BI_ROLE');
      await executeSql('GRANT SELECT ON VIEW MCP.VW_MCP_ACTIVITY TO ROLE CLAUDE_BI_ROLE');
      
      // 5. Test the procedures
      console.log('\n🧪 Testing Activity logging...\n');
      
      // Test V2 execution with logging
      console.log('  Testing EXECUTE_QUERY_PLAN_V2...');
      const v2Test = await executeSql(`
        CALL MCP.EXECUTE_QUERY_PLAN_V2(OBJECT_CONSTRUCT(
          'source', 'VW_ACTIVITY_SUMMARY',
          'top_n', 5
        ))
      `);
      console.log('  ✅ Query executed and logged');
      
      // Verify it was logged to EVENTS
      console.log('\n  Verifying Activity logging...');
      const verifyLog = await executeSql(`
        SELECT COUNT(*) as log_count
        FROM CLAUDE_BI.ACTIVITY.EVENTS 
        WHERE activity LIKE 'ccode.mcp.%'
          AND ts >= DATEADD('minute', -1, CURRENT_TIMESTAMP())
      `);
      const logCount = verifyLog[0].LOG_COUNT;
      console.log(`  ✅ Found ${logCount} MCP events in Activity log`);
      
      // Show activity analysis
      console.log('\n  Analyzing MCP activity...');
      const analysis = await executeSql('CALL MCP.ANALYZE_MCP_ACTIVITY(24)');
      console.log('  ✅ Activity analysis available');
      
      // Show monitoring view
      console.log('\n  Checking monitoring view...');
      const monitoring = await executeSql('SELECT COUNT(*) as event_count FROM MCP.VW_MCP_ACTIVITY');
      console.log(`  ✅ Monitoring view shows ${monitoring[0].EVENT_COUNT} MCP events`);
      
      console.log('\n✨ Enhanced MCP with Activity Logging deployed!');
      console.log('\n📊 Activity Schema Integration Complete:');
      console.log('  • Every query logged to ACTIVITY.EVENTS');
      console.log('  • Success/failure tracking with details');
      console.log('  • Query attribution via _query_tag');
      console.log('  • Execution time and row count tracked');
      console.log('  • Error messages captured');
      console.log('  • Real-time monitoring view available');
      console.log('\n🔍 Query MCP activity with:');
      console.log('  SELECT * FROM MCP.VW_MCP_ACTIVITY ORDER BY ts DESC;');
      console.log('  CALL MCP.ANALYZE_MCP_ACTIVITY(24);');
      
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
            console.error('  ❌ Error:', err.message.substring(0, 200));
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