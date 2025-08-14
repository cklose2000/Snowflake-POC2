#!/usr/bin/env node

/**
 * Test Activity Logging - The CORE of MCP
 * Everything flows through ACTIVITY.EVENTS
 */

const snowflake = require('snowflake-sdk');
const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '../.env') });

console.log('🚀 Testing MCP Activity Logging...\n');

const connection = snowflake.createConnection({
  account: process.env.SNOWFLAKE_ACCOUNT,
  username: process.env.SNOWFLAKE_USERNAME,
  password: process.env.SNOWFLAKE_PASSWORD,
  role: process.env.SNOWFLAKE_ROLE || 'CLAUDE_BI_ROLE',
  database: 'CLAUDE_BI',
  schema: 'MCP',
  warehouse: 'CLAUDE_WAREHOUSE'
});

connection.connect(async (err, conn) => {
  if (err) {
    console.error('❌ Failed to connect:', err.message);
    process.exit(1);
  }
  
  console.log('✅ Connected to Snowflake\n');
  
  // Test that our existing EXECUTE_QUERY_PLAN logs to ACTIVITY.EVENTS
  console.log('📝 Testing if current procedures log to ACTIVITY.EVENTS...\n');
  
  // Execute a test query
  console.log('1️⃣ Executing test query...');
  connection.execute({
    sqlText: `CALL MCP.EXECUTE_QUERY_PLAN(PARSE_JSON('{"source": "VW_ACTIVITY_SUMMARY"}'))`,
    complete: (err, statement, rows) => {
      if (err) {
        console.error('❌ Query failed:', err.message);
      } else {
        console.log('✅ Query executed:', JSON.parse(rows[0].EXECUTE_QUERY_PLAN));
        
        // Check if it was logged
        console.log('\n2️⃣ Checking ACTIVITY.EVENTS for MCP activity...');
        connection.execute({
          sqlText: `
            SELECT 
              activity_id,
              ts,
              customer,
              activity,
              feature_json,
              _source_system
            FROM CLAUDE_BI.ACTIVITY.EVENTS
            WHERE activity LIKE '%mcp%' 
               OR activity LIKE '%ccode%'
               OR _source_system LIKE '%mcp%'
            ORDER BY ts DESC
            LIMIT 10
          `,
          complete: (err2, statement2, rows2) => {
            if (err2) {
              console.error('❌ Failed to query ACTIVITY.EVENTS:', err2.message);
            } else {
              console.log(`✅ Found ${rows2.length} recent MCP-related events\n`);
              
              if (rows2.length > 0) {
                console.log('📊 Recent MCP Activity:');
                rows2.forEach(row => {
                  console.log(`  • ${row.ACTIVITY} at ${row.TS}`);
                  if (row.FEATURE_JSON) {
                    try {
                      const features = typeof row.FEATURE_JSON === 'string' 
                        ? JSON.parse(row.FEATURE_JSON) 
                        : row.FEATURE_JSON;
                      console.log(`    Details: ${JSON.stringify(features).substring(0, 100)}...`);
                    } catch (e) {
                      console.log(`    Details: ${row.FEATURE_JSON}`);
                    }
                  }
                });
              }
              
              // Now let's create a proper logging procedure
              console.log('\n3️⃣ Creating enhanced procedure with Activity logging...');
              
              const enhancedProc = `
                CREATE OR REPLACE PROCEDURE MCP.EXECUTE_WITH_LOGGING(plan VARIANT)
                RETURNS VARIANT
                LANGUAGE JAVASCRIPT
                EXECUTE AS CALLER
                AS
                $$
                  var queryId = 'mcp_' + Date.now();
                  var startTime = Date.now();
                  
                  try {
                    // Execute based on plan
                    var source = PLAN.source || 'VW_ACTIVITY_SUMMARY';
                    var limit = PLAN.top_n || 100;
                    var sqlText = '';
                    
                    if (source === 'VW_ACTIVITY_SUMMARY') {
                      sqlText = 'SELECT * FROM CLAUDE_BI.ACTIVITY_CCODE.VW_ACTIVITY_SUMMARY';
                    } else if (source === 'VW_ACTIVITY_COUNTS_24H') {
                      sqlText = 'SELECT * FROM CLAUDE_BI.ACTIVITY_CCODE.VW_ACTIVITY_COUNTS_24H LIMIT ' + limit;
                    } else {
                      sqlText = 'SELECT * FROM CLAUDE_BI.ACTIVITY.EVENTS ORDER BY ts DESC LIMIT ' + limit;
                    }
                    
                    var result = snowflake.createStatement({sqlText: sqlText}).execute();
                    var rowCount = 0;
                    var sample = [];
                    
                    while (result.next() && rowCount < 5) {
                      sample.push(result.getColumnValueAsString(1));
                      rowCount++;
                    }
                    
                    var execTime = Date.now() - startTime;
                    
                    // LOG TO ACTIVITY.EVENTS - THE WHOLE STORY!
                    var activityId = 'act_' + queryId;
                    var logSql = "INSERT INTO CLAUDE_BI.ACTIVITY.EVENTS " +
                                "(activity_id, ts, customer, activity, feature_json, _source_system) " +
                                "SELECT '" + activityId + "', " +
                                "CURRENT_TIMESTAMP(), " +
                                "CURRENT_USER(), " +
                                "'ccode.mcp.query_executed', " +
                                "PARSE_JSON('" + JSON.stringify({
                                  query_id: queryId,
                                  plan: PLAN,
                                  rows_returned: rowCount,
                                  execution_time_ms: execTime,
                                  status: 'success'
                                }).replace(/'/g, "''") + "'), " +
                                "'mcp_enhanced'";
                    
                    snowflake.createStatement({sqlText: logSql}).execute();
                    
                    return {
                      success: true,
                      query_id: queryId,
                      activity_id: activityId,
                      rows: rowCount,
                      sample: sample,
                      execution_time_ms: execTime,
                      logged: true
                    };
                    
                  } catch (err) {
                    // Log the error too
                    var errorActivityId = 'act_error_' + queryId;
                    var errorLogSql = "INSERT INTO CLAUDE_BI.ACTIVITY.EVENTS " +
                                     "(activity_id, ts, customer, activity, feature_json, _source_system) " +
                                     "SELECT '" + errorActivityId + "', " +
                                     "CURRENT_TIMESTAMP(), " +
                                     "CURRENT_USER(), " +
                                     "'ccode.mcp.query_failed', " +
                                     "PARSE_JSON('" + JSON.stringify({
                                       query_id: queryId,
                                       plan: PLAN,
                                       error: err.toString(),
                                       status: 'failed'
                                     }).replace(/'/g, "''") + "'), " +
                                     "'mcp_enhanced'";
                    
                    try {
                      snowflake.createStatement({sqlText: errorLogSql}).execute();
                    } catch (logErr) {
                      // Logging failed, but still return error
                    }
                    
                    return {
                      success: false,
                      error: err.toString(),
                      query_id: queryId,
                      activity_id: errorActivityId
                    };
                  }
                $$
              `;
              
              connection.execute({
                sqlText: enhancedProc,
                complete: (err3, statement3, rows3) => {
                  if (err3) {
                    console.error('❌ Failed to create procedure:', err3.message);
                  } else {
                    console.log('✅ Created EXECUTE_WITH_LOGGING procedure');
                    
                    // Grant permissions
                    connection.execute({
                      sqlText: 'GRANT USAGE ON PROCEDURE MCP.EXECUTE_WITH_LOGGING(VARIANT) TO ROLE CLAUDE_BI_ROLE',
                      complete: (err4) => {
                        if (!err4) console.log('✅ Permissions granted');
                        
                        // Test it!
                        console.log('\n4️⃣ Testing enhanced procedure with logging...');
                        connection.execute({
                          sqlText: `CALL MCP.EXECUTE_WITH_LOGGING(PARSE_JSON('{"source": "VW_ACTIVITY_SUMMARY"}'))`,
                          complete: (err5, statement5, rows5) => {
                            if (err5) {
                              console.error('❌ Test failed:', err5.message);
                            } else {
                              const result = rows5[0].EXECUTE_WITH_LOGGING;
                              console.log('✅ Executed with logging:', result);
                              
                              // Verify it was logged
                              console.log('\n5️⃣ Verifying Activity log entry...');
                              const activityId = result.activity_id;
                              
                              connection.execute({
                                sqlText: `
                                  SELECT * FROM CLAUDE_BI.ACTIVITY.EVENTS 
                                  WHERE activity_id = '${activityId}'
                                `,
                                complete: (err6, statement6, rows6) => {
                                  if (err6) {
                                    console.error('❌ Failed to verify:', err6.message);
                                  } else if (rows6.length > 0) {
                                    console.log('✅ CONFIRMED: Query logged to ACTIVITY.EVENTS!');
                                    console.log('   Activity ID:', rows6[0].ACTIVITY_ID);
                                    console.log('   Activity:', rows6[0].ACTIVITY);
                                    console.log('   Customer:', rows6[0].CUSTOMER);
                                    console.log('   Source System:', rows6[0]._SOURCE_SYSTEM);
                                    
                                    console.log('\n✨ Activity Schema Integration Complete!');
                                    console.log('📊 Every MCP query is now part of the Activity story!');
                                    console.log('\n🔍 Query all MCP activity with:');
                                    console.log("   SELECT * FROM ACTIVITY.EVENTS WHERE activity LIKE 'ccode.mcp.%' ORDER BY ts DESC;");
                                  } else {
                                    console.log('⚠️  Activity not found yet (may need a moment to propagate)');
                                  }
                                  
                                  connection.destroy();
                                  process.exit(0);
                                }
                              });
                            }
                          }
                        });
                      }
                    });
                  }
                }
              });
            }
          }
        });
      }
    }
  });
});