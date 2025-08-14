#!/usr/bin/env node

const snowflake = require('snowflake-sdk');
const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '../.env') });

const connection = snowflake.createConnection({
  account: process.env.SNOWFLAKE_ACCOUNT,
  username: process.env.SNOWFLAKE_USERNAME,
  password: process.env.SNOWFLAKE_PASSWORD,
  role: 'CLAUDE_BI_ROLE',
  database: 'CLAUDE_BI',
  schema: 'MCP',
  warehouse: 'CLAUDE_WAREHOUSE'
});

connection.connect((err, conn) => {
  if (err) {
    console.error('❌ Failed to connect:', err.message);
    process.exit(1);
  }
  
  console.log('✅ Connected');
  
  // Create a simpler procedure that works
  const createProc = `
    CREATE OR REPLACE PROCEDURE EXECUTE_QUERY_PLAN(plan VARIANT)
    RETURNS VARCHAR
    LANGUAGE JAVASCRIPT
    EXECUTE AS CALLER
    AS
    $$
      var source = PLAN.source || 'VW_ACTIVITY_SUMMARY';
      var top_n = PLAN.top_n || 100;
      var sql_text = '';
      
      if (source === 'VW_ACTIVITY_SUMMARY') {
        sql_text = 'SELECT * FROM CLAUDE_BI.ACTIVITY_CCODE.VW_ACTIVITY_SUMMARY';
      } else if (source === 'VW_ACTIVITY_COUNTS_24H') {
        sql_text = 'SELECT * FROM CLAUDE_BI.ACTIVITY_CCODE.VW_ACTIVITY_COUNTS_24H LIMIT ' + top_n;
      } else if (source === 'EVENTS') {
        sql_text = 'SELECT * FROM CLAUDE_BI.ACTIVITY.EVENTS ORDER BY TS DESC LIMIT ' + top_n;
      } else {
        return 'Unknown source: ' + source;
      }
      
      var stmt = snowflake.createStatement({sqlText: sql_text});
      var result = stmt.execute();
      
      var rows = [];
      while (result.next()) {
        rows.push(result.getColumnValueAsString(1));
      }
      
      return JSON.stringify({
        success: true,
        source: source,
        row_count: rows.length,
        sample: rows.slice(0, 5)
      });
    $$
  `;
  
  connection.execute({
    sqlText: createProc,
    complete: (err, statement, rows) => {
      if (err) {
        console.error('❌ Failed to create procedure:', err.message);
      } else {
        console.log('✅ Procedure created');
        
        // Grant permissions
        connection.execute({
          sqlText: 'GRANT USAGE ON PROCEDURE CLAUDE_BI.MCP.EXECUTE_QUERY_PLAN(VARIANT) TO ROLE CLAUDE_BI_ROLE',
          complete: (err2) => {
            if (!err2) console.log('✅ Permissions granted');
            
            // Test it
            connection.execute({
              sqlText: `CALL CLAUDE_BI.MCP.EXECUTE_QUERY_PLAN(PARSE_JSON('{"source": "VW_ACTIVITY_SUMMARY"}'))`,
              complete: (err3, statement3, rows3) => {
                if (err3) {
                  console.error('❌ Test failed:', err3.message);
                } else {
                  console.log('✅ Test successful! Result:', JSON.stringify(rows3[0]));
                }
                connection.destroy();
                process.exit(0);
              }
            });
          }
        });
      }
    }
  });
});