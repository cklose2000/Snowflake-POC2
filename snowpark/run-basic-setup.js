#!/usr/bin/env node

const snowflake = require('snowflake-sdk');
const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '../.env') });

console.log('🚀 Running basic MCP setup in Snowflake...');

const connection = snowflake.createConnection({
  account: process.env.SNOWFLAKE_ACCOUNT,
  username: process.env.SNOWFLAKE_USERNAME,
  password: process.env.SNOWFLAKE_PASSWORD,
  role: process.env.SNOWFLAKE_ROLE || 'CLAUDE_BI_ROLE',
  database: 'CLAUDE_BI',
  schema: 'PUBLIC',
  warehouse: 'CLAUDE_WAREHOUSE'
});

const statements = [
  'USE ROLE ACCOUNTADMIN',
  'USE DATABASE CLAUDE_BI',
  'USE WAREHOUSE CLAUDE_WAREHOUSE',
  'CREATE SCHEMA IF NOT EXISTS CLAUDE_BI.MCP',
  'USE SCHEMA CLAUDE_BI.MCP',
  
  // Create validation procedure
  `CREATE OR REPLACE PROCEDURE VALIDATE_QUERY_PLAN(plan VARIANT)
   RETURNS VARIANT
   LANGUAGE SQL
   EXECUTE AS CALLER
   AS
   'DECLARE
     source STRING;
     max_rows INTEGER;
   BEGIN
     source := plan:source::STRING;
     max_rows := COALESCE(plan:top_n::INTEGER, 10000);
     IF (max_rows > 10000) THEN
       RETURN OBJECT_CONSTRUCT(''valid'', FALSE, ''error'', ''Row limit exceeds 10000'');
     END IF;
     RETURN OBJECT_CONSTRUCT(''valid'', TRUE, ''message'', ''Plan is valid'');
   END;'`,
   
  // Create execution procedure
  `CREATE OR REPLACE PROCEDURE EXECUTE_QUERY_PLAN(plan VARIANT)
   RETURNS TABLE(result VARIANT)
   LANGUAGE SQL
   EXECUTE AS CALLER
   AS
   'DECLARE
     source STRING;
     top_n INTEGER;
     sql_text STRING;
     res RESULTSET;
   BEGIN
     source := plan:source::STRING;
     top_n := COALESCE(plan:top_n::INTEGER, 100);
     
     IF (source = ''VW_ACTIVITY_SUMMARY'') THEN
       sql_text := ''SELECT * FROM CLAUDE_BI.ACTIVITY_CCODE.VW_ACTIVITY_SUMMARY'';
     ELSEIF (source = ''VW_ACTIVITY_COUNTS_24H'') THEN
       sql_text := ''SELECT * FROM CLAUDE_BI.ACTIVITY_CCODE.VW_ACTIVITY_COUNTS_24H LIMIT '' || top_n::STRING;
     ELSEIF (source = ''EVENTS'') THEN
       sql_text := ''SELECT * FROM CLAUDE_BI.ACTIVITY.EVENTS ORDER BY TS DESC LIMIT '' || top_n::STRING;
     ELSE
       sql_text := ''SELECT ''''Unknown source: '' || source || '''' as ERROR'';
     END IF;
     
     res := (EXECUTE IMMEDIATE :sql_text);
     RETURN TABLE(res);
   END;'`,
   
  // Grant permissions
  'GRANT USAGE ON SCHEMA CLAUDE_BI.MCP TO ROLE CLAUDE_BI_ROLE',
  'GRANT USAGE ON PROCEDURE CLAUDE_BI.MCP.VALIDATE_QUERY_PLAN(VARIANT) TO ROLE CLAUDE_BI_ROLE',
  'GRANT USAGE ON PROCEDURE CLAUDE_BI.MCP.EXECUTE_QUERY_PLAN(VARIANT) TO ROLE CLAUDE_BI_ROLE'
];

connection.connect(async (err, conn) => {
  if (err) {
    console.error('❌ Failed to connect:', err.message);
    process.exit(1);
  }
  
  console.log('✅ Connected to Snowflake');
  
  for (let i = 0; i < statements.length; i++) {
    const stmt = statements[i];
    const preview = stmt.substring(0, 50).replace(/\n/g, ' ');
    
    await new Promise((resolve) => {
      connection.execute({
        sqlText: stmt,
        complete: (err, statement, rows) => {
          if (err) {
            console.error(`❌ [${i+1}/${statements.length}] ${preview}...`);
            console.error(`   Error: ${err.message}`);
          } else {
            console.log(`✅ [${i+1}/${statements.length}] ${preview}...`);
          }
          resolve();
        }
      });
    });
  }
  
  // Test the procedures
  console.log('\n🧪 Testing MCP procedures...');
  
  // Test validation
  connection.execute({
    sqlText: `CALL CLAUDE_BI.MCP.VALIDATE_QUERY_PLAN(PARSE_JSON('{"source": "VW_ACTIVITY_SUMMARY", "top_n": 5}'))`,
    complete: (err, statement, rows) => {
      if (err) {
        console.error('❌ Validation test failed:', err.message);
      } else {
        console.log('✅ Validation test passed:', JSON.stringify(rows[0]));
      }
      
      // Test execution
      connection.execute({
        sqlText: `CALL CLAUDE_BI.MCP.EXECUTE_QUERY_PLAN(PARSE_JSON('{"source": "VW_ACTIVITY_SUMMARY", "top_n": 5}'))`,
        complete: (err, statement, rows) => {
          if (err) {
            console.error('❌ Execution test failed:', err.message);
          } else {
            console.log('✅ Execution test passed! Returned', rows.length, 'rows');
            if (rows[0]) {
              console.log('   Sample:', JSON.stringify(rows[0]));
            }
          }
          
          connection.destroy();
          console.log('\n✨ MCP basic setup complete!');
          console.log('📝 You can now call:');
          console.log('   CALL CLAUDE_BI.MCP.VALIDATE_QUERY_PLAN(plan)');
          console.log('   CALL CLAUDE_BI.MCP.EXECUTE_QUERY_PLAN(plan)');
          process.exit(0);
        }
      });
    }
  });
});