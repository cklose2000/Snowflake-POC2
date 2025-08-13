#!/usr/bin/env node

const snowflake = require('snowflake-sdk');
require('dotenv').config();

async function checkActivityData() {
  console.log('📊 Checking Activity Data in Views\n');
  
  const connection = snowflake.createConnection({
    account: process.env.SNOWFLAKE_ACCOUNT,
    username: process.env.SNOWFLAKE_USERNAME,
    password: process.env.SNOWFLAKE_PASSWORD,
    role: process.env.SNOWFLAKE_ROLE,
    warehouse: process.env.SNOWFLAKE_WAREHOUSE,
    database: process.env.SNOWFLAKE_DATABASE,
    schema: process.env.SNOWFLAKE_SCHEMA
  });
  
  await new Promise((resolve, reject) => {
    connection.connect((err, conn) => {
      if (err) reject(err);
      else resolve(conn);
    });
  });
  
  // Check each view
  const views = [
    {
      name: 'VW_ACTIVITY_COUNTS_24H',
      sql: 'SELECT * FROM CLAUDE_BI.ACTIVITY_CCODE.VW_ACTIVITY_COUNTS_24H LIMIT 5'
    },
    {
      name: 'VW_LLM_TELEMETRY',
      sql: 'SELECT * FROM CLAUDE_BI.ACTIVITY_CCODE.VW_LLM_TELEMETRY LIMIT 5'
    },
    {
      name: 'VW_SQL_EXECUTIONS',
      sql: 'SELECT * FROM CLAUDE_BI.ACTIVITY_CCODE.VW_SQL_EXECUTIONS LIMIT 5'
    },
    {
      name: 'VW_ACTIVITY_SUMMARY',
      sql: 'SELECT * FROM CLAUDE_BI.ACTIVITY_CCODE.VW_ACTIVITY_SUMMARY'
    }
  ];
  
  for (const view of views) {
    await new Promise((resolve) => {
      connection.execute({
        sqlText: view.sql,
        complete: (err, stmt, rows) => {
          if (err) {
            console.error(`❌ ${view.name}: ${err.message}`);
          } else {
            console.log(`✅ ${view.name}: ${rows.length} rows`);
            if (rows.length > 0 && view.name.includes('SUMMARY')) {
              console.log('   Summary data:', {
                total_events_24h: rows[0].TOTAL_EVENTS_24H,
                unique_customers: rows[0].UNIQUE_CUSTOMERS_24H,
                unique_activities: rows[0].UNIQUE_ACTIVITIES_24H
              });
            } else if (rows.length > 0) {
              console.log('   Sample activity:', rows[0].ACTIVITY || rows[0].CUSTOMER);
            }
          }
          resolve();
        }
      });
    });
  }
  
  // Check total events
  await new Promise((resolve) => {
    connection.execute({
      sqlText: "SELECT COUNT(*) as total FROM CLAUDE_BI.ACTIVITY.EVENTS WHERE ts >= DATEADD('hour', -24, CURRENT_TIMESTAMP())",
      complete: (err, stmt, rows) => {
        if (!err && rows && rows[0]) {
          console.log(`\n📈 Total events in last 24h: ${rows[0].TOTAL}`);
        }
        resolve();
      }
    });
  });
  
  connection.destroy();
  console.log('\n✅ Activity data check complete!');
}

checkActivityData().catch(console.error);