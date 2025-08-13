#!/usr/bin/env node

// Verify dashboard objects were created

const snowflake = require('snowflake-sdk');
require('dotenv').config();

async function verifyDashboard() {
  console.log('🔍 Verifying dashboard objects in Snowflake...\n');
  
  // Create Snowflake connection
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
  
  console.log('✅ Connected to Snowflake\n');
  
  // Check created views
  const viewsSQL = `
    SELECT table_name, table_type
    FROM CLAUDE_BI.INFORMATION_SCHEMA.TABLES
    WHERE table_schema = 'ANALYTICS'
      AND table_name LIKE 'activity_dashboard__%'
    ORDER BY table_name
  `;
  
  console.log('📊 Dashboard views created:');
  await new Promise((resolve, reject) => {
    connection.execute({
      sqlText: viewsSQL,
      complete: (err, stmt, rows) => {
        if (err) {
          console.error('Error:', err.message);
          reject(err);
        } else {
          rows.forEach(row => {
            console.log(`  - ${row.TABLE_NAME} (${row.TABLE_TYPE})`);
          });
          resolve(rows);
        }
      }
    });
  });
  
  // Sample data from one of the views
  const sampleSQL = `
    SELECT * FROM activity_dashboard__panel_1__5ceb197b LIMIT 5
  `;
  
  console.log('\n📈 Sample data from panel_1 view:');
  await new Promise((resolve, reject) => {
    connection.execute({
      sqlText: sampleSQL,
      complete: (err, stmt, rows) => {
        if (err) {
          console.error('Error:', err.message);
          reject(err);
        } else {
          console.table(rows);
          resolve(rows);
        }
      }
    });
  });
  
  // Check Activity logs for dashboard creation
  const activitySQL = `
    SELECT 
      activity,
      ts,
      feature_json:spec_id as dashboard_name,
      feature_json:panels as panel_count,
      feature_json:objects_created as objects_created
    FROM CLAUDE_BI.ACTIVITY.EVENTS
    WHERE activity LIKE 'ccode.dashboard%'
      AND ts >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
    ORDER BY ts DESC
    LIMIT 10
  `;
  
  console.log('\n📝 Recent dashboard activities:');
  await new Promise((resolve, reject) => {
    connection.execute({
      sqlText: activitySQL,
      complete: (err, stmt, rows) => {
        if (err) {
          console.error('Error:', err.message);
          reject(err);
        } else {
          rows.forEach(row => {
            console.log(`  - ${row.ACTIVITY} at ${row.TS}: ${row.DASHBOARD_NAME || 'N/A'}`);
          });
          resolve(rows);
        }
      }
    });
  });
  
  connection.destroy();
  console.log('\n✅ Verification complete!');
}

verifyDashboard().catch(console.error);