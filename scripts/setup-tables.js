// Simplified setup script for core tables
const snowflake = require('snowflake-sdk');
require('dotenv').config();

async function setupTables() {
  console.log('🚀 Setting up Snowflake tables...');
  
  const connection = snowflake.createConnection({
    account: process.env.SNOWFLAKE_ACCOUNT,
    username: process.env.SNOWFLAKE_USERNAME,
    password: process.env.SNOWFLAKE_PASSWORD,
    role: process.env.SNOWFLAKE_ROLE,
    warehouse: process.env.SNOWFLAKE_WAREHOUSE
  });

  await new Promise((resolve, reject) => {
    connection.connect((err, conn) => {
      if (err) reject(err);
      else {
        console.log('✅ Connected to Snowflake');
        resolve(conn);
      }
    });
  });

  const statements = [
    'USE DATABASE CLAUDE_BI',
    'USE SCHEMA ANALYTICS',
    
    // Create activity schema if needed
    'CREATE SCHEMA IF NOT EXISTS ACTIVITY',
    
    // Main activity events table
    `CREATE TABLE IF NOT EXISTS ACTIVITY.EVENTS (
      activity_id VARCHAR(255) NOT NULL,
      ts TIMESTAMP_NTZ NOT NULL,
      customer VARCHAR(255) NOT NULL, 
      activity VARCHAR(255) NOT NULL,
      feature_json VARIANT NOT NULL,
      anonymous_customer_id VARCHAR(255),
      revenue_impact FLOAT,
      link VARCHAR(255),
      _source_system VARCHAR(255) DEFAULT 'claude_code',
      _source_version VARCHAR(255),
      _session_id VARCHAR(255),
      _query_tag VARCHAR(255),
      _activity_occurrence INTEGER,
      _activity_repeated_at TIMESTAMP_NTZ,
      PRIMARY KEY (activity_id)
    )`,
    
    // Create activity_ccode schema
    'CREATE SCHEMA IF NOT EXISTS ACTIVITY_CCODE',
    
    // Artifacts table
    `CREATE TABLE IF NOT EXISTS ACTIVITY_CCODE.ARTIFACTS (
      artifact_id VARCHAR(255) NOT NULL,
      sample VARIANT,
      row_count INTEGER,
      schema_json VARIANT,
      storage_type VARCHAR(50),
      storage_location VARCHAR(500),
      bytes BIGINT,
      compressed_bytes BIGINT,
      created_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
      customer VARCHAR(255),
      created_by_activity VARCHAR(255),
      PRIMARY KEY (artifact_id)
    )`,
    
    // Artifact data table
    `CREATE TABLE IF NOT EXISTS ACTIVITY_CCODE.ARTIFACT_DATA (
      artifact_id VARCHAR(255) NOT NULL,
      row_number INTEGER NOT NULL,
      row_data VARIANT NOT NULL,
      PRIMARY KEY (artifact_id, row_number)
    )`,
    
    // Audit results table
    `CREATE TABLE IF NOT EXISTS ACTIVITY_CCODE.AUDIT_RESULTS (
      audit_id VARCHAR(255) NOT NULL,
      activity_id VARCHAR(255),
      passed BOOLEAN,
      findings VARIANT,
      remediation TEXT,
      audit_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
      customer VARCHAR(255),
      PRIMARY KEY (audit_id)
    )`,
    
    // Create internal stage
    'CREATE STAGE IF NOT EXISTS ACTIVITY_CCODE.ARTIFACT_STAGE',
    
    // Insert test record
    `INSERT INTO ACTIVITY.EVENTS (
      activity_id, ts, customer, activity, feature_json
    ) SELECT 
      'test_001', 
      CURRENT_TIMESTAMP(), 
      'system_user', 
      'ccode.system_initialized',
      PARSE_JSON('{"version": "1.0.0", "setup_complete": true}')
    WHERE NOT EXISTS (
      SELECT 1 FROM ACTIVITY.EVENTS WHERE activity_id = 'test_001'
    )`
  ];

  for (let i = 0; i < statements.length; i++) {
    console.log(`Executing: ${statements[i].substring(0, 50)}...`);
    
    await new Promise((resolve, reject) => {
      connection.execute({
        sqlText: statements[i],
        complete: (err, statement, rows) => {
          if (err) {
            console.error('Error:', err.message);
            resolve(); // Continue anyway
          } else {
            console.log('✅ Success');
            resolve(rows);
          }
        }
      });
    });
  }

  connection.destroy();
  console.log('🎉 Setup complete!');
}

setupTables().catch(console.error);