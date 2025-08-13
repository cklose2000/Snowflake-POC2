// Verify Snowflake setup
const snowflake = require('snowflake-sdk');
require('dotenv').config();

async function verifySetup() {
  console.log('🔍 Verifying Snowflake setup...');
  
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
      else {
        console.log('✅ Connected to Snowflake');
        resolve(conn);
      }
    });
  });

  // Check what tables exist
  const checkQueries = [
    {
      name: 'Schemas',
      sql: `SHOW SCHEMAS IN DATABASE CLAUDE_BI`
    },
    {
      name: 'Tables in ACTIVITY',
      sql: `SHOW TABLES IN SCHEMA ACTIVITY`
    },
    {
      name: 'Tables in ACTIVITY_CCODE',
      sql: `SHOW TABLES IN SCHEMA ACTIVITY_CCODE`
    },
    {
      name: 'Test record',
      sql: `SELECT * FROM ACTIVITY.EVENTS WHERE activity_id = 'test_001'`
    }
  ];

  for (const query of checkQueries) {
    console.log(`\n📊 ${query.name}:`);
    
    await new Promise((resolve) => {
      connection.execute({
        sqlText: query.sql,
        complete: (err, statement, rows) => {
          if (err) {
            console.error('Error:', err.message);
          } else {
            if (rows && rows.length > 0) {
              console.table(rows.slice(0, 5));
            } else {
              console.log('No results');
            }
          }
          resolve();
        }
      });
    });
  }

  // Fix the ARTIFACTS table if needed
  console.log('\n🔧 Fixing ARTIFACTS table...');
  await new Promise((resolve) => {
    connection.execute({
      sqlText: `CREATE OR REPLACE TABLE ACTIVITY_CCODE.ARTIFACTS (
        artifact_id VARCHAR(255) NOT NULL,
        sample_data VARIANT,
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
      complete: (err) => {
        if (err) {
          console.error('Error creating ARTIFACTS:', err.message);
        } else {
          console.log('✅ ARTIFACTS table created/fixed');
        }
        resolve();
      }
    });
  });

  connection.destroy();
  console.log('\n🎉 Verification complete!');
}

verifySetup().catch(console.error);