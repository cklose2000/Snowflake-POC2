// Quick test of the bridge components
const snowflake = require('snowflake-sdk');
require('dotenv').config();

async function testBridge() {
  console.log('🧪 Testing Bridge components...\n');
  
  // Test 1: Snowflake Connection
  console.log('1️⃣ Testing Snowflake connection...');
  const connection = snowflake.createConnection({
    account: process.env.SNOWFLAKE_ACCOUNT,
    username: process.env.SNOWFLAKE_USERNAME,
    password: process.env.SNOWFLAKE_PASSWORD,
    role: process.env.SNOWFLAKE_ROLE,
    warehouse: process.env.SNOWFLAKE_WAREHOUSE,
    database: process.env.SNOWFLAKE_DATABASE,
    schema: process.env.SNOWFLAKE_SCHEMA
  });

  try {
    await new Promise((resolve, reject) => {
      connection.connect((err, conn) => {
        if (err) reject(err);
        else resolve(conn);
      });
    });
    console.log('✅ Snowflake connection successful\n');
  } catch (err) {
    console.error('❌ Snowflake connection failed:', err.message);
    return;
  }

  // Test 2: Activity Logging
  console.log('2️⃣ Testing activity logging...');
  const activityId = 'test_' + Date.now();
  try {
    await new Promise((resolve, reject) => {
      connection.execute({
        sqlText: `INSERT INTO ACTIVITY.EVENTS (
          activity_id, ts, customer, activity, feature_json, _source_system
        ) VALUES (?, CURRENT_TIMESTAMP(), ?, ?, PARSE_JSON(?), ?)`,
        binds: [
          activityId,
          'test_user',
          'ccode.bridge_test',
          JSON.stringify({ test: true, timestamp: new Date().toISOString() }),
          'bridge_test'
        ],
        complete: (err, stmt, rows) => {
          if (err) reject(err);
          else resolve(rows);
        }
      });
    });
    console.log('✅ Activity logging successful\n');
  } catch (err) {
    console.error('❌ Activity logging failed:', err.message);
  }

  // Test 3: SafeSQL Template simulation
  console.log('3️⃣ Testing SafeSQL template (sample_top)...');
  try {
    const result = await new Promise((resolve, reject) => {
      connection.execute({
        sqlText: `SELECT * FROM ACTIVITY.EVENTS LIMIT 5`,
        complete: (err, stmt, rows) => {
          if (err) reject(err);
          else resolve(rows);
        }
      });
    });
    console.log(`✅ SafeSQL template successful - ${result.length} rows returned\n`);
  } catch (err) {
    console.error('❌ SafeSQL template failed:', err.message);
  }

  // Test 4: Artifact storage
  console.log('4️⃣ Testing artifact storage...');
  const artifactId = 'art_' + Date.now();
  try {
    await new Promise((resolve, reject) => {
      connection.execute({
        sqlText: `INSERT INTO ACTIVITY_CCODE.ARTIFACTS (
          artifact_id, sample_data, row_count, storage_type, storage_location, customer
        ) VALUES (?, PARSE_JSON(?), ?, ?, ?, ?)`,
        binds: [
          artifactId,
          JSON.stringify([{ col1: 'value1', col2: 'value2' }]),
          1,
          'inline',
          'artifacts.sample_data',
          'test_user'
        ],
        complete: (err, stmt, rows) => {
          if (err) reject(err);
          else resolve(rows);
        }
      });
    });
    console.log('✅ Artifact storage successful\n');
  } catch (err) {
    console.error('❌ Artifact storage failed:', err.message);
  }

  connection.destroy();
  console.log('🎉 All tests complete!');
}

testBridge().catch(console.error);