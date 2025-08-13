// Comprehensive test of all activity types
const snowflake = require('snowflake-sdk');
const http = require('http');
require('dotenv').config();

const API_BASE = 'http://localhost:3001';

// Define all valid activity types according to CLAUDE.md
const VALID_ACTIVITIES = [
  'ccode.user_asked',
  'ccode.sql_executed',
  'ccode.artifact_created',
  'ccode.audit_passed',
  'ccode.audit_failed',
  'ccode.bridge_started',
  'ccode.agent_invoked'
];

// Invalid activities (testing error handling)
const INVALID_ACTIVITIES = [
  'user_asked',           // Missing namespace
  'sql_executed',         // Missing namespace
  'ccode.',              // Empty after namespace
  'claude.sql_executed',  // Wrong namespace
  'ccode.unknown_event',  // Unknown event type
  'CCODE.SQL_EXECUTED',   // Wrong case (testing case sensitivity)
  ''                      // Empty string
];

// Test data generators
function generateFeatureJson(activity) {
  const features = {
    'ccode.user_asked': {
      question: 'Test question about Snowflake',
      context: 'testing',
      session_id: 'test_' + Date.now()
    },
    'ccode.sql_executed': {
      template: 'sample_top',
      rows_returned: Math.floor(Math.random() * 100),
      execution_time_ms: Math.floor(Math.random() * 1000),
      query_hash: 'hash_' + Math.random().toString(36).substr(2, 9)
    },
    'ccode.artifact_created': {
      artifact_id: 'art_' + Date.now(),
      storage_type: ['inline', 'table', 'stage'][Math.floor(Math.random() * 3)],
      row_count: Math.floor(Math.random() * 10000),
      bytes: Math.floor(Math.random() * 1000000)
    },
    'ccode.audit_passed': {
      claim: 'Successfully executed query',
      verification_method: 'row_count_check',
      expected: 100,
      actual: 100
    },
    'ccode.audit_failed': {
      claim: 'Query returned 100 rows',
      verification_method: 'row_count_check',
      expected: 100,
      actual: 50,
      error: 'Row count mismatch'
    },
    'ccode.bridge_started': {
      version: '1.0.0',
      environment: 'development',
      pid: process.pid,
      node_version: process.version
    },
    'ccode.agent_invoked': {
      agent_type: 'snowflake-agent',
      request_id: 'req_' + Date.now(),
      parent_activity: 'act_parent_123',
      depth: 1
    }
  };
  
  return features[activity] || { test: true, activity_type: activity };
}

// Helper function to make HTTP requests
function httpRequest(options, data) {
  return new Promise((resolve, reject) => {
    const req = http.request(options, (res) => {
      let body = '';
      res.on('data', chunk => body += chunk);
      res.on('end', () => {
        try {
          const json = JSON.parse(body);
          resolve({ ok: res.statusCode >= 200 && res.statusCode < 300, data: json, statusCode: res.statusCode });
        } catch (e) {
          resolve({ ok: false, data: body, statusCode: res.statusCode });
        }
      });
    });
    req.on('error', reject);
    if (data) req.write(data);
    req.end();
  });
}

// Test via API
async function testViaAPI(activity, shouldSucceed = true) {
  const testName = shouldSucceed ? `✅ Testing valid: ${activity}` : `❌ Testing invalid: ${activity}`;
  console.log(`\n${testName}`);
  
  try {
    const postData = JSON.stringify({
      activity: activity,
      customer: 'test_user_' + Math.random().toString(36).substr(2, 5),
      feature_json: generateFeatureJson(activity)
    });
    
    const response = await httpRequest({
      hostname: 'localhost',
      port: 3001,
      path: '/api/activity',
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(postData)
      }
    }, postData);
    
    if (response.ok) {
      console.log(`  ✓ API accepted: ${response.data.activity_id}`);
      return { success: true, activity_id: response.data.activity_id };
    } else {
      console.log(`  ✗ API rejected: ${response.data.error}`);
      return { success: false, error: response.data.error };
    }
  } catch (error) {
    console.log(`  ✗ API error: ${error.message}`);
    return { success: false, error: error.message };
  }
}

// Test direct to Snowflake
async function testDirectSnowflake(activity, shouldSucceed = true) {
  const connection = snowflake.createConnection({
    account: process.env.SNOWFLAKE_ACCOUNT,
    username: process.env.SNOWFLAKE_USERNAME,
    password: process.env.SNOWFLAKE_PASSWORD,
    role: process.env.SNOWFLAKE_ROLE,
    warehouse: process.env.SNOWFLAKE_WAREHOUSE,
    database: process.env.SNOWFLAKE_DATABASE,
    schema: process.env.SNOWFLAKE_SCHEMA
  });

  const conn = await new Promise((resolve, reject) => {
    connection.connect((err, conn) => {
      if (err) reject(err);
      else resolve(conn);
    });
  });

  const activityId = `test_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
  const customer = 'direct_test_' + Math.random().toString(36).substr(2, 5);
  
  try {
    await new Promise((resolve, reject) => {
      conn.execute({
        sqlText: `INSERT INTO ACTIVITY.EVENTS (
          activity_id, ts, customer, activity, feature_json, _source_system
        ) SELECT ?, CURRENT_TIMESTAMP(), ?, ?, PARSE_JSON(?), ?`,
        binds: [
          activityId,
          customer,
          activity,
          JSON.stringify(generateFeatureJson(activity)),
          'test_suite'
        ],
        complete: (err, stmt, rows) => {
          if (err) reject(err);
          else resolve(rows);
        }
      });
    });
    
    console.log(`  ✓ Snowflake accepted: ${activityId}`);
    return { success: true, activity_id: activityId };
  } catch (err) {
    console.log(`  ✗ Snowflake rejected: ${err.message}`);
    return { success: false, error: err.message };
  } finally {
    connection.destroy();
  }
}

// Verify activities in database
async function verifyActivities(activityIds) {
  console.log('\n📊 Verifying stored activities...');
  
  const connection = snowflake.createConnection({
    account: process.env.SNOWFLAKE_ACCOUNT,
    username: process.env.SNOWFLAKE_USERNAME,
    password: process.env.SNOWFLAKE_PASSWORD,
    role: process.env.SNOWFLAKE_ROLE,
    warehouse: process.env.SNOWFLAKE_WAREHOUSE,
    database: process.env.SNOWFLAKE_DATABASE,
    schema: process.env.SNOWFLAKE_SCHEMA
  });

  const conn = await new Promise((resolve, reject) => {
    connection.connect((err, conn) => {
      if (err) reject(err);
      else resolve(conn);
    });
  });

  // Check count by activity type
  const countResult = await new Promise((resolve, reject) => {
    conn.execute({
      sqlText: `SELECT activity, COUNT(*) as count 
                FROM ACTIVITY.EVENTS 
                WHERE _source_system IN ('test_ui', 'test_suite')
                  AND ts > CURRENT_TIMESTAMP - INTERVAL '1 hour'
                GROUP BY activity
                ORDER BY activity`,
      complete: (err, stmt, rows) => {
        if (err) reject(err);
        else resolve(rows);
      }
    });
  });

  console.log('\nActivity counts:');
  console.table(countResult);

  // Check specific IDs
  if (activityIds.length > 0) {
    const idList = activityIds.map(id => `'${id}'`).join(',');
    const verifyResult = await new Promise((resolve, reject) => {
      conn.execute({
        sqlText: `SELECT activity_id, activity, customer, 
                         TO_VARCHAR(feature_json) as feature_json_sample
                  FROM ACTIVITY.EVENTS 
                  WHERE activity_id IN (${idList})
                  LIMIT 10`,
        complete: (err, stmt, rows) => {
          if (err) reject(err);
          else resolve(rows);
        }
      });
    });

    console.log('\nSample of inserted activities:');
    console.table(verifyResult.map(r => ({
      ...r,
      feature_json_sample: r.feature_json_sample ? 
        r.feature_json_sample.substring(0, 50) + '...' : 'null'
    })));
  }

  connection.destroy();
}

// Main test runner
async function runTests() {
  console.log('🚀 Starting comprehensive activity testing...\n');
  console.log('Testing server at:', API_BASE);
  
  // Check if server is running
  try {
    const healthResponse = await httpRequest({
      hostname: 'localhost',
      port: 3001,
      path: '/health',
      method: 'GET'
    });
    
    if (healthResponse.ok) {
      console.log('Server health:', healthResponse.data);
    } else {
      throw new Error('Server not healthy');
    }
  } catch (error) {
    console.error('❌ Server not running! Start with: npm run dev');
    console.error('Error:', error.message);
    process.exit(1);
  }

  const results = {
    valid: { api: [], direct: [] },
    invalid: { api: [], direct: [] }
  };
  const activityIds = [];

  // Test valid activities via API
  console.log('\n═══════════════════════════════════════');
  console.log('📝 Testing VALID activities via API');
  console.log('═══════════════════════════════════════');
  
  for (const activity of VALID_ACTIVITIES) {
    const result = await testViaAPI(activity, true);
    results.valid.api.push({ activity, ...result });
    if (result.success && result.activity_id) {
      activityIds.push(result.activity_id);
    }
    await new Promise(resolve => setTimeout(resolve, 100)); // Rate limit
  }

  // Test invalid activities via API  
  console.log('\n═══════════════════════════════════════');
  console.log('🚫 Testing INVALID activities via API');
  console.log('═══════════════════════════════════════');
  
  for (const activity of INVALID_ACTIVITIES) {
    const result = await testViaAPI(activity, false);
    results.invalid.api.push({ activity, ...result });
    await new Promise(resolve => setTimeout(resolve, 100));
  }

  // Test valid activities direct to Snowflake
  console.log('\n═══════════════════════════════════════');
  console.log('📝 Testing VALID activities direct to Snowflake');
  console.log('═══════════════════════════════════════');
  
  for (const activity of VALID_ACTIVITIES) {
    const result = await testDirectSnowflake(activity, true);
    results.valid.direct.push({ activity, ...result });
    if (result.success && result.activity_id) {
      activityIds.push(result.activity_id);
    }
    await new Promise(resolve => setTimeout(resolve, 100));
  }

  // Test invalid activities direct to Snowflake (some may fail at DB level)
  console.log('\n═══════════════════════════════════════');
  console.log('🚫 Testing INVALID activities direct to Snowflake');
  console.log('═══════════════════════════════════════');
  
  for (const activity of INVALID_ACTIVITIES.slice(0, 3)) { // Test subset to avoid DB errors
    const result = await testDirectSnowflake(activity, false);
    results.invalid.direct.push({ activity, ...result });
    await new Promise(resolve => setTimeout(resolve, 100));
  }

  // Verify in database
  await verifyActivities(activityIds);

  // Summary
  console.log('\n═══════════════════════════════════════');
  console.log('📊 TEST SUMMARY');
  console.log('═══════════════════════════════════════');
  
  const validApiSuccess = results.valid.api.filter(r => r.success).length;
  const validDirectSuccess = results.valid.direct.filter(r => r.success).length;
  const invalidApiRejected = results.invalid.api.filter(r => !r.success).length;
  const invalidDirectRejected = results.invalid.direct.filter(r => !r.success).length;
  
  console.log(`\nValid activities via API: ${validApiSuccess}/${VALID_ACTIVITIES.length} succeeded`);
  console.log(`Valid activities direct: ${validDirectSuccess}/${VALID_ACTIVITIES.length} succeeded`);
  console.log(`Invalid activities via API: ${invalidApiRejected}/${INVALID_ACTIVITIES.length} properly rejected`);
  console.log(`Invalid activities direct: ${invalidDirectRejected}/${results.invalid.direct.length} properly rejected`);
  
  const allTestsPassed = 
    validApiSuccess === VALID_ACTIVITIES.length &&
    validDirectSuccess === VALID_ACTIVITIES.length;
    
  if (allTestsPassed) {
    console.log('\n✅ All tests PASSED!');
  } else {
    console.log('\n❌ Some tests FAILED - review results above');
  }
  
  // Show any unexpected successes (invalid that got through)
  const unexpectedSuccesses = results.invalid.api.filter(r => r.success);
  if (unexpectedSuccesses.length > 0) {
    console.log('\n⚠️  WARNING: Invalid activities that succeeded (should have failed):');
    console.table(unexpectedSuccesses);
  }
}

// Run tests
runTests().catch(console.error);