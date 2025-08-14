#!/usr/bin/env node

// Test Dashboard Factory with natural language input
// This simulates what happens when a user types a dashboard request

const DashboardFactory = require('./packages/dashboard-factory');
const snowflake = require('snowflake-sdk');
require('dotenv').config();

async function testNaturalLanguageDashboard() {
  console.log('🧪 Testing Natural Language Dashboard Creation\n');
  console.log('=' .repeat(60));
  
  // Connect to Snowflake
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
      if (err) {
        console.error('❌ Connection failed:', err.message);
        reject(err);
      } else {
        console.log('✅ Connected to Snowflake');
        resolve(conn);
      }
    });
  });

  // Create Dashboard Factory
  const dashboardFactory = new DashboardFactory(connection, {
    dryRun: false,
    timeout: 60000
  });

  // Test various natural language inputs
  const testCases = [
    {
      input: "write me a report of events by hour for the last 24 hours",
      expected: "Should create a timeseries dashboard"
    },
    {
      input: "create a dashboard showing activity breakdown by customer",
      expected: "Should create an activity breakdown dashboard"
    },
    {
      input: "show me dashboard operations over time",
      expected: "Should create a dashboard operations timeseries"
    }
  ];

  for (const testCase of testCases) {
    console.log('\n' + '='.repeat(60));
    console.log(`\n📝 Test Input: "${testCase.input}"`);
    console.log(`   Expected: ${testCase.expected}`);
    console.log('\n' + '-'.repeat(60));
    
    // Create conversation history as the integrated server would
    const conversationHistory = [
      { role: 'user', content: testCase.input }
    ];
    
    const customerID = `test_user_${Date.now()}`;
    const sessionID = `test_session_${Date.now()}`;
    
    try {
      console.log('\n⏳ Creating dashboard...');
      const result = await dashboardFactory.createDashboard(
        conversationHistory,
        customerID,
        sessionID
      );
      
      if (result.success) {
        console.log('\n✅ Dashboard created successfully!');
        console.log(`   Name: ${result.name}`);
        console.log(`   Spec ID: ${result.specId}`);
        console.log(`   Panels: ${result.panelsCount}`);
        console.log(`   Objects: ${result.objectsCreated}`);
        console.log(`   Time: ${(result.creationTimeMs / 1000).toFixed(1)}s`);
        console.log(`   URL: ${result.url}`);
      } else {
        console.error('\n❌ Dashboard creation failed:');
        console.error(`   Error: ${result.error}`);
        console.error(`   Time: ${(result.creationTimeMs / 1000).toFixed(1)}s`);
      }
    } catch (error) {
      console.error('\n💥 Unexpected error:', error.message);
      console.error(error.stack);
    }
    
    // Small delay between tests
    await new Promise(resolve => setTimeout(resolve, 2000));
  }

  // Check Activity log for our creations
  console.log('\n' + '='.repeat(60));
  console.log('\n📊 Checking Activity Log...\n');
  
  const activitySQL = `
    SELECT 
      activity,
      customer,
      feature_json:step::STRING as step,
      feature_json:error::STRING as error,
      ts
    FROM CLAUDE_BI.ACTIVITY.EVENTS
    WHERE activity LIKE 'ccode.dashboard_%'
      AND ts >= DATEADD('minute', -5, CURRENT_TIMESTAMP())
    ORDER BY ts DESC
    LIMIT 20
  `;
  
  const activityResult = await new Promise((resolve, reject) => {
    connection.execute({
      sqlText: activitySQL,
      complete: (err, stmt, rows) => {
        if (err) reject(err);
        else resolve(rows);
      }
    });
  });
  
  console.log('Recent dashboard activities:');
  activityResult.forEach(row => {
    const status = row.ERROR ? '❌' : '✅';
    console.log(`${status} ${row.ACTIVITY} - ${row.STEP || 'N/A'} (${row.CUSTOMER})`);
    if (row.ERROR) {
      console.log(`   Error: ${row.ERROR}`);
    }
  });
  
  // Disconnect
  connection.destroy();
  console.log('\n✅ Test complete');
}

// Run test
testNaturalLanguageDashboard().catch(console.error);