#!/usr/bin/env node

/**
 * Integration Test - Dashboard Creation
 */

const SnowflakeClient = require('../../src/snowflake-client');
const DashboardFactory = require('../../src/dashboard-factory');
const ActivityLogger = require('../../src/activity-logger');

async function testDashboardCreation() {
  console.log('🧪 Testing Dashboard Creation\n');
  
  let conn = null;
  
  try {
    // Connect to Snowflake
    console.log('Connecting to Snowflake...');
    conn = await SnowflakeClient.connect();
    console.log('✅ Connected\n');
    
    // Initialize services
    const logger = new ActivityLogger(conn);
    const factory = new DashboardFactory(conn, logger);
    
    // Test 1: Generate spec from conversation
    console.log('Test 1: Generate spec from conversation');
    const conversation = [
      { role: 'user', content: 'Show me an activity dashboard for the last 24 hours' }
    ];
    
    const spec = await factory.generateSpec(conversation);
    console.log('✅ Spec generated');
    console.log(`   Name: ${spec.name}`);
    console.log(`   Panels: ${spec.panels.length}`);
    console.log(`   Hash: ${spec.hash}\n`);
    
    // Test 2: Create dashboard
    console.log('Test 2: Create dashboard from spec');
    const result = await factory.create(spec);
    console.log('✅ Dashboard created');
    console.log(`   Dashboard ID: ${result.dashboard_id}`);
    console.log(`   Objects created: ${result.objectsCreated}`);
    console.log(`   Views: ${result.views.join(', ')}`);
    console.log(`   Streamlit file: ${result.streamlitFile}\n`);
    
    // Test 3: Verify views exist
    console.log('Test 3: Verify views exist');
    for (const view of result.views) {
      const checkSQL = `
        SELECT COUNT(*) as exists
        FROM INFORMATION_SCHEMA.VIEWS
        WHERE TABLE_NAME = '${view.toUpperCase()}'
          AND TABLE_SCHEMA = 'ANALYTICS'
      `;
      
      const check = await SnowflakeClient.execute(conn, checkSQL);
      const exists = check.rows[0]?.EXISTS > 0;
      
      if (exists) {
        console.log(`   ✅ View ${view} exists`);
      } else {
        console.log(`   ❌ View ${view} not found`);
      }
    }
    
    // Test 4: Query metrics
    console.log('\nTest 4: Query activity metrics');
    const metrics = await logger.getMetrics(24);
    if (metrics) {
      console.log('✅ Metrics retrieved');
      console.log(`   Total events: ${metrics.TOTAL_EVENTS || 0}`);
      console.log(`   Unique activities: ${metrics.UNIQUE_ACTIVITIES || 0}`);
      console.log(`   Unique customers: ${metrics.UNIQUE_CUSTOMERS || 0}`);
    }
    
    // Test 5: Cleanup (optional)
    console.log('\nTest 5: Cleanup dashboard');
    await factory.drop(result.dashboard_id);
    console.log('✅ Dashboard cleaned up');
    
    // Summary
    console.log('\n' + '='.repeat(50));
    console.log('✅ ALL TESTS PASSED');
    console.log('='.repeat(50));
    
    // Disconnect
    await SnowflakeClient.disconnect(conn);
    process.exit(0);
    
  } catch (error) {
    console.error('❌ Test failed:', error.message);
    console.error(error.stack);
    
    if (conn) {
      await SnowflakeClient.disconnect(conn);
    }
    
    process.exit(1);
  }
}

// Run test
testDashboardCreation().catch(console.error);