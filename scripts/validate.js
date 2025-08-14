#!/usr/bin/env node

/**
 * Validation Script - Check system health
 */

const SnowflakeClient = require('../src/snowflake-client');
const SchemaContract = require('../src/schema-contract');
const ActivityLogger = require('../src/activity-logger');

async function validate() {
  console.log('🔍 Validating Snowflake POC2 System\n');
  
  let conn = null;
  const results = {
    connection: false,
    schema: false,
    activity: false,
    views: false
  };

  try {
    // 1. Test Snowflake connection
    console.log('Testing Snowflake connection...');
    conn = await SnowflakeClient.connect();
    results.connection = true;
    console.log('✅ Snowflake connected\n');

    // 2. Validate schema contract
    console.log('Validating schema contract...');
    const validation = await SchemaContract.validate(conn);
    results.schema = validation.valid;
    
    if (validation.valid) {
      console.log('✅ Schema contract valid');
    } else {
      console.log('⚠️ Schema issues:', validation.issues);
    }
    console.log(`   Contract hash: ${SchemaContract.getHash()}\n`);

    // 3. Test Activity logging
    console.log('Testing Activity logging...');
    const logger = new ActivityLogger(conn);
    const activityId = await logger.log('system_validation', {
      timestamp: new Date().toISOString()
    });
    
    if (activityId) {
      results.activity = true;
      console.log('✅ Activity logging working');
      console.log(`   Activity ID: ${activityId}\n`);
    } else {
      console.log('⚠️ Activity logging failed\n');
    }

    // 4. Check Activity views
    console.log('Checking Activity views...');
    const viewCheck = await SnowflakeClient.execute(conn, `
      SELECT COUNT(*) as view_count
      FROM INFORMATION_SCHEMA.VIEWS
      WHERE TABLE_SCHEMA = 'ACTIVITY_CCODE'
    `);
    
    const viewCount = viewCheck.rows[0]?.VIEW_COUNT || 0;
    results.views = viewCount >= 2;
    
    if (results.views) {
      console.log(`✅ Activity views found: ${viewCount}`);
    } else {
      console.log(`⚠️ Missing Activity views (found ${viewCount}, need at least 2)`);
    }

    // 5. Test query execution
    console.log('\nTesting query execution...');
    const testQuery = await SnowflakeClient.executeTemplate(
      conn,
      'sample_top',
      { limit: 5 }
    );
    
    console.log(`✅ Query executed: ${testQuery.rowCount} rows returned`);

    // Summary
    console.log('\n' + '='.repeat(50));
    console.log('VALIDATION SUMMARY');
    console.log('='.repeat(50));
    
    const allPassed = Object.values(results).every(v => v === true);
    
    Object.entries(results).forEach(([key, value]) => {
      const status = value ? '✅' : '❌';
      console.log(`${status} ${key.toUpperCase()}: ${value ? 'PASSED' : 'FAILED'}`);
    });

    if (allPassed) {
      console.log('\n🎉 All validations passed! System is ready.');
    } else {
      console.log('\n⚠️ Some validations failed. Please check the issues above.');
    }

    // Cleanup
    if (conn) {
      await SnowflakeClient.disconnect(conn);
    }

    process.exit(allPassed ? 0 : 1);

  } catch (error) {
    console.error('❌ Validation failed:', error.message);
    
    if (conn) {
      await SnowflakeClient.disconnect(conn);
    }
    
    process.exit(1);
  }
}

// Run validation
validate().catch(console.error);