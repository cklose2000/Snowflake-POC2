// Test SafeSQL Template Engine
const snowflake = require('snowflake-sdk');
const SafeSQLTemplateEngine = require('./packages/safesql/template-engine-cjs');
require('dotenv').config();

async function testTemplates() {
  console.log('🧪 Testing SafeSQL Template Engine...\n');

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

  const conn = await new Promise((resolve, reject) => {
    connection.connect((err, conn) => {
      if (err) reject(err);
      else {
        console.log('✅ Connected to Snowflake\n');
        resolve(conn);
      }
    });
  });

  const engine = new SafeSQLTemplateEngine(conn);

  // Test 1: List available templates
  console.log('═══════════════════════════════════════');
  console.log('📋 Available Templates');
  console.log('═══════════════════════════════════════');
  const templates = engine.getTemplateList();
  console.table(templates.map(t => ({
    name: t.name,
    params: t.params.length,
    maxRows: t.maxRows,
    selectStar: t.allowSelectStar || false
  })));

  // Test 2: Describe table
  console.log('\n═══════════════════════════════════════');
  console.log('🔍 Test: describe_table');
  console.log('═══════════════════════════════════════');
  try {
    const result = await engine.execute('describe_table', {
      schema: 'ACTIVITY',
      table: 'EVENTS'
    });
    console.log(`✅ Success: ${result.count} columns found`);
    console.table(result.rows.slice(0, 5));
  } catch (error) {
    console.error('❌ Failed:', error.message);
  }

  // Test 3: Sample top
  console.log('\n═══════════════════════════════════════');
  console.log('🔍 Test: sample_top');
  console.log('═══════════════════════════════════════');
  try {
    const result = await engine.execute('sample_top', {
      schema: 'ACTIVITY',
      table: 'EVENTS',
      n: 5
    });
    console.log(`✅ Success: ${result.count} rows returned`);
    if (result.rows.length > 0) {
      console.log('Sample row:', {
        activity_id: result.rows[0].ACTIVITY_ID,
        activity: result.rows[0].ACTIVITY,
        customer: result.rows[0].CUSTOMER
      });
    }
  } catch (error) {
    console.error('❌ Failed:', error.message);
  }

  // Test 4: Recent activities
  console.log('\n═══════════════════════════════════════');
  console.log('🔍 Test: recent_activities');
  console.log('═══════════════════════════════════════');
  try {
    const result = await engine.execute('recent_activities', {
      hours: 1,
      limit: 10
    });
    console.log(`✅ Success: ${result.count} activities in last hour`);
    if (result.rows.length > 0) {
      console.table(result.rows.map(r => ({
        activity: r.ACTIVITY,
        customer: r.CUSTOMER,
        source: r._SOURCE_SYSTEM
      })));
    }
  } catch (error) {
    console.error('❌ Failed:', error.message);
  }

  // Test 5: Activity by type
  console.log('\n═══════════════════════════════════════');
  console.log('🔍 Test: activity_by_type');
  console.log('═══════════════════════════════════════');
  try {
    const result = await engine.execute('activity_by_type', {
      hours: 24
    });
    console.log(`✅ Success: ${result.count} activity types found`);
    if (result.rows.length > 0) {
      console.table(result.rows);
    }
  } catch (error) {
    console.error('❌ Failed:', error.message);
  }

  // Test 6: Validation
  console.log('\n═══════════════════════════════════════');
  console.log('✓ Test: Template Validation');
  console.log('═══════════════════════════════════════');
  
  // Valid params
  let validation = engine.validateTemplate('sample_top', {
    schema: 'ACTIVITY',
    table: 'EVENTS',
    n: 10
  });
  console.log('Valid params:', validation);

  // Missing params
  validation = engine.validateTemplate('sample_top', {
    schema: 'ACTIVITY'
  });
  console.log('Missing params:', validation);

  // Invalid template
  validation = engine.validateTemplate('invalid_template', {});
  console.log('Invalid template:', validation);

  // Test 7: Activity summary
  console.log('\n═══════════════════════════════════════');
  console.log('🔍 Test: activity_summary');
  console.log('═══════════════════════════════════════');
  try {
    const result = await engine.execute('activity_summary', {
      hours: 24
    });
    console.log(`✅ Success: Summary retrieved`);
    if (result.rows.length > 0) {
      console.table(result.rows);
    }
  } catch (error) {
    console.error('❌ Failed:', error.message);
  }

  connection.destroy();
  console.log('\n🎉 Template engine tests complete!');
}

testTemplates().catch(console.error);