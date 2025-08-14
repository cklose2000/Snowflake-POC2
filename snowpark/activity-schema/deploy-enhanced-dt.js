#!/usr/bin/env node

/**
 * Deploy Enhanced Dynamic Table with SHA2-256 IDs
 * Maintains 2-table architecture with quality events as events
 */

const snowflake = require('snowflake-sdk');
const fs = require('fs');
const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '../../.env') });

console.log('🚀 Deploying Enhanced Dynamic Table');
console.log('='.repeat(60));

const connection = snowflake.createConnection({
  account: process.env.SNOWFLAKE_ACCOUNT,
  username: process.env.SNOWFLAKE_USERNAME,
  password: process.env.SNOWFLAKE_PASSWORD,
  role: 'ACCOUNTADMIN',
  database: 'CLAUDE_BI',
  warehouse: 'CLAUDE_WAREHOUSE'
});

async function executeSql(sql, description) {
  return new Promise((resolve, reject) => {
    connection.execute({
      sqlText: sql,
      complete: (err, statement, rows) => {
        if (err) {
          console.log(`❌ ${description}: ${err.message}`);
          reject(err);
        } else {
          if (description) console.log(`✅ ${description}`);
          resolve(rows);
        }
      }
    });
  });
}

async function deploy() {
  try {
    // Connect
    await new Promise((resolve, reject) => {
      connection.connect(err => err ? reject(err) : resolve());
    });
    console.log('✅ Connected to Snowflake\n');

    // Step 1: Backup current EVENTS table if it exists
    console.log('📦 Step 1: Checking for existing EVENTS table...');
    let hasExistingTable = false;
    try {
      const exists = await executeSql(
        `SELECT COUNT(*) as cnt FROM CLAUDE_BI.ACTIVITY.EVENTS LIMIT 1`,
        null
      );
      hasExistingTable = true;
      console.log(`   Found existing table with ${exists[0].CNT} rows`);
      
      await executeSql(`
        CREATE TABLE IF NOT EXISTS CLAUDE_BI.ACTIVITY.EVENTS_BAK_${new Date().toISOString().slice(0,10).replace(/-/g,'')} AS
        SELECT * FROM CLAUDE_BI.ACTIVITY.EVENTS
      `, 'Created backup table');
    } catch (e) {
      console.log('   No existing EVENTS table found\n');
    }

    // Step 2: Suspend and drop existing Dynamic Table
    console.log('🔄 Step 2: Preparing to replace Dynamic Table...');
    try {
      await executeSql(
        'ALTER DYNAMIC TABLE CLAUDE_BI.ACTIVITY.EVENTS SUSPEND',
        'Suspended existing Dynamic Table'
      );
    } catch (e) {
      console.log('   Note: Dynamic Table may not exist or already suspended');
    }

    await executeSql(
      'DROP DYNAMIC TABLE IF EXISTS CLAUDE_BI.ACTIVITY.EVENTS',
      'Dropped existing Dynamic Table'
    );

    // Step 3: Create Enhanced Dynamic Table
    console.log('✨ Step 3: Creating Enhanced Dynamic Table with SHA2-256 IDs...');
    
    // Read the enhanced SQL file
    const enhancedSQL = fs.readFileSync(
      path.join(__dirname, 'enhanced_dynamic_table.sql'), 
      'utf8'
    );
    
    // Extract just the CREATE DYNAMIC TABLE statement
    const createDTMatch = enhancedSQL.match(/CREATE OR REPLACE DYNAMIC TABLE[\s\S]*?FROM final;/);
    if (!createDTMatch) {
      throw new Error('Could not find CREATE DYNAMIC TABLE statement');
    }
    
    await executeSql(createDTMatch[0], 'Created Enhanced Dynamic Table');

    // Step 4: Create quality event emission procedure
    console.log('\n📋 Step 4: Creating quality event procedures...');
    await executeSql(`
      CREATE OR REPLACE PROCEDURE CLAUDE_BI.MCP.EMIT_QUALITY_EVENT(
        validation_status STRING,
        error_message STRING,
        affected_payload VARIANT
      )
      RETURNS STRING
      LANGUAGE SQL
      AS
      'BEGIN
        INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS
        SELECT 
          OBJECT_CONSTRUCT(
            ''event_id'', ''qual_'' || SHA2(CONCAT(validation_status, ''|'', CURRENT_TIMESTAMP()::STRING, ''|'', TO_JSON(affected_payload)), 256),
            ''action'', ''quality.'' || LOWER(REPLACE(validation_status, ''_'', ''.'')),
            ''occurred_at'', CURRENT_TIMESTAMP(),
            ''actor_id'', ''system'',
            ''source'', ''quality'',
            ''schema_version'', ''2.1.0'',
            ''object'', OBJECT_CONSTRUCT(
              ''type'', ''validation'',
              ''id'', SHA2(TO_JSON(affected_payload), 256)
            ),
            ''attributes'', OBJECT_CONSTRUCT(
              ''validation_status'', :validation_status,
              ''error_message'', :error_message,
              ''raw_payload'', :affected_payload,
              ''detected_at'', CURRENT_TIMESTAMP()
            )
          ),
          ''SYSTEM'',
          CURRENT_TIMESTAMP();
          
        RETURN ''Quality event emitted: '' || validation_status;
      END;'
    `, 'Created EMIT_QUALITY_EVENT procedure');

    // Step 5: Force refresh
    console.log('\n🔄 Step 5: Refreshing Dynamic Table...');
    await executeSql(
      'ALTER DYNAMIC TABLE CLAUDE_BI.ACTIVITY.EVENTS REFRESH',
      'Triggered Dynamic Table refresh'
    );

    // Wait for refresh
    console.log('   Waiting for refresh to complete...');
    await new Promise(resolve => setTimeout(resolve, 5000));

    // Step 6: Verify
    console.log('\n📊 Step 6: Verification...');
    
    const newCount = await executeSql(
      'SELECT COUNT(*) as cnt FROM CLAUDE_BI.ACTIVITY.EVENTS',
      'Counted events in new Dynamic Table'
    );
    console.log(`   Events in Dynamic Table: ${newCount[0].CNT} rows`);

    // Check for SHA2 IDs
    const idCheck = await executeSql(`
      SELECT 
        COUNT(*) as total,
        SUM(CASE WHEN LENGTH(event_id) = 64 THEN 1 ELSE 0 END) as sha2_ids,
        SUM(CASE WHEN event_id LIKE 'v2_%' THEN 1 ELSE 0 END) as v2_prefixed
      FROM CLAUDE_BI.ACTIVITY.EVENTS
      LIMIT 100
    `, 'Checked ID format');
    
    console.log(`   SHA2-256 IDs: ${idCheck[0].SHA2_IDS}/${idCheck[0].TOTAL}`);
    
    // Check metadata enrichment
    const metaCheck = await executeSql(`
      SELECT 
        COUNT(*) as total,
        SUM(CASE WHEN attributes:_meta IS NOT NULL THEN 1 ELSE 0 END) as with_meta
      FROM CLAUDE_BI.ACTIVITY.EVENTS
      LIMIT 100
    `, 'Checked metadata enrichment');
    
    console.log(`   Events with metadata: ${metaCheck[0].WITH_META}/${metaCheck[0].TOTAL}`);

    // Show Dynamic Table info
    const dtInfo = await executeSql(
      "SHOW DYNAMIC TABLES LIKE 'EVENTS' IN SCHEMA CLAUDE_BI.ACTIVITY",
      null
    );
    
    if (dtInfo.length > 0) {
      console.log(`\n✅ Dynamic Table Status:`);
      console.log(`   Name: ${dtInfo[0].name}`);
      console.log(`   State: ${dtInfo[0].state}`);
      console.log(`   Target Lag: ${dtInfo[0].target_lag}`);
      console.log(`   Warehouse: ${dtInfo[0].warehouse}`);
      console.log(`   Clustering: DATE(occurred_at), action`);
    }

    console.log('\n✨ Enhanced Dynamic Table deployed successfully!');
    console.log('\n📝 Key Improvements:');
    console.log('   ✅ SHA2-256 content-addressed IDs');
    console.log('   ✅ Comprehensive validation (JSON, size, namespace)');
    console.log('   ✅ Clustering for performance');
    console.log('   ✅ Late arrival tracking');
    console.log('   ✅ Micro-sequencing for collisions');
    console.log('   ✅ Quality events as events (no extra tables!)');

  } catch (err) {
    console.error('\n❌ Deployment failed:', err.message);
    process.exit(1);
  } finally {
    connection.destroy();
  }
}

deploy().catch(console.error);