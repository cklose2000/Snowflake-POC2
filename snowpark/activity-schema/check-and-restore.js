#!/usr/bin/env node

/**
 * Check backup table structure and restore data properly
 */

const snowflake = require('snowflake-sdk');
const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '../../.env') });

console.log('🔍 Checking backup table and restoring data');
console.log('='.repeat(60));

const connection = snowflake.createConnection({
  account: process.env.SNOWFLAKE_ACCOUNT,
  username: process.env.SNOWFLAKE_USERNAME,
  password: process.env.SNOWFLAKE_PASSWORD,
  role: 'ACCOUNTADMIN',
  database: 'CLAUDE_BI',
  schema: 'ACTIVITY',
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

async function restore() {
  try {
    // Connect
    await new Promise((resolve, reject) => {
      connection.connect(err => err ? reject(err) : resolve());
    });
    console.log('✅ Connected to Snowflake\n');

    // Check backup table structure
    console.log('📋 Checking backup table structure...');
    const columns = await executeSql(`
      SELECT COLUMN_NAME, DATA_TYPE, ORDINAL_POSITION
      FROM INFORMATION_SCHEMA.COLUMNS
      WHERE TABLE_CATALOG = 'CLAUDE_BI'
        AND TABLE_SCHEMA = 'ACTIVITY'
        AND TABLE_NAME = 'EVENTS_BAK'
      ORDER BY ORDINAL_POSITION
    `);
    
    console.log('Backup table columns:');
    columns.forEach(c => {
      console.log(`  ${c.ORDINAL_POSITION}. ${c.COLUMN_NAME} (${c.DATA_TYPE})`);
    });

    // Check if backup has old Activity schema columns
    const hasActivityId = columns.some(c => c.COLUMN_NAME === 'ACTIVITY_ID');
    const hasEventId = columns.some(c => c.COLUMN_NAME === 'EVENT_ID');
    
    console.log(`\nBackup table type: ${hasActivityId ? 'Old Activity Schema' : 'New Event Schema'}`);

    // Restore based on schema type
    if (hasActivityId) {
      // Old Activity Schema - need to transform
      console.log('\n📥 Restoring from old Activity Schema format...');
      const insertSql = `
        INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS
        SELECT 
          OBJECT_CONSTRUCT(
            'event_id', ACTIVITY_ID,
            'occurred_at', TS,
            'actor_id', CUSTOMER,
            'action', ACTIVITY,
            'object', OBJECT_CONSTRUCT(
              'type', CASE 
                WHEN ACTIVITY LIKE 'order.%' THEN 'order'
                WHEN ACTIVITY LIKE 'user.%' THEN 'user'
                WHEN ACTIVITY LIKE 'payment.%' THEN 'payment'
                WHEN ACTIVITY LIKE 'ccode.%' THEN 'query'
                WHEN ACTIVITY LIKE 'mcp.%' THEN 'query'
                ELSE 'unknown'
              END,
              'id', COALESCE(LINK, ACTIVITY_ID)
            ),
            'source', COALESCE(_SOURCE_SYSTEM, 'legacy'),
            'schema_version', COALESCE(_SOURCE_VERSION, '2.0.0'),
            'attributes', FEATURE_JSON
          ) AS payload,
          'RESTORED',
          CURRENT_TIMESTAMP()
        FROM CLAUDE_BI.ACTIVITY.EVENTS_BAK
      `;
      await executeSql(insertSql, 'Inserted old schema data into RAW_EVENTS');
      
    } else if (hasEventId) {
      // New Event Schema - direct restore
      console.log('\n📥 Restoring from new Event Schema format...');
      const insertSql = `
        INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS
        SELECT 
          OBJECT_CONSTRUCT(
            'event_id', EVENT_ID,
            'occurred_at', OCCURRED_AT,
            'actor_id', ACTOR_ID,
            'action', ACTION,
            'object', OBJECT_CONSTRUCT(
              'type', OBJECT_TYPE,
              'id', OBJECT_ID
            ),
            'source', SOURCE,
            'schema_version', SCHEMA_VERSION,
            'attributes', ATTRIBUTES,
            'depends_on_event_id', DEPENDS_ON_EVENT_ID
          ) AS payload,
          _SOURCE_LANE,
          _RECV_AT
        FROM CLAUDE_BI.ACTIVITY.EVENTS_BAK
      `;
      await executeSql(insertSql, 'Inserted new schema data into RAW_EVENTS');
    }

    // Force refresh
    console.log('\n🔄 Refreshing Dynamic Table...');
    await executeSql(
      'ALTER DYNAMIC TABLE CLAUDE_BI.ACTIVITY.EVENTS REFRESH',
      'Triggered Dynamic Table refresh'
    );

    // Wait for refresh
    console.log('   Waiting for refresh to complete...');
    await new Promise(resolve => setTimeout(resolve, 5000));

    // Verify
    console.log('\n📊 Verification:');
    const counts = await executeSql(`
      SELECT 
        'EVENTS_BAK' as table_name,
        COUNT(*) as row_count 
      FROM CLAUDE_BI.ACTIVITY.EVENTS_BAK
      UNION ALL
      SELECT 
        'RAW_EVENTS' as table_name,
        COUNT(*) as row_count 
      FROM CLAUDE_BI.LANDING.RAW_EVENTS
      UNION ALL
      SELECT 
        'EVENTS (Dynamic)' as table_name,
        COUNT(*) as row_count 
      FROM CLAUDE_BI.ACTIVITY.EVENTS
      ORDER BY 1
    `);

    counts.forEach(c => {
      console.log(`   ${c.TABLE_NAME}: ${c.ROW_COUNT} rows`);
    });

    // Sample data from Dynamic Table
    console.log('\n📝 Sample data from Dynamic Table:');
    const sample = await executeSql(`
      SELECT 
        action,
        COUNT(*) as count
      FROM CLAUDE_BI.ACTIVITY.EVENTS
      GROUP BY action
      ORDER BY count DESC
      LIMIT 5
    `);
    
    sample.forEach(s => {
      console.log(`   ${s.ACTION}: ${s.COUNT} events`);
    });

    console.log('\n✨ Data restoration complete!');

  } catch (err) {
    console.error('\n❌ Restore failed:', err.message);
    process.exit(1);
  } finally {
    connection.destroy();
  }
}

restore().catch(console.error);