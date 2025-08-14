#!/usr/bin/env node

/**
 * Update Views with UPPERCASE columns
 */

require('dotenv').config();
const SnowflakeClient = require('../src/snowflake-client');

async function updateViews() {
  let conn = null;
  
  try {
    console.log('🔄 Updating views with UPPERCASE columns...\n');
    
    // Connect to Snowflake
    conn = await SnowflakeClient.connect();
    console.log('✅ Connected to Snowflake');
    
    // Set context
    await SnowflakeClient.execute(conn, 'USE DATABASE CLAUDE_BI');
    await SnowflakeClient.execute(conn, 'USE SCHEMA ACTIVITY_CCODE');
    console.log('✅ Context set to ACTIVITY_CCODE');
    
    // Update VW_ACTIVITY_COUNTS_24H
    const sql1 = `
      CREATE OR REPLACE VIEW VW_ACTIVITY_COUNTS_24H AS
      SELECT 
        DATE_TRUNC('hour', ts) AS HOUR,
        activity AS ACTIVITY,
        COUNT(*) AS EVENT_COUNT,
        COUNT(DISTINCT customer) AS UNIQUE_CUSTOMERS
      FROM CLAUDE_BI.ACTIVITY.EVENTS
      WHERE ts >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
      GROUP BY HOUR, ACTIVITY
    `;
    
    await SnowflakeClient.execute(conn, sql1);
    console.log('✅ Updated VW_ACTIVITY_COUNTS_24H');
    
    // Update VW_ACTIVITY_SUMMARY
    const sql2 = `
      CREATE OR REPLACE VIEW VW_ACTIVITY_SUMMARY AS
      SELECT 
        COUNT(*) AS TOTAL_EVENTS,
        COUNT(DISTINCT customer) AS UNIQUE_CUSTOMERS,
        COUNT(DISTINCT activity) AS UNIQUE_ACTIVITIES,
        MAX(ts) AS LAST_EVENT
      FROM CLAUDE_BI.ACTIVITY.EVENTS
      WHERE ts >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
    `;
    
    await SnowflakeClient.execute(conn, sql2);
    console.log('✅ Updated VW_ACTIVITY_SUMMARY');
    
    // Verify columns
    const verify = `
      SELECT COLUMN_NAME
      FROM INFORMATION_SCHEMA.COLUMNS
      WHERE TABLE_CATALOG = 'CLAUDE_BI'
        AND TABLE_SCHEMA = 'ACTIVITY_CCODE'
        AND TABLE_NAME = 'VW_ACTIVITY_COUNTS_24H'
      ORDER BY ORDINAL_POSITION
    `;
    
    const result = await SnowflakeClient.execute(conn, verify);
    console.log('\n📊 VW_ACTIVITY_COUNTS_24H columns:');
    result.rows.forEach(r => console.log(`   - ${r.COLUMN_NAME}`));
    
    console.log('\n✅ Views updated successfully with UPPERCASE columns!');
    
  } catch (error) {
    console.error('❌ Error updating views:', error.message);
    process.exit(1);
  } finally {
    if (conn) {
      await SnowflakeClient.disconnect(conn);
    }
  }
}

updateViews().catch(console.error);