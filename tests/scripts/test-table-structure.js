#!/usr/bin/env node

const snowflake = require('snowflake-sdk');
require('dotenv').config();

const connection = snowflake.createConnection({
  account: process.env.SNOWFLAKE_ACCOUNT,
  username: process.env.SNOWFLAKE_USERNAME,
  password: process.env.SNOWFLAKE_PASSWORD,
  database: 'CLAUDE_BI',
  warehouse: process.env.SNOWFLAKE_WAREHOUSE || 'CLAUDE_WAREHOUSE'
});

async function testTableStructure() {
  console.log('🔍 Checking RAW_EVENTS table structure...\n');

  try {
    await new Promise((resolve, reject) => {
      connection.connect((err) => {
        if (err) reject(err);
        else resolve();
      });
    });

    // Check table structure
    const describeResult = await new Promise((resolve, reject) => {
      connection.execute({
        sqlText: "DESCRIBE TABLE CLAUDE_BI.LANDING.RAW_EVENTS",
        complete: (err, stmt, rows) => {
          if (err) reject(err);
          else resolve(rows);
        }
      });
    });

    console.log('📋 RAW_EVENTS Table Structure:');
    describeResult.forEach(row => {
      console.log(`  ${row.name}: ${row.type} (null: ${row.null})`);
    });

    // Test proper insert format
    console.log('\n🧪 Testing correct insert format...');
    
    const testEvent = {
      event_id: 'test_123',
      action: 'test.action',
      actor_id: 'admin',
      occurred_at: new Date().toISOString()
    };

    const insertResult = await new Promise((resolve, reject) => {
      connection.execute({
        sqlText: "INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS VALUES (PARSE_JSON(?), 'SYSTEM', ?)",
        binds: [JSON.stringify(testEvent), new Date().toISOString()],
        complete: (err, stmt, rows) => {
          if (err) reject(err);
          else resolve(rows);
        }
      });
    });

    console.log('✅ Insert test successful');

  } catch (error) {
    console.error('❌ Test failed:', error.message);
  } finally {
    await new Promise((resolve) => {
      connection.destroy(() => resolve());
    });
  }
}

testTableStructure().catch(console.error);