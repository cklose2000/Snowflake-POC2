#!/usr/bin/env node

/**
 * Fix warehouse access for MCP_SERVICE_USER
 */

const snowflake = require('snowflake-sdk');
require('dotenv').config();

async function fixWarehouseAccess() {
  console.log('🏗️  Fixing warehouse access for MCP_SERVICE_USER...\n');

  const connection = snowflake.createConnection({
    account: process.env.SNOWFLAKE_ACCOUNT,
    username: process.env.SNOWFLAKE_USERNAME,
    password: process.env.SNOWFLAKE_PASSWORD,
    database: 'CLAUDE_BI',
    warehouse: process.env.SNOWFLAKE_WAREHOUSE || 'CLAUDE_WAREHOUSE'
  });

  try {
    await new Promise((resolve, reject) => {
      connection.connect((err) => {
        if (err) reject(err);
        else resolve();
      });
    });

    console.log('✅ Connected to Snowflake as admin');

    // Set context
    await executeSql('USE ROLE ACCOUNTADMIN', 'Set ACCOUNTADMIN role');

    // Check if MCP_XS_WH warehouse exists, if not create it
    console.log('\n🏗️  Setting up MCP warehouse...');
    
    try {
      await executeSql(`
        CREATE WAREHOUSE IF NOT EXISTS MCP_XS_WH 
        WITH WAREHOUSE_SIZE = XSMALL 
        AUTO_SUSPEND = 60 
        AUTO_RESUME = TRUE
        COMMENT = 'Extra small warehouse for MCP operations'
      `, 'Created/verified MCP_XS_WH warehouse');
    } catch (e) {
      console.log('  ⚠️  Warehouse might already exist');
    }

    // Also ensure CLAUDE_WAREHOUSE exists
    try {
      await executeSql(`
        CREATE WAREHOUSE IF NOT EXISTS CLAUDE_WAREHOUSE 
        WITH WAREHOUSE_SIZE = XSMALL 
        AUTO_SUSPEND = 60 
        AUTO_RESUME = TRUE
        COMMENT = 'Warehouse for Claude BI operations'
      `, 'Created/verified CLAUDE_WAREHOUSE');
    } catch (e) {
      console.log('  ⚠️  Warehouse might already exist');
    }

    // Grant usage on warehouses to MCP_SERVICE_ROLE
    console.log('\n🔑 Granting warehouse access...');
    
    await executeSql(`
      GRANT USAGE ON WAREHOUSE MCP_XS_WH TO ROLE MCP_SERVICE_ROLE
    `, 'Granted MCP_XS_WH usage');

    await executeSql(`
      GRANT USAGE ON WAREHOUSE CLAUDE_WAREHOUSE TO ROLE MCP_SERVICE_ROLE
    `, 'Granted CLAUDE_WAREHOUSE usage');

    // Set default warehouse for the user
    await executeSql(`
      ALTER USER MCP_SERVICE_USER SET DEFAULT_WAREHOUSE = 'CLAUDE_WAREHOUSE'
    `, 'Set default warehouse');

    console.log('\n✅ Warehouse access fixed!');
    console.log('\n📋 Updated environment variables:');
    console.log('export MCP_SERVICE_WAREHOUSE="CLAUDE_WAREHOUSE"  # or MCP_XS_WH');

  } catch (error) {
    console.error('❌ Fix failed:', error.message);
  } finally {
    await new Promise((resolve) => {
      connection.destroy(() => resolve());
    });
  }

  async function executeSql(sql, description = '') {
    return new Promise((resolve, reject) => {
      connection.execute({
        sqlText: sql,
        complete: (err, stmt, rows) => {
          if (err) {
            console.log(`  ❌ Failed: ${description}`);
            console.log(`     Error: ${err.message}`);
            reject(err);
          } else {
            if (description) console.log(`  ✅ ${description}`);
            resolve({ stmt, rows });
          }
        }
      });
    });
  }
}

fixWarehouseAccess().catch(console.error);