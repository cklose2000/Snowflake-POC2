#!/usr/bin/env node

/**
 * Fix role grants
 */

const snowflake = require('snowflake-sdk');
const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '../../.env') });

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
          console.log(`✅ ${description}`);
          resolve(rows);
        }
      }
    });
  });
}

async function fix() {
  try {
    await new Promise((resolve, reject) => {
      connection.connect(err => err ? reject(err) : resolve());
    });
    console.log('✅ Connected\n');

    // Fix role grants
    await executeSql('GRANT ROLE MCP_SERVICE_ROLE TO ROLE MCP_ADMIN_ROLE', 'Granted service role to admin');
    await executeSql('GRANT ROLE MCP_ADMIN_ROLE TO ROLE ACCOUNTADMIN', 'Granted admin role to accountadmin');
    await executeSql('GRANT ROLE MCP_SERVICE_ROLE TO ROLE ACCOUNTADMIN', 'Granted service role to accountadmin');
    await executeSql('GRANT ROLE MCP_USER_ROLE TO ROLE ACCOUNTADMIN', 'Granted user role to accountadmin');
    
    // Grant Dynamic Table permissions
    await executeSql('GRANT SELECT ON DYNAMIC TABLE CLAUDE_BI.ACTIVITY.EVENTS TO ROLE MCP_SERVICE_ROLE', 'Granted DT select to service role');

    console.log('\n✨ Roles fixed!');

  } catch (err) {
    console.error('❌ Failed:', err.message);
  } finally {
    connection.destroy();
  }
}

fix().catch(console.error);