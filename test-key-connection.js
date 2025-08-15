#!/usr/bin/env node

/**
 * Test key-pair connection directly
 */

const snowflake = require('snowflake-sdk');
const fs = require('fs');

async function testKeyConnection() {
  console.log('🔐 Testing key-pair connection...\n');
  
  const privateKey = fs.readFileSync(process.env.HOME + '/.snowflake/mcp_rsa_key_unencrypted.p8', 'utf8');
  console.log('📋 Private key format:', privateKey.substring(0, 50) + '...');
  
  const connection = snowflake.createConnection({
    account: 'uec18397.us-east-1',
    username: 'MCP_SERVICE_USER',
    privateKey: privateKey,
    // privateKeyPass: 'CHANGE_THIS_PASSPHRASE', // Not needed for unencrypted key
    authenticator: 'SNOWFLAKE_JWT',
    role: 'MCP_SERVICE_ROLE',
    warehouse: 'CLAUDE_WAREHOUSE',
    database: 'CLAUDE_BI',
    clientSessionKeepAlive: true
  });

  try {
    await new Promise((resolve, reject) => {
      connection.connect((err) => {
        if (err) {
          console.error('❌ Connection failed:', err.message);
          reject(err);
        } else {
          console.log('✅ Connection successful!');
          resolve();
        }
      });
    });

    // Test a simple query
    const result = await new Promise((resolve, reject) => {
      connection.execute({
        sqlText: 'SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE()',
        complete: (err, stmt, rows) => {
          if (err) reject(err);
          else resolve(rows);
        }
      });
    });

    console.log('📊 Connection info:', result[0]);

  } catch (error) {
    console.error('❌ Test failed:', error.message);
  } finally {
    await new Promise((resolve) => {
      connection.destroy(() => resolve());
    });
  }
}

testKeyConnection().catch(console.error);