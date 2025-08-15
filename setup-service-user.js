#!/usr/bin/env node

/**
 * Setup MCP_SERVICE_USER with key-pair authentication
 */

const snowflake = require('snowflake-sdk');
require('dotenv').config();

const PUBLIC_KEY = 'MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAkswDXIJx4rXOcI30kSmRaH89uef2UOAxDoTu1BAXAuFXHBaE80joS2lZ0fPqnv0v/1B6nUd8BK+f2ut3aIJWf9JpiZDYkGbu3lXAWHHuk9qapu2JP1tbnyuzhhJpjVBvaMxTd77bG8lXDAEodnAfrQFzxg6cX/O6K3hZNl73cZ3zTvkMWZckq3v9As7fAaPGtN0gOOLyp1h3oTDQ3ToC3N+YRMm+lbcYrRzrF6cnI+aZEtvFzVA4B+/bduuQr5uxzVoa9Dg7cE+Xh8My4hVB4bzJXbHqDRRtOkWE8FIFJkzEF+itm2imuz2hkI4N0KXiUD0oEbphlR+i0KFQenpkkwIDAQAB';

async function setupServiceUser() {
  console.log('🔐 Setting up MCP_SERVICE_USER with key-pair auth...\n');

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

    // Create service user if not exists
    console.log('\n📋 Creating service user...');
    
    try {
      await executeSql(`
        CREATE USER IF NOT EXISTS MCP_SERVICE_USER
        PASSWORD = 'temp_password_will_be_removed'
        DEFAULT_ROLE = 'MCP_SERVICE_ROLE'
        DEFAULT_WAREHOUSE = 'MCP_XS_WH'
        COMMENT = 'Service user for MCP Snowflake integration'
      `, 'Created MCP_SERVICE_USER');
    } catch (e) {
      console.log('  ⚠️  User might already exist');
    }

    // Set the public key
    console.log('\n🔑 Setting RSA public key...');
    
    await executeSql(`
      ALTER USER MCP_SERVICE_USER SET RSA_PUBLIC_KEY='${PUBLIC_KEY}'
    `, 'Set RSA public key');

    // Grant role
    await executeSql(`
      GRANT ROLE MCP_SERVICE_ROLE TO USER MCP_SERVICE_USER
    `, 'Granted MCP_SERVICE_ROLE');

    // Verify the setup
    console.log('\n🧪 Verifying setup...');
    
    const userInfo = await executeSql(`
      DESCRIBE USER MCP_SERVICE_USER
    `, 'Describe user');

    console.log('✅ Service user setup complete!');
    console.log('\n📋 Environment variables to set:');
    console.log('export SNOWFLAKE_ACCOUNT="uec18397.us-east-1"');
    console.log('export MCP_SERVICE_USER="MCP_SERVICE_USER"');
    console.log('export SF_PK_PATH="$HOME/.snowflake/mcp_rsa_key.p8"');
    console.log('export SF_PK_PASSPHRASE="CHANGE_THIS_PASSPHRASE"');
    console.log('export MCP_SERVICE_ROLE="MCP_SERVICE_ROLE"');
    console.log('export MCP_SERVICE_WAREHOUSE="CLAUDE_WAREHOUSE"');
    console.log('export SNOWFLAKE_DATABASE="CLAUDE_BI"');
    console.log('unset MCP_SERVICE_PASSWORD');

  } catch (error) {
    console.error('❌ Setup failed:', error.message);
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

setupServiceUser().catch(console.error);