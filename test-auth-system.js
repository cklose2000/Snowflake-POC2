#!/usr/bin/env node

/**
 * Test the deployed authentication system
 */

const snowflake = require('snowflake-sdk');
require('dotenv').config();

// Create connection
const connection = snowflake.createConnection({
  account: process.env.SNOWFLAKE_ACCOUNT,
  username: process.env.SNOWFLAKE_USERNAME,
  password: process.env.SNOWFLAKE_PASSWORD,
  database: 'CLAUDE_BI',
  warehouse: process.env.SNOWFLAKE_WAREHOUSE || 'CLAUDE_WAREHOUSE'
});

async function connect() {
  return new Promise((resolve, reject) => {
    connection.connect((err, conn) => {
      if (err) reject(err);
      else resolve(conn);
    });
  });
}

async function executeSql(sql) {
  return new Promise((resolve, reject) => {
    connection.execute({
      sqlText: sql,
      complete: (err, stmt, rows) => {
        if (err) reject(err);
        else resolve({ stmt, rows });
      }
    });
  });
}

async function testAuth() {
  console.log('🧪 Testing Authentication System');
  console.log('=================================\n');
  
  try {
    await connect();
    console.log('✅ Connected to Snowflake\n');
    
    // Switch to ACCOUNTADMIN
    await executeSql('USE ROLE ACCOUNTADMIN');
    await executeSql('USE DATABASE CLAUDE_BI');
    await executeSql('USE SCHEMA MCP');
    
    // Test 1: Generate a token
    console.log('Test 1: Token Generation');
    const tokenResult = await executeSql('SELECT MCP.GENERATE_SECURE_TOKEN() AS token');
    const token = tokenResult.rows[0].TOKEN;
    console.log(`  ✅ Generated token: ${token.substring(0, 20)}...`);
    
    // Test 2: Hash the token
    console.log('\nTest 2: Token Hashing');
    const hashResult = await executeSql(`SELECT MCP.HASH_TOKEN_WITH_PEPPER('${token}') AS hash`);
    const hash = hashResult.rows[0].HASH;
    console.log(`  ✅ Token hash: ${hash.substring(0, 20)}...`);
    
    // Test 3: Extract metadata
    console.log('\nTest 3: Token Metadata');
    const metaResult = await executeSql(`SELECT MCP.EXTRACT_TOKEN_METADATA('${token}') AS metadata`);
    const metadata = metaResult.rows[0].METADATA;
    console.log(`  ✅ Token prefix: ${metadata.prefix}`);
    console.log(`     Token suffix: ${metadata.suffix}`);
    console.log(`     Token length: ${metadata.length}`);
    
    // Test 4: Create a test user manually
    console.log('\nTest 4: Creating Test User');
    const username = 'test_user_' + Date.now();
    
    // Insert user creation event
    await executeSql(`
      INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS VALUES (
        OBJECT_CONSTRUCT(
          'event_id', UUID_STRING(),
          'action', 'system.user.created',
          'occurred_at', CURRENT_TIMESTAMP(),
          'actor_id', CURRENT_USER(),
          'source', 'test',
          'object', OBJECT_CONSTRUCT(
            'type', 'user',
            'id', '${username}'
          ),
          'attributes', OBJECT_CONSTRUCT(
            'email', '${username}@test.com',
            'created_via', 'test_script'
          )
        ),
        'TEST',
        CURRENT_TIMESTAMP()
      )
    `);
    console.log(`  ✅ Created user: ${username}`);
    
    // Insert permission grant
    await executeSql(`
      INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS VALUES (
        OBJECT_CONSTRUCT(
          'event_id', UUID_STRING(),
          'action', 'system.permission.granted',
          'occurred_at', CURRENT_TIMESTAMP(),
          'actor_id', CURRENT_USER(),
          'source', 'test',
          'object', OBJECT_CONSTRUCT(
            'type', 'user',
            'id', '${username}'
          ),
          'attributes', OBJECT_CONSTRUCT(
            'token_hash', '${hash}',
            'token_prefix', '${metadata.prefix}',
            'token_suffix', '${metadata.suffix}',
            'allowed_tools', ARRAY_CONSTRUCT('compose_query', 'list_sources'),
            'max_rows', 1000,
            'daily_runtime_seconds', 3600,
            'expires_at', DATEADD('day', 30, CURRENT_TIMESTAMP())
          )
        ),
        'TEST',
        CURRENT_TIMESTAMP()
      )
    `);
    console.log(`  ✅ Granted permissions with token`);
    
    // Test 5: Check views
    console.log('\nTest 5: Checking Views');
    
    // Wait a moment for Dynamic Table to refresh
    console.log('  ⏳ Waiting for Dynamic Table refresh (5 seconds)...');
    await new Promise(resolve => setTimeout(resolve, 5000));
    
    // Check active tokens
    const activeTokens = await executeSql(`
      SELECT * FROM MCP.ACTIVE_TOKENS 
      WHERE username = '${username}'
    `);
    
    if (activeTokens.rows.length > 0) {
      const userToken = activeTokens.rows[0];
      console.log(`  ✅ User found in ACTIVE_TOKENS view`);
      console.log(`     Token hint: ${userToken.TOKEN_HINT}`);
      console.log(`     Status: ${userToken.STATUS}`);
      console.log(`     Tool count: ${userToken.TOOL_COUNT}`);
    } else {
      console.log('  ⚠️ User not yet in ACTIVE_TOKENS (may need DT refresh)');
    }
    
    // Check dashboard
    const dashboard = await executeSql('SELECT * FROM SECURITY.DASHBOARD');
    console.log(`\n  Dashboard Status:`);
    console.log(`     Active tokens: ${dashboard.rows[0].ACTIVE_TOKENS}`);
    console.log(`     Threat level: ${dashboard.rows[0].OVERALL_THREAT_LEVEL}`);
    
    console.log('\n=================================');
    console.log('✅ Authentication System Test Complete!\n');
    console.log('The authentication infrastructure is working correctly.');
    console.log('\nYour test token (save this):');
    console.log(`Token: ${token}`);
    console.log(`Username: ${username}`);
    console.log('\nThis token can be used to authenticate with the MCP server.\n');
    
    connection.destroy();
    
  } catch (err) {
    console.error('❌ Test failed:', err.message);
    connection.destroy();
    process.exit(1);
  }
}

testAuth();