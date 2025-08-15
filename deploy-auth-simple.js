#!/usr/bin/env node

/**
 * Simple deployment script that handles permissions properly
 */

const snowflake = require('snowflake-sdk');
const fs = require('fs').promises;
require('dotenv').config();

// Create connection
const connection = snowflake.createConnection({
  account: process.env.SNOWFLAKE_ACCOUNT,
  username: process.env.SNOWFLAKE_USERNAME,
  password: process.env.SNOWFLAKE_PASSWORD,
  database: 'CLAUDE_BI',
  warehouse: process.env.SNOWFLAKE_WAREHOUSE || 'CLAUDE_WAREHOUSE'
  // Don't specify role - let it use default
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

async function deploy() {
  console.log('🚀 Deploying Authentication System');
  console.log('=====================================\n');
  
  try {
    await connect();
    console.log('✅ Connected to Snowflake\n');
    
    // Check current role
    const roleResult = await executeSql('SELECT CURRENT_ROLE() as role');
    const currentRole = roleResult.rows[0].ROLE;
    console.log(`Current role: ${currentRole}\n`);
    
    // Try to use ACCOUNTADMIN
    try {
      await executeSql('USE ROLE ACCOUNTADMIN');
      console.log('✅ Switched to ACCOUNTADMIN role\n');
    } catch (err) {
      console.log(`⚠️ Cannot use ACCOUNTADMIN role. Continuing with ${currentRole}\n`);
      console.log('Note: Some operations may fail without ACCOUNTADMIN privileges.\n');
    }
    
    // Ensure we're in the right database
    await executeSql('USE DATABASE CLAUDE_BI');
    
    // Deploy core components that don't require ACCOUNTADMIN
    console.log('📦 Deploying core authentication components...\n');
    
    // 1. Check if MCP schema exists
    try {
      await executeSql('USE SCHEMA MCP');
      console.log('✅ MCP schema exists\n');
    } catch (err) {
      console.log('Creating MCP schema...');
      await executeSql('CREATE SCHEMA IF NOT EXISTS MCP');
      await executeSql('USE SCHEMA MCP');
    }
    
    // 2. Create basic token functions (simplified)
    console.log('Creating token generation function...');
    const tokenFunction = `
      CREATE OR REPLACE FUNCTION MCP.GENERATE_SECURE_TOKEN()
      RETURNS STRING
      LANGUAGE SQL
      AS
      $$
        SELECT 'tk_' || REPLACE(UUID_STRING(), '-', '') || '_user'
      $$
    `;
    await executeSql(tokenFunction);
    console.log('✅ Token function created\n');
    
    // 3. Create simple token hash function
    console.log('Creating token hash function...');
    const hashFunction = `
      CREATE OR REPLACE FUNCTION MCP.HASH_TOKEN_WITH_PEPPER(raw_token STRING)
      RETURNS STRING
      LANGUAGE SQL
      AS
      $$
        SELECT SHA2(raw_token || 'simple_pepper_for_demo', 256)
      $$
    `;
    await executeSql(hashFunction);
    console.log('✅ Hash function created\n');
    
    // 4. Create simplified user creation procedure
    console.log('Creating user creation procedure...');
    const createUserProc = `
      CREATE OR REPLACE PROCEDURE MCP.CREATE_TEST_USER(
        username STRING,
        email STRING
      )
      RETURNS VARIANT
      LANGUAGE SQL
      AS
      $$
      DECLARE
        token STRING;
        token_hash STRING;
        event_id_val STRING;
      BEGIN
        -- Generate token
        SELECT MCP.GENERATE_SECURE_TOKEN() INTO token;
        SELECT MCP.HASH_TOKEN_WITH_PEPPER(token) INTO token_hash;
        SELECT SHA2(CONCAT_WS('|', 'user.create', username, CURRENT_TIMESTAMP()::STRING), 256) INTO event_id_val;
        
        -- Create user event
        INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS VALUES (
          OBJECT_CONSTRUCT(
            'event_id', event_id_val,
            'action', 'system.user.created',
            'occurred_at', CURRENT_TIMESTAMP(),
            'actor_id', CURRENT_USER(),
            'source', 'system',
            'object', OBJECT_CONSTRUCT(
              'type', 'user',
              'id', username
            ),
            'attributes', OBJECT_CONSTRUCT(
              'email', email,
              'token_hash', token_hash,
              'token_prefix', SUBSTR(token, 1, 8)
            )
          ),
          'ADMIN',
          CURRENT_TIMESTAMP()
        );
        
        RETURN OBJECT_CONSTRUCT(
          'success', TRUE,
          'username', username,
          'token', token,
          'message', 'Test user created. Save this token - it will not be shown again.'
        );
      END;
      $$
    `;
    await executeSql(createUserProc);
    console.log('✅ User creation procedure created\n');
    
    // 5. Create basic views
    console.log('Creating monitoring views...');
    
    const activeUsersView = `
      CREATE OR REPLACE VIEW MCP.ACTIVE_USERS AS
      SELECT 
        object_id AS username,
        attributes:email::STRING AS email,
        attributes:token_prefix::STRING AS token_prefix,
        occurred_at AS created_at
      FROM CLAUDE_BI.ACTIVITY.EVENTS
      WHERE action = 'system.user.created'
        AND object_type = 'user'
      ORDER BY occurred_at DESC
    `;
    await executeSql(activeUsersView);
    console.log('✅ Active users view created\n');
    
    // Test the deployment
    console.log('🧪 Testing deployment...\n');
    
    // Create a test user
    const testResult = await executeSql(`
      CALL MCP.CREATE_TEST_USER('demo_user', 'demo@example.com')
    `);
    
    const result = JSON.parse(testResult.rows[0].CREATE_TEST_USER);
    
    if (result.success) {
      console.log('✅ Test user created successfully!');
      console.log(`   Username: ${result.username}`);
      console.log(`   Token: ${result.token}`);
      console.log('   ⚠️ Save this token - it will not be shown again!\n');
    }
    
    // Check active users
    const usersResult = await executeSql('SELECT * FROM MCP.ACTIVE_USERS');
    console.log(`Active users in system: ${usersResult.rows.length}\n`);
    
    console.log('=====================================');
    console.log('✅ Basic Authentication System Deployed!\n');
    console.log('Note: This is a simplified deployment.');
    console.log('For full features, you need ACCOUNTADMIN privileges.\n');
    console.log('Next steps:');
    console.log('1. Save the demo token shown above');
    console.log('2. Start the activation gateway:');
    console.log('   cd activation-gateway && npm install && npm start\n');
    
    connection.destroy();
    
  } catch (err) {
    console.error('❌ Deployment failed:', err.message);
    connection.destroy();
    process.exit(1);
  }
}

deploy();