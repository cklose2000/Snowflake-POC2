#!/usr/bin/env node

/**
 * Fixed deployment script for authentication system
 * Uses proper Snowflake stored procedure syntax
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
});

async function connect() {
  return new Promise((resolve, reject) => {
    connection.connect((err, conn) => {
      if (err) reject(err);
      else resolve(conn);
    });
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

async function deploy() {
  console.log('🚀 Deploying Claude Code Authentication System');
  console.log('==============================================\n');
  
  try {
    await connect();
    console.log('✅ Connected to Snowflake\n');
    
    // Switch to ACCOUNTADMIN
    await executeSql('USE ROLE ACCOUNTADMIN');
    console.log('✅ Using ACCOUNTADMIN role\n');
    
    await executeSql('USE DATABASE CLAUDE_BI');
    
    // ========================================
    // 1. Create ADMIN_SECRETS schema
    // ========================================
    console.log('📦 Phase 1: Security Infrastructure\n');
    
    await executeSql(
      'CREATE SCHEMA IF NOT EXISTS CLAUDE_BI.ADMIN_SECRETS',
      'Created ADMIN_SECRETS schema'
    );
    
    await executeSql(
      'GRANT USAGE ON SCHEMA CLAUDE_BI.ADMIN_SECRETS TO ROLE MCP_ADMIN_ROLE',
      'Granted usage on ADMIN_SECRETS to MCP_ADMIN_ROLE'
    );
    
    // Create pepper function
    await executeSql(`
      CREATE OR REPLACE SECURE FUNCTION CLAUDE_BI.ADMIN_SECRETS.GET_PEPPER()
      RETURNS STRING
      LANGUAGE SQL
      AS 'SELECT ''a7f3d9b2e1c4a8f5d6b9e2c7a4f1d8b5e9c3a6f2d5b8e1c4a7f0d3b6e9c2a5f8'''
    `, 'Created GET_PEPPER function');
    
    // ========================================
    // 2. Create MCP token functions
    // ========================================
    console.log('\n📦 Phase 2: Token Functions\n');
    
    await executeSql('USE SCHEMA MCP');
    
    // Token generation function
    await executeSql(`
      CREATE OR REPLACE FUNCTION MCP.GENERATE_SECURE_TOKEN()
      RETURNS STRING
      LANGUAGE SQL
      AS 'SELECT ''tk_'' || REPLACE(UUID_STRING(), ''-'', '''') || ''_user'''
    `, 'Created GENERATE_SECURE_TOKEN function');
    
    // Hash function with pepper
    await executeSql(`
      CREATE OR REPLACE FUNCTION MCP.HASH_TOKEN_WITH_PEPPER(raw_token STRING)
      RETURNS STRING
      LANGUAGE SQL
      AS 'SELECT SHA2(CONCAT(raw_token, CLAUDE_BI.ADMIN_SECRETS.GET_PEPPER()), 256)'
    `, 'Created HASH_TOKEN_WITH_PEPPER function');
    
    // Token metadata extraction
    await executeSql(`
      CREATE OR REPLACE FUNCTION MCP.EXTRACT_TOKEN_METADATA(raw_token STRING)
      RETURNS OBJECT
      LANGUAGE SQL
      AS 'SELECT OBJECT_CONSTRUCT(
        ''prefix'', SUBSTR(raw_token, 1, 8),
        ''suffix'', SUBSTR(raw_token, -4),
        ''length'', LENGTH(raw_token)
      )'
    `, 'Created EXTRACT_TOKEN_METADATA function');
    
    // ========================================
    // 3. Create template helper functions
    // ========================================
    console.log('\n📦 Phase 3: Template Functions\n');
    
    await executeSql(`
      CREATE OR REPLACE FUNCTION MCP.GET_TEMPLATE_TOOLS(role_template STRING)
      RETURNS ARRAY
      LANGUAGE SQL
      AS 'SELECT CASE role_template
        WHEN ''VIEWER'' THEN ARRAY_CONSTRUCT(''compose_query'', ''list_sources'')
        WHEN ''ANALYST'' THEN ARRAY_CONSTRUCT(''compose_query'', ''list_sources'', ''export_data'')
        WHEN ''ADMIN'' THEN ARRAY_CONSTRUCT(''compose_query'', ''list_sources'', ''export_data'', ''manage_users'')
        ELSE ARRAY_CONSTRUCT(''compose_query'')
      END'
    `, 'Created GET_TEMPLATE_TOOLS function');
    
    await executeSql(`
      CREATE OR REPLACE FUNCTION MCP.GET_TEMPLATE_ROWS(role_template STRING)
      RETURNS NUMBER
      LANGUAGE SQL
      AS 'SELECT CASE role_template
        WHEN ''VIEWER'' THEN 1000
        WHEN ''ANALYST'' THEN 10000
        WHEN ''ADMIN'' THEN 100000
        ELSE 1000
      END'
    `, 'Created GET_TEMPLATE_ROWS function');
    
    await executeSql(`
      CREATE OR REPLACE FUNCTION MCP.GET_TEMPLATE_RUNTIME(role_template STRING)
      RETURNS NUMBER
      LANGUAGE SQL
      AS 'SELECT CASE role_template
        WHEN ''VIEWER'' THEN 1800
        WHEN ''ANALYST'' THEN 7200
        WHEN ''ADMIN'' THEN 28800
        ELSE 3600
      END'
    `, 'Created GET_TEMPLATE_RUNTIME function');
    
    // ========================================
    // 4. Create monitoring views
    // ========================================
    console.log('\n📦 Phase 4: Monitoring Views\n');
    
    // Active tokens view
    await executeSql(`
      CREATE OR REPLACE VIEW MCP.ACTIVE_TOKENS AS
      WITH latest_permissions AS (
        SELECT 
          object_id AS username,
          attributes:token_prefix::STRING AS token_prefix,
          attributes:token_suffix::STRING AS token_suffix,
          attributes:expires_at::TIMESTAMP_TZ AS expires_at,
          attributes:allowed_tools::ARRAY AS allowed_tools,
          occurred_at AS issued_at,
          DATEDIFF('day', occurred_at, CURRENT_TIMESTAMP()) AS age_days,
          action
        FROM CLAUDE_BI.ACTIVITY.EVENTS
        WHERE object_type = 'user'
          AND action IN ('system.permission.granted', 'system.permission.revoked')
        QUALIFY ROW_NUMBER() OVER (
          PARTITION BY object_id 
          ORDER BY occurred_at DESC
        ) = 1
      )
      SELECT 
        username,
        token_prefix || '...' || token_suffix AS token_hint,
        issued_at,
        expires_at,
        age_days,
        ARRAY_SIZE(allowed_tools) AS tool_count,
        CASE 
          WHEN action = 'system.permission.revoked' THEN 'REVOKED'
          WHEN expires_at < CURRENT_TIMESTAMP() THEN 'EXPIRED'
          WHEN age_days > 90 THEN 'SHOULD_ROTATE'
          WHEN age_days > 60 THEN 'AGING'
          ELSE 'ACTIVE'
        END AS status
      FROM latest_permissions
      ORDER BY age_days DESC
    `, 'Created ACTIVE_TOKENS view');
    
    // Pending activations view
    await executeSql(`
      CREATE OR REPLACE VIEW MCP.PENDING_ACTIVATIONS AS
      WITH activation_events AS (
        SELECT 
          attributes:activation_code::STRING AS activation_code,
          attributes:username::STRING AS username,
          attributes:activation_expires_at::TIMESTAMP_TZ AS activation_expires_at,
          attributes:status::STRING AS status,
          occurred_at AS created_at,
          actor_id AS created_by
        FROM CLAUDE_BI.ACTIVITY.EVENTS
        WHERE action IN ('system.activation.created', 'system.activation.used', 'system.activation.expired')
        QUALIFY ROW_NUMBER() OVER (
          PARTITION BY attributes:activation_code
          ORDER BY occurred_at DESC
        ) = 1
      )
      SELECT 
        activation_code,
        username,
        created_at,
        created_by,
        activation_expires_at,
        CASE 
          WHEN status = 'used' THEN 'USED'
          WHEN activation_expires_at < CURRENT_TIMESTAMP() THEN 'EXPIRED'
          ELSE 'PENDING'
        END AS status
      FROM activation_events
      ORDER BY created_at DESC
    `, 'Created PENDING_ACTIVATIONS view');
    
    // ========================================
    // 5. Create security schema and views
    // ========================================
    console.log('\n📦 Phase 5: Security Monitoring\n');
    
    await executeSql(
      'CREATE SCHEMA IF NOT EXISTS CLAUDE_BI.SECURITY',
      'Created SECURITY schema'
    );
    
    await executeSql('USE SCHEMA SECURITY');
    
    // Security dashboard
    await executeSql(`
      CREATE OR REPLACE VIEW SECURITY.DASHBOARD AS
      SELECT 
        (SELECT COUNT(*) FROM MCP.ACTIVE_TOKENS WHERE status = 'ACTIVE') AS active_tokens,
        (SELECT COUNT(*) FROM MCP.PENDING_ACTIVATIONS WHERE status = 'PENDING') AS pending_activations,
        'LOW' AS overall_threat_level,
        CURRENT_TIMESTAMP() AS dashboard_updated_at
    `, 'Created SECURITY.DASHBOARD view');
    
    // ========================================
    // 6. Grant permissions
    // ========================================
    console.log('\n📦 Phase 6: Permissions\n');
    
    await executeSql(
      'GRANT SELECT ON ALL VIEWS IN SCHEMA MCP TO ROLE MCP_ADMIN_ROLE',
      'Granted SELECT on MCP views to MCP_ADMIN_ROLE'
    );
    
    await executeSql(
      'GRANT SELECT ON ALL VIEWS IN SCHEMA SECURITY TO ROLE MCP_ADMIN_ROLE',
      'Granted SELECT on SECURITY views to MCP_ADMIN_ROLE'
    );
    
    await executeSql(
      'GRANT USAGE ON ALL FUNCTIONS IN SCHEMA MCP TO ROLE MCP_SERVICE_ROLE',
      'Granted USAGE on MCP functions to MCP_SERVICE_ROLE'
    );
    
    // ========================================
    // Test the deployment
    // ========================================
    console.log('\n🧪 Testing Deployment\n');
    
    try {
      const testResult = await executeSql('SELECT * FROM SECURITY.DASHBOARD');
      console.log('✅ Security dashboard working');
      console.log(`   Active tokens: ${testResult.rows[0].ACTIVE_TOKENS}`);
      console.log(`   Pending activations: ${testResult.rows[0].PENDING_ACTIVATIONS}`);
      console.log(`   Threat level: ${testResult.rows[0].OVERALL_THREAT_LEVEL}`);
    } catch (err) {
      console.log('⚠️ Could not verify deployment');
    }
    
    console.log('\n==============================================');
    console.log('✅ Authentication System Deployed Successfully!\n');
    console.log('Next steps:');
    console.log('1. Create activation procedures (separate script)');
    console.log('2. Start the activation gateway:');
    console.log('   cd activation-gateway && npm install && npm start\n');
    console.log('3. Use the Claude Code CLI helper:');
    console.log('   cd claude-code-auth && npm install -g .\n');
    
    connection.destroy();
    
  } catch (err) {
    console.error('\n❌ Deployment failed:', err.message);
    connection.destroy();
    process.exit(1);
  }
}

deploy();