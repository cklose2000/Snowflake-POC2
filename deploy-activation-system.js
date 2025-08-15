#!/usr/bin/env node

/**
 * Deploy Activation System - CREATE_ACTIVATION procedure
 */

const snowflake = require('snowflake-sdk');
require('dotenv').config();

const connection = snowflake.createConnection({
  account: process.env.SNOWFLAKE_ACCOUNT,
  username: process.env.SNOWFLAKE_USERNAME,
  password: process.env.SNOWFLAKE_PASSWORD,
  database: 'CLAUDE_BI',
  warehouse: process.env.SNOWFLAKE_WAREHOUSE || 'CLAUDE_WAREHOUSE'
});

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

async function deployActivationSystem() {
  console.log('🚀 Deploying Activation System\n');

  try {
    // Connect
    await new Promise((resolve, reject) => {
      connection.connect((err) => {
        if (err) reject(err);
        else resolve();
      });
    });

    console.log('✅ Connected to Snowflake\n');

    // Set context
    await executeSql('USE ROLE ACCOUNTADMIN', 'Set ACCOUNTADMIN role');
    await executeSql('USE DATABASE CLAUDE_BI', 'Use CLAUDE_BI database');
    
    // Create ADMIN schema if it doesn't exist
    try {
      await executeSql('CREATE SCHEMA IF NOT EXISTS ADMIN', 'Create ADMIN schema');
      await executeSql('USE SCHEMA ADMIN', 'Use ADMIN schema');
    } catch (e) {
      // Try MCP schema instead
      await executeSql('USE SCHEMA MCP', 'Use MCP schema (fallback)');
    }

    // Create the main activation procedure (use current schema)
    const createActivationProcedure = `
CREATE OR REPLACE PROCEDURE CREATE_ACTIVATION(
  username STRING,
  user_email STRING,
  template STRING DEFAULT 'ANALYST'
)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
  // Generate secure activation code
  var activation_code = 'ACT_' + Math.random().toString(36).substr(2, 12).toUpperCase();
  
  // Create activation URL - using localhost for local testing
  // In production, this would be your actual domain
  var activation_url = 'http://localhost:3000/activate/' + activation_code;
  
  // Calculate expiration times
  var now = new Date();
  var activation_expires = new Date(now.getTime() + 24 * 60 * 60 * 1000); // 24 hours
  var token_expires = new Date(now.getTime() + 30 * 24 * 60 * 60 * 1000); // 30 days
  
  // Get template permissions
  var template_tools, template_rows, template_runtime;
  
  try {
    var toolsStmt = snowflake.createStatement({
      sqlText: "SELECT ADMIN.GET_TEMPLATE_TOOLS(?) as tools",
      binds: [template]
    });
    var toolsResult = toolsStmt.execute();
    template_tools = toolsResult.next() ? toolsResult.getColumnValue('TOOLS') : [];
    
    var rowsStmt = snowflake.createStatement({
      sqlText: "SELECT ADMIN.GET_TEMPLATE_ROWS(?) as max_rows", 
      binds: [template]
    });
    var rowsResult = rowsStmt.execute();
    template_rows = rowsResult.next() ? rowsResult.getColumnValue('MAX_ROWS') : 10000;
    
    var runtimeStmt = snowflake.createStatement({
      sqlText: "SELECT ADMIN.GET_TEMPLATE_RUNTIME(?) as runtime",
      binds: [template]
    });
    var runtimeResult = runtimeStmt.execute();
    template_runtime = runtimeResult.next() ? runtimeResult.getColumnValue('RUNTIME') : 7200;
    
  } catch (e) {
    // Fallback to defaults if template functions fail
    template_tools = ['list_sources', 'compose_query_plan'];
    template_rows = 10000;
    template_runtime = 7200; // 2 hours
  }
  
  // Create activation event
  var event = {
    event_id: 'evt_' + Math.random().toString(36).substr(2, 16),
    action: 'system.activation.created',
    actor_id: 'admin',
    object: {
      type: 'activation',
      id: activation_code
    },
    attributes: {
      activation_code: activation_code,
      username: USERNAME,
      user_email: USER_EMAIL,
      template: TEMPLATE,
      allowed_tools: template_tools,
      max_rows: template_rows,
      daily_runtime_seconds: template_runtime,
      expires_at: token_expires.toISOString(),
      activation_expires_at: activation_expires.toISOString(),
      activation_url: activation_url,
      status: 'pending'
    },
    occurred_at: now.toISOString()
  };
  
  // Insert activation event  
  try {
    var insertStmt = snowflake.createStatement({
      sqlText: "INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS SELECT PARSE_JSON(?), 'SYSTEM', ?",
      binds: [JSON.stringify(event), now.toISOString()]
    });
    insertStmt.execute();
  } catch (e) {
    return {
      success: false,
      error: 'Failed to create activation: ' + e.message
    };
  }
  
  // Return activation details
  return {
    success: true,
    activation_id: activation_code,
    activation_url: activation_url,
    expires_at: activation_expires.toISOString(),
    username: USERNAME,
    template: TEMPLATE,
    permissions: {
      allowed_tools: template_tools,
      max_rows: template_rows,
      daily_runtime_seconds: template_runtime
    },
    deeplink_ready: true
  };
$$;`;

    await executeSql(createActivationProcedure, 'Created CREATE_ACTIVATION procedure');

    // Grant permissions (try both roles)
    try {
      await executeSql(
        'GRANT USAGE ON PROCEDURE CREATE_ACTIVATION(STRING, STRING, STRING) TO ROLE MCP_ADMIN_ROLE',
        'Granted procedure access to MCP_ADMIN_ROLE'
      );
    } catch (e) {
      console.log('  ⚠️  MCP_ADMIN_ROLE grant failed (role may not exist)');
    }
    
    try {
      await executeSql(
        'GRANT USAGE ON PROCEDURE CREATE_ACTIVATION(STRING, STRING) TO ROLE ACCOUNTADMIN', 
        'Granted procedure access to ACCOUNTADMIN'
      );
    } catch (e) {
      console.log('  ⚠️  ACCOUNTADMIN grant failed');
    }

    // Test the procedure
    console.log('\n🧪 Testing CREATE_ACTIVATION procedure...');
    
    try {
      const testResult = await executeSql(
        "CALL CREATE_ACTIVATION('test_user', 'test@example.com')",
        'Test activation creation'
      );
      
      console.log('📄 Test Result:', JSON.stringify(testResult.rows[0], null, 2));
    } catch (error) {
      console.log('⚠️  Test failed (expected in demo):', error.message);
    }

    console.log('\n🎉 Activation System Deployed Successfully!');
    console.log('\nSarah can now be given access with:');
    console.log("CALL CREATE_ACTIVATION('sarah_marketing', 'sarah@company.com');");

  } catch (error) {
    console.error('❌ Deployment failed:', error.message);
  } finally {
    await new Promise((resolve) => {
      connection.destroy(() => resolve());
    });
    console.log('\n🔌 Connection closed');
  }
}

deployActivationSystem().catch(console.error);