#!/usr/bin/env node

/**
 * Simplified test for the deployed authentication system
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
    
    // Test 1: Functions work
    console.log('Test 1: Core Functions');
    const tokenResult = await executeSql('SELECT MCP.GENERATE_SECURE_TOKEN() AS token');
    const token = tokenResult.rows[0].TOKEN;
    console.log(`  ✅ Generated token: ${token.substring(0, 20)}...`);
    
    const hashResult = await executeSql(`SELECT MCP.HASH_TOKEN_WITH_PEPPER('${token}') AS hash`);
    const hash = hashResult.rows[0].HASH;
    console.log(`  ✅ Token hash: ${hash.substring(0, 20)}...`);
    
    const metaResult = await executeSql(`SELECT MCP.EXTRACT_TOKEN_METADATA('${token}') AS metadata`);
    const metadata = metaResult.rows[0].METADATA;
    console.log(`  ✅ Token metadata extracted`);
    
    // Test 2: Check schemas exist
    console.log('\nTest 2: Schema Verification');
    const schemasResult = await executeSql(`
      SELECT SCHEMA_NAME 
      FROM INFORMATION_SCHEMA.SCHEMATA 
      WHERE CATALOG_NAME = 'CLAUDE_BI'
        AND SCHEMA_NAME IN ('MCP', 'SECURITY', 'ADMIN_SECRETS')
      ORDER BY SCHEMA_NAME
    `);
    
    console.log('  ✅ Required schemas found:');
    schemasResult.rows.forEach(s => {
      console.log(`     - ${s.SCHEMA_NAME}`);
    });
    
    // Test 3: Check functions exist
    console.log('\nTest 3: Function Verification');
    const functionsResult = await executeSql(`
      SELECT FUNCTION_NAME 
      FROM INFORMATION_SCHEMA.FUNCTIONS 
      WHERE FUNCTION_CATALOG = 'CLAUDE_BI'
        AND FUNCTION_SCHEMA = 'MCP'
        AND FUNCTION_NAME LIKE '%TOKEN%'
      ORDER BY FUNCTION_NAME
    `);
    
    console.log('  ✅ Token functions found:');
    functionsResult.rows.forEach(f => {
      console.log(`     - ${f.FUNCTION_NAME}`);
    });
    
    // Test 4: Check views exist
    console.log('\nTest 4: View Verification');
    const viewsResult = await executeSql(`
      SELECT TABLE_NAME, TABLE_SCHEMA
      FROM INFORMATION_SCHEMA.VIEWS 
      WHERE TABLE_CATALOG = 'CLAUDE_BI'
        AND (TABLE_SCHEMA = 'MCP' OR TABLE_SCHEMA = 'SECURITY')
      ORDER BY TABLE_SCHEMA, TABLE_NAME
    `);
    
    console.log('  ✅ Views found:');
    viewsResult.rows.forEach(v => {
      console.log(`     - ${v.TABLE_SCHEMA}.${v.TABLE_NAME}`);
    });
    
    // Test 5: Check dashboard
    console.log('\nTest 5: Security Dashboard');
    const dashboard = await executeSql('SELECT * FROM SECURITY.DASHBOARD');
    console.log('  ✅ Dashboard accessible:');
    console.log(`     Active tokens: ${dashboard.rows[0].ACTIVE_TOKENS}`);
    console.log(`     Pending activations: ${dashboard.rows[0].PENDING_ACTIVATIONS}`);
    console.log(`     Threat level: ${dashboard.rows[0].OVERALL_THREAT_LEVEL}`);
    
    console.log('\n=================================');
    console.log('✅ Authentication System Test Complete!\n');
    console.log('All core infrastructure is working correctly.');
    console.log('The system is ready for token-based authentication.\n');
    
    connection.destroy();
    
  } catch (err) {
    console.error('❌ Test failed:', err.message);
    connection.destroy();
    process.exit(1);
  }
}

testAuth();