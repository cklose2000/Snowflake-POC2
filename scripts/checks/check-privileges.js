#!/usr/bin/env node

/**
 * Check current user privileges and available roles
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

async function checkPrivileges() {
  console.log('🔍 Checking User Privileges and Roles');
  console.log('=====================================\n');
  
  try {
    await connect();
    console.log('✅ Connected to Snowflake\n');
    
    // Check current user
    const userResult = await executeSql('SELECT CURRENT_USER() as user, CURRENT_ROLE() as role');
    console.log(`Current User: ${userResult.rows[0].USER}`);
    console.log(`Current Role: ${userResult.rows[0].ROLE}\n`);
    
    // Check available roles
    console.log('Available Roles:');
    const rolesResult = await executeSql('SHOW GRANTS TO USER ' + userResult.rows[0].USER);
    const roles = rolesResult.rows.filter(r => r.granted_on === 'ROLE');
    roles.forEach(r => {
      console.log(`  - ${r.name} (${r.privilege})`);
    });
    console.log('');
    
    // Try to use ACCOUNTADMIN
    console.log('Testing ACCOUNTADMIN access:');
    try {
      await executeSql('USE ROLE ACCOUNTADMIN');
      console.log('  ✅ Can use ACCOUNTADMIN role');
      
      // Check what we can do
      const adminCheck = await executeSql('SELECT CURRENT_ROLE() as role');
      console.log(`  Current role after switch: ${adminCheck.rows[0].ROLE}\n`);
      
      // Check if we can create schemas
      console.log('Testing schema creation privileges:');
      try {
        await executeSql('CREATE SCHEMA IF NOT EXISTS TEST_SCHEMA_DELETE_ME');
        console.log('  ✅ Can create schemas');
        await executeSql('DROP SCHEMA IF EXISTS TEST_SCHEMA_DELETE_ME');
        console.log('  ✅ Can drop schemas\n');
      } catch (err) {
        console.log(`  ❌ Cannot create schemas: ${err.message}\n`);
      }
      
      // Check if MCP_ADMIN_ROLE exists
      console.log('Checking for MCP roles:');
      try {
        await executeSql('SHOW ROLES LIKE \'MCP%\'');
        const mcpRoles = await executeSql('SHOW ROLES LIKE \'MCP%\'');
        if (mcpRoles.rows.length > 0) {
          console.log('  Found MCP roles:');
          mcpRoles.rows.forEach(r => {
            console.log(`    - ${r.name}`);
          });
        } else {
          console.log('  ❌ No MCP roles found - need to create them');
        }
      } catch (err) {
        console.log('  ❌ Error checking MCP roles');
      }
      
    } catch (err) {
      console.log(`  ❌ Cannot use ACCOUNTADMIN: ${err.message}`);
      console.log('  This is required for full authentication deployment\n');
    }
    
    // Check database objects
    console.log('\nChecking existing database objects:');
    
    try {
      const schemas = await executeSql(`
        SELECT SCHEMA_NAME 
        FROM INFORMATION_SCHEMA.SCHEMATA 
        WHERE CATALOG_NAME = 'CLAUDE_BI'
        ORDER BY SCHEMA_NAME
      `);
      console.log('  Schemas in CLAUDE_BI:');
      schemas.rows.forEach(s => {
        console.log(`    - ${s.SCHEMA_NAME}`);
      });
    } catch (err) {
      console.log('  Error listing schemas');
    }
    
    console.log('\n=====================================');
    console.log('✅ Privilege check complete\n');
    
    connection.destroy();
    
  } catch (err) {
    console.error('❌ Error:', err.message);
    connection.destroy();
    process.exit(1);
  }
}

checkPrivileges();