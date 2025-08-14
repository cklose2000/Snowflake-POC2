#!/usr/bin/env node

/**
 * Setup MCP in Snowflake
 * Executes the SQL setup script using Node.js Snowflake client
 */

const snowflake = require('snowflake-sdk');
const fs = require('fs');
const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '../.env') });

// Read SQL setup script
const setupSQL = fs.readFileSync(path.join(__dirname, '../infra/snowflake/mcp-setup.sql'), 'utf8');

// Split into individual statements (removing comments and empty lines)
const statements = setupSQL
  .split(';')
  .map(stmt => stmt.trim())
  .filter(stmt => stmt && !stmt.startsWith('--') && stmt.length > 10)
  .map(stmt => stmt + ';');

console.log('🚀 Setting up MCP in Snowflake...');
console.log(`📝 Found ${statements.length} SQL statements to execute`);

// Create connection
const connection = snowflake.createConnection({
  account: process.env.SNOWFLAKE_ACCOUNT,
  username: process.env.SNOWFLAKE_USERNAME,
  password: process.env.SNOWFLAKE_PASSWORD,
  role: process.env.SNOWFLAKE_ROLE || 'CLAUDE_BI_ROLE',
  database: 'CLAUDE_BI',
  schema: 'PUBLIC',
  warehouse: 'CLAUDE_WAREHOUSE'
});

// Connect and execute
connection.connect(async (err, conn) => {
  if (err) {
    console.error('❌ Failed to connect:', err.message);
    process.exit(1);
  }

  console.log('✅ Connected to Snowflake');

  let successCount = 0;
  let errorCount = 0;

  for (let i = 0; i < statements.length; i++) {
    const stmt = statements[i];
    
    // Skip certain statements that might fail if objects already exist
    const isCreateStatement = stmt.toUpperCase().includes('CREATE ') && 
                             !stmt.toUpperCase().includes('CREATE OR REPLACE');
    
    try {
      await new Promise((resolve, reject) => {
        connection.execute({
          sqlText: stmt,
          complete: (err, statement, rows) => {
            if (err) {
              // Ignore "already exists" errors for CREATE statements
              if (isCreateStatement && err.message.includes('already exists')) {
                console.log(`⏭️  [${i+1}/${statements.length}] Object already exists (skipped)`);
                successCount++;
                resolve();
              } else {
                console.error(`❌ [${i+1}/${statements.length}] Error:`, err.message.substring(0, 100));
                errorCount++;
                resolve(); // Continue with next statement
              }
            } else {
              const preview = stmt.substring(0, 50).replace(/\n/g, ' ');
              console.log(`✅ [${i+1}/${statements.length}] ${preview}...`);
              successCount++;
              resolve();
            }
          }
        });
      });
    } catch (error) {
      console.error(`❌ [${i+1}/${statements.length}] Unexpected error:`, error.message);
      errorCount++;
    }
  }

  console.log('\n📊 Setup Summary:');
  console.log(`  ✅ Successful: ${successCount}`);
  console.log(`  ❌ Errors: ${errorCount}`);

  // Test the setup
  console.log('\n🧪 Testing MCP setup...');
  
  connection.execute({
    sqlText: `CALL CLAUDE_BI.MCP.VALIDATE_QUERY_PLAN(PARSE_JSON('{"source": "ACTIVITY_CCODE.VW_ACTIVITY_SUMMARY", "top_n": 5}'))`,
    complete: (err, statement, rows) => {
      if (err) {
        console.error('❌ Test failed:', err.message);
      } else {
        console.log('✅ MCP validation procedure working!');
        console.log('   Result:', JSON.stringify(rows[0]));
      }
      
      connection.destroy();
      console.log('\n✨ MCP setup complete!');
      process.exit(0);
    }
  });
});