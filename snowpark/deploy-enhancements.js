#!/usr/bin/env node

/**
 * Deploy Enhanced MCP Procedures
 * Adds SafeSQL templates, cost prediction, circuit breaker, and tracking
 */

const snowflake = require('snowflake-sdk');
const fs = require('fs');
const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '../.env') });

console.log('🚀 Deploying Enhanced MCP Procedures...\n');

// Read the SQL file
const sqlContent = fs.readFileSync(path.join(__dirname, 'mcp-enhanced-procedures.sql'), 'utf8');

// Parse into individual statements
const statements = sqlContent
  .split(/^-- ============================================================================$/gm)
  .map(section => section.trim())
  .filter(section => section.length > 0)
  .flatMap(section => {
    // Extract SQL statements from each section
    return section
      .split(';')
      .map(stmt => stmt.trim())
      .filter(stmt => 
        stmt.length > 10 && 
        !stmt.startsWith('--') &&
        (stmt.includes('CREATE') || stmt.includes('GRANT') || stmt.includes('CALL'))
      )
      .map(stmt => stmt + ';');
  });

console.log(`📝 Found ${statements.length} statements to deploy\n`);

// Create connection
const connection = snowflake.createConnection({
  account: process.env.SNOWFLAKE_ACCOUNT,
  username: process.env.SNOWFLAKE_USERNAME,
  password: process.env.SNOWFLAKE_PASSWORD,
  role: process.env.SNOWFLAKE_ROLE || 'CLAUDE_BI_ROLE',
  database: 'CLAUDE_BI',
  schema: 'MCP',
  warehouse: 'CLAUDE_WAREHOUSE'
});

// Deploy procedures
connection.connect(async (err, conn) => {
  if (err) {
    console.error('❌ Failed to connect:', err.message);
    process.exit(1);
  }
  
  console.log('✅ Connected to Snowflake\n');
  
  let successCount = 0;
  let errorCount = 0;
  
  for (let i = 0; i < statements.length; i++) {
    const stmt = statements[i];
    const preview = stmt.substring(0, 60).replace(/\n/g, ' ');
    
    await new Promise((resolve) => {
      connection.execute({
        sqlText: stmt,
        complete: (err, statement, rows) => {
          if (err) {
            console.error(`❌ [${i+1}/${statements.length}] ${preview}...`);
            console.error(`   Error: ${err.message.substring(0, 100)}`);
            errorCount++;
          } else {
            console.log(`✅ [${i+1}/${statements.length}] ${preview}...`);
            
            // Show results for test calls
            if (stmt.includes('CALL ')) {
              if (rows && rows[0]) {
                const result = Object.values(rows[0])[0];
                if (typeof result === 'string') {
                  try {
                    const parsed = JSON.parse(result);
                    console.log(`   Result: ${JSON.stringify(parsed).substring(0, 100)}...`);
                  } catch {
                    console.log(`   Result: ${result.substring(0, 100)}...`);
                  }
                } else {
                  console.log(`   Result: ${JSON.stringify(result).substring(0, 100)}...`);
                }
              }
            }
            successCount++;
          }
          resolve();
        }
      });
    });
  }
  
  console.log('\n📊 Deployment Summary:');
  console.log(`  ✅ Successful: ${successCount}`);
  console.log(`  ❌ Errors: ${errorCount}`);
  
  if (errorCount === 0) {
    console.log('\n🎉 All enhanced procedures deployed successfully!');
    console.log('\n📝 New Features Available:');
    console.log('  • SafeSQL Templates - RENDER_SAFE_SQL()');
    console.log('  • Cost Prediction - ESTIMATE_QUERY_COST()');
    console.log('  • Circuit Breaker - CHECK_CIRCUIT_BREAKER()');
    console.log('  • Enhanced Execution - EXECUTE_QUERY_PLAN_V2()');
    console.log('  • Full Activity Tracking - All queries logged');
    console.log('  • Query Tagging - Attribution in query history');
  } else {
    console.log('\n⚠️  Some procedures failed to deploy. Check errors above.');
  }
  
  connection.destroy();
  process.exit(errorCount > 0 ? 1 : 0);
});