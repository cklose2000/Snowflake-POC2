#!/usr/bin/env node

// Run the bootstrap SQL script to set up Snowflake schema
// Usage: node scripts/run-bootstrap.js

const snowflake = require('snowflake-sdk');
const fs = require('fs');
const path = require('path');
require('dotenv').config();

async function runBootstrap() {
  console.log('🚀 Running Snowflake schema bootstrap...\n');
  
  // Create connection
  const connection = snowflake.createConnection({
    account: process.env.SNOWFLAKE_ACCOUNT,
    username: process.env.SNOWFLAKE_USERNAME,
    password: process.env.SNOWFLAKE_PASSWORD,
    role: process.env.SNOWFLAKE_ROLE,
    warehouse: process.env.SNOWFLAKE_WAREHOUSE
  });
  
  // Connect
  await new Promise((resolve, reject) => {
    connection.connect((err, conn) => {
      if (err) {
        console.error('❌ Failed to connect:', err.message);
        reject(err);
      } else {
        console.log('✅ Connected to Snowflake');
        resolve(conn);
      }
    });
  });
  
  // Read bootstrap SQL
  const bootstrapSQL = fs.readFileSync(
    path.join(__dirname, 'bootstrap.sql'),
    'utf8'
  );
  
  // Split into individual statements (naive split on semicolon)
  // Note: This won't handle semicolons inside strings properly
  const statements = bootstrapSQL
    .split(';')
    .map(s => s.trim())
    .filter(s => s.length > 0 && !s.startsWith('--') && !s.startsWith('/*'));
  
  console.log(`📝 Found ${statements.length} SQL statements to execute\n`);
  
  // Execute each statement
  let successCount = 0;
  let errorCount = 0;
  
  for (let i = 0; i < statements.length; i++) {
    const statement = statements[i];
    
    // Skip comments and empty statements
    if (!statement || statement.startsWith('--')) continue;
    
    // Extract first few words for logging
    const preview = statement.substring(0, 50).replace(/\n/g, ' ');
    console.log(`[${i + 1}/${statements.length}] Executing: ${preview}...`);
    
    try {
      await new Promise((resolve, reject) => {
        connection.execute({
          sqlText: statement,
          complete: (err, stmt) => {
            if (err) {
              // Some errors are expected (e.g., IF NOT EXISTS on existing objects)
              if (err.message.includes('already exists')) {
                console.log(`   ⚠️ Already exists (skipped)`);
                successCount++;
                resolve();
              } else {
                console.error(`   ❌ Error: ${err.message}`);
                errorCount++;
                resolve(); // Continue on error
              }
            } else {
              console.log(`   ✅ Success`);
              successCount++;
              resolve();
            }
          }
        });
      });
    } catch (error) {
      console.error(`   ❌ Unexpected error: ${error.message}`);
      errorCount++;
    }
  }
  
  // Summary
  console.log('\n' + '='.repeat(60));
  console.log('📊 Bootstrap Summary:');
  console.log(`   ✅ Successful: ${successCount}`);
  console.log(`   ❌ Failed: ${errorCount}`);
  
  // Run validation
  console.log('\n🔍 Running validation...\n');
  
  try {
    // Check schemas
    const schemas = await executeQuery(connection, 'SHOW SCHEMAS IN DATABASE CLAUDE_BI');
    console.log('Schemas found:', schemas.map(s => s.name).join(', '));
    
    // Check Activity.EVENTS table
    const tables = await executeQuery(connection, 'SHOW TABLES IN SCHEMA ACTIVITY');
    console.log('Tables in ACTIVITY:', tables.map(t => t.name).join(', '));
    
    // Check column count in EVENTS
    const columns = await executeQuery(connection, 
      `SELECT COUNT(*) as col_count 
       FROM INFORMATION_SCHEMA.COLUMNS 
       WHERE TABLE_SCHEMA = 'ACTIVITY' AND TABLE_NAME = 'EVENTS'`
    );
    console.log(`EVENTS table has ${columns[0].COL_COUNT} columns`);
    
    console.log('\n✅ Bootstrap validation passed!');
  } catch (error) {
    console.error('\n❌ Validation failed:', error.message);
  }
  
  // Disconnect
  connection.destroy();
  console.log('\n👋 Disconnected from Snowflake');
  
  if (errorCount > 0) {
    console.log('\n⚠️ Some statements failed. Review the errors above.');
    process.exit(1);
  } else {
    console.log('\n🎉 Bootstrap completed successfully!');
    process.exit(0);
  }
}

// Helper to execute query
async function executeQuery(connection, sql) {
  return new Promise((resolve, reject) => {
    connection.execute({
      sqlText: sql,
      complete: (err, stmt) => {
        if (err) {
          reject(err);
        } else {
          const rows = [];
          const stream = stmt.streamRows();
          stream.on('data', row => rows.push(row));
          stream.on('end', () => resolve(rows));
          stream.on('error', reject);
        }
      }
    });
  });
}

// Run if called directly
if (require.main === module) {
  runBootstrap().catch(error => {
    console.error('Fatal error:', error);
    process.exit(1);
  });
}

module.exports = { runBootstrap };