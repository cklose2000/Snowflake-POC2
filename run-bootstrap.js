#!/usr/bin/env node

// Run bootstrap SQL to create Activity views

const snowflake = require('snowflake-sdk');
const fs = require('fs');
require('dotenv').config();

async function runBootstrap() {
  console.log('📦 Running bootstrap script to create Activity views...\n');
  
  // Create Snowflake connection
  const connection = snowflake.createConnection({
    account: process.env.SNOWFLAKE_ACCOUNT,
    username: process.env.SNOWFLAKE_USERNAME,
    password: process.env.SNOWFLAKE_PASSWORD,
    role: process.env.SNOWFLAKE_ROLE,
    warehouse: process.env.SNOWFLAKE_WAREHOUSE,
    database: process.env.SNOWFLAKE_DATABASE,
    schema: process.env.SNOWFLAKE_SCHEMA
  });
  
  await new Promise((resolve, reject) => {
    connection.connect((err, conn) => {
      if (err) reject(err);
      else resolve(conn);
    });
  });
  
  console.log('✅ Connected to Snowflake\n');
  
  // Read bootstrap SQL file
  const bootstrapSQL = fs.readFileSync('scripts/bootstrap_activity_views.sql', 'utf8');
  
  // Split SQL into individual statements
  const statements = bootstrapSQL
    .split(';')
    .map(s => s.trim())
    .filter(s => s.length > 0 && !s.startsWith('--'));
  
  console.log(`📝 Found ${statements.length} SQL statements to execute\n`);
  
  // Execute each statement
  for (let i = 0; i < statements.length; i++) {
    const stmt = statements[i];
    
    // Extract object name for logging
    let objectName = 'Statement';
    if (stmt.includes('CREATE')) {
      const match = stmt.match(/CREATE\s+(?:OR\s+REPLACE\s+)?(\w+)\s+(?:IF\s+NOT\s+EXISTS\s+)?([^\s(]+)/i);
      if (match) {
        objectName = `${match[1]} ${match[2]}`;
      }
    }
    
    console.log(`[${i+1}/${statements.length}] Creating ${objectName}...`);
    
    try {
      await new Promise((resolve, reject) => {
        connection.execute({
          sqlText: stmt,
          complete: (err, stmt, rows) => {
            if (err) {
              console.error(`❌ Failed: ${err.message}`);
              reject(err);
            } else {
              console.log(`✅ Success`);
              resolve(rows);
            }
          }
        });
      });
    } catch (error) {
      console.error(`⚠️ Continuing after error: ${error.message}\n`);
    }
  }
  
  console.log('\n✅ Bootstrap complete!');
  
  // Verify views were created
  console.log('\n🔍 Verifying Activity views...');
  
  const verifySQL = `
    SELECT table_name, table_type, row_count
    FROM CLAUDE_BI.INFORMATION_SCHEMA.TABLES 
    WHERE table_schema = 'ACTIVITY_CCODE'
      AND table_type = 'VIEW'
    ORDER BY table_name
  `;
  
  await new Promise((resolve, reject) => {
    connection.execute({
      sqlText: verifySQL,
      complete: (err, stmt, rows) => {
        if (err) {
          console.error(`❌ Verification failed: ${err.message}`);
          reject(err);
        } else {
          console.log(`\n📊 Activity views created:`);
          rows.forEach(row => {
            console.log(`  - ${row.TABLE_NAME}`);
          });
          resolve(rows);
        }
      }
    });
  });
  
  connection.destroy();
}

runBootstrap().catch(console.error);