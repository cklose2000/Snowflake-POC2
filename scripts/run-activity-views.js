#!/usr/bin/env node

// Bootstrap Activity Views for Dashboard Factory
// Creates typed views in ACTIVITY_CCODE schema for Activity-native dashboards

const snowflake = require('snowflake-sdk');
const fs = require('fs');
const path = require('path');
require('dotenv').config();

async function bootstrapActivityViews() {
  console.log('🚀 Bootstrapping Activity Views for Dashboard Factory');
  console.log('='.repeat(60));
  
  // Load the SQL script
  const sqlPath = path.join(__dirname, 'bootstrap_activity_views.sql');
  const sqlContent = fs.readFileSync(sqlPath, 'utf8');
  
  // Parse SQL statements (split on semicolons, but handle procedures correctly)
  const statements = [];
  let currentStatement = '';
  let inProcedure = false;
  
  sqlContent.split('\n').forEach(line => {
    // Check for procedure start/end
    if (line.includes('CREATE OR REPLACE PROCEDURE') || line.includes('CREATE OR REPLACE FUNCTION')) {
      inProcedure = true;
    }
    if (line.trim() === '$$;') {
      inProcedure = false;
      currentStatement += line + '\n';
      statements.push(currentStatement.trim());
      currentStatement = '';
      return;
    }
    
    // Handle normal statements
    if (!inProcedure && line.includes(';') && !line.trim().startsWith('--')) {
      currentStatement += line;
      const stmt = currentStatement.trim();
      if (stmt && !stmt.startsWith('--')) {
        statements.push(stmt);
      }
      currentStatement = '';
    } else {
      currentStatement += line + '\n';
    }
  });
  
  console.log(`\n📝 Found ${statements.length} SQL statements to execute\n`);
  
  // Create connection
  const connection = snowflake.createConnection({
    account: process.env.SNOWFLAKE_ACCOUNT,
    username: process.env.SNOWFLAKE_USERNAME,
    password: process.env.SNOWFLAKE_PASSWORD,
    role: process.env.SNOWFLAKE_ROLE,
    warehouse: process.env.SNOWFLAKE_WAREHOUSE,
    database: process.env.SNOWFLAKE_DATABASE,
    schema: process.env.SNOWFLAKE_SCHEMA
  });
  
  // Connect
  await new Promise((resolve, reject) => {
    connection.connect((err, conn) => {
      if (err) {
        console.error('❌ Failed to connect:', err.message);
        reject(err);
      } else {
        console.log('✅ Connected to Snowflake\n');
        resolve(conn);
      }
    });
  });
  
  // Execute statements
  let successCount = 0;
  let errorCount = 0;
  
  for (let i = 0; i < statements.length; i++) {
    const stmt = statements[i];
    
    // Skip comments and empty statements
    if (!stmt || stmt.startsWith('--') || stmt === 'NULL;') {
      continue;
    }
    
    // Extract statement type for logging
    let stmtType = 'SQL';
    if (stmt.includes('CREATE OR REPLACE VIEW')) {
      const viewMatch = stmt.match(/VIEW\s+(\S+)/i);
      stmtType = `CREATE VIEW ${viewMatch ? viewMatch[1] : ''}`;
    } else if (stmt.includes('CREATE OR REPLACE PROCEDURE')) {
      const procMatch = stmt.match(/PROCEDURE\s+(\S+)/i);
      stmtType = `CREATE PROCEDURE ${procMatch ? procMatch[1] : ''}`;
    } else if (stmt.includes('CREATE OR REPLACE FUNCTION')) {
      const funcMatch = stmt.match(/FUNCTION\s+(\S+)/i);
      stmtType = `CREATE FUNCTION ${funcMatch ? funcMatch[1] : ''}`;
    } else if (stmt.includes('USE DATABASE')) {
      stmtType = 'SET CONTEXT';
    } else if (stmt.includes('USE SCHEMA')) {
      stmtType = 'SET CONTEXT';
    } else if (stmt.includes('SHOW VIEWS')) {
      stmtType = 'SHOW VIEWS';
    } else if (stmt.includes('SELECT') && stmt.includes('UNION ALL')) {
      stmtType = 'VALIDATION QUERY';
    } else if (stmt.includes('COMMENT ON')) {
      stmtType = 'ADD COMMENT';
    }
    
    console.log(`[${i + 1}/${statements.length}] Executing: ${stmtType}...`);
    
    try {
      await new Promise((resolve, reject) => {
        connection.execute({
          sqlText: stmt,
          complete: (err, stmt, rows) => {
            if (err) {
              console.error(`   ❌ Error: ${err.message}`);
              errorCount++;
              // Continue on error for idempotent operations
              resolve();
            } else {
              console.log(`   ✅ Success`);
              successCount++;
              
              // Show results for SELECT statements
              if (rows && rows.length > 0 && stmtType.includes('SELECT')) {
                console.log(`   📊 Results:`);
                rows.forEach(row => {
                  if (row.STATUS) {
                    console.log(`      ${row.STATUS}`);
                  } else if (row.VIEW_NAME) {
                    console.log(`      - ${row.VIEW_NAME}: ${row.ROW_COUNT || 0} rows`);
                  }
                });
              }
              
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
  console.log('\n📊 Bootstrap Results:\n');
  console.log(`   ✅ Successful operations: ${successCount}`);
  console.log(`   ❌ Failed operations: ${errorCount}`);
  
  // List created views
  console.log('\n📋 Verifying Activity Views...\n');
  
  const verifySQL = `
    SELECT VIEW_NAME, COMMENT 
    FROM INFORMATION_SCHEMA.VIEWS 
    WHERE TABLE_SCHEMA = 'ACTIVITY_CCODE'
    ORDER BY VIEW_NAME
  `;
  
  await new Promise((resolve, reject) => {
    connection.execute({
      sqlText: verifySQL,
      complete: (err, stmt, rows) => {
        if (!err && rows) {
          console.log('   Activity Views:');
          rows.forEach(row => {
            console.log(`   ✅ ${row.VIEW_NAME}`);
            if (row.COMMENT) {
              console.log(`      ${row.COMMENT}`);
            }
          });
        }
        resolve();
      }
    });
  });
  
  // Check for procedures and functions
  const verifyProcsSQL = `
    SELECT PROCEDURE_NAME 
    FROM INFORMATION_SCHEMA.PROCEDURES 
    WHERE PROCEDURE_SCHEMA = 'ACTIVITY_CCODE'
  `;
  
  await new Promise((resolve, reject) => {
    connection.execute({
      sqlText: verifyProcsSQL,
      complete: (err, stmt, rows) => {
        if (!err && rows && rows.length > 0) {
          console.log('\n   Procedures:');
          rows.forEach(row => {
            console.log(`   ✅ ${row.PROCEDURE_NAME}`);
          });
        }
        resolve();
      }
    });
  });
  
  const verifyFuncsSQL = `
    SELECT FUNCTION_NAME 
    FROM INFORMATION_SCHEMA.FUNCTIONS 
    WHERE FUNCTION_SCHEMA = 'ACTIVITY_CCODE'
  `;
  
  await new Promise((resolve, reject) => {
    connection.execute({
      sqlText: verifyFuncsSQL,
      complete: (err, stmt, rows) => {
        if (!err && rows && rows.length > 0) {
          console.log('\n   Functions:');
          rows.forEach(row => {
            console.log(`   ✅ ${row.FUNCTION_NAME}`);
          });
        }
        resolve();
      }
    });
  });
  
  // Disconnect
  connection.destroy();
  
  console.log('\n' + '='.repeat(60));
  if (errorCount === 0) {
    console.log('\n✅ Activity Views bootstrap completed successfully!');
    console.log('Your Dashboard Factory can now create Activity-native dashboards.\n');
  } else {
    console.log('\n⚠️ Bootstrap completed with some errors.');
    console.log('Review the errors above and re-run if needed.\n');
  }
  
  process.exit(errorCount > 0 ? 1 : 0);
}

// Run if called directly
if (require.main === module) {
  bootstrapActivityViews().catch(error => {
    console.error('\n💥 Fatal error:', error.message);
    process.exit(1);
  });
}

module.exports = { bootstrapActivityViews };