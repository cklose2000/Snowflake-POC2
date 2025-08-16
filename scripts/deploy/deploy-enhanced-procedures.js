#!/usr/bin/env node

/**
 * Deploy Enhanced Stored Procedures
 * 
 * Deploys the enhanced MCP procedures to Snowflake using JavaScript
 * since snowsql is not available on this system.
 */

const fs = require('fs');
const path = require('path');
const snowflake = require('snowflake-sdk');

// Load environment variables
require('dotenv').config();

async function deployEnhancedProcedures() {
  console.log('🚀 Deploying Enhanced Stored Procedures...\n');

  // Read the SQL file
  const sqlFile = path.join(__dirname, 'snowpark/activity-schema/28_enhanced_procedures.sql');
  
  if (!fs.existsSync(sqlFile)) {
    console.error(`❌ SQL file not found: ${sqlFile}`);
    process.exit(1);
  }

  const sqlContent = fs.readFileSync(sqlFile, 'utf8');
  
  // Split into individual statements - look for procedure/command boundaries
  // This handles embedded JavaScript in stored procedures correctly
  const statements = [];
  let current = '';
  let inProcedure = false;
  
  const lines = sqlContent.split('\n');
  for (const line of lines) {
    const trimmed = line.trim();
    
    // Skip comments and empty lines
    if (!trimmed || trimmed.startsWith('--')) {
      continue;
    }
    
    current += line + '\n';
    
    // Start of procedure
    if (trimmed.startsWith('CREATE OR REPLACE PROCEDURE')) {
      inProcedure = true;
    }
    
    // End of procedure or other statements
    if ((!inProcedure && trimmed.endsWith(';')) || 
        (inProcedure && trimmed === '$$;')) {
      statements.push(current.trim());
      current = '';
      inProcedure = false;
    }
  }
  
  // Add any remaining content
  if (current.trim()) {
    statements.push(current.trim());
  }

  console.log(`📝 Found ${statements.length} SQL statements to execute`);

  // Create connection using same approach as working deployment script
  const connection = snowflake.createConnection({
    account: process.env.SNOWFLAKE_ACCOUNT,
    username: process.env.SNOWFLAKE_USERNAME,
    password: process.env.SNOWFLAKE_PASSWORD,
    database: 'CLAUDE_BI',
    warehouse: process.env.SNOWFLAKE_WAREHOUSE || 'CLAUDE_WAREHOUSE'
  });

  try {
    // Connect
    await new Promise((resolve, reject) => {
      connection.connect((err) => {
        if (err) reject(err);
        else resolve();
      });
    });

    console.log('✅ Connected to Snowflake');

    // Execute each statement
    let successCount = 0;
    let errorCount = 0;

    for (let i = 0; i < statements.length; i++) {
      const statement = statements[i];
      
      if (!statement.trim()) continue;

      try {
        console.log(`\n📋 Executing statement ${i + 1}/${statements.length}...`);
        
        // Show first line of statement for context
        const firstLine = statement.split('\n')[0].substring(0, 80);
        console.log(`   ${firstLine}${statement.length > 80 ? '...' : ''}`);

        await new Promise((resolve, reject) => {
          connection.execute({
            sqlText: statement,
            complete: (err, stmt, rows) => {
              if (err) {
                reject(err);
              } else {
                resolve(rows);
              }
            }
          });
        });

        console.log(`✅ Statement ${i + 1} executed successfully`);
        successCount++;

      } catch (error) {
        console.error(`❌ Statement ${i + 1} failed:`, error.message);
        errorCount++;
        
        // Continue with other statements unless it's a critical error
        if (error.message.includes('does not exist')) {
          console.log('   ⚠️  Continuing with remaining statements...');
        }
      }
    }

    console.log('\n📊 Deployment Summary:');
    console.log(`✅ Successful: ${successCount}`);
    console.log(`❌ Failed: ${errorCount}`);
    console.log(`📄 Total: ${statements.length}`);

    if (errorCount === 0) {
      console.log('\n🎉 All enhanced procedures deployed successfully!');
    } else if (successCount > 0) {
      console.log('\n⚠️  Deployment completed with some errors');
    } else {
      console.log('\n💥 Deployment failed completely');
      process.exit(1);
    }

    // Test the main procedure
    console.log('\n🧪 Testing HANDLE_REQUEST procedure...');
    
    try {
      await new Promise((resolve, reject) => {
        connection.execute({
          sqlText: `CALL CLAUDE_BI.MCP.HANDLE_REQUEST('tools/call', '{"name":"list_sources","arguments":{}}', 'test_token')`,
          complete: (err, stmt, rows) => {
            if (err) {
              // Expected to fail with invalid token - that's OK
              if (err.message.includes('Token validation failed') || err.message.includes('Invalid token')) {
                console.log('✅ Procedure exists and validates tokens correctly');
                resolve(rows);
              } else {
                reject(err);
              }
            } else {
              console.log('✅ Procedure executed successfully');
              resolve(rows);
            }
          }
        });
      });
    } catch (error) {
      console.error('❌ Procedure test failed:', error.message);
    }

  } catch (error) {
    console.error('❌ Deployment failed:', error.message);
    process.exit(1);
  } finally {
    // Close connection
    await new Promise((resolve) => {
      connection.destroy(() => resolve());
    });
    console.log('\n🔌 Connection closed');
  }
}

// Run deployment
deployEnhancedProcedures().catch(error => {
  console.error('💥 Deployment script failed:', error);
  process.exit(1);
});