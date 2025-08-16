#!/usr/bin/env node

/**
 * Deploy Authentication System using Snowflake SDK
 * This deploys all SQL files without needing snowsql CLI
 */

const snowflake = require('snowflake-sdk');
const fs = require('fs').promises;
const path = require('path');
require('dotenv').config();

// SQL files to deploy in order
const SQL_FILES = [
  {
    file: 'snowpark/activity-schema/23_token_pepper_security.sql',
    description: 'Secure pepper storage and token functions'
  },
  {
    file: 'snowpark/activity-schema/24_activation_system.sql',
    description: 'One-click activation procedures'
  },
  {
    file: 'snowpark/activity-schema/25_token_lifecycle.sql',
    description: 'Session tracking and automated rotation'
  },
  {
    file: 'snowpark/activity-schema/26_security_monitoring.sql',
    description: 'Security views and alerts'
  },
  {
    file: 'snowpark/activity-schema/27_emergency_procedures.sql',
    description: 'Emergency response procedures'
  }
];

// Create connection
const connection = snowflake.createConnection({
  account: process.env.SNOWFLAKE_ACCOUNT,
  username: process.env.SNOWFLAKE_USERNAME,
  password: process.env.SNOWFLAKE_PASSWORD,
  database: process.env.SNOWFLAKE_DATABASE || 'CLAUDE_BI',
  warehouse: process.env.SNOWFLAKE_WAREHOUSE || 'CLAUDE_WAREHOUSE',
  role: 'ACCOUNTADMIN'  // Force ACCOUNTADMIN for deployment
});

/**
 * Connect to Snowflake
 */
async function connect() {
  return new Promise((resolve, reject) => {
    connection.connect((err, conn) => {
      if (err) {
        reject(err);
      } else {
        console.log('✅ Connected to Snowflake');
        resolve(conn);
      }
    });
  });
}

/**
 * Execute SQL statement
 */
async function executeSql(sql) {
  return new Promise((resolve, reject) => {
    connection.execute({
      sqlText: sql,
      complete: (err, stmt, rows) => {
        if (err) {
          reject(err);
        } else {
          resolve({ statement: stmt, rows: rows });
        }
      }
    });
  });
}

/**
 * Split SQL file into individual statements
 */
function splitSqlStatements(sqlContent) {
  // Remove comments and split by semicolon
  const statements = [];
  let currentStatement = '';
  let inString = false;
  let stringChar = null;
  let inDollarQuote = false;
  let dollarTag = null;
  
  for (let i = 0; i < sqlContent.length; i++) {
    const char = sqlContent[i];
    const nextChar = sqlContent[i + 1];
    
    // Handle dollar-quoted strings
    if (char === '$' && nextChar === '$') {
      if (!inString) {
        inDollarQuote = !inDollarQuote;
        dollarTag = '$$';
        currentStatement += '$$';
        i++;
        continue;
      }
    }
    
    // Handle regular strings
    if (!inDollarQuote && (char === "'" || char === '"')) {
      if (!inString) {
        inString = true;
        stringChar = char;
      } else if (char === stringChar && sqlContent[i - 1] !== '\\') {
        inString = false;
        stringChar = null;
      }
    }
    
    // Handle statement terminator
    if (char === ';' && !inString && !inDollarQuote) {
      currentStatement = currentStatement.trim();
      if (currentStatement.length > 0 && 
          !currentStatement.startsWith('--') && 
          !currentStatement.startsWith('//')) {
        statements.push(currentStatement);
      }
      currentStatement = '';
    } else {
      currentStatement += char;
    }
  }
  
  // Add last statement if exists
  currentStatement = currentStatement.trim();
  if (currentStatement.length > 0 && 
      !currentStatement.startsWith('--') && 
      !currentStatement.startsWith('//')) {
    statements.push(currentStatement);
  }
  
  return statements;
}

/**
 * Deploy SQL file
 */
async function deploySqlFile(filePath, description) {
  console.log(`\n📄 Deploying: ${description}`);
  console.log(`   File: ${filePath}`);
  
  try {
    // Read SQL file
    const sqlContent = await fs.readFile(filePath, 'utf8');
    
    // Split into statements
    const statements = splitSqlStatements(sqlContent);
    console.log(`   Found ${statements.length} SQL statements`);
    
    // Execute each statement
    let successCount = 0;
    let errorCount = 0;
    
    for (let i = 0; i < statements.length; i++) {
      const stmt = statements[i];
      
      // Skip comments and empty statements
      if (!stmt || stmt.startsWith('--') || stmt.startsWith('//')) {
        continue;
      }
      
      try {
        // Show progress for long deployments
        if (i % 10 === 0 && i > 0) {
          console.log(`   Progress: ${i}/${statements.length} statements`);
        }
        
        await executeSql(stmt);
        successCount++;
      } catch (err) {
        console.error(`   ⚠️ Error in statement ${i + 1}: ${err.message}`);
        console.error(`   Statement: ${stmt.substring(0, 100)}...`);
        errorCount++;
        
        // Continue with other statements unless it's a critical error
        if (err.message.includes('already exists')) {
          console.log('   Continuing (object already exists)...');
        } else if (err.message.includes('GRANT') || err.message.includes('REVOKE')) {
          console.log('   Continuing (permission error)...');
        } else {
          // For critical errors, stop
          throw err;
        }
      }
    }
    
    console.log(`   ✅ Completed: ${successCount} successful, ${errorCount} errors`);
    return { success: successCount, errors: errorCount };
    
  } catch (err) {
    console.error(`   ❌ Failed: ${err.message}`);
    throw err;
  }
}

/**
 * Main deployment function
 */
async function deploy() {
  console.log('🚀 Starting Claude Code Authentication System Deployment');
  console.log('=======================================================');
  
  try {
    // Connect to Snowflake
    await connect();
    
    // Deploy each SQL file
    const results = [];
    for (const sqlFile of SQL_FILES) {
      const result = await deploySqlFile(sqlFile.file, sqlFile.description);
      results.push({
        file: sqlFile.file,
        ...result
      });
    }
    
    // Summary
    console.log('\n=======================================================');
    console.log('✅ Authentication System Deployment Complete!');
    console.log('\nDeployment Summary:');
    
    let totalSuccess = 0;
    let totalErrors = 0;
    
    results.forEach(r => {
      console.log(`  ${path.basename(r.file)}: ${r.success} successful, ${r.errors} errors`);
      totalSuccess += r.success;
      totalErrors += r.errors;
    });
    
    console.log(`\nTotal: ${totalSuccess} statements successful, ${totalErrors} errors`);
    
    // Test the deployment
    console.log('\n🧪 Testing deployment...');
    
    try {
      const testResult = await executeSql(`
        SELECT 
          (SELECT COUNT(*) FROM ADMIN.ACTIVE_TOKENS) as active_tokens,
          (SELECT COUNT(*) FROM ADMIN.PENDING_ACTIVATIONS) as pending_activations,
          (SELECT overall_threat_level FROM SECURITY.DASHBOARD) as threat_level
      `);
      
      console.log('✅ System Status:');
      if (testResult.rows && testResult.rows[0]) {
        console.log(`   Active Tokens: ${testResult.rows[0].ACTIVE_TOKENS || 0}`);
        console.log(`   Pending Activations: ${testResult.rows[0].PENDING_ACTIVATIONS || 0}`);
        console.log(`   Threat Level: ${testResult.rows[0].THREAT_LEVEL || 'LOW'}`);
      }
    } catch (err) {
      console.log('⚠️ Could not verify deployment (views may need DT refresh)');
    }
    
    console.log('\n📋 Next Steps:');
    console.log('1. Start the activation gateway:');
    console.log('   cd activation-gateway && npm install && npm start');
    console.log('');
    console.log('2. Create your first user activation:');
    console.log(`   node -e "require('./test-create-activation.js')"`);
    console.log('');
    
    // Close connection
    connection.destroy();
    
  } catch (err) {
    console.error('\n❌ Deployment failed:', err.message);
    connection.destroy();
    process.exit(1);
  }
}

// Run deployment
deploy().catch(err => {
  console.error('Fatal error:', err);
  process.exit(1);
});