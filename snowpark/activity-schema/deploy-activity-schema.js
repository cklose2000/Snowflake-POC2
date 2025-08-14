#!/usr/bin/env node

/**
 * Deploy Activity Schema 2.0
 * Automated deployment of the complete event-based data warehouse
 */

const snowflake = require('snowflake-sdk');
const fs = require('fs');
const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '../../.env') });

console.log('🚀 Activity Schema 2.0 Deployment');
console.log('='.repeat(60));
console.log('Deploying hyper-simple data warehouse with event-based permissions\n');

// SQL files to deploy in order
const SQL_FILES = [
  '01_setup_database.sql',
  '02_create_raw_events.sql',
  '03_create_dynamic_table.sql',
  '04_create_roles.sql',
  '05_mcp_procedures.sql',
  '06_monitoring_views.sql',
  '07_user_management.sql',
  '08_test_setup.sql'
];

// Create connection
const connection = snowflake.createConnection({
  account: process.env.SNOWFLAKE_ACCOUNT,
  username: process.env.SNOWFLAKE_USERNAME,
  password: process.env.SNOWFLAKE_PASSWORD,
  role: 'ACCOUNTADMIN',
  database: 'CLAUDE_BI',
  warehouse: 'CLAUDE_WAREHOUSE'
});

/**
 * Execute SQL statement
 */
function executeSql(sqlText) {
  return new Promise((resolve, reject) => {
    connection.execute({
      sqlText: sqlText,
      complete: (err, statement, rows) => {
        if (err) {
          reject(err);
        } else {
          resolve(rows);
        }
      }
    });
  });
}

/**
 * Parse SQL file into individual statements
 */
function parseSqlFile(filePath) {
  const content = fs.readFileSync(filePath, 'utf8');
  
  // Remove comments and split by semicolon
  const statements = content
    .split('\n')
    .filter(line => !line.trim().startsWith('--'))
    .join('\n')
    .split(/;\s*$/m)
    .map(stmt => stmt.trim())
    .filter(stmt => stmt.length > 10);
  
  return statements;
}

/**
 * Deploy a single SQL file
 */
async function deploySqlFile(fileName) {
  console.log(`\n📄 Deploying ${fileName}`);
  console.log('-'.repeat(40));
  
  const filePath = path.join(__dirname, fileName);
  
  if (!fs.existsSync(filePath)) {
    console.log(`⚠️  File not found: ${fileName}`);
    return { file: fileName, success: 0, errors: 0, skipped: 1 };
  }
  
  const statements = parseSqlFile(filePath);
  console.log(`Found ${statements.length} statements`);
  
  let successCount = 0;
  let errorCount = 0;
  const errors = [];
  
  for (let i = 0; i < statements.length; i++) {
    const stmt = statements[i];
    const preview = stmt.substring(0, 50).replace(/\s+/g, ' ');
    
    try {
      // Skip certain statements during deployment
      if (stmt.includes('SHOW ') || stmt.includes('DESCRIBE ') || stmt.includes('SELECT ')) {
        console.log(`⏭️  [${i+1}/${statements.length}] Skipping: ${preview}...`);
        continue;
      }
      
      await executeSql(stmt + ';');
      console.log(`✅ [${i+1}/${statements.length}] ${preview}...`);
      successCount++;
      
    } catch (err) {
      console.log(`❌ [${i+1}/${statements.length}] ${preview}...`);
      console.log(`   Error: ${err.message.substring(0, 100)}`);
      errorCount++;
      errors.push({
        statement: preview,
        error: err.message
      });
    }
  }
  
  return {
    file: fileName,
    success: successCount,
    errors: errorCount,
    errorDetails: errors
  };
}

/**
 * Verify deployment
 */
async function verifyDeployment() {
  console.log('\n🔍 Verifying Deployment');
  console.log('='.repeat(60));
  
  const checks = [
    {
      name: 'Database exists',
      sql: "SELECT COUNT(*) AS cnt FROM INFORMATION_SCHEMA.DATABASES WHERE DATABASE_NAME = 'CLAUDE_BI'"
    },
    {
      name: 'Schemas created',
      sql: "SELECT COUNT(*) AS cnt FROM INFORMATION_SCHEMA.SCHEMATA WHERE CATALOG_NAME = 'CLAUDE_BI' AND SCHEMA_NAME IN ('LANDING', 'ACTIVITY', 'MCP')"
    },
    {
      name: 'Landing table exists',
      sql: "SELECT COUNT(*) AS cnt FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG = 'CLAUDE_BI' AND TABLE_SCHEMA = 'LANDING' AND TABLE_NAME = 'RAW_EVENTS'"
    },
    {
      name: 'Dynamic table exists',
      sql: "SELECT COUNT(*) AS cnt FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG = 'CLAUDE_BI' AND TABLE_SCHEMA = 'ACTIVITY' AND TABLE_NAME = 'EVENTS'"
    },
    {
      name: 'MCP procedures created',
      sql: "SELECT COUNT(*) AS cnt FROM INFORMATION_SCHEMA.PROCEDURES WHERE PROCEDURE_CATALOG = 'CLAUDE_BI' AND PROCEDURE_SCHEMA = 'MCP'"
    },
    {
      name: 'Test events loaded',
      sql: "SELECT COUNT(*) AS cnt FROM CLAUDE_BI.ACTIVITY.EVENTS WHERE source IN ('ecommerce', 'auth', 'payments')"
    },
    {
      name: 'Test users created',
      sql: "SELECT COUNT(*) AS cnt FROM CLAUDE_BI.MCP.CURRENT_USER_PERMISSIONS WHERE status = 'ACTIVE'"
    }
  ];
  
  const results = [];
  for (const check of checks) {
    try {
      const result = await executeSql(check.sql);
      const count = result[0].CNT;
      const passed = count > 0;
      results.push({
        name: check.name,
        passed: passed,
        count: count
      });
      console.log(`${passed ? '✅' : '❌'} ${check.name}: ${count}`);
    } catch (err) {
      results.push({
        name: check.name,
        passed: false,
        error: err.message
      });
      console.log(`❌ ${check.name}: ${err.message.substring(0, 50)}`);
    }
  }
  
  return results;
}

/**
 * Main deployment function
 */
async function deploy() {
  try {
    // Connect to Snowflake
    console.log('🔌 Connecting to Snowflake...');
    await new Promise((resolve, reject) => {
      connection.connect((err) => {
        if (err) {
          reject(err);
        } else {
          resolve();
        }
      });
    });
    console.log('✅ Connected\n');
    
    // Deploy each SQL file
    const deploymentResults = [];
    for (const file of SQL_FILES) {
      const result = await deploySqlFile(file);
      deploymentResults.push(result);
      
      // Stop if critical file fails
      if (result.errors > 0 && SQL_FILES.indexOf(file) < 4) {
        console.log('\n⚠️  Critical file failed, stopping deployment');
        break;
      }
    }
    
    // Verify deployment
    const verificationResults = await verifyDeployment();
    
    // Summary
    console.log('\n' + '='.repeat(60));
    console.log('📊 Deployment Summary');
    console.log('='.repeat(60));
    
    console.log('\nFile Results:');
    deploymentResults.forEach(r => {
      const status = r.errors === 0 ? '✅' : '⚠️';
      console.log(`  ${status} ${r.file}: ${r.success} succeeded, ${r.errors} failed`);
    });
    
    const allVerificationsPassed = verificationResults.every(r => r.passed);
    console.log('\nVerification:', allVerificationsPassed ? '✅ All checks passed' : '⚠️  Some checks failed');
    
    if (allVerificationsPassed) {
      console.log('\n✨ Activity Schema 2.0 deployed successfully!');
      console.log('\n📝 Next Steps:');
      console.log('  1. Test users have been created with password: TempPassword123!');
      console.log('  2. Users must change password on first login');
      console.log('  3. Run test-mcp-access.js to validate permissions');
      console.log('  4. Use monitoring queries to track activity');
      console.log('\n🔍 Test the system:');
      console.log('  node test-mcp-access.js');
    } else {
      console.log('\n⚠️  Deployment completed with issues. Review the errors above.');
    }
    
  } catch (err) {
    console.error('\n❌ Deployment failed:', err.message);
    process.exit(1);
  } finally {
    connection.destroy();
  }
}

// Run deployment
console.log('Prerequisites:');
console.log('  • Snowflake account with ACCOUNTADMIN role');
console.log('  • Environment variables configured in .env');
console.log('  • Network access to Snowflake');
console.log('\nStarting deployment in 3 seconds...\n');

setTimeout(() => {
  deploy().catch(console.error);
}, 3000);