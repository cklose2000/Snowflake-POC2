#!/usr/bin/env node

/**
 * Runtime Schema Validation
 * Validates that the live Snowflake environment matches our contract
 * Used for startup validation and health checks
 */

const SchemaSentinel = require('../packages/schema-sentinel');
const snowflake = require('snowflake-sdk');
require('dotenv').config();

async function validateRuntimeSchema() {
  console.log('🛡️  Runtime Schema Validation');
  console.log('=' .repeat(50));
  
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

  try {
    // Connect to Snowflake
    await new Promise((resolve, reject) => {
      connection.connect((err, conn) => {
        if (err) {
          console.error('❌ Failed to connect to Snowflake:', err.message);
          reject(err);
        } else {
          console.log('✅ Connected to Snowflake');
          resolve(conn);
        }
      });
    });

    // Run validation
    const options = {
      throwOnDrift: process.argv.includes('--strict'),
      logActivity: !process.argv.includes('--no-log'),
      skipViewChecks: process.argv.includes('--skip-views'),
      strictMode: process.argv.includes('--strict-mode')
    };

    console.log('\nValidation options:', options);
    console.log('');

    const results = await SchemaSentinel.validate(connection, options);

    // Generate remediation script if issues found
    if (!results.passed && process.argv.includes('--generate-fix')) {
      const sentinel = new SchemaSentinel(connection, options);
      sentinel.validationResults = results;
      const script = sentinel.generateRemediationScript();
      
      const fs = require('fs');
      const scriptPath = './fix-schema.sh';
      fs.writeFileSync(scriptPath, script);
      fs.chmodSync(scriptPath, '755');
      
      console.log(`\n📄 Remediation script generated: ${scriptPath}`);
      console.log('Run: ./fix-schema.sh');
    }

    // Exit code
    const exitCode = results.passed ? 0 : 1;
    console.log(`\n${results.passed ? '✅' : '❌'} Validation ${results.passed ? 'passed' : 'failed'}`);
    
    if (!results.passed) {
      console.log('\n💡 To fix issues, run with --generate-fix flag');
      console.log('   node scripts/validate-runtime-schema.js --generate-fix');
    }
    
    connection.destroy();
    process.exit(exitCode);

  } catch (error) {
    console.error('\n💥 Validation error:', error.message);
    
    connection.destroy();
    process.exit(1);
  }
}

// Show help
if (process.argv.includes('--help') || process.argv.includes('-h')) {
  console.log(`
Runtime Schema Validation

Usage: node scripts/validate-runtime-schema.js [options]

Options:
  --strict           Throw error on any drift (exit code 1)
  --no-log          Don't log validation activity to ACTIVITY.EVENTS
  --skip-views      Skip Activity view validation
  --strict-mode     Enable strict validation mode
  --generate-fix    Generate remediation script on failure
  --help, -h        Show this help message

Examples:
  node scripts/validate-runtime-schema.js
  node scripts/validate-runtime-schema.js --strict --generate-fix
  node scripts/validate-runtime-schema.js --skip-views --no-log
`);
  process.exit(0);
}

// Run validation
validateRuntimeSchema().catch(console.error);