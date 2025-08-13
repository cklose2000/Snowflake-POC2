#!/usr/bin/env node

// Validate Snowflake schema matches expectations
// Usage: npm run validate-schema

const snowflake = require('snowflake-sdk');
const SchemaValidator = require('../packages/snowflake-schema/validator');
const schemaConfig = require('../packages/snowflake-schema');
require('dotenv').config();

async function validateSchema() {
  console.log('🔍 Snowflake Schema Validation\n');
  console.log('='.repeat(60));
  
  // Show configuration
  console.log('\n📋 Expected Configuration:');
  const config = schemaConfig.config.exportConfig();
  console.log(`   Database: ${config.database}`);
  console.log(`   Warehouse: ${config.warehouse}`);
  console.log(`   Role: ${config.role}`);
  console.log(`   Default Schema: ${config.defaultSchema}`);
  console.log(`   Known Schemas: ${config.schemas.map(s => s.name).join(', ')}`);
  
  console.log('\n' + '='.repeat(60));
  console.log('\n🔌 Connecting to Snowflake...\n');
  
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
        console.log('✅ Connected to Snowflake');
        resolve(conn);
      }
    });
  });
  
  // Set context
  console.log('\n🔧 Setting database context...');
  const contextSQL = schemaConfig.getContextSQL();
  for (const sql of contextSQL) {
    console.log(`   Executing: ${sql}`);
    await new Promise((resolve, reject) => {
      connection.execute({
        sqlText: sql,
        complete: (err) => {
          if (err) {
            console.error(`   ❌ Failed: ${err.message}`);
            reject(err);
          } else {
            console.log(`   ✅ Success`);
            resolve();
          }
        }
      });
    });
  }
  
  console.log('\n' + '='.repeat(60));
  
  // Run validation
  const validator = new SchemaValidator(connection);
  const results = await validator.validateAll();
  
  // Display results
  console.log('\n' + '='.repeat(60));
  console.log('\n📊 VALIDATION RESULTS\n');
  
  // Context
  if (results.context) {
    console.log('Current Context:');
    console.log(`   Database: ${results.context.database}`);
    console.log(`   Schema: ${results.context.schema}`);
    console.log(`   Role: ${results.context.role}`);
    console.log(`   Warehouse: ${results.context.warehouse}`);
    console.log(`   User: ${results.context.user}`);
  }
  
  // Schema existence
  if (results.existence) {
    console.log('\nSchema Existence:');
    console.log(`   Database ${config.database}: ${results.existence.database ? '✅' : '❌'}`);
    for (const [schema, exists] of Object.entries(results.existence.schemas || {})) {
      console.log(`   Schema ${schema}: ${exists ? '✅' : '❌'}`);
    }
  }
  
  // Table existence
  if (results.tables) {
    console.log('\nTable Existence:');
    for (const [schema, tables] of Object.entries(results.tables)) {
      for (const [table, info] of Object.entries(tables)) {
        const status = info.exists ? '✅' : '❌';
        const colCount = info.columns ? `(${info.columns.length} columns)` : '';
        console.log(`   ${schema}.${table}: ${status} ${colCount}`);
      }
    }
  }
  
  // Activity Schema v2
  if (results.activitySchema) {
    console.log('\nActivity Schema v2 Compliance:');
    console.log(`   Table exists: ${results.activitySchema.hasTable ? '✅' : '❌'}`);
    
    if (results.activitySchema.hasRequiredColumns) {
      console.log('   Required columns:');
      for (const [col, exists] of Object.entries(results.activitySchema.hasRequiredColumns)) {
        console.log(`      ${col}: ${exists ? '✅' : '❌'}`);
      }
    }
    
    if (results.activitySchema.hasV2Columns) {
      console.log('   V2 specific columns:');
      for (const [col, exists] of Object.entries(results.activitySchema.hasV2Columns)) {
        console.log(`      ${col}: ${exists ? '✅' : '❌'}`);
      }
    }
  }
  
  // Privileges
  if (results.privileges) {
    console.log('\nPrivileges:');
    for (const [priv, granted] of Object.entries(results.privileges)) {
      const formatted = priv.replace(/_/g, ' ');
      console.log(`   ${formatted}: ${granted ? '✅' : '❌'}`);
    }
  }
  
  // Query tag
  if (results.queryTag) {
    console.log('\nQuery Tag:');
    console.log(`   Can set tag: ${results.queryTag.tagSet ? '✅' : '❌'}`);
    console.log(`   Tag visible in history: ${results.queryTag.tagVisible ? '✅' : '⚠️ (may need elevated privileges)'}`);
  }
  
  // Errors and warnings
  console.log('\n' + '='.repeat(60));
  
  if (results.errors && results.errors.length > 0) {
    console.error('\n❌ ERRORS FOUND:\n');
    results.errors.forEach((error, i) => {
      console.error(`${i + 1}. [${error.code}] ${error.message}`);
      if (error.remediation) {
        console.error(`   Fix: ${error.remediation}`);
      }
    });
  }
  
  if (results.warnings && results.warnings.length > 0) {
    console.warn('\n⚠️ WARNINGS:\n');
    results.warnings.forEach((warning, i) => {
      console.warn(`${i + 1}. [${warning.code}] ${warning.message}`);
      if (warning.remediation) {
        console.warn(`   Note: ${warning.remediation}`);
      }
    });
  }
  
  // Summary
  console.log('\n' + '='.repeat(60));
  if (results.isValid) {
    console.log('\n✅ VALIDATION PASSED');
    console.log('Your Snowflake schema matches the expected structure.');
  } else {
    console.error('\n❌ VALIDATION FAILED');
    console.error(`Found ${results.errors.length} error(s) that must be fixed.`);
    console.error('\nTo fix the issues, run: npm run bootstrap-schema');
  }
  
  if (results.hasWarnings) {
    console.warn(`\n⚠️ Found ${results.warnings.length} warning(s) to review.`);
  }
  
  // Disconnect
  connection.destroy();
  
  // Exit with appropriate code
  process.exit(results.isValid ? 0 : 1);
}

// Run if called directly
if (require.main === module) {
  validateSchema().catch(error => {
    console.error('\n💥 Fatal error:', error.message);
    process.exit(1);
  });
}

module.exports = { validateSchema };