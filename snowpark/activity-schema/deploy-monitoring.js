#!/usr/bin/env node

/**
 * Deploy Enhanced Monitoring Views
 * Comprehensive monitoring while maintaining 2-table architecture
 */

const snowflake = require('snowflake-sdk');
const fs = require('fs');
const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '../../.env') });

console.log('📊 Deploying Enhanced Monitoring Views');
console.log('='.repeat(60));

const connection = snowflake.createConnection({
  account: process.env.SNOWFLAKE_ACCOUNT,
  username: process.env.SNOWFLAKE_USERNAME,
  password: process.env.SNOWFLAKE_PASSWORD,
  role: 'ACCOUNTADMIN',
  database: 'CLAUDE_BI',
  warehouse: 'CLAUDE_WAREHOUSE'
});

const MONITORING_VIEWS = [
  'QUALITY_EVENTS',
  'ID_STABILITY_MONITOR',
  'DEDUP_METRICS',
  'LATE_ARRIVAL_MONITOR',
  'DYNAMIC_TABLE_HEALTH',
  'VALIDATION_FAILURES',
  'PIPELINE_METRICS',
  'NAMESPACE_COMPLIANCE',
  'MICROSECOND_SEQUENCING',
  'DEPENDENCY_CHAIN_HEALTH'
];

async function executeSql(sql, description) {
  return new Promise((resolve, reject) => {
    connection.execute({
      sqlText: sql,
      complete: (err, statement, rows) => {
        if (err) {
          console.log(`❌ ${description}: ${err.message.substring(0, 100)}`);
          reject(err);
        } else {
          if (description) console.log(`✅ ${description}`);
          resolve(rows);
        }
      }
    });
  });
}

async function deploy() {
  try {
    // Connect
    await new Promise((resolve, reject) => {
      connection.connect(err => err ? reject(err) : resolve());
    });
    console.log('✅ Connected to Snowflake\n');

    // Read monitoring SQL file
    const monitoringSQL = fs.readFileSync(
      path.join(__dirname, 'monitoring_enhanced.sql'),
      'utf8'
    );

    // Parse and execute each CREATE VIEW statement
    const viewStatements = monitoringSQL.split(/CREATE OR REPLACE VIEW/i)
      .filter(s => s.trim().length > 0);

    console.log(`📝 Found ${viewStatements.length - 1} monitoring views to deploy\n`);

    // Execute setup statements
    await executeSql('USE ROLE ACCOUNTADMIN', 'Set role');
    await executeSql('USE DATABASE CLAUDE_BI', 'Set database');
    await executeSql('USE SCHEMA MCP', 'Set schema');

    // Deploy each view
    let successCount = 0;
    let failCount = 0;
    
    for (let i = 1; i < viewStatements.length; i++) {
      const viewSQL = 'CREATE OR REPLACE VIEW' + viewStatements[i].split(';')[0] + ';';
      const viewNameMatch = viewSQL.match(/VIEW\s+([\w\.]+)/i);
      const viewName = viewNameMatch ? viewNameMatch[1] : `View ${i}`;
      
      try {
        await executeSql(viewSQL, `Created ${viewName}`);
        successCount++;
      } catch (err) {
        failCount++;
      }
    }

    // Execute grant statements
    console.log('\n🔐 Setting permissions...');
    await executeSql(
      'GRANT SELECT ON ALL VIEWS IN SCHEMA CLAUDE_BI.MCP TO ROLE MCP_ADMIN_ROLE',
      'Granted MCP views to admin'
    );
    await executeSql(
      'GRANT SELECT ON VIEW CLAUDE_BI.MCP.PIPELINE_METRICS TO ROLE MCP_SERVICE_ROLE',
      'Granted pipeline metrics to service'
    );
    await executeSql(
      'GRANT SELECT ON VIEW CLAUDE_BI.MCP.DYNAMIC_TABLE_HEALTH TO ROLE MCP_SERVICE_ROLE',
      'Granted DT health to service'
    );
    await executeSql(
      'GRANT SELECT ON VIEW CLAUDE_BI.ACTIVITY.QUALITY_EVENTS TO ROLE MCP_SERVICE_ROLE',
      'Granted quality events to service'
    );

    // Test each view
    console.log('\n🧪 Testing monitoring views...');
    const testResults = [];
    
    for (const viewName of MONITORING_VIEWS) {
      try {
        const schema = viewName === 'QUALITY_EVENTS' ? 'ACTIVITY' : 'MCP';
        const result = await executeSql(
          `SELECT COUNT(*) as cnt FROM CLAUDE_BI.${schema}.${viewName} LIMIT 1`,
          null
        );
        testResults.push({ view: viewName, status: 'OK', rows: result[0]?.CNT || 0 });
        console.log(`✅ ${viewName}: Working`);
      } catch (err) {
        testResults.push({ view: viewName, status: 'ERROR', error: err.message.substring(0, 50) });
        console.log(`❌ ${viewName}: ${err.message.substring(0, 50)}`);
      }
    }

    // Summary
    console.log('\n' + '='.repeat(60));
    console.log('📊 Deployment Summary');
    console.log('='.repeat(60));
    console.log(`✅ Views deployed: ${successCount}`);
    if (failCount > 0) console.log(`❌ Views failed: ${failCount}`);
    
    const workingViews = testResults.filter(r => r.status === 'OK').length;
    console.log(`\n🧪 Test Results: ${workingViews}/${MONITORING_VIEWS.length} views working`);

    console.log('\n✨ Monitoring suite deployed!');
    console.log('\n📈 Key Monitoring Views:');
    console.log('   • ID_STABILITY_MONITOR - Track SHA2 ID uniqueness');
    console.log('   • DEDUP_METRICS - Monitor duplicate removal');
    console.log('   • PIPELINE_METRICS - End-to-end health');
    console.log('   • QUALITY_EVENTS - Validation failures (as events!)');
    console.log('   • NAMESPACE_COMPLIANCE - Reserved namespace violations');

  } catch (err) {
    console.error('\n❌ Deployment failed:', err.message);
    process.exit(1);
  } finally {
    connection.destroy();
  }
}

deploy().catch(console.error);