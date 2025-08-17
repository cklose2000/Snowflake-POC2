#!/usr/bin/env node
/**
 * Verify Dashboard Deployment
 * 
 * Checks that dashboard is properly deployed and accessible
 * 
 * Usage: node verify-dashboard.js --version=v20240101_123456_abc123 --dashboards="coo_dashboard"
 */

const { execSync } = require('child_process');
const fs = require('fs');
const path = require('path');

// Parse arguments
const args = process.argv.slice(2);
const argMap = {};
args.forEach(arg => {
  const [key, value] = arg.split('=');
  argMap[key.replace('--', '')] = value;
});

const version = argMap.version;
const dashboards = argMap.dashboards ? argMap.dashboards.split(' ') : [];

if (!version) {
  console.error('❌ Version is required');
  process.exit(1);
}

// Ensure SF_PK_PATH is set
const keyPath = process.env.SF_PK_PATH || './claude_code_rsa_key.p8';
if (!fs.existsSync(keyPath)) {
  console.error(`❌ RSA key not found at ${keyPath}`);
  process.exit(1);
}

/**
 * Execute Snowflake SQL command
 */
function executeSql(sql, silent = false) {
  const sfPath = path.join(process.env.HOME, 'bin', 'sf');
  const cmd = `SF_PK_PATH=${keyPath} ${sfPath} sql "${sql.replace(/"/g, '\\"').replace(/\n/g, ' ')}"`;
  
  try {
    const result = execSync(cmd, { encoding: 'utf8' });
    if (!silent) {
      console.log(result);
    }
    return result;
  } catch (error) {
    console.error(`❌ SQL execution failed: ${error.message}`);
    throw error;
  }
}

/**
 * Verify dashboard exists and is running the correct version
 */
async function verifyDashboard(dashboard, version) {
  const appName = dashboard.toUpperCase().replace(/-/g, '_');
  
  console.log(`\n🔍 Verifying ${appName}...`);
  
  // Check if app exists
  const checkSql = `
    SELECT 
      name,
      root_location,
      main_file,
      comment,
      created_on
    FROM CLAUDE_BI.INFORMATION_SCHEMA.STREAMLITS
    WHERE name = '${appName}'
      AND schema_name = 'MCP'
      AND catalog_name = 'CLAUDE_BI';
  `;
  
  try {
    const result = executeSql(checkSql, true);
    
    if (!result.includes(appName)) {
      console.error(`❌ Dashboard ${appName} not found`);
      return false;
    }
    
    // Check if it's using the correct version
    if (result.includes(version)) {
      console.log(`✅ Dashboard ${appName} is running version ${version}`);
      return true;
    } else {
      console.error(`⚠️ Dashboard ${appName} exists but not running version ${version}`);
      
      // Try to extract current version from comment or root_location
      const versionMatch = result.match(/v\d{8}_\d{6}_[a-f0-9]+/);
      if (versionMatch) {
        console.log(`   Current version: ${versionMatch[0]}`);
      }
      
      return false;
    }
  } catch (error) {
    console.error(`❌ Failed to verify ${appName}: ${error.message}`);
    return false;
  }
}

/**
 * Verify files exist in stage
 */
async function verifyStageFiles(dashboard, version) {
  console.log(`\n📁 Verifying stage files for ${dashboard}...`);
  
  const listSql = `
    LIST @CLAUDE_BI.MCP.DASH_APPS/${dashboard}/${version}/;
  `;
  
  try {
    const result = executeSql(listSql, true);
    
    // Check for Python files
    if (result.includes('.py')) {
      const pyFiles = result.match(/\w+\.py/g) || [];
      console.log(`✅ Found ${pyFiles.length} Python files:`);
      pyFiles.forEach(file => console.log(`   - ${file}`));
      
      // Check for metadata
      if (result.includes('metadata.json')) {
        console.log(`✅ Metadata file present`);
      } else {
        console.log(`⚠️ Metadata file missing`);
      }
      
      return true;
    } else {
      console.error(`❌ No Python files found in stage`);
      return false;
    }
  } catch (error) {
    console.error(`❌ Failed to list stage files: ${error.message}`);
    return false;
  }
}

/**
 * Test dashboard query capability
 */
async function testDashboardQuery(dashboard) {
  const appName = dashboard.toUpperCase().replace(/-/g, '_');
  
  console.log(`\n🧪 Testing query capability for ${appName}...`);
  
  // Try a simple query that dashboard might run
  const testSql = `
    -- Test query as if from dashboard context
    SELECT COUNT(*) as event_count
    FROM CLAUDE_BI.ACTIVITY.EVENTS
    WHERE occurred_at >= DATEADD('day', -1, CURRENT_TIMESTAMP())
    LIMIT 1;
  `;
  
  try {
    const result = executeSql(testSql, true);
    
    if (result.includes('EVENT_COUNT') || result.includes('event_count')) {
      console.log(`✅ Dashboard can query ACTIVITY.EVENTS`);
      return true;
    } else {
      console.error(`⚠️ Query test returned unexpected result`);
      return false;
    }
  } catch (error) {
    console.error(`❌ Query test failed: ${error.message}`);
    return false;
  }
}

/**
 * Main verification function
 */
async function main() {
  console.log('🔍 Dashboard Deployment Verification');
  console.log(`📦 Version: ${version}`);
  console.log(`🎯 Dashboards: ${dashboards.join(', ')}`);
  console.log('');

  let allPassed = true;
  const results = [];

  for (const dashboard of dashboards) {
    console.log(`\n${'='.repeat(60)}`);
    console.log(`📊 Verifying: ${dashboard}`);
    console.log(`${'='.repeat(60)}`);

    const checks = {
      dashboard: dashboard,
      appExists: false,
      correctVersion: false,
      stageFiles: false,
      queryCapability: false
    };

    // Run verification checks
    checks.appExists = await verifyDashboard(dashboard, version);
    checks.stageFiles = await verifyStageFiles(dashboard, version);
    
    if (checks.appExists) {
      checks.correctVersion = checks.appExists; // Already checked in verifyDashboard
      checks.queryCapability = await testDashboardQuery(dashboard);
    }

    // Determine overall status
    const passed = checks.appExists && checks.correctVersion && checks.stageFiles;
    checks.overall = passed ? 'PASSED' : 'FAILED';
    
    if (!passed) {
      allPassed = false;
    }

    results.push(checks);

    // Display summary for this dashboard
    console.log(`\n📋 Verification Summary for ${dashboard}:`);
    console.log(`  App Exists:        ${checks.appExists ? '✅' : '❌'}`);
    console.log(`  Correct Version:   ${checks.correctVersion ? '✅' : '❌'}`);
    console.log(`  Stage Files:       ${checks.stageFiles ? '✅' : '❌'}`);
    console.log(`  Query Capability:  ${checks.queryCapability ? '✅' : '⚠️'}`);
    console.log(`  Overall:           ${checks.overall === 'PASSED' ? '✅ PASSED' : '❌ FAILED'}`);
  }

  // Final summary
  console.log(`\n${'='.repeat(60)}`);
  console.log('📊 VERIFICATION SUMMARY');
  console.log(`${'='.repeat(60)}`);
  
  const passed = results.filter(r => r.overall === 'PASSED');
  const failed = results.filter(r => r.overall === 'FAILED');
  
  console.log(`✅ Passed: ${passed.length}/${results.length}`);
  console.log(`❌ Failed: ${failed.length}/${results.length}`);
  
  if (failed.length > 0) {
    console.log('\n❌ Failed verifications:');
    failed.forEach(r => {
      console.log(`  - ${r.dashboard}`);
      if (!r.appExists) console.log(`    • App doesn't exist`);
      if (!r.correctVersion) console.log(`    • Wrong version deployed`);
      if (!r.stageFiles) console.log(`    • Stage files missing`);
    });
    
    process.exit(1);
  }
  
  console.log('\n✅ All verifications passed!');
}

// Run if executed directly
if (require.main === module) {
  main().catch(error => {
    console.error('❌ Verification failed:', error.message);
    process.exit(1);
  });
}

module.exports = { main, verifyDashboard, verifyStageFiles };