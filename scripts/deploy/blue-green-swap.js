#!/usr/bin/env node
/**
 * Blue-Green Deployment for Snowflake Streamlit Dashboards
 * 
 * Implements zero-downtime deployment by:
 * 1. Creating new "green" dashboard with new version
 * 2. Testing the green dashboard
 * 3. Swapping blue and green (updating the main dashboard)
 * 4. Keeping old version as backup
 * 
 * Usage: node blue-green-swap.js --version=v20240101_123456_abc123 --dashboards="coo_dashboard"
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
const environment = argMap.environment || 'production';

if (!version) {
  console.error('❌ Version is required');
  process.exit(1);
}

if (dashboards.length === 0) {
  console.error('❌ No dashboards specified');
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
 * Get dashboard app name in Snowflake format
 */
function getAppName(dashboard) {
  return dashboard.toUpperCase().replace(/-/g, '_');
}

/**
 * Create or replace Streamlit app with specific version
 */
async function deployDashboard(dashboard, version, isGreen = false) {
  const appName = getAppName(dashboard);
  const appFullName = isGreen ? `${appName}_GREEN` : appName;
  
  console.log(`\n🔵 Deploying ${appFullName} with version ${version}`);
  
  // Determine main file (assume main.py or <dashboard>.py)
  const mainFile = fs.existsSync(path.join('dashboards', dashboard, 'main.py')) 
    ? 'main.py' 
    : `${dashboard}.py`;
  
  // Create the Streamlit app pointing to versioned file
  const sql = `
    CREATE OR REPLACE STREAMLIT CLAUDE_BI.MCP.${appFullName}
    ROOT_LOCATION = '@CLAUDE_BI.MCP.DASH_APPS/${dashboard}/${version}'
    MAIN_FILE = '${mainFile}'
    QUERY_WAREHOUSE = 'CLAUDE_WAREHOUSE'
    COMMENT = 'Version: ${version}, Environment: ${environment}, Deployed: ${new Date().toISOString()}';
  `;
  
  try {
    const result = executeSql(sql, true);
    
    // Extract URL ID from result if available
    const urlMatch = result.match(/([a-z0-9]{20})/);
    const urlId = urlMatch ? urlMatch[1] : 'unknown';
    
    console.log(`✅ Created ${appFullName}`);
    console.log(`📍 URL ID: ${urlId}`);
    
    return { appName: appFullName, urlId, version };
  } catch (error) {
    console.error(`❌ Failed to create ${appFullName}: ${error.message}`);
    throw error;
  }
}

/**
 * Test dashboard health
 */
async function testDashboard(appName) {
  console.log(`\n🧪 Testing ${appName}...`);
  
  // Query to check if app exists and get details
  const sql = `
    SELECT 
      name,
      created_on,
      comment
    FROM CLAUDE_BI.INFORMATION_SCHEMA.STREAMLITS
    WHERE name = '${appName}'
      AND schema_name = 'MCP'
      AND catalog_name = 'CLAUDE_BI';
  `;
  
  try {
    const result = executeSql(sql, true);
    
    if (result.includes(appName)) {
      console.log(`✅ Dashboard ${appName} exists and is accessible`);
      return true;
    } else {
      console.error(`❌ Dashboard ${appName} not found`);
      return false;
    }
  } catch (error) {
    console.error(`❌ Failed to test ${appName}: ${error.message}`);
    return false;
  }
}

/**
 * Swap blue and green deployments
 */
async function swapDeployments(dashboard, greenVersion) {
  const appName = getAppName(dashboard);
  const blueApp = appName;
  const greenApp = `${appName}_GREEN`;
  const backupApp = `${appName}_BACKUP`;
  
  console.log(`\n🔄 Swapping deployments for ${dashboard}`);
  console.log(`  Blue (current): ${blueApp}`);
  console.log(`  Green (new): ${greenApp} (version: ${greenVersion})`);
  
  try {
    // Step 1: Drop old backup if exists
    console.log('  1️⃣ Removing old backup...');
    executeSql(`DROP STREAMLIT IF EXISTS CLAUDE_BI.MCP.${backupApp};`, true);
    
    // Step 2: Rename current blue to backup
    console.log('  2️⃣ Backing up current version...');
    executeSql(`ALTER STREAMLIT CLAUDE_BI.MCP.${blueApp} RENAME TO ${backupApp};`, true);
    
    // Step 3: Rename green to blue
    console.log('  3️⃣ Promoting green to production...');
    executeSql(`ALTER STREAMLIT CLAUDE_BI.MCP.${greenApp} RENAME TO ${blueApp};`, true);
    
    console.log(`✅ Swap complete! ${blueApp} now running version ${greenVersion}`);
    
    // Log the swap event
    const eventSql = `
      INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS (PAYLOAD, _SOURCE_LANE, _RECV_AT)
      SELECT 
        OBJECT_CONSTRUCT(
          'event_id', UUID_STRING(),
          'action', 'dashboard.blue_green.swapped',
          'actor_id', '${process.env.GITHUB_ACTOR || process.env.USER || 'system'}',
          'object', OBJECT_CONSTRUCT(
            'type', 'dashboard',
            'id', '${dashboard}',
            'app_name', '${blueApp}'
          ),
          'attributes', OBJECT_CONSTRUCT(
            'new_version', '${greenVersion}',
            'backup_created', '${backupApp}',
            'environment', '${environment}'
          ),
          'occurred_at', CURRENT_TIMESTAMP()
        ),
        'BLUE_GREEN_DEPLOY',
        CURRENT_TIMESTAMP();
    `;
    
    executeSql(eventSql, true);
    
    return true;
  } catch (error) {
    console.error(`❌ Swap failed: ${error.message}`);
    console.log('🔄 Attempting rollback...');
    
    // Try to rollback
    try {
      executeSql(`ALTER STREAMLIT CLAUDE_BI.MCP.${backupApp} RENAME TO ${blueApp};`, true);
      console.log('✅ Rollback successful');
    } catch (rollbackError) {
      console.error('❌ Rollback failed:', rollbackError.message);
    }
    
    throw error;
  }
}

/**
 * Main deployment function
 */
async function main() {
  console.log(`🚀 Blue-Green Deployment`);
  console.log(`📦 Version: ${version}`);
  console.log(`🎯 Dashboards: ${dashboards.join(', ')}`);
  console.log(`🌍 Environment: ${environment}`);
  console.log('');

  const deploymentResults = [];

  for (const dashboard of dashboards) {
    console.log(`\n${'='.repeat(60)}`);
    console.log(`📊 Processing: ${dashboard}`);
    console.log(`${'='.repeat(60)}`);

    try {
      // Step 1: Deploy to green
      const greenDeployment = await deployDashboard(dashboard, version, true);
      
      // Step 2: Test green deployment
      const testPassed = await testDashboard(greenDeployment.appName);
      
      if (!testPassed) {
        console.error(`❌ Green deployment test failed for ${dashboard}`);
        
        // Clean up failed green deployment
        executeSql(`DROP STREAMLIT IF EXISTS CLAUDE_BI.MCP.${greenDeployment.appName};`, true);
        
        deploymentResults.push({
          dashboard,
          status: 'failed',
          reason: 'Green deployment test failed'
        });
        continue;
      }
      
      // Step 3: Perform blue-green swap
      await swapDeployments(dashboard, version);
      
      deploymentResults.push({
        dashboard,
        status: 'success',
        version,
        urlId: greenDeployment.urlId
      });
      
      console.log(`\n✅ ${dashboard} successfully deployed!`);
      
    } catch (error) {
      console.error(`\n❌ Deployment failed for ${dashboard}: ${error.message}`);
      
      deploymentResults.push({
        dashboard,
        status: 'failed',
        reason: error.message
      });
    }
  }

  // Summary
  console.log(`\n${'='.repeat(60)}`);
  console.log('📊 DEPLOYMENT SUMMARY');
  console.log(`${'='.repeat(60)}`);
  
  const successful = deploymentResults.filter(r => r.status === 'success');
  const failed = deploymentResults.filter(r => r.status === 'failed');
  
  console.log(`✅ Successful: ${successful.length}`);
  console.log(`❌ Failed: ${failed.length}`);
  
  if (successful.length > 0) {
    console.log('\n✅ Successfully deployed:');
    successful.forEach(r => {
      console.log(`  - ${r.dashboard} (v${r.version})`);
      console.log(`    URL: https://app.snowflake.com/uec18397/us-east-1/streamlit-apps/CLAUDE_BI/MCP/${getAppName(r.dashboard)}/${r.urlId}`);
    });
  }
  
  if (failed.length > 0) {
    console.log('\n❌ Failed deployments:');
    failed.forEach(r => {
      console.log(`  - ${r.dashboard}: ${r.reason}`);
    });
    
    // Exit with error if any deployments failed
    process.exit(1);
  }
  
  console.log('\n🎉 Blue-green deployment complete!');
}

// Run if executed directly
if (require.main === module) {
  main().catch(error => {
    console.error('❌ Deployment failed:', error.message);
    process.exit(1);
  });
}

module.exports = { main, deployDashboard, swapDeployments };