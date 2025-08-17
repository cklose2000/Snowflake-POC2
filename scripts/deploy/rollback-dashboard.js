#!/usr/bin/env node
/**
 * Rollback Dashboard to Previous Version
 * 
 * Uses event history to find and restore a previous dashboard version
 * 
 * Usage: 
 *   node rollback-dashboard.js --version=v20240101_123456_abc123  # Rollback to specific version
 *   node rollback-dashboard.js --dashboard=coo_dashboard --steps=1  # Rollback N versions
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
 * Parse SQL result to extract data
 */
function parseSqlResult(result) {
  const lines = result.split('\n').filter(line => line.trim());
  const dataLines = lines.filter(line => line.includes('|') && !line.includes('---'));
  
  if (dataLines.length === 0) {
    return [];
  }
  
  // Parse header
  const headerLine = dataLines[0];
  const headers = headerLine.split('|').map(h => h.trim()).filter(h => h);
  
  // Parse data rows
  const rows = [];
  for (let i = 1; i < dataLines.length; i++) {
    const values = dataLines[i].split('|').map(v => v.trim()).filter(v => v);
    const row = {};
    headers.forEach((header, index) => {
      row[header] = values[index] || '';
    });
    rows.push(row);
  }
  
  return rows;
}

/**
 * Get deployment history for a dashboard
 */
async function getDeploymentHistory(dashboard, limit = 10) {
  console.log(`📜 Getting deployment history for ${dashboard}...`);
  
  const sql = `
    SELECT 
      attributes:version::STRING as version,
      actor_id,
      occurred_at,
      attributes:environment::STRING as environment,
      attributes:commit_sha::STRING as commit_sha
    FROM CLAUDE_BI.ACTIVITY.EVENTS
    WHERE action = 'dashboard.version.active'
      AND object:id = '${dashboard}'
    ORDER BY occurred_at DESC
    LIMIT ${limit};
  `;
  
  try {
    const result = executeSql(sql, true);
    const history = parseSqlResult(result);
    
    if (history.length === 0) {
      console.log('⚠️ No deployment history found');
      return [];
    }
    
    console.log(`✅ Found ${history.length} deployments`);
    history.forEach((deployment, index) => {
      console.log(`  ${index + 1}. ${deployment.version} - ${deployment.occurred_at} (${deployment.environment})`);
    });
    
    return history;
  } catch (error) {
    console.error(`❌ Failed to get history: ${error.message}`);
    return [];
  }
}

/**
 * Check if version exists in stage
 */
async function checkVersionExists(dashboard, version) {
  const sql = `
    LIST @CLAUDE_BI.MCP.DASH_APPS/${dashboard}/${version}/;
  `;
  
  try {
    const result = executeSql(sql, true);
    return result.includes(version) && result.includes('.py');
  } catch (error) {
    // LIST command might fail if path doesn't exist
    return false;
  }
}

/**
 * Perform rollback
 */
async function performRollback(dashboard, targetVersion) {
  const appName = dashboard.toUpperCase().replace(/-/g, '_');
  
  console.log(`\n🔄 Rolling back ${appName} to version ${targetVersion}...`);
  
  // Check if version exists in stage
  const versionExists = await checkVersionExists(dashboard, targetVersion);
  if (!versionExists) {
    console.error(`❌ Version ${targetVersion} not found in stage for ${dashboard}`);
    return false;
  }
  
  // Determine main file
  const mainFile = `${dashboard}.py`;  // Assume standard naming
  
  try {
    // Step 1: Backup current version
    console.log('📦 Creating backup of current version...');
    const backupSql = `
      CREATE OR REPLACE STREAMLIT CLAUDE_BI.MCP.${appName}_ROLLBACK_BACKUP
      ROOT_LOCATION = (SELECT ROOT_LOCATION FROM CLAUDE_BI.INFORMATION_SCHEMA.STREAMLITS 
                       WHERE NAME = '${appName}' AND SCHEMA_NAME = 'MCP')
      MAIN_FILE = (SELECT MAIN_FILE FROM CLAUDE_BI.INFORMATION_SCHEMA.STREAMLITS 
                   WHERE NAME = '${appName}' AND SCHEMA_NAME = 'MCP')
      QUERY_WAREHOUSE = 'CLAUDE_WAREHOUSE'
      COMMENT = 'Backup before rollback to ${targetVersion}';
    `;
    executeSql(backupSql, true);
    
    // Step 2: Update main app to target version
    console.log(`📝 Updating ${appName} to version ${targetVersion}...`);
    const updateSql = `
      CREATE OR REPLACE STREAMLIT CLAUDE_BI.MCP.${appName}
      ROOT_LOCATION = '@CLAUDE_BI.MCP.DASH_APPS/${dashboard}/${targetVersion}'
      MAIN_FILE = '${mainFile}'
      QUERY_WAREHOUSE = 'CLAUDE_WAREHOUSE'
      COMMENT = 'Rolled back to version: ${targetVersion} at ${new Date().toISOString()}';
    `;
    
    const result = executeSql(updateSql, true);
    
    // Extract URL ID
    const urlMatch = result.match(/([a-z0-9]{20})/);
    const urlId = urlMatch ? urlMatch[1] : 'unknown';
    
    console.log(`✅ Rollback successful!`);
    console.log(`📍 New URL: https://app.snowflake.com/uec18397/us-east-1/streamlit-apps/CLAUDE_BI/MCP/${appName}/${urlId}`);
    
    // Log rollback event
    console.log('📝 Logging rollback event...');
    const eventSql = `
      INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS (PAYLOAD, _SOURCE_LANE, _RECV_AT)
      SELECT 
        OBJECT_CONSTRUCT(
          'event_id', UUID_STRING(),
          'action', 'dashboard.rollback.executed',
          'actor_id', '${process.env.GITHUB_ACTOR || process.env.USER || 'system'}',
          'object', OBJECT_CONSTRUCT(
            'type', 'dashboard',
            'id', '${dashboard}',
            'app_name', '${appName}'
          ),
          'attributes', OBJECT_CONSTRUCT(
            'target_version', '${targetVersion}',
            'url_id', '${urlId}',
            'reason', '${argMap.reason || 'Manual rollback'}',
            'backup_created', '${appName}_ROLLBACK_BACKUP'
          ),
          'occurred_at', CURRENT_TIMESTAMP()
        ),
        'ROLLBACK_EXECUTOR',
        CURRENT_TIMESTAMP();
    `;
    executeSql(eventSql, true);
    
    return true;
    
  } catch (error) {
    console.error(`❌ Rollback failed: ${error.message}`);
    
    // Try to restore from backup
    console.log('🔄 Attempting to restore from backup...');
    try {
      executeSql(`ALTER STREAMLIT CLAUDE_BI.MCP.${appName}_ROLLBACK_BACKUP RENAME TO ${appName};`, true);
      console.log('✅ Restored from backup');
    } catch (restoreError) {
      console.error('❌ Restore failed:', restoreError.message);
    }
    
    return false;
  }
}

/**
 * Main rollback function
 */
async function main() {
  console.log('🔄 Dashboard Rollback Tool');
  console.log('');
  
  const targetVersion = argMap.version;
  const dashboard = argMap.dashboard;
  const steps = parseInt(argMap.steps || '1');
  
  if (targetVersion) {
    // Rollback to specific version
    console.log(`📍 Target version: ${targetVersion}`);
    
    // Extract dashboard name from version if possible
    // Try to find which dashboards were deployed with this version
    const sql = `
      SELECT DISTINCT object:dashboards as dashboards
      FROM CLAUDE_BI.ACTIVITY.EVENTS
      WHERE action = 'dashboard.version.uploaded'
        AND object:id = '${targetVersion}'
      LIMIT 1;
    `;
    
    try {
      const result = executeSql(sql, true);
      const data = parseSqlResult(result);
      
      if (data.length > 0 && data[0].dashboards) {
        // Parse the dashboards array
        const dashboardsStr = data[0].dashboards.replace(/[\[\]"]/g, '');
        const dashboards = dashboardsStr.split(',').map(d => d.trim());
        
        console.log(`📦 Found dashboards for version: ${dashboards.join(', ')}`);
        
        for (const dash of dashboards) {
          await performRollback(dash, targetVersion);
        }
      } else {
        console.error('❌ No dashboards found for this version');
        console.log('💡 Try specifying --dashboard parameter');
        process.exit(1);
      }
    } catch (error) {
      console.error(`❌ Failed to find dashboards for version: ${error.message}`);
      process.exit(1);
    }
    
  } else if (dashboard) {
    // Rollback specific dashboard by N steps
    console.log(`📍 Dashboard: ${dashboard}`);
    console.log(`📍 Steps back: ${steps}`);
    
    const history = await getDeploymentHistory(dashboard, steps + 1);
    
    if (history.length <= steps) {
      console.error(`❌ Not enough history to rollback ${steps} steps`);
      process.exit(1);
    }
    
    const targetDeployment = history[steps];
    console.log(`\n📍 Rolling back to: ${targetDeployment.version}`);
    console.log(`   Deployed: ${targetDeployment.occurred_at}`);
    console.log(`   Environment: ${targetDeployment.environment}`);
    
    await performRollback(dashboard, targetDeployment.version);
    
  } else {
    console.error('❌ Must specify either --version or --dashboard');
    console.log('\nUsage:');
    console.log('  Rollback to specific version:');
    console.log('    node rollback-dashboard.js --version=v20240101_123456_abc123');
    console.log('');
    console.log('  Rollback dashboard by N steps:');
    console.log('    node rollback-dashboard.js --dashboard=coo_dashboard --steps=1');
    console.log('');
    console.log('  Rollback with reason:');
    console.log('    node rollback-dashboard.js --version=v123 --reason="Performance issues"');
    process.exit(1);
  }
  
  console.log('\n✅ Rollback process complete!');
}

// Run if executed directly
if (require.main === module) {
  main().catch(error => {
    console.error('❌ Rollback failed:', error.message);
    process.exit(1);
  });
}

module.exports = { main, performRollback, getDeploymentHistory };