#!/usr/bin/env node
/**
 * Log Deployment Event to Snowflake Activity Stream
 * 
 * Records deployment events in ACTIVITY.EVENTS for tracking and rollback
 * 
 * Usage: node log-deployment-event.js --action="dashboard.deployed" --version="v123" --dashboards="coo_dashboard"
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
 * Log deployment event
 */
async function logEvent() {
  const action = argMap.action || 'dashboard.deployment';
  const version = argMap.version || 'unknown';
  const dashboards = argMap.dashboards ? argMap.dashboards.split(' ') : [];
  const environment = argMap.environment || 'production';
  const actor = argMap.actor || process.env.GITHUB_ACTOR || process.env.USER || 'system';
  const commit = argMap.commit || process.env.GITHUB_SHA || 'local';
  const workflowRun = argMap.workflow_run || process.env.GITHUB_RUN_ID || 'local';
  
  console.log(`📝 Logging deployment event: ${action}`);
  
  // Build attributes object
  const attributes = {
    version,
    environment,
    commit_sha: commit,
    workflow_run: workflowRun,
    dashboards: dashboards.join(','),
    timestamp: new Date().toISOString()
  };
  
  // Add any additional attributes from command line
  Object.keys(argMap).forEach(key => {
    if (!['action', 'version', 'dashboards', 'environment', 'actor', 'commit', 'workflow_run'].includes(key)) {
      attributes[key] = argMap[key];
    }
  });
  
  // Build the SQL statement
  const sql = `
    INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS (PAYLOAD, _SOURCE_LANE, _RECV_AT)
    SELECT 
      OBJECT_CONSTRUCT(
        'event_id', UUID_STRING(),
        'action', '${action}',
        'actor_id', '${actor}',
        'object', OBJECT_CONSTRUCT(
          'type', 'dashboard_deployment',
          'id', '${version}',
          'dashboards', ARRAY_CONSTRUCT(${dashboards.map(d => `'${d}'`).join(', ')})
        ),
        'attributes', PARSE_JSON('${JSON.stringify(attributes).replace(/'/g, "''")}'),
        'occurred_at', CURRENT_TIMESTAMP()
      ),
      'DEPLOYMENT_TRACKER',
      CURRENT_TIMESTAMP();
  `;
  
  try {
    executeSql(sql, true);
    console.log(`✅ Event logged: ${action}`);
    console.log(`  Version: ${version}`);
    console.log(`  Dashboards: ${dashboards.join(', ')}`);
    console.log(`  Environment: ${environment}`);
    console.log(`  Actor: ${actor}`);
    
    // If this is a deployment, also log individual dashboard events
    if (action === 'dashboard.deployed' && dashboards.length > 0) {
      for (const dashboard of dashboards) {
        const dashboardSql = `
          INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS (PAYLOAD, _SOURCE_LANE, _RECV_AT)
          SELECT 
            OBJECT_CONSTRUCT(
              'event_id', UUID_STRING(),
              'action', 'dashboard.version.active',
              'actor_id', '${actor}',
              'object', OBJECT_CONSTRUCT(
                'type', 'dashboard',
                'id', '${dashboard}',
                'version', '${version}'
              ),
              'attributes', OBJECT_CONSTRUCT(
                'environment', '${environment}',
                'previous_version', (
                  SELECT attributes:version
                  FROM CLAUDE_BI.ACTIVITY.EVENTS
                  WHERE action = 'dashboard.version.active'
                    AND object:id = '${dashboard}'
                    AND attributes:environment = '${environment}'
                  ORDER BY occurred_at DESC
                  LIMIT 1
                ),
                'deployed_at', CURRENT_TIMESTAMP()
              ),
              'occurred_at', CURRENT_TIMESTAMP()
            ),
            'VERSION_TRACKER',
            CURRENT_TIMESTAMP();
        `;
        
        try {
          executeSql(dashboardSql, true);
          console.log(`  ✅ Version tracked for ${dashboard}`);
        } catch (error) {
          console.error(`  ⚠️ Failed to track version for ${dashboard}: ${error.message}`);
        }
      }
    }
    
    // If this is a rollback, update the active version
    if (action === 'dashboard.rolled_back' && version !== 'unknown') {
      const rollbackSql = `
        INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS (PAYLOAD, _SOURCE_LANE, _RECV_AT)
        SELECT 
          OBJECT_CONSTRUCT(
            'event_id', UUID_STRING(),
            'action', 'dashboard.rollback.completed',
            'actor_id', '${actor}',
            'object', OBJECT_CONSTRUCT(
              'type', 'rollback',
              'id', UUID_STRING(),
              'target_version', '${version}'
            ),
            'attributes', OBJECT_CONSTRUCT(
              'reason', '${argMap.reason || 'Manual rollback'}',
              'rolled_back_from', (
                SELECT attributes:version
                FROM CLAUDE_BI.ACTIVITY.EVENTS
                WHERE action = 'dashboard.version.active'
                ORDER BY occurred_at DESC
                LIMIT 1
              ),
              'workflow_run', '${workflowRun}'
            ),
            'occurred_at', CURRENT_TIMESTAMP()
          ),
          'ROLLBACK_TRACKER',
          CURRENT_TIMESTAMP();
      `;
      
      try {
        executeSql(rollbackSql, true);
        console.log(`✅ Rollback event logged`);
      } catch (error) {
        console.error(`⚠️ Failed to log rollback event: ${error.message}`);
      }
    }
    
  } catch (error) {
    console.error(`❌ Failed to log event: ${error.message}`);
    // Don't fail the deployment if event logging fails
    console.log('⚠️ Continuing despite event logging failure');
  }
}

// Run if executed directly
if (require.main === module) {
  logEvent().catch(error => {
    console.error('❌ Event logging failed:', error.message);
    // Exit with success even if logging fails (non-critical)
    process.exit(0);
  });
}

module.exports = { logEvent };