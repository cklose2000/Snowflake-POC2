#!/usr/bin/env node
/**
 * Generate Dashboard URLs
 * 
 * Creates access URLs for deployed dashboards
 * 
 * Usage: node generate-urls.js --dashboards="coo_dashboard executive_dashboard"
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

const dashboards = argMap.dashboards ? argMap.dashboards.split(' ') : [];

// Ensure SF_PK_PATH is set
const keyPath = process.env.SF_PK_PATH || './claude_code_rsa_key.p8';

/**
 * Execute Snowflake SQL command
 */
function executeSql(sql, silent = false) {
  if (!fs.existsSync(keyPath)) {
    // If no key, just return placeholder
    return 'URL_ID_PLACEHOLDER';
  }
  
  const sfPath = path.join(process.env.HOME, 'bin', 'sf');
  const cmd = `SF_PK_PATH=${keyPath} ${sfPath} sql "${sql.replace(/"/g, '\\"').replace(/\n/g, ' ')}"`;
  
  try {
    const result = execSync(cmd, { encoding: 'utf8' });
    if (!silent) {
      console.log(result);
    }
    return result;
  } catch (error) {
    return 'URL_ID_ERROR';
  }
}

/**
 * Get URL ID for a dashboard
 */
function getUrlId(dashboard) {
  const appName = dashboard.toUpperCase().replace(/-/g, '_');
  
  const sql = `
    SELECT url_id
    FROM CLAUDE_BI.INFORMATION_SCHEMA.STREAMLITS
    WHERE name = '${appName}'
      AND schema_name = 'MCP'
      AND catalog_name = 'CLAUDE_BI'
    LIMIT 1;
  `;
  
  const result = executeSql(sql, true);
  
  // Try to extract 20-character URL ID
  const match = result.match(/([a-z0-9]{20})/);
  return match ? match[1] : 'URL_ID_NOT_FOUND';
}

/**
 * Generate URLs for dashboards
 */
function generateUrls() {
  const account = 'uec18397';
  const region = 'us-east-1';
  const database = 'CLAUDE_BI';
  const schema = 'MCP';
  
  const urls = [];
  
  for (const dashboard of dashboards) {
    const appName = dashboard.toUpperCase().replace(/-/g, '_');
    const urlId = getUrlId(dashboard);
    
    const url = {
      dashboard: dashboard,
      appName: appName,
      urlId: urlId,
      directUrl: `https://app.snowflake.com/${account}/${region}/streamlit-apps/${database}/${schema}/${appName}/${urlId}`,
      snowsightUrl: `https://app.snowflake.com/#/streamlit-apps/${database}.${schema}.${appName}`,
      navigationPath: `Snowsight → Projects → Streamlit → ${appName}`
    };
    
    urls.push(url);
  }
  
  // Output in markdown format
  if (process.stdout.isTTY) {
    // Interactive mode - pretty output
    console.log('\n📍 Dashboard Access URLs\n');
    
    urls.forEach(url => {
      console.log(`### ${url.dashboard}`);
      console.log(`- **App Name:** ${url.appName}`);
      console.log(`- **Direct URL:** ${url.directUrl}`);
      console.log(`- **Snowsight:** ${url.snowsightUrl}`);
      console.log(`- **Navigation:** ${url.navigationPath}`);
      console.log('');
    });
  } else {
    // Pipe mode - markdown table
    urls.forEach(url => {
      console.log(`- **${url.dashboard}:** [Direct Link](${url.directUrl}) | ${url.navigationPath}`);
    });
  }
  
  return urls;
}

// Run if executed directly
if (require.main === module) {
  if (dashboards.length === 0) {
    console.error('❌ No dashboards specified');
    console.log('Usage: node generate-urls.js --dashboards="dashboard1 dashboard2"');
    process.exit(1);
  }
  
  generateUrls();
}

module.exports = { generateUrls, getUrlId };