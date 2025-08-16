#!/usr/bin/env node
/**
 * SQL Deployer with Mode Selection
 * Deploys different SQL procedure variants based on mode
 */

const fs = require('fs');
const { execSync } = require('child_process');
const path = require('path');

// Available SQL modes and their purposes
const SQL_MODES = {
  'main': {
    file: 'dashboard-procs.sql',
    description: 'Production default with full error handling (11,562 bytes)',
    purpose: 'All 5 procedures with comprehensive error handling'
  },
  'demo': {
    file: 'dashboard-procs-simple.sql', 
    description: 'Simplified for quick demos/testing (3,859 bytes)',
    purpose: 'Minimal procedures for fast deployment and demos'
  },
  'variant': {
    file: 'dashboard-procs-variant.sql',
    description: 'Single VARIANT parameter pattern (7,625 bytes)', 
    purpose: 'For MCP integration testing with VARIANT parameters'
  },
  'hotfix': {
    file: 'dashboard-procs-fixed.sql',
    description: 'Bug fixes for specific issues (7,612 bytes)',
    purpose: 'Contains specific bug fixes - temporary until merged to main'
  },
  'working': {
    file: 'dashboard-procs-working.sql',
    description: 'Development version (6,499 bytes)',
    purpose: 'Work-in-progress procedures for development'
  },
  'final': {
    file: 'dashboard-procs-final.sql',
    description: 'Final tested version (7,981 bytes)',
    purpose: 'Finalized procedures after testing'
  }
};

function showModes() {
  console.log('📊 Available SQL Deployment Modes:\n');
  
  Object.entries(SQL_MODES).forEach(([mode, info]) => {
    const filePath = `scripts/sql/${info.file}`;
    const exists = fs.existsSync(filePath);
    const status = exists ? '✅' : '❌';
    
    console.log(`${mode.toUpperCase().padEnd(10)} ${status}`);
    console.log(`  File: ${info.file}`);
    console.log(`  Size: ${info.description}`);
    console.log(`  Purpose: ${info.purpose}`);
    console.log('');
  });
}

function deploySQL(mode, dryRun = false) {
  if (!SQL_MODES[mode]) {
    console.error(`❌ Unknown mode: ${mode}`);
    console.error(`Available modes: ${Object.keys(SQL_MODES).join(', ')}`);
    process.exit(1);
  }
  
  const modeInfo = SQL_MODES[mode];
  const sqlPath = `scripts/sql/${modeInfo.file}`;
  
  if (!fs.existsSync(sqlPath)) {
    console.error(`❌ SQL file not found: ${sqlPath}`);
    process.exit(1);
  }
  
  console.log(`🚀 Deploying SQL Mode: ${mode.toUpperCase()}`);
  console.log(`📁 File: ${sqlPath}`);
  console.log(`📝 Description: ${modeInfo.description}`);
  console.log(`🎯 Purpose: ${modeInfo.purpose}`);
  console.log('');
  
  if (dryRun) {
    console.log('🔍 DRY RUN - Would execute:');
    console.log(`~/bin/sf exec-file ${sqlPath}`);
    return;
  }
  
  try {
    const sfCli = process.env.SF_CLI || '~/bin/sf';
    const keyPath = process.env.SF_PK_PATH || './claude_code_rsa_key.p8';
    
    console.log(`🔧 Using SF CLI: ${sfCli}`);
    console.log(`🔐 Using Key: ${keyPath}`);
    console.log('');
    
    const cmd = `SF_PK_PATH=${keyPath} ${sfCli} exec-file ${sqlPath}`;
    console.log(`⚡ Executing: ${cmd}`);
    
    execSync(cmd, { 
      stdio: 'inherit',
      env: { ...process.env, SF_PK_PATH: keyPath }
    });
    
    console.log(`\n✅ SQL deployment completed successfully!`);
    console.log(`📊 Mode: ${mode.toUpperCase()}`);
    console.log(`📁 File: ${sqlPath}`);
    
  } catch (error) {
    console.error(`\n❌ SQL deployment failed:`);
    console.error(`Mode: ${mode}`);
    console.error(`File: ${sqlPath}`);
    console.error(`Error: ${error.message}`);
    process.exit(1);
  }
}

function main() {
  const args = process.argv.slice(2);
  let mode = 'main'; // default
  let dryRun = false;
  
  // Parse arguments
  for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    if (arg === '--mode' || arg === '-m') {
      mode = args[++i];
    } else if (arg === '--dry-run' || arg === '-d') {
      dryRun = true;
    } else if (arg === '--list' || arg === '-l') {
      showModes();
      return;
    } else if (arg === '--help' || arg === '-h') {
      showHelp();
      return;
    } else if (!arg.startsWith('-')) {
      mode = arg; // positional argument
    }
  }
  
  if (!mode) {
    showModes();
    return;
  }
  
  deploySQL(mode, dryRun);
}

function showHelp() {
  console.log(`
🛠️  SQL Deployer for Snowflake POC2

Usage:
  npm run sql:deploy                    # Deploy main mode
  npm run sql:deploy -- --mode demo    # Deploy demo mode  
  npm run sql:deploy -- demo           # Deploy demo mode (positional)
  npm run sql:deploy -- --dry-run      # Show what would be deployed
  npm run sql:deploy -- --list         # List available modes

Options:
  --mode, -m <mode>     SQL mode to deploy (default: main)
  --dry-run, -d         Show commands without executing
  --list, -l            List available modes
  --help, -h            Show this help

Environment Variables:
  SF_CLI                Path to SF CLI (default: ~/bin/sf)
  SF_PK_PATH           Path to RSA private key (default: ./claude_code_rsa_key.p8)

Examples:
  npm run sql:deploy -- --mode main
  npm run sql:deploy -- demo --dry-run
  npm run sql:deploy -- --list
`);
}

if (require.main === module) {
  main();
}

module.exports = { SQL_MODES, deploySQL, showModes };