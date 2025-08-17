#!/usr/bin/env node
/**
 * Unified CLI Dispatcher for Snowflake POC2
 * Single entry point for all deploy, test, and check operations
 * 
 * Usage: node scripts/deploy/cli.js <command> [args...]
 * Or via npm: npm run <command>
 */

const { spawnSync } = require('node:child_process');
const path = require('node:path');
const fs = require('node:fs');

// Command registry - maps CLI commands to actual script files
const commands = {
  // Authentication deployment variants
  'deploy:auth:password': './scripts/deploy/deploy-auth-password.js',
  'deploy:auth:keypair':  './scripts/deploy/deploy-auth-keypair.js',
  'deploy:auth:unified':  './scripts/deploy/deploy-auth.js',
  
  // Core deployment operations
  'deploy:native':        './scripts/deploy/deploy-native-procedures.js',
  'deploy:enhanced':      './scripts/deploy/deploy-enhanced-procedures.js',
  'deploy:logging':       './scripts/deploy/deploy-logging.js',
  'deploy:activation':    './scripts/deploy/deploy-activation-system.js',
  
  // Testing operations
  'test:integration':     './tests/integration/test-dashboard.js',
  'test:auth':           './tests/scripts/test-auth-system.js',
  'test:mcp':            './tests/scripts/test-mcp-integration.js',
  'test:connection':     './tests/scripts/test-key-connection.js',
  'test:logging':        './tests/scripts/test-logging-check.js',
  
  // Security and validation checks
  'check:guards':        './scripts/checks/repo-guards.js',
  'check:events':        './scripts/checks/check-events.js',
  'check:privileges':    './scripts/checks/check-privileges.js',
  'check:tables':        './scripts/checks/check-table-structure.js',
  'verify:logging':      './scripts/checks/verify-logging.js',
  
  // Utilities
  'sql:deploy':          './scripts/deploy/sql-deployer.js',
  'app:put':             './scripts/deploy/streamlit-put.js',
  'app:create':          './scripts/deploy/streamlit-create.js',
  
  // Dashboard SDLC
  'dashboard:upload':    './scripts/deploy/upload-dashboard-version.js',
  'dashboard:deploy':    './scripts/deploy/blue-green-swap.js',
  'dashboard:rollback':  './scripts/deploy/rollback-dashboard.js',
  'dashboard:verify':    './scripts/deploy/verify-dashboard.js',
  'dashboard:urls':      './scripts/deploy/generate-urls.js'
};

// Helper commands that show useful information
const metaCommands = {
  'help': showHelp,
  'list': listCommands,
  'status': showStatus
};

function showHelp() {
  console.log(`
🚀 Snowflake POC2 Unified CLI

Usage:
  npm run <command>                 # Via npm scripts
  node scripts/deploy/cli.js <cmd>  # Direct invocation

Categories:

📦 DEPLOYMENT
  deploy:auth:password    - Deploy with password authentication
  deploy:auth:keypair     - Deploy with key-pair authentication  
  deploy:auth:unified     - Deploy with unified auth system
  deploy:native           - Deploy native procedures
  deploy:enhanced         - Deploy enhanced procedures
  deploy:logging          - Deploy logging infrastructure
  deploy:activation       - Deploy activation system

🧪 TESTING  
  test:integration        - Run integration tests
  test:auth              - Test authentication systems
  test:mcp               - Test MCP integration
  test:connection        - Test Snowflake connection
  test:logging           - Test logging functionality

🛡️  SECURITY & VALIDATION
  check:guards           - Run all security guards (Two-Table Law, secrets, etc.)
  check:events           - Check event logging
  check:privileges       - Check user privileges
  check:tables           - Check table structure
  verify:logging         - Verify logging system

🔧 UTILITIES
  sql:deploy             - Deploy SQL with mode selection
  app:put               - Put Streamlit app to stage
  app:create            - Create Streamlit application

📊 DASHBOARD SDLC
  dashboard:upload       - Upload versioned dashboard to stage
  dashboard:deploy       - Blue-green dashboard deployment
  dashboard:rollback     - Rollback to previous version
  dashboard:verify       - Verify dashboard deployment
  dashboard:urls         - Generate dashboard access URLs
  
Examples:
  npm run deploy:auth:keypair
  npm run test:integration
  npm run check:guards
  npm run sql:deploy -- --mode=main
`);
}

function listCommands() {
  console.log('Available commands:');
  Object.keys(commands).sort().forEach(cmd => {
    const script = commands[cmd];
    const exists = fs.existsSync(script);
    console.log(`  ${cmd.padEnd(25)} → ${script} ${exists ? '✅' : '❌'}`);
  });
}

function showStatus() {
  console.log('🔍 System Status Check...\n');
  
  // Check if we're in the right directory
  const isCorrectDir = fs.existsSync('package.json') && fs.existsSync('scripts/deploy');
  console.log(`📁 Working Directory: ${isCorrectDir ? '✅' : '❌'} ${process.cwd()}`);
  
  // Check RSA keys
  const keyPath = path.join(process.env.HOME, '.snowflake-keys/claude_code_rsa_key.p8');
  const hasKeys = fs.existsSync('claude_code_rsa_key.p8') && fs.existsSync(keyPath);
  console.log(`🔐 RSA Keys: ${hasKeys ? '✅' : '❌'} ${hasKeys ? 'Secured with symlinks' : 'Missing or not secured'}`);
  
  // Check script availability
  const availableScripts = Object.keys(commands).filter(cmd => fs.existsSync(commands[cmd]));
  console.log(`📜 Available Scripts: ${availableScripts.length}/${Object.keys(commands).length}`);
  
  // Check Two-Table Law (requires SF connection)
  console.log(`\n💡 To check Two-Table Law compliance: npm run check:guards`);
  console.log(`💡 To test connection: npm run test:connection`);
}

function expandPath(scriptPath) {
  // Handle home directory expansion
  if (scriptPath.startsWith('~/')) {
    return path.join(process.env.HOME, scriptPath.slice(2));
  }
  return scriptPath;
}

function main() {
  const cmd = process.argv[2];
  const args = process.argv.slice(3);
  
  if (!cmd) {
    console.error('❌ No command provided\n');
    showHelp();
    process.exit(1);
  }
  
  // Handle meta commands
  if (metaCommands[cmd]) {
    metaCommands[cmd]();
    return;
  }
  
  // Handle regular commands
  if (!commands[cmd]) {
    console.error(`❌ Unknown command: ${cmd}\n`);
    console.error('Available commands:');
    Object.keys(commands).sort().forEach(c => console.error(`  ${c}`));
    console.error('\nRun "npm run help" for detailed usage.');
    process.exit(2);
  }
  
  const scriptPath = expandPath(commands[cmd]);
  
  if (!fs.existsSync(scriptPath)) {
    console.error(`❌ Script not found: ${scriptPath}`);
    console.error(`Command: ${cmd}`);
    process.exit(3);
  }
  
  console.log(`🚀 Running: ${cmd}`);
  console.log(`📜 Script: ${scriptPath}`);
  if (args.length > 0) {
    console.log(`📝 Args: ${args.join(' ')}`);
  }
  console.log('');
  
  // Execute the script
  const result = spawnSync('node', [scriptPath, ...args], { 
    stdio: 'inherit',
    cwd: process.cwd()
  });
  
  // Report results
  if (result.error) {
    console.error(`❌ Execution error: ${result.error.message}`);
    process.exit(4);
  }
  
  const exitCode = result.status ?? 1;
  if (exitCode === 0) {
    console.log(`\n✅ Command completed successfully: ${cmd}`);
  } else {
    console.error(`\n❌ Command failed with exit code ${exitCode}: ${cmd}`);
  }
  
  process.exit(exitCode);
}

if (require.main === module) {
  main();
}

module.exports = { commands, main };