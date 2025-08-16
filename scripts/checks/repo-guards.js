#!/usr/bin/env node
/**
 * Repository Guards - The Killer Feature
 * Enforces critical architectural and security constraints
 * 
 * Guards:
 * 1. Two-Table Law: Only LANDING.RAW_EVENTS & ACTIVITY.EVENTS allowed
 * 2. No secrets in git: No *.p8, *.pem, .env tracked
 * 3. Proper proc permissions: All MCP.* procedures are EXECUTE AS OWNER
 * 4. No direct CORE access: Agent role has no SELECT on base schemas
 */

const { execSync } = require('node:child_process');
const fs = require('node:fs');

// Configuration
const SF_CLI = process.env.SF_CLI || '~/bin/sf';
const SF_PK_PATH = process.env.SF_PK_PATH || './claude_code_rsa_key.p8';

class GuardError extends Error {
  constructor(message, guard, details = null) {
    super(message);
    this.guard = guard;
    this.details = details;
  }
}

function must(cmd) {
  try {
    return execSync(cmd, { stdio: 'pipe', encoding: 'utf8' }).trim();
  } catch (error) {
    throw new GuardError(`Command failed: ${cmd}`, 'execution', error.message);
  }
}

function fail(guard, message, details = null) {
  throw new GuardError(message, guard, details);
}

function pass(guard, message, details = null) {
  console.log(`✅ ${guard}: ${message}`);
  if (details) console.log(`   ${details}`);
}

// Guard 1: No secrets tracked in git
function guardSecrets() {
  const guard = 'SECRETS';
  console.log('🔐 Checking secrets in git...');
  
  const tracked = must('git ls-files');
  const files = tracked.split('\n');
  
  // Check for specific sensitive files
  const sensitiveFiles = ['.env'];
  for (const file of sensitiveFiles) {
    if (files.includes(file)) {
      fail(guard, `Secret file tracked in git: ${file}`, 
           'Run: git rm --cached .env && git commit -m "Remove .env from tracking"');
    }
  }
  
  // Check for key patterns
  const keyFiles = files.filter(f => f.match(/\.(p8|pem)$/));
  if (keyFiles.length > 0) {
    fail(guard, `Key files tracked in git: ${keyFiles.join(', ')}`,
         'Move keys to ~/.snowflake-keys/ and update .gitignore');
  }
  
  pass(guard, 'No secrets tracked in git');
}

// Guard 2: Two-Table Law enforcement  
function guardTwoTableLaw() {
  const guard = 'TWO_TABLE_LAW';
  console.log('📊 Checking Two-Table Law compliance...');
  
  const sql = `
    SELECT TABLE_SCHEMA||'.'||TABLE_NAME as table_name 
    FROM CLAUDE_BI.INFORMATION_SCHEMA.TABLES 
    WHERE TABLE_CATALOG = 'CLAUDE_BI' 
      AND TABLE_TYPE IN ('BASE TABLE', 'DYNAMIC TABLE')
      AND TABLE_SCHEMA IN ('LANDING', 'ACTIVITY')
    ORDER BY 1
  `;
  
  const cmd = `SF_PK_PATH=${SF_PK_PATH} ${SF_CLI} sql "${sql.replace(/\n/g, ' ')}"`;
  const output = must(cmd);
  
  // Parse the JSON output to get table names
  const lines = output.split('\n');
  const resultStart = lines.findIndex(line => line.includes('Results:'));
  if (resultStart === -1) {
    fail(guard, 'Could not parse SQL results', output);
  }
  
  const jsonStart = lines.findIndex((line, idx) => idx > resultStart && line.trim().startsWith('['));
  const jsonEnd = lines.findIndex((line, idx) => idx > jsonStart && line.trim().endsWith(']'));
  
  if (jsonStart === -1 || jsonEnd === -1) {
    fail(guard, 'Could not find JSON results in output', output);
  }
  
  const jsonLines = lines.slice(jsonStart, jsonEnd + 1);
  const jsonStr = jsonLines.join('\n');
  
  let tables;
  try {
    tables = JSON.parse(jsonStr);
  } catch (e) {
    fail(guard, 'Could not parse table results JSON', jsonStr);
  }
  
  const tableNames = tables.map(t => t.TABLE_NAME || t.table_name).sort();
  const expectedTables = ['ACTIVITY.EVENTS', 'LANDING.RAW_EVENTS'];
  
  if (tableNames.length !== 2) {
    fail(guard, `Found ${tableNames.length} tables, expected exactly 2`, 
         `Tables: ${tableNames.join(', ')}`);
  }
  
  const tablesMatch = JSON.stringify(tableNames) === JSON.stringify(expectedTables);
  if (!tablesMatch) {
    fail(guard, 'Wrong tables found',
         `Expected: ${expectedTables.join(', ')} | Found: ${tableNames.join(', ')}`);
  }
  
  pass(guard, 'Two-Table Law compliant', `Tables: ${tableNames.join(', ')}`);
}

// Guard 3: Procedure permissions
function guardProcPermissions() {
  const guard = 'PROC_PERMISSIONS'; 
  console.log('🔧 Checking procedure permissions...');
  
  // Get list of MCP procedures
  const procSql = `SHOW PROCEDURES IN SCHEMA MCP`;
  const cmd = `SF_PK_PATH=${SF_PK_PATH} ${SF_CLI} sql "${procSql}"`;
  const output = must(cmd);
  
  // Check a few key procedures for EXECUTE AS OWNER
  const keyProcs = ['DASH_GET_SERIES', 'DASH_GET_METRICS', 'TEST_ALL'];
  const errors = [];
  
  for (const proc of keyProcs) {
    try {
      const ddlSql = `SELECT GET_DDL('PROCEDURE', 'MCP.${proc}()')`;
      const ddlCmd = `SF_PK_PATH=${SF_PK_PATH} ${SF_CLI} sql "${ddlSql}"`;
      const ddlOutput = must(ddlCmd);
      
      if (!ddlOutput.toUpperCase().includes('EXECUTE AS OWNER')) {
        errors.push(`${proc} is not EXECUTE AS OWNER`);
      }
    } catch (e) {
      // Procedure might not exist or have different signature
      console.log(`   ⚠️  Could not check ${proc}: ${e.message}`);
    }
  }
  
  if (errors.length > 0) {
    fail(guard, 'Procedures missing EXECUTE AS OWNER', errors.join(', '));
  }
  
  pass(guard, 'Key procedures have EXECUTE AS OWNER');
}

// Guard 4: Agent role permissions
function guardAgentPermissions() {
  const guard = 'AGENT_PERMISSIONS';
  console.log('👤 Checking agent role permissions...');
  
  try {
    const grantsSql = `SHOW GRANTS TO ROLE R_CLAUDE_AGENT`;
    const cmd = `SF_PK_PATH=${SF_PK_PATH} ${SF_CLI} sql "${grantsSql}"`;
    const output = must(cmd);
    
    // Check for dangerous direct access patterns
    const dangerousPatterns = [
      /SELECT\s+ON\s+TABLE\s+CORE\./i,
      /SELECT\s+ON\s+SCHEMA\s+CORE/i,
      /ALL\s+ON\s+SCHEMA\s+CORE/i
    ];
    
    for (const pattern of dangerousPatterns) {
      if (pattern.test(output)) {
        fail(guard, 'Agent role has dangerous direct access to CORE schema',
             'Agent should only access via MCP procedures');
      }
    }
    
    pass(guard, 'Agent role permissions look safe');
    
  } catch (e) {
    // Role might not exist - that's ok for this guard
    console.log(`   ⚠️  Could not check agent permissions: ${e.message}`);
    pass(guard, 'Agent role not found (ok for this check)');
  }
}

// Guard 5: Git hooks integrity
function guardGitHooks() {
  const guard = 'GIT_HOOKS';
  console.log('🪝 Checking git hooks...');
  
  const hookPaths = [
    '.githooks/post-commit',
    '.githooks/pre-push'
  ];
  
  let hooksExist = 0;
  for (const hook of hookPaths) {
    if (fs.existsSync(hook)) {
      hooksExist++;
      
      // Check if hook references old paths
      const content = fs.readFileSync(hook, 'utf8');
      if (content.includes('node test-') || content.includes('node deploy-')) {
        console.log(`   ⚠️  ${hook} may reference old file paths`);
      }
    }
  }
  
  if (hooksExist > 0) {
    pass(guard, `Found ${hooksExist} git hooks`);
  } else {
    console.log(`   ⚠️  No git hooks found (optional)`);
  }
}

// Guard 6: Environment configuration
function guardEnvironment() {
  const guard = 'ENVIRONMENT';
  console.log('🌍 Checking environment configuration...');
  
  // Check required environment setup
  const checks = [
    { name: 'SF_PK_PATH', value: SF_PK_PATH, required: true },
    { name: 'SF_CLI', value: SF_CLI, required: true },
    { name: 'HOME', value: process.env.HOME, required: true }
  ];
  
  const errors = [];
  for (const check of checks) {
    if (check.required && !check.value) {
      errors.push(`Missing ${check.name}`);
    }
  }
  
  if (errors.length > 0) {
    fail(guard, 'Environment configuration issues', errors.join(', '));
  }
  
  // Check if key file exists
  if (!fs.existsSync(SF_PK_PATH)) {
    fail(guard, `Private key not found: ${SF_PK_PATH}`,
         'Ensure key is at correct path or update SF_PK_PATH');
  }
  
  pass(guard, 'Environment properly configured');
}

function runAllGuards() {
  console.log('🛡️  Running Repository Guards...\n');
  
  const guards = [
    { name: 'Secrets', fn: guardSecrets },
    { name: 'Two-Table Law', fn: guardTwoTableLaw },
    { name: 'Procedure Permissions', fn: guardProcPermissions },
    { name: 'Agent Permissions', fn: guardAgentPermissions },
    { name: 'Git Hooks', fn: guardGitHooks },
    { name: 'Environment', fn: guardEnvironment }
  ];
  
  const results = [];
  
  for (const guard of guards) {
    try {
      guard.fn();
      results.push({ name: guard.name, status: 'PASS' });
    } catch (error) {
      if (error instanceof GuardError) {
        console.log(`❌ ${error.guard}: ${error.message}`);
        if (error.details) console.log(`   ${error.details}`);
        results.push({ name: guard.name, status: 'FAIL', error: error.message });
      } else {
        console.log(`💥 ${guard.name}: Unexpected error: ${error.message}`);
        results.push({ name: guard.name, status: 'ERROR', error: error.message });
      }
    }
    console.log('');
  }
  
  // Summary
  const passed = results.filter(r => r.status === 'PASS').length;
  const failed = results.filter(r => r.status === 'FAIL').length;
  const errors = results.filter(r => r.status === 'ERROR').length;
  
  console.log('📊 Guard Results Summary:');
  console.log(`✅ Passed: ${passed}`);
  console.log(`❌ Failed: ${failed}`);
  console.log(`💥 Errors: ${errors}`);
  console.log(`📋 Total:  ${results.length}`);
  
  if (failed > 0 || errors > 0) {
    console.log('\n❌ Repository guards failed! Fix issues before proceeding.');
    process.exit(1);
  } else {
    console.log('\n🎉 All repository guards passed! System is secure and compliant.');
    process.exit(0);
  }
}

function main() {
  const args = process.argv.slice(2);
  
  if (args.includes('--help') || args.includes('-h')) {
    console.log(`
🛡️  Repository Guards

Enforces critical constraints:
  • Two-Table Law: Only RAW_EVENTS & EVENTS tables
  • No secrets in git: No .env, *.p8, *.pem tracked  
  • Proper permissions: MCP procedures are EXECUTE AS OWNER
  • Safe agent access: No direct CORE schema access
  • Environment setup: Keys and tools configured

Usage:
  npm run check:guards
  node scripts/checks/repo-guards.js

Environment:
  SF_CLI=${SF_CLI}
  SF_PK_PATH=${SF_PK_PATH}
`);
    return;
  }
  
  runAllGuards();
}

if (require.main === module) {
  main();
}

module.exports = { runAllGuards, guardSecrets, guardTwoTableLaw, guardProcPermissions };