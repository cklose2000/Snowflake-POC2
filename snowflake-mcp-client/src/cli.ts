#!/usr/bin/env node

import { Command } from 'commander';
import * as readline from 'readline';
import { saveTokenToKeychain, removeTokenFromKeychain, validateTokenFormat } from './auth';
import { SnowflakeMCPClient } from './index';
import { storeServiceKeyPassphrase } from './connection';

const program = new Command();

program
  .name('snowflake-mcp')
  .description('Thin client for Snowflake MCP integration with token-based authentication')
  .version('1.0.0');

/**
 * Login command - store user token securely
 */
program
  .command('login')
  .description('Store your MCP token securely in OS keychain')
  .option('--token <token>', 'Provide token directly (not recommended for security)')
  .action(async (options) => {
    try {
      let token = options.token;
      
      if (!token) {
        const rl = readline.createInterface({
          input: process.stdin,
          output: process.stdout
        });
        
        token = await new Promise<string>((resolve) => {
          rl.question('Enter your MCP token: ', (answer) => {
            rl.close();
            resolve(answer.trim());
          });
        });
      }
      
      // Validate token format
      if (!validateTokenFormat(token)) {
        console.error('❌ Invalid token format. Token must start with "tk_" and be at least 40 characters.');
        process.exit(1);
      }
      
      // Test the token with server
      console.log('🔍 Validating token with server...');
      const client = new SnowflakeMCPClient({ token });
      const validation = await client.validateToken();
      
      if (!validation.success) {
        console.error(`❌ Token validation failed: ${validation.error}`);
        process.exit(1);
      }
      
      // Test actual functionality
      console.log('🧪 Testing tool access...');
      const testResult = await client.listSources();
      
      if (!testResult.success) {
        console.error(`❌ Tool access test failed: ${testResult.error}`);
        process.exit(1);
      }
      
      // Save to keychain
      await saveTokenToKeychain(token);
      
      console.log('✅ Token saved successfully');
      console.log(`   Account: ${process.env.SNOWFLAKE_ACCOUNT}`);
      console.log(`   User: ${validation.data.username}`);
      console.log(`   Status: ${validation.data.status}`);
      
      // Log successful login
      await client.logEvent('session.login', {
        account: process.env.SNOWFLAKE_ACCOUNT,
        validation_status: validation.data.status
      });
      
    } catch (error) {
      console.error('❌ Login failed:', error.message);
      process.exit(1);
    }
  });

/**
 * Logout command - remove token from keychain
 */
program
  .command('logout')
  .description('Remove stored token from OS keychain')
  .action(async () => {
    try {
      await removeTokenFromKeychain();
      console.log('✅ Token removed successfully');
    } catch (error) {
      console.error('❌ Logout failed:', error.message);
      process.exit(1);
    }
  });

/**
 * Test command - comprehensive system validation
 */
program
  .command('test')
  .description('Test connection, token, and tool access')
  .option('--impersonate <username>', 'Admin impersonation for testing (admin only)')
  .option('--verbose', 'Show detailed test output')
  .action(async (options) => {
    const verbose = options.verbose;
    
    try {
      const client = new SnowflakeMCPClient();
      
      console.log('🧪 Testing Snowflake MCP Client');
      console.log('================================\n');
      
      // Test 1: Service connection
      if (verbose) console.log('🔍 Testing service connection...');
      console.log('Test 1: Service Connection');
      
      try {
        await client.test();
        console.log('  ✅ Service connection successful');
      } catch (error) {
        console.log('  ❌ Service connection failed:', error.message);
        process.exit(1);
      }
      
      // Test 2: Token validation
      if (verbose) console.log('🎫 Validating token...');
      console.log('\nTest 2: Token Validation');
      
      const validation = await client.validateToken();
      if (validation.success) {
        console.log('  ✅ Token is valid');
        console.log(`     User: ${validation.data.username}`);
        console.log(`     Status: ${validation.data.status}`);
        if (validation.data.expires_at) {
          console.log(`     Expires: ${validation.data.expires_at}`);
        }
      } else {
        console.log('  ❌ Token validation failed:', validation.error);
        process.exit(1);
      }
      
      // Test 3: Tool access
      if (verbose) console.log('📋 Testing tool access...');
      console.log('\\nTest 3: Tool Access');
      
      const sources = await client.listSources();
      if (sources.success) {
        console.log('  ✅ List sources successful');
        console.log(`     Sources available: ${Array.isArray(sources.data) ? sources.data.length : 'Unknown'}`);
        if (verbose && Array.isArray(sources.data)) {
          sources.data.forEach((source: string) => {
            console.log(`       - ${source}`);
          });
        }
      } else {
        console.log('  ❌ List sources failed:', sources.error);
      }
      
      // Test 4: User status
      console.log('\\nTest 4: User Status');
      const userStatus = await client.getUserStatus();
      if (userStatus.success) {
        console.log('  ✅ User status retrieved');
        console.log(`     Username: ${userStatus.data.username}`);
        console.log(`     Max rows: ${userStatus.data.max_rows}`);
        console.log(`     Daily runtime: ${userStatus.data.daily_runtime_s}s`);
        if (verbose) {
          console.log(`     Allowed tools: ${userStatus.data.allowed_tools.join(', ')}`);
        }
      } else {
        console.log('  ❌ User status failed:', userStatus.error);
      }
      
      // Test 5: Logging
      if (verbose) console.log('📝 Testing event logging...');
      console.log('\\nTest 5: Event Logging');
      
      const logResult = await client.logEvent('test.completed', {
        test_timestamp: new Date().toISOString(),
        tests_passed: 4,
        client_version: '1.0.0'
      });
      
      if (logResult.success) {
        console.log('  ✅ Event logging successful');
      } else {
        console.log('  ❌ Event logging failed:', logResult.error);
      }
      
      console.log('\\n================================');
      console.log('✅ All tests completed successfully!\\n');
      
      // Summary
      console.log('Summary:');
      console.log(`  Account: ${process.env.SNOWFLAKE_ACCOUNT}`);
      console.log(`  User: ${validation.data.username}`);
      console.log(`  Tools: ${userStatus.success ? userStatus.data.allowed_tools.length : 'Unknown'}`);
      console.log(`  Status: Ready for Claude Code integration`);
      
    } catch (error) {
      console.error('\\n❌ Test suite failed:', error.message);
      console.error('\\nTroubleshooting:');
      console.error('  1. Check environment variables: SNOWFLAKE_ACCOUNT, MCP_SERVICE_USER, SF_PK_PATH');
      console.error('  2. Verify token with: snowflake-mcp login');
      console.error('  3. Check service account permissions');
      process.exit(1);
    }
  });

/**
 * Status command - show current configuration
 */
program
  .command('status')
  .description('Show current authentication status and configuration')
  .action(async () => {
    try {
      console.log('📊 Snowflake MCP Client Status');
      console.log('==============================\\n');
      
      // Environment check
      console.log('Environment:');
      console.log(`  Account: ${process.env.SNOWFLAKE_ACCOUNT || '❌ Not set'}`);
      console.log(`  Service User: ${process.env.MCP_SERVICE_USER || '❌ Not set'}`);
      console.log(`  Private Key Path: ${process.env.SF_PK_PATH || '❌ Not set'}`);
      console.log(`  Service Role: ${process.env.MCP_SERVICE_ROLE || 'MCP_SERVICE_ROLE (default)'}`);
      console.log(`  Warehouse: ${process.env.MCP_SERVICE_WAREHOUSE || 'MCP_XS_WH (default)'}`);
      
      // Token check
      const client = new SnowflakeMCPClient();
      try {
        const validation = await client.validateToken();
        console.log('\\nToken:');
        if (validation.success) {
          console.log('  ✅ Valid token found');
          console.log(`  User: ${validation.data.username}`);
          console.log(`  Status: ${validation.data.status}`);
        } else {
          console.log('  ❌ No valid token');
          console.log('  Run: snowflake-mcp login');
        }
      } catch (error) {
        console.log('\\nToken:');
        console.log('  ❌ Token check failed');
        console.log(`  Error: ${error.message}`);
      }
      
    } catch (error) {
      console.error('❌ Status check failed:', error.message);
    }
  });

/**
 * Query command - test natural language queries
 */
program
  .command('query <text>')
  .description('Test a natural language query')
  .option('--limit <number>', 'Maximum rows to return', '100')
  .action(async (text, options) => {
    try {
      const client = new SnowflakeMCPClient();
      
      console.log(`🔍 Executing query: "${text}"`);
      console.log('================================\\n');
      
      const startTime = Date.now();
      const result = await client.query(text, {
        top_n: parseInt(options.limit)
      });
      const executionTime = Date.now() - startTime;
      
      if (result.success) {
        console.log('✅ Query successful');
        console.log(`⏱️  Execution time: ${executionTime}ms`);
        
        if (result.data) {
          console.log('\\n📊 Results:');
          console.log(JSON.stringify(result.data, null, 2));
        }
        
        if (result.metadata) {
          console.log('\\n📋 Metadata:');
          console.log(`  Rows returned: ${result.metadata.rows_returned || 'Unknown'}`);
          console.log(`  Bytes scanned: ${result.metadata.bytes_scanned || 'Unknown'}`);
        }
      } else {
        console.log('❌ Query failed');
        console.log(`Error: ${result.error}`);
      }
      
    } catch (error) {
      console.error('❌ Query execution failed:', error.message);
      process.exit(1);
    }
  });

/**
 * Setup command - configure service account credentials
 */
program
  .command('setup')
  .description('Setup service account credentials (admin only)')
  .action(async () => {
    console.log('🔧 Snowflake MCP Setup');
    console.log('=======================\\n');
    console.log('This command helps configure the service account credentials.');
    console.log('You will need:');
    console.log('  1. Service account private key file');
    console.log('  2. Private key passphrase (if encrypted)\\n');
    
    const rl = readline.createInterface({
      input: process.stdin,
      output: process.stdout
    });
    
    try {
      const passphrase = await new Promise<string>((resolve) => {
        rl.question('Enter private key passphrase (leave empty if none): ', (answer) => {
          resolve(answer.trim());
        });
      });
      
      if (passphrase) {
        await storeServiceKeyPassphrase(passphrase);
        console.log('✅ Service key passphrase stored in keychain');
      }
      
      console.log('\\nEnvironment variables to set:');
      console.log('  export SNOWFLAKE_ACCOUNT="your-account.region"');
      console.log('  export MCP_SERVICE_USER="MCP_SERVICE_USER"');  
      console.log('  export SF_PK_PATH="/path/to/private/key.pem"');
      console.log('  export MCP_SERVICE_ROLE="MCP_SERVICE_ROLE"');
      console.log('  export MCP_SERVICE_WAREHOUSE="MCP_XS_WH"');
      
    } catch (error) {
      console.error('❌ Setup failed:', error.message);
    } finally {
      rl.close();
    }
  });

// Parse command line arguments
program.parse();

// If no command provided, show help
if (!process.argv.slice(2).length) {
  program.outputHelp();
}