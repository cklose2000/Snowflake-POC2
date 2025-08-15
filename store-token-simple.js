#!/usr/bin/env node

/**
 * Simple token storage for Sarah
 */

const keytar = require('./snowflake-mcp-client/node_modules/keytar');

async function storeToken() {
  const token = 'tk_nuf3ry2eqzx9e1mcky0jb_user_ck_dev_test1';
  const account = 'uec18397.us-east-1';
  const serviceName = `SnowflakeMCP:${account}`;
  
  console.log('🔐 Storing Sarah\'s token...');
  console.log(`📋 Token: ${token.substring(0, 20)}...`);
  console.log(`🏢 Account: ${account}`);
  
  try {
    await keytar.setPassword(serviceName, 'user_token', token);
    console.log('\n✅ Token stored successfully in OS keychain!');
    console.log('\nSarah can now run:');
    console.log('cd snowflake-mcp-client && node dist/cli.js query "Show me sales data"');
  } catch (error) {
    console.error('❌ Failed to store token:', error.message);
  }
}

storeToken();