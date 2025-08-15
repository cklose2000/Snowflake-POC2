#!/usr/bin/env node

/**
 * Claude Code Authentication Helper
 * 
 * Securely stores MCP tokens in the OS keychain
 * Never stores tokens in files or environment variables
 */

const keytar = require('keytar');
const { program } = require('commander');
const readline = require('readline');
const crypto = require('crypto');

const SERVICE_NAME = 'claude-code-mcp';
const ACCOUNT_NAME = 'snowflake-token';

/**
 * Securely prompt for token (hidden input)
 */
async function promptForToken() {
  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
    terminal: true
  });

  return new Promise((resolve) => {
    rl.question('Paste your MCP token (input hidden): ', (token) => {
      rl.close();
      resolve(token);
    });

    // Hide input
    rl._writeToOutput = function _writeToOutput(stringToWrite) {
      if (rl.line.length === 0) {
        rl.output.write(stringToWrite);
      } else {
        rl.output.write('*');
      }
    };
  });
}

/**
 * Validate token format
 */
function validateToken(token) {
  // Token format: tk_[32chars]_[hint]
  const tokenRegex = /^tk_[A-Za-z0-9_-]{32}_[a-z]{4}$/;
  
  if (!tokenRegex.test(token)) {
    throw new Error('Invalid token format. Expected: tk_xxxx...xxxx_hint');
  }
  
  return true;
}

/**
 * Extract token metadata for display
 */
function getTokenMetadata(token) {
  const parts = token.split('_');
  return {
    prefix: token.substring(0, 8),
    suffix: token.substring(token.length - 4),
    hint: parts[parts.length - 1]
  };
}

/**
 * Store token in OS keychain
 */
async function storeToken(token) {
  try {
    validateToken(token);
    
    // Store in OS keychain
    await keytar.setPassword(SERVICE_NAME, ACCOUNT_NAME, token);
    
    // Store metadata separately (non-sensitive)
    const metadata = getTokenMetadata(token);
    await keytar.setPassword(SERVICE_NAME, 'metadata', JSON.stringify(metadata));
    
    return metadata;
  } catch (error) {
    throw new Error(`Failed to store token: ${error.message}`);
  }
}

/**
 * Retrieve token from OS keychain
 */
async function getToken() {
  try {
    const token = await keytar.getPassword(SERVICE_NAME, ACCOUNT_NAME);
    if (!token) {
      throw new Error('No token found. Run "claude-code pair" first.');
    }
    return token;
  } catch (error) {
    throw new Error(`Failed to retrieve token: ${error.message}`);
  }
}

/**
 * Remove token from OS keychain
 */
async function removeToken() {
  try {
    const deleted = await keytar.deletePassword(SERVICE_NAME, ACCOUNT_NAME);
    await keytar.deletePassword(SERVICE_NAME, 'metadata');
    return deleted;
  } catch (error) {
    throw new Error(`Failed to remove token: ${error.message}`);
  }
}

/**
 * Display current token status
 */
async function showStatus() {
  try {
    const metadataStr = await keytar.getPassword(SERVICE_NAME, 'metadata');
    
    if (!metadataStr) {
      console.log('❌ No Claude Code token configured');
      console.log('Run "claude-code pair" to set up authentication');
      return;
    }
    
    const metadata = JSON.parse(metadataStr);
    console.log('✅ Claude Code token configured');
    console.log(`Token: ${metadata.prefix}...${metadata.suffix}`);
    console.log(`User hint: ${metadata.hint}`);
    
    // Test token exists
    const token = await keytar.getPassword(SERVICE_NAME, ACCOUNT_NAME);
    if (token) {
      console.log('Status: Token stored securely in keychain');
    } else {
      console.log('Status: ⚠️  Metadata exists but token missing');
    }
  } catch (error) {
    console.error('Error checking status:', error.message);
  }
}

// CLI Commands
program
  .name('claude-code')
  .description('Claude Code authentication helper')
  .version('1.0.0');

program
  .command('pair')
  .description('Store MCP token in OS keychain')
  .option('-t, --token <token>', 'MCP token (or paste when prompted)')
  .action(async (options) => {
    try {
      let token = options.token;
      
      if (!token) {
        console.log('🔐 Claude Code Authentication Setup');
        console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
        console.log('You will need your MCP token from your administrator.');
        console.log('The token will be stored securely in your OS keychain.\n');
        
        token = await promptForToken();
        console.log(''); // New line after hidden input
      }
      
      if (!token || token.trim() === '') {
        console.error('❌ No token provided');
        process.exit(1);
      }
      
      const metadata = await storeToken(token.trim());
      
      console.log('✅ Token stored successfully!');
      console.log(`Token: ${metadata.prefix}...${metadata.suffix}`);
      console.log('\nClaude Code is now authenticated with Snowflake MCP.');
      console.log('The token is stored securely in your OS keychain.');
      
    } catch (error) {
      console.error('❌ Error:', error.message);
      process.exit(1);
    }
  });

program
  .command('status')
  .description('Check authentication status')
  .action(async () => {
    await showStatus();
  });

program
  .command('unpair')
  .description('Remove stored token')
  .action(async () => {
    try {
      const deleted = await removeToken();
      
      if (deleted) {
        console.log('✅ Token removed successfully');
        console.log('Run "claude-code pair" to authenticate again.');
      } else {
        console.log('No token found to remove');
      }
    } catch (error) {
      console.error('❌ Error:', error.message);
      process.exit(1);
    }
  });

program
  .command('export')
  .description('Export token for use in environment (use with caution)')
  .action(async () => {
    try {
      const token = await getToken();
      console.log(`export CLAUDE_CODE_MCP_TOKEN="${token}"`);
      console.log('\n⚠️  Warning: Only use this for debugging. Never commit tokens to version control.');
    } catch (error) {
      console.error('❌ Error:', error.message);
      process.exit(1);
    }
  });

// Handle deeplink activation (called by the gateway)
program
  .command('activate')
  .description('Handle activation deeplink (internal use)')
  .argument('<url>', 'Activation URL')
  .action(async (url) => {
    try {
      // Parse claudecode://activate?token=xxx URL
      const urlObj = new URL(url);
      
      if (urlObj.protocol !== 'claudecode:' || urlObj.hostname !== 'activate') {
        throw new Error('Invalid activation URL');
      }
      
      const token = urlObj.searchParams.get('token');
      
      if (!token) {
        throw new Error('No token in activation URL');
      }
      
      const metadata = await storeToken(token);
      
      console.log('✅ Activation successful!');
      console.log(`Token: ${metadata.prefix}...${metadata.suffix}`);
      console.log('\nClaude Code is now authenticated with Snowflake MCP.');
      
      // Optional: Launch Claude Code
      // require('child_process').exec('claude-code');
      
    } catch (error) {
      console.error('❌ Activation failed:', error.message);
      process.exit(1);
    }
  });

// Parse CLI arguments
program.parse(process.argv);

// Show help if no command provided
if (!process.argv.slice(2).length) {
  program.outputHelp();
}