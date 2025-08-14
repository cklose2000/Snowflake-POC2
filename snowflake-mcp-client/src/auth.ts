import { ServiceCredentials } from './types';

/**
 * Account-scoped service name for keychain storage
 * SECURITY: Tokens are scoped per Snowflake account
 */
function getServiceName(): string {
  const account = process.env.SNOWFLAKE_ACCOUNT;
  if (!account) {
    throw new Error('SNOWFLAKE_ACCOUNT environment variable is required');
  }
  return `SnowflakeMCP:${account}`;
}

const ACCOUNT_NAME = 'user_token';

export async function getTokenFromKeychain(): Promise<string> {
  try {
    const keytar = require('keytar');
    const serviceName = getServiceName();
    const token = await keytar.getPassword(serviceName, ACCOUNT_NAME);
    
    if (!token) {
      throw new Error(`No MCP token found for account ${process.env.SNOWFLAKE_ACCOUNT}. Please run: snowflake-mcp login`);
    }
    
    return token;
  } catch (error) {
    if (error.message.includes('No MCP token found')) {
      throw error;
    }
    
    // Fallback for environments without keychain access
    const envToken = process.env.SNOWFLAKE_MCP_TOKEN;
    if (envToken) {
      return envToken;
    }
    
    throw new Error(`Token access failed: ${error.message}. Set SNOWFLAKE_MCP_TOKEN environment variable as fallback.`);
  }
}

export async function saveTokenToKeychain(token: string): Promise<void> {
  try {
    const keytar = require('keytar');
    const serviceName = getServiceName();
    await keytar.setPassword(serviceName, ACCOUNT_NAME, token);
  } catch (error) {
    console.warn(`Could not save to keychain: ${error.message}`);
    console.warn('Consider setting SNOWFLAKE_MCP_TOKEN environment variable instead.');
    throw error;
  }
}

export async function removeTokenFromKeychain(): Promise<void> {
  try {
    const keytar = require('keytar');
    const serviceName = getServiceName();
    await keytar.deletePassword(serviceName, ACCOUNT_NAME);
  } catch (error) {
    console.warn(`Could not remove from keychain: ${error.message}`);
  }
}

export function validateTokenFormat(token: string): boolean {
  // Quick format validation without hitting Snowflake
  return token.startsWith('tk_') && token.length >= 40;
}

export function extractTokenMetadata(token: string): { prefix: string; suffix: string; length: number } {
  if (!validateTokenFormat(token)) {
    throw new Error('Invalid token format');
  }
  
  return {
    prefix: token.substring(0, 8),
    suffix: token.substring(token.length - 8),
    length: token.length
  };
}