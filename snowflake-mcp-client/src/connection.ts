import * as fs from 'fs';
import { ServiceCredentials } from './types';

/**
 * Get service account credentials using key-pair authentication
 * SECURITY: Uses private key + passphrase instead of embedded passwords
 */
export async function getServiceCredentials(): Promise<ServiceCredentials> {
  try {
    // Validate required environment variables
    const requiredEnvVars = [
      'SNOWFLAKE_ACCOUNT',
      'MCP_SERVICE_USER', 
      'SF_PK_PATH'
    ];
    
    for (const envVar of requiredEnvVars) {
      if (!process.env[envVar]) {
        throw new Error(`Missing required environment variable: ${envVar}`);
      }
    }
    
    // Read private key from file
    let privateKey: Buffer;
    try {
      privateKey = fs.readFileSync(process.env.SF_PK_PATH!);
    } catch (error) {
      throw new Error(`Failed to read private key from ${process.env.SF_PK_PATH}: ${error.message}`);
    }
    
    // Get passphrase from environment or keychain
    let privateKeyPass = process.env.SF_PK_PASSPHRASE;
    if (!privateKeyPass) {
      try {
        const keytar = require('keytar');
        privateKeyPass = await keytar.getPassword('SnowflakeMCP', 'service_key_passphrase');
      } catch (error) {
        // Optional passphrase - some keys may not have one
        console.warn('No passphrase found for private key');
      }
    }
    
    return {
      account: process.env.SNOWFLAKE_ACCOUNT!,          // e.g. "uec18397.us-east-1"
      username: process.env.MCP_SERVICE_USER!,          // "MCP_SERVICE_USER"
      privateKey,                                       // Buffer from PEM file
      privateKeyPass,                                   // Optional passphrase
      authenticator: 'SNOWFLAKE_JWT',                   // Key-pair auth
      role: process.env.MCP_SERVICE_ROLE || 'MCP_SERVICE_ROLE',
      warehouse: process.env.MCP_SERVICE_WAREHOUSE || 'MCP_XS_WH',
      database: process.env.SNOWFLAKE_DATABASE || 'CLAUDE_BI',
      clientSessionKeepAlive: true                      // Prevent mid-session drops
    };
    
  } catch (error) {
    throw new Error(`Failed to load service credentials: ${error.message}`);
  }
}

/**
 * Store service key passphrase in OS keychain (setup utility)
 */
export async function storeServiceKeyPassphrase(passphrase: string): Promise<void> {
  try {
    const keytar = require('keytar');
    await keytar.setPassword('SnowflakeMCP', 'service_key_passphrase', passphrase);
  } catch (error) {
    throw new Error(`Failed to store service key passphrase: ${error.message}`);
  }
}