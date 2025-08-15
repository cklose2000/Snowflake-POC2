import * as fs from 'fs';
import { ServiceCredentials } from './types';

/**
 * Get service account credentials using key-pair authentication
 * SECURITY: Uses private key + passphrase instead of embedded passwords
 */
export async function getServiceCredentials(): Promise<ServiceCredentials> {
  try {
    // Validate required environment variables
    const account = process.env.SNOWFLAKE_ACCOUNT;
    const username = process.env.MCP_SERVICE_USER;
    
    if (!account) {
      throw new Error('Missing required environment variable: SNOWFLAKE_ACCOUNT');
    }
    
    if (!username) {
      throw new Error('Missing required environment variable: MCP_SERVICE_USER');
    }
    
    const base = {
      account,
      username,
      role: process.env.MCP_SERVICE_ROLE || 'MCP_SERVICE_ROLE',
      warehouse: process.env.MCP_SERVICE_WAREHOUSE || 'CLAUDE_WAREHOUSE',
      database: process.env.SNOWFLAKE_DATABASE || 'CLAUDE_BI',
      clientSessionKeepAlive: true
    };

    // Priority order: key-pair (if SF_PK_PATH exists), else password, else error
    if (process.env.SF_PK_PATH) {
      // Key-pair authentication
      let privateKey: string;
      try {
        privateKey = fs.readFileSync(process.env.SF_PK_PATH, 'utf8');
      } catch (error) {
        throw new Error(`Failed to read private key from ${process.env.SF_PK_PATH}: ${error.message}`);
      }

      return {
        ...base,
        privateKey,
        privateKeyPass: process.env.SF_PK_PASSPHRASE,
        authenticator: 'SNOWFLAKE_JWT'
      } as ServiceCredentials;
      
    } else if (process.env.MCP_SERVICE_PASSWORD) {
      // Password authentication (fallback)
      return {
        ...base,
        password: process.env.MCP_SERVICE_PASSWORD
      } as ServiceCredentials;
      
    } else {
      throw new Error('No auth configured: set SF_PK_PATH (preferred) or MCP_SERVICE_PASSWORD');
    }
    
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