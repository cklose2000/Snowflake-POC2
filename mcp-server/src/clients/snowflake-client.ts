import snowflake from 'snowflake-sdk';
import { SecurityValidator } from '../validators/security-validator.js';
import { loadSchemaContract } from '../utils/schema-loader.js';

export interface QueryResult {
  rows: any[];
  queryId: string;
  bytesScanned?: number;
}

export class SnowflakeClient {
  private connection: any;
  private connected: boolean = false;
  private securityValidator: SecurityValidator | null = null;
  
  async connect(): Promise<void> {
    if (this.connected) {
      return;
    }
    
    // Load contract for security validation
    const contract = await loadSchemaContract();
    this.securityValidator = new SecurityValidator(contract);
    
    // Validate role before connecting
    const role = process.env.SNOWFLAKE_ROLE || 'CLAUDE_BI_READONLY';
    if (!this.securityValidator.validateRole(role)) {
      throw new Error(`Invalid role: ${role}. Only read-only roles are allowed.`);
    }
    
    // Create connection with strict read-only configuration
    this.connection = snowflake.createConnection({
      account: process.env.SNOWFLAKE_ACCOUNT!,
      username: process.env.SNOWFLAKE_USERNAME!,
      password: process.env.SNOWFLAKE_PASSWORD!,
      role: role,
      warehouse: process.env.SNOWFLAKE_WAREHOUSE || 'CLAUDE_WAREHOUSE',
      database: process.env.SNOWFLAKE_DATABASE || 'CLAUDE_BI',
      schema: process.env.SNOWFLAKE_SCHEMA || 'ANALYTICS',
      // Security settings
      clientSessionKeepAlive: false,
      clientSessionKeepAliveHeartbeatFrequency: 0
    });
    
    return new Promise((resolve, reject) => {
      this.connection.connect((err: any) => {
        if (err) {
          reject(new Error(`Snowflake connection failed: ${err.message}`));
        } else {
          this.connected = true;
          this.setSessionContext().then(resolve).catch(reject);
        }
      });
    });
  }
  
  private async setSessionContext(): Promise<void> {
    // Set strict session parameters
    const contextCommands = [
      `USE DATABASE ${process.env.SNOWFLAKE_DATABASE || 'CLAUDE_BI'}`,
      `USE SCHEMA ${process.env.SNOWFLAKE_SCHEMA || 'ANALYTICS'}`,
      `USE WAREHOUSE ${process.env.SNOWFLAKE_WAREHOUSE || 'CLAUDE_WAREHOUSE'}`,
      // Set query tag for audit trail
      `ALTER SESSION SET QUERY_TAG = 'mcp_server:${Date.now()}'`,
      // Set timeout
      `ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = ${this.securityValidator?.getQueryTimeout() || 300}`
    ];
    
    for (const cmd of contextCommands) {
      await this.executeRaw(cmd);
    }
  }
  
  async executeQuery(sql: string): Promise<QueryResult> {
    if (!this.connected) {
      await this.connect();
    }
    
    if (!this.securityValidator) {
      throw new Error('Security validator not initialized');
    }
    
    // Validate SQL security
    const validation = this.securityValidator.validateSQL(sql);
    if (!validation.valid) {
      throw new Error(`Security validation failed: ${validation.errors?.join(', ')}`);
    }
    
    // Dry run first (compile check)
    await this.dryRun(sql);
    
    // Execute with timeout
    const timeout = this.securityValidator.getQueryTimeout();
    return this.executeWithTimeout(sql, timeout);
  }
  
  private async dryRun(sql: string): Promise<void> {
    // Wrap in EXPLAIN to validate without executing
    const explainSQL = `EXPLAIN ${sql}`;
    
    return new Promise((resolve, reject) => {
      this.connection.execute({
        sqlText: explainSQL,
        complete: (err: any) => {
          if (err) {
            reject(new Error(`Query compilation failed: ${err.message}`));
          } else {
            resolve();
          }
        }
      });
    });
  }
  
  private async executeWithTimeout(sql: string, timeoutMs: number): Promise<QueryResult> {
    return new Promise((resolve, reject) => {
      let timedOut = false;
      
      const timeout = setTimeout(() => {
        timedOut = true;
        reject(new Error(`Query timeout after ${timeoutMs}ms`));
      }, timeoutMs);
      
      this.connection.execute({
        sqlText: sql,
        complete: (err: any, stmt: any, rows: any[]) => {
          clearTimeout(timeout);
          
          if (timedOut) {
            return;
          }
          
          if (err) {
            reject(new Error(`Query execution failed: ${err.message}`));
          } else {
            resolve({
              rows: rows || [],
              queryId: stmt.getQueryId ? stmt.getQueryId() : 'unknown',
              bytesScanned: stmt.getNumRowsAffected ? stmt.getNumRowsAffected() : undefined
            });
          }
        }
      });
    });
  }
  
  private async executeRaw(sql: string): Promise<void> {
    return new Promise((resolve, reject) => {
      this.connection.execute({
        sqlText: sql,
        complete: (err: any) => {
          if (err) {
            reject(err);
          } else {
            resolve();
          }
        }
      });
    });
  }
  
  async logActivity(activity: string, featureJson: any = {}): Promise<void> {
    if (!this.connected) {
      await this.connect();
    }
    
    const sql = `
      INSERT INTO CLAUDE_BI.ACTIVITY.EVENTS (
        activity_id, ts, customer, activity, feature_json,
        _source_system, _source_version, _session_id, _query_tag
      )
      SELECT 
        'act_' || UUID_STRING(),
        CURRENT_TIMESTAMP,
        '${process.env.ACTIVITY_CUSTOMER || 'mcp_server'}',
        '${activity}',
        PARSE_JSON('${JSON.stringify(featureJson).replace(/'/g, "''")}'),
        'mcp_server',
        '1.0.0',
        'session_${Date.now()}',
        'mcp:${activity}'
    `;
    
    try {
      await this.executeRaw(sql);
    } catch (error) {
      // Activity logging should not fail operations
      console.error('Activity logging failed:', error);
    }
  }
  
  async disconnect(): Promise<void> {
    if (!this.connected) {
      return;
    }
    
    return new Promise((resolve) => {
      this.connection.destroy((err: any) => {
        if (err) {
          console.error('Disconnect error:', err);
        }
        this.connected = false;
        resolve();
      });
    });
  }
}