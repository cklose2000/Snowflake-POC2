/**
 * Simplified Snowflake MCP Client using native auth
 * No tokens, no keychain, just direct Snowflake connections
 */

import * as snowflake from 'snowflake-sdk';
import * as fs from 'fs';
import { Connection, Statement } from 'snowflake-sdk';

export interface ClientConfig {
  account?: string;
  username?: string;
  password?: string;
  privateKeyPath?: string;
  role?: string;
  warehouse?: string;
  database?: string;
  schema?: string;
}

export interface QueryResult {
  success: boolean;
  data?: any;
  error?: string;
  metadata?: {
    queryId?: string;
    executionTimeMs?: number;
    rowCount?: number;
  };
}

export class SnowflakeSimpleClient {
  private connection: Connection | null = null;
  private config: ClientConfig;
  private isConnected: boolean = false;

  constructor(config?: ClientConfig) {
    // Load from environment if not provided
    this.config = {
      account: config?.account || process.env.SNOWFLAKE_ACCOUNT,
      username: config?.username || process.env.SNOWFLAKE_USERNAME,
      password: config?.password || process.env.SNOWFLAKE_PASSWORD,
      privateKeyPath: config?.privateKeyPath || process.env.SF_PK_PATH,
      role: config?.role || process.env.SNOWFLAKE_ROLE,
      warehouse: config?.warehouse || process.env.SNOWFLAKE_WAREHOUSE || 'CLAUDE_WAREHOUSE',
      database: config?.database || process.env.SNOWFLAKE_DATABASE || 'CLAUDE_BI',
      schema: config?.schema || process.env.SNOWFLAKE_SCHEMA || 'MCP'
    };

    // Validate required fields
    if (!this.config.account) {
      throw new Error('SNOWFLAKE_ACCOUNT is required');
    }
    if (!this.config.username) {
      throw new Error('SNOWFLAKE_USERNAME is required');
    }
    if (!this.config.password && !this.config.privateKeyPath) {
      throw new Error('Either SNOWFLAKE_PASSWORD or SF_PK_PATH is required');
    }
  }

  /**
   * Connect to Snowflake
   */
  async connect(): Promise<void> {
    if (this.isConnected) return;

    const connectionOptions: any = {
      account: this.config.account,
      username: this.config.username,
      role: this.config.role,
      warehouse: this.config.warehouse,
      database: this.config.database,
      schema: this.config.schema
    };

    // Use key-pair auth if private key path provided
    if (this.config.privateKeyPath) {
      try {
        const privateKey = fs.readFileSync(this.config.privateKeyPath, 'utf8');
        connectionOptions.privateKey = privateKey;
        connectionOptions.authenticator = 'SNOWFLAKE_JWT';
        console.log(`🔐 Using key-pair authentication for ${this.config.username}`);
      } catch (error: any) {
        throw new Error(`Failed to read private key: ${error.message}`);
      }
    } else {
      // Use password auth
      connectionOptions.password = this.config.password;
      console.log(`🔑 Using password authentication for ${this.config.username}`);
    }

    return new Promise((resolve, reject) => {
      this.connection = snowflake.createConnection(connectionOptions);
      
      this.connection.connect((err) => {
        if (err) {
          reject(new Error(`Connection failed: ${err.message}`));
        } else {
          this.isConnected = true;
          console.log(`✅ Connected to Snowflake as ${this.config.username}`);
          
          // Set query tag for observability
          this.setQueryTag().then(() => resolve()).catch(reject);
        }
      });
    });
  }

  /**
   * Set query tag for observability
   */
  private async setQueryTag(): Promise<void> {
    const tag = {
      client: 'snowflake-mcp-simple',
      user: this.config.username,
      role: this.config.role,
      timestamp: new Date().toISOString()
    };

    return this.execute('ALTER SESSION SET QUERY_TAG = ?', [JSON.stringify(tag)]);
  }

  /**
   * Execute a SQL statement
   */
  private execute(sqlText: string, binds: any[] = []): Promise<any> {
    if (!this.connection) {
      throw new Error('Not connected to Snowflake');
    }

    return new Promise((resolve, reject) => {
      this.connection!.execute({
        sqlText,
        binds,
        complete: (err: any, stmt: Statement, rows: any[]) => {
          if (err) {
            reject(err);
          } else {
            resolve(rows || []);
          }
        }
      });
    });
  }

  /**
   * Call a stored procedure
   */
  async callProcedure(procedureName: string, ...args: any[]): Promise<QueryResult> {
    await this.connect();
    
    const startTime = Date.now();
    const placeholders = args.map(() => '?').join(', ');
    const sql = `CALL ${procedureName}(${placeholders})`;

    try {
      const result = await this.execute(sql, args);
      const executionTime = Date.now() - startTime;

      // Parse result (procedures return single row with variant)
      const data = result[0] ? result[0][Object.keys(result[0])[0]] : null;
      
      return {
        success: true,
        data: data,
        metadata: {
          executionTimeMs: executionTime,
          rowCount: result.length
        }
      };
    } catch (error: any) {
      return {
        success: false,
        error: error.message,
        metadata: {
          executionTimeMs: Date.now() - startTime
        }
      };
    }
  }

  /**
   * Insert an event (using SAFE_INSERT_EVENT procedure)
   */
  async insertEvent(event: any, sourceLane: string = 'APPLICATION'): Promise<QueryResult> {
    return this.callProcedure('CLAUDE_BI.MCP.SAFE_INSERT_EVENT', event, sourceLane);
  }

  /**
   * Compose a query plan from natural language
   */
  async composeQueryPlan(intent: string, limit: number = 100, timeWindowHours: number = 24): Promise<QueryResult> {
    return this.callProcedure('CLAUDE_BI.MCP.COMPOSE_QUERY_PLAN', intent, limit, timeWindowHours);
  }

  /**
   * Validate a query plan
   */
  async validateQueryPlan(plan: any): Promise<QueryResult> {
    return this.callProcedure('CLAUDE_BI.MCP.VALIDATE_QUERY_PLAN', plan);
  }

  /**
   * Execute a query plan
   */
  async executeQueryPlan(plan: any): Promise<QueryResult> {
    const result = await this.callProcedure('CLAUDE_BI.MCP.EXECUTE_QUERY_PLAN', plan);
    
    // If successful, fetch the actual results using RESULT_SCAN
    if (result.success && result.data?.query_id) {
      try {
        const queryResults = await this.execute('SELECT * FROM TABLE(RESULT_SCAN(?));', [result.data.query_id]);
        result.data.results = queryResults;
      } catch (error: any) {
        console.warn('Could not fetch query results:', error.message);
      }
    }
    
    return result;
  }

  /**
   * List available data sources
   */
  async listSources(): Promise<QueryResult> {
    return this.callProcedure('CLAUDE_BI.MCP.LIST_SOURCES');
  }

  /**
   * Get current user status and permissions
   */
  async getUserStatus(): Promise<QueryResult> {
    return this.callProcedure('CLAUDE_BI.MCP.GET_USER_STATUS');
  }

  /**
   * Natural language query helper
   */
  async query(naturalLanguageQuery: string, limit: number = 100): Promise<QueryResult> {
    // Compose the plan
    const planResult = await this.composeQueryPlan(naturalLanguageQuery, limit);
    if (!planResult.success) {
      return planResult;
    }

    // Validate the plan
    const validationResult = await this.validateQueryPlan(planResult.data.plan);
    if (!validationResult.success || !validationResult.data.is_valid) {
      return {
        success: false,
        error: 'Plan validation failed',
        data: validationResult.data
      };
    }

    // Execute the plan
    return this.executeQueryPlan(planResult.data.plan);
  }

  /**
   * Execute raw SQL (for testing/debugging)
   */
  async executeSql(sql: string, binds: any[] = []): Promise<QueryResult> {
    await this.connect();
    
    const startTime = Date.now();
    try {
      const rows = await this.execute(sql, binds);
      return {
        success: true,
        data: rows,
        metadata: {
          executionTimeMs: Date.now() - startTime,
          rowCount: rows.length
        }
      };
    } catch (error: any) {
      return {
        success: false,
        error: error.message,
        metadata: {
          executionTimeMs: Date.now() - startTime
        }
      };
    }
  }

  /**
   * Disconnect from Snowflake
   */
  async disconnect(): Promise<void> {
    if (!this.connection || !this.isConnected) return;

    return new Promise((resolve) => {
      this.connection!.destroy(() => {
        this.isConnected = false;
        this.connection = null;
        console.log('🔌 Disconnected from Snowflake');
        resolve();
      });
    });
  }
}

// Export for convenience
export default SnowflakeSimpleClient;