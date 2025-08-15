/**
 * Simplified Snowflake MCP Client using native auth
 * No tokens, no keychain, just direct Snowflake connections
 */

import * as snowflake from 'snowflake-sdk';
import * as fs from 'fs';
import { Connection } from 'snowflake-sdk';

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

export interface LogEvent {
  action: string;
  session_id?: string;
  actor_id?: string;
  attributes?: Record<string, any>;
  object?: {
    type: string;
    id: string;
  };
  occurred_at?: string;
}

export interface LogConfig {
  batchThreshold?: number;  // Switch to batch mode above this events/min
  flushIntervalMs?: number;  // Auto-flush interval for batched events
  maxBatchSize?: number;     // Maximum events per batch
}

export class SnowflakeSimpleClient {
  private connection: Connection | null = null;
  private config: ClientConfig;
  private isConnected: boolean = false;
  private sessionId: string;
  
  // Logging state
  private eventBuffer: LogEvent[] = [];
  private flushTimer: NodeJS.Timeout | null = null;
  private eventCounts: Map<string, number> = new Map();
  private lastCountReset: number = Date.now();
  private logConfig: LogConfig = {
    batchThreshold: 10,    // Events per minute to trigger batch mode
    flushIntervalMs: 5000,  // Flush every 5 seconds
    maxBatchSize: 100       // Max events per batch
  };

  constructor(config?: ClientConfig, logConfig?: LogConfig) {
    // Generate unique session ID
    this.sessionId = `cc-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
    
    // Load from environment if not provided
    this.config = {
      account: config?.account || process.env.SNOWFLAKE_ACCOUNT,
      username: config?.username || process.env.SNOWFLAKE_USERNAME,
      password: config?.password || process.env.SNOWFLAKE_PASSWORD,
      privateKeyPath: config?.privateKeyPath || process.env.SF_PK_PATH,
      // Note: Role should be set as DEFAULT_ROLE on the user, not in config
      warehouse: config?.warehouse || process.env.SNOWFLAKE_WAREHOUSE || 'CLAUDE_AGENT_WH',
      database: config?.database || process.env.SNOWFLAKE_DATABASE || 'CLAUDE_BI',
      schema: config?.schema || process.env.SNOWFLAKE_SCHEMA || 'MCP'
    };
    
    // Merge log config
    if (logConfig) {
      this.logConfig = { ...this.logConfig, ...logConfig };
    }

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
      warehouse: this.config.warehouse,
      database: this.config.database,
      schema: this.config.schema,
      // Performance optimizations
      clientSessionKeepAlive: true,
      statementTimeout: 120
      // Role is intentionally omitted - use DEFAULT_ROLE on user
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
      
      this.connection.connect(async (err) => {
        if (err) {
          reject(new Error(`Connection failed: ${err.message}`));
        } else {
          this.isConnected = true;
          console.log(`✅ Connected to Snowflake as ${this.config.username}`);
          console.log(`📊 Session ID: ${this.sessionId}`);
          
          try {
            // Session optimization settings for performance
            await this.execute(`ALTER SESSION SET 
              AUTOCOMMIT = TRUE,
              USE_CACHED_RESULT = TRUE,
              STATEMENT_TIMEOUT_IN_SECONDS = 120,
              QUERY_TAG = 'cc-cli|session:${this.sessionId}'`);
            
            // Explicitly set warehouse to ensure deterministic usage
            await this.execute(`USE WAREHOUSE ${this.config.warehouse}`);
            
            // Log session start to ACTIVITY.EVENTS
            await this.logEvent({
              action: 'ccode.session.started',
              session_id: this.sessionId,
              attributes: {
                user: this.config.username,
                warehouse: this.config.warehouse,
                database: this.config.database,
                schema: this.config.schema
              }
            });
            
            resolve();
          } catch (error) {
            reject(error);
          }
        }
      });
    });
  }

  /**
   * Set query tag for observability
   */
  private async setQueryTag(operation: string = 'session_init', sessionId?: string): Promise<void> {
    const tag = {
      agent: 'claude-code',
      op: operation,
      sess: sessionId || 'default',
      user: this.config.username,
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
        complete: (err: any, stmt: any, rows: any[]) => {
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
    // Convert objects to JSON strings for VARIANT parameters
    const processedArgs = args.map(arg => {
      if (typeof arg === 'object' && arg !== null) {
        return JSON.stringify(arg);
      }
      return arg;
    });
    
    // Build placeholders - use PARSE_JSON for object arguments
    const placeholders = args.map((arg, i) => {
      if (typeof arg === 'object' && arg !== null) {
        return 'PARSE_JSON(?)';
      }
      return '?';
    }).join(', ');
    const sql = `CALL ${procedureName}(${placeholders})`;

    try {
      const result = await this.execute(sql, processedArgs);
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
    let success = false;
    let error: string | undefined;
    let rows: any;
    let rowCount: number | undefined;
    
    try {
      rows = await this.execute(sql, binds);
      success = true;
      rowCount = Array.isArray(rows) ? rows.length : undefined;
    } catch (err: any) {
      success = false;
      error = err.message;
    }
    
    const executionTime = Date.now() - startTime;
    
    // Log to ACTIVITY.EVENTS
    await this.logEvent({
      action: 'ccode.sql.executed',
      session_id: this.sessionId,
      attributes: {
        sql_preview: sql.substring(0, 200),
        sql_type: this.detectSqlType(sql),
        execution_time_ms: executionTime,
        success,
        rows_affected: rowCount,
        error
      }
    });
    
    return {
      success,
      data: rows,
      error,
      metadata: {
        executionTimeMs: executionTime,
        rowCount
      }
    };
  }
  
  /**
   * Detect SQL statement type for logging
   */
  private detectSqlType(sql: string): string {
    const upper = sql.toUpperCase().trim();
    if (upper.startsWith('CREATE')) return 'DDL_CREATE';
    if (upper.startsWith('ALTER')) return 'DDL_ALTER';
    if (upper.startsWith('DROP')) return 'DDL_DROP';
    if (upper.startsWith('GRANT')) return 'DCL_GRANT';
    if (upper.startsWith('REVOKE')) return 'DCL_REVOKE';
    if (upper.startsWith('INSERT')) return 'DML_INSERT';
    if (upper.startsWith('UPDATE')) return 'DML_UPDATE';
    if (upper.startsWith('DELETE')) return 'DML_DELETE';
    if (upper.startsWith('MERGE')) return 'DML_MERGE';
    if (upper.startsWith('SELECT')) return 'DQL_SELECT';
    if (upper.startsWith('CALL')) return 'PROC_CALL';
    if (upper.startsWith('USE')) return 'SESSION';
    if (upper.startsWith('SHOW')) return 'METADATA';
    if (upper.startsWith('DESC')) return 'METADATA';
    return 'OTHER';
  }

  /**
   * Log a single event using direct SP call
   */
  async logEvent(event: LogEvent, sessionId?: string): Promise<QueryResult> {
    await this.connect();
    
    // Track event rate
    this.trackEventRate(event.action);
    
    // Check if we should batch
    if (this.shouldUseBatchMode()) {
      return this.addToBatch(event);
    }
    
    // Set query tag for this operation
    await this.setQueryTag('log_event', sessionId || event.session_id);
    
    // Direct SP call
    return this.callProcedure('CLAUDE_BI.MCP.LOG_CLAUDE_EVENT', event, 'CLAUDE_CODE');
  }
  
  /**
   * Log multiple events in batch
   */
  async logEventsBatch(events: LogEvent[], sessionId?: string): Promise<QueryResult> {
    await this.connect();
    
    // Set query tag
    await this.setQueryTag('log_batch', sessionId);
    
    // Call batch procedure
    return this.callProcedure('CLAUDE_BI.MCP.LOG_CLAUDE_EVENTS_BATCH', events, 'CLAUDE_CODE');
  }
  
  /**
   * Track event rate for auto-batching
   */
  private trackEventRate(action: string): void {
    const now = Date.now();
    
    // Reset counts every minute
    if (now - this.lastCountReset > 60000) {
      this.eventCounts.clear();
      this.lastCountReset = now;
    }
    
    // Increment count
    const count = (this.eventCounts.get(action) || 0) + 1;
    this.eventCounts.set(action, count);
  }
  
  /**
   * Check if we should use batch mode based on event rate
   */
  private shouldUseBatchMode(): boolean {
    let totalEvents = 0;
    for (const count of this.eventCounts.values()) {
      totalEvents += count;
    }
    
    // Events per minute (extrapolated)
    const elapsedMs = Date.now() - this.lastCountReset;
    const eventsPerMinute = (totalEvents / elapsedMs) * 60000;
    
    return eventsPerMinute > this.logConfig.batchThreshold!;
  }
  
  /**
   * Add event to batch buffer
   */
  private async addToBatch(event: LogEvent): Promise<QueryResult> {
    this.eventBuffer.push(event);
    
    // Flush if buffer is full
    if (this.eventBuffer.length >= this.logConfig.maxBatchSize!) {
      return this.flushBatch();
    }
    
    // Schedule flush if not already scheduled
    if (!this.flushTimer) {
      this.flushTimer = setTimeout(() => {
        this.flushBatch().catch(err => {
          console.error('Batch flush failed:', err);
        });
      }, this.logConfig.flushIntervalMs);
    }
    
    return {
      success: true,
      data: { buffered: true, bufferSize: this.eventBuffer.length }
    };
  }
  
  /**
   * Flush buffered events
   */
  async flushBatch(): Promise<QueryResult> {
    if (this.eventBuffer.length === 0) {
      return { success: true, data: { flushed: 0 } };
    }
    
    // Clear timer
    if (this.flushTimer) {
      clearTimeout(this.flushTimer);
      this.flushTimer = null;
    }
    
    // Get events to flush
    const events = [...this.eventBuffer];
    this.eventBuffer = [];
    
    // Send batch
    return this.logEventsBatch(events);
  }
  
  /**
   * Log session start
   */
  async logSessionStart(sessionId: string, metadata?: Record<string, any>): Promise<QueryResult> {
    return this.logEvent({
      action: 'ccode.session.started',
      session_id: sessionId,
      attributes: {
        ...metadata,
        started_at: new Date().toISOString()
      }
    });
  }
  
  /**
   * Log session end and flush any pending events
   */
  async logSessionEnd(sessionId: string, metadata?: Record<string, any>): Promise<QueryResult> {
    // Flush any pending events first
    if (this.eventBuffer.length > 0) {
      await this.flushBatch();
    }
    
    // Log end event
    return this.logEvent({
      action: 'ccode.session.ended',
      session_id: sessionId,
      attributes: {
        ...metadata,
        ended_at: new Date().toISOString()
      }
    });
  }
  
  /**
   * Disconnect from Snowflake
   */
  async disconnect(): Promise<void> {
    // Flush any pending events
    if (this.eventBuffer.length > 0) {
      await this.flushBatch().catch(err => {
        console.error('Failed to flush on disconnect:', err);
      });
    }
    
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