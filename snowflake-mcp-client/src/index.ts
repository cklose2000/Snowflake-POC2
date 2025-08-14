import * as snowflake from 'snowflake-sdk';
import { getTokenFromKeychain, validateTokenFormat, extractTokenMetadata } from './auth';
import { getServiceCredentials } from './connection';
import { exec, execWithRetry, setQueryTag, configureSession } from './helpers/execute';
import { 
  ServiceCredentials, 
  MCPResponse, 
  QueryParams, 
  LogEventParams,
  ToolCallParams 
} from './types';

export class SnowflakeMCPClient {
  private connection: snowflake.Connection | null = null;
  private token: string | null = null;
  private credentials: ServiceCredentials | null = null;
  
  constructor(options: { token?: string } = {}) {
    // Token can be provided or retrieved from keychain
    this.token = options.token || null;
  }
  
  /**
   * Main method - maps directly to MCP.HANDLE_REQUEST stored procedure
   * CORRECTNESS: Uses proper Node SDK wrapper with retry logic
   * OBSERVABILITY: Query tagging and comprehensive error handling
   */
  async call(tool: string, params: any): Promise<MCPResponse> {
    await this.ensureConnection();
    await this.ensureToken();
    
    const startTime = Date.now();
    const version = require('../../package.json').version;
    
    try {
      // Set query tag for observability
      await setQueryTag(this.connection!, {
        agent: 'claude_code',
        tool,
        version,
        ts: Date.now()
      });
      
      // Execute with retry logic
      const result = await execWithRetry(
        this.connection!,
        `CALL CLAUDE_BI.MCP.HANDLE_REQUEST(?, ?, ?)`,
        [
          'tools/call',
          JSON.stringify({ name: tool, arguments: params }),
          this.token
        ]
      );
      
      const executionTime = Date.now() - startTime;
      const response = this.parseResult(result);
      
      // Log the request for observability
      await this.logRequestEvent(tool, executionTime, !response.error);
      
      // Return standardized response
      return {
        success: !response.error,
        data: response.data || response,
        error: response.error,
        metadata: {
          execution_time_ms: executionTime,
          client_version: version,
          ...response.metadata
        }
      };
      
    } catch (error) {
      const executionTime = Date.now() - startTime;
      
      // Log failed request
      await this.logRequestEvent(tool, executionTime, false, error.message);
      
      return {
        success: false,
        error: `Tool execution failed: ${error.message}`,
        metadata: {
          execution_time_ms: executionTime,
          client_version: version,
          user: 'unknown'
        }
      };
    }
  }
  
  /**
   * Natural language query interface
   */
  async query(naturalLanguage: string, options: Partial<QueryParams> = {}): Promise<MCPResponse> {
    return this.call('compose_query_plan', { 
      intent_text: naturalLanguage,
      ...options
    });
  }
  
  /**
   * List available data sources
   */
  async listSources(includeColumns: boolean = false): Promise<MCPResponse> {
    return this.call('list_sources', {
      include_columns: includeColumns
    });
  }
  
  /**
   * Create dashboard from query results
   */
  async createDashboard(title: string, queries: QueryParams[]): Promise<MCPResponse> {
    return this.call('create_dashboard', {
      title,
      queries
    });
  }
  
  /**
   * Validate a query plan without executing
   */
  async validatePlan(queryParams: QueryParams): Promise<MCPResponse> {
    return this.call('validate_plan', queryParams);
  }
  
  /**
   * Log a development event for observability
   * OPERATIONAL: Uses dedicated logging procedure with batching
   */
  async logEvent(action: string, attributes: any): Promise<MCPResponse> {
    await this.ensureConnection();
    
    const payload = {
      action: action.startsWith('ccode.') ? action : `ccode.${action}`,
      occurred_at: new Date().toISOString(),
      attributes
    };
    
    try {
      await exec(this.connection!, "CALL MCP.LOG_DEV_EVENT(?)", [JSON.stringify(payload)]);
      return { success: true };
    } catch (error) {
      return {
        success: false,
        error: `Logging failed: ${error.message}`
      };
    }
  }
  
  /**
   * Get current user permissions and status
   */
  async getUserStatus(): Promise<MCPResponse> {
    return this.call('get_user_status', {});
  }
  
  /**
   * Test connection and token validity
   * CORRECTNESS: Comprehensive validation with server-side token check
   */
  async test(): Promise<MCPResponse> {
    try {
      // Test service connection
      await this.ensureConnection();
      
      // Test token format
      await this.ensureToken();
      
      // Test token validity with server
      const validation = await this.validateToken();
      if (!validation.success) {
        return validation;
      }
      
      // Test actual tool call
      const result = await this.call('list_sources', {});
      
      if (result.success) {
        return {
          success: true,
          data: {
            message: 'Connection and token are valid',
            token_hint: this.token ? extractTokenMetadata(this.token) : null,
            sources_available: Array.isArray(result.data) ? result.data.length : 0,
            validation_status: validation.data
          }
        };
      } else {
        return result;
      }
      
    } catch (error) {
      return {
        success: false,
        error: `Connection test failed: ${error.message}`
      };
    }
  }
  
  /**
   * Validate token with server-side check
   * OPERATIONAL: Quick token validation without full permission lookup
   */
  async validateToken(): Promise<MCPResponse> {
    try {
      await this.ensureConnection();
      await this.ensureToken();
      
      const result = await exec(
        this.connection!,
        "CALL MCP.VALIDATE_TOKEN(?)",
        [this.token]
      );
      
      const validation = this.parseResult(result);
      
      return {
        success: validation.valid,
        data: validation,
        error: validation.valid ? undefined : validation.status
      };
      
    } catch (error) {
      return {
        success: false,
        error: `Token validation failed: ${error.message}`
      };
    }
  }
  
  /**
   * Ensure we have a valid Snowflake connection
   * CORRECTNESS: Uses key-pair auth and proper session configuration
   */
  private async ensureConnection(): Promise<void> {
    if (this.connection) {
      return;
    }
    
    if (!this.credentials) {
      this.credentials = await getServiceCredentials();
    }
    
    this.connection = snowflake.createConnection({
      account: this.credentials.account,
      username: this.credentials.username,
      privateKey: this.credentials.privateKey as any,
      privateKeyPass: this.credentials.privateKeyPass,
      authenticator: this.credentials.authenticator,
      role: this.credentials.role,
      warehouse: this.credentials.warehouse,
      database: this.credentials.database,
      clientSessionKeepAlive: this.credentials.clientSessionKeepAlive
    });
    
    return new Promise((resolve, reject) => {
      this.connection!.connect(async (err: any) => {
        if (err) {
          this.connection = null;
          reject(new Error(`Snowflake connection failed: ${err.message}`));
        } else {
          try {
            // Configure session for optimal performance and security
            await configureSession(this.connection!);
            resolve();
          } catch (configError) {
            reject(new Error(`Session configuration failed: ${configError.message}`));
          }
        }
      });
    });
  }
  
  /**
   * Ensure we have a valid user token
   */
  private async ensureToken(): Promise<void> {
    if (this.token) {
      return;
    }
    
    this.token = await getTokenFromKeychain();
    
    if (!validateTokenFormat(this.token)) {
      throw new Error('Invalid token format. Please run: snowflake-mcp login');
    }
  }
  
  /**
   * Log request events for observability
   */
  private async logRequestEvent(
    tool: string, 
    executionTime: number, 
    success: boolean, 
    error?: string
  ): Promise<void> {
    try {
      await this.logEvent('mcp.request', {
        tool,
        success,
        execution_time_ms: executionTime,
        error: error || undefined,
        timestamp: new Date().toISOString()
      });
    } catch (logError) {
      // Don't throw on logging failures
      console.warn('Failed to log request event:', logError.message);
    }
  }
  
  /**
   * Parse the result from stored procedures
   * CORRECTNESS: Handles single column VARIANT returns properly
   */
  private parseResult(rows: any[]): any {
    if (!rows?.length) {
      return null;
    }
    
    const first = rows[0];
    
    // Get the first column (procedures return single column)
    const key = Object.keys(first)[0];
    const val = first[key];
    
    // Parse JSON if it's a string
    return typeof val === 'string' ? JSON.parse(val) : val;
  }
  
  /**
   * Close the connection
   */
  async disconnect(): Promise<void> {
    if (this.connection) {
      await new Promise<void>((resolve) => {
        this.connection!.destroy((err: any) => {
          this.connection = null;
          resolve();
        });
      });
    }
  }
}

// Export for convenience
export * from './types';
export * from './auth';
export { getServiceCredentials } from './connection';