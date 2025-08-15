import { SnowflakeClient } from '../clients/snowflake-client.js';
import { McpError, ErrorCode } from '@modelcontextprotocol/sdk/types.js';

export interface UserContext {
  username: string;
  allowedTools: string[];
  maxRows: number;
  dailyRuntimeSeconds: number;
  expiresAt: Date;
  tokenPrefix: string;
  usageTracking?: {
    dailyRuntimeUsed: number;
    lastUsed: Date;
  };
}

export class TokenAuthenticator {
  constructor(private snowflakeClient: SnowflakeClient) {}

  async authenticate(token: string): Promise<UserContext> {
    try {
      // Hash the token with pepper
      const hashResult = await this.snowflakeClient.executeQuery(
        `SELECT MCP.HASH_TOKEN_WITH_PEPPER('${token}') AS token_hash`
      );
      
      if (!hashResult.rows || hashResult.rows.length === 0) {
        throw new McpError(ErrorCode.InvalidRequest, 'Invalid token format');
      }
      
      const tokenHash = hashResult.rows[0].TOKEN_HASH;
      
      // Look up user by token hash
      const userResult = await this.snowflakeClient.executeQuery(`
        SELECT 
          object_id AS username,
          attributes:allowed_tools::ARRAY AS allowed_tools,
          attributes:max_rows::NUMBER AS max_rows,
          attributes:daily_runtime_seconds::NUMBER AS daily_runtime_seconds,
          attributes:expires_at::TIMESTAMP_TZ AS expires_at,
          attributes:token_prefix::STRING AS token_prefix,
          occurred_at AS issued_at
        FROM CLAUDE_BI.ACTIVITY.EVENTS
        WHERE action = 'system.permission.granted'
          AND object_type = 'user'
          AND attributes:token_hash::STRING = '${tokenHash}'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY object_id ORDER BY occurred_at DESC) = 1
      `);
      
      if (!userResult.rows || userResult.rows.length === 0) {
        throw new McpError(ErrorCode.InvalidRequest, 'Invalid or expired token');
      }
      
      const user = userResult.rows[0];
      
      // Check if token is expired
      const expiresAt = new Date(user.EXPIRES_AT);
      if (expiresAt < new Date()) {
        throw new McpError(ErrorCode.InvalidRequest, 'Token has expired');
      }
      
      // Extract metadata from token for validation
      const metaResult = await this.snowflakeClient.executeQuery(
        `SELECT MCP.EXTRACT_TOKEN_METADATA('${token}') AS metadata`
      );
      const metadata = metaResult.rows[0].METADATA;
      
      // Verify token prefix matches
      if (metadata.prefix !== user.TOKEN_PREFIX) {
        throw new McpError(ErrorCode.InvalidRequest, 'Token validation failed');
      }
      
      // Log authentication event
      await this.logAuthEvent(user.USERNAME, 'auth.success', {
        token_prefix: user.TOKEN_PREFIX,
        tools_available: user.ALLOWED_TOOLS?.length || 0
      });
      
      // Get usage tracking
      const usageResult = await this.snowflakeClient.executeQuery(`
        SELECT 
          COALESCE(SUM(attributes:runtime_seconds::NUMBER), 0) AS daily_runtime_used,
          MAX(occurred_at) AS last_used
        FROM CLAUDE_BI.ACTIVITY.EVENTS
        WHERE action = 'mcp.tool.executed'
          AND actor_id = '${user.USERNAME}'
          AND DATE(occurred_at) = CURRENT_DATE()
      `);
      
      const usage = usageResult.rows?.[0];
      
      return {
        username: user.USERNAME,
        allowedTools: Array.isArray(user.ALLOWED_TOOLS) ? user.ALLOWED_TOOLS : [],
        maxRows: user.MAX_ROWS || 1000,
        dailyRuntimeSeconds: user.DAILY_RUNTIME_SECONDS || 3600,
        expiresAt,
        tokenPrefix: user.TOKEN_PREFIX,
        usageTracking: usage ? {
          dailyRuntimeUsed: usage.DAILY_RUNTIME_USED || 0,
          lastUsed: usage.LAST_USED ? new Date(usage.LAST_USED) : new Date()
        } : undefined
      };
      
    } catch (error) {
      if (error instanceof McpError) {
        throw error;
      }
      
      console.error('Authentication error:', error);
      throw new McpError(
        ErrorCode.InternalError,
        'Authentication failed: ' + (error instanceof Error ? error.message : 'Unknown error')
      );
    }
  }
  
  async logAuthEvent(username: string, action: string, attributes: any): Promise<void> {
    try {
      await this.snowflakeClient.executeQuery(`
        INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS 
        SELECT 
          OBJECT_CONSTRUCT(
            'event_id', UUID_STRING(),
            'action', '${action}',
            'occurred_at', CURRENT_TIMESTAMP(),
            'actor_id', '${username}',
            'source', 'mcp_server',
            'object', OBJECT_CONSTRUCT(
              'type', 'authentication',
              'id', '${username}'
            ),
            'attributes', PARSE_JSON('${JSON.stringify(attributes)}')
          ),
          'MCP_AUTH',
          CURRENT_TIMESTAMP()
      `);
    } catch (error) {
      console.error('Failed to log auth event:', error);
      // Don't throw - auth logging failure shouldn't break authentication
    }
  }
  
  async logToolExecution(
    username: string, 
    toolName: string, 
    success: boolean, 
    runtimeSeconds: number,
    attributes?: any
  ): Promise<void> {
    try {
      await this.snowflakeClient.executeQuery(`
        INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS 
        SELECT 
          OBJECT_CONSTRUCT(
            'event_id', UUID_STRING(),
            'action', 'mcp.tool.executed',
            'occurred_at', CURRENT_TIMESTAMP(),
            'actor_id', '${username}',
            'source', 'mcp_server',
            'object', OBJECT_CONSTRUCT(
              'type', 'tool_execution',
              'id', '${toolName}'
            ),
            'attributes', OBJECT_CONSTRUCT(
              'tool_name', '${toolName}',
              'success', ${success},
              'runtime_seconds', ${runtimeSeconds},
              'additional_data', PARSE_JSON('${JSON.stringify(attributes || {})}')
            )
          ),
          'MCP_TOOL',
          CURRENT_TIMESTAMP()
      `);
    } catch (error) {
      console.error('Failed to log tool execution:', error);
      // Don't throw - logging failure shouldn't break tool execution
    }
  }
}