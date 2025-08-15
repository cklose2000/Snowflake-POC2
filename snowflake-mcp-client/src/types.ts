export interface ServiceCredentials {
  account: string;
  username: string;
  // Key-pair auth
  privateKey?: string;  // PEM string, not Buffer
  privateKeyPass?: string;
  authenticator?: string;
  // Password auth (fallback)
  password?: string;
  // Common fields
  role: string;
  warehouse: string;
  database: string;
  clientSessionKeepAlive: boolean;
}

export interface UserPermissions {
  username: string;
  allowed_tools: string[];
  max_rows: number;
  daily_runtime_seconds: number;
  expires_at: Date;
}

export interface ToolCallParams {
  name: string;
  arguments: Record<string, any>;
}

export interface MCPResponse {
  success: boolean;
  data?: any;
  error?: string;
  metadata?: {
    execution_time_ms: number;
    user?: string;
    rows_returned?: number;
    bytes_scanned?: number;
    client_version?: string;
    [key: string]: any;
  };
}

export interface QueryParams {
  intent_text: string;
  source?: string;
  dimensions?: string[];
  measures?: Array<{ fn: string; column: string }>;
  filters?: Array<{ column: string; operator: string; value: any }>;
  top_n?: number;
}

export interface LogEventParams {
  action: string;
  occurred_at: string;
  attributes: Record<string, any>;
}