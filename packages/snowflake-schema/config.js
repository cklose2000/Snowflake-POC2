// Snowflake Schema Configuration
// Single source of truth for all Snowflake object references
// Environment-aware, no hardcoding

class SnowflakeSchemaConfig {
  constructor() {
    // Pull from environment, never hardcode
    this.database = process.env.SNOWFLAKE_DATABASE || 'CLAUDE_BI';
    this.warehouse = process.env.SNOWFLAKE_WAREHOUSE || 'CLAUDE_WAREHOUSE';
    this.role = process.env.SNOWFLAKE_ROLE || 'CLAUDE_BI_ROLE';
    this.defaultSchema = process.env.SNOWFLAKE_SCHEMA || 'ANALYTICS';
    
    // Schema definitions with required columns
    this.schemas = {
      ACTIVITY: {
        tables: {
          EVENTS: {
            name: 'EVENTS',
            requiredColumns: [
              'ACTIVITY_ID',
              'TS', 
              'CUSTOMER',
              'ACTIVITY',
              'FEATURE_JSON'
            ],
            activitySchemaV2: [
              '_ACTIVITY_OCCURRENCE',
              '_ACTIVITY_REPEATED_AT'
            ],
            systemColumns: [
              '_SOURCE_SYSTEM',
              '_SOURCE_VERSION',
              '_SESSION_ID',
              '_QUERY_TAG'
            ],
            optionalColumns: [
              'ANONYMOUS_CUSTOMER_ID',
              'REVENUE_IMPACT',
              'LINK'
            ]
          }
        }
      },
      ACTIVITY_CCODE: {
        tables: {
          ARTIFACTS: {
            name: 'ARTIFACTS',
            requiredColumns: [
              'ARTIFACT_ID',
              'SAMPLE',
              'ROW_COUNT',
              'SCHEMA_JSON'
            ]
          },
          AUDIT_RESULTS: {
            name: 'AUDIT_RESULTS',
            requiredColumns: [
              'AUDIT_ID',
              'TS',
              'PASSED',
              'DETAILS'
            ]
          }
        },
        views: {
          VW_ACTIVITY_COUNTS_24H: {
            name: 'VW_ACTIVITY_COUNTS_24H',
            description: 'Activity counts by type and customer for last 24 hours',
            timeWindow: '24h'
          },
          VW_LLM_TELEMETRY: {
            name: 'VW_LLM_TELEMETRY',
            description: 'LLM usage telemetry including tokens and latency',
            timeWindow: '7d'
          },
          VW_SQL_EXECUTIONS: {
            name: 'VW_SQL_EXECUTIONS',
            description: 'SQL execution telemetry with cost and performance',
            timeWindow: '7d'
          },
          VW_DASHBOARD_OPERATIONS: {
            name: 'VW_DASHBOARD_OPERATIONS',
            description: 'Dashboard lifecycle events',
            timeWindow: 'all'
          },
          VW_SAFESQL_TEMPLATES: {
            name: 'VW_SAFESQL_TEMPLATES',
            description: 'SafeSQL template usage patterns',
            timeWindow: '30d'
          },
          VW_ACTIVITY_SUMMARY: {
            name: 'VW_ACTIVITY_SUMMARY',
            description: 'High-level activity metrics overview',
            timeWindow: '24h'
          }
        },
        procedures: {
          DESTROY_DASHBOARD: {
            name: 'DESTROY_DASHBOARD',
            parameters: ['dashboard_name', 'spec_hash'],
            returns: 'VARCHAR'
          }
        },
        functions: {
          LIST_DASHBOARDS: {
            name: 'LIST_DASHBOARDS',
            returns: 'TABLE'
          }
        }
      },
      ANALYTICS: {
        isDefault: true,
        tables: {
          SCHEMA_VERSION: {
            name: 'SCHEMA_VERSION',
            requiredColumns: [
              'VERSION',
              'APPLIED_AT',
              'APPLIED_BY',
              'DESCRIPTION'
            ]
          }
        }
      }
    };
  }
  
  // Get database name from environment
  getDatabase() {
    return this.database;
  }
  
  // Get warehouse name from environment
  getWarehouse() {
    return this.warehouse;
  }
  
  // Get role name from environment
  getRole() {
    return this.role;
  }
  
  // Get default schema from environment
  getDefaultSchema() {
    return this.defaultSchema;
  }
  
  // Build fully qualified name with optional quoting
  getFQN(schema, table, options = {}) {
    const db = options.database || this.database;
    const useQuotes = options.quote || false;
    
    // Quote function - only quote if needed or requested
    const quote = (identifier) => {
      if (useQuotes || this.needsQuoting(identifier)) {
        return `"${identifier}"`;
      }
      return identifier.toUpperCase();
    };
    
    return `${quote(db)}.${quote(schema)}.${quote(table)}`;
  }
  
  // Get two-part name (assumes database context is set)
  getTwoPartName(schema, table, options = {}) {
    const useQuotes = options.quote || false;
    
    const quote = (identifier) => {
      if (useQuotes || this.needsQuoting(identifier)) {
        return `"${identifier}"`;
      }
      return identifier.toUpperCase();
    };
    
    return `${quote(schema)}.${quote(table)}`;
  }
  
  // Check if identifier needs quoting
  needsQuoting(identifier) {
    // Needs quoting if: mixed case, starts with number, contains special chars
    return /[a-z]/.test(identifier) || 
           /^[0-9]/.test(identifier) ||
           /[^A-Z0-9_]/.test(identifier);
  }
  
  // Get context-setting SQL statements
  getContextSQL(options = {}) {
    const role = options.role || this.role;
    const warehouse = options.warehouse || this.warehouse;
    const db = options.database || this.database;
    const schema = options.schema || this.defaultSchema;
    const queryTag = this.getQueryTag(options);
    
    return [
      role && `USE ROLE ${role}`,
      warehouse && `USE WAREHOUSE ${warehouse}`,
      db && `USE DATABASE ${db}`,
      schema && `USE SCHEMA ${schema}`,
      queryTag && `ALTER SESSION SET QUERY_TAG='${queryTag}'`
    ].filter(Boolean);
  }
  
  // Generate query tag with provenance
  getQueryTag(options = {}) {
    const prefix = process.env.QUERY_TAG_PREFIX || 'ccode';
    const service = options.service || process.env.SERVICE_NAME || 'dashboard-factory';
    const gitSha = process.env.GIT_SHA || process.env.GIT_COMMIT || 'dev';
    const env = process.env.NODE_ENV || 'development';
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    
    return `${prefix}-${service}-${env}-${gitSha.substring(0, 7)}-${timestamp}`;
  }
  
  // Get table reference helper
  getTableRef(schemaName, tableName) {
    const schema = this.schemas[schemaName];
    if (!schema) {
      throw new Error(`Unknown schema: ${schemaName}`);
    }
    
    const table = schema.tables[tableName];
    if (!table) {
      throw new Error(`Unknown table: ${tableName} in schema ${schemaName}`);
    }
    
    return {
      fqn: this.getFQN(schemaName, tableName),
      twoPartName: this.getTwoPartName(schemaName, tableName),
      schema: schemaName,
      table: tableName,
      definition: table
    };
  }
  
  // Validate that a schema/table exists in our config
  isKnownTable(schemaName, tableName) {
    return !!(this.schemas[schemaName]?.tables[tableName]);
  }
  
  // Get all known schemas
  getAllSchemas() {
    return Object.keys(this.schemas);
  }
  
  // Get all tables in a schema
  getTablesInSchema(schemaName) {
    const schema = this.schemas[schemaName];
    return schema ? Object.keys(schema.tables) : [];
  }
  
  // Export configuration for logging/debugging
  exportConfig() {
    return {
      database: this.database,
      warehouse: this.warehouse,
      role: this.role,
      defaultSchema: this.defaultSchema,
      schemas: Object.keys(this.schemas).map(s => ({
        name: s,
        tables: this.getTablesInSchema(s),
        isDefault: this.schemas[s].isDefault || false
      }))
    };
  }

  // Simple FQN helper for common use
  fqn(schema, object) {
    return `${this.database}.${schema}.${object}`;
  }

  // Qualify a source table/view name with proper schema
  qualifySource(source) {
    // Already qualified?
    if (source.includes('.')) return source;
    
    // Known Activity views map to ACTIVITY_CCODE schema
    const activityViews = new Set([
      'VW_ACTIVITY_COUNTS_24H',
      'VW_LLM_TELEMETRY',
      'VW_SQL_EXECUTIONS',
      'VW_DASHBOARD_OPERATIONS',
      'VW_SAFESQL_TEMPLATES',
      'VW_ACTIVITY_SUMMARY'
    ]);
    
    if (activityViews.has(source)) {
      return this.fqn('ACTIVITY_CCODE', source);
    }
    
    // Default to ANALYTICS schema
    return this.fqn(this.defaultSchema, source);
  }
}

// Export singleton instance
module.exports = new SnowflakeSchemaConfig();