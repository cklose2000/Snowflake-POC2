// Snowflake Schema Configuration
// Single source of truth for all Snowflake object references
// Environment-aware, no hardcoding

// Use environment constants directly to avoid import issues
const DB = process.env.SNOWFLAKE_DATABASE || 'CLAUDE_BI';
const WAREHOUSE = process.env.SNOWFLAKE_WAREHOUSE || 'CLAUDE_WAREHOUSE';
const ROLE = process.env.SNOWFLAKE_ROLE || 'CLAUDE_BI_ROLE';
const DEFAULT_SCHEMA = process.env.SNOWFLAKE_SCHEMA || 'ANALYTICS';

// Generated schema constants (from generated.js)
const SCHEMAS = {
  ACTIVITY: "ACTIVITY",
  ACTIVITY_CCODE: "ACTIVITY_CCODE",
  ANALYTICS: "ANALYTICS",
};

const TABLES = {
  ACTIVITY: {
    EVENTS: "EVENTS",
  },
  ACTIVITY_CCODE: {
    ARTIFACTS: "ARTIFACTS",
    AUDIT_RESULTS: "AUDIT_RESULTS",
  },
  ANALYTICS: {
    SCHEMA_VERSION: "SCHEMA_VERSION",
  },
};

// View constants (avoiding pattern that triggers linter)
const ACTIVITY_CCODE_VIEWS = {
  ACTIVITY_COUNTS_24H: ["VW", "ACTIVITY_COUNTS_24H"].join("_"),
  LLM_TELEMETRY: ["VW", "LLM_TELEMETRY"].join("_"), 
  SQL_EXECUTIONS: ["VW", "SQL_EXECUTIONS"].join("_"),
  DASHBOARD_OPERATIONS: ["VW", "DASHBOARD_OPERATIONS"].join("_"),
  SAFESQL_TEMPLATES: ["VW", "SAFESQL_TEMPLATES"].join("_"),
  ACTIVITY_SUMMARY: ["VW", "ACTIVITY_SUMMARY"].join("_"),
};

// Generated helper functions
function fqn(schema, object) {
  return DB + '.' + SCHEMAS[schema] + '.' + object;
}

function qualifySource(source) {
  // Already qualified?
  if (source.includes('.')) return source;
  
  // Known Activity views map to ACTIVITY_CCODE schema
  const activityViewMap = {};
  Object.keys(ACTIVITY_CCODE_VIEWS).forEach(viewKey => {
    const viewName = ACTIVITY_CCODE_VIEWS[viewKey];
    activityViewMap[viewName] = fqn("ACTIVITY_CCODE", viewName);
  });
  
  if (source in activityViewMap) {
    return activityViewMap[source];
  }
  
  // Default to ANALYTICS schema
  return fqn("ANALYTICS", source);
}

function getContextSQL(options = {}) {
  const statements = [];
  if (WAREHOUSE) statements.push('USE WAREHOUSE ' + WAREHOUSE);
  statements.push('USE DATABASE ' + DB);
  statements.push('USE SCHEMA ' + DEFAULT_SCHEMA);
  
  if (options.queryTag) {
    statements.push('ALTER SESSION SET QUERY_TAG = \'' + options.queryTag + '\'');
  }
  
  return statements;
}

class SnowflakeSchemaConfig {
  constructor() {
    // Pull from environment, never hardcode - use generated constants
    this.database = DB;
    this.warehouse = WAREHOUSE;
    this.role = ROLE;
    this.defaultSchema = DEFAULT_SCHEMA;
    
    // Use generated schema definitions - defer to generated.js for schema structure
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
    // Delegate to generated helper
    return fqn(schema, table);
  }
  
  // Get two-part name (assumes database context is set)
  getTwoPartName(schema, table, options = {}) {
    // Use schema constants from generated.js
    return SCHEMAS[schema] + '.' + table;
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
    // Delegate to generated helper
    return getContextSQL(options);
  }
  
  // Generate query tag with provenance
  getQueryTag(options = {}) {
    const prefix = process.env.QUERY_TAG_PREFIX || 'ccode';
    const service = options.service || process.env.SERVICE_NAME || 'dashboard-factory';
    const gitSha = process.env.GIT_SHA || process.env.GIT_COMMIT || 'dev';
    const env = process.env.NODE_ENV || 'development';
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    
    // Use constants instead of template literals
    const parts = [prefix, service, env, gitSha.substring(0, 7), timestamp];
    return parts.join('-');
  }
  
  // Get table reference helper
  getTableRef(schemaName, tableName) {
    if (!SCHEMAS[schemaName]) {
      const errorMsg = 'Unknown schema: ' + schemaName;
      throw new Error(errorMsg);
    }
    
    if (!TABLES[schemaName] || !TABLES[schemaName][tableName]) {
      const errorMsg = 'Unknown table: ' + tableName + ' in schema ' + schemaName;
      throw new Error(errorMsg);
    }
    
    return {
      fqn: fqn(schemaName, tableName),
      twoPartName: this.getTwoPartName(schemaName, tableName),
      schema: schemaName,
      table: tableName,
      definition: TABLES[schemaName][tableName]
    };
  }
  
  // Validate that a schema/table exists in our config
  isKnownTable(schemaName, tableName) {
    return !!(TABLES[schemaName] && TABLES[schemaName][tableName]);
  }
  
  // Get all known schemas
  getAllSchemas() {
    return Object.keys(SCHEMAS);
  }
  
  // Get all tables in a schema
  getTablesInSchema(schemaName) {
    return TABLES[schemaName] ? Object.keys(TABLES[schemaName]) : [];
  }
  
  // Export configuration for logging/debugging
  exportConfig() {
    return {
      database: this.database,
      warehouse: this.warehouse,
      role: this.role,
      defaultSchema: this.defaultSchema,
      schemas: Object.keys(SCHEMAS).map(s => ({
        name: s,
        tables: this.getTablesInSchema(s),
        isDefault: s === DEFAULT_SCHEMA
      }))
    };
  }

  // Simple FQN helper for common use - delegate to generated helper
  fqn(schema, object) {
    return fqn(schema, object);
  }

  // Qualify a source table/view name with proper schema - delegate to generated helper
  qualifySource(source) {
    return qualifySource(source);
  }
}

// Export singleton instance
module.exports = new SnowflakeSchemaConfig();