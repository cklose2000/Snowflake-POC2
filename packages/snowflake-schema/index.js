// Snowflake Schema Package
// Central export for schema configuration and validation

const config = require('./config');
const SchemaValidator = require('./validator');

module.exports = {
  // Export config singleton
  config,
  
  // Export validator class
  SchemaValidator,
  
  // Convenience methods from config
  getFQN: (schema, table, options) => config.getFQN(schema, table, options),
  getTwoPartName: (schema, table, options) => config.getTwoPartName(schema, table, options),
  getContextSQL: (options) => config.getContextSQL(options),
  getQueryTag: (options) => config.getQueryTag(options),
  getTableRef: (schema, table) => config.getTableRef(schema, table),
  
  // Quick access to common tables
  tables: {
    EVENTS: () => config.getTableRef('ACTIVITY', 'EVENTS'),
    ARTIFACTS: () => config.getTableRef('ACTIVITY_CCODE', 'ARTIFACTS'),
    AUDIT_RESULTS: () => config.getTableRef('ACTIVITY_CCODE', 'AUDIT_RESULTS'),
    SCHEMA_VERSION: () => config.getTableRef('ANALYTICS', 'SCHEMA_VERSION')
  },
  
  // Quick access to Activity views (v1 Dashboard Factory)
  views: {
    ACTIVITY_COUNTS_24H: () => config.getTableRef('ACTIVITY_CCODE', 'VW_ACTIVITY_COUNTS_24H'),
    LLM_TELEMETRY: () => config.getTableRef('ACTIVITY_CCODE', 'VW_LLM_TELEMETRY'),
    SQL_EXECUTIONS: () => config.getTableRef('ACTIVITY_CCODE', 'VW_SQL_EXECUTIONS'),
    DASHBOARD_OPERATIONS: () => config.getTableRef('ACTIVITY_CCODE', 'VW_DASHBOARD_OPERATIONS'),
    SAFESQL_TEMPLATES: () => config.getTableRef('ACTIVITY_CCODE', 'VW_SAFESQL_TEMPLATES'),
    ACTIVITY_SUMMARY: () => config.getTableRef('ACTIVITY_CCODE', 'VW_ACTIVITY_SUMMARY')
  }
};