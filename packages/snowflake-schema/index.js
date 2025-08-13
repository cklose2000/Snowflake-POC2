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
  }
};