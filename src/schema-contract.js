/**
 * Schema Contract - Contract enforcement and validation
 * Single source of truth for schema references
 */

const crypto = require('crypto');

class SchemaContract {
  constructor() {
    // Contract definition (inline for simplicity)
    this.contract = {
      version: '2.0.0',
      database: 'CLAUDE_BI',
      schemas: {
        ACTIVITY: {
          tables: {
            EVENTS: {
              columns: ['activity_id', 'ts', 'customer', 'activity', 'feature_json']
            }
          }
        },
        ACTIVITY_CCODE: {
          tables: {
            ARTIFACTS: {},
            AUDIT_RESULTS: {}
          },
          views: [
            'VW_ACTIVITY_COUNTS_24H',
            'VW_LLM_TELEMETRY',
            'VW_SQL_EXECUTIONS',
            'VW_DASHBOARD_OPERATIONS',
            'VW_SAFESQL_TEMPLATES',
            'VW_ACTIVITY_SUMMARY'
          ]
        },
        ANALYTICS: {
          tables: {
            SCHEMA_VERSION: {}
          }
        }
      }
    };

    // Generate contract hash
    this.hash = this.generateHash();

    // Valid SafeSQL templates
    this.validTemplates = [
      'sample_top',
      'time_series',
      'top_n',
      'breakdown',
      'comparison'
    ];
  }

  /**
   * Generate contract hash
   */
  generateHash() {
    const content = JSON.stringify(this.contract);
    return crypto.createHash('md5').update(content).digest('hex').substring(0, 16);
  }

  /**
   * Get contract hash
   */
  getHash() {
    return this.hash;
  }

  /**
   * Generate fully qualified name
   */
  fqn(schema, object) {
    return `${this.contract.database}.${schema}.${object}`;
  }

  /**
   * Qualify a source (view or table)
   */
  qualifySource(source) {
    // Check if it's an Activity view
    const activityViews = this.contract.schemas.ACTIVITY_CCODE.views;
    if (activityViews.includes(source)) {
      return this.fqn('ACTIVITY_CCODE', source);
    }
    
    // Check if already qualified
    if (source.includes('.')) {
      return source;
    }
    
    // Default to ANALYTICS schema
    return this.fqn('ANALYTICS', source);
  }

  /**
   * Create activity name with namespace
   */
  createActivityName(name) {
    return `ccode.${name}`;
  }

  /**
   * Validate schema contract against live database
   */
  async validate(conn) {
    const issues = [];

    try {
      // Check database exists
      const dbResult = await this.checkDatabase(conn, this.contract.database);
      if (!dbResult) {
        issues.push('Database not found');
      }

      // Check schemas exist
      for (const schema of Object.keys(this.contract.schemas)) {
        const schemaResult = await this.checkSchema(conn, schema);
        if (!schemaResult) {
          issues.push(`Schema ${schema} not found`);
        }
      }

      // Check critical tables
      const criticalTables = [
        { schema: 'ACTIVITY', table: 'EVENTS' },
        { schema: 'ACTIVITY_CCODE', table: 'ARTIFACTS' }
      ];

      for (const { schema, table } of criticalTables) {
        const tableResult = await this.checkTable(conn, schema, table);
        if (!tableResult) {
          issues.push(`Table ${schema}.${table} not found`);
        }
      }

      if (issues.length > 0) {
        console.warn('⚠️ Schema validation issues:', issues);
        // Continue anyway - graceful degradation
      }

      return { valid: issues.length === 0, issues };

    } catch (error) {
      console.error('Schema validation error:', error);
      // Continue anyway
      return { valid: false, error: error.message };
    }
  }

  /**
   * Check if database exists
   */
  async checkDatabase(conn, database) {
    return new Promise((resolve) => {
      conn.execute({
        sqlText: `SHOW DATABASES LIKE '${database}'`,
        complete: (err, stmt, rows) => {
          resolve(!err && rows && rows.length > 0);
        }
      });
    });
  }

  /**
   * Check if schema exists
   */
  async checkSchema(conn, schema) {
    return new Promise((resolve) => {
      conn.execute({
        sqlText: `SHOW SCHEMAS LIKE '${schema}'`,
        complete: (err, stmt, rows) => {
          resolve(!err && rows && rows.length > 0);
        }
      });
    });
  }

  /**
   * Check if table exists
   */
  async checkTable(conn, schema, table) {
    return new Promise((resolve) => {
      conn.execute({
        sqlText: `SHOW TABLES LIKE '${table}' IN SCHEMA ${schema}`,
        complete: (err, stmt, rows) => {
          resolve(!err && rows && rows.length > 0);
        }
      });
    });
  }

  /**
   * Check if template is valid
   */
  isValidTemplate(template) {
    return this.validTemplates.includes(template);
  }

  /**
   * Get all schemas
   */
  getSchemas() {
    return Object.keys(this.contract.schemas);
  }

  /**
   * Get tables for a schema
   */
  getTables(schema) {
    return Object.keys(this.contract.schemas[schema]?.tables || {});
  }

  /**
   * Get views for a schema
   */
  getViews(schema) {
    return this.contract.schemas[schema]?.views || [];
  }
}

// Export singleton instance
module.exports = new SchemaContract();