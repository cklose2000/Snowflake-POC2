// SafeSQL Template Engine - CommonJS version
const snowflake = require('snowflake-sdk');

class SafeSQLTemplateEngine {
  constructor(connection) {
    this.connection = connection;
    this.templates = {
      describe_table: {
        sql: `SELECT column_name, data_type, is_nullable, column_default
              FROM information_schema.columns 
              WHERE table_schema = ? AND table_name = ?
              ORDER BY ordinal_position`,
        params: ['schema', 'table'],
        maxRows: 1000
      },

      sample_top: {
        // Using fully qualified name directly since IDENTIFIER() with bind doesn't work
        sql: `SELECT * FROM ACTIVITY.EVENTS ORDER BY TS DESC LIMIT ?`,
        params: ['limit'],
        maxRows: 1000,
        allowSelectStar: true,
        preprocessor: (params) => {
          return {
            limit: Math.min(params.n || params.limit || 10, 1000)
          };
        }
      },

      recent_activities: {
        sql: `SELECT activity_id, ts, customer, activity, 
                     TO_VARCHAR(feature_json) as feature_json, _source_system
              FROM ACTIVITY.EVENTS
              WHERE ts > DATEADD('hour', -?, CURRENT_TIMESTAMP())
              ORDER BY ts DESC
              LIMIT ?`,
        params: ['hours', 'limit'],
        maxRows: 1000,
        preprocessor: (params) => {
          return {
            hours: params.hours || 24,
            limit: Math.min(params.limit || 100, 1000)
          };
        }
      },

      activity_by_type: {
        sql: `SELECT activity, COUNT(*) as count, 
                     MIN(ts) as first_seen, MAX(ts) as last_seen
              FROM ACTIVITY.EVENTS
              WHERE ts > DATEADD('hour', -?, CURRENT_TIMESTAMP())
              GROUP BY activity
              ORDER BY count DESC`,
        params: ['hours'],
        maxRows: 1000,
        preprocessor: (params) => {
          return {
            hours: params.hours || 24
          };
        }
      },

      activity_summary: {
        sql: `SELECT 
                COUNT(*) as total_events,
                COUNT(DISTINCT customer) as unique_customers,
                COUNT(DISTINCT activity) as unique_activities,
                MIN(ts) as earliest_event,
                MAX(ts) as latest_event
              FROM ACTIVITY.EVENTS
              WHERE ts > DATEADD('hour', -?, CURRENT_TIMESTAMP())`,
        params: ['hours'],
        maxRows: 1,
        preprocessor: (params) => {
          return {
            hours: params.hours || 24
          };
        }
      }
    };
  }

  async execute(templateName, userParams = {}) {
    const template = this.templates[templateName];
    if (!template) {
      throw new Error(`Unknown template: ${templateName}`);
    }

    // Preprocess parameters if needed
    let processedParams = userParams;
    if (template.preprocessor) {
      processedParams = template.preprocessor(userParams);
    }

    // Build bind array in correct order
    const binds = [];
    for (const param of template.params) {
      if (processedParams[param] === undefined) {
        throw new Error(`Missing required parameter: ${param}`);
      }
      binds.push(processedParams[param]);
    }

    // Log activity
    await this.logActivity(templateName, processedParams);

    // Execute query
    return new Promise((resolve, reject) => {
      this.connection.execute({
        sqlText: template.sql,
        binds: binds,
        complete: (err, stmt, rows) => {
          if (err) {
            this.logError(templateName, err);
            reject(err);
          } else {
            // Check row limit
            if (rows && rows.length > template.maxRows) {
              rows = rows.slice(0, template.maxRows);
            }
            
            this.logSuccess(templateName, rows.length);
            resolve({
              template: templateName,
              rows: rows,
              count: rows.length,
              metadata: {
                maxRows: template.maxRows,
                truncated: rows.length === template.maxRows
              }
            });
          }
        }
      });
    });
  }

  async logActivity(template, params) {
    const activityId = `act_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
    const sql = `INSERT INTO ACTIVITY.EVENTS (
      activity_id, ts, customer, activity, feature_json, _source_system
    ) SELECT
      ?,
      CURRENT_TIMESTAMP(),
      ?,
      'ccode.sql_executed',
      PARSE_JSON(?),
      'safesql_engine'`;

    const binds = [
      activityId,
      params.customer || 'safesql_engine',
      JSON.stringify({
        template: template,
        params: params,
        timestamp: new Date().toISOString()
      })
    ];

    return new Promise((resolve) => {
      this.connection.execute({
        sqlText: sql,
        binds: binds,
        complete: (err) => {
          if (err) console.error('Activity logging failed:', err.message);
          resolve(); // Don't fail the main query if logging fails
        }
      });
    });
  }

  async logSuccess(template, rowCount) {
    console.log(`✅ Template ${template} executed successfully, returned ${rowCount} rows`);
  }

  async logError(template, error) {
    console.error(`❌ Template ${template} failed:`, error.message);
  }

  getTemplateList() {
    return Object.keys(this.templates).map(name => ({
      name,
      params: this.templates[name].params,
      maxRows: this.templates[name].maxRows,
      allowSelectStar: this.templates[name].allowSelectStar || false
    }));
  }

  validateTemplate(templateName, params) {
    const template = this.templates[templateName];
    if (!template) {
      return { valid: false, error: `Unknown template: ${templateName}` };
    }

    try {
      const processedParams = template.preprocessor ? template.preprocessor(params) : params;
      const missingParams = template.params.filter(p => processedParams[p] === undefined);
      
      if (missingParams.length > 0) {
        return { valid: false, error: `Missing parameters: ${missingParams.join(', ')}` };
      }

      return { valid: true };
    } catch (error) {
      return { valid: false, error: error.message };
    }
  }
}

module.exports = SafeSQLTemplateEngine;