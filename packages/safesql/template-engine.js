// SafeSQL Template Engine - Core template processing
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
        sql: `SELECT * FROM IDENTIFIER(?) LIMIT ?`,
        params: ['table_full', 'limit'],
        maxRows: 1000,
        allowSelectStar: true,
        preprocessor: (params) => {
          return {
            table_full: `${params.schema}.${params.table}`,
            limit: Math.min(params.n || 10, 1000)
          };
        }
      },

      top_n: {
        sql: `SELECT ??, COUNT(*) as count_value, SUM(??) as sum_value
              FROM IDENTIFIER(?)
              WHERE ?? >= ? AND ?? <= ?
              GROUP BY ??
              ORDER BY count_value DESC
              LIMIT ?`,
        params: ['dimension', 'metric', 'table_full', 'date_column', 'start_date', 'date_column', 'end_date', 'dimension', 'limit'],
        maxRows: 100,
        preprocessor: (params) => {
          return {
            ...params,
            table_full: `${params.schema}.${params.table}`,
            limit: Math.min(params.n || 10, 100)
          };
        }
      },

      time_series: {
        sql: `SELECT DATE_TRUNC(?, ??) as time_period,
                     COUNT(*) as count_value,
                     SUM(??) as sum_value,
                     AVG(??) as avg_value
              FROM IDENTIFIER(?)
              WHERE ?? >= ? AND ?? <= ?
              GROUP BY time_period
              ORDER BY time_period`,
        params: ['grain', 'date_column', 'metric', 'metric', 'table_full', 'date_column', 'start_date', 'date_column', 'end_date'],
        maxRows: 1000,
        preprocessor: (params) => {
          const validGrains = ['hour', 'day', 'week', 'month', 'quarter', 'year'];
          if (!validGrains.includes(params.grain)) {
            throw new Error(`Invalid grain. Must be one of: ${validGrains.join(', ')}`);
          }
          return {
            ...params,
            table_full: `${params.schema}.${params.table}`
          };
        }
      },

      breakdown: {
        sql: `SELECT ??, COUNT(*) as count_value, SUM(??) as sum_value
              FROM IDENTIFIER(?)
              WHERE ?? >= ? AND ?? <= ?
              GROUP BY ??
              ORDER BY sum_value DESC
              LIMIT ?`,
        params: ['dimensions', 'metric', 'table_full', 'date_column', 'start_date', 'date_column', 'end_date', 'dimensions', 'limit'],
        maxRows: 1000,
        preprocessor: (params) => {
          return {
            ...params,
            table_full: `${params.schema}.${params.table}`,
            limit: params.limit || 100
          };
        }
      },

      comparison: {
        sql: `WITH period_a AS (
                SELECT SUM(??) as metric_a
                FROM IDENTIFIER(?)
                WHERE ?? >= ? AND ?? <= ?
              ),
              period_b AS (
                SELECT SUM(??) as metric_b
                FROM IDENTIFIER(?)
                WHERE ?? >= ? AND ?? <= ?
              )
              SELECT 
                (SELECT metric_a FROM period_a) as period_a_value,
                (SELECT metric_b FROM period_b) as period_b_value,
                ((SELECT metric_b FROM period_b) - (SELECT metric_a FROM period_a)) as difference,
                (((SELECT metric_b FROM period_b) - (SELECT metric_a FROM period_a)) / NULLIF((SELECT metric_a FROM period_a), 0)) * 100 as percent_change`,
        params: ['metric', 'table_full', 'date_column', 'start_date_a', 'date_column', 'end_date_a',
                 'metric', 'table_full', 'date_column', 'start_date_b', 'date_column', 'end_date_b'],
        maxRows: 1,
        preprocessor: (params) => {
          return {
            ...params,
            table_full: `${params.schema}.${params.table}`
          };
        }
      },

      // Activity-specific templates
      recent_activities: {
        sql: `SELECT activity_id, ts, customer, activity, 
                     TO_VARCHAR(feature_json) as feature_json, _source_system
              FROM ACTIVITY.EVENTS
              WHERE ts > CURRENT_TIMESTAMP - INTERVAL '? hours'
              ORDER BY ts DESC
              LIMIT ?`,
        params: ['hours', 'limit'],
        maxRows: 1000,
        preprocessor: (params) => {
          return {
            hours: params.hours || 24,
            limit: params.limit || 100
          };
        }
      },

      activity_by_type: {
        sql: `SELECT activity, COUNT(*) as count, 
                     MIN(ts) as first_seen, MAX(ts) as last_seen
              FROM ACTIVITY.EVENTS
              WHERE ts > CURRENT_TIMESTAMP - INTERVAL '? hours'
              GROUP BY activity
              ORDER BY count DESC`,
        params: ['hours'],
        maxRows: 1000,
        preprocessor: (params) => {
          return {
            hours: params.hours || 24
          };
        }
      }
    };
  }

  async execute(templateName, userParams) {
    const template = this.templates[templateName];
    if (!template) {
      throw new Error(`Unknown template: ${templateName}`);
    }

    // Preprocess parameters if needed
    let processedParams = userParams;
    if (template.preprocessor) {
      processedParams = template.preprocessor(userParams);
    }

    // Validate required parameters
    const missingParams = [];
    for (const param of template.params) {
      if (processedParams[param] === undefined) {
        // Check if it's a duplicate param (like date_column appearing multiple times)
        const baseParam = param.replace(/_a$|_b$/, '');
        if (!processedParams[baseParam]) {
          missingParams.push(param);
        }
      }
    }

    if (missingParams.length > 0) {
      throw new Error(`Missing required parameters: ${missingParams.join(', ')}`);
    }

    // Build bind array in correct order
    const binds = template.params.map(param => {
      // Handle duplicate params
      const baseParam = param.replace(/_a$|_b$/, '');
      return processedParams[param] || processedParams[baseParam];
    });

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
      '${activityId}',
      CURRENT_TIMESTAMP(),
      '${params.customer || 'safesql_engine'}',
      'ccode.sql_executed',
      PARSE_JSON('${JSON.stringify({
        template: template,
        params: params,
        timestamp: new Date().toISOString()
      })}'),
      'safesql_engine'`;

    return new Promise((resolve) => {
      this.connection.execute({
        sqlText: sql,
        complete: (err) => {
          if (err) console.error('Activity logging failed:', err);
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
      const missingParams = template.params.filter(p => !processedParams[p] && !processedParams[p.replace(/_a$|_b$/, '')]);
      
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