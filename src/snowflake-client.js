/**
 * Snowflake Client - Unified connection and query management
 */

const snowflake = require('snowflake-sdk');
require('dotenv').config();

class SnowflakeClient {
  /**
   * Create and connect to Snowflake
   */
  static async connect() {
    const connection = snowflake.createConnection({
      account: process.env.SNOWFLAKE_ACCOUNT,
      username: process.env.SNOWFLAKE_USERNAME,
      password: process.env.SNOWFLAKE_PASSWORD,
      role: process.env.SNOWFLAKE_ROLE,
      warehouse: process.env.SNOWFLAKE_WAREHOUSE,
      database: process.env.SNOWFLAKE_DATABASE || 'CLAUDE_BI',
      schema: process.env.SNOWFLAKE_SCHEMA || 'ANALYTICS'
    });

    return new Promise((resolve, reject) => {
      connection.connect((err, conn) => {
        if (err) {
          reject(new Error(`Snowflake connection failed: ${err.message}`));
        } else {
          // Set session context
          this.setSessionContext(conn);
          resolve(conn);
        }
      });
    });
  }

  /**
   * Set session context (database, schema, warehouse)
   */
  static async setSessionContext(conn) {
    const contextSQL = [
      `USE DATABASE ${process.env.SNOWFLAKE_DATABASE || 'CLAUDE_BI'}`,
      `USE SCHEMA ${process.env.SNOWFLAKE_SCHEMA || 'ANALYTICS'}`,
      `USE WAREHOUSE ${process.env.SNOWFLAKE_WAREHOUSE}`
    ];

    for (const sql of contextSQL) {
      await this.execute(conn, sql);
    }
  }

  /**
   * Execute a SQL query with binds
   */
  static execute(conn, sqlText, binds = []) {
    return new Promise((resolve, reject) => {
      conn.execute({
        sqlText,
        binds,
        complete: (err, stmt, rows) => {
          if (err) {
            reject(err);
          } else {
            resolve({
              rows: rows || [],
              rowCount: stmt.getNumRows(),
              queryId: stmt.getQueryId ? stmt.getQueryId() : null
            });
          }
        }
      });
    });
  }

  /**
   * Execute a SafeSQL template
   */
  static async executeTemplate(conn, template, parameters = {}) {
    // Template validation would happen in schema-contract
    const { sql, binds } = this.buildTemplateQuery(template, parameters);
    return this.execute(conn, sql, binds);
  }

  /**
   * Build query from template
   */
  static buildTemplateQuery(template, parameters) {
    // Simplified template building
    const templates = {
      sample_top: {
        sql: 'SELECT * FROM CLAUDE_BI.ACTIVITY.EVENTS LIMIT ?',
        binds: [parameters.limit || 10]
      },
      time_series: {
        sql: `SELECT DATE_TRUNC('hour', ts) as hour, COUNT(*) as count 
              FROM CLAUDE_BI.ACTIVITY.EVENTS 
              WHERE ts > CURRENT_TIMESTAMP - INTERVAL '? hours'
              GROUP BY hour ORDER BY hour`,
        binds: [parameters.hours || 24]
      },
      top_n: {
        sql: `SELECT customer, COUNT(*) as count 
              FROM CLAUDE_BI.ACTIVITY.EVENTS 
              GROUP BY customer 
              ORDER BY count DESC 
              LIMIT ?`,
        binds: [parameters.limit || 10]
      }
    };

    const tmpl = templates[template];
    if (!tmpl) {
      throw new Error(`Unknown template: ${template}`);
    }

    return tmpl;
  }

  /**
   * Disconnect from Snowflake
   */
  static async disconnect(conn) {
    return new Promise((resolve) => {
      conn.destroy((err) => {
        if (err) {
          console.error('Error disconnecting:', err);
        }
        resolve();
      });
    });
  }

  /**
   * Stream large result sets
   */
  static stream(conn, sqlText, binds = []) {
    return conn.execute({
      sqlText,
      binds,
      streamResult: true
    });
  }
}

module.exports = SnowflakeClient;