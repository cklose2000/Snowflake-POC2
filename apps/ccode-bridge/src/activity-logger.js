// Activity Logger - Folded into Bridge (replaces separate Activity Agent)
import snowflake from 'snowflake-sdk';
import { v4 as uuidv4 } from 'uuid';

export class ActivityLogger {
  constructor() {
    this.connection = null;
    this.queue = [];
    this.batchSize = 100;
    this.flushInterval = 5000; // 5 seconds
    
    this.initializeConnection();
    this.startBatchProcessor();
  }

  async initializeConnection() {
    // CRITICAL: Follow CLAUDE.md guidance exactly
    this.connection = snowflake.createConnection({
      account: process.env.SNOWFLAKE_ACCOUNT,
      username: process.env.SNOWFLAKE_USERNAME,
      password: process.env.SNOWFLAKE_PASSWORD,
      database: process.env.SNOWFLAKE_DATABASE,
      schema: process.env.SNOWFLAKE_SCHEMA,
      warehouse: process.env.SNOWFLAKE_WAREHOUSE,
      role: process.env.SNOWFLAKE_ROLE
    });

    return new Promise((resolve, reject) => {
      this.connection.connect((err, conn) => {
        if (err) {
          console.error('Failed to connect to Snowflake:', err);
          reject(err);
        } else {
          // Set context immediately after connection
          conn.execute({
            sqlText: `USE DATABASE ${process.env.SNOWFLAKE_DATABASE}`,
            complete: () => {
              conn.execute({
                sqlText: `USE SCHEMA ${process.env.SNOWFLAKE_SCHEMA}`,
                complete: () => {
                  console.log('Activity Logger connected to Snowflake');
                  resolve(conn);
                }
              });
            }
          });
        }
      });
    });
  }

  async logEvent({ activity, customer, feature_json, revenue_impact, link, anonymous_customer_id }) {
    const event = {
      activity_id: `act_${uuidv4()}`,
      ts: new Date().toISOString(),
      customer,
      activity,
      feature_json: JSON.stringify(feature_json),
      revenue_impact: revenue_impact || null,
      link: link || null,
      anonymous_customer_id: anonymous_customer_id || null,
      _source_system: 'claude_code',
      _source_version: '1.0.0',
      _session_id: customer // Use customer as session for now
    };

    this.queue.push(event);
    
    if (this.queue.length >= this.batchSize) {
      await this.flush();
    }
  }

  async flush() {
    if (this.queue.length === 0) return;

    const events = this.queue.splice(0, this.batchSize);
    
    try {
      // Insert into Activity Schema base stream
      const sql = `
        INSERT INTO analytics.activity.events (
          activity_id, ts, customer, activity, feature_json,
          revenue_impact, link, anonymous_customer_id,
          _source_system, _source_version, _session_id
        ) VALUES ${events.map(() => '(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)').join(', ')}
      `;

      const values = events.flatMap(event => [
        event.activity_id, event.ts, event.customer, event.activity,
        event.feature_json, event.revenue_impact, event.link,
        event.anonymous_customer_id, event._source_system,
        event._source_version, event._session_id
      ]);

      await this.executeQuery(sql, values);
      console.log(`Logged ${events.length} activities to Activity Schema`);
    } catch (error) {
      console.error('Failed to log activities:', error);
      // Re-queue events for retry
      this.queue.unshift(...events);
    }
  }

  executeQuery(sql, values) {
    return new Promise((resolve, reject) => {
      this.connection.execute({
        sqlText: sql,
        binds: values,
        complete: (err, stmt, rows) => {
          if (err) reject(err);
          else resolve(rows);
        }
      });
    });
  }

  startBatchProcessor() {
    setInterval(async () => {
      await this.flush();
    }, this.flushInterval);
  }
}