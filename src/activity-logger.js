/**
 * Activity Logger - Unified Activity Schema v2.0 logging
 */

const SchemaContract = require('./schema-contract');

class ActivityLogger {
  constructor(snowflakeConn) {
    this.conn = snowflakeConn;
    this.eventsTable = SchemaContract.fqn('ACTIVITY', 'EVENTS');
    this.artifactsTable = SchemaContract.fqn('ACTIVITY_CCODE', 'ARTIFACTS');
    this.customer = process.env.ACTIVITY_CUSTOMER || 'default_user';
  }

  /**
   * Log an activity event
   */
  async log(activity, featureJson = {}) {
    const activityId = this.generateActivityId();
    const activityName = SchemaContract.createActivityName(activity);
    
    const sql = `
      INSERT INTO ${this.eventsTable} (
        activity_id, ts, customer, activity, feature_json,
        _source_system, _source_version, _session_id
      )
      SELECT ?, CURRENT_TIMESTAMP, ?, ?, PARSE_JSON(?), ?, ?, ?
    `;

    const binds = [
      activityId,
      this.customer,
      activityName,
      JSON.stringify(featureJson),
      'snowflake-poc2',
      '2.0.0',
      this.getSessionId()
    ];

    try {
      await this.execute(sql, binds);
      return activityId;
    } catch (error) {
      console.error('Activity logging failed:', error);
      // Non-blocking - don't fail the operation
      return null;
    }
  }

  /**
   * Log artifact creation
   */
  async logArtifact(artifactId, data, metadata = {}) {
    const sql = `
      INSERT INTO ${this.artifactsTable} (
        artifact_id, sample, row_count, schema_json, 
        created_ts, customer, bytes
      )
      SELECT ?, PARSE_JSON(?), ?, PARSE_JSON(?), 
              CURRENT_TIMESTAMP, ?, ?
    `;

    const sample = Array.isArray(data) ? data.slice(0, 10) : data;
    const binds = [
      artifactId,
      JSON.stringify(sample),
      metadata.rowCount || 0,
      JSON.stringify(metadata.schema || {}),
      this.customer,
      JSON.stringify(data).length
    ];

    try {
      await this.execute(sql, binds);
      
      // Log activity for artifact creation
      await this.log('artifact_created', {
        artifact_id: artifactId,
        row_count: metadata.rowCount,
        bytes: binds[5]
      });
      
      return artifactId;
    } catch (error) {
      console.error('Artifact logging failed:', error);
      return null;
    }
  }

  /**
   * Query recent activities
   */
  async queryRecent(limit = 100) {
    const sql = `
      SELECT activity_id, ts, customer, activity, feature_json
      FROM ${this.eventsTable}
      WHERE customer = ?
      ORDER BY ts DESC
      LIMIT ?
    `;

    const binds = [this.customer, limit];
    
    try {
      const result = await this.execute(sql, binds);
      return result.rows;
    } catch (error) {
      console.error('Activity query failed:', error);
      return [];
    }
  }

  /**
   * Get activity metrics
   */
  async getMetrics(hours = 24) {
    const sql = `
      SELECT 
        COUNT(*) as total_events,
        COUNT(DISTINCT activity) as unique_activities,
        COUNT(DISTINCT customer) as unique_customers,
        MIN(ts) as earliest,
        MAX(ts) as latest
      FROM ${this.eventsTable}
      WHERE ts > CURRENT_TIMESTAMP - INTERVAL '? hours'
    `;

    const binds = [hours];
    
    try {
      const result = await this.execute(sql, binds);
      return result.rows[0];
    } catch (error) {
      console.error('Metrics query failed:', error);
      return null;
    }
  }

  /**
   * Execute SQL with error handling
   */
  execute(sql, binds) {
    return new Promise((resolve, reject) => {
      this.conn.execute({
        sqlText: sql,
        binds,
        complete: (err, stmt, rows) => {
          if (err) {
            reject(err);
          } else {
            resolve({ rows, stmt });
          }
        }
      });
    });
  }

  /**
   * Generate activity ID
   */
  generateActivityId() {
    const timestamp = Date.now();
    const random = Math.random().toString(36).substring(2, 8);
    return `act_${timestamp}_${random}`;
  }

  /**
   * Get or create session ID
   */
  getSessionId() {
    if (!this.sessionId) {
      this.sessionId = `session_${Date.now()}_${Math.random().toString(36).substring(2, 8)}`;
    }
    return this.sessionId;
  }

  /**
   * Create a new session
   */
  newSession() {
    this.sessionId = null;
    return this.getSessionId();
  }
}

module.exports = ActivityLogger;