// Activity Schema 2.0 Client Logger
import { v4 as uuidv4 } from 'uuid';

export class ActivityLogger {
  constructor(connection, config = {}) {
    this.connection = connection;
    this.batchSize = config.batchSize || 100;
    this.flushInterval = config.flushInterval || 5000;
    this.queue = [];
    this.customer = config.customer || 'system';
    this.sourceSystem = config.sourceSystem || 'claude_code';
    this.sourceVersion = config.sourceVersion || '1.0.0';
    
    this.startBatchProcessor();
  }

  async logActivity({
    activity,
    feature_json = {},
    revenue_impact = null,
    link = null,
    anonymous_customer_id = null,
    customer = null
  }) {
    const event = {
      activity_id: `act_${uuidv4()}`,
      ts: new Date().toISOString(),
      customer: customer || this.customer,
      activity: this.ensureNamespace(activity),
      feature_json: JSON.stringify(feature_json),
      revenue_impact,
      link,
      anonymous_customer_id,
      _source_system: this.sourceSystem,
      _source_version: this.sourceVersion,
      _session_id: this.getSessionId(),
      _query_tag: this.getQueryTag(),
      _activity_occurrence: await this.getActivityOccurrence(activity, customer || this.customer)
    };

    this.queue.push(event);
    
    if (this.queue.length >= this.batchSize) {
      await this.flush();
    }

    return event.activity_id;
  }

  ensureNamespace(activity) {
    // Ensure all activities are properly namespaced
    if (!activity.includes('.')) {
      return `ccode.${activity}`;
    }
    return activity;
  }

  getSessionId() {
    // Get or create session ID
    if (!this.sessionId) {
      this.sessionId = `session_${uuidv4()}`;
    }
    return this.sessionId;
  }

  getQueryTag() {
    // Generate query tag for correlation
    return `ccode_${Date.now()}`;
  }

  async getActivityOccurrence(activity, customer) {
    // Calculate which occurrence this is for the customer
    const sql = `
      SELECT COUNT(*) + 1 as occurrence
      FROM analytics.activity.events
      WHERE customer = ?
        AND activity = ?
    `;
    
    try {
      const result = await this.executeQuery(sql, [customer, activity]);
      return result[0]?.OCCURRENCE || 1;
    } catch (error) {
      console.error('Failed to get activity occurrence:', error);
      return 1;
    }
  }

  async flush() {
    if (this.queue.length === 0) return;

    const events = this.queue.splice(0, this.batchSize);
    
    try {
      const sql = `
        INSERT INTO analytics.activity.events (
          activity_id, ts, customer, activity, feature_json,
          revenue_impact, link, anonymous_customer_id,
          _source_system, _source_version, _session_id,
          _query_tag, _activity_occurrence
        ) VALUES ${events.map(() => '(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)').join(', ')}
      `;

      const values = events.flatMap(event => [
        event.activity_id, event.ts, event.customer, event.activity,
        event.feature_json, event.revenue_impact, event.link,
        event.anonymous_customer_id, event._source_system,
        event._source_version, event._session_id,
        event._query_tag, event._activity_occurrence
      ]);

      await this.executeQuery(sql, values);
      console.log(`Flushed ${events.length} activities to Activity Schema 2.0`);
    } catch (error) {
      console.error('Failed to flush activities:', error);
      // Re-queue events for retry
      this.queue.unshift(...events);
    }
  }

  executeQuery(sql, params = []) {
    return new Promise((resolve, reject) => {
      this.connection.execute({
        sqlText: sql,
        binds: params,
        complete: (err, stmt, rows) => {
          if (err) reject(err);
          else resolve(rows);
        }
      });
    });
  }

  startBatchProcessor() {
    this.flushTimer = setInterval(async () => {
      await this.flush();
    }, this.flushInterval);
  }

  async stop() {
    if (this.flushTimer) {
      clearInterval(this.flushTimer);
    }
    await this.flush();
  }
}

// Standard activity types for Activity Schema 2.0
export const ActivityTypes = {
  // User interactions
  USER_ASKED: 'ccode.user_asked',
  USER_FEEDBACK: 'ccode.user_feedback',
  
  // SQL operations
  SQL_EXECUTED: 'ccode.sql_executed',
  SQL_FAILED: 'ccode.sql_failed',
  
  // Artifacts
  ARTIFACT_CREATED: 'ccode.artifact_created',
  ARTIFACT_RETRIEVED: 'ccode.artifact_retrieved',
  
  // Auditing
  AUDIT_PASSED: 'ccode.audit_passed',
  AUDIT_FAILED: 'ccode.audit_failed',
  
  // System events
  BRIDGE_STARTED: 'ccode.bridge_started',
  BRIDGE_STOPPED: 'ccode.bridge_stopped',
  AGENT_INVOKED: 'ccode.agent_invoked',
  AGENT_COMPLETED: 'ccode.agent_completed',
  
  // Errors
  ERROR_OCCURRED: 'ccode.error_occurred',
  ERROR_RECOVERED: 'ccode.error_recovered'
};