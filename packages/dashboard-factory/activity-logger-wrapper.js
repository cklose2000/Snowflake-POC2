// Activity Logger Wrapper for Dashboard Factory
// Ensures proper Activity Schema v2.0 compliance for dashboard operations

const snowflake = require('snowflake-sdk');
const crypto = require('crypto');
const { 
  fqn, 
  SCHEMAS, 
  TABLES, 
  ACTIVITY_NAMESPACE,
  createActivityName 
} = require('../snowflake-schema/generated.js');

class DashboardActivityLogger {
  constructor(snowflakeConnection) {
    this.snowflake = snowflakeConnection;
    this.namespace = ACTIVITY_NAMESPACE;
    this.sourceSystem = 'dashboard_factory';
    this.sourceVersion = '1.0.0';
    
    // Cache the fully qualified table name
    this.eventsTable = fqn(SCHEMAS.ACTIVITY, TABLES.ACTIVITY.EVENTS);
  }

  // Generate unique activity ID
  generateActivityId(prefix = 'act') {
    return `${prefix}_${crypto.randomBytes(8).toString('hex')}`;
  }

  // Log dashboard creation started
  async logDashboardCreationStarted(spec, conversationContext) {
    const activityId = this.generateActivityId('dash_create');
    
    const featureJson = {
      spec_id: spec.name,
      spec_hash: this.generateSpecHash(spec),
      panels: spec.panels.length,
      schedule: spec.schedule.mode,
      timezone: spec.timezone,
      conversation_length: conversationContext?.messages || 0,
      intent_confidence: conversationContext?.confidence || 0
    };

    return await this.logActivity({
      activityId,
      activity: createActivityName('dashboard_creation_started'),
      customer: this.getCurrentUser(),
      featureJson,
      link: null
    });
  }

  // Log dashboard creation completed
  async logDashboardCreated(spec, results, streamlitUrl) {
    const activityId = this.generateActivityId('dash_complete');
    
    const featureJson = {
      spec_id: spec.name,
      spec_hash: this.generateSpecHash(spec),
      panels: spec.panels.length,
      schedule: spec.schedule.mode,
      objects_created: results.objectsCreated,
      views_created: results.views?.length || 0,
      tasks_created: results.tasks?.length || 0,
      dynamic_tables_created: results.dynamicTables?.length || 0,
      creation_time_ms: results.creationTimeMs,
      streamlit_url: streamlitUrl
    };

    return await this.logActivity({
      activityId,
      activity: createActivityName('dashboard_created'),
      customer: this.getCurrentUser(),
      featureJson,
      link: streamlitUrl
    });
  }

  // Log dashboard refresh
  async logDashboardRefreshed(dashboardName, specHash) {
    const activityId = this.generateActivityId('dash_refresh');
    
    const featureJson = {
      dashboard_name: dashboardName,
      spec_hash: specHash,
      refresh_type: 'manual',
      refreshed_at: new Date().toISOString()
    };

    return await this.logActivity({
      activityId,
      activity: createActivityName('dashboard_refreshed'),
      customer: this.getCurrentUser(),
      featureJson,
      link: null
    });
  }

  // Log dashboard destruction
  async logDashboardDestroyed(dashboardName, specHash, objectsDropped) {
    const activityId = this.generateActivityId('dash_destroy');
    
    const featureJson = {
      dashboard_name: dashboardName,
      spec_hash: specHash,
      objects_dropped: objectsDropped,
      destroyed_at: new Date().toISOString()
    };

    return await this.logActivity({
      activityId,
      activity: createActivityName('dashboard_destroyed'),
      customer: this.getCurrentUser(),
      featureJson,
      link: null
    });
  }

  // Log dashboard failure
  async logDashboardFailed(spec, error, phase) {
    const activityId = this.generateActivityId('dash_fail');
    
    const featureJson = {
      spec_id: spec?.name || 'unknown',
      spec_hash: spec ? this.generateSpecHash(spec) : null,
      error_message: error.message,
      error_code: error.code,
      failure_phase: phase,
      stack_trace: error.stack?.substring(0, 500) // Truncate for storage
    };

    return await this.logActivity({
      activityId,
      activity: createActivityName('dashboard_failed'),
      customer: this.getCurrentUser(),
      featureJson,
      link: null
    });
  }

  // Log preflight check results
  async logPreflightResults(spec, results) {
    const activityId = this.generateActivityId('preflight');
    
    const featureJson = {
      spec_id: spec.name,
      spec_hash: this.generateSpecHash(spec),
      passed: results.passed,
      issues: results.issues,
      warnings: results.warnings,
      cost_estimate: results.cost_estimate,
      estimated_objects: results.estimated_objects,
      activity_data_available: results.checks?.activity_data?.event_count > 0
    };

    return await this.logActivity({
      activityId,
      activity: createActivityName(`dashboard_preflight_${results.passed ? 'passed' : 'failed'}`),
      customer: this.getCurrentUser(),
      featureJson,
      link: null
    });
  }

  // Generic event logging method (for compatibility)
  async logEvent({ activity, customer, ts, link, feature_json, source_system, source_version, query_tag }) {
    const activityId = this.generateActivityId('event');
    
    // Map old format to new format
    return await this.logActivity({
      activityId,
      activity: activity || createActivityName('dashboard_event'),
      customer: customer || this.getCurrentUser(),
      featureJson: feature_json || {},
      link: link || null
    });
  }

  // Core activity logging method - SAFE with parameter binds
  async logActivity({ activityId, activity, customer, featureJson, link = null }) {
    // Convert featureJson to string for VARIANT column
    const featureJsonStr = typeof featureJson === 'string' 
      ? featureJson 
      : JSON.stringify(featureJson || {});
    
    // Use positional parameter binds (? placeholders) for Snowflake SDK
    const sql = `
      INSERT INTO ${this.eventsTable} (
        activity_id,
        ts,
        customer,
        activity,
        feature_json,
        link,
        _source_system,
        _source_version,
        _session_id,
        _query_tag
      ) 
      SELECT
        ?,
        CURRENT_TIMESTAMP(),
        ?,
        ?,
        PARSE_JSON(?),
        ?,
        ?,
        ?,
        ?,
        CURRENT_QUERY_TAG()
    `;

    // Snowflake SDK requires positional binds as an array
    const binds = [
      activityId,           // ?1
      customer,             // ?2
      activity,             // ?3
      featureJsonStr,       // ?4
      link,                 // ?5
      this.sourceSystem,    // ?6
      this.sourceVersion,   // ?7
      this.getSessionId()   // ?8
    ];

    try {
      const result = await new Promise((resolve, reject) => {
        this.snowflake.execute({
          sqlText: sql,
          binds: binds,
          complete: (err, stmt) => {
            if (err) {
              console.error(`Failed to log activity ${activity}:`, err.message);
              reject(err);
            } else {
              console.log(`✅ Logged activity: ${activity} (${activityId})`);
              // Return statement for query_id capture
              resolve(stmt);
            }
          }
        });
      });

      // Capture query_id if available
      if (result && result.getQueryId) {
        const queryId = result.getQueryId();
        if (queryId && activity === 'ccode.sql_executed') {
          // Store query_id for later correlation
          this.lastQueryId = queryId;
        }
      }

      return activityId;
    } catch (error) {
      console.error('Activity logging error:', error);
      // Don't fail dashboard creation due to logging errors
      return null;
    }
  }

  // Get current user from connection or environment
  getCurrentUser() {
    return process.env.SNOWFLAKE_USERNAME || 'dashboard_factory_user';
  }

  // Get or generate session ID
  getSessionId() {
    if (!this.sessionId) {
      this.sessionId = `session_${crypto.randomBytes(8).toString('hex')}`;
    }
    return this.sessionId;
  }

  // Generate deterministic hash for spec (for idempotent operations)
  generateSpecHash(spec) {
    const specString = JSON.stringify({
      name: spec.name,
      panels: spec.panels.map(p => ({
        id: p.id,
        type: p.type,
        source: p.source,
        x: p.x,
        y: p.y,
        metric: p.metric
      })),
      schedule: spec.schedule
    });
    
    return crypto
      .createHash('sha256')
      .update(specString)
      .digest('hex')
      .substring(0, 8);
  }

  // Query recent dashboard activities
  async queryRecentActivities(dashboardName = null, limit = 100) {
    let sql = `
      SELECT 
        activity_id,
        ts,
        customer,
        activity,
        feature_json,
        link
      FROM ${this.eventsTable}
      WHERE activity LIKE ?
    `;
    
    const binds = [this.namespace + '.dashboard%'];

    if (dashboardName) {
      sql += ` AND feature_json:spec_id = ?`;
      binds.push(dashboardName);
    }

    sql += ` ORDER BY ts DESC LIMIT ?`;
    binds.push(limit);

    return new Promise((resolve, reject) => {
      this.snowflake.execute({
        sqlText: sql,
        binds: binds,
        complete: (err, stmt, rows) => {
          if (err) {
            reject(err);
          } else {
            resolve(rows);
          }
        }
      });
    });
  }

  // Get dashboard metrics from Activity stream
  async getDashboardMetrics(timeWindow = '24 hours') {
    const hours = parseInt(timeWindow.replace(/\D/g, '')) || 24;
    
    const sql = `
      SELECT 
        COUNT(DISTINCT feature_json:spec_id) as unique_dashboards,
        COUNT(CASE WHEN activity = ? THEN 1 END) as created_count,
        COUNT(CASE WHEN activity = ? THEN 1 END) as refresh_count,
        COUNT(CASE WHEN activity = ? THEN 1 END) as destroy_count,
        COUNT(CASE WHEN activity = ? THEN 1 END) as failure_count,
        AVG(feature_json:creation_time_ms) as avg_creation_time_ms,
        SUM(feature_json:objects_created) as total_objects_created
      FROM ${this.eventsTable}
      WHERE activity LIKE ?
        AND ts >= DATEADD('hour', ?, CURRENT_TIMESTAMP())
    `;

    const binds = [
      this.namespace + '.dashboard_created',
      this.namespace + '.dashboard_refreshed', 
      this.namespace + '.dashboard_destroyed',
      this.namespace + '.dashboard_failed',
      this.namespace + '.dashboard%',
      -hours
    ];

    return new Promise((resolve, reject) => {
      this.snowflake.execute({
        sqlText: sql,
        binds: binds,
        complete: (err, stmt, rows) => {
          if (err) {
            reject(err);
          } else {
            resolve(rows[0]);
          }
        }
      });
    });
  }
}

module.exports = DashboardActivityLogger;