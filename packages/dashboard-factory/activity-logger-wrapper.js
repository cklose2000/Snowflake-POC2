// Activity Logger Wrapper - Bridges CommonJS Dashboard Factory to ES Module Activity Schema
// Provides strict Activity Schema v2.0 compliance for Dashboard Factory

const schema = require('../snowflake-schema');

class ActivityLoggerWrapper {
  constructor(snowflakeConnection) {
    this.snowflake = snowflakeConnection;
    this.version = '2.0.0';
    this.sourceSystem = 'claude_code';
    this.sourceVersion = 'dashboard_factory_v1.0.0';
    
    // Dashboard Factory specific activity types
    this.activityTypes = {
      'ccode.dashboard_intent': 'Dashboard intent detected in conversation',
      'ccode.dashboard_spec_created': 'Dashboard specification created from intent', 
      'ccode.dashboard_created': 'Dashboard successfully created and deployed',
      'ccode.dashboard_refreshed': 'Dashboard data refreshed',
      'ccode.dashboard_failed': 'Dashboard operation failed',
      'ccode.dashboard_destroyed': 'Dashboard and all objects removed'
    };
  }

  // Main activity logging method with strict schema compliance
  async logEvent(eventData) {
    try {
      const activityRecord = await this.buildActivityRecord(eventData);
      await this.insertActivity(activityRecord);
      return activityRecord.activity_id;
    } catch (error) {
      console.error(`❌ Activity logging failed: ${error.message}`);
      // Don't throw - activity logging should be non-blocking
      return null;
    }
  }

  // Build complete activity record with all 14 columns
  async buildActivityRecord(eventData) {
    const activityId = this.generateActivityId();
    const timestamp = new Date().toISOString();
    
    // Validate required fields
    if (!eventData.activity || !eventData.customer) {
      throw new Error('Activity and customer are required fields');
    }
    
    // Get activity occurrence count
    const occurrence = await this.getActivityOccurrence(
      eventData.customer, 
      eventData.activity
    );
    
    // Build complete record with all 14 columns per Activity Schema v2.0
    const record = {
      // 5 Core columns (required)
      activity_id: activityId,
      ts: timestamp,
      customer: eventData.customer,
      activity: eventData.activity,
      feature_json: JSON.stringify(eventData.feature_json || {}),
      
      // 3 Optional columns 
      anonymous_customer_id: eventData.anonymous_customer_id || null,
      revenue_impact: eventData.revenue_impact || null,
      link: eventData.link || null,
      
      // 6 System extension columns (always populated)
      _source_system: this.sourceSystem,
      _source_version: this.sourceVersion,
      _session_id: eventData.session_id || 'dashboard_factory',
      _query_tag: this.generateQueryTag(eventData),
      _activity_occurrence: occurrence.count,
      _activity_repeated_at: occurrence.previous_ts
    };
    
    return record;
  }

  // Generate unique activity ID
  generateActivityId() {
    const timestamp = Date.now();
    const random = Math.floor(Math.random() * 1000).toString().padStart(3, '0');
    return `act_${timestamp}_${random}`;
  }

  // Generate query tag for correlation
  generateQueryTag(eventData) {
    const baseTag = process.env.QUERY_TAG_PREFIX || 'ccode-dash';
    const operation = eventData.activity.split('.')[1] || 'unknown';
    const timestamp = Math.floor(Date.now() / 1000);
    
    return `${baseTag}_${operation}_${timestamp}`;
  }

  // Get activity occurrence count for customer  
  async getActivityOccurrence(customer, activity) {
    try {
      const query = `
        SELECT 
          COUNT(*) as occurrence_count,
          MAX(ts) as previous_ts
        FROM ACTIVITY.EVENTS
        WHERE customer = ? AND activity = ?
      `;
      
      const result = await this.executeSQL(query, [customer, activity]);
      const row = result.rows?.[0];
      const count = (row?.OCCURRENCE_COUNT || 0) + 1;
      const previousTs = row?.PREVIOUS_TS || null;
      
      return {
        count: count,
        previous_ts: previousTs
      };
      
    } catch (error) {
      console.log(`⚠️ Could not get occurrence count: ${error.message}`);
      return { count: 1, previous_ts: null };
    }
  }

  // Insert activity record into Snowflake
  async insertActivity(record) {
    const insertSQL = `
      INSERT INTO ACTIVITY.EVENTS (
        activity_id, ts, customer, activity, feature_json,
        anonymous_customer_id, revenue_impact, link,
        _source_system, _source_version, _session_id, _query_tag,
        _activity_occurrence, _activity_repeated_at
      ) VALUES (?, ?, ?, ?, PARSE_JSON(?), ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `;
    
    const binds = [
      record.activity_id,
      record.ts,
      record.customer,
      record.activity,
      record.feature_json,
      record.anonymous_customer_id,
      record.revenue_impact,
      record.link,
      record._source_system,
      record._source_version,
      record._session_id,
      record._query_tag,
      record._activity_occurrence,
      record._activity_repeated_at
    ];
    
    await this.executeSQL(insertSQL, binds);
    console.log(`✅ Activity logged: ${record.activity_id} (${record.activity})`);
  }

  // Dashboard-specific logging methods

  async logDashboardStep(step, creationId, customerID, metadata = {}) {
    const activityName = `ccode.dashboard_${step}`;
    
    await this.logEvent({
      activity: activityName,
      customer: customerID,
      session_id: creationId,
      feature_json: {
        creation_id: creationId,
        step: step,
        step_timestamp: new Date().toISOString(),
        ...metadata
      },
      link: metadata.dashboard_url || null,
      revenue_impact: metadata.estimated_cost || null
    });
  }

  async logDashboardCreated(dashboardDetails, customerID, sessionID) {
    await this.logEvent({
      activity: 'ccode.dashboard_created',
      customer: customerID,
      session_id: sessionID,
      link: dashboardDetails.url,
      feature_json: {
        dashboard_name: dashboardDetails.name,
        spec_id: dashboardDetails.specId,
        panels_count: dashboardDetails.panelsCount,
        creation_time_ms: dashboardDetails.creationTimeMs,
        objects_created: dashboardDetails.objectsCreated,
        refresh_schedule: dashboardDetails.refreshSchedule,
        deployment_type: 'streamlit_snowflake'
      }
    });
  }

  async logIntentAnalysis(analysisResult, customerID, sessionID) {
    const activityName = analysisResult.isDashboardRequest 
      ? 'ccode.dashboard_intent' 
      : 'ccode.intent_rejected';
    
    await this.logEvent({
      activity: activityName,
      customer: customerID,
      session_id: sessionID,
      feature_json: {
        confidence_percent: analysisResult.confidence,
        dashboard_intent: analysisResult.isDashboardRequest,
        detected_metrics: analysisResult.requirements?.metrics || [],
        detected_panels: analysisResult.requirements?.panels?.length || 0,
        conversation_length: analysisResult.analysis?.user_messages_analyzed || 0,
        key_phrases: analysisResult.analysis?.key_phrases || [],
        analysis_reason: analysisResult.reason
      }
    });
  }

  async logSpecOperation(operation, specData, customerID, sessionID) {
    const activityName = `ccode.dashboard_spec_${operation}`;
    
    await this.logEvent({
      activity: activityName,
      customer: customerID,
      session_id: sessionID,
      feature_json: {
        spec_name: specData.name,
        spec_hash: specData.hash,
        panels_count: specData.panels?.length || 0,
        schedule_mode: specData.schedule?.mode,
        validation_errors: specData.validationErrors || [],
        generation_time_ms: specData.generationTime || null
      }
    });
  }

  // Execute SQL with Snowflake connection
  async executeSQL(sqlText, binds = []) {
    // Context is set once at connection time, just execute the query
    return new Promise((resolve, reject) => {
      this.snowflake.execute({
        sqlText: sqlText,
        binds: binds,
        complete: (err, stmt) => {
          if (err) {
            reject(err);
          } else {
            resolve({
              rows: stmt.getResultSet(),
              rowCount: stmt.getNumRows()
            });
          }
        }
      });
    });
  }

  // Analytics methods
  async getDashboardAnalytics(customerID, timeRange = '7 days') {
    const query = `
      SELECT 
        activity,
        COUNT(*) as activity_count,
        COUNT(DISTINCT customer) as unique_customers,
        AVG(_activity_occurrence) as avg_occurrence,
        MIN(ts) as first_activity,
        MAX(ts) as latest_activity
      FROM EVENTS
      WHERE customer = ?
        AND activity LIKE 'ccode.dashboard_%'
        AND ts >= CURRENT_TIMESTAMP - INTERVAL '${timeRange}'
      GROUP BY activity
      ORDER BY activity_count DESC
    `;
    
    const result = await this.executeSQL(query, [customerID]);
    return result.rows || [];
  }

  async getDashboardCreationFunnel(timeRange = '30 days') {
    const query = `
      WITH funnel_steps AS (
        SELECT 
          customer,
          feature_json:creation_id::STRING as creation_id,
          activity,
          ts,
          ROW_NUMBER() OVER (PARTITION BY customer, feature_json:creation_id ORDER BY ts) as step_order
        FROM ACTIVITY.EVENTS
        WHERE activity LIKE 'ccode.dashboard_%'
          AND ts >= CURRENT_TIMESTAMP - INTERVAL '${timeRange}'
      )
      SELECT 
        activity,
        COUNT(*) as attempts,
        COUNT(DISTINCT customer) as unique_customers,
        AVG(step_order) as avg_step_position
      FROM funnel_steps
      GROUP BY activity
      ORDER BY avg_step_position
    `;
    
    const result = await this.executeSQL(query);
    return result.rows || [];
  }

  // Get logger version and capabilities
  getVersion() {
    return {
      version: this.version,
      schema_version: '2.0.0',
      capabilities: {
        total_columns: 14,
        supported_activities: Object.keys(this.activityTypes).length,
        features: [
          'strict_schema_compliance',
          'activity_occurrence_tracking', 
          'query_correlation',
          'dashboard_funnel_analysis',
          'non_blocking_logging'
        ]
      }
    };
  }
}

module.exports = ActivityLoggerWrapper;