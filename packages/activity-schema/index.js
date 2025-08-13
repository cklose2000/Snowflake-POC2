// Activity Schema v2 Logger - Strict compliance with 14-column schema
// Logs all Dashboard Factory operations as activities for audit and analysis

class ActivityLogger {
  constructor(snowflakeConnection) {
    this.snowflake = snowflakeConnection;
    this.version = '2.0.0';
    this.sourceSystem = 'claude_code';
    this.sourceVersion = 'dashboard_factory_v1.0.0';
    
    // Activity Schema 2.0 specification (14 columns total)
    this.schema = {
      // 5 Core required columns
      core: ['activity_id', 'ts', 'customer', 'activity', 'feature_json'],
      
      // 3 Optional columns  
      optional: ['anonymous_customer_id', 'revenue_impact', 'link'],
      
      // 6 System extension columns (always populated)
      system: [
        '_source_system',
        '_source_version', 
        '_session_id',
        '_query_tag',
        '_activity_occurrence',
        '_activity_repeated_at'
      ]
    };
    
    // Dashboard Factory activity types
    this.activityTypes = {
      // Dashboard creation pipeline
      'ccode.dashboard_analyze_conversation': 'Analyzed conversation for dashboard intent',
      'ccode.dashboard_generate_spec': 'Generated dashboard specification',
      'ccode.dashboard_validate_spec': 'Validated dashboard specification',
      'ccode.dashboard_preflight_checks': 'Ran preflight checks for object creation',
      'ccode.dashboard_create_objects': 'Created Snowflake objects (views/tasks/dynamic tables)',
      'ccode.dashboard_generate_streamlit': 'Generated Streamlit application code',
      'ccode.dashboard_deploy_app': 'Deployed Streamlit app to Snowflake',
      'ccode.dashboard_log_completion': 'Dashboard creation completed successfully',
      'ccode.dashboard_creation_failed': 'Dashboard creation failed',
      
      // Dashboard management
      'ccode.dashboard_created': 'Dashboard successfully created and deployed',
      'ccode.dashboard_destroyed': 'Dashboard and all objects removed',
      'ccode.dashboard_accessed': 'Dashboard accessed by user',
      'ccode.dashboard_refreshed': 'Dashboard data refreshed',
      
      // Object operations
      'ccode.snowflake_object_created': 'Snowflake object created (view/task/dynamic table)',
      'ccode.snowflake_object_dropped': 'Snowflake object removed',
      'ccode.preflight_check_passed': 'Preflight check passed',
      'ccode.preflight_check_failed': 'Preflight check failed',
      
      // Spec operations
      'ccode.spec_generated': 'Dashboard spec generated from intent',
      'ccode.spec_validated': 'Dashboard spec passed validation',
      'ccode.spec_validation_failed': 'Dashboard spec failed validation',
      
      // Intent analysis
      'ccode.intent_detected': 'Dashboard intent detected in conversation',
      'ccode.intent_rejected': 'No dashboard intent found in conversation'
    };
  }

  // Main activity logging method - enforces strict schema compliance
  async logEvent(eventData) {
    try {
      const activityRecord = await this.buildActivityRecord(eventData);
      await this.insertActivity(activityRecord);
      return activityRecord.activity_id;
    } catch (error) {
      console.error(`❌ Activity logging failed: ${error.message}`);
      throw error;
    }
  }

  // Build complete activity record with all 14 columns
  async buildActivityRecord(eventData) {
    const activityId = this.generateActivityId();
    const timestamp = new Date().toISOString();
    
    // Validate required core fields
    if (!eventData.activity || !eventData.customer) {
      throw new Error('Activity and customer are required fields');
    }
    
    // Get activity occurrence for this customer
    const occurrence = await this.getActivityOccurrence(
      eventData.customer, 
      eventData.activity
    );
    
    // Build complete record with all 14 columns
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
    const baseTag = 'dashboard_factory';
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
        FROM analytics.activity.events
        WHERE customer = ? AND activity = ?
      `;
      
      const result = await this.snowflake.execute({
        sqlText: query,
        binds: [customer, activity]
      });
      
      const row = result.resultSet?.[0];
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
      INSERT INTO analytics.activity.events (
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
    
    await this.snowflake.execute({
      sqlText: insertSQL,
      binds: binds
    });
    
    console.log(`✅ Activity logged: ${record.activity_id} (${record.activity})`);
  }

  // Log dashboard creation pipeline step
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

  // Log successful dashboard creation
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

  // Log dashboard access/usage
  async logDashboardAccess(dashboardUrl, customerID, sessionID, accessMetadata = {}) {
    await this.logEvent({
      activity: 'ccode.dashboard_accessed',
      customer: customerID,
      session_id: sessionID,
      link: dashboardUrl,
      feature_json: {
        access_timestamp: new Date().toISOString(),
        user_agent: accessMetadata.userAgent || null,
        session_duration_ms: accessMetadata.sessionDuration || null,
        pages_viewed: accessMetadata.pagesViewed || 1,
        ...accessMetadata
      }
    });
  }

  // Log Snowflake object creation
  async logObjectCreation(objectName, objectType, customerID, sessionID, metadata = {}) {
    await this.logEvent({
      activity: 'ccode.snowflake_object_created',
      customer: customerID,
      session_id: sessionID,
      feature_json: {
        object_name: objectName,
        object_type: objectType, // view, task, dynamic_table, warehouse, etc
        creation_timestamp: new Date().toISOString(),
        sql_executed: metadata.sql || null,
        dependencies: metadata.dependencies || [],
        ...metadata
      }
    });
  }

  // Log spec generation and validation
  async logSpecOperation(operation, specData, customerID, sessionID) {
    const activityName = `ccode.spec_${operation}`;
    
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

  // Log conversation analysis results
  async logIntentAnalysis(analysisResult, customerID, sessionID) {
    const activityName = analysisResult.isDashboardRequest 
      ? 'ccode.intent_detected' 
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

  // Query activities for dashboard analytics
  async getDashboardAnalytics(customerID, timeRange = '7 days') {
    const query = `
      SELECT 
        activity,
        COUNT(*) as activity_count,
        COUNT(DISTINCT customer) as unique_customers,
        AVG(_activity_occurrence) as avg_occurrence,
        MIN(ts) as first_activity,
        MAX(ts) as latest_activity
      FROM analytics.activity.events
      WHERE customer = ?
        AND activity LIKE 'ccode.dashboard_%'
        AND ts >= CURRENT_TIMESTAMP - INTERVAL '${timeRange}'
      GROUP BY activity
      ORDER BY activity_count DESC
    `;
    
    const result = await this.snowflake.execute({
      sqlText: query,
      binds: [customerID]
    });
    
    return result.resultSet || [];
  }

  // Query dashboard creation funnel
  async getDashboardCreationFunnel(timeRange = '30 days') {
    const query = `
      WITH funnel_steps AS (
        SELECT 
          customer,
          feature_json:creation_id::STRING as creation_id,
          activity,
          ts,
          ROW_NUMBER() OVER (PARTITION BY customer, feature_json:creation_id ORDER BY ts) as step_order
        FROM analytics.activity.events
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
    
    const result = await this.snowflake.execute({ sqlText: query });
    return result.resultSet || [];
  }

  // Validate activity before logging
  validateActivity(eventData) {
    const errors = [];
    
    // Check required fields
    if (!eventData.activity) errors.push('activity is required');
    if (!eventData.customer) errors.push('customer is required');
    
    // Check activity type is known
    if (!this.activityTypes[eventData.activity]) {
      errors.push(`unknown activity type: ${eventData.activity}`);
    }
    
    // Validate feature_json is serializable
    if (eventData.feature_json) {
      try {
        JSON.stringify(eventData.feature_json);
      } catch (e) {
        errors.push('feature_json must be valid JSON');
      }
    }
    
    if (errors.length > 0) {
      throw new Error(`Activity validation failed: ${errors.join(', ')}`);
    }
  }

  // Get logger version and capabilities
  getVersion() {
    return {
      version: this.version,
      schema_version: '2.0.0',
      capabilities: {
        total_columns: 14,
        core_columns: this.schema.core.length,
        optional_columns: this.schema.optional.length,
        system_columns: this.schema.system.length,
        supported_activities: Object.keys(this.activityTypes).length,
        features: [
          'strict_schema_compliance',
          'activity_occurrence_tracking',
          'query_correlation',
          'dashboard_funnel_analysis',
          'automatic_metadata_enrichment'
        ]
      }
    };
  }
}

module.exports = ActivityLogger;