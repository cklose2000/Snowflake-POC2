// Dashboard Factory v1 - Orchestrator
// Converts conversation to dashboard in under 5 minutes

const { validateSpec, generateSpecHash, generateObjectNames } = require('./schema');
const ConversationAnalyzer = require('./conversation-analyzer');
const SpecGenerator = require('./spec-generator');
const SnowflakeObjectManager = require('./snowflake-objects');
const StreamlitGenerator = require('./streamlit-generator');
const ActivityLogger = require('../activity-schema');

class DashboardFactory {
  constructor(snowflakeConnection, options = {}) {
    this.snowflake = snowflakeConnection;
    this.options = {
      timeout: options.timeout || 300000, // 5 minute timeout
      dryRun: options.dryRun || false,
      ...options
    };
    
    // Initialize components
    this.conversationAnalyzer = new ConversationAnalyzer();
    this.specGenerator = new SpecGenerator();
    this.objectManager = new SnowflakeObjectManager(snowflakeConnection);
    this.streamlitGenerator = new StreamlitGenerator();
    this.activityLogger = new ActivityLogger(snowflakeConnection);
    
    // Track creation progress
    this.creationSteps = [
      'analyze_conversation',
      'detect_intent', 
      'generate_spec',
      'validate_spec',
      'preflight_checks',
      'create_objects',
      'generate_streamlit',
      'deploy_app',
      'log_completion'
    ];
  }

  // Main orchestration method: Conversation → Dashboard URL
  async createDashboard(conversationHistory, customerID, sessionID) {
    const startTime = Date.now();
    const creationID = `dash_${Date.now()}_${sessionID}`;
    
    console.log(`🏭 Dashboard Factory: Starting creation ${creationID}`);
    
    try {
      // Step 1: Analyze conversation for dashboard intent
      await this.logProgress('analyze_conversation', creationID, customerID);
      const intent = await this.conversationAnalyzer.analyzeDashboardIntent(conversationHistory);
      
      if (!intent.isDashboardRequest) {
        throw new Error('No dashboard intent detected in conversation');
      }
      
      console.log(`✅ Dashboard intent detected: ${intent.confidence}% confidence`);
      
      // Step 2: Generate dashboard specification
      await this.logProgress('generate_spec', creationID, customerID);
      const spec = await this.specGenerator.generateFromIntent(intent);
      
      // Step 3: Validate specification
      await this.logProgress('validate_spec', creationID, customerID);
      const validation = validateSpec(spec);
      if (!validation.valid) {
        throw new Error(`Spec validation failed: ${validation.summary}`);
      }
      
      console.log(`✅ Dashboard spec generated and validated: ${spec.name}`);
      
      // Step 4: Preflight checks
      await this.logProgress('preflight_checks', creationID, customerID);
      const preflightResult = await this.objectManager.runPreflightChecks(spec);
      
      if (!preflightResult.passed) {
        console.log(`⚠️ Preflight checks failed, applying fallbacks:`, preflightResult.issues);
        spec.schedule = await this.applyFallbacks(spec, preflightResult);
      }
      
      // Step 5: Create Snowflake objects (Views/Tasks/Dynamic Tables)
      await this.logProgress('create_objects', creationID, customerID);
      const objectResults = await this.objectManager.createDashboardObjects(spec);
      
      console.log(`✅ Created ${objectResults.objectsCreated} Snowflake objects`);
      
      // Step 6: Generate Streamlit application
      await this.logProgress('generate_streamlit', creationID, customerID);
      const streamlitCode = await this.streamlitGenerator.generateApp(spec, objectResults);
      
      // Step 7: Deploy Streamlit app
      await this.logProgress('deploy_app', creationID, customerID);
      const deployResult = await this.deployStreamlitApp(spec, streamlitCode);
      
      // Step 8: Log successful completion
      await this.logProgress('log_completion', creationID, customerID, {
        dashboard_url: deployResult.url,
        creation_time_ms: Date.now() - startTime,
        spec_hash: generateSpecHash(spec)
      });
      
      console.log(`🎉 Dashboard created successfully: ${deployResult.url}`);
      
      // Return dashboard details
      return {
        success: true,
        url: deployResult.url,
        name: spec.name,
        refreshSchedule: this.formatScheduleDisplay(spec),
        panelsCount: spec.panels.length,
        creationTimeMs: Date.now() - startTime,
        specId: generateSpecHash(spec),
        objectsCreated: objectResults.objectsCreated
      };
      
    } catch (error) {
      console.error(`❌ Dashboard creation failed: ${error.message}`);
      
      // Log failure
      await this.logProgress('creation_failed', creationID, customerID, {
        error: error.message,
        creation_time_ms: Date.now() - startTime
      });
      
      // Attempt cleanup
      try {
        if (spec) {
          await this.cleanupFailedDashboard(spec);
        }
      } catch (cleanupError) {
        console.error(`❌ Cleanup failed: ${cleanupError.message}`);
      }
      
      return {
        success: false,
        error: error.message,
        creationTimeMs: Date.now() - startTime
      };
    }
  }

  // Apply fallback strategies when preflight checks fail
  async applyFallbacks(spec, preflightResult) {
    console.log(`🔧 Applying fallbacks for preflight issues`);
    
    // If Dynamic Tables prerequisites failed, fall back to Tasks
    if (spec.schedule.mode === 'freshness' && preflightResult.issues.includes('change_tracking_missing')) {
      console.log(`🔄 Falling back from Dynamic Tables to Tasks due to missing change tracking`);
      
      // Convert freshness schedule to equivalent exact schedule
      return {
        mode: 'exact',
        cron_utc: this.convertFreshnessToExact(spec.schedule.target_lag)
      };
    }
    
    return spec.schedule;
  }

  // Convert freshness window to approximate cron schedule
  convertFreshnessToExact(targetLag) {
    const lagToInterval = {
      '15 minutes': '*/15 * * * *',
      '30 minutes': '*/30 * * * *', 
      '1 hour': '0 * * * *',
      '2 hours': '0 */2 * * *',
      '4 hours': '0 */4 * * *',
      '6 hours': '0 */6 * * *',
      '12 hours': '0 */12 * * *',
      '1 day': '0 12 * * *' // Default to noon UTC
    };
    
    return lagToInterval[targetLag] || '0 12 * * *';
  }

  // Deploy Streamlit application to Snowflake
  async deployStreamlitApp(spec, streamlitCode) {
    const objectNames = generateObjectNames(spec);
    const appName = objectNames.streamlit_app;
    
    if (this.options.dryRun) {
      console.log(`🧪 DRY RUN: Would deploy Streamlit app: ${appName}`);
      return {
        url: `https://mock-account.snowflakecomputing.com/console#/streamlit-apps/${appName}`,
        appName: appName
      };
    }
    
    // Create Streamlit application in Snowflake
    const createAppSQL = `
      CREATE OR REPLACE STREAMLIT ${appName}
      ROOT_LOCATION = '@STREAMLIT_STAGE'
      MAIN_FILE = '${appName}.py'
      QUERY_WAREHOUSE = '${objectNames.warehouse}'
    `;
    
    await this.snowflake.execute({ sqlText: createAppSQL });
    
    // Upload the Python file to the stage
    const uploadSQL = `
      PUT 'data:${Buffer.from(streamlitCode).toString('base64')}' '@STREAMLIT_STAGE/${appName}.py' 
      SOURCE_COMPRESSION = NONE
      AUTO_COMPRESS = FALSE
      OVERWRITE = TRUE
    `;
    
    await this.snowflake.execute({ sqlText: uploadSQL });
    
    // Get Snowflake account identifier for URL
    const accountResult = await this.snowflake.execute({
      sqlText: "SELECT CURRENT_ACCOUNT() as account"
    });
    
    const accountName = accountResult.resultSet[0].ACCOUNT;
    const url = `https://${accountName}.snowflakecomputing.com/console#/streamlit-apps/${appName}`;
    
    return {
      url: url,
      appName: appName
    };
  }

  // Format schedule for user display
  formatScheduleDisplay(spec) {
    const { convertCronToLocalDisplay } = require('./schema');
    
    if (spec.schedule.mode === 'exact') {
      return `Refreshes ${convertCronToLocalDisplay(spec.schedule.cron_utc, spec.timezone)}`;
    } else {
      return `Refreshes within ${spec.schedule.target_lag} of new data`;
    }
  }

  // Clean up failed dashboard creation
  async cleanupFailedDashboard(spec) {
    console.log(`🧹 Cleaning up failed dashboard: ${spec.name}`);
    
    try {
      const dropResult = await this.objectManager.dropDashboardObjects(spec);
      console.log(`✅ Cleanup completed: ${dropResult.objectsDropped} objects removed`);
    } catch (error) {
      console.error(`❌ Cleanup error: ${error.message}`);
      // Don't rethrow - cleanup is best-effort
    }
  }

  // Get list of existing dashboards for management
  async listDashboards(customerID) {
    // Query Activity Schema for dashboard creation events
    const query = `
      SELECT 
        _feature_json:spec_id::STRING as spec_id,
        activity,
        customer,
        link as dashboard_url,
        ts as created_at,
        _feature_json:panels::INTEGER as panels_count,
        _feature_json:schedule_mode::STRING as schedule_mode
      FROM analytics.activity.events
      WHERE activity = 'ccode.dashboard_created'
        AND customer = ?
      ORDER BY ts DESC
    `;
    
    const result = await this.snowflake.execute({
      sqlText: query,
      binds: [customerID]
    });
    
    return result.resultSet || [];
  }

  // Destroy dashboard and all its objects
  async destroyDashboard(specId, customerID) {
    // This would require storing the original spec or reconstructing it
    // For v1, we'll implement this as a separate management interface
    throw new Error('Dashboard destruction not implemented in v1 - manual cleanup required');
  }

  // Log progress through creation steps
  async logProgress(step, creationID, customerID, metadata = {}) {
    const activityName = `ccode.dashboard_${step}`;
    
    await this.activityLogger.logEvent({
      activity: activityName,
      customer: customerID,
      ts: new Date().toISOString(),
      link: null,
      feature_json: {
        creation_id: creationID,
        step: step,
        ...metadata
      },
      source_system: 'claude_code',
      source_version: 'v1.0.0',
      query_tag: `dashboard_factory_${step}_${creationID}`
    });
  }

  // Get creation statistics
  getCreationStats() {
    return {
      version: '1.0.0',
      components: {
        conversation_analyzer: this.conversationAnalyzer.getVersion(),
        spec_generator: this.specGenerator.getVersion(),
        object_manager: this.objectManager.getVersion(),
        streamlit_generator: this.streamlitGenerator.getVersion()
      },
      creation_steps: this.creationSteps.length,
      timeout_ms: this.options.timeout
    };
  }
}

module.exports = DashboardFactory;