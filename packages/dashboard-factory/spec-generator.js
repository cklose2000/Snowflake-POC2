// Spec Generator - Converts conversation analysis to Activity-native dashboard specifications
// v2.0.0 - Activity Schema IS the product (no fake tables!)

const { validateSpec } = require('./schema');
const schemaConfig = require('../snowflake-schema');

class SpecGenerator {
  constructor() {
    this.version = '2.0.0';
    
    // Activity-native panel configurations (the ONLY data source for v1)
    this.activityPanelTypes = {
      'activity_breakdown': {
        source: 'ACTIVITY_CCODE.VW_ACTIVITY_COUNTS_24H',
        x: 'activity',
        y: 'events_24h',
        type: 'bar',
        description: 'Activity counts by type and customer (24h window)'
      },
      'llm_performance': {
        source: 'ACTIVITY_CCODE.VW_LLM_TELEMETRY',
        metric: 'latency_ms',
        type: 'histogram',
        description: 'LLM latency distribution and token usage'
      },
      'sql_cost_analysis': {
        source: 'ACTIVITY_CCODE.VW_SQL_EXECUTIONS',
        x: 'ts',
        y: 'bytes_scanned',
        type: 'timeseries',
        grain: 'hour',
        description: 'SQL query costs and performance over time'
      },
      'dashboard_operations': {
        source: 'ACTIVITY_CCODE.VW_DASHBOARD_OPERATIONS',
        x: 'ts',
        y: 'panel_count',
        type: 'timeseries',
        description: 'Dashboard lifecycle events'
      },
      'template_usage': {
        source: 'ACTIVITY_CCODE.VW_SAFESQL_TEMPLATES',
        x: 'template',
        y: 'template_running_count',
        type: 'bar',
        description: 'SafeSQL template usage patterns'
      },
      'activity_summary': {
        source: 'ACTIVITY_CCODE.VW_ACTIVITY_SUMMARY',
        type: 'metrics',
        description: 'High-level activity metrics overview'
      }
    };
    
    // Keywords that map to Activity panel types
    this.intentKeywords = {
      // Activity patterns
      'activities': 'activity_breakdown',
      'activity': 'activity_breakdown',
      'events': 'activity_breakdown',
      'actions': 'activity_breakdown',
      'operations': 'activity_breakdown',
      
      // LLM/Claude patterns
      'llm': 'llm_performance',
      'claude': 'llm_performance',
      'model': 'llm_performance',
      'latency': 'llm_performance',
      'tokens': 'llm_performance',
      'response time': 'llm_performance',
      
      // SQL/Query patterns
      'sql': 'sql_cost_analysis',
      'query': 'sql_cost_analysis',
      'queries': 'sql_cost_analysis',
      'cost': 'sql_cost_analysis',
      'bytes': 'sql_cost_analysis',
      'performance': 'sql_cost_analysis',
      
      // Dashboard patterns
      'dashboard': 'dashboard_operations',
      'dashboards': 'dashboard_operations',
      'dashboard metrics': 'dashboard_operations',
      
      // Template patterns
      'template': 'template_usage',
      'templates': 'template_usage',
      'safesql': 'template_usage',
      
      // Overview patterns
      'summary': 'activity_summary',
      'overview': 'activity_summary',
      'metrics': 'activity_summary'
    };
  }

  // Main generation method: Intent → Activity Dashboard Spec
  async generateFromIntent(intent) {
    console.log(`📝 Generating Activity-native dashboard spec from intent (${intent.confidence}% confidence)`);
    
    if (!intent.isDashboardRequest) {
      throw new Error('Cannot generate spec: no dashboard intent detected');
    }
    
    const requirements = intent.requirements;
    
    // Build the Activity dashboard specification
    const spec = {
      name: this.generateValidName(requirements.name || 'activity_dashboard'),
      timezone: requirements.timezone || 'America/New_York',
      panels: await this.generateActivityPanels(requirements),
      schedule: this.generateSchedule(requirements.schedule)
    };
    
    // Validate the generated spec
    const validation = validateSpec(spec);
    if (!validation.valid) {
      console.error('❌ Generated spec failed validation:', validation.errors);
      throw new Error(`Invalid spec generated: ${validation.summary}`);
    }
    
    console.log(`✅ Generated valid dashboard spec: ${spec.name} with ${spec.panels.length} panels`);
    return spec;
  }

  // Generate Activity-native panels based on requirements
  async generateActivityPanels(requirements) {
    const panels = [];
    const detectedTypes = new Set();
    
    // Extract Activity panel types from conversation
    const keywords = this.extractActivityKeywords(requirements);
    
    // If no specific Activity patterns detected, use defaults
    if (keywords.length === 0) {
      console.log('📊 No specific Activity patterns detected, using default panels');
      panels.push(this.createActivityPanel('activity_breakdown', 0));
      panels.push(this.createActivityPanel('activity_summary', 1));
    } else {
      // Create panels for each detected pattern
      keywords.forEach((keyword, index) => {
        const panelType = this.intentKeywords[keyword];
        if (panelType && !detectedTypes.has(panelType)) {
          panels.push(this.createActivityPanel(panelType, index));
          detectedTypes.add(panelType);
        }
      });
    }
    
    // Ensure we have at least one panel
    if (panels.length === 0) {
      panels.push(this.createDefaultActivityPanel());
    }
    
    console.log(`📊 Generated ${panels.length} Activity-native panels`);
    return panels;
  }

  // Create an Activity panel from type
  createActivityPanel(panelType, index) {
    const config = this.activityPanelTypes[panelType];
    if (!config) {
      console.warn(`⚠️ Unknown panel type: ${panelType}, using default`);
      return this.createDefaultActivityPanel();
    }
    
    // Map Activity view types to schema-compliant types
    const typeMapping = {
      'bar': 'chart',
      'histogram': 'chart',
      'timeseries': 'timeseries',
      'metrics': 'metric'
    };
    
    // Extract just the table name from the source (remove schema prefix)
    const sourceParts = config.source.split('.');
    const tableName = sourceParts[sourceParts.length - 1];
    
    // Generate panel with schema-compliant configuration
    const panel = {
      id: `panel_${index + 1}`,
      type: typeMapping[config.type] || 'table',
      source: tableName,
      metric: 'COUNT(*)'  // Required field per schema
    };
    
    // Add required fields based on panel type
    if (panel.type === 'timeseries') {
      panel.grain = config.grain || 'hour';
      // Timeseries panels need a group_by for the time column
      panel.group_by = ['ts'];
    } else if (panel.type === 'chart' || panel.type === 'table') {
      // Charts and tables need group_by for categorical breakdowns
      if (config.x) {
        panel.group_by = [config.x];
      }
      panel.top_n = 10;
    }
    
    // Add a time window (required for meaningful Activity data)
    panel.window = { days: 7 };  // Default 7-day window
    
    return panel;
  }

  // Create default Activity panel
  createDefaultActivityPanel() {
    return {
      id: 'activity_default',
      type: 'chart',
      source: 'VW_ACTIVITY_COUNTS_24H',
      metric: 'COUNT(*)',
      group_by: ['activity'],
      window: { days: 1 },
      top_n: 10
    };
  }

  // Extract Activity-related keywords from requirements
  extractActivityKeywords(requirements) {
    const keywords = [];
    
    // Combine all text from requirements
    const searchText = [
      requirements.name || '',
      requirements.description || '',
      JSON.stringify(requirements.metrics || []),
      JSON.stringify(requirements.panels || []),
      requirements.visualization || ''
    ].join(' ').toLowerCase();
    
    // Look for Activity-related keywords
    Object.keys(this.intentKeywords).forEach(keyword => {
      if (searchText.includes(keyword.toLowerCase())) {
        keywords.push(keyword);
      }
    });
    
    // Remove duplicates and return
    return [...new Set(keywords)];
  }

  // Generate schedule configuration (Tasks only for v1, no Dynamic Tables)
  generateSchedule(scheduleReq) {
    // Always return a valid schedule (schema requires mode to be exact or freshness)
    // Default to exact mode with daily schedule
    if (!scheduleReq || !scheduleReq.enabled) {
      return {
        mode: 'exact',
        cron_utc: '0 12 * * *'  // Daily at noon UTC (8am ET)
      };
    }
    
    // Use Tasks for exact times (schedule stored in UTC)
    const schedule = {
      mode: 'exact',
      enabled: true
    };
    
    // Convert local time to UTC cron
    if (scheduleReq.exact_time) {
      schedule.cron_utc = this.convertToCronUTC(
        scheduleReq.exact_time,
        scheduleReq.timezone || 'America/New_York'
      );
      schedule.display_time = scheduleReq.exact_time; // Show local time in UI
    } else if (scheduleReq.frequency === 'daily') {
      // Default to 8am ET (12pm UTC)
      schedule.cron_utc = '0 12 * * *';
      schedule.display_time = '8:00 AM ET';
    } else if (scheduleReq.frequency === 'hourly') {
      schedule.cron_utc = '0 * * * *';
      schedule.display_time = 'Every hour';
    } else {
      // Default to daily
      schedule.cron_utc = '0 12 * * *';
      schedule.display_time = '8:00 AM ET';
    }
    
    return schedule;
  }

  // Convert local time to UTC cron expression
  convertToCronUTC(localTime, timezone) {
    // Simplified conversion - in production, use moment-timezone
    const timezoneOffsets = {
      'America/New_York': -5,
      'America/Chicago': -6,
      'America/Denver': -7,
      'America/Los_Angeles': -8,
      'UTC': 0
    };
    
    const offset = timezoneOffsets[timezone] || -5;
    
    // Parse time (HH:MM format)
    const [hours, minutes] = localTime.split(':').map(Number);
    const utcHours = (hours - offset + 24) % 24;
    
    return `${minutes} ${utcHours} * * *`;
  }

  // Generate valid dashboard name (idempotent naming)
  generateValidName(inputName) {
    // Clean and format the name
    let cleanName = inputName
      .toLowerCase()
      .replace(/[^a-z0-9_]/g, '_')
      .replace(/_+/g, '_')
      .replace(/^_|_$/g, '');
    
    // Ensure it starts with a letter
    if (!/^[a-z]/.test(cleanName)) {
      cleanName = 'dashboard_' + cleanName;
    }
    
    // Limit length
    if (cleanName.length > 50) {
      cleanName = cleanName.substring(0, 50);
    }
    
    // Add activity prefix if not present
    if (!cleanName.includes('activity')) {
      cleanName = 'activity_' + cleanName;
    }
    
    return cleanName;
  }

  // Get spec version for compatibility checking
  getVersion() {
    return this.version;
  }
}

module.exports = SpecGenerator;