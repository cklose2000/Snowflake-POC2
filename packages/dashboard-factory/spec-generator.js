// Spec Generator - Converts conversation analysis to valid dashboard specifications

const { validateSpec, EXAMPLE_SPECS } = require('./schema');

class SpecGenerator {
  constructor() {
    this.version = '1.0.0';
    
    // Mapping from conversation requirements to spec elements
    this.metricMapping = {
      'revenue': { metric: 'SUM(revenue)', source: 'fact_sales' },
      'sales': { metric: 'SUM(revenue)', source: 'fact_sales' },
      'customers': { metric: 'COUNT(DISTINCT customer_id)', source: 'fact_sales' },
      'orders': { metric: 'COUNT(DISTINCT order_id)', source: 'fact_sales' },
      'quantity': { metric: 'SUM(quantity)', source: 'fact_sales' },
      'profit': { metric: 'SUM(profit)', source: 'fact_sales' }
    };
    
    // Default panel configurations
    this.defaultPanelConfigs = {
      table: {
        type: 'table',
        top_n: 10,
        window: { days: 90 }
      },
      timeseries: {
        type: 'timeseries',
        grain: 'quarter',
        window: { quarters: 8 }
      },
      metric: {
        type: 'metric',
        window: { days: 30 }
      }
    };
  }

  // Main generation method: Intent → Dashboard Spec
  async generateFromIntent(intent) {
    console.log(`📝 Generating dashboard spec from intent (${intent.confidence}% confidence)`);
    
    if (!intent.isDashboardRequest) {
      throw new Error('Cannot generate spec: no dashboard intent detected');
    }
    
    const requirements = intent.requirements;
    
    // Build the dashboard specification
    const spec = {
      name: this.generateValidName(requirements.name),
      timezone: requirements.timezone || 'America/New_York',
      panels: await this.generatePanels(requirements),
      schedule: this.generateSchedule(requirements.schedule)
    };
    
    // Validate the generated spec
    const validation = validateSpec(spec);
    if (!validation.valid) {
      console.error('Generated spec failed validation:', validation.errors);
      
      // Try fallback to example spec
      const fallbackSpec = this.generateFallbackSpec(requirements);
      const fallbackValidation = validateSpec(fallbackSpec);
      
      if (fallbackValidation.valid) {
        console.log('✅ Using fallback spec after validation failure');
        return fallbackSpec;
      }
      
      throw new Error(`Spec generation failed validation: ${validation.summary}`);
    }
    
    console.log(`✅ Generated valid dashboard spec: ${spec.name} with ${spec.panels.length} panels`);
    return spec;
  }

  // Generate valid dashboard name
  generateValidName(suggestedName) {
    if (!suggestedName) {
      suggestedName = 'generated_dashboard';
    }
    
    // Clean and validate name
    let cleanName = suggestedName
      .toLowerCase()
      .replace(/[^a-z0-9_]/g, '_')
      .replace(/_+/g, '_')
      .replace(/^_|_$/g, '');
    
    // Ensure it starts with letter and meets length requirements
    if (!/^[a-z]/.test(cleanName)) {
      cleanName = 'd_' + cleanName;
    }
    
    if (cleanName.length < 3) {
      cleanName = cleanName + '_dashboard';
    }
    
    if (cleanName.length > 63) {
      cleanName = cleanName.substring(0, 60) + '_db';
    }
    
    return cleanName;
  }

  // Generate panels from requirements
  async generatePanels(requirements) {
    const panels = [];
    
    // If specific panels were detected, use those
    if (requirements.panels && requirements.panels.length > 0) {
      for (let i = 0; i < requirements.panels.length; i++) {
        const panelReq = requirements.panels[i];
        const panel = await this.generatePanel(panelReq, requirements, i);
        if (panel) {
          panels.push(panel);
        }
      }
    } else {
      // Generate default panel based on detected metrics
      const defaultPanel = await this.generateDefaultPanel(requirements);
      if (defaultPanel) {
        panels.push(defaultPanel);
      }
    }
    
    // Ensure we have at least one panel
    if (panels.length === 0) {
      panels.push(this.generateBasicPanel());
    }
    
    return panels;
  }

  // Generate individual panel from requirements
  async generatePanel(panelReq, requirements, index) {
    const panelId = `panel_${index + 1}`;
    
    // Start with base configuration
    const baseConfig = this.defaultPanelConfigs[panelReq.type] || this.defaultPanelConfigs.table;
    
    // Select metric based on requirements
    const selectedMetric = this.selectMetric(requirements.metrics);
    const source = this.selectSource(requirements.sources, selectedMetric);
    
    const panel = {
      id: panelId,
      type: panelReq.type || 'table',
      source: source,
      metric: selectedMetric.metric,
      ...baseConfig
    };
    
    // Add specific configurations
    if (panelReq.top_n) {
      panel.top_n = Math.min(panelReq.top_n, 1000);
    }
    
    // Add group_by based on metric and type
    panel.group_by = this.generateGroupBy(selectedMetric, panel.type);
    
    return panel;
  }

  // Generate default panel when no specific requirements
  async generateDefaultPanel(requirements) {
    const selectedMetric = this.selectMetric(requirements.metrics);
    const source = this.selectSource(requirements.sources, selectedMetric);
    
    return {
      id: 'main_panel',
      type: 'table',
      source: source,
      metric: selectedMetric.metric,
      group_by: this.generateGroupBy(selectedMetric, 'table'),
      window: { days: 90 },
      top_n: 10
    };
  }

  // Generate basic fallback panel
  generateBasicPanel() {
    return {
      id: 'basic_panel',
      type: 'table',
      source: 'fact_sales',
      metric: 'SUM(revenue)',
      group_by: ['customer_name'],
      window: { days: 90 },
      top_n: 10
    };
  }

  // Select best metric from detected ones
  selectMetric(detectedMetrics) {
    if (!detectedMetrics || detectedMetrics.length === 0) {
      return { metric: 'SUM(revenue)', key: 'revenue' };
    }
    
    // Map SafeSQL metrics back to keys
    const metricToKey = {
      'SUM(revenue)': 'revenue',
      'COUNT(DISTINCT customer_id)': 'customers',
      'COUNT(DISTINCT order_id)': 'orders',
      'SUM(quantity)': 'quantity',
      'SUM(profit)': 'profit'
    };
    
    // Use first detected metric
    const selectedMetric = detectedMetrics[0];
    const key = metricToKey[selectedMetric] || 'revenue';
    
    return { metric: selectedMetric, key: key };
  }

  // Select data source
  selectSource(detectedSources, metric) {
    if (detectedSources && detectedSources.length > 0) {
      // Use source from successful SafeSQL execution if available
      const successfulSource = detectedSources.find(s => s.success);
      if (successfulSource && successfulSource.table) {
        return successfulSource.table;
      }
    }
    
    // Default sources based on metric
    const metricSources = {
      'SUM(revenue)': 'fact_sales',
      'COUNT(DISTINCT customer_id)': 'fact_sales',
      'COUNT(DISTINCT order_id)': 'fact_orders',
      'SUM(quantity)': 'fact_sales',
      'SUM(profit)': 'fact_sales'
    };
    
    return metricSources[metric.metric] || 'fact_sales';
  }

  // Generate appropriate group_by columns
  generateGroupBy(metric, panelType) {
    const groupByOptions = {
      revenue: ['customer_name'],
      customers: ['region'],
      orders: ['customer_name'],
      quantity: ['product_name'],
      profit: ['customer_name']
    };
    
    const baseGroupBy = groupByOptions[metric.key] || ['customer_name'];
    
    // Add time dimension for timeseries
    if (panelType === 'timeseries') {
      return [...baseGroupBy, 'order_date'];
    }
    
    return baseGroupBy;
  }

  // Generate schedule from requirements
  generateSchedule(scheduleReq) {
    if (!scheduleReq) {
      // Default daily schedule
      return {
        mode: 'exact',
        cron_utc: '0 12 * * *' // 8am ET = 12pm UTC (simplified)
      };
    }
    
    if (scheduleReq.mode === 'exact') {
      let cronUtc = scheduleReq.cron_utc;
      
      // Parse user-specified time if available
      if (scheduleReq.extracted_time && !cronUtc) {
        cronUtc = this.parseTimeToUTCCron(scheduleReq.extracted_time);
      }
      
      return {
        mode: 'exact',
        cron_utc: cronUtc || '0 12 * * *'
      };
    } else if (scheduleReq.mode === 'freshness') {
      let targetLag = '1 hour';
      
      // Parse freshness requirement
      if (scheduleReq.extracted_requirement) {
        targetLag = this.parseFreshnessRequirement(scheduleReq.extracted_requirement);
      }
      
      return {
        mode: 'freshness',
        target_lag: targetLag
      };
    }
    
    // Fallback to default
    return {
      mode: 'exact',
      cron_utc: '0 12 * * *'
    };
  }

  // Parse user time to UTC cron
  parseTimeToUTCCron(timeStr) {
    const timeMatch = timeStr.match(/(\d{1,2}):?(\d{2})?\s*(am|pm)/i);
    if (!timeMatch) {
      return '0 12 * * *'; // Default noon UTC
    }
    
    let hour = parseInt(timeMatch[1]);
    const minute = parseInt(timeMatch[2] || '0');
    const ampm = timeMatch[3].toLowerCase();
    
    // Convert to 24-hour
    if (ampm === 'pm' && hour !== 12) {
      hour += 12;
    } else if (ampm === 'am' && hour === 12) {
      hour = 0;
    }
    
    // Convert ET to UTC (simplified - doesn't handle DST)
    const utcHour = (hour + 5) % 24; // ET is UTC-5
    
    return `${minute} ${utcHour} * * *`;
  }

  // Parse freshness requirement
  parseFreshnessRequirement(reqStr) {
    const validLags = [
      '15 minutes', '30 minutes', '1 hour', '2 hours', 
      '4 hours', '6 hours', '12 hours', '1 day'
    ];
    
    // Look for explicit lag mention
    for (const lag of validLags) {
      if (reqStr.toLowerCase().includes(lag)) {
        return lag;
      }
    }
    
    // Parse numeric requirements
    const numMatch = reqStr.match(/(\d+)\s*(minutes?|hours?)/i);
    if (numMatch) {
      const num = parseInt(numMatch[1]);
      const unit = numMatch[2].toLowerCase();
      
      if (unit.startsWith('minute')) {
        if (num <= 15) return '15 minutes';
        if (num <= 30) return '30 minutes';
        return '1 hour';
      } else if (unit.startsWith('hour')) {
        if (num <= 1) return '1 hour';
        if (num <= 2) return '2 hours';
        if (num <= 4) return '4 hours';
        if (num <= 6) return '6 hours';
        if (num <= 12) return '12 hours';
        return '1 day';
      }
    }
    
    return '1 hour'; // Default
  }

  // Generate fallback spec when primary generation fails
  generateFallbackSpec(requirements) {
    console.log('🔄 Generating fallback spec using template');
    
    // Use sales executive template as base
    const baseSpec = JSON.parse(JSON.stringify(EXAMPLE_SPECS.sales_executive));
    
    // Customize name if available
    if (requirements.name) {
      baseSpec.name = this.generateValidName(requirements.name);
    }
    
    // Customize timezone if available
    if (requirements.timezone) {
      baseSpec.timezone = requirements.timezone;
    }
    
    return baseSpec;
  }

  // Test spec generation with sample intent
  async testGeneration(sampleIntent) {
    try {
      const spec = await this.generateFromIntent(sampleIntent);
      return {
        success: true,
        spec: spec,
        validation: validateSpec(spec)
      };
    } catch (error) {
      return {
        success: false,
        error: error.message,
        intent: sampleIntent
      };
    }
  }

  // Get generator version and capabilities
  getVersion() {
    return {
      version: this.version,
      capabilities: {
        metric_mappings: Object.keys(this.metricMapping).length,
        panel_types: Object.keys(this.defaultPanelConfigs).length,
        schedule_modes: 2, // exact, freshness
        fallback_specs: Object.keys(EXAMPLE_SPECS).length
      }
    };
  }
}

module.exports = SpecGenerator;