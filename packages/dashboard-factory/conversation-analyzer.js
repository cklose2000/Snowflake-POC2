// Conversation Analyzer - Detects dashboard intent from BI conversation history

class ConversationAnalyzer {
  constructor() {
    this.version = '1.0.0';
    
    // Dashboard intent patterns
    this.dashboardPatterns = [
      // Direct dashboard requests
      /(?:build|create|make|generate).*dashboard/i,
      /dashboard.*(?:showing|with|for)/i,
      /I need a dashboard/i,
      
      // Visualization requests
      /(?:visualize|chart|graph|plot).*(?:this|these|data)/i,
      /show.*(?:trends|over time|by month|by quarter)/i,
      
      // Executive/reporting language
      /(?:executive|management|board) (?:dashboard|report)/i,
      /(?:kpi|metrics|performance) dashboard/i,
      /(?:sales|revenue|customer) dashboard/i,
      
      // Multiple metric indicators
      /(?:track|monitor|watch).*(?:and|,).*(?:and|,)/i, // "track sales and customers and..."
      /(?:top \d+|bottom \d+).*(?:and|,).*(?:trends|over)/i // "top 10 and trends over..."
    ];
    
    // Anti-patterns (things that look like dashboard requests but aren't)
    this.antiPatterns = [
      /just show me/i,
      /quick question/i,
      /what is/i,
      /how do I/i,
      /help me understand/i
    ];
    
    // Metric extraction patterns
    this.metricPatterns = {
      revenue: /(?:revenue|sales|income|earnings)/i,
      customers: /(?:customers?|clients?|accounts?)/i,
      orders: /(?:orders?|purchases?|transactions?)/i,
      quantity: /(?:quantity|volume|amount|count)/i,
      profit: /(?:profit|margin|earnings)/i,
      growth: /(?:growth|increase|trend)/i
    };
    
    // Time window patterns
    this.timePatterns = {
      days: /(?:last|past)\s+(\d+)\s+days?/i,
      weeks: /(?:last|past)\s+(\d+)\s+weeks?/i,
      months: /(?:last|past)\s+(\d+)\s+months?/i,
      quarters: /(?:last|past)\s+(\d+)\s+quarters?/i,
      years: /(?:last|past)\s+(\d+)\s+years?/i
    };
    
    // Panel type indicators
    this.panelTypePatterns = {
      table: /(?:list|table|top \d+|bottom \d+)/i,
      timeseries: /(?:trends?|over time|by (?:month|quarter|week|day))/i,
      metric: /(?:total|sum|average|count)/i,
      chart: /(?:chart|graph|plot|visualize)/i
    };
  }

  // Main analysis method
  async analyzeDashboardIntent(conversationHistory) {
    console.log(`🔍 Analyzing conversation for dashboard intent (${conversationHistory.length} messages)`);
    
    // Combine all user messages into analysis text
    const userMessages = conversationHistory
      .filter(msg => msg.type === 'user' || msg.role === 'user')
      .map(msg => msg.content || msg.message)
      .join(' ');
    
    if (!userMessages.trim()) {
      return { isDashboardRequest: false, confidence: 0, reason: 'No user messages found' };
    }
    
    // Calculate dashboard intent confidence
    const intentScore = this.calculateDashboardIntentScore(userMessages);
    
    if (intentScore.total < 0.3) {
      return {
        isDashboardRequest: false,
        confidence: Math.round(intentScore.total * 100),
        reason: 'Insufficient dashboard indicators',
        details: intentScore
      };
    }
    
    // Extract dashboard requirements
    const requirements = this.extractDashboardRequirements(userMessages, conversationHistory);
    
    return {
      isDashboardRequest: true,
      confidence: Math.round(intentScore.total * 100),
      reason: `Dashboard intent detected with ${intentScore.indicators.length} indicators`,
      requirements: requirements,
      details: intentScore,
      analysis: {
        user_messages_analyzed: conversationHistory.length,
        key_phrases: intentScore.matches,
        suggested_metrics: requirements.metrics,
        suggested_panels: requirements.panels
      }
    };
  }

  // Calculate confidence score for dashboard intent
  calculateDashboardIntentScore(text) {
    const indicators = [];
    const matches = [];
    let score = 0;
    
    // Check for explicit dashboard patterns
    for (const pattern of this.dashboardPatterns) {
      const match = text.match(pattern);
      if (match) {
        indicators.push('explicit_dashboard_request');
        matches.push(match[0]);
        score += 0.4; // High weight for explicit requests
        break; // Only count once
      }
    }
    
    // Check for anti-patterns (reduce confidence)
    for (const pattern of this.antiPatterns) {
      if (pattern.test(text)) {
        indicators.push('anti_pattern_detected');
        score -= 0.2;
        break;
      }
    }
    
    // Check for multiple metrics (indicator of dashboard need)
    const metricCount = Object.keys(this.metricPatterns).filter(metric => 
      this.metricPatterns[metric].test(text)
    ).length;
    
    if (metricCount >= 2) {
      indicators.push('multiple_metrics');
      matches.push(`${metricCount} metrics detected`);
      score += 0.3;
    } else if (metricCount === 1) {
      score += 0.1;
    }
    
    // Check for time-based analysis (common in dashboards)
    for (const [unit, pattern] of Object.entries(this.timePatterns)) {
      if (pattern.test(text)) {
        indicators.push('time_analysis');
        matches.push(`${unit} time window`);
        score += 0.2;
        break;
      }
    }
    
    // Check for "top N" patterns (common dashboard element)
    const topNMatch = text.match(/(?:top|bottom)\s+(\d+)/i);
    if (topNMatch) {
      indicators.push('top_n_analysis');
      matches.push(`top ${topNMatch[1]} analysis`);
      score += 0.2;
    }
    
    // Check for visualization language
    const vizPatterns = [/visualize/i, /chart/i, /graph/i, /plot/i];
    if (vizPatterns.some(pattern => pattern.test(text))) {
      indicators.push('visualization_request');
      matches.push('visualization language');
      score += 0.25;
    }
    
    return {
      total: Math.min(score, 1.0), // Cap at 100%
      indicators: indicators,
      matches: matches,
      metric_count: metricCount
    };
  }

  // Extract specific requirements from conversation
  extractDashboardRequirements(userMessages, conversationHistory) {
    const requirements = {
      name: this.extractDashboardName(userMessages),
      timezone: this.extractTimezone(userMessages),
      metrics: this.extractMetrics(userMessages),
      panels: this.extractPanels(userMessages),
      schedule: this.extractSchedulePreference(userMessages),
      sources: this.extractDataSources(conversationHistory)
    };
    
    return requirements;
  }

  // Extract dashboard name from conversation
  extractDashboardName(text) {
    // Look for explicit naming
    const namePatterns = [
      /(?:call|name) (?:it|this|the dashboard) (.+?)(?:\.|$)/i,
      /(?:executive|sales|ops|customer) dashboard/i,
      /dashboard (?:for|showing) (.+?)(?:\.|$)/i
    ];
    
    for (const pattern of namePatterns) {
      const match = text.match(pattern);
      if (match) {
        const name = match[1] || match[0];
        return name.toLowerCase()
          .replace(/[^a-z0-9\s]/g, '')
          .replace(/\s+/g, '_')
          .substring(0, 32);
      }
    }
    
    // Default names based on detected metrics
    const metrics = this.extractMetrics(text);
    if (metrics.includes('revenue') || metrics.includes('sales')) {
      return 'sales_dashboard';
    } else if (metrics.includes('customers')) {
      return 'customer_dashboard';
    } else {
      return 'exec_dashboard';
    }
  }

  // Extract timezone preference (default to ET for now)
  extractTimezone(text) {
    const timezonePatterns = {
      'America/New_York': /(?:eastern|et|est|edt|new york)/i,
      'America/Chicago': /(?:central|ct|cst|cdt|chicago)/i,
      'America/Denver': /(?:mountain|mt|mst|mdt|denver)/i,
      'America/Los_Angeles': /(?:pacific|pt|pst|pdt|los angeles)/i,
      'UTC': /(?:utc|gmt|universal)/i
    };
    
    for (const [timezone, pattern] of Object.entries(timezonePatterns)) {
      if (pattern.test(text)) {
        return timezone;
      }
    }
    
    return 'America/New_York'; // Default to Eastern
  }

  // Extract metrics from conversation
  extractMetrics(text) {
    const detectedMetrics = [];
    
    for (const [metric, pattern] of Object.entries(this.metricPatterns)) {
      if (pattern.test(text)) {
        detectedMetrics.push(metric);
      }
    }
    
    // Map to allowed SafeSQL metrics
    const metricMapping = {
      revenue: 'SUM(revenue)',
      customers: 'COUNT(DISTINCT customer_id)',
      orders: 'COUNT(DISTINCT order_id)',
      quantity: 'SUM(quantity)',
      profit: 'SUM(profit)'
    };
    
    return detectedMetrics.map(metric => metricMapping[metric]).filter(Boolean);
  }

  // Extract panel requirements
  extractPanels(text) {
    const panels = [];
    
    // Detect top-N tables
    const topNMatch = text.match(/(?:top|bottom)\s+(\d+)/i);
    if (topNMatch) {
      panels.push({
        type: 'table',
        requirement: `Top ${topNMatch[1]} analysis`,
        top_n: parseInt(topNMatch[1])
      });
    }
    
    // Detect time series
    if (/(?:trends?|over time|by (?:month|quarter))/i.test(text)) {
      panels.push({
        type: 'timeseries',
        requirement: 'Trend analysis over time'
      });
    }
    
    // Default to table if no specific type detected
    if (panels.length === 0) {
      panels.push({
        type: 'table',
        requirement: 'Basic metric display',
        top_n: 10
      });
    }
    
    return panels;
  }

  // Extract schedule preferences
  extractSchedulePreference(text) {
    // Look for exact time mentions
    const exactTimePatterns = [
      /(?:at|by) (\d{1,2}):?(\d{2})?\s*(?:am|pm)/i,
      /(\d{1,2}):(\d{2})\s*(?:am|pm)/i,
      /daily at/i,
      /every morning/i
    ];
    
    for (const pattern of exactTimePatterns) {
      const match = text.match(pattern);
      if (match) {
        return {
          mode: 'exact',
          preference: 'user_specified_time',
          extracted_time: match[0]
        };
      }
    }
    
    // Look for freshness indicators
    const freshnessPatterns = [
      /(?:fresh|current|real.?time|up.?to.?date)/i,
      /within (\d+) (?:minutes?|hours?)/i,
      /refresh.*(\d+) (?:minutes?|hours?)/i
    ];
    
    for (const pattern of freshnessPatterns) {
      if (pattern.test(text)) {
        return {
          mode: 'freshness',
          preference: 'data_freshness',
          extracted_requirement: text.match(pattern)[0]
        };
      }
    }
    
    // Default to daily morning refresh
    return {
      mode: 'exact',
      preference: 'default_daily',
      cron_utc: '0 12 * * *' // 8am ET = 12pm UTC (simplified)
    };
  }

  // Extract data sources from SafeSQL conversation history
  extractDataSources(conversationHistory) {
    const sources = [];
    
    // Look for successful SafeSQL template executions
    conversationHistory.forEach(msg => {
      if (msg.type === 'sql-result' && msg.template) {
        // These would be from the existing BI router
        sources.push({
          template: msg.template,
          success: true,
          row_count: msg.count
        });
      }
    });
    
    // Default source table if none detected
    if (sources.length === 0) {
      sources.push({
        table: 'fact_sales', // Default assumption
        inferred: true
      });
    }
    
    return sources;
  }

  // Get analyzer version and capabilities
  getVersion() {
    return {
      version: this.version,
      capabilities: {
        dashboard_patterns: this.dashboardPatterns.length,
        metric_patterns: Object.keys(this.metricPatterns).length,
        time_patterns: Object.keys(this.timePatterns).length,
        panel_types: Object.keys(this.panelTypePatterns).length
      }
    };
  }

  // Test method for development
  testAnalysis(sampleText) {
    const score = this.calculateDashboardIntentScore(sampleText);
    const requirements = this.extractDashboardRequirements(sampleText, []);
    
    return {
      input: sampleText,
      score: score,
      requirements: requirements
    };
  }
}

module.exports = ConversationAnalyzer;