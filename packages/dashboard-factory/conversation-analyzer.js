// Conversation Analyzer - Detects Activity dashboard intent from conversation history
// v2.0.0 - Activity-native patterns (no fake tables!)

class ConversationAnalyzer {
  constructor() {
    this.version = '2.0.0';
    
    // Activity dashboard intent patterns (primary patterns for v1)
    this.activityDashboardPatterns = [
      // Direct Activity dashboard requests
      /(?:show|create|build).*activity.*(?:dashboard|metrics|patterns)/i,
      /dashboard.*(?:activity|activities|events|operations)/i,
      /activity.*(?:breakdown|summary|overview)/i,
      
      // LLM/Claude performance requests
      /(?:claude|llm|model).*(?:performance|latency|usage)/i,
      /(?:token|tokens).*(?:usage|consumption|cost)/i,
      /response.*(?:time|latency|speed)/i,
      
      // SQL/Query performance requests
      /(?:sql|query|queries).*(?:performance|cost|metrics)/i,
      /(?:bytes|data).*(?:scanned|processed|cost)/i,
      /query.*(?:patterns|usage|history)/i,
      
      // Dashboard operations requests
      /dashboard.*(?:operations|lifecycle|metrics)/i,
      /(?:created|refreshed|destroyed).*dashboards/i,
      
      // SafeSQL template requests
      /(?:template|safesql).*(?:usage|patterns|popular)/i,
      /which.*templates.*(?:used|popular|common)/i,
      
      // Generic Activity requests
      /(?:telemetry|metrics|operations).*dashboard/i,
      /(?:monitor|track|watch).*(?:activity|usage|performance)/i
    ];
    
    // Legacy patterns (for SAMPLES schema - Phase 1 only)
    this.legacyDashboardPatterns = [
      /(?:sales|revenue|customer).*dashboard/i,
      /(?:build|create).*(?:sales|revenue).*(?:dashboard|report)/i
    ];
    
    // Anti-patterns (not dashboard requests)
    this.antiPatterns = [
      /just show me/i,
      /quick question/i,
      /what is/i,
      /how do I/i,
      /help me understand/i,
      /can you explain/i
    ];
    
    // Activity metric patterns
    this.activityMetricPatterns = {
      events: /(?:events?|activities|activity|actions?)/i,
      llm_latency: /(?:latency|response time|speed)/i,
      llm_tokens: /(?:tokens?|token usage|consumption)/i,
      sql_cost: /(?:bytes|cost|data scanned)/i,
      sql_performance: /(?:query|sql).*(?:performance|duration|time)/i,
      dashboard_ops: /dashboard.*(?:operations?|created|refreshed)/i,
      template_usage: /(?:template|safesql).*(?:usage|used|popular)/i
    };
    
    // Time window patterns (fixed windows for v1)
    this.timeWindows = {
      '24h': /(?:last|past)?\s*24\s*hours?/i,
      '7d': /(?:last|past)?\s*(?:7\s*days?|week)/i,
      '30d': /(?:last|past)?\s*(?:30\s*days?|month)/i,
      'all': /all\s*time/i
    };
    
    // Panel type detection
    this.panelTypeIndicators = {
      bar: /(?:top \d+|breakdown|by type|by activity)/i,
      timeseries: /(?:over time|trends?|by (?:hour|day|week))/i,
      histogram: /(?:distribution|histogram|latency)/i,
      metrics: /(?:summary|overview|total|count)/i
    };
  }

  // Main analysis method - Activity patterns first!
  async analyzeDashboardIntent(conversationHistory) {
    console.log(`🔍 Analyzing conversation for dashboard intent (${conversationHistory.length} messages)`);
    
    // Combine all user messages
    const userMessages = conversationHistory
      .filter(msg => msg.type === 'user' || msg.role === 'user')
      .map(msg => msg.content || msg.message)
      .join(' ');
    
    if (!userMessages.trim()) {
      return { 
        isDashboardRequest: false, 
        confidence: 0, 
        reason: 'No user messages found' 
      };
    }
    
    // Check for Activity dashboard patterns FIRST
    const activityScore = this.calculateActivityIntentScore(userMessages);
    
    // Only check legacy patterns if explicitly requested
    const legacyScore = this.calculateLegacyIntentScore(userMessages);
    
    // Determine overall intent
    const isActivityDashboard = activityScore >= 30;
    const isLegacyDashboard = legacyScore >= 50 && userMessages.toLowerCase().includes('sample');
    
    if (!isActivityDashboard && !isLegacyDashboard) {
      return {
        isDashboardRequest: false,
        confidence: Math.max(activityScore, legacyScore),
        reason: 'No clear dashboard intent detected'
      };
    }
    
    // Extract requirements based on type
    const requirements = isActivityDashboard 
      ? await this.extractActivityRequirements(userMessages)
      : await this.extractLegacyRequirements(userMessages);
    
    return {
      isDashboardRequest: true,
      confidence: isActivityDashboard ? activityScore : legacyScore,
      type: isActivityDashboard ? 'activity' : 'legacy',
      requirements: requirements,
      rawText: userMessages
    };
  }

  // Calculate Activity dashboard intent score
  calculateActivityIntentScore(text) {
    let score = 0;
    
    // Check for Activity dashboard patterns
    this.activityDashboardPatterns.forEach(pattern => {
      if (pattern.test(text)) {
        score += 40;
      }
    });
    
    // Check for Activity metrics
    Object.values(this.activityMetricPatterns).forEach(pattern => {
      if (pattern.test(text)) {
        score += 20;
      }
    });
    
    // Bonus for multiple Activity indicators
    const activityKeywords = ['activity', 'events', 'llm', 'claude', 'sql', 'query', 'template', 'telemetry'];
    const keywordCount = activityKeywords.filter(kw => text.toLowerCase().includes(kw)).length;
    score += keywordCount * 10;
    
    // Reduce score for anti-patterns
    this.antiPatterns.forEach(pattern => {
      if (pattern.test(text)) {
        score -= 20;
      }
    });
    
    // Cap at 100
    return Math.min(100, Math.max(0, score));
  }

  // Calculate legacy dashboard intent score (for SAMPLES schema)
  calculateLegacyIntentScore(text) {
    let score = 0;
    
    // Only score high if explicitly asking for sales/revenue dashboards
    this.legacyDashboardPatterns.forEach(pattern => {
      if (pattern.test(text)) {
        score += 30;
      }
    });
    
    // Must explicitly mention sample/demo data
    if (/(?:sample|demo|test|example)/i.test(text)) {
      score += 20;
    }
    
    return Math.min(100, Math.max(0, score));
  }

  // Extract Activity dashboard requirements
  async extractActivityRequirements(text) {
    const requirements = {
      name: this.extractDashboardName(text) || 'activity_dashboard',
      metrics: [],
      panels: [],
      timeWindow: this.extractTimeWindow(text),
      schedule: this.extractSchedule(text),
      timezone: this.extractTimezone(text)
    };
    
    // Extract Activity metrics
    Object.entries(this.activityMetricPatterns).forEach(([metric, pattern]) => {
      if (pattern.test(text)) {
        requirements.metrics.push(metric);
      }
    });
    
    // Default to events if no metrics detected
    if (requirements.metrics.length === 0) {
      requirements.metrics.push('events');
    }
    
    // Detect panel types
    Object.entries(this.panelTypeIndicators).forEach(([type, pattern]) => {
      if (pattern.test(text)) {
        requirements.panels.push({ type });
      }
    });
    
    // Default panels if none detected
    if (requirements.panels.length === 0) {
      requirements.panels.push({ type: 'bar' });
      requirements.panels.push({ type: 'metrics' });
    }
    
    return requirements;
  }

  // Extract legacy requirements (Phase 1 only)
  async extractLegacyRequirements(text) {
    return {
      name: 'sample_dashboard',
      metrics: ['revenue', 'customers'],
      panels: [{ type: 'table' }],
      sources: [{ table: 'SAMPLES.FACT_SALES' }],
      note: 'Legacy dashboard using SAMPLES schema'
    };
  }

  // Extract dashboard name from text
  extractDashboardName(text) {
    // Look for explicit name patterns
    const namePatterns = [
      /dashboard (?:called|named) ["']?([^"']+)["']?/i,
      /["']([^"']+)["'] dashboard/i,
      /create (?:a|an) ([a-z_]+) dashboard/i
    ];
    
    for (const pattern of namePatterns) {
      const match = text.match(pattern);
      if (match && match[1]) {
        return match[1].toLowerCase().replace(/[^a-z0-9_]/g, '_');
      }
    }
    
    // Generate name from detected metrics
    if (text.includes('activity')) return 'activity_dashboard';
    if (text.includes('llm') || text.includes('claude')) return 'llm_performance';
    if (text.includes('sql') || text.includes('query')) return 'sql_analytics';
    if (text.includes('template')) return 'template_usage';
    
    return null;
  }

  // Extract time window (fixed windows for v1)
  extractTimeWindow(text) {
    for (const [window, pattern] of Object.entries(this.timeWindows)) {
      if (pattern.test(text)) {
        return { window };
      }
    }
    // Default to 24h
    return { window: '24h' };
  }

  // Extract schedule requirements
  extractSchedule(text) {
    const schedule = {
      enabled: false,
      frequency: null,
      exact_time: null
    };
    
    // Check for refresh/schedule patterns
    if (/(?:refresh|update|run).*(?:daily|every day)/i.test(text)) {
      schedule.enabled = true;
      schedule.frequency = 'daily';
    } else if (/(?:refresh|update|run).*(?:hourly|every hour)/i.test(text)) {
      schedule.enabled = true;
      schedule.frequency = 'hourly';
    }
    
    // Extract specific time
    const timeMatch = text.match(/at (\d{1,2}):?(\d{2})?\s*(am|pm|AM|PM)?/i);
    if (timeMatch) {
      schedule.exact_time = this.parseTime(timeMatch);
    }
    
    // Default to 8am for daily schedules without specific time
    if (schedule.frequency === 'daily' && !schedule.exact_time) {
      schedule.exact_time = '08:00';
    }
    
    return schedule;
  }

  // Extract timezone
  extractTimezone(text) {
    const timezones = {
      'et': 'America/New_York',
      'eastern': 'America/New_York',
      'ct': 'America/Chicago',
      'central': 'America/Chicago',
      'mt': 'America/Denver',
      'mountain': 'America/Denver',
      'pt': 'America/Los_Angeles',
      'pacific': 'America/Los_Angeles',
      'utc': 'UTC'
    };
    
    const lowercaseText = text.toLowerCase();
    for (const [key, tz] of Object.entries(timezones)) {
      if (lowercaseText.includes(key)) {
        return tz;
      }
    }
    
    // Default to ET
    return 'America/New_York';
  }

  // Parse time from regex match
  parseTime(match) {
    let hours = parseInt(match[1]);
    const minutes = match[2] ? parseInt(match[2]) : 0;
    const meridiem = match[3];
    
    if (meridiem) {
      if (meridiem.toLowerCase() === 'pm' && hours < 12) {
        hours += 12;
      } else if (meridiem.toLowerCase() === 'am' && hours === 12) {
        hours = 0;
      }
    }
    
    return `${hours.toString().padStart(2, '0')}:${minutes.toString().padStart(2, '0')}`;
  }

  // Get analyzer version
  getVersion() {
    return this.version;
  }
}

module.exports = ConversationAnalyzer;