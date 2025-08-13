// BI-First Smart Query Router
// Routes common BI patterns directly to SafeSQL templates for optimal performance

class BIQueryRouter {
  constructor() {
    // Define routing patterns for direct SafeSQL execution
    this.directRoutes = new Map([
      // Top N patterns
      [/(?:show|list|get).*(?:top|first)\s*(\d+)/i, { template: 'sample_top', extractor: this.extractTopN }],
      [/(?:top|first)\s*(\d+).*(?:activities|events|records)/i, { template: 'sample_top', extractor: this.extractTopN }],
      
      // Activity listing patterns
      [/(?:list|show|display).*(?:activities|events)/i, { template: 'sample_top', extractor: () => ({ n: 10 }) }],
      [/(?:recent|latest).*(?:activities|events)/i, { template: 'recent_activities', extractor: this.extractTimeRange }],
      
      // Time-based patterns
      [/(?:last|past|recent)\s*(\d+)\s*(?:hours?|hrs?)/i, { template: 'recent_activities', extractor: this.extractTimeRange }],
      [/(?:activity|activities).*(?:last|past)\s*(\d+)\s*(?:hours?|days?)/i, { template: 'recent_activities', extractor: this.extractTimeRange }],
      
      // Breakdown patterns
      [/(?:group|breakdown|categorize).*(?:by|activity|type)/i, { template: 'activity_by_type', extractor: () => ({ hours: 24 }) }],
      [/(?:activity|activities).*(?:type|category|breakdown)/i, { template: 'activity_by_type', extractor: () => ({ hours: 24 }) }],
      
      // Summary patterns
      [/(?:summary|summarize|overview).*(?:activity|activities)/i, { template: 'activity_summary', extractor: () => ({ hours: 24 }) }],
      [/(?:activity|activities).*(?:summary|overview)/i, { template: 'activity_summary', extractor: () => ({ hours: 24 }) }],
      
      // Count/aggregation patterns
      [/(?:count|how many).*(?:activities|events|records)/i, { template: 'activity_summary', extractor: () => ({ hours: 24 }) }]
    ]);

    // Performance tracking
    this.routeStats = {
      direct: { count: 0, totalTime: 0, totalCost: 0 },
      lite: { count: 0, totalTime: 0, totalCost: 0 },
      full: { count: 0, totalTime: 0, totalCost: 0 }
    };
  }

  // Main routing decision engine
  classify(query) {
    const normalizedQuery = query.trim().toLowerCase();
    
    // Check for direct SafeSQL patterns (Tier 1)
    for (const [pattern, config] of this.directRoutes) {
      const match = normalizedQuery.match(pattern);
      if (match) {
        const params = config.extractor(match, normalizedQuery);
        return {
          tier: 1,
          route: 'direct_safesql',
          template: config.template,
          params: { ...params, schema: 'ACTIVITY', table: 'EVENTS' },
          confidence: 0.95,
          expectedTime: 2000,
          expectedCost: 0.001,
          reasoning: `Direct pattern match: ${pattern}`
        };
      }
    }

    // Check if needs simple AI interpretation (Tier 2)
    if (this.needsLiteAI(normalizedQuery)) {
      return {
        tier: 2,
        route: 'lite_ai',
        confidence: 0.7,
        expectedTime: 8000,
        expectedCost: 0.05,
        reasoning: 'Needs AI interpretation but likely maps to SafeSQL'
      };
    }

    // Default to full Claude Code (Tier 3)
    return {
      tier: 3,
      route: 'full_claude',
      confidence: 0.5,
      expectedTime: 30000,
      expectedCost: 0.20,
      reasoning: 'Complex query requiring full Claude Code context'
    };
  }

  // Determine if query needs lite AI interpretation
  needsLiteAI(query) {
    const liteAIIndicators = [
      /(?:analyze|analysis)/i,
      /(?:compare|comparison|vs|versus)/i,
      /(?:trend|pattern|insight)/i,
      /(?:filter|where|condition)/i,
      /(?:explain|why|how)/i
    ];

    return liteAIIndicators.some(pattern => pattern.test(query)) &&
           !this.isComplexAnalysis(query);
  }

  // Determine if query requires full Claude Code
  isComplexAnalysis(query) {
    const complexIndicators = [
      /(?:report|document|write)/i,
      /(?:recommendation|suggest|advise)/i,
      /(?:correlation|relationship)/i,
      /(?:predict|forecast|model)/i,
      /(?:multiple|several|various).*(?:table|dataset)/i
    ];

    return complexIndicators.some(pattern => pattern.test(query));
  }

  // Parameter extractors for different patterns
  extractTopN(match, query) {
    const number = match[1] ? parseInt(match[1]) : 10;
    return { n: Math.min(Math.max(number, 1), 1000) }; // Cap between 1-1000
  }

  extractTimeRange(match, query) {
    const number = match[1] ? parseInt(match[1]) : 1;
    const unit = query.includes('day') ? 'days' : 'hours';
    
    if (unit === 'days') {
      return { hours: number * 24, limit: 100 };
    } else {
      return { hours: Math.min(Math.max(number, 1), 168), limit: 100 }; // Cap at 1 week
    }
  }

  // Track routing performance for analytics
  trackPerformance(route, duration, cost, success) {
    const tierName = route.tier === 1 ? 'direct' : route.tier === 2 ? 'lite' : 'full';
    const stats = this.routeStats[tierName];
    
    stats.count++;
    stats.totalTime += duration;
    stats.totalCost += cost || 0;
    
    // Log to Activity Schema (will be called by message router)
    return {
      activity: 'ccode.query_routed',
      feature_json: {
        tier: route.tier,
        route_type: route.route,
        template: route.template || null,
        duration_ms: duration,
        cost_usd: cost || 0,
        success: success,
        confidence: route.confidence,
        reasoning: route.reasoning,
        expected_time: route.expectedTime,
        expected_cost: route.expectedCost,
        performance_ratio: duration / route.expectedTime
      }
    };
  }

  // Get performance statistics
  getStats() {
    const total = Object.values(this.routeStats).reduce((sum, stat) => sum + stat.count, 0);
    
    return {
      total_queries: total,
      tier_breakdown: Object.keys(this.routeStats).map(tier => ({
        tier,
        count: this.routeStats[tier].count,
        percentage: total > 0 ? Math.round((this.routeStats[tier].count / total) * 100) : 0,
        avg_time: this.routeStats[tier].count > 0 ? 
          Math.round(this.routeStats[tier].totalTime / this.routeStats[tier].count) : 0,
        avg_cost: this.routeStats[tier].count > 0 ? 
          (this.routeStats[tier].totalCost / this.routeStats[tier].count).toFixed(4) : 0
      })),
      performance_summary: {
        fastest_avg_time: Math.min(...Object.values(this.routeStats).map(s => 
          s.count > 0 ? s.totalTime / s.count : Infinity)),
        total_cost_saved: this.estimateCostSavings(),
        total_time_saved: this.estimateTimeSavings()
      }
    };
  }

  // Estimate cost savings from direct routing
  estimateCostSavings() {
    const directQueries = this.routeStats.direct.count;
    const avgDirectCost = directQueries > 0 ? this.routeStats.direct.totalCost / directQueries : 0;
    const estimatedFullCost = 0.20; // Average full Claude Code cost
    
    return directQueries * (estimatedFullCost - avgDirectCost);
  }

  // Estimate time savings from direct routing
  estimateTimeSavings() {
    const directQueries = this.routeStats.direct.count;
    const avgDirectTime = directQueries > 0 ? this.routeStats.direct.totalTime / directQueries : 0;
    const estimatedFullTime = 30000; // Average full Claude Code time (30s)
    
    return directQueries * (estimatedFullTime - avgDirectTime);
  }

  // Get routing suggestions for query optimization
  getSuggestions(query) {
    const route = this.classify(query);
    const suggestions = [];

    if (route.tier > 1) {
      // Suggest more direct alternatives
      for (const [pattern, config] of this.directRoutes) {
        const similarity = this.calculateSimilarity(query, pattern);
        if (similarity > 0.3) {
          suggestions.push({
            template: config.template,
            suggestion: `Try: /sql ${config.template} ${Object.entries(config.extractor()).map(([k,v]) => `${k}=${v}`).join(' ')}`,
            similarity: similarity
          });
        }
      }
    }

    return {
      classification: route,
      suggestions: suggestions.sort((a, b) => b.similarity - a.similarity).slice(0, 3)
    };
  }

  // Simple similarity calculation for suggestions
  calculateSimilarity(query, pattern) {
    // This is a simplified implementation - could be enhanced with more sophisticated NLP
    const patternStr = pattern.toString().toLowerCase();
    const queryWords = query.toLowerCase().split(/\s+/);
    const patternWords = patternStr.match(/\w+/g) || [];
    
    const overlap = queryWords.filter(word => 
      patternWords.some(pWord => pWord.includes(word) || word.includes(pWord))
    ).length;
    
    return overlap / Math.max(queryWords.length, patternWords.length);
  }
}

module.exports = BIQueryRouter;