/**
 * Natural Language to Procedure Parameters Compiler
 * Converts user text into stored procedure calls - no ad-hoc SQL
 */

class NLCompiler {
  constructor() {
    // Time patterns
    this.timePatterns = {
      // Relative time
      'last (\\d+) hours?': (match) => ({
        start_ts: `DATEADD(hour, -${match[1]}, CURRENT_TIMESTAMP())`,
        end_ts: 'CURRENT_TIMESTAMP()'
      }),
      'last (\\d+) days?': (match) => ({
        start_ts: `DATEADD(day, -${match[1]}, CURRENT_TIMESTAMP())`,
        end_ts: 'CURRENT_TIMESTAMP()'
      }),
      'last (\\d+) minutes?': (match) => ({
        start_ts: `DATEADD(minute, -${match[1]}, CURRENT_TIMESTAMP())`,
        end_ts: 'CURRENT_TIMESTAMP()'
      }),
      'today': () => ({
        start_ts: 'DATE_TRUNC(day, CURRENT_TIMESTAMP())',
        end_ts: 'CURRENT_TIMESTAMP()'
      }),
      'yesterday': () => ({
        start_ts: 'DATEADD(day, -1, DATE_TRUNC(day, CURRENT_TIMESTAMP()))',
        end_ts: 'DATE_TRUNC(day, CURRENT_TIMESTAMP())'
      }),
      'this week': () => ({
        start_ts: 'DATE_TRUNC(week, CURRENT_DATE())',
        end_ts: 'CURRENT_TIMESTAMP()'
      }),
      'this month': () => ({
        start_ts: 'DATE_TRUNC(month, CURRENT_DATE())',
        end_ts: 'CURRENT_TIMESTAMP()'
      })
    };

    // Interval patterns
    this.intervalPatterns = {
      'by (\\d+) minutes?': (match) => `${match[1]} minute`,
      'by hour': () => 'hour',
      'by day': () => 'day',
      'by week': () => 'week',
      'hourly': () => 'hour',
      'daily': () => 'day',
      'every (\\d+) minutes?': (match) => `${match[1]} minute`,
      '(\\d+)[-\\s]?min(?:ute)?': (match) => `${match[1]} minute`,
      '15[-\\s]?min': () => '15 minute',
      '5[-\\s]?min': () => '5 minute'
    };

    // Dimension patterns for top-N
    this.dimensionPatterns = {
      'top actions?': () => 'action',
      'top users?': () => 'actor_id',
      'top customers?': () => 'actor_id',
      'top errors?': () => ({ dimension: 'action', filter: { action: "LIKE 'error.%'" } }),
      'most frequent (.+)': (match) => this.normalizeDimension(match[1]),
      'by action': () => 'action',
      'by user': () => 'actor_id',
      'by type': () => 'action'
    };

    // Query type patterns
    this.queryTypePatterns = {
      time_series: [
        'show .* over time',
        'trend',
        'timeline',
        'by (hour|day|minute)',
        'events? (by|per|every)',
        'over the last',
        'time series'
      ],
      top_n: [
        'top \\d+',
        'most (frequent|common|active)',
        'ranking',
        'leaderboard',
        'highest',
        'top'
      ],
      metrics: [
        'summary',
        'total',
        'count',
        'statistics',
        'metrics',
        'kpi',
        'overview'
      ],
      live: [
        'live',
        'real[-\\s]?time',
        'stream',
        'latest',
        'recent activity'
      ]
    };
  }

  /**
   * Main compile method - converts natural language to procedure call
   */
  compile(text) {
    const normalized = text.toLowerCase().trim();
    
    // Determine query type
    const queryType = this.detectQueryType(normalized);
    
    // Compile based on type
    switch (queryType) {
      case 'time_series':
        return this.compileTimeSeries(normalized);
      case 'top_n':
        return this.compileTopN(normalized);
      case 'metrics':
        return this.compileMetrics(normalized);
      case 'live':
        return this.compileLive(normalized);
      default:
        // Default to metrics if unclear
        return this.compileMetrics(normalized);
    }
  }

  /**
   * Detect the type of query from the text
   */
  detectQueryType(text) {
    for (const [type, patterns] of Object.entries(this.queryTypePatterns)) {
      for (const pattern of patterns) {
        if (new RegExp(pattern).test(text)) {
          return type;
        }
      }
    }
    return 'metrics'; // Default
  }

  /**
   * Compile time series query
   */
  compileTimeSeries(text) {
    const result = {
      proc: 'DASH_GET_SERIES',
      params: {
        start_ts: 'DATEADD(hour, -24, CURRENT_TIMESTAMP())',
        end_ts: 'CURRENT_TIMESTAMP()',
        interval_str: 'hour',
        filters: null,
        group_by: null
      }
    };

    // Extract time range
    const timeRange = this.extractTimeRange(text);
    if (timeRange) {
      result.params.start_ts = timeRange.start_ts;
      result.params.end_ts = timeRange.end_ts;
    }

    // Extract interval
    const interval = this.extractInterval(text);
    if (interval) {
      result.params.interval_str = interval;
    }

    // Extract group by
    if (text.includes('by action')) {
      result.params.group_by = 'action';
    } else if (text.includes('by user') || text.includes('by customer')) {
      result.params.group_by = 'actor_id';
    }

    // Extract filters
    const filters = this.extractFilters(text);
    if (filters) {
      result.params.filters = filters;
    }

    return result;
  }

  /**
   * Compile top-N query
   */
  compileTopN(text) {
    const result = {
      proc: 'DASH_GET_TOPN',
      params: {
        start_ts: 'DATEADD(hour, -24, CURRENT_TIMESTAMP())',
        end_ts: 'CURRENT_TIMESTAMP()',
        dimension: 'action',
        filters: null,
        n: 10
      }
    };

    // Extract time range
    const timeRange = this.extractTimeRange(text);
    if (timeRange) {
      result.params.start_ts = timeRange.start_ts;
      result.params.end_ts = timeRange.end_ts;
    }

    // Extract N value
    const nMatch = text.match(/top (\d+)/);
    if (nMatch) {
      result.params.n = parseInt(nMatch[1]);
    }

    // Extract dimension
    for (const [pattern, extractor] of Object.entries(this.dimensionPatterns)) {
      const regex = new RegExp(pattern);
      const match = text.match(regex);
      if (match) {
        const extracted = extractor(match);
        if (typeof extracted === 'object') {
          result.params.dimension = extracted.dimension;
          result.params.filters = extracted.filter;
        } else {
          result.params.dimension = extracted;
        }
        break;
      }
    }

    return result;
  }

  /**
   * Compile metrics/summary query
   */
  compileMetrics(text) {
    const result = {
      proc: 'DASH_GET_METRICS',
      params: {
        start_ts: 'DATEADD(hour, -24, CURRENT_TIMESTAMP())',
        end_ts: 'CURRENT_TIMESTAMP()',
        filters: null
      }
    };

    // Extract time range
    const timeRange = this.extractTimeRange(text);
    if (timeRange) {
      result.params.start_ts = timeRange.start_ts;
      result.params.end_ts = timeRange.end_ts;
    }

    // Extract filters
    const filters = this.extractFilters(text);
    if (filters) {
      result.params.filters = filters;
    }

    return result;
  }

  /**
   * Compile live stream query
   */
  compileLive(text) {
    const result = {
      proc: 'DASH_GET_EVENTS',
      params: {
        cursor_ts: 'DATEADD(minute, -5, CURRENT_TIMESTAMP())',
        limit_rows: 50
      }
    };

    // Extract limit if specified
    const limitMatch = text.match(/last (\d+) events?/);
    if (limitMatch) {
      result.params.limit_rows = parseInt(limitMatch[1]);
    }

    return result;
  }

  /**
   * Extract time range from text
   */
  extractTimeRange(text) {
    for (const [pattern, extractor] of Object.entries(this.timePatterns)) {
      const regex = new RegExp(pattern);
      const match = text.match(regex);
      if (match) {
        return extractor(match);
      }
    }
    return null;
  }

  /**
   * Extract interval from text
   */
  extractInterval(text) {
    for (const [pattern, extractor] of Object.entries(this.intervalPatterns)) {
      const regex = new RegExp(pattern);
      const match = text.match(regex);
      if (match) {
        return extractor(match);
      }
    }
    return null;
  }

  /**
   * Extract filters from text
   */
  extractFilters(text) {
    const filters = {};

    // Error filter
    if (text.includes('error') || text.includes('fail')) {
      filters.action = "LIKE 'error.%'";
    }

    // User filter
    const userMatch = text.match(/for user (\w+)/);
    if (userMatch) {
      filters.actor_id = userMatch[1];
    }

    // Action filter
    const actionMatch = text.match(/action[s]? (?:like|=) ['"]?(\w+)['"]?/);
    if (actionMatch) {
      filters.action = actionMatch[1];
    }

    return Object.keys(filters).length > 0 ? filters : null;
  }

  /**
   * Normalize dimension names
   */
  normalizeDimension(text) {
    const mappings = {
      'actions': 'action',
      'users': 'actor_id',
      'customers': 'actor_id',
      'events': 'action',
      'types': 'action'
    };

    const normalized = text.toLowerCase().trim();
    return mappings[normalized] || normalized;
  }

  /**
   * Generate a title for the query
   */
  generateTitle(compiledQuery) {
    const { proc, params } = compiledQuery;
    
    switch (proc) {
      case 'DASH_GET_SERIES':
        return `Activity over ${params.interval_str}`;
      case 'DASH_GET_TOPN':
        return `Top ${params.n} ${params.dimension}`;
      case 'DASH_GET_METRICS':
        return 'Summary Metrics';
      case 'DASH_GET_EVENTS':
        return 'Live Activity Stream';
      default:
        return 'Dashboard Query';
    }
  }
}

// Export for use in other modules
if (typeof module !== 'undefined' && module.exports) {
  module.exports = NLCompiler;
}