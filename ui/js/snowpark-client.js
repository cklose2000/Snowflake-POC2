/**
 * Snowpark MCP Client
 * Connects to MCP server running in Snowpark Container Services
 */

class SnowparkMCPClient {
  constructor(config = {}) {
    // In production, this would be the Snowflake proxy endpoint
    // Format: https://<account>.snowflakecomputing.com/api/services/CLAUDE_BI/PUBLIC/MCP_SERVER
    this.baseUrl = config.baseUrl || process.env.SNOWPARK_MCP_URL || 'http://localhost:8080';
    this.token = config.token || null;
    this.debug = config.debug || false;
  }

  /**
   * Set authentication token
   */
  setToken(token) {
    this.token = token;
  }

  /**
   * Make authenticated request to MCP server
   */
  async request(endpoint, options = {}) {
    const url = `${this.baseUrl}${endpoint}`;
    
    const headers = {
      'Content-Type': 'application/json',
      ...options.headers
    };

    // Add Snowflake auth token if available
    if (this.token) {
      headers['Authorization'] = `Bearer ${this.token}`;
    }

    try {
      const response = await fetch(url, {
        ...options,
        headers
      });

      if (!response.ok) {
        const error = await response.text();
        throw new Error(`MCP request failed: ${response.status} - ${error}`);
      }

      return await response.json();
    } catch (error) {
      if (this.debug) {
        console.error('MCP request error:', error);
      }
      throw error;
    }
  }

  /**
   * List available MCP tools
   */
  async listTools() {
    return this.request('/tools');
  }

  /**
   * Compose and execute a query plan
   */
  async composeQueryPlan(params) {
    return this.request('/tools/compose_query_plan', {
      method: 'POST',
      body: JSON.stringify(params)
    });
  }

  /**
   * Validate a query plan without execution
   */
  async validatePlan(plan) {
    return this.request('/tools/validate_plan', {
      method: 'POST',
      body: JSON.stringify(plan)
    });
  }

  /**
   * List available data sources
   */
  async listSources(includeColumns = false) {
    const query = includeColumns ? '?include_columns=true' : '';
    return this.request(`/tools/list_sources${query}`);
  }

  /**
   * Create a dashboard
   */
  async createDashboard(spec) {
    return this.request('/tools/create_dashboard', {
      method: 'POST',
      body: JSON.stringify(spec)
    });
  }

  /**
   * Process natural language query
   */
  async processNaturalLanguage(query) {
    // Parse intent and map to appropriate source
    const intent = query.toLowerCase();
    
    let params = {
      intent_text: query
    };

    // Simple intent mapping (in production, use NLP)
    if (intent.includes('summary') || intent.includes('overview')) {
      params.source = 'ACTIVITY_CCODE.VW_ACTIVITY_SUMMARY';
    } else if (intent.includes('hour') || intent.includes('trend') || intent.includes('time')) {
      params.source = 'ACTIVITY_CCODE.VW_ACTIVITY_COUNTS_24H';
      params.dimensions = ['HOUR'];
      params.measures = [{ fn: 'SUM', column: 'EVENT_COUNT' }];
      params.order_by = [{ column: 'HOUR', direction: 'ASC' }];
    } else if (intent.includes('last') || intent.includes('recent')) {
      params.source = 'ACTIVITY.EVENTS';
      params.order_by = [{ column: 'TS', direction: 'DESC' }];
      
      // Extract number from query
      const match = intent.match(/\d+/);
      params.top_n = match ? parseInt(match[0]) : 10;
    } else if (intent.includes('top') || intent.includes('most')) {
      params.source = 'ACTIVITY_CCODE.VW_ACTIVITY_COUNTS_24H';
      params.dimensions = ['ACTIVITY'];
      params.measures = [{ fn: 'SUM', column: 'EVENT_COUNT' }];
      params.order_by = [{ column: 'SUM_EVENT_COUNT', direction: 'DESC' }];
      
      // Extract number from query
      const match = intent.match(/\d+/);
      params.top_n = match ? parseInt(match[0]) : 5;
    }

    return this.composeQueryPlan(params);
  }

  /**
   * Execute a dashboard panel query
   */
  async executePanelQuery(panel) {
    const params = {
      intent_text: `Execute panel: ${panel.type}`,
      source: panel.source
    };

    // Map panel configuration to query parameters
    if (panel.x && panel.metric) {
      params.dimensions = [panel.x];
      params.measures = [{ fn: 'SUM', column: panel.metric }];
      params.order_by = [{ column: panel.x, direction: 'ASC' }];
    }

    if (panel.group_by) {
      params.dimensions = panel.group_by;
    }

    if (panel.top_n) {
      params.top_n = panel.top_n;
    }

    if (panel.limit) {
      params.top_n = panel.limit;
    }

    return this.composeQueryPlan(params);
  }

  /**
   * Get health status
   */
  async health() {
    return this.request('/health');
  }

  /**
   * Get metrics
   */
  async metrics() {
    const response = await this.request('/metrics');
    return response;
  }
}

// Export for use in browser and Node.js
if (typeof module !== 'undefined' && module.exports) {
  module.exports = SnowparkMCPClient;
} else if (typeof window !== 'undefined') {
  window.SnowparkMCPClient = SnowparkMCPClient;
}