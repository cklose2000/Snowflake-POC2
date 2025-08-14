/**
 * MCP Snowflake Client
 * Calls MCP stored procedures directly in Snowflake
 */

class MCPSnowflakeClient {
  constructor(snowflakeConnection) {
    this.conn = snowflakeConnection;
  }

  /**
   * Validate a query plan
   */
  async validatePlan(plan) {
    return new Promise((resolve, reject) => {
      const sql = `CALL CLAUDE_BI.MCP.VALIDATE_QUERY_PLAN(PARSE_JSON('${JSON.stringify(plan)}'))`;
      
      this.conn.execute({
        sqlText: sql,
        complete: (err, statement, rows) => {
          if (err) {
            reject(err);
          } else {
            const result = rows[0].VALIDATE_QUERY_PLAN;
            resolve(result);
          }
        }
      });
    });
  }

  /**
   * Execute a query plan
   */
  async executePlan(plan) {
    return new Promise((resolve, reject) => {
      const sql = `CALL CLAUDE_BI.MCP.EXECUTE_QUERY_PLAN(PARSE_JSON('${JSON.stringify(plan)}'))`;
      
      this.conn.execute({
        sqlText: sql,
        complete: (err, statement, rows) => {
          if (err) {
            reject(err);
          } else {
            const result = JSON.parse(rows[0].EXECUTE_QUERY_PLAN);
            resolve(result);
          }
        }
      });
    });
  }

  /**
   * Process natural language query
   */
  async processNaturalLanguage(query) {
    const intent = query.toLowerCase();
    let plan = {};

    // Simple intent mapping
    if (intent.includes('summary') || intent.includes('overview')) {
      plan.source = 'VW_ACTIVITY_SUMMARY';
    } else if (intent.includes('hour') || intent.includes('trend') || intent.includes('time')) {
      plan.source = 'VW_ACTIVITY_COUNTS_24H';
      plan.top_n = 24;
    } else if (intent.includes('last') || intent.includes('recent')) {
      plan.source = 'EVENTS';
      const match = intent.match(/\d+/);
      plan.top_n = match ? parseInt(match[0]) : 10;
    } else {
      plan.source = 'VW_ACTIVITY_SUMMARY';
    }

    // Validate first
    const validation = await this.validatePlan(plan);
    if (!validation.valid) {
      throw new Error(validation.error || 'Invalid plan');
    }

    // Execute
    return this.executePlan(plan);
  }

  /**
   * Test the connection and procedures
   */
  async test() {
    try {
      console.log('Testing MCP Snowflake procedures...');
      
      // Test validation
      const validation = await this.validatePlan({
        source: 'VW_ACTIVITY_SUMMARY',
        top_n: 5
      });
      console.log('✅ Validation:', validation);
      
      // Test execution
      const result = await this.executePlan({
        source: 'VW_ACTIVITY_SUMMARY'
      });
      console.log('✅ Execution:', result);
      
      // Test natural language
      const nlResult = await this.processNaturalLanguage('Show me the last 10 events');
      console.log('✅ Natural language:', nlResult);
      
      return true;
    } catch (error) {
      console.error('❌ Test failed:', error);
      return false;
    }
  }
}

// Export for Node.js and browser
if (typeof module !== 'undefined' && module.exports) {
  module.exports = MCPSnowflakeClient;
} else if (typeof window !== 'undefined') {
  window.MCPSnowflakeClient = MCPSnowflakeClient;
}