// Message Router - Connects UI, Claude Code, and SafeSQL with BI-First Smart Routing
const ClaudeCodeWrapper = require('../../../packages/claude-wrapper');
const SafeSQLTemplateEngine = require('../../../packages/safesql/template-engine-cjs');
const BIQueryRouter = require('../../../packages/bi-router');
const EventEmitter = require('events');

class MessageRouter extends EventEmitter {
  constructor(snowflakeConnection) {
    super();
    this.claudeWrapper = new ClaudeCodeWrapper();
    this.safesqlEngine = new SafeSQLTemplateEngine(snowflakeConnection);
    this.biRouter = new BIQueryRouter();
    this.sessions = new Map();
    this.setupClaudeHandlers();
  }

  setupClaudeHandlers() {
    // When Claude Code wrapper is ready
    this.claudeWrapper.on('ready', () => {
      console.log('✅ Claude Code wrapper is ready');
      this.emit('claude-ready');
    });

    // When Claude outputs text
    this.claudeWrapper.on('output', (text) => {
      this.emit('claude-output', text);
      // Broadcast is handled in handleUserMessage after async call
    });

    // When Claude mentions SQL
    this.claudeWrapper.on('sql-intent', async (data) => {
      console.log('🔍 SQL intent detected:', data.query ? 'Query found' : 'No query extracted');
      
      // Try to map to a SafeSQL template
      const template = this.detectTemplate(data.text);
      if (template) {
        await this.executeSafeSQL(template.name, template.params, data.session_id);
      } else {
        this.sendToSession(data.session_id, {
          type: 'info',
          content: 'SQL intent detected but no matching SafeSQL template found. Use one of: sample_top, recent_activities, activity_by_type, activity_summary'
        });
      }
    });

    // When Claude claims success
    this.claudeWrapper.on('success-claim', (data) => {
      console.log('✅ Success claim detected:', data.claim);
      this.emit('audit-needed', { ...data.claim, session_id: data.session_id });
    });

    // Handle errors
    this.claudeWrapper.on('error', (error) => {
      console.error('Claude Code error:', error);
      this.broadcastToSessions({
        type: 'error',
        content: `Claude Code error: ${error}`
      });
    });
  }

  async start() {
    this.claudeWrapper.start();
  }

  // Handle message from UI with BI-First Smart Routing
  async handleUserMessage(sessionId, message) {
    console.log(`[Session ${sessionId}] User: ${message}`);
    const startTime = Date.now();
    
    // Log activity
    await this.logActivity('ccode.user_asked', {
      session_id: sessionId,
      message: message,
      timestamp: new Date().toISOString()
    });

    // Check if it's a direct SQL template request
    if (message.startsWith('/sql ')) {
      return this.handleSQLCommand(sessionId, message.substring(5));
    }

    // Check if it's a help request
    if (message === '/help' || message === '/templates') {
      return this.sendTemplateHelp(sessionId);
    }
    
    // Check if it's a session management command
    if (message === '/clear' || message === '/reset') {
      this.claudeWrapper.clearSession(sessionId);
      this.sendToSession(sessionId, {
        type: 'info',
        content: 'Session context cleared. Starting fresh conversation.'
      });
      return;
    }

    // BI-First Smart Routing: Classify the query
    const route = this.biRouter.classify(message);
    console.log(`🎯 Query classified as Tier ${route.tier} (${route.route}):`);
    console.log(`   Expected: ${route.expectedTime}ms, $${route.expectedCost}`);
    console.log(`   Reasoning: ${route.reasoning}`);

    try {
      let result = null;
      let cost = 0;

      switch (route.tier) {
        case 1: // Direct SafeSQL routing
          console.log(`⚡ Direct SafeSQL: ${route.template} with params:`, route.params);
          result = await this.executeSafeSQL(route.template, route.params, sessionId);
          cost = 0.001; // Minimal cost for direct query
          break;

        case 2: // Lite AI interpretation
          console.log(`🧠 Lite AI interpretation needed`);
          result = await this.handleLiteAI(sessionId, message, route);
          cost = 0.05; // Estimated lite AI cost
          break;

        case 3: // Full Claude Code
          console.log(`🚀 Full Claude Code processing`);
          result = await this.handleFullClaude(sessionId, message);
          cost = 0.20; // Estimated full Claude cost
          break;
      }

      // Track routing performance
      const duration = Date.now() - startTime;
      const perfData = this.biRouter.trackPerformance(route, duration, cost, result !== null);
      await this.logActivity(perfData.activity, perfData.feature_json);

      console.log(`✅ Query completed in ${duration}ms (expected ${route.expectedTime}ms)`);

    } catch (error) {
      console.error('Error in smart routing:', error);
      const duration = Date.now() - startTime;
      const perfData = this.biRouter.trackPerformance(route, duration, 0, false);
      await this.logActivity(perfData.activity, { ...perfData.feature_json, error: error.message });

      this.sendToSession(sessionId, {
        type: 'error',
        content: `Error: ${error.message}`
      });
    }
  }

  async handleSQLCommand(sessionId, command) {
    // Parse command like: "sample_top n=5"
    const parts = command.split(' ');
    const templateName = parts[0];
    const params = {};

    // Parse parameters
    for (let i = 1; i < parts.length; i++) {
      const [key, value] = parts[i].split('=');
      if (key && value) {
        params[key] = isNaN(value) ? value : Number(value);
      }
    }

    // Add default schema/table for activity templates
    if (['sample_top', 'recent_activities', 'activity_by_type', 'activity_summary'].includes(templateName)) {
      params.schema = params.schema || 'ACTIVITY';
      params.table = params.table || 'EVENTS';
    }

    await this.executeSafeSQL(templateName, params, sessionId);
  }

  async executeSafeSQL(templateName, params, sessionId = null) {
    try {
      console.log(`Executing SafeSQL template: ${templateName}`, params);
      
      const result = await this.safesqlEngine.execute(templateName, params);
      
      const message = {
        type: 'sql-result',
        template: templateName,
        rows: result.rows,
        count: result.count,
        metadata: result.metadata
      };

      if (sessionId) {
        this.sendToSession(sessionId, message);
      } else {
        this.broadcastToSessions(message);
      }

      // Log activity
      await this.logActivity('ccode.sql_executed', {
        template: templateName,
        params: params,
        row_count: result.count
      });

    } catch (error) {
      const errorMessage = {
        type: 'error',
        content: `SafeSQL error: ${error.message}`
      };

      if (sessionId) {
        this.sendToSession(sessionId, errorMessage);
      } else {
        this.broadcastToSessions(errorMessage);
      }
    }
  }

  detectTemplate(text) {
    // Try to detect which template to use based on the text
    const patterns = [
      {
        regex: /show\s+me\s+(\d+)?\s*(?:recent|latest)?\s*(?:rows?|records?|events?)/i,
        template: 'sample_top',
        params: (match) => ({ n: match[1] || 10 })
      },
      {
        regex: /(?:recent|last)\s+(\d+)?\s*(?:hours?)?\s*(?:of)?\s*activit/i,
        template: 'recent_activities',
        params: (match) => ({ hours: match[1] || 1, limit: 100 })
      },
      {
        regex: /group\s+by\s+activity|activity\s+types?|breakdown\s+by\s+activity/i,
        template: 'activity_by_type',
        params: () => ({ hours: 24 })
      },
      {
        regex: /summar(?:y|ize)\s+(?:the\s+)?activit/i,
        template: 'activity_summary',
        params: () => ({ hours: 24 })
      }
    ];

    for (const pattern of patterns) {
      const match = text.match(pattern.regex);
      if (match) {
        return {
          name: pattern.template,
          params: pattern.params(match)
        };
      }
    }

    return null;
  }

  sendTemplateHelp(sessionId) {
    const templates = this.safesqlEngine.getTemplateList();
    const help = `
Available Commands:
  /help or /templates - Show this help
  /clear or /reset - Clear conversation context
  /sql [template] - Execute SafeSQL template

Available SafeSQL Templates:
${templates.map(t => `  • ${t.name} (params: ${t.params.join(', ')})`).join('\n')}

Usage:
  /sql template_name param1=value1 param2=value2
  
Examples:
  /sql sample_top n=10
  /sql recent_activities hours=2 limit=50
  /sql activity_by_type hours=24
  /sql activity_summary hours=48
    `.trim();

    this.sendToSession(sessionId, {
      type: 'info',
      content: help
    });
  }

  // Session management
  registerSession(sessionId, ws) {
    this.sessions.set(sessionId, ws);
    console.log(`Session ${sessionId} registered`);
  }

  unregisterSession(sessionId) {
    this.sessions.delete(sessionId);
    console.log(`Session ${sessionId} unregistered`);
  }

  sendToSession(sessionId, message) {
    const ws = this.sessions.get(sessionId);
    if (ws && ws.readyState === 1) { // WebSocket.OPEN
      ws.send(JSON.stringify(message));
    }
  }

  broadcastToSessions(message) {
    for (const [sessionId, ws] of this.sessions) {
      if (ws.readyState === 1) {
        ws.send(JSON.stringify(message));
      }
    }
  }

  // Handle Lite AI interpretation (Tier 2)
  async handleLiteAI(sessionId, message, route) {
    console.log(`🧠 Lite AI processing: "${message}"`);
    
    // Create minimal context for Claude
    const liteContext = `You are a BI analyst assistant. Be concise and direct.
Available SafeSQL templates: sample_top, recent_activities, activity_by_type, activity_summary.
Always prefer suggesting SafeSQL templates when possible.

Query: "${message}"

Respond with either:
1. A SafeSQL template suggestion: "/sql template_name param=value"  
2. A brief analysis (max 100 words) if SafeSQL won't work`;

    try {
      // Use Claude with minimal context
      const response = await this.claudeWrapper.callClaude(liteContext, `lite_${sessionId}`);
      
      if (response && response.result) {
        // Check if Claude suggested a SafeSQL template
        if (response.result.includes('/sql ')) {
          const sqlMatch = response.result.match(/\/sql\s+([^\n]+)/);
          if (sqlMatch) {
            console.log(`🔄 Lite AI suggested SafeSQL: ${sqlMatch[1]}`);
            // Execute the suggested template
            await this.handleSQLCommand(sessionId, sqlMatch[1]);
            return true;
          }
        }
        
        // Send the lite AI response
        this.sendToSession(sessionId, {
          type: 'assistant-message',
          content: response.result
        });
        return true;
      }
      
      return false;
    } catch (error) {
      console.error('Lite AI failed, falling back to full Claude:', error);
      return await this.handleFullClaude(sessionId, message);
    }
  }

  // Handle Full Claude Code (Tier 3) 
  async handleFullClaude(sessionId, message) {
    console.log(`🚀 Full Claude Code processing: "${message}"`);
    
    try {
      const response = await this.claudeWrapper.send(message, sessionId);
      
      if (response) {
        // Send the response directly to the session
        console.log(`Sending Claude response to session ${sessionId}:`, response.result.substring(0, 100));
        this.sendToSession(sessionId, {
          type: 'assistant-message',
          content: response.result
        });
        return true;
      } else {
        this.sendToSession(sessionId, {
          type: 'error',
          content: 'Failed to get response from Claude. Please try again.'
        });
        return false;
      }
    } catch (error) {
      console.error('Full Claude failed:', error);
      this.sendToSession(sessionId, {
        type: 'error',
        content: `Claude error: ${error.message}`
      });
      return false;
    }
  }

  // Get routing statistics for admin/debugging
  getRoutingStats() {
    return this.biRouter.getStats();
  }

  // Get routing suggestions for a query (for debugging/optimization)
  getQuerySuggestions(query) {
    return this.biRouter.getSuggestions(query);
  }

  async logActivity(activity, feature_json) {
    // This would use the actual activity logger
    console.log(`[ACTIVITY] ${activity}:`, feature_json);
  }

  stop() {
    this.claudeWrapper.stop();
  }
}

module.exports = MessageRouter;