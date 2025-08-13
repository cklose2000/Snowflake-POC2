// Message Router - Connects UI, Claude Code, and SafeSQL
const ClaudeCodeWrapper = require('../../../packages/claude-wrapper');
const SafeSQLTemplateEngine = require('../../../packages/safesql/template-engine-cjs');
const EventEmitter = require('events');

class MessageRouter extends EventEmitter {
  constructor(snowflakeConnection) {
    super();
    this.claudeWrapper = new ClaudeCodeWrapper();
    this.safesqlEngine = new SafeSQLTemplateEngine(snowflakeConnection);
    this.sessions = new Map();
    this.setupClaudeHandlers();
  }

  setupClaudeHandlers() {
    // When Claude Code is ready
    this.claudeWrapper.on('ready', () => {
      console.log('✅ Claude Code is ready');
      this.emit('claude-ready');
    });

    // When Claude outputs text
    this.claudeWrapper.on('output', (text) => {
      this.emit('claude-output', text);
      this.broadcastToSessions({
        type: 'assistant-message',
        content: text
      });
    });

    // When Claude mentions SQL
    this.claudeWrapper.on('sql-intent', async (data) => {
      console.log('🔍 SQL intent detected:', data.query ? 'Query found' : 'No query extracted');
      
      // Try to map to a SafeSQL template
      const template = this.detectTemplate(data.text);
      if (template) {
        await this.executeSafeSQL(template.name, template.params);
      } else {
        this.broadcastToSessions({
          type: 'info',
          content: 'SQL intent detected but no matching SafeSQL template found. Use one of: sample_top, recent_activities, activity_by_type, activity_summary'
        });
      }
    });

    // When Claude claims success
    this.claudeWrapper.on('success-claim', (data) => {
      console.log('✅ Success claim detected:', data.claim);
      this.emit('audit-needed', data.claim);
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

  // Handle message from UI
  async handleUserMessage(sessionId, message) {
    console.log(`[Session ${sessionId}] User: ${message}`);
    
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

    // Otherwise, send to Claude Code
    const sent = this.claudeWrapper.send(message);
    if (!sent) {
      this.sendToSession(sessionId, {
        type: 'error',
        content: 'Claude Code is not ready. Please wait...'
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

  async logActivity(activity, feature_json) {
    // This would use the actual activity logger
    console.log(`[ACTIVITY] ${activity}:`, feature_json);
  }

  stop() {
    this.claudeWrapper.stop();
  }
}

module.exports = MessageRouter;