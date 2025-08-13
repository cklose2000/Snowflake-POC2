// Claude Code CLI Wrapper - Non-Interactive JSON Mode
const { spawn } = require('child_process');
const EventEmitter = require('events');

class ClaudeCodeWrapper extends EventEmitter {
  constructor(options = {}) {
    super();
    this.options = {
      outputFormat: 'json', // Use JSON for structured responses
      ...options
    };
    this.sessions = new Map(); // Store conversation context per session
    this.processManager = new ProcessSafetyManager();
    this.isReady = false;
  }

  start() {
    console.log('🚀 Starting Claude Code CLI Wrapper (Non-Interactive Mode)...');
    
    // Check if claude is available
    const testProcess = spawn('which', ['claude']);
    testProcess.on('close', (code) => {
      if (code !== 0) {
        console.error('❌ Claude Code CLI not found. Please install it first.');
        this.emit('error', new Error('Claude Code CLI not found'));
        return;
      }
      // In non-interactive mode, we're ready immediately
      this.isReady = true;
      this.emit('ready');
      console.log('✅ Claude Code wrapper ready (non-interactive mode)');
    });
  }

  async callClaude(message, sessionId = 'default') {
    return new Promise((resolve, reject) => {
      const startTime = Date.now();
      
      // Get session context
      const context = this.getSessionContext(sessionId);
      const fullMessage = context ? `${context}\n\nUser: ${message}` : message;
      
      if (context) {
        console.log(`📝 Using context for session ${sessionId} (${this.sessions.get(sessionId).messages.length} messages)`);
      }
      
      // Spawn claude with --print and JSON output
      const args = [
        '--print',
        '--output-format', this.options.outputFormat
      ];
      
      const claudeProcess = spawn('claude', args, {
        env: { ...process.env }
      });
      
      // Track this process
      this.processManager.track(claudeProcess);
      
      let output = '';
      let errorOutput = '';
      
      claudeProcess.stdout.on('data', (data) => {
        output += data.toString();
      });
      
      claudeProcess.stderr.on('data', (data) => {
        errorOutput += data.toString();
      });
      
      claudeProcess.on('close', (code) => {
        const duration = Date.now() - startTime;
        this.processManager.untrack(claudeProcess.pid);
        
        if (code !== 0) {
          console.error(`Claude process exited with code ${code}`);
          reject(new Error(`Claude failed: ${errorOutput}`));
          return;
        }
        
        try {
          // Parse JSON response
          const response = this.parseResponse(output);
          
          // Update session context
          this.updateSessionContext(sessionId, message, response.result);
          
          // Log activity
          this.logActivity('ccode.claude_response', {
            session_id: sessionId,
            duration_ms: duration,
            tokens: response.usage,
            cost_usd: response.total_cost_usd
          });
          
          // Process response for intents
          this.processResponse(response.result, sessionId);
          
          resolve(response);
        } catch (error) {
          console.error('Failed to parse Claude response:', error);
          reject(error);
        }
      });
      
      // Send input
      claudeProcess.stdin.write(fullMessage);
      claudeProcess.stdin.end();
    });
  }

  async send(message, sessionId = 'default') {
    if (!this.isReady) {
      console.error('Claude Code wrapper not ready');
      return null;
    }

    // Log the interaction
    this.logActivity('ccode.user_asked', {
      session_id: sessionId,
      message: message,
      timestamp: new Date().toISOString()
    });

    try {
      const response = await this.callClaude(message, sessionId);
      
      // Emit the formatted output
      this.emit('output', response.result);
      
      return response;
    } catch (error) {
      console.error('Failed to send message to Claude:', error);
      this.emit('error', error.message);
      return null;
    }
  }
  
  // Session context management
  getSessionContext(sessionId) {
    const session = this.sessions.get(sessionId);
    if (!session || session.messages.length === 0) return null;
    
    // Build context from recent messages (last 10)
    const recentMessages = session.messages.slice(-10);
    return recentMessages.map(m => `${m.role}: ${m.content}`).join('\n');
  }
  
  updateSessionContext(sessionId, userMessage, assistantResponse) {
    if (!this.sessions.has(sessionId)) {
      this.sessions.set(sessionId, { messages: [], created: Date.now() });
    }
    
    const session = this.sessions.get(sessionId);
    session.messages.push(
      { role: 'User', content: userMessage, timestamp: Date.now() },
      { role: 'Assistant', content: assistantResponse, timestamp: Date.now() }
    );
    
    // Limit context to last 20 messages
    if (session.messages.length > 20) {
      session.messages = session.messages.slice(-20);
    }
  }
  
  clearSession(sessionId) {
    this.sessions.delete(sessionId);
  }
  
  // Parse Claude's JSON response
  parseResponse(output) {
    try {
      // The output might have multiple JSON objects, get the last one (the result)
      const lines = output.trim().split('\n');
      const resultLine = lines[lines.length - 1];
      const parsed = JSON.parse(resultLine);
      
      // Extract the actual response text
      if (parsed.result) {
        return parsed;
      } else if (parsed.message) {
        // Handle assistant message format
        return {
          result: parsed.message.content[0].text,
          usage: parsed.message.usage,
          total_cost_usd: null
        };
      }
      
      return parsed;
    } catch (error) {
      // If not JSON, return as plain text
      return { result: output, usage: null, total_cost_usd: null };
    }
  }
  
  // Process response for SQL intents and success claims
  processResponse(text, sessionId) {
    // Detect SQL intent
    if (this.detectSQLIntent(text)) {
      this.emit('sql-intent', {
        session_id: sessionId,
        text: text,
        query: this.extractSQLQuery(text)
      });
    }

    // Detect success claims
    if (this.detectSuccessClaim(text)) {
      this.emit('success-claim', {
        session_id: sessionId,
        text: text,
        claim: this.extractClaim(text)
      });
    }
  }

  detectSQLIntent(text) {
    const sqlPatterns = [
      /\bSELECT\s+/i,
      /\bFROM\s+/i,
      /\bWHERE\s+/i,
      /\bGROUP BY\s+/i,
      /\bORDER BY\s+/i,
      /query the database/i,
      /get.*from.*table/i,
      /show me.*data/i,
      /analyze.*table/i
    ];
    
    return sqlPatterns.some(pattern => pattern.test(text));
  }

  extractSQLQuery(text) {
    // Try to extract SQL query from code blocks
    const codeBlockMatch = text.match(/```sql\n([\s\S]*?)\n```/);
    if (codeBlockMatch) {
      return codeBlockMatch[1].trim();
    }

    // Try to extract inline SQL
    const sqlMatch = text.match(/SELECT[\s\S]*?(?:;|$)/i);
    if (sqlMatch) {
      return sqlMatch[0].trim();
    }

    return null;
  }

  detectSuccessClaim(text) {
    const claimPatterns = [
      /✅/,
      /successfully/i,
      /completed?/i,
      /done/i,
      /finished/i,
      /\d+\s+rows?\s+returned/i,
      /\d+\s+records?\s+found/i,
      /\d+%\s+(?:faster|improvement|increase)/i
    ];

    return claimPatterns.some(pattern => pattern.test(text));
  }

  extractClaim(text) {
    // Extract quantifiable claims
    const patterns = [
      { regex: /(\d+)\s+rows?\s+returned/i, type: 'row_count' },
      { regex: /(\d+)\s+records?\s+found/i, type: 'record_count' },
      { regex: /(\d+)%\s+faster/i, type: 'performance' },
      { regex: /query executed in\s+(\d+(?:\.\d+)?)\s*(?:ms|seconds?)/i, type: 'execution_time' }
    ];

    for (const { regex, type } of patterns) {
      const match = text.match(regex);
      if (match) {
        return {
          type: type,
          value: match[1],
          full_text: match[0]
        };
      }
    }

    return {
      type: 'generic',
      value: null,
      full_text: text.substring(0, 100)
    };
  }

  async logActivity(activity, feature_json) {
    // This would connect to the activity logger
    // For now, just log to console
    console.log(`[ACTIVITY] ${activity}:`, feature_json);
  }

  stop() {
    // Clean up all sessions
    this.sessions.clear();
    // Clean up any lingering processes
    this.processManager.cleanup();
  }
}

// Process Safety Manager - Tracks and safely manages Claude processes
class ProcessSafetyManager {
  constructor() {
    this.activeProcesses = new Map();
    this.protectedPids = new Set([38242]); // Protect the assistant's process
    this.setupCleanupHandlers();
  }
  
  setupCleanupHandlers() {
    const cleanup = () => {
      console.log('\n🧹 Cleaning up Claude processes...');
      this.cleanup();
    };
    
    process.on('SIGINT', cleanup);
    process.on('SIGTERM', cleanup);
    process.on('exit', cleanup);
  }
  
  track(process) {
    if (process && process.pid) {
      if (this.protectedPids.has(process.pid)) {
        console.warn(`⚠️ Attempted to track protected PID ${process.pid} - ignoring`);
        return;
      }
      this.activeProcesses.set(process.pid, process);
      console.log(`📝 Tracking Claude process ${process.pid}`);
    }
  }
  
  untrack(pid) {
    this.activeProcesses.delete(pid);
  }
  
  cleanup() {
    for (const [pid, proc] of this.activeProcesses) {
      if (!this.protectedPids.has(pid)) {
        try {
          proc.kill('SIGTERM');
          console.log(`✅ Cleaned up process ${pid}`);
        } catch (error) {
          // Process might already be dead
        }
      }
    }
    this.activeProcesses.clear();
  }
}

module.exports = ClaudeCodeWrapper;