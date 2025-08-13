// Claude Code CLI Wrapper
const { spawn } = require('child_process');
const EventEmitter = require('events');

class ClaudeCodeWrapper extends EventEmitter {
  constructor(options = {}) {
    super();
    this.options = {
      interactive: true,
      ...options
    };
    this.process = null;
    this.buffer = '';
    this.isReady = false;
  }

  start() {
    console.log('🚀 Starting Claude Code CLI...');
    
    // Check if claude is available
    const testProcess = spawn('which', ['claude']);
    testProcess.on('close', (code) => {
      if (code !== 0) {
        console.error('❌ Claude Code CLI not found. Please install it first.');
        this.emit('error', new Error('Claude Code CLI not found'));
        return;
      }
      this.spawnClaudeCode();
    });
  }

  spawnClaudeCode() {
    const args = [];
    // Claude CLI doesn't have --interactive flag, it's interactive by default
    
    this.process = spawn('claude', args, {
      stdio: ['pipe', 'pipe', 'pipe'],
      env: { ...process.env }
    });

    // Claude CLI is ready immediately after spawning
    setTimeout(() => {
      this.isReady = true;
      this.emit('ready');
      console.log('✅ Claude Code is ready for input');
    }, 1000);

    this.process.stdout.on('data', (data) => {
      const text = data.toString();
      this.buffer += text;
      console.log('[Claude Output]:', text.substring(0, 100) + (text.length > 100 ? '...' : ''));

      // Detect SQL intent
      if (this.detectSQLIntent(text)) {
        this.emit('sql-intent', {
          text: text,
          query: this.extractSQLQuery(text)
        });
      }

      // Detect success claims
      if (this.detectSuccessClaim(text)) {
        this.emit('success-claim', {
          text: text,
          claim: this.extractClaim(text)
        });
      }

      // Always emit the raw output
      this.emit('output', text);
    });

    this.process.stderr.on('data', (data) => {
      console.error('Claude Code stderr:', data.toString());
      this.emit('error', data.toString());
    });

    this.process.on('close', (code) => {
      console.log(`Claude Code exited with code ${code}`);
      this.emit('close', code);
    });

    this.process.on('error', (err) => {
      console.error('Failed to start Claude Code:', err);
      this.emit('error', err);
    });
  }

  send(message) {
    if (!this.process || !this.isReady) {
      console.error('Claude Code not ready to receive messages');
      return false;
    }

    // Log the interaction
    this.logActivity('ccode.user_asked', {
      message: message,
      timestamp: new Date().toISOString()
    });

    this.process.stdin.write(message + '\n');
    return true;
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
    if (this.process) {
      this.process.kill();
      this.process = null;
    }
  }
}

module.exports = ClaudeCodeWrapper;