// Claude Code Bridge - Main Process
import express from 'express';
import { WebSocketServer } from 'ws';
import { spawn } from 'child_process';
import dotenv from 'dotenv';
import { ActivityLogger } from './activity-logger.js';
import { SubagentRouter } from './subagent-router.js';

dotenv.config();

const app = express();
const port = process.env.BRIDGE_PORT || 3001;

// Initialize activity logger (replaces separate Activity Agent)
const activityLogger = new ActivityLogger();
const subagentRouter = new SubagentRouter();

// WebSocket server for UI communication
const wss = new WebSocketServer({ port: 8080 });

class ClaudeCodeBridge {
  constructor() {
    this.activeProcesses = new Map();
    this.sessions = new Map();
  }

  async handleMessage(ws, message) {
    const { type, payload, sessionId } = JSON.parse(message);
    
    // Log user interaction
    await activityLogger.logEvent({
      activity: 'ccode.user_asked',
      customer: sessionId,
      feature_json: { 
        message_type: type,
        tokens_in: payload.content?.length || 0 
      }
    });

    switch (type) {
      case 'chat':
        return this.handleChat(ws, payload, sessionId);
      case 'sql_query':
        return this.handleSQLQuery(ws, payload, sessionId);
      default:
        ws.send(JSON.stringify({ error: 'Unknown message type' }));
    }
  }

  async handleChat(ws, payload, sessionId) {
    // Spawn Claude Code CLI process
    const codeProcess = spawn('ccode', ['--interactive'], {
      stdio: ['pipe', 'pipe', 'pipe']
    });

    // Stream input to Claude Code
    codeProcess.stdin.write(payload.content);

    // Stream output back to UI
    codeProcess.stdout.on('data', (data) => {
      const response = data.toString();
      
      // Check if response indicates Snowflake intent
      if (this.detectSnowflakeIntent(response)) {
        this.routeToSnowflakeAgent(response, sessionId);
      }
      
      // Check for success claims
      if (this.detectSuccessClaim(response)) {
        this.routeToAuditAgent(response, sessionId);
      }

      ws.send(JSON.stringify({
        type: 'response',
        content: response
      }));
    });
  }

  detectSnowflakeIntent(response) {
    const patterns = [
      /SELECT\s+/i,
      /FROM\s+/i,
      /snowflake/i,
      /query/i,
      /database/i
    ];
    return patterns.some(pattern => pattern.test(response));
  }

  detectSuccessClaim(response) {
    const patterns = [
      /✅/,
      /\bcomplete\b/i,
      /\bsuccessfully\b/i,
      /\bread(y|iness)\b/i,
      /\d+%/
    ];
    return patterns.some(pattern => pattern.test(response));
  }

  async routeToSnowflakeAgent(request, sessionId) {
    return subagentRouter.route('snowflake', {
      type: 'sql_request',
      content: request,
      sessionId
    });
  }

  async routeToAuditAgent(claim, sessionId) {
    return subagentRouter.route('audit', {
      type: 'verify_claim',
      claim: claim,
      sessionId
    });
  }
}

const bridge = new ClaudeCodeBridge();

// WebSocket connection handling
wss.on('connection', (ws) => {
  console.log('New WebSocket connection');
  
  ws.on('message', async (message) => {
    try {
      await bridge.handleMessage(ws, message);
    } catch (error) {
      console.error('Bridge error:', error);
      ws.send(JSON.stringify({ error: error.message }));
    }
  });
});

app.listen(port, () => {
  console.log(`Claude Code Bridge running on port ${port}`);
});