// Integrated Server - Complete WebSocket + API + Claude Code + SafeSQL
const express = require('express');
const WebSocket = require('ws');
const snowflake = require('snowflake-sdk');
const MessageRouter = require('./apps/ccode-bridge/src/message-router');
const path = require('path');
require('dotenv').config();

console.log('🚀 Starting SnowflakePOC2 Integrated Server...\n');

const app = express();
const PORT = process.env.BRIDGE_PORT || 3001;
const WS_PORT = 8080;

// Middleware
app.use(express.json());
app.use(express.static(__dirname));

// Snowflake connection
let snowflakeConn = null;
let messageRouter = null;

async function initSnowflake() {
  const connection = snowflake.createConnection({
    account: process.env.SNOWFLAKE_ACCOUNT,
    username: process.env.SNOWFLAKE_USERNAME,
    password: process.env.SNOWFLAKE_PASSWORD,
    role: process.env.SNOWFLAKE_ROLE,
    warehouse: process.env.SNOWFLAKE_WAREHOUSE,
    database: process.env.SNOWFLAKE_DATABASE,
    schema: process.env.SNOWFLAKE_SCHEMA
  });

  return new Promise((resolve, reject) => {
    connection.connect((err, conn) => {
      if (err) {
        console.error('❌ Snowflake connection failed:', err.message);
        reject(err);
      } else {
        console.log('✅ Connected to Snowflake');
        snowflakeConn = conn;
        resolve(conn);
      }
    });
  });
}

// Initialize services
async function initialize() {
  try {
    await initSnowflake();
    
    // Create message router
    messageRouter = new MessageRouter(snowflakeConn);
    
    // Start Claude Code CLI integration
    await messageRouter.start();
    
    console.log('✅ Message router initialized with Claude Code CLI');
  } catch (error) {
    console.error('Initialization error:', error);
  }
}

// Create WebSocket server
const wss = new WebSocket.Server({ port: WS_PORT });

wss.on('connection', (ws) => {
  console.log('🔌 New WebSocket connection');
  let sessionId = null;
  
  ws.send(JSON.stringify({
    type: 'welcome',
    message: 'Connected to SnowflakePOC2 Bridge'
  }));
  
  ws.on('message', async (message) => {
    try {
      const data = JSON.parse(message);
      
      switch(data.type) {
        case 'register':
          sessionId = data.sessionId;
          if (messageRouter) {
            messageRouter.registerSession(sessionId, ws);
          }
          console.log(`Session registered: ${sessionId}`);
          break;
          
        case 'user-message':
          if (messageRouter) {
            await messageRouter.handleUserMessage(data.sessionId || sessionId, data.content);
          } else {
            // Fallback: handle SQL commands directly
            await handleDirectSQL(ws, data.content);
          }
          break;
          
        default:
          console.log('Unknown message type:', data.type);
      }
    } catch (err) {
      console.error('WebSocket error:', err);
      ws.send(JSON.stringify({
        type: 'error',
        error: err.message
      }));
    }
  });
  
  ws.on('close', () => {
    if (sessionId && messageRouter) {
      messageRouter.unregisterSession(sessionId);
    }
    console.log('🔌 WebSocket disconnected');
  });
});

// Direct SQL handling (when Claude Code is not available)
async function handleDirectSQL(ws, message) {
  const SafeSQLTemplateEngine = require('./packages/safesql/template-engine-cjs');
  
  if (!snowflakeConn) {
    ws.send(JSON.stringify({
      type: 'error',
      content: 'Snowflake not connected'
    }));
    return;
  }
  
  const engine = new SafeSQLTemplateEngine(snowflakeConn);
  
  // Check for SQL commands
  if (message.startsWith('/sql ')) {
    const parts = message.substring(5).split(' ');
    const templateName = parts[0];
    const params = {};
    
    // Parse parameters
    for (let i = 1; i < parts.length; i++) {
      const [key, value] = parts[i].split('=');
      if (key && value) {
        params[key] = isNaN(value) ? value : Number(value);
      }
    }
    
    try {
      const result = await engine.execute(templateName, params);
      ws.send(JSON.stringify({
        type: 'sql-result',
        template: templateName,
        rows: result.rows,
        count: result.count,
        metadata: result.metadata
      }));
    } catch (error) {
      ws.send(JSON.stringify({
        type: 'error',
        content: `SQL Error: ${error.message}`
      }));
    }
  } else if (message === '/help' || message === '/templates') {
    const templates = engine.getTemplateList();
    ws.send(JSON.stringify({
      type: 'info',
      content: `Available templates:\n${templates.map(t => `• ${t.name} (${t.params.join(', ')})`).join('\n')}`
    }));
  } else {
    ws.send(JSON.stringify({
      type: 'info',
      content: 'Claude Code CLI not available. Use /sql commands or /help for templates.'
    }));
  }
}

// Express routes
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'chat-ui.html'));
});

app.get('/health', (req, res) => {
  res.json({
    status: 'healthy',
    snowflake: snowflakeConn ? 'connected' : 'disconnected',
    websocket: wss.clients.size,
    timestamp: new Date()
  });
});

// Start servers
app.listen(PORT, () => {
  console.log(`\n🌉 API server running on http://localhost:${PORT}`);
  console.log(`🔌 WebSocket server running on ws://localhost:${WS_PORT}`);
  console.log('\n📊 Available features:');
  console.log('  • Chat interface at http://localhost:' + PORT);
  console.log('  • SafeSQL templates via /sql commands');
  console.log('  • Real-time WebSocket communication');
  console.log('  • Activity logging to Snowflake');
  console.log('\n✨ Integrated server ready!');
});

// Initialize on startup
initialize().catch(console.error);

// Graceful shutdown
process.on('SIGINT', () => {
  console.log('\n👋 Shutting down gracefully...');
  if (messageRouter) {
    messageRouter.stop();
  }
  if (snowflakeConn) {
    snowflakeConn.destroy();
  }
  wss.close();
  process.exit(0);
});