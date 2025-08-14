/**
 * Unified Server - WebSocket + API + Dashboard Factory
 * Streamlined core functionality for SnowflakePOC2
 */

const express = require('express');
const WebSocket = require('ws');
const snowflake = require('snowflake-sdk');
const path = require('path');
require('dotenv').config();

// Core modules (to be created)
const DashboardFactory = require('./dashboard-factory');
const ActivityLogger = require('./activity-logger');
const SchemaContract = require('./schema-contract');
const SnowflakeClient = require('./snowflake-client');

const app = express();
const PORT = process.env.PORT || 3000;
const WS_PORT = process.env.WS_PORT || 8080;

// Middleware
app.use(express.json());
app.use(express.static(path.join(__dirname, '../ui')));

// Global connections
let snowflakeConn = null;
let dashboardFactory = null;
let activityLogger = null;

// Initialize services
async function initialize() {
  console.log('🚀 Starting Snowflake POC2 Server...\n');
  
  try {
    // 1. Connect to Snowflake
    snowflakeConn = await SnowflakeClient.connect();
    console.log('✅ Snowflake connected');
    
    // 2. Validate schema contract
    await SchemaContract.validate(snowflakeConn);
    console.log('✅ Schema contract validated');
    
    // 3. Initialize services
    activityLogger = new ActivityLogger(snowflakeConn);
    dashboardFactory = new DashboardFactory(snowflakeConn, activityLogger);
    console.log('✅ Services initialized');
    
    // 4. Start servers
    startHttpServer();
    startWebSocketServer();
    
  } catch (error) {
    console.error('❌ Initialization failed:', error.message);
    process.exit(1);
  }
}

// HTTP API endpoints
function startHttpServer() {
  // Health check
  app.get('/health', (req, res) => {
    res.json({
      status: 'healthy',
      snowflake: snowflakeConn ? 'connected' : 'disconnected',
      version: '2.0.0',
      contractHash: SchemaContract.getHash()
    });
  });
  
  // Dashboard creation endpoint
  app.post('/api/dashboard', async (req, res) => {
    try {
      const { conversation, spec } = req.body;
      
      // Log activity
      await activityLogger.log('dashboard_requested', {
        conversation_length: conversation?.length || 0,
        spec_provided: !!spec
      });
      
      // Generate or use provided spec
      const dashboardSpec = spec || await dashboardFactory.generateSpec(conversation);
      
      // Create dashboard
      const result = await dashboardFactory.create(dashboardSpec);
      
      // Log success
      await activityLogger.log('dashboard_created', {
        dashboard_id: result.dashboard_id,
        objects_created: result.objectsCreated
      });
      
      res.json(result);
      
    } catch (error) {
      await activityLogger.log('dashboard_failed', { error: error.message });
      res.status(500).json({ error: error.message });
    }
  });
  
  // Query execution endpoint (SafeSQL)
  app.post('/api/query', async (req, res) => {
    try {
      const { template, parameters } = req.body;
      
      // Validate against SafeSQL templates
      if (!SchemaContract.isValidTemplate(template)) {
        throw new Error('Invalid SQL template');
      }
      
      // Execute query
      const result = await SnowflakeClient.executeTemplate(snowflakeConn, template, parameters);
      
      // Log activity
      await activityLogger.log('sql_executed', {
        template,
        rows_returned: result.rows?.length || 0
      });
      
      res.json(result);
      
    } catch (error) {
      await activityLogger.log('sql_failed', { error: error.message });
      res.status(500).json({ error: error.message });
    }
  });
  
  app.listen(PORT, () => {
    console.log(`📡 HTTP server running on http://localhost:${PORT}`);
  });
}

// WebSocket server for real-time communication
function startWebSocketServer() {
  const wss = new WebSocket.Server({ port: WS_PORT });
  
  wss.on('connection', (ws) => {
    console.log('🔌 WebSocket client connected');
    
    ws.on('message', async (message) => {
      try {
        const data = JSON.parse(message);
        
        switch (data.type) {
          case 'chat':
            // Process chat message
            await handleChatMessage(ws, data);
            break;
            
          case 'dashboard':
            // Create dashboard from conversation
            await handleDashboardRequest(ws, data);
            break;
            
          case 'query':
            // Execute SafeSQL query
            await handleQueryRequest(ws, data);
            break;
            
          default:
            ws.send(JSON.stringify({ 
              type: 'error', 
              message: 'Unknown message type' 
            }));
        }
        
      } catch (error) {
        ws.send(JSON.stringify({ 
          type: 'error', 
          message: error.message 
        }));
      }
    });
    
    ws.on('close', () => {
      console.log('🔌 WebSocket client disconnected');
    });
  });
  
  console.log(`📡 WebSocket server running on ws://localhost:${WS_PORT}`);
}

// WebSocket handlers
async function handleChatMessage(ws, data) {
  // Log chat activity
  await activityLogger.log('user_asked', {
    message_length: data.message?.length || 0
  });
  
  // Echo for now (would integrate with Claude in production)
  ws.send(JSON.stringify({
    type: 'response',
    message: `Received: ${data.message}`
  }));
}

async function handleDashboardRequest(ws, data) {
  try {
    // Send progress updates
    ws.send(JSON.stringify({ type: 'progress', status: 'analyzing' }));
    
    const spec = await dashboardFactory.generateSpec(data.conversation);
    
    ws.send(JSON.stringify({ type: 'progress', status: 'creating' }));
    
    const result = await dashboardFactory.create(spec);
    
    ws.send(JSON.stringify({
      type: 'dashboard_complete',
      result
    }));
    
  } catch (error) {
    ws.send(JSON.stringify({
      type: 'dashboard_error',
      error: error.message
    }));
  }
}

async function handleQueryRequest(ws, data) {
  try {
    const result = await SnowflakeClient.executeTemplate(
      snowflakeConn,
      data.template,
      data.parameters
    );
    
    ws.send(JSON.stringify({
      type: 'query_result',
      result
    }));
    
  } catch (error) {
    ws.send(JSON.stringify({
      type: 'query_error',
      error: error.message
    }));
  }
}

// Graceful shutdown
process.on('SIGINT', async () => {
  console.log('\n📴 Shutting down gracefully...');
  
  if (snowflakeConn) {
    await SnowflakeClient.disconnect(snowflakeConn);
  }
  
  process.exit(0);
});

// Start the server
initialize().catch(console.error);

module.exports = { app };