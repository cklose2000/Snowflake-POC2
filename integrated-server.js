// Integrated Server - Complete WebSocket + API + Claude Code + SafeSQL
const express = require('express');
const WebSocket = require('ws');
const snowflake = require('snowflake-sdk');
const MessageRouter = require('./apps/ccode-bridge/src/message-router');
const DashboardFactory = require('./packages/dashboard-factory/index.js');
const schemaConfig = require('./packages/snowflake-schema');
const SchemaValidator = require('./packages/snowflake-schema/validator');
const path = require('path');
require('dotenv').config();

console.log('🚀 Starting SnowflakePOC2 Integrated Server...\n');

const app = express();
const PORT = process.env.BRIDGE_PORT || 3001;
const WS_PORT = 8080;

// Middleware
app.use(express.json());
app.use(express.static(__dirname));

// Snowflake connection and services
let snowflakeConn = null;
let messageRouter = null;
let dashboardFactory = null;

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
    
    // Set database context using schema config
    const contextSQL = schemaConfig.getContextSQL();
    for (const sql of contextSQL) {
      await new Promise((resolve, reject) => {
        snowflakeConn.execute({
          sqlText: sql,
          complete: (err) => err ? reject(err) : resolve()
        });
      });
    }
    console.log('✅ Database context set');
    
    // Validate schema structure
    console.log('\n🔍 Validating schema structure...');
    const validator = new SchemaValidator(snowflakeConn);
    const validation = await validator.validateAll();
    
    if (!validation.isValid) {
      console.error('\n❌ SCHEMA VALIDATION FAILED');
      console.error('Errors:', validation.errors);
      console.error('\nTo fix, run: npm run bootstrap-schema');
      
      if (process.env.NODE_ENV === 'production') {
        console.error('Exiting due to schema validation failure in production');
        process.exit(1);
      } else {
        console.warn('\n⚠️ Continuing in development mode despite validation errors');
      }
    } else {
      console.log('✅ Schema validation passed');
    }
    
    if (validation.hasWarnings) {
      console.warn('\n⚠️ Schema warnings:');
      validation.warnings.forEach(w => console.warn(`   - ${w.message}`));
    }
    
    console.log(`\n📍 Context: ${validation.context.database}.${validation.context.schema}`);
    console.log(`👤 Role: ${validation.context.role}`);
    console.log(`🏭 Warehouse: ${validation.context.warehouse}\n`);
    
    // Create message router
    messageRouter = new MessageRouter(snowflakeConn);
    
    // Create dashboard factory
    dashboardFactory = new DashboardFactory(snowflakeConn, {
      dryRun: process.env.DASHBOARD_DRY_RUN === 'true',
      timeout: parseInt(process.env.DASHBOARD_TIMEOUT) || 300000
    });
    
    // Start Claude Code CLI integration
    await messageRouter.start();
    
    console.log('✅ Message router initialized with Claude Code CLI');
    console.log('✅ Dashboard Factory initialized');
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

        case 'dashboard-create':
          if (dashboardFactory) {
            await handleDashboardCreate(ws, data, sessionId);
          } else {
            ws.send(JSON.stringify({
              type: 'dashboard.error',
              spec_id: data.spec_id,
              code: 'service_unavailable',
              message: 'Dashboard Factory not available'
            }));
          }
          break;

        case 'dashboard-destroy':
          if (dashboardFactory) {
            await handleDashboardDestroy(ws, data, sessionId);
          } else {
            ws.send(JSON.stringify({
              type: 'dashboard.error',
              spec_id: data.spec_id,
              code: 'service_unavailable',
              message: 'Dashboard Factory not available'
            }));
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

// Dashboard WebSocket handlers
async function handleDashboardCreate(ws, data, sessionId) {
  const { conversationHistory, customerID } = data;
  const effectiveCustomerID = customerID || sessionId || 'anonymous';
  
  console.log(`🏭 Creating dashboard for customer: ${effectiveCustomerID}`);
  
  // Send initial progress
  ws.send(JSON.stringify({
    type: 'dashboard.progress',
    step: 'analyzing',
    spec_id: null,
    pct: 10
  }));

  try {
    const result = await dashboardFactory.createDashboard(
      conversationHistory || [],
      effectiveCustomerID,
      sessionId
    );

    if (result.success) {
      // Send completion event
      ws.send(JSON.stringify({
        type: 'dashboard.created',
        spec_id: result.specId,
        url: result.url,
        schedule: {
          mode: result.schedule?.mode || 'exact',
          display: result.refreshSchedule
        },
        panels_count: result.panelsCount,
        creation_time_ms: result.creationTimeMs
      }));
    } else {
      // Send error event
      ws.send(JSON.stringify({
        type: 'dashboard.error',
        spec_id: null,
        code: 'creation_failed',
        message: result.error,
        remediation: 'Check logs for details and try again with a simpler request'
      }));
    }
  } catch (error) {
    console.error('Dashboard creation error:', error);
    ws.send(JSON.stringify({
      type: 'dashboard.error',
      spec_id: null,
      code: 'internal_error',
      message: error.message,
      remediation: 'Check server logs and try again'
    }));
  }
}

async function handleDashboardDestroy(ws, data, sessionId) {
  const { spec_id, customerID } = data;
  const effectiveCustomerID = customerID || sessionId || 'anonymous';
  
  console.log(`🗑️ Destroying dashboard ${spec_id} for customer: ${effectiveCustomerID}`);
  
  try {
    // For now, we'll implement this as a placeholder since destroy isn't implemented in v1
    ws.send(JSON.stringify({
      type: 'dashboard.error',
      spec_id: spec_id,
      code: 'not_implemented',
      message: 'Dashboard destruction not implemented in v1',
      remediation: 'Manual cleanup required - see documentation'
    }));
  } catch (error) {
    console.error('Dashboard destruction error:', error);
    ws.send(JSON.stringify({
      type: 'dashboard.error',
      spec_id: spec_id,
      code: 'internal_error',
      message: error.message
    }));
  }
}

// Express routes
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'chat-ui.html'));
});

app.get('/health', async (req, res) => {
  // Enhanced health check with Dashboard Factory
  const healthStatus = {
    status: 'healthy',
    snowflake: snowflakeConn ? 'connected' : 'disconnected',
    websocket: wss.clients.size,
    dashboard_factory: dashboardFactory ? 'initialized' : 'unavailable',
    timestamp: new Date()
  };

  // Test Snowflake connectivity
  if (snowflakeConn) {
    try {
      await new Promise((resolve, reject) => {
        snowflakeConn.execute({
          sqlText: 'SELECT 1 as health_check',
          complete: (err, stmt) => {
            if (err) {
              reject(err);
            } else {
              resolve(stmt);
            }
          }
        });
      });
      healthStatus.snowflake_test = 'success';
    } catch (error) {
      healthStatus.snowflake_test = 'failed';
      healthStatus.snowflake_error = error.message;
      healthStatus.status = 'degraded';
    }
  }

  // Test Activity Schema connectivity
  if (snowflakeConn) {
    try {
      await new Promise((resolve, reject) => {
        snowflakeConn.execute({
          sqlText: 'SELECT COUNT(*) as event_count FROM analytics.activity.events LIMIT 1',
          complete: (err, stmt) => {
            if (err) {
              reject(err);
            } else {
              healthStatus.activity_schema = 'accessible';
              resolve(stmt);
            }
          }
        });
      });
    } catch (error) {
      healthStatus.activity_schema = 'inaccessible';
      healthStatus.activity_error = error.message;
    }
  }

  const statusCode = healthStatus.status === 'healthy' ? 200 : 503;
  res.status(statusCode).json(healthStatus);
});

// Routing statistics endpoint
app.get('/api/routing-stats', (req, res) => {
  if (!messageRouter) {
    return res.status(503).json({ error: 'Message router not initialized' });
  }
  
  try {
    const stats = messageRouter.getRoutingStats();
    res.json({
      status: 'success',
      timestamp: new Date().toISOString(),
      routing_performance: stats
    });
  } catch (error) {
    res.status(500).json({ 
      error: 'Failed to get routing stats', 
      details: error.message 
    });
  }
});

// Query suggestion endpoint (for debugging/optimization)
app.post('/api/query-suggestions', express.json(), (req, res) => {
  if (!messageRouter) {
    return res.status(503).json({ error: 'Message router not initialized' });
  }
  
  const { query } = req.body;
  if (!query) {
    return res.status(400).json({ error: 'Query parameter required' });
  }
  
  try {
    const suggestions = messageRouter.getQuerySuggestions(query);
    res.json({
      status: 'success',
      query: query,
      suggestions: suggestions
    });
  } catch (error) {
    res.status(500).json({ 
      error: 'Failed to get query suggestions', 
      details: error.message 
    });
  }
});

// Dashboard API endpoints
app.post('/api/dashboard/create', express.json(), async (req, res) => {
  if (!dashboardFactory) {
    return res.status(503).json({ 
      error: 'Dashboard Factory not available',
      code: 'service_unavailable'
    });
  }

  const { conversationHistory, customerID, sessionID } = req.body;
  
  if (!conversationHistory || !Array.isArray(conversationHistory)) {
    return res.status(400).json({ 
      error: 'conversationHistory is required and must be an array',
      code: 'invalid_request'
    });
  }

  const effectiveCustomerID = customerID || 'api_user';
  const effectiveSessionID = sessionID || `rest_${Date.now()}`;

  try {
    console.log(`🏭 REST API: Creating dashboard for customer: ${effectiveCustomerID}`);
    
    const result = await dashboardFactory.createDashboard(
      conversationHistory,
      effectiveCustomerID,
      effectiveSessionID
    );

    if (result.success) {
      res.json({
        success: true,
        spec_id: result.specId,
        url: result.url,
        name: result.name,
        panels_count: result.panelsCount,
        creation_time_ms: result.creationTimeMs,
        schedule: result.refreshSchedule,
        objects_created: result.objectsCreated
      });
    } else {
      res.status(400).json({
        success: false,
        error: result.error,
        code: 'creation_failed',
        creation_time_ms: result.creationTimeMs
      });
    }
  } catch (error) {
    console.error('Dashboard creation error:', error);
    res.status(500).json({
      success: false,
      error: error.message,
      code: 'internal_error'
    });
  }
});

app.post('/api/dashboard/destroy', express.json(), async (req, res) => {
  if (!dashboardFactory) {
    return res.status(503).json({ 
      error: 'Dashboard Factory not available',
      code: 'service_unavailable'
    });
  }

  const { spec_id } = req.body;
  
  if (!spec_id) {
    return res.status(400).json({ 
      error: 'spec_id is required',
      code: 'invalid_request'
    });
  }

  try {
    // For v1, dashboard destruction is not implemented
    res.status(501).json({
      success: false,
      error: 'Dashboard destruction not implemented in v1',
      code: 'not_implemented',
      remediation: 'Manual cleanup required - contact administrator'
    });
  } catch (error) {
    console.error('Dashboard destruction error:', error);
    res.status(500).json({
      success: false,
      error: error.message,
      code: 'internal_error'
    });
  }
});

// Dashboard Factory statistics endpoint
app.get('/api/dashboard/stats', (req, res) => {
  if (!dashboardFactory) {
    return res.status(503).json({ error: 'Dashboard Factory not available' });
  }

  try {
    const stats = dashboardFactory.getCreationStats();
    res.json({
      success: true,
      timestamp: new Date().toISOString(),
      stats: stats
    });
  } catch (error) {
    res.status(500).json({ 
      success: false,
      error: 'Failed to get Dashboard Factory stats', 
      details: error.message 
    });
  }
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
  console.log('  • Dashboard Factory integration');
  console.log('  • Dashboard creation API at /api/dashboard/create');
  console.log('  • Enhanced health checks at /health');
  console.log('\n✨ Integrated server with Dashboard Factory ready!');
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