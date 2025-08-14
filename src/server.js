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
const GeneratedSchema = require('./generated-schema');

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

// Runtime schema validation
async function validateRuntimeSchema(conn) {
  const mismatches = [];
  
  // Check each view in the contract
  for (const [viewName, expectedColumns] of Object.entries(GeneratedSchema.VIEW_COLUMNS)) {
    try {
      // Query actual columns from Snowflake
      const sql = `
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_CATALOG = '${GeneratedSchema.DB}'
          AND TABLE_SCHEMA = 'ACTIVITY_CCODE'
          AND TABLE_NAME = '${viewName}'
        ORDER BY ORDINAL_POSITION
      `;
      
      const result = await SnowflakeClient.execute(conn, sql);
      
      if (result.rows.length === 0) {
        mismatches.push(`View ${viewName} not found in database`);
        continue;
      }
      
      const actualColumns = result.rows.map(r => r.COLUMN_NAME.toUpperCase());
      const expectedSet = new Set(expectedColumns);
      const actualSet = new Set(actualColumns);
      
      // Check for missing columns
      for (const col of expectedColumns) {
        if (!actualSet.has(col)) {
          mismatches.push(`View ${viewName}: Expected column ${col} not found`);
        }
      }
      
      // Check for extra columns (warning only)
      for (const col of actualColumns) {
        if (!expectedSet.has(col)) {
          console.warn(`⚠️  View ${viewName}: Extra column ${col} found (not in contract)`);
        }
      }
    } catch (error) {
      mismatches.push(`View ${viewName}: ${error.message}`);
    }
  }
  
  if (mismatches.length > 0) {
    console.error('❌ Schema validation failed:');
    mismatches.forEach(m => console.error(`   - ${m}`));
    
    // Log schema violation
    if (activityLogger) {
      await activityLogger.log('schema_violation', {
        mismatches: mismatches,
        contract_hash: GeneratedSchema.CONTRACT_HASH
      });
    }
    
    throw new Error(`Schema mismatch detected: ${mismatches.length} issues found`);
  }
}

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
    
    // 3. Runtime sentinel - validate actual schema matches contract
    await validateRuntimeSchema(snowflakeConn);
    console.log('✅ Runtime schema validated');
    
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
        console.log('📨 Received message:', message.toString());
        const data = JSON.parse(message);
        console.log('📦 Parsed data:', data);
        
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
  
  // Parse the message to understand the query intent
  const message = data.message.toLowerCase();
  
  // Check if this is a time series request
  if (message.includes('hour') && (message.includes('24 hour') || message.includes('last 24'))) {
    // Generate and execute a time series query
    try {
      const sql = `
        SELECT 
          DATE_TRUNC('hour', ts) as hour,
          COUNT(*) as activity_count
        FROM ${SchemaContract.fqn('ACTIVITY', 'EVENTS')}
        WHERE ts >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
        GROUP BY 1
        ORDER BY 1 DESC
      `;
      
      const result = await SnowflakeClient.execute(snowflakeConn, sql);
      
      // Log the SQL execution
      await activityLogger.log('sql_executed', {
        template: 'time_series',
        rows_returned: result.rows?.length || 0
      });
      
      ws.send(JSON.stringify({
        type: 'query_result',
        result: {
          rows: result.rows,
          rowCount: result.rows?.length || 0,
          queryId: result.queryId
        }
      }));
      
      ws.send(JSON.stringify({
        type: 'response',
        message: 'I\'ve generated a time series query showing activity counts by hour for the last 24 hours.'
      }));
      
    } catch (error) {
      ws.send(JSON.stringify({
        type: 'error',
        message: `Query failed: ${error.message}`
      }));
    }
  } else {
    // Default echo response
    ws.send(JSON.stringify({
      type: 'response',
      message: `Received: ${data.message}`
    }));
  }
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