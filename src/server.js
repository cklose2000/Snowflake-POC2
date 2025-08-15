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

// CORS for meta endpoints
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Headers', 'Content-Type');
  next();
});

// Redirect root to dashboard.html BEFORE static files
app.get('/', (req, res) => {
  res.redirect('/dashboard.html');
});

// Dashboard viewer route - must come before static files
app.get('/d/:dashboardId', (req, res) => {
  res.sendFile(path.join(__dirname, '../ui/viewer.html'));
});

// Static files (after redirect and viewer route)
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
  // Meta endpoints for schema-driven UI
  app.get('/meta/schema', (req, res) => {
    res.json({
      views: GeneratedSchema.VIEW_COLUMNS,
      tables: GeneratedSchema.TABLE_COLUMNS,
      hash: GeneratedSchema.CONTRACT_HASH,
      timestamp: new Date().toISOString()
    });
  });

  app.get('/meta/user', (req, res) => {
    res.json({
      timezone: Intl.DateTimeFormat().resolvedOptions().timeZone,
      theme: req.query.theme || 'dark',
      customer: process.env.ACTIVITY_CUSTOMER || 'default_user'
    });
  });
  
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

  // New API endpoint for dashboard data (calls stored procedures)
  app.post('/api/data', async (req, res) => {
    try {
      const { proc, params } = req.body;
      
      // Validate procedure name
      const validProcs = ['DASH_GET_SERIES', 'DASH_GET_TOPN', 'DASH_GET_EVENTS', 'DASH_GET_METRICS'];
      if (!validProcs.includes(proc)) {
        throw new Error('Invalid procedure name');
      }
      
      // Resolve SQL expressions in params
      const resolvedParams = {};
      for (const [key, value] of Object.entries(params || {})) {
        if (value && typeof value === 'object' && value.sql_expr) {
          // Execute SQL expression to get actual value
          const result = await SnowflakeClient.execute(snowflakeConn, `SELECT ${value.sql_expr} AS val`);
          resolvedParams[key] = result.rows[0].VAL;
        } else {
          resolvedParams[key] = value;
        }
      }
      
      // Build procedure call
      const paramList = Object.keys(resolvedParams).map(k => '?').join(', ');
      const sql = `CALL MCP.${proc}(${paramList})`;
      
      // Execute procedure
      const result = await SnowflakeClient.execute(snowflakeConn, sql, Object.values(resolvedParams));
      
      // Parse result (procedures return VARIANT)
      const procResult = result.rows[0][proc];
      
      res.json(procResult);
      
    } catch (error) {
      console.error('API data error:', error);
      res.status(500).json({ ok: false, error: error.message });
    }
  });

  // Get dashboard specification
  app.get('/api/dashboard/:dashboardId', async (req, res) => {
    try {
      const { dashboardId } = req.params;
      
      // Call GET_DASHBOARD procedure
      const result = await SnowflakeClient.execute(
        snowflakeConn, 
        'CALL MCP.GET_DASHBOARD(?)', 
        [dashboardId]
      );
      
      const procResult = result.rows[0].GET_DASHBOARD;
      res.json(procResult);
      
    } catch (error) {
      console.error('Get dashboard error:', error);
      res.status(500).json({ ok: false, error: error.message });
    }
  });

  // Save dashboard
  app.post('/api/dashboard/save', async (req, res) => {
    try {
      const { dashboard_id, title, spec, refresh_interval_sec } = req.body;
      
      // Call SAVE_DASHBOARD procedure
      const result = await SnowflakeClient.execute(
        snowflakeConn,
        'CALL MCP.SAVE_DASHBOARD(?, ?, ?, ?)',
        [dashboard_id, title, spec, refresh_interval_sec || 300]
      );
      
      const procResult = result.rows[0].SAVE_DASHBOARD;
      res.json(procResult);
      
    } catch (error) {
      console.error('Save dashboard error:', error);
      res.status(500).json({ ok: false, error: error.message });
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
            
          case 'execute_panel':
            // Execute panel query from dashboard
            await handlePanelRequest(ws, data);
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
  
  try {
    console.log('Processing NL query:', message);
    let sql = '';
    let queryType = '';
    
    // Check for different query patterns
    if (message.includes('hour') && (message.includes('24 hour') || message.includes('last 24'))) {
      // Use the view that already has the right column names
      sql = `SELECT * FROM ${GeneratedSchema.fqn('ACTIVITY_CCODE', 'VW_ACTIVITY_COUNTS_24H')} ORDER BY HOUR DESC`;
      queryType = 'time_series';
      
    } else if (message.includes('top') && (message.includes('activity') || message.includes('activities'))) {
      // Top activities
      sql = `
        SELECT ACTIVITY, SUM(EVENT_COUNT) as EVENT_COUNT
        FROM ${GeneratedSchema.fqn('ACTIVITY_CCODE', 'VW_ACTIVITY_COUNTS_24H')}
        GROUP BY ACTIVITY
        ORDER BY EVENT_COUNT DESC
        LIMIT 10
      `;
      queryType = 'ranking';
      
    } else if (message.includes('summary') || message.includes('metrics') || message.includes('total')) {
      // Summary metrics
      sql = `SELECT * FROM ${GeneratedSchema.fqn('ACTIVITY_CCODE', 'VW_ACTIVITY_SUMMARY')}`;
      queryType = 'metrics';
      
    } else if (message.includes('recent') || message.includes('latest') || 
               (message.includes('last') && message.includes('event'))) {
      // Recent events - extract number if specified
      const numberMatch = message.match(/\d+/);
      const limit = numberMatch ? parseInt(numberMatch[0]) : 20;
      
      sql = `
        SELECT activity_id, ts, customer, activity
        FROM ${GeneratedSchema.fqn('ACTIVITY', 'EVENTS')}
        ORDER BY ts DESC
        LIMIT ${limit}
      `;
      queryType = 'feed';
      
    } else {
      // Default - show summary
      sql = `SELECT * FROM ${GeneratedSchema.fqn('ACTIVITY_CCODE', 'VW_ACTIVITY_SUMMARY')}`;
      queryType = 'metrics';
      
      ws.send(JSON.stringify({
        type: 'response',
        message: 'I\'ll show you a summary of the activity data. Try asking for "activities by hour", "top activities", or "recent events" for more specific results.'
      }));
    }
    
    // Execute the query
    console.log('Executing SQL:', sql);
    const result = await SnowflakeClient.execute(snowflakeConn, sql);
    console.log('Query returned', result.rows?.length || 0, 'rows');
    
    // Log the SQL execution
    await activityLogger.log('sql_executed', {
      template: queryType,
      rows_returned: result.rows?.length || 0,
      natural_language: true
    });
    
    // Send result
    ws.send(JSON.stringify({
      type: 'query_result',
      result: {
        rows: result.rows,
        rowCount: result.rows?.length || 0,
        queryId: result.queryId,
        queryType: queryType
      }
    }));
    
  } catch (error) {
    console.error('Chat query error:', error);
    ws.send(JSON.stringify({
      type: 'error',
      message: `Query failed: ${error.message}`
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

async function handlePanelRequest(ws, data) {
  try {
    const panel = data.panel;
    let sql = '';
    
    // Build SQL based on panel type
    if (panel.type === 'time_series' && panel.source) {
      sql = `
        SELECT ${panel.x}, ${panel.metric}
        FROM ${GeneratedSchema.fqn('ACTIVITY_CCODE', panel.source)}
        ORDER BY ${panel.x}
      `;
    } else if (panel.type === 'ranking' && panel.source) {
      if (panel.group_by && panel.group_by.length > 0) {
        const groupCols = panel.group_by.join(', ');
        sql = `
          SELECT ${groupCols}, 
                 SUM(${panel.metric}) AS METRIC_VALUE
          FROM ${GeneratedSchema.fqn('ACTIVITY_CCODE', panel.source)}
          GROUP BY ${groupCols}
          ORDER BY METRIC_VALUE DESC
          LIMIT ${panel.top_n || 10}
        `;
      }
    } else if (panel.type === 'metrics' && panel.source) {
      sql = `SELECT * FROM ${GeneratedSchema.fqn('ACTIVITY_CCODE', panel.source)}`;
    } else if (panel.type === 'live_feed') {
      sql = `
        SELECT activity_id, ts, customer, activity, feature_json
        FROM ${GeneratedSchema.fqn('ACTIVITY', 'EVENTS')}
        ORDER BY ts DESC
        LIMIT ${panel.limit || 50}
      `;
    }
    
    if (!sql) {
      throw new Error('Unable to generate SQL for panel');
    }
    
    // Execute the query
    const result = await SnowflakeClient.execute(snowflakeConn, sql);
    
    // Log the activity
    await activityLogger.log('panel_executed', {
      panel_type: panel.type,
      source: panel.source,
      rows_returned: result.rows?.length || 0
    });
    
    // Send result
    ws.send(JSON.stringify({
      type: 'query_result',
      result: {
        rows: result.rows,
        rowCount: result.rows?.length || 0,
        queryId: result.queryId,
        panel: panel
      }
    }));
    
  } catch (error) {
    console.error('Panel execution error:', error);
    ws.send(JSON.stringify({
      type: 'error',
      message: `Panel execution failed: ${error.message}`
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