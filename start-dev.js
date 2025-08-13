// Simple development server starter
const { spawn } = require('child_process');
const path = require('path');
const fs = require('fs');

console.log('🚀 Starting SnowflakePOC2 Development Server...\n');

// For now, just start a simple test server
const express = require('express');
const WebSocket = require('ws');
const snowflake = require('snowflake-sdk');
require('dotenv').config();

const app = express();
const PORT = process.env.BRIDGE_PORT || 3001;
const WS_PORT = 8080;

// Create WebSocket server
const wss = new WebSocket.Server({ port: WS_PORT });

// Snowflake connection pool
let snowflakeConn = null;

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

// Initialize Snowflake connection
initSnowflake().catch(console.error);

// Express routes
app.use(express.json());
app.use(express.static(__dirname)); // Serve static files

// Serve test UI at root
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'test-ui.html'));
});

app.get('/health', (req, res) => {
  res.json({
    status: 'healthy',
    snowflake: snowflakeConn ? 'connected' : 'disconnected',
    websocket: wss.clients.size,
    timestamp: new Date()
  });
});

app.post('/api/query', async (req, res) => {
  const { template, params } = req.body;
  
  if (!snowflakeConn) {
    return res.status(500).json({ error: 'Snowflake not connected' });
  }
  
  let sql;
  
  // SafeSQL template handling
  switch(template) {
    case 'sample_top':
      sql = `SELECT * FROM ACTIVITY.EVENTS ORDER BY TS DESC LIMIT ${params.limit || 10}`;
      break;
      
    case 'describe_table':
      sql = `SELECT column_name, data_type, is_nullable, column_default 
             FROM information_schema.columns 
             WHERE table_schema = 'ACTIVITY' AND table_name = 'EVENTS'
             ORDER BY ordinal_position`;
      break;
      
    case 'time_series':
      sql = `SELECT DATE_TRUNC('hour', ts) as time_period, COUNT(*) as event_count
             FROM ACTIVITY.EVENTS
             WHERE ts > CURRENT_TIMESTAMP - INTERVAL '24 hours'
             GROUP BY time_period
             ORDER BY time_period DESC
             LIMIT ${params.limit || 24}`;
      break;
      
    case 'top_n':
      sql = `SELECT customer, activity, COUNT(*) as count
             FROM ACTIVITY.EVENTS
             GROUP BY customer, activity
             ORDER BY count DESC
             LIMIT ${params.limit || 10}`;
      break;
      
    default:
      return res.status(400).json({ error: 'Unknown template: ' + template });
  }
  
  snowflakeConn.execute({
    sqlText: sql,
    complete: (err, stmt, rows) => {
      if (err) {
        res.status(500).json({ error: err.message });
      } else {
        res.json({ rows, count: rows.length, template, sql });
      }
    }
  });
});

// Activity logging endpoint
app.post('/api/activity', async (req, res) => {
  const { activity, customer, feature_json } = req.body;
  
  if (!snowflakeConn) {
    return res.status(500).json({ error: 'Snowflake not connected' });
  }
  
  const activityId = `act_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
  
  const sql = `INSERT INTO ACTIVITY.EVENTS (
    activity_id, ts, customer, activity, feature_json, _source_system
  ) SELECT
    '${activityId}',
    CURRENT_TIMESTAMP(),
    '${customer || 'test_user'}',
    '${activity}',
    PARSE_JSON('${JSON.stringify(feature_json || {})}'),
    'test_ui'`;
  
  snowflakeConn.execute({
    sqlText: sql,
    complete: (err, stmt, rows) => {
      if (err) {
        res.status(500).json({ error: err.message });
      } else {
        res.json({ 
          success: true, 
          activity_id: activityId,
          message: 'Activity logged successfully'
        });
      }
    }
  });
});

// WebSocket handling
wss.on('connection', (ws) => {
  console.log('🔌 New WebSocket connection');
  
  ws.send(JSON.stringify({
    type: 'welcome',
    message: 'Connected to SnowflakePOC2 Bridge'
  }));
  
  ws.on('message', (message) => {
    try {
      const data = JSON.parse(message);
      console.log('📨 Received:', data.type);
      
      // Echo back for now
      ws.send(JSON.stringify({
        type: 'response',
        original: data,
        timestamp: new Date()
      }));
    } catch (err) {
      ws.send(JSON.stringify({
        type: 'error',
        error: err.message
      }));
    }
  });
  
  ws.on('close', () => {
    console.log('🔌 WebSocket disconnected');
  });
});

// Start Express server
app.listen(PORT, () => {
  console.log(`\n🌉 Bridge server running on http://localhost:${PORT}`);
  console.log(`🔌 WebSocket server running on ws://localhost:${WS_PORT}`);
  console.log('\n📊 Available endpoints:');
  console.log('  GET  /health        - Health check');
  console.log('  POST /api/query     - Execute SafeSQL template');
  console.log('\n✨ Development server ready!');
  console.log('Press Ctrl+C to stop\n');
});