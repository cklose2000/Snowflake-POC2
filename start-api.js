// Start the improved API server with SafeSQL templates
const APIServer = require('./apps/ccode-bridge/src/api-server');
const WebSocket = require('ws');

console.log('🚀 Starting SnowflakePOC2 API Server with SafeSQL Templates...\n');

// Start API server
const apiServer = new APIServer(process.env.BRIDGE_PORT || 3001);
apiServer.start();

// Start WebSocket server
const wss = new WebSocket.Server({ port: 8080 });

wss.on('connection', (ws) => {
  console.log('🔌 New WebSocket connection');
  
  ws.send(JSON.stringify({
    type: 'welcome',
    message: 'Connected to SnowflakePOC2 Bridge',
    timestamp: new Date()
  }));
  
  ws.on('message', (message) => {
    try {
      const data = JSON.parse(message);
      console.log('📨 Received:', data.type);
      
      // Echo back with timestamp
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

console.log('🔌 WebSocket server running on ws://localhost:8080');

// Graceful shutdown
process.on('SIGINT', () => {
  console.log('\n👋 Shutting down gracefully...');
  wss.close();
  process.exit(0);
});