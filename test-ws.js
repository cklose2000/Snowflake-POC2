const WebSocket = require('ws');

const ws = new WebSocket('ws://localhost:8080');

ws.on('open', () => {
  console.log('Connected to WebSocket server');
  
  // Send a test chat message
  ws.send(JSON.stringify({
    type: 'chat',
    message: 'test message from script'
  }));
});

ws.on('message', (data) => {
  console.log('Received:', data.toString());
  ws.close();
});

ws.on('error', (error) => {
  console.error('WebSocket error:', error);
});