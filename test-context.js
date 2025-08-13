// Test if Claude maintains context across messages
const WebSocket = require('ws');

const ws = new WebSocket('ws://localhost:8080');
const sessionId = 'context_test_' + Date.now();

ws.on('open', () => {
  console.log('✅ Connected to WebSocket');
  
  // Register session
  ws.send(JSON.stringify({
    type: 'register',
    sessionId: sessionId
  }));
  
  const messages = [
    { delay: 500, msg: "My name is TestBot and I'm testing the system" },
    { delay: 5000, msg: "What is my name?" },
    { delay: 10000, msg: "Show me the last 5 activities in the database" },
    { delay: 15000, msg: "How many rows did that return?" }
  ];
  
  messages.forEach(({ delay, msg }) => {
    setTimeout(() => {
      console.log(`\n📤 [${new Date().toLocaleTimeString()}] Sending: ${msg}`);
      ws.send(JSON.stringify({
        type: 'user-message',
        sessionId: sessionId,
        content: msg
      }));
    }, delay);
  });
  
  // Close after 20 seconds
  setTimeout(() => {
    console.log('\n👋 Testing complete, closing connection');
    ws.close();
    process.exit(0);
  }, 20000);
});

ws.on('message', (data) => {
  const msg = JSON.parse(data);
  const time = new Date().toLocaleTimeString();
  
  if (msg.type === 'assistant-message') {
    console.log(`📥 [${time}] Claude:`, msg.content.substring(0, 150) + (msg.content.length > 150 ? '...' : ''));
  } else if (msg.type === 'sql-result') {
    console.log(`📥 [${time}] SQL Result: ${msg.count} rows from ${msg.template}`);
  } else if (msg.type === 'info' || msg.type === 'error') {
    console.log(`📥 [${time}] ${msg.type}:`, msg.content || msg.error);
  }
});

ws.on('error', (error) => {
  console.error('❌ WebSocket error:', error);
});

ws.on('close', () => {
  console.log('🔌 Disconnected');
});