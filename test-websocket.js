// Test WebSocket connection and Claude Code integration
const WebSocket = require('ws');

const ws = new WebSocket('ws://localhost:8080');
const sessionId = 'test_session_' + Date.now();

ws.on('open', () => {
  console.log('✅ Connected to WebSocket');
  
  // Register session
  ws.send(JSON.stringify({
    type: 'register',
    sessionId: sessionId
  }));
  
  // Wait a bit then send test messages
  setTimeout(() => {
    console.log('\n📤 Sending: /help');
    ws.send(JSON.stringify({
      type: 'user-message',
      sessionId: sessionId,
      content: '/help'
    }));
  }, 500);
  
  setTimeout(() => {
    console.log('\n📤 Sending: /sql sample_top n=3');
    ws.send(JSON.stringify({
      type: 'user-message',
      sessionId: sessionId,
      content: '/sql sample_top n=3'
    }));
  }, 2000);
  
  setTimeout(() => {
    console.log('\n📤 Sending: Show me recent database activity');
    ws.send(JSON.stringify({
      type: 'user-message',
      sessionId: sessionId,
      content: 'Show me recent database activity'
    }));
  }, 4000);
  
  // Close after 10 seconds
  setTimeout(() => {
    console.log('\n👋 Closing connection');
    ws.close();
    process.exit(0);
  }, 10000);
});

ws.on('message', (data) => {
  const msg = JSON.parse(data);
  console.log('\n📥 Received:', msg.type);
  
  if (msg.type === 'sql-result') {
    console.log(`   → ${msg.count} rows returned from template: ${msg.template}`);
    if (msg.rows && msg.rows.length > 0) {
      console.log('   → Sample row:', {
        activity: msg.rows[0].ACTIVITY,
        customer: msg.rows[0].CUSTOMER
      });
    }
  } else if (msg.type === 'info') {
    console.log('   → Info:', msg.content.substring(0, 100) + '...');
  } else if (msg.type === 'assistant-message') {
    console.log('   → Claude says:', msg.content.substring(0, 200) + '...');
  } else if (msg.type === 'error') {
    console.log('   → Error:', msg.content || msg.error);
  }
});

ws.on('error', (error) => {
  console.error('❌ WebSocket error:', error);
});

ws.on('close', () => {
  console.log('🔌 Disconnected');
});