#!/usr/bin/env node

// Test the integrated server with new Claude wrapper
const WebSocket = require('ws');

async function testIntegratedServer() {
  console.log('🧪 Testing Integrated Server with Claude\n');
  
  // Wait a moment for server to be ready
  await new Promise(resolve => setTimeout(resolve, 2000));
  
  const ws = new WebSocket('ws://localhost:8080');
  const sessionId = 'integration_test_' + Date.now();
  
  const results = {
    connection: false,
    claude: false,
    safesql: false,
    context: false
  };
  
  ws.on('open', () => {
    console.log('✅ Connected to WebSocket server');
    results.connection = true;
    
    // Register session
    ws.send(JSON.stringify({
      type: 'register',
      sessionId: sessionId
    }));
    
    // Test 1: Basic Claude interaction
    setTimeout(() => {
      console.log('\n📝 Test 1: Claude Integration');
      console.log('   Sending: "Hello, what is 2+2?"');
      ws.send(JSON.stringify({
        type: 'user-message',
        sessionId: sessionId,
        content: 'Hello, what is 2+2?'
      }));
    }, 500);
    
    // Test 2: SafeSQL template
    setTimeout(() => {
      console.log('\n📝 Test 2: SafeSQL Template');
      console.log('   Sending: "/sql sample_top n=5"');
      ws.send(JSON.stringify({
        type: 'user-message',
        sessionId: sessionId,
        content: '/sql sample_top n=5'
      }));
    }, 8000);
    
    // Test 3: Context persistence
    setTimeout(() => {
      console.log('\n📝 Test 3: Context Check');
      console.log('   Sending: "My favorite number is 42"');
      ws.send(JSON.stringify({
        type: 'user-message',
        sessionId: sessionId,
        content: 'My favorite number is 42'
      }));
    }, 12000);
    
    setTimeout(() => {
      console.log('   Sending: "What is my favorite number?"');
      ws.send(JSON.stringify({
        type: 'user-message',
        sessionId: sessionId,
        content: 'What is my favorite number?'
      }));
    }, 18000);
    
    // Close and show results
    setTimeout(() => {
      console.log('\n📊 Test Results:');
      console.log('   WebSocket Connection:', results.connection ? '✅ PASS' : '❌ FAIL');
      console.log('   Claude Integration:', results.claude ? '✅ PASS' : '❌ FAIL');
      console.log('   SafeSQL Templates:', results.safesql ? '✅ PASS' : '❌ FAIL');
      console.log('   Context Persistence:', results.context ? '✅ PASS' : '❌ FAIL');
      
      const passCount = Object.values(results).filter(r => r).length;
      console.log(`\n🎯 Overall: ${passCount}/4 tests passed`);
      
      ws.close();
      process.exit(passCount === 4 ? 0 : 1);
    }, 25000);
  });
  
  ws.on('message', (data) => {
    const msg = JSON.parse(data);
    const time = new Date().toLocaleTimeString();
    
    if (msg.type === 'assistant-message') {
      console.log(`   [${time}] Claude:`, msg.content.substring(0, 100) + (msg.content.length > 100 ? '...' : ''));
      
      // Check responses
      if (msg.content.includes('4')) {
        results.claude = true;
      }
      if (msg.content.toLowerCase().includes('42') || msg.content.toLowerCase().includes('forty-two')) {
        results.context = true;
      }
    } else if (msg.type === 'sql-result') {
      console.log(`   [${time}] SQL Result: ${msg.count} rows from template "${msg.template}"`);
      if (msg.rows && msg.rows.length > 0) {
        results.safesql = true;
      }
    } else if (msg.type === 'info') {
      console.log(`   [${time}] Info:`, msg.content);
    } else if (msg.type === 'error') {
      console.log(`   [${time}] Error:`, msg.content);
    }
  });
  
  ws.on('error', (error) => {
    console.error('❌ WebSocket error:', error.message);
    process.exit(1);
  });
  
  ws.on('close', () => {
    console.log('🔌 WebSocket closed');
  });
}

// Check if server is running
const http = require('http');
http.get('http://localhost:3001/health', (res) => {
  if (res.statusCode === 200) {
    console.log('✅ Server is running on port 3001');
    testIntegratedServer();
  } else {
    console.error('❌ Server returned status:', res.statusCode);
    process.exit(1);
  }
}).on('error', (err) => {
  console.error('❌ Server is not running. Start it with: node integrated-server.js');
  console.error('   Error:', err.message);
  process.exit(1);
});