#!/usr/bin/env node

// Test the new Claude JSON integration
const ClaudeCodeWrapper = require('./packages/claude-wrapper');

async function testClaudeIntegration() {
  console.log('🧪 Testing Claude Code JSON Integration\n');
  
  const claude = new ClaudeCodeWrapper({ outputFormat: 'json' });
  const sessionId = 'test_' + Date.now();
  
  // Test results
  const results = {
    basic: false,
    context: false,
    sqlDetection: false,
    successClaim: false
  };
  
  // Setup event handlers
  claude.on('ready', () => {
    console.log('✅ Claude wrapper ready\n');
  });
  
  claude.on('sql-intent', (data) => {
    console.log('🔍 SQL Intent Detected!');
    console.log('   Session:', data.session_id);
    console.log('   Query:', data.query || 'No query extracted');
    results.sqlDetection = true;
  });
  
  claude.on('success-claim', (data) => {
    console.log('✅ Success Claim Detected!');
    console.log('   Session:', data.session_id);
    console.log('   Claim:', JSON.stringify(data.claim, null, 2));
    results.successClaim = true;
  });
  
  claude.on('error', (error) => {
    console.error('❌ Error:', error);
  });
  
  // Start the wrapper
  claude.start();
  
  // Wait for ready
  await new Promise(resolve => {
    claude.once('ready', resolve);
  });
  
  console.log('📝 Test 1: Basic Communication');
  console.log('   Sending: "What is 2+2?"');
  try {
    const response = await claude.send('What is 2+2?', sessionId);
    if (response) {
      console.log('   Response:', response.result.substring(0, 100));
      results.basic = response.result.includes('4');
    }
  } catch (error) {
    console.error('   Failed:', error.message);
  }
  
  // Test 2: Context Persistence
  console.log('\n📝 Test 2: Context Persistence');
  console.log('   Sending: "My name is TestBot"');
  try {
    const response1 = await claude.send('My name is TestBot', sessionId);
    if (response1) {
      console.log('   Response:', response1.result.substring(0, 100));
    }
    
    console.log('   Sending: "What is my name?"');
    const response2 = await claude.send('What is my name?', sessionId);
    if (response2) {
      console.log('   Response:', response2.result.substring(0, 200));
      results.context = response2.result.toLowerCase().includes('testbot');
    }
  } catch (error) {
    console.error('   Failed:', error.message);
  }
  
  // Test 3: SQL Intent Detection
  console.log('\n📝 Test 3: SQL Intent Detection');
  console.log('   Sending: "Show me the top 10 customers from the database"');
  try {
    const response = await claude.send('Show me the top 10 customers from the database', sessionId);
    if (response) {
      console.log('   Response preview:', response.result.substring(0, 150));
      // Check if SQL intent was detected during processing
      // Give it a moment to process events
      await new Promise(resolve => setTimeout(resolve, 100));
    }
  } catch (error) {
    console.error('   Failed:', error.message);
  }
  
  // Test 4: Success Claim Detection
  console.log('\n📝 Test 4: Success Claim Detection');
  console.log('   Sending: "I successfully returned 100 rows from the query"');
  try {
    // Simulate a success claim response
    const successText = 'The query completed successfully and returned 100 rows from the events table.';
    claude.processResponse(successText, sessionId);
  } catch (error) {
    console.error('   Failed:', error.message);
  }
  
  // Results Summary
  console.log('\n📊 Test Results Summary:');
  console.log('   Basic Communication:', results.basic ? '✅ PASS' : '❌ FAIL');
  console.log('   Context Persistence:', results.context ? '✅ PASS' : '❌ FAIL');
  console.log('   SQL Intent Detection:', results.sqlDetection ? '✅ PASS' : '❌ FAIL');
  console.log('   Success Claim Detection:', results.successClaim ? '✅ PASS' : '❌ FAIL');
  
  const passCount = Object.values(results).filter(r => r).length;
  console.log(`\n🎯 Overall: ${passCount}/4 tests passed`);
  
  // Cleanup
  claude.stop();
  process.exit(passCount === 4 ? 0 : 1);
}

// Run tests with timeout
const timeout = setTimeout(() => {
  console.error('\n⏱️ Test timeout after 30 seconds');
  process.exit(1);
}, 30000);

testClaudeIntegration().then(() => {
  clearTimeout(timeout);
}).catch(error => {
  console.error('Fatal error:', error);
  clearTimeout(timeout);
  process.exit(1);
});