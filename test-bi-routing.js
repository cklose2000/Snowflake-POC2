#!/usr/bin/env node

// Test BI-First Smart Routing Performance
const WebSocket = require('ws');
const http = require('http');

async function testBIRouting() {
  console.log('🎯 Testing BI-First Smart Routing\n');
  
  // Wait for server to be ready
  await new Promise(resolve => setTimeout(resolve, 3000));
  
  const ws = new WebSocket('ws://localhost:8080');
  const sessionId = 'bi_test_' + Date.now();
  
  // Test queries for each tier
  const testQueries = [
    // Tier 1: Direct SafeSQL (should be < 3 seconds)
    { tier: 1, query: "show me the top 5 activities", expectedRoute: "direct_safesql" },
    { tier: 1, query: "list recent activities", expectedRoute: "direct_safesql" },
    { tier: 1, query: "activity breakdown by type", expectedRoute: "direct_safesql" },
    
    // Tier 2: Lite AI (should be < 8 seconds) 
    { tier: 2, query: "compare this month vs last month", expectedRoute: "lite_ai" },
    { tier: 2, query: "filter activities by user", expectedRoute: "lite_ai" },
    
    // Tier 3: Full Claude (should be < 30 seconds)
    { tier: 3, query: "write a report analyzing our data trends", expectedRoute: "full_claude" },
  ];
  
  const results = [];
  let currentTest = 0;
  
  ws.on('open', () => {
    console.log('✅ Connected to WebSocket server');
    
    // Register session
    ws.send(JSON.stringify({
      type: 'register',
      sessionId: sessionId
    }));
    
    // Start first test
    runNextTest();
  });
  
  ws.on('message', (data) => {
    const msg = JSON.parse(data);
    const time = new Date().toLocaleTimeString();
    
    if (msg.type === 'assistant-message' || msg.type === 'sql-result') {
      const testQuery = testQueries[currentTest - 1];
      if (testQuery) {
        const duration = Date.now() - testQuery.startTime;
        results.push({
          ...testQuery,
          actualDuration: duration,
          success: true,
          response: msg.type
        });
        
        console.log(`   ✅ Response received in ${duration}ms`);
        console.log(`   📊 Expected <${testQuery.tier === 1 ? '3000' : testQuery.tier === 2 ? '8000' : '30000'}ms`);
        console.log(`   🎯 Performance: ${duration < (testQuery.tier === 1 ? 3000 : testQuery.tier === 2 ? 8000 : 30000) ? 'GOOD' : 'SLOW'}\n`);
      }
      
      // Run next test after delay
      setTimeout(runNextTest, 2000);
    } else if (msg.type === 'error') {
      console.log(`   ❌ Error: ${msg.content}`);
      setTimeout(runNextTest, 2000);
    }
  });
  
  function runNextTest() {
    if (currentTest >= testQueries.length) {
      showResults();
      return;
    }
    
    const testQuery = testQueries[currentTest];
    console.log(`📝 Test ${currentTest + 1}/${testQueries.length}: Tier ${testQuery.tier}`);
    console.log(`   Query: "${testQuery.query}"`);
    console.log(`   Expected: ${testQuery.expectedRoute}`);
    
    testQuery.startTime = Date.now();
    
    ws.send(JSON.stringify({
      type: 'user-message',
      sessionId: sessionId,
      content: testQuery.query
    }));
    
    currentTest++;
  }
  
  function showResults() {
    console.log('📊 BI Routing Performance Results:\n');
    
    const tier1Results = results.filter(r => r.tier === 1);
    const tier2Results = results.filter(r => r.tier === 2);
    const tier3Results = results.filter(r => r.tier === 3);
    
    console.log('⚡ Tier 1 (Direct SafeSQL):');
    tier1Results.forEach(r => {
      console.log(`   ${r.query}: ${r.actualDuration}ms ${r.actualDuration < 3000 ? '✅' : '❌'}`);
    });
    
    console.log('\n🧠 Tier 2 (Lite AI):');
    tier2Results.forEach(r => {
      console.log(`   ${r.query}: ${r.actualDuration}ms ${r.actualDuration < 8000 ? '✅' : '❌'}`);
    });
    
    console.log('\n🚀 Tier 3 (Full Claude):');
    tier3Results.forEach(r => {
      console.log(`   ${r.query}: ${r.actualDuration}ms ${r.actualDuration < 30000 ? '✅' : '❌'}`);
    });
    
    // Performance summary
    const avgTier1 = tier1Results.reduce((sum, r) => sum + r.actualDuration, 0) / tier1Results.length;
    const avgTier2 = tier2Results.reduce((sum, r) => sum + r.actualDuration, 0) / tier2Results.length;
    const avgTier3 = tier3Results.reduce((sum, r) => sum + r.actualDuration, 0) / tier3Results.length;
    
    console.log('\n🎯 Performance Summary:');
    console.log(`   Tier 1 Average: ${Math.round(avgTier1)}ms (target: <3000ms)`);
    console.log(`   Tier 2 Average: ${Math.round(avgTier2)}ms (target: <8000ms)`);
    console.log(`   Tier 3 Average: ${Math.round(avgTier3)}ms (target: <30000ms)`);
    
    const allPassed = results.every(r => {
      const target = r.tier === 1 ? 3000 : r.tier === 2 ? 8000 : 30000;
      return r.actualDuration < target;
    });
    
    console.log(`\n🏆 Overall: ${allPassed ? 'ALL TARGETS MET!' : 'Some targets missed'}`);
    
    // Get routing stats
    fetchRoutingStats();
    
    ws.close();
  }
  
  ws.on('error', (error) => {
    console.error('❌ WebSocket error:', error.message);
    process.exit(1);
  });
  
  ws.on('close', () => {
    console.log('🔌 Test complete');
    process.exit(0);
  });
}

// Fetch routing statistics from the server
async function fetchRoutingStats() {
  try {
    const response = await new Promise((resolve, reject) => {
      const req = http.get('http://localhost:3001/api/routing-stats', resolve);
      req.on('error', reject);
    });
    
    let data = '';
    response.on('data', chunk => data += chunk);
    response.on('end', () => {
      try {
        const stats = JSON.parse(data);
        console.log('\n📈 Routing Statistics:');
        console.log(JSON.stringify(stats.routing_performance, null, 2));
      } catch (error) {
        console.log('Unable to parse routing stats');
      }
    });
  } catch (error) {
    console.log('Could not fetch routing stats:', error.message);
  }
}

// Check if server is running
http.get('http://localhost:3001/health', (res) => {
  if (res.statusCode === 200) {
    console.log('✅ Server is running, starting BI routing tests...\n');
    testBIRouting();
  } else {
    console.error('❌ Server returned status:', res.statusCode);
    process.exit(1);
  }
}).on('error', (err) => {
  console.error('❌ Server is not running. Start it with: node integrated-server.js');
  console.error('   Error:', err.message);
  process.exit(1);
});