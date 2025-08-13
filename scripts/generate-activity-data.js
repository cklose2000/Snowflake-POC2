#!/usr/bin/env node

// Generate Activity data by executing SafeSQL queries
const WebSocket = require('ws');

const queries = [
  "Show me the top 10 activities by count",
  "What are the most recent events in the activity stream?",
  "Show a breakdown of activities by customer", 
  "List the different activity types we track",
  "Show me activity counts by hour for today",
  "What's the total number of events in the last 24 hours?",
  "Show me failed activities if any",
  "List unique customers from recent activity",
  "Show activity patterns over the last week",
  "What are the most common activities?"
];

async function sendQuery(query, index) {
  return new Promise((resolve) => {
    const ws = new WebSocket('ws://localhost:8080');
    
    ws.on('open', () => {
      console.log(`[${index + 1}/${queries.length}] Sending: ${query}`);
      
      const message = {
        type: 'user-message',
        content: query,
        sessionId: `activity-gen-${Date.now()}-${index}`
      };
      
      ws.send(JSON.stringify(message));
    });
    
    let gotResult = false;
    ws.on('message', (data) => {
      try {
        const msg = JSON.parse(data);
        if (msg.type === 'sql_result' || msg.sql_result) {
          gotResult = true;
          console.log(`   ✅ Got result with ${msg.sql_result?.rows?.length || 0} rows`);
        }
        if (msg.type === 'complete' || msg.type === 'error') {
          ws.close();
          resolve(gotResult);
        }
      } catch (e) {
        // Ignore parse errors
      }
    });
    
    ws.on('error', (err) => {
      console.error(`   ❌ Error: ${err.message}`);
      ws.close();
      resolve(false);
    });
    
    // Timeout after 3 seconds
    setTimeout(() => {
      ws.close();
      resolve(gotResult);
    }, 3000);
  });
}

async function generateActivityData() {
  console.log('🚀 Generating Activity Data via SafeSQL Queries\n');
  
  let successCount = 0;
  
  for (let i = 0; i < queries.length; i++) {
    const success = await sendQuery(queries[i], i);
    if (success) successCount++;
    await new Promise(resolve => setTimeout(resolve, 500)); // Wait between queries
  }
  
  console.log(`\n✅ Generated activity data: ${successCount}/${queries.length} queries succeeded`);
  console.log('Activity data is now available in ACTIVITY.EVENTS table');
}

generateActivityData().catch(console.error);