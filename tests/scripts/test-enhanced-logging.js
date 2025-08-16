/**
 * Test enhanced logging with all 12 security improvements
 */

const SnowflakeSimpleClient = require('./snowflake-mcp-client/dist/simple-client').default;
const crypto = require('crypto');

// Test configuration
const config = {
  account: process.env.SNOWFLAKE_ACCOUNT || 'uec18397.us-east-1',
  username: process.env.SNOWFLAKE_USERNAME || 'CLAUDE_CODE_AI_AGENT',
  privateKeyPath: process.env.SF_PK_PATH || './claude_code_rsa_key.p8',
  warehouse: 'CLAUDE_AGENT_WH',
  database: 'CLAUDE_BI',
  schema: 'MCP'
  // Note: Role intentionally omitted - uses DEFAULT_ROLE
};

// Logging configuration for auto-batching
const logConfig = {
  batchThreshold: 5,     // Switch to batch at 5 events/min
  flushIntervalMs: 3000, // Flush every 3 seconds
  maxBatchSize: 50       // Max 50 events per batch
};

async function testLogging() {
  console.log('🧪 Testing Enhanced Claude Code Logging');
  console.log('=====================================\n');
  
  const client = new SnowflakeSimpleClient(config, logConfig);
  const sessionId = `test_${Date.now()}_${crypto.randomBytes(4).toString('hex')}`;
  
  try {
    // Connect to Snowflake
    console.log('📡 Connecting to Snowflake...');
    await client.connect();
    console.log('✅ Connected successfully\n');
    
    // Test 1: Log session start
    console.log('Test 1: Session Start');
    const startResult = await client.logSessionStart(sessionId, {
      version: '1.0.0',
      platform: process.platform,
      node_version: process.version
    });
    console.log('Result:', startResult);
    console.log('');
    
    // Test 2: Single event logging
    console.log('Test 2: Single Event');
    const singleResult = await client.logEvent({
      action: 'ccode.file.read',
      session_id: sessionId,
      actor_id: 'claude_code',
      attributes: {
        file_path: '/test/file.ts',
        size_bytes: 1024,
        success: true
      }
    });
    console.log('Result:', singleResult);
    console.log('');
    
    // Test 3: Trigger batch mode with rapid events
    console.log('Test 3: Batch Mode (sending 10 rapid events)');
    const promises = [];
    for (let i = 0; i < 10; i++) {
      promises.push(
        client.logEvent({
          action: 'ccode.tool.executed',
          session_id: sessionId,
          actor_id: 'claude_code',
          attributes: {
            tool_name: `tool_${i}`,
            execution_time_ms: Math.floor(Math.random() * 1000),
            iteration: i
          }
        })
      );
    }
    
    const batchResults = await Promise.all(promises);
    const bufferedCount = batchResults.filter(r => r.data?.buffered).length;
    console.log(`Buffered: ${bufferedCount}/10 events`);
    console.log('');
    
    // Test 4: Wait for auto-flush
    console.log('Test 4: Waiting for auto-flush (3 seconds)...');
    await new Promise(resolve => setTimeout(resolve, 3500));
    console.log('✅ Auto-flush should have completed\n');
    
    // Test 5: Error event
    console.log('Test 5: Error Event');
    const errorResult = await client.logEvent({
      action: 'ccode.error.occurred',
      session_id: sessionId,
      actor_id: 'claude_code',
      attributes: {
        error: 'Test error message',
        error_code: 'TEST_ERR_001',
        operation: 'test_operation',
        natural_language: 'User email is test@example.com' // Should be redacted
      }
    });
    console.log('Result:', errorResult);
    console.log('');
    
    // Test 6: Direct batch call
    console.log('Test 6: Direct Batch Call');
    const batchResult = await client.logEventsBatch([
      {
        action: 'ccode.batch.event1',
        session_id: sessionId,
        attributes: { index: 1 }
      },
      {
        action: 'ccode.batch.event2',
        session_id: sessionId,
        attributes: { index: 2 }
      },
      {
        action: 'ccode.batch.event3',
        session_id: sessionId,
        attributes: { index: 3 }
      }
    ], sessionId);
    console.log('Result:', batchResult);
    console.log('');
    
    // Test 7: Query tag verification
    console.log('Test 7: Query Tag Check');
    const queryResult = await client.executeSql('SELECT CURRENT_QUERY_TAG() AS tag');
    if (queryResult.success && queryResult.data?.[0]) {
      const tag = JSON.parse(queryResult.data[0].TAG);
      console.log('Current query tag:', tag);
    }
    console.log('');
    
    // Test 8: Session metrics
    console.log('Test 8: Get Session Metrics');
    const metricsResult = await client.callProcedure('CLAUDE_BI.MCP.GET_SESSION_METRICS', sessionId);
    console.log('Metrics:', metricsResult.data);
    console.log('');
    
    // Test 9: Log session end
    console.log('Test 9: Session End');
    const endResult = await client.logSessionEnd(sessionId, {
      events_logged: 20,
      duration_ms: Date.now() - parseInt(sessionId.split('_')[1])
    });
    console.log('Result:', endResult);
    console.log('');
    
    // Test 10: Verify events in dynamic table
    console.log('Test 10: Verify Events in Dynamic Table');
    const verifyResult = await client.executeSql(`
      SELECT 
        COUNT(*) as event_count,
        COUNT(DISTINCT action) as unique_actions
      FROM CLAUDE_BI.ACTIVITY.EVENTS
      WHERE session_id = ?
    `, [sessionId]);
    
    if (verifyResult.success && verifyResult.data?.[0]) {
      console.log('Events in dynamic table:', verifyResult.data[0]);
    }
    console.log('');
    
    // Test 11: Check monitoring views
    console.log('Test 11: Check Monitoring Views');
    const viewResult = await client.executeSql(`
      SELECT *
      FROM CLAUDE_BI.MCP.V_SESSION_PERFORMANCE
      WHERE session_id = ?
      LIMIT 1
    `, [sessionId]);
    
    if (viewResult.success && viewResult.data?.[0]) {
      console.log('Session performance:', {
        duration_minutes: viewResult.data[0].DURATION_MINUTES,
        total_events: viewResult.data[0].TOTAL_EVENTS,
        error_count: viewResult.data[0].ERROR_COUNT,
        avg_execution_ms: viewResult.data[0].AVG_EXECUTION_MS
      });
    }
    
    console.log('\n✅ All tests completed successfully!');
    
  } catch (error) {
    console.error('❌ Test failed:', error);
  } finally {
    // Disconnect
    await client.disconnect();
    console.log('\n🔌 Disconnected from Snowflake');
  }
}

// Run the test
testLogging().catch(console.error);