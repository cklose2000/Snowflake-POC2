/**
 * Test if Claude Code logging works for simple queries
 * 
 * This uses ONLY RSA key authentication - no passwords!
 * The key was already set up on the CLAUDE_CODE_AI_AGENT user in Snowflake.
 */

const SnowflakeSimpleClient = require('./snowflake-mcp-client/dist/simple-client').default;
const crypto = require('crypto');

async function testQueryLogging() {
  console.log('🧪 Claude Code Query Test (RSA Key Auth Only)\n');
  console.log('Using RSA key: ./claude_code_rsa_key.p8');
  console.log('No passwords involved!\n');
  
  // ONLY configuration needed - RSA key does the auth
  const config = {
    account: 'uec18397.us-east-1',
    username: 'CLAUDE_CODE_AI_AGENT',
    privateKeyPath: './claude_code_rsa_key.p8',
    warehouse: 'CLAUDE_AGENT_WH',
    database: 'CLAUDE_BI',
    schema: 'MCP'
    // NO PASSWORD FIELD - authentication is via RSA key
  };
  
  const client = new SnowflakeSimpleClient(config);
  const sessionId = `query_test_${crypto.randomBytes(4).toString('hex')}`;
  
  try {
    await client.connect();
    
    // Log that we're starting a query session
    await client.logEvent({
      action: 'ccode.query.initiated',
      session_id: sessionId,
      actor_id: 'claude_code',
      attributes: {
        query_type: 'last_10_events',
        purpose: 'user_request'
      }
    });
    
    // Execute the actual query
    console.log('📊 Querying last 10 events...\n');
    const result = await client.executeSql(`
      SELECT 
        occurred_at,
        action,
        actor_id,
        session_id,
        attributes
      FROM CLAUDE_BI.ACTIVITY.EVENTS
      ORDER BY occurred_at DESC
      LIMIT 10
    `);
    
    // Log the query completion
    await client.logEvent({
      action: 'ccode.query.completed',
      session_id: sessionId,
      actor_id: 'claude_code',
      attributes: {
        rows_returned: result.data?.length || 0,
        execution_time_ms: result.metadata?.executionTimeMs,
        success: result.success
      }
    });
    
    if (result.success && result.data) {
      console.log(`Found ${result.data.length} events:\n`);
      result.data.forEach((row, i) => {
        console.log(`${i + 1}. ${row.OCCURRED_AT} - ${row.ACTION} (${row.ACTOR_ID})`);
      });
    }
    
    console.log('\n✅ Query executed and logged successfully!');
    
  } catch (error) {
    // Log the error
    await client.logEvent({
      action: 'ccode.query.failed',
      session_id: sessionId,
      actor_id: 'claude_code',
      attributes: {
        error: error.message,
        query_type: 'last_10_events'
      }
    }).catch(console.error);
    
    console.error('❌ Error:', error);
  } finally {
    await client.disconnect();
  }
}

// Run it
testQueryLogging().catch(console.error);