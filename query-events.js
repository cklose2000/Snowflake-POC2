/**
 * Query the last 10 events from the system
 */

const SnowflakeSimpleClient = require('./snowflake-mcp-client/dist/simple-client').default;

async function queryEvents() {
  const config = {
    account: 'uec18397.us-east-1',
    username: 'CLAUDE_CODE_AI_AGENT',
    privateKeyPath: './claude_code_rsa_key.p8',
    warehouse: 'CLAUDE_WAREHOUSE',
    database: 'CLAUDE_BI',
    schema: 'MCP'
  };
  
  const client = new SnowflakeSimpleClient(config);
  
  try {
    await client.connect();
    
    // Log this query as an event
    await client.callProcedure(
      'CLAUDE_BI.MCP.LOG_CLAUDE_EVENT',
      {
        event_id: `query_${Date.now()}`,
        action: 'ccode.query.last_10_events',
        actor_id: 'claude_code',
        occurred_at: new Date().toISOString(),
        attributes: {
          purpose: 'user_requested_last_10_events',
          query_type: 'activity_events'
        }
      },
      'CLAUDE_CODE'
    );
    
    console.log('📊 Last 10 Events in the System:\n');
    console.log('=' .repeat(80));
    
    const result = await client.executeSql(`
      SELECT 
        EVENT_ID,
        OCCURRED_AT,
        ACTION,
        ACTOR_ID,
        OBJECT_TYPE,
        OBJECT_ID,
        SOURCE,
        ATTRIBUTES
      FROM CLAUDE_BI.ACTIVITY.EVENTS
      ORDER BY OCCURRED_AT DESC
      LIMIT 10
    `);
    
    if (result.success && result.data) {
      result.data.forEach((event, i) => {
        console.log(`\n${i + 1}. Event: ${event.EVENT_ID}`);
        console.log(`   Time: ${event.OCCURRED_AT}`);
        console.log(`   Action: ${event.ACTION}`);
        console.log(`   Actor: ${event.ACTOR_ID}`);
        if (event.OBJECT_TYPE && event.OBJECT_ID) {
          console.log(`   Object: ${event.OBJECT_TYPE} (${event.OBJECT_ID})`);
        }
        console.log(`   Source: ${event.SOURCE || 'N/A'}`);
        if (event.ATTRIBUTES) {
          const attrs = typeof event.ATTRIBUTES === 'string' 
            ? JSON.parse(event.ATTRIBUTES) 
            : event.ATTRIBUTES;
          if (attrs && Object.keys(attrs).length > 0) {
            console.log(`   Attributes:`);
            Object.entries(attrs).slice(0, 3).forEach(([key, value]) => {
              console.log(`     - ${key}: ${JSON.stringify(value)}`);
            });
          }
        }
      });
      
      console.log('\n' + '=' .repeat(80));
      console.log(`Total: ${result.data.length} events retrieved`);
    } else {
      console.log('No events found');
    }
    
  } catch (error) {
    console.error('Error:', error);
  } finally {
    await client.disconnect();
  }
}

queryEvents().catch(console.error);