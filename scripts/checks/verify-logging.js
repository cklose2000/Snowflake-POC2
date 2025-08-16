/**
 * Verify that logging is working by checking recent events
 */

const SnowflakeSimpleClient = require('./snowflake-mcp-client/dist/simple-client').default;

async function verifyLogging() {
  console.log('✅ LOGGING VERIFICATION\n');
  
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
    
    // Check RAW_EVENTS
    console.log('📊 Recent events in RAW_EVENTS:');
    const rawEvents = await client.executeSql(`
      SELECT 
        PAYLOAD:event_id::STRING AS event_id,
        PAYLOAD:action::STRING AS action,
        PAYLOAD:actor_id::STRING AS actor_id,
        _SOURCE_LANE,
        _RECV_AT
      FROM CLAUDE_BI.LANDING.RAW_EVENTS
      WHERE _RECV_AT >= DATEADD('minute', -10, CURRENT_TIMESTAMP())
      ORDER BY _RECV_AT DESC
      LIMIT 5
    `);
    
    if (rawEvents.success && rawEvents.data?.length > 0) {
      console.log(`Found ${rawEvents.data.length} events in RAW_EVENTS:`);
      rawEvents.data.forEach((row, i) => {
        console.log(`${i + 1}. ${row.EVENT_ID} - ${row.ACTION} (${row._SOURCE_LANE})`);
      });
    } else {
      console.log('No recent events in RAW_EVENTS');
    }
    
    console.log('\n📊 Events in ACTIVITY.EVENTS (dynamic table):');
    const activityEvents = await client.executeSql(`
      SELECT 
        EVENT_ID,
        ACTION,
        ACTOR_ID,
        OCCURRED_AT,
        SOURCE
      FROM CLAUDE_BI.ACTIVITY.EVENTS
      WHERE OCCURRED_AT >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
      ORDER BY OCCURRED_AT DESC
      LIMIT 5
    `);
    
    if (activityEvents.success && activityEvents.data?.length > 0) {
      console.log(`Found ${activityEvents.data.length} events in ACTIVITY.EVENTS:`);
      activityEvents.data.forEach((row, i) => {
        console.log(`${i + 1}. ${row.EVENT_ID} - ${row.ACTION} (${row.SOURCE})`);
      });
    } else {
      console.log('No recent events in ACTIVITY.EVENTS (may need 1 minute to refresh)');
    }
    
    console.log('\n🎉 SUMMARY:');
    if (rawEvents.data?.length > 0) {
      console.log('✅ Logging to RAW_EVENTS is WORKING!');
      console.log('✅ Claude Code can log events using RSA key authentication');
      console.log('✅ No passwords needed - pure key-based auth');
      
      if (activityEvents.data?.length === 0) {
        console.log('⏳ Dynamic table ACTIVITY.EVENTS will refresh in ~1 minute');
      } else {
        console.log('✅ Dynamic table is also populated');
      }
    } else {
      console.log('❌ No events found - logging may not be working');
    }
    
  } catch (error) {
    console.error('Error:', error);
  } finally {
    await client.disconnect();
  }
}

verifyLogging().catch(console.error);