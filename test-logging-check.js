/**
 * Check if logging is working by:
 * 1. Logging a test event
 * 2. Querying recent events to verify
 */

const SnowflakeSimpleClient = require('./snowflake-mcp-client/dist/simple-client').default;
const crypto = require('crypto');

async function testLogging() {
  console.log('🔍 Testing Claude Code Logging\n');
  
  const config = {
    account: 'uec18397.us-east-1',
    username: 'CLAUDE_CODE_AI_AGENT',
    privateKeyPath: './claude_code_rsa_key.p8',
    warehouse: 'CLAUDE_AGENT_WH',
    database: 'CLAUDE_BI',
    schema: 'MCP'
  };
  
  const client = new SnowflakeSimpleClient(config);
  const testId = crypto.randomBytes(4).toString('hex');
  
  try {
    await client.connect();
    console.log('✅ Connected with RSA key\n');
    
    // Check current role and session info
    console.log('🔍 Checking session info...');
    const sessionInfo = await client.executeSql(`
      SELECT 
        CURRENT_USER() AS user,
        CURRENT_ROLE() AS role,
        IS_ROLE_IN_SESSION('R_APP_WRITE') AS has_write_role,
        IS_ROLE_IN_SESSION('ACCOUNTADMIN') AS has_admin
    `);
    
    if (sessionInfo.success && sessionInfo.data?.[0]) {
      const info = sessionInfo.data[0];
      console.log(`User: ${info.USER}`);
      console.log(`Current Role: ${info.ROLE}`);
      console.log(`Has R_APP_WRITE: ${info.HAS_WRITE_ROLE}`);
      console.log(`Has ACCOUNTADMIN: ${info.HAS_ADMIN}`);
      console.log('');
    }
    
    // Step 1: Log a test event using the NEW LOG_CLAUDE_EVENT procedure  
    console.log('📝 Logging test event using LOG_CLAUDE_EVENT...');
    const logResult = await client.callProcedure(
      'CLAUDE_BI.MCP.LOG_CLAUDE_EVENT',
      {
        event_id: `test_${testId}`,
        action: 'ccode.test.verification',
        actor_id: 'claude_code',
        session_id: `session_${testId}`,
        occurred_at: new Date().toISOString(),
        attributes: {
          test_id: testId,
          purpose: 'verify_logging',
          timestamp: Date.now()
        }
      },
      'CLAUDE_CODE'
    );
    
    console.log('Full result:', JSON.stringify(logResult, null, 2));
    if (logResult.data) {
      console.log('Procedure returned:', logResult.data);
      if (logResult.data.ok === true) {
        console.log('✅ Event logged successfully!');
        console.log('Event ID:', logResult.data.event_id);
      } else {
        console.log('❌ Logging failed:', logResult.data.error);
      }
    } else {
      console.log('⚠️  No data returned from procedure');
    }
    console.log('');
    
    // Step 2: Query recent events to see if our test event is there
    console.log('📊 Querying last 10 events from ACTIVITY.EVENTS...\n');
    const queryResult = await client.executeSql(`
      SELECT 
        occurred_at,
        action,
        actor_id,
        attributes:session_id::STRING AS session_id,
        attributes:test_id::STRING AS test_id,
        source
      FROM CLAUDE_BI.ACTIVITY.EVENTS
      WHERE occurred_at >= DATEADD('minute', -5, CURRENT_TIMESTAMP())
      ORDER BY occurred_at DESC
      LIMIT 10
    `);
    
    if (queryResult.success && queryResult.data) {
      console.log(`Found ${queryResult.data.length} recent events:\n`);
      
      let foundOurEvent = false;
      queryResult.data.forEach((row, i) => {
        const isOurEvent = row.TEST_ID === testId;
        if (isOurEvent) foundOurEvent = true;
        
        console.log(`${i + 1}. ${row.OCCURRED_AT}`);
        console.log(`   Action: ${row.ACTION}`);
        console.log(`   Actor: ${row.ACTOR_ID}`);
        console.log(`   Session: ${row.SESSION_ID || 'N/A'}`);
        if (isOurEvent) {
          console.log(`   ✅ THIS IS OUR TEST EVENT!`);
        }
        console.log('');
      });
      
      if (foundOurEvent) {
        console.log('🎉 SUCCESS: Logging is working! Our test event was stored and retrieved.');
      } else if (queryResult.data.length === 0) {
        console.log('⚠️  No recent events found. The dynamic table might need time to refresh (1 minute lag).');
      } else {
        console.log('⚠️  Our test event not found yet. The dynamic table has a 1-minute lag.');
      }
    }
    
    // Step 3: Check the raw table directly
    console.log('\n📊 Checking RAW_EVENTS table directly...\n');
    const rawResult = await client.executeSql(`
      SELECT 
        ingested_at,
        payload:event_id::STRING AS event_id,
        payload:action::STRING AS action,
        payload:attributes:test_id::STRING AS test_id,
        source_lane
      FROM CLAUDE_BI.LANDING.RAW_EVENTS  
      WHERE payload:attributes:test_id = ?
      ORDER BY ingested_at DESC
      LIMIT 1
    `, [testId]);
    
    if (rawResult.success && rawResult.data?.length > 0) {
      console.log('✅ Found in RAW_EVENTS:');
      console.log(`   Event ID: ${rawResult.data[0].EVENT_ID}`);
      console.log(`   Action: ${rawResult.data[0].ACTION}`);
      console.log(`   Ingested: ${rawResult.data[0].INGESTED_AT}`);
      console.log('\n✅ CONFIRMED: Logging is working! Event is in raw table.');
    } else {
      console.log('❌ Event not found in RAW_EVENTS - logging may have failed.');
    }
    
  } catch (error) {
    console.error('❌ Error:', error);
  } finally {
    await client.disconnect();
    console.log('\n🔌 Disconnected');
  }
}

testLogging().catch(console.error);