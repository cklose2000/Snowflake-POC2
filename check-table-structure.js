/**
 * Check the actual structure of RAW_EVENTS table
 */

const SnowflakeSimpleClient = require('./snowflake-mcp-client/dist/simple-client').default;

async function checkStructure() {
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
    
    const result = await client.executeSql(`
      DESCRIBE TABLE CLAUDE_BI.LANDING.RAW_EVENTS
    `);
    
    console.log('RAW_EVENTS table structure:');
    console.log('----------------------------');
    if (result.success && result.data) {
      result.data.forEach(col => {
        console.log(`${col.name} - ${col.type}`);
      });
    }
    
  } catch (error) {
    console.error('Error:', error);
  } finally {
    await client.disconnect();
  }
}

checkStructure().catch(console.error);