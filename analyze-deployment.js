/**
 * Analyze what SQL has been executed vs what we need for logging
 */

const SnowflakeSimpleClient = require('./snowflake-mcp-client/dist/simple-client').default;

async function analyzeDeployment() {
  console.log('🔍 Analyzing Deployment Status\n');
  
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
    console.log('✅ Connected as CLAUDE_CODE_AI_AGENT with ACCOUNTADMIN\n');
    
    // 1. Check recent queries
    console.log('📜 Recent SQL Executions (Last Hour):\n');
    const recentQueries = await client.executeSql(`
      SELECT 
        query_text,
        query_type,
        user_name,
        role_name,
        start_time,
        execution_status
      FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
      WHERE start_time >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
        AND query_type IN ('CREATE', 'CREATE_PROCEDURE', 'ALTER', 'GRANT')
        AND execution_status = 'SUCCESS'
      ORDER BY start_time DESC
      LIMIT 20
    `);
    
    if (recentQueries.success && recentQueries.data) {
      console.log(`Found ${recentQueries.data.length} DDL queries\n`);
      
      const proceduresCreated = new Set();
      recentQueries.data.forEach(row => {
        if (row.QUERY_TEXT.includes('CREATE OR REPLACE PROCEDURE')) {
          const match = row.QUERY_TEXT.match(/PROCEDURE\s+([A-Z_\.]+)\s*\(/i);
          if (match) {
            proceduresCreated.add(match[1]);
          }
        }
      });
      
      console.log('Procedures created:', Array.from(proceduresCreated).join(', ') || 'None');
    }
    
    // 2. Check what procedures we NEED
    console.log('\n📋 Required Procedures for Logging:\n');
    const requiredProcedures = [
      'CLAUDE_BI.MCP.LOG_CLAUDE_EVENT',
      'CLAUDE_BI.MCP.LOG_CLAUDE_EVENTS_BATCH', 
      'CLAUDE_BI.MCP.ROTATE_AGENT_KEY',
      'CLAUDE_BI.MCP.GET_SESSION_METRICS'
    ];
    
    // 3. Check what actually exists
    console.log('🔎 Checking Existing Procedures:\n');
    const existingProcs = await client.executeSql(`
      SHOW PROCEDURES IN SCHEMA CLAUDE_BI.MCP
    `);
    
    const existingProcNames = new Set();
    if (existingProcs.success && existingProcs.data) {
      existingProcs.data.forEach(row => {
        existingProcNames.add(`CLAUDE_BI.MCP.${row.name}`);
      });
    }
    
    console.log('Existing procedures in MCP schema:');
    existingProcs.data?.forEach(row => {
      console.log(`  - ${row.name}`);
    });
    
    // 4. Find missing procedures
    console.log('\n❌ Missing Procedures:');
    const missing = [];
    requiredProcedures.forEach(proc => {
      const shortName = proc.split('.').pop();
      if (!existingProcNames.has(proc) && !existingProcNames.has(`CLAUDE_BI.MCP.${shortName}`)) {
        console.log(`  - ${proc}`);
        missing.push(proc);
      }
    });
    
    if (missing.length === 0) {
      console.log('  None - all required procedures exist!');
    }
    
    // 5. Check for the dynamic table
    console.log('\n📊 Checking Dynamic Table:\n');
    const dynamicTable = await client.executeSql(`
      SHOW DYNAMIC TABLES LIKE 'EVENTS' IN SCHEMA CLAUDE_BI.ACTIVITY
    `);
    
    if (dynamicTable.success && dynamicTable.data?.length > 0) {
      console.log('✅ ACTIVITY.EVENTS dynamic table exists');
    } else {
      console.log('❌ ACTIVITY.EVENTS dynamic table is missing');
    }
    
    // 6. Check resource monitors
    console.log('\n🔧 Checking Resource Monitors:\n');
    const monitors = await client.executeSql(`
      SHOW RESOURCE MONITORS
    `);
    
    if (monitors.success && monitors.data) {
      const relevantMonitors = monitors.data.filter(m => 
        m.name?.includes('CLAUDE') || m.name?.includes('AGENT')
      );
      if (relevantMonitors.length > 0) {
        console.log('Found monitors:', relevantMonitors.map(m => m.name).join(', '));
      } else {
        console.log('❌ No Claude-related resource monitors found');
      }
    }
    
    // 7. Check grants
    console.log('\n🔐 Checking Role Grants:\n');
    const grants = await client.executeSql(`
      SHOW GRANTS TO ROLE R_APP_WRITE
    `);
    
    if (grants.success && grants.data) {
      const procedureGrants = grants.data.filter(g => 
        g.granted_on === 'PROCEDURE' && g.name?.includes('LOG')
      );
      if (procedureGrants.length > 0) {
        console.log('✅ Logging procedure grants exist');
      } else {
        console.log('⚠️  No logging procedure grants found for R_APP_WRITE');
      }
    }
    
    // 8. Summary
    console.log('\n📝 DEPLOYMENT SUMMARY:\n');
    console.log('Required for logging to work:');
    console.log('1. LOG_CLAUDE_EVENT procedure - ' + 
      (existingProcNames.has('CLAUDE_BI.MCP.LOG_CLAUDE_EVENT') ? '✅' : '❌ MISSING'));
    console.log('2. LOG_CLAUDE_EVENTS_BATCH procedure - ' + 
      (existingProcNames.has('CLAUDE_BI.MCP.LOG_CLAUDE_EVENTS_BATCH') ? '✅' : '❌ MISSING'));
    console.log('3. Dynamic table ACTIVITY.EVENTS - ' + 
      (dynamicTable.data?.length > 0 ? '✅' : '❌ MISSING'));
    console.log('4. Grants to R_APP_WRITE - Need to verify');
    console.log('5. CLAUDE_CODE_AI_AGENT has R_APP_WRITE - Need to verify');
    
    // 9. Check if Claude Code agent has R_APP_WRITE
    console.log('\n🔑 Checking CLAUDE_CODE_AI_AGENT roles:\n');
    const userGrants = await client.executeSql(`
      SHOW GRANTS TO USER CLAUDE_CODE_AI_AGENT
    `);
    
    if (userGrants.success && userGrants.data) {
      const roles = userGrants.data
        .filter(g => g.granted_on === 'ROLE')
        .map(g => g.role);
      console.log('Roles:', roles.join(', '));
      
      if (roles.includes('R_APP_WRITE')) {
        console.log('✅ Has R_APP_WRITE role');
      } else {
        console.log('❌ Missing R_APP_WRITE role');
      }
    }
    
    console.log('\n🎯 NEXT STEPS:');
    if (missing.length > 0) {
      console.log('1. Deploy the missing procedures from 04_logging_procedures.sql');
      console.log('2. Verify grants are set correctly');
      console.log('3. Test logging again');
    } else {
      console.log('All procedures exist - check grants and test logging');
    }
    
  } catch (error) {
    console.error('❌ Error:', error);
  } finally {
    await client.disconnect();
  }
}

analyzeDeployment().catch(console.error);