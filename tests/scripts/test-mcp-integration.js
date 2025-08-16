#!/usr/bin/env node

/**
 * Test MCP Server Integration
 * This simulates what the UI would do - calling MCP tools through the server
 */

const { spawn } = require('child_process');
const readline = require('readline');

// Colors for output
const colors = {
  reset: '\x1b[0m',
  bright: '\x1b[1m',
  green: '\x1b[32m',
  yellow: '\x1b[33m',
  blue: '\x1b[34m',
  red: '\x1b[31m',
  cyan: '\x1b[36m'
};

function log(message, color = 'reset') {
  console.log(`${colors[color]}${message}${colors.reset}`);
}

// Start MCP server as subprocess
const mcpServer = spawn('node', ['mcp-server/dist/index.js'], {
  stdio: ['pipe', 'pipe', 'pipe'],
  cwd: process.cwd()
});

// Create readline interface for communication
const rl = readline.createInterface({
  input: mcpServer.stdout,
  crlfDelay: Infinity
});

let requestId = 1;
const pendingRequests = new Map();

// Handle responses from MCP server
rl.on('line', (line) => {
  try {
    const response = JSON.parse(line);
    
    // Skip log messages
    if (response.level) return;
    
    if (response.id && pendingRequests.has(response.id)) {
      const { resolve, testName } = pendingRequests.get(response.id);
      pendingRequests.delete(response.id);
      log(`✅ ${testName} completed`, 'green');
      resolve(response);
    }
  } catch (error) {
    // Not JSON, probably a log message
  }
});

// Handle errors
mcpServer.stderr.on('data', (data) => {
  if (!data.toString().includes('MCP Server started')) {
    console.error(`MCP Error: ${data}`);
  }
});

// Send request to MCP server
function sendRequest(method, params = {}) {
  return new Promise((resolve) => {
    const id = requestId++;
    const request = {
      jsonrpc: '2.0',
      method,
      params,
      id
    };
    
    pendingRequests.set(id, { resolve, testName: method });
    mcpServer.stdin.write(JSON.stringify(request) + '\n');
  });
}

// Test functions
async function testListTools() {
  log('\n📋 TEST 1: List Available Tools', 'bright');
  const response = await sendRequest('tools/list');
  
  if (response.result?.tools) {
    log(`Found ${response.result.tools.length} tools:`, 'cyan');
    response.result.tools.forEach(tool => {
      console.log(`  - ${tool.name}: ${tool.description}`);
    });
  }
  return response;
}

async function testListSources() {
  log('\n📊 TEST 2: List Data Sources', 'bright');
  const response = await sendRequest('tools/call', {
    name: 'list_sources',
    arguments: { include_columns: true }
  });
  
  if (response.result?.content?.[0]) {
    const result = JSON.parse(response.result.content[0].text);
    if (result.success) {
      log(`Found ${result.metadata.total_sources} sources:`, 'cyan');
      console.log(`  - Views: ${result.metadata.views}`);
      console.log(`  - Tables: ${result.metadata.tables}`);
      
      // Show first few sources
      result.sources.slice(0, 3).forEach(source => {
        console.log(`\n  ${source.name} (${source.type}):`);
        console.log(`    Schema: ${source.schema}`);
        console.log(`    Columns: ${source.columns.slice(0, 5).join(', ')}...`);
      });
    }
  }
  return response;
}

async function testSimpleQuery() {
  log('\n🔍 TEST 3: Simple Query - Activity Summary', 'bright');
  const response = await sendRequest('tools/call', {
    name: 'compose_query_plan',
    arguments: {
      intent_text: 'Show me the activity summary',
      source: 'VW_ACTIVITY_SUMMARY'
    }
  });
  
  if (response.result?.content?.[0]) {
    const result = JSON.parse(response.result.content[0].text);
    if (result.success) {
      log('Query executed successfully!', 'cyan');
      console.log(`  SQL: ${result.sql?.substring(0, 100)}...`);
      console.log(`  Rows returned: ${result.metadata?.row_count || 0}`);
      console.log(`  Execution time: ${result.metadata?.execution_time_ms}ms`);
      
      if (result.results?.[0]) {
        console.log('\n  Sample result:');
        console.log('  ', JSON.stringify(result.results[0], null, 2).substring(0, 200));
      }
    } else {
      log(`Query failed: ${result.error}`, 'red');
    }
  }
  return response;
}

async function testComplexQuery() {
  log('\n📈 TEST 4: Complex Query - Top Activities', 'bright');
  const response = await sendRequest('tools/call', {
    name: 'compose_query_plan',
    arguments: {
      intent_text: 'Top 5 activities by event count',
      source: 'VW_ACTIVITY_COUNTS_24H',
      dimensions: ['ACTIVITY'],
      measures: [{ fn: 'SUM', column: 'EVENT_COUNT' }],
      top_n: 5,
      order_by: [{ column: 'SUM_EVENT_COUNT', direction: 'DESC' }]
    }
  });
  
  if (response.result?.content?.[0]) {
    const result = JSON.parse(response.result.content[0].text);
    if (result.success) {
      log('Complex query executed successfully!', 'cyan');
      console.log(`  Validated: ${result.plan?.validated}`);
      console.log(`  Rows returned: ${result.metadata?.row_count || 0}`);
      
      if (result.results) {
        console.log('\n  Top activities:');
        result.results.forEach((row, i) => {
          console.log(`    ${i + 1}. ${row.ACTIVITY}: ${row.SUM_EVENT_COUNT || 0} events`);
        });
      }
    } else {
      log(`Query failed: ${result.error || JSON.stringify(result.errors)}`, 'red');
    }
  }
  return response;
}

async function testValidatePlan() {
  log('\n✅ TEST 5: Validate Query Plan', 'bright');
  
  // Test invalid plan
  const invalidResponse = await sendRequest('tools/call', {
    name: 'validate_plan',
    arguments: {
      plan: {
        source: 'INVALID_TABLE',
        dimensions: ['FAKE_COLUMN'],
        measures: [{ fn: 'INVALID_FN', column: 'FAKE' }]
      }
    }
  });
  
  if (invalidResponse.result?.content?.[0]) {
    const result = JSON.parse(invalidResponse.result.content[0].text);
    log(`Invalid plan validation: ${!result.valid ? 'Correctly rejected' : 'ERROR - should have failed'}`, 
        !result.valid ? 'cyan' : 'red');
    if (result.errors) {
      console.log('  Errors detected:');
      result.errors.forEach(err => console.log(`    - ${err}`));
    }
  }
  
  // Test valid plan
  const validResponse = await sendRequest('tools/call', {
    name: 'validate_plan',
    arguments: {
      plan: {
        source: 'VW_ACTIVITY_COUNTS_24H',
        dimensions: ['ACTIVITY'],
        measures: [{ fn: 'SUM', column: 'EVENT_COUNT' }],
        top_n: 10
      },
      dry_run: true
    }
  });
  
  if (validResponse.result?.content?.[0]) {
    const result = JSON.parse(validResponse.result.content[0].text);
    log(`Valid plan validation: ${result.valid ? 'Passed' : 'Failed'}`, 
        result.valid ? 'cyan' : 'red');
    if (result.sql) {
      console.log(`  Generated SQL: ${result.sql.substring(0, 100)}...`);
    }
  }
  
  return { invalidResponse, validResponse };
}

async function testDashboardCreation() {
  log('\n📊 TEST 6: Dashboard Creation', 'bright');
  const response = await sendRequest('tools/call', {
    name: 'create_dashboard',
    arguments: {
      title: 'Test Dashboard',
      description: 'MCP integration test dashboard',
      queries: [
        {
          name: 'Activity Summary',
          plan: {
            source: 'VW_ACTIVITY_SUMMARY'
          },
          chart_type: 'metric'
        },
        {
          name: 'Hourly Trend',
          plan: {
            source: 'VW_ACTIVITY_COUNTS_24H',
            dimensions: ['HOUR'],
            measures: [{ fn: 'SUM', column: 'EVENT_COUNT' }],
            order_by: [{ column: 'HOUR', direction: 'ASC' }]
          },
          chart_type: 'line'
        }
      ],
      refresh_method: 'manual'
    }
  });
  
  if (response.result?.content?.[0]) {
    const result = JSON.parse(response.result.content[0].text);
    if (result.success) {
      log('Dashboard created successfully!', 'cyan');
      console.log(`  Dashboard ID: ${result.dashboard_id}`);
      console.log(`  Dashboard URL: ${result.dashboard_url}`);
      console.log(`  Views created: ${result.artifacts_created?.views?.join(', ')}`);
      console.log(`  Streamlit code length: ${result.streamlit_code?.length} chars`);
    } else {
      log(`Dashboard creation failed: ${result.error}`, 'red');
    }
  }
  return response;
}

async function testSecurity() {
  log('\n🔒 TEST 7: Security Validation', 'bright');
  
  // Test SQL injection attempt
  const injectionTest = await sendRequest('tools/call', {
    name: 'compose_query_plan',
    arguments: {
      intent_text: "'; DROP TABLE users; --",
      source: 'VW_ACTIVITY_SUMMARY'
    }
  });
  
  log('SQL Injection test: Should execute safely', 'yellow');
  
  // Test row limit enforcement
  const rowLimitTest = await sendRequest('tools/call', {
    name: 'validate_plan',
    arguments: {
      plan: {
        source: 'EVENTS',
        top_n: 50000  // Exceeds limit
      }
    }
  });
  
  if (rowLimitTest.result?.content?.[0]) {
    const result = JSON.parse(rowLimitTest.result.content[0].text);
    log(`Row limit test: ${!result.valid ? 'Correctly rejected' : 'ERROR - should have failed'}`, 
        !result.valid ? 'cyan' : 'red');
    if (result.errors) {
      console.log(`  Error: ${result.errors[0]}`);
    }
  }
  
  return { injectionTest, rowLimitTest };
}

async function testNaturalLanguage() {
  log('\n💬 TEST 8: Natural Language Queries', 'bright');
  
  const queries = [
    'Show me activities from the last 24 hours',
    'What are the top customers?',
    'Give me a summary of all activity'
  ];
  
  for (const query of queries) {
    log(`\n  Query: "${query}"`, 'yellow');
    
    const response = await sendRequest('tools/call', {
      name: 'compose_query_plan',
      arguments: {
        intent_text: query
      }
    });
    
    if (response.result?.content?.[0]) {
      const result = JSON.parse(response.result.content[0].text);
      if (result.success) {
        console.log(`    ✓ Source: ${result.plan?.source}`);
        console.log(`    ✓ Rows: ${result.metadata?.row_count || 0}`);
      } else if (result.needs_clarification) {
        console.log(`    ℹ️  Needs clarification: ${result.message}`);
        console.log(`    Available sources: ${result.available_sources?.slice(0, 3).join(', ')}...`);
      }
    }
  }
}

// Run all tests
async function runTests() {
  log('\n🚀 Starting MCP Server Integration Tests', 'bright');
  log('=' .repeat(50), 'blue');
  
  // Wait for server to start
  await new Promise(resolve => setTimeout(resolve, 2000));
  
  try {
    await testListTools();
    await testListSources();
    await testSimpleQuery();
    await testComplexQuery();
    await testValidatePlan();
    await testDashboardCreation();
    await testSecurity();
    await testNaturalLanguage();
    
    log('\n' + '=' .repeat(50), 'blue');
    log('✅ All tests completed!', 'green');
    
  } catch (error) {
    log(`\n❌ Test failed: ${error.message}`, 'red');
    console.error(error);
  } finally {
    // Cleanup
    setTimeout(() => {
      mcpServer.kill();
      process.exit(0);
    }, 1000);
  }
}

// Handle errors
mcpServer.on('error', (error) => {
  log(`Failed to start MCP server: ${error.message}`, 'red');
  process.exit(1);
});

// Run tests
runTests();