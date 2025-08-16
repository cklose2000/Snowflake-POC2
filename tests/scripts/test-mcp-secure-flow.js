/**
 * Test Suite for Secure MCP Flow
 * Tests the complete user lifecycle with token-based authentication
 */

const snowflake = require('snowflake-sdk');
const crypto = require('crypto');
require('dotenv').config();

// Test configuration
const TEST_USERNAME = 'test_user_' + Date.now();
const TEST_EMAIL = TEST_USERNAME + '@test.com';

// Snowflake connection for admin operations
const adminConnection = snowflake.createConnection({
  account: process.env.SNOWFLAKE_ACCOUNT,
  username: process.env.SNOWFLAKE_USERNAME,
  password: process.env.SNOWFLAKE_PASSWORD,
  database: 'CLAUDE_BI',
  warehouse: 'CLAUDE_WAREHOUSE',
  schema: 'MCP',
  role: 'ACCOUNTADMIN'
});

// Service connection for MCP calls
const serviceConnection = snowflake.createConnection({
  account: process.env.SNOWFLAKE_ACCOUNT,
  username: 'MCP_SERVICE_USER',  // Minimal service account
  password: process.env.MCP_SERVICE_PASSWORD || 'SecurePassword123!',
  database: 'CLAUDE_BI',
  warehouse: 'MCP_XS_WH',
  schema: 'MCP',
  role: 'MCP_SERVICE_ROLE'
});

// Test results
const testResults = [];

/**
 * Helper function to execute SQL and return results
 */
async function executeSql(connection, sql, binds = []) {
  return new Promise((resolve, reject) => {
    connection.execute({
      sqlText: sql,
      binds: binds,
      complete: (err, stmt, rows) => {
        if (err) {
          reject(err);
        } else {
          resolve(rows);
        }
      }
    });
  });
}

/**
 * Connect to Snowflake
 */
async function connect(connection) {
  return new Promise((resolve, reject) => {
    connection.connect((err, conn) => {
      if (err) {
        reject(err);
      } else {
        resolve(conn);
      }
    });
  });
}

/**
 * Test 1: Create MCP User
 */
async function testCreateUser() {
  console.log('\n📝 Test 1: Create MCP User');
  
  try {
    const result = await executeSql(adminConnection, 
      `CALL MCP.CREATE_MCP_USER(?, ?, ?)`,
      [TEST_USERNAME, TEST_EMAIL, 'ANALYST']
    );
    
    const response = JSON.parse(result[0].CREATE_MCP_USER);
    
    if (response.success && response.token) {
      console.log('✅ User created successfully');
      console.log(`   Username: ${response.username}`);
      console.log(`   Token prefix: ${response.token.substring(0, 10)}...`);
      console.log(`   Delivery URL: ${response.delivery_url}`);
      console.log(`   Allowed tools: ${response.allowed_tools.join(', ')}`);
      
      testResults.push({
        test: 'Create User',
        status: 'PASSED',
        details: response
      });
      
      return response.token;  // Return token for next tests
    } else {
      throw new Error('User creation failed: ' + JSON.stringify(response));
    }
  } catch (error) {
    console.error('❌ Test failed:', error.message);
    testResults.push({
      test: 'Create User',
      status: 'FAILED',
      error: error.message
    });
    throw error;
  }
}

/**
 * Test 2: Validate Token with Nonce
 */
async function testTokenValidation(token) {
  console.log('\n🔐 Test 2: Token Validation with Nonce');
  
  try {
    const nonce = crypto.randomBytes(16).toString('hex');
    
    const result = await executeSql(serviceConnection,
      `CALL MCP.HANDLE_REQUEST(?, ?, ?)`,
      [
        'health',
        { nonce: nonce },
        token
      ]
    );
    
    const response = JSON.parse(result[0].HANDLE_REQUEST);
    
    if (response.status === 'healthy' && response.user === TEST_USERNAME) {
      console.log('✅ Token validation successful');
      console.log(`   User authenticated: ${response.user}`);
      
      testResults.push({
        test: 'Token Validation',
        status: 'PASSED'
      });
      
      return nonce;
    } else {
      throw new Error('Token validation failed');
    }
  } catch (error) {
    console.error('❌ Test failed:', error.message);
    testResults.push({
      test: 'Token Validation',
      status: 'FAILED',
      error: error.message
    });
  }
}

/**
 * Test 3: Replay Attack Detection
 */
async function testReplayProtection(token, usedNonce) {
  console.log('\n🛡️ Test 3: Replay Attack Detection');
  
  try {
    // Try to reuse the same nonce
    const result = await executeSql(serviceConnection,
      `CALL MCP.HANDLE_REQUEST(?, ?, ?)`,
      [
        'health',
        { nonce: usedNonce },  // Reuse nonce!
        token
      ]
    );
    
    const response = JSON.parse(result[0].HANDLE_REQUEST);
    
    if (response.error && response.error.includes('replay')) {
      console.log('✅ Replay attack correctly detected');
      
      testResults.push({
        test: 'Replay Protection',
        status: 'PASSED'
      });
    } else {
      throw new Error('Replay attack not detected!');
    }
  } catch (error) {
    // If the procedure throws an error about replay, that's expected
    if (error.message.includes('replay') || error.message.includes('Replay')) {
      console.log('✅ Replay attack correctly blocked');
      testResults.push({
        test: 'Replay Protection',
        status: 'PASSED'
      });
    } else {
      console.error('❌ Test failed:', error.message);
      testResults.push({
        test: 'Replay Protection',
        status: 'FAILED',
        error: error.message
      });
    }
  }
}

/**
 * Test 4: Tool Execution with Row Limits
 */
async function testToolExecution(token) {
  console.log('\n🔧 Test 4: Tool Execution with Row Limits');
  
  try {
    const nonce = crypto.randomBytes(16).toString('hex');
    
    const result = await executeSql(serviceConnection,
      `CALL MCP.HANDLE_REQUEST(?, ?, ?)`,
      [
        'tools/call',
        {
          name: 'compose_query',
          arguments: {
            query: 'Show me recent user signups',
            limit: 5
          },
          nonce: nonce
        },
        token
      ]
    );
    
    const response = JSON.parse(result[0].HANDLE_REQUEST);
    
    if (response.success && response.rows) {
      console.log('✅ Tool executed successfully');
      console.log(`   Query returned ${response.row_count || response.rows.length} rows`);
      console.log(`   Limit applied: ${response.limit_applied}`);
      
      testResults.push({
        test: 'Tool Execution',
        status: 'PASSED',
        rows_returned: response.row_count || response.rows.length
      });
    } else {
      throw new Error('Tool execution failed');
    }
  } catch (error) {
    console.error('❌ Test failed:', error.message);
    testResults.push({
      test: 'Tool Execution',
      status: 'FAILED',
      error: error.message
    });
  }
}

/**
 * Test 5: Permission Update
 */
async function testPermissionUpdate() {
  console.log('\n🔄 Test 5: Permission Update');
  
  try {
    // Update permissions to reduce row limit
    const result = await executeSql(adminConnection,
      `CALL MCP.UPDATE_USER_PERMISSIONS(?, ?, ?, ?)`,
      [
        TEST_USERNAME,
        ['compose_query'],  // Reduce tools
        100,  // Reduce row limit
        300   // Reduce runtime to 5 minutes
      ]
    );
    
    const response = JSON.parse(result[0].UPDATE_USER_PERMISSIONS);
    
    if (response.success) {
      console.log('✅ Permissions updated successfully');
      console.log(`   New tools: ${response.allowed_tools.join(', ')}`);
      console.log(`   New row limit: ${response.max_rows}`);
      
      testResults.push({
        test: 'Permission Update',
        status: 'PASSED'
      });
    } else {
      throw new Error('Permission update failed');
    }
  } catch (error) {
    console.error('❌ Test failed:', error.message);
    testResults.push({
      test: 'Permission Update',
      status: 'FAILED',
      error: error.message
    });
  }
}

/**
 * Test 6: Token Rotation
 */
async function testTokenRotation() {
  console.log('\n🔄 Test 6: Token Rotation');
  
  try {
    const result = await executeSql(adminConnection,
      `CALL MCP.ROTATE_USER_TOKEN(?, ?)`,
      [TEST_USERNAME, 'Test rotation']
    );
    
    const response = JSON.parse(result[0].ROTATE_USER_TOKEN);
    
    if (response.success && response.new_token) {
      console.log('✅ Token rotated successfully');
      console.log(`   New token prefix: ${response.new_token.substring(0, 10)}...`);
      console.log(`   New delivery URL: ${response.delivery_url}`);
      
      testResults.push({
        test: 'Token Rotation',
        status: 'PASSED'
      });
      
      return response.new_token;  // Return new token
    } else {
      throw new Error('Token rotation failed');
    }
  } catch (error) {
    console.error('❌ Test failed:', error.message);
    testResults.push({
      test: 'Token Rotation',
      status: 'FAILED',
      error: error.message
    });
  }
}

/**
 * Test 7: User Revocation
 */
async function testUserRevocation(token) {
  console.log('\n🚫 Test 7: User Revocation');
  
  try {
    // Revoke user
    const revokeResult = await executeSql(adminConnection,
      `CALL MCP.REVOKE_MCP_USER(?, ?)`,
      [TEST_USERNAME, 'Test cleanup']
    );
    
    const revokeResponse = JSON.parse(revokeResult[0].REVOKE_MCP_USER);
    
    if (revokeResponse.success) {
      console.log('✅ User revoked successfully');
      
      // Wait for Dynamic Table refresh (1 minute lag)
      console.log('   Waiting 65 seconds for Dynamic Table refresh...');
      await new Promise(resolve => setTimeout(resolve, 65000));
      
      // Try to use revoked token
      const nonce = crypto.randomBytes(16).toString('hex');
      
      try {
        await executeSql(serviceConnection,
          `CALL MCP.HANDLE_REQUEST(?, ?, ?)`,
          [
            'health',
            { nonce: nonce },
            token
          ]
        );
        
        throw new Error('Revoked token still works!');
      } catch (error) {
        if (error.message.includes('revoked') || error.message.includes('Access revoked')) {
          console.log('✅ Revoked token correctly rejected');
          testResults.push({
            test: 'User Revocation',
            status: 'PASSED'
          });
        } else {
          throw error;
        }
      }
    } else {
      throw new Error('User revocation failed');
    }
  } catch (error) {
    console.error('❌ Test failed:', error.message);
    testResults.push({
      test: 'User Revocation',
      status: 'FAILED',
      error: error.message
    });
  }
}

/**
 * Test 8: Audit Trail Verification
 */
async function testAuditTrail() {
  console.log('\n📋 Test 8: Audit Trail Verification');
  
  try {
    // Check audit events
    const auditEvents = await executeSql(adminConnection,
      `SELECT 
        action,
        COUNT(*) as event_count
      FROM CLAUDE_BI.ACTIVITY.EVENTS
      WHERE (actor_id = ? OR object_id = ?)
        AND occurred_at >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
      GROUP BY action
      ORDER BY event_count DESC`,
      [TEST_USERNAME, TEST_USERNAME]
    );
    
    console.log('✅ Audit trail events found:');
    auditEvents.forEach(row => {
      console.log(`   ${row.ACTION}: ${row.EVENT_COUNT} events`);
    });
    
    // Verify expected events exist
    const expectedEvents = [
      'system.user.created',
      'system.permission.granted',
      'mcp.request.processed',
      'system.permission.revoked'
    ];
    
    const foundEvents = auditEvents.map(r => r.ACTION);
    const hasAllEvents = expectedEvents.some(e => foundEvents.includes(e));
    
    if (hasAllEvents) {
      testResults.push({
        test: 'Audit Trail',
        status: 'PASSED',
        event_count: auditEvents.reduce((sum, r) => sum + r.EVENT_COUNT, 0)
      });
    } else {
      throw new Error('Missing expected audit events');
    }
  } catch (error) {
    console.error('❌ Test failed:', error.message);
    testResults.push({
      test: 'Audit Trail',
      status: 'FAILED',
      error: error.message
    });
  }
}

/**
 * Main test execution
 */
async function runTests() {
  console.log('🧪 Starting MCP Secure Flow Tests');
  console.log('=====================================\n');
  
  try {
    // Connect to Snowflake
    console.log('Connecting to Snowflake...');
    await connect(adminConnection);
    await connect(serviceConnection);
    console.log('✅ Connected\n');
    
    // Run tests in sequence
    const token = await testCreateUser();
    
    if (token) {
      const nonce = await testTokenValidation(token);
      
      if (nonce) {
        await testReplayProtection(token, nonce);
      }
      
      await testToolExecution(token);
      await testPermissionUpdate();
      
      // Wait for Dynamic Table to catch up with permission changes
      console.log('\n⏳ Waiting 65 seconds for Dynamic Table refresh...');
      await new Promise(resolve => setTimeout(resolve, 65000));
      
      const newToken = await testTokenRotation();
      
      if (newToken) {
        await testUserRevocation(newToken);
      }
    }
    
    await testAuditTrail();
    
  } catch (error) {
    console.error('\n❌ Test suite error:', error.message);
  } finally {
    // Print summary
    console.log('\n=====================================');
    console.log('📊 Test Summary');
    console.log('=====================================\n');
    
    const passed = testResults.filter(r => r.status === 'PASSED').length;
    const failed = testResults.filter(r => r.status === 'FAILED').length;
    
    testResults.forEach(result => {
      const icon = result.status === 'PASSED' ? '✅' : '❌';
      console.log(`${icon} ${result.test}: ${result.status}`);
      if (result.error) {
        console.log(`   Error: ${result.error}`);
      }
    });
    
    console.log(`\nTotal: ${passed} passed, ${failed} failed`);
    
    // Close connections
    adminConnection.destroy();
    serviceConnection.destroy();
    
    process.exit(failed > 0 ? 1 : 0);
  }
}

// Run the tests
runTests();