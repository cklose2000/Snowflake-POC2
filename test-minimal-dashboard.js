#!/usr/bin/env node

// Test minimal dashboard creation

const snowflake = require('snowflake-sdk');
const DashboardFactory = require('./packages/dashboard-factory');
require('dotenv').config();

async function testDashboard() {
  console.log('🧪 Testing minimal dashboard creation\n');
  
  // Create Snowflake connection
  const connection = snowflake.createConnection({
    account: process.env.SNOWFLAKE_ACCOUNT,
    username: process.env.SNOWFLAKE_USERNAME,
    password: process.env.SNOWFLAKE_PASSWORD,
    role: process.env.SNOWFLAKE_ROLE,
    warehouse: process.env.SNOWFLAKE_WAREHOUSE,
    database: process.env.SNOWFLAKE_DATABASE,
    schema: process.env.SNOWFLAKE_SCHEMA
  });
  
  await new Promise((resolve, reject) => {
    connection.connect((err, conn) => {
      if (err) reject(err);
      else resolve(conn);
    });
  });
  
  console.log('✅ Connected to Snowflake\n');
  
  // Create dashboard factory
  const factory = new DashboardFactory(connection);
  
  // Minimal conversation history
  const conversationHistory = [
    {
      type: 'user',
      content: 'Show activity dashboard'
    }
  ];
  
  try {
    console.log('🏭 Creating dashboard...\n');
    const result = await factory.createDashboard(
      conversationHistory,
      'test_user',
      'test_session_' + Date.now()
    );
    
    console.log('Result:', JSON.stringify(result, null, 2));
    
  } catch (error) {
    console.error('❌ Error:', error.message);
    if (error.stack) {
      console.error('Stack:', error.stack);
    }
  }
  
  connection.destroy();
}

testDashboard().catch(console.error);