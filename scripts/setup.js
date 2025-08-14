#!/usr/bin/env node

/**
 * Setup Script - One-time initialization
 */

const fs = require('fs');
const path = require('path');
const { execSync } = require('child_process');

async function setup() {
  console.log('🚀 Snowflake POC2 Setup\n');

  // 1. Check for .env file
  const envPath = path.join(__dirname, '../.env');
  const envExamplePath = path.join(__dirname, '../.env.example');
  
  if (!fs.existsSync(envPath)) {
    if (fs.existsSync(envExamplePath)) {
      fs.copyFileSync(envExamplePath, envPath);
      console.log('✅ Created .env file from template');
      console.log('⚠️  Please edit .env with your Snowflake credentials\n');
      process.exit(0);
    } else {
      // Create .env.example
      const envTemplate = `# Snowflake Configuration
SNOWFLAKE_ACCOUNT=your-account.snowflakecomputing.com
SNOWFLAKE_USERNAME=your-username
SNOWFLAKE_PASSWORD=your-password
SNOWFLAKE_DATABASE=CLAUDE_BI
SNOWFLAKE_SCHEMA=ANALYTICS
SNOWFLAKE_WAREHOUSE=CLAUDE_WAREHOUSE
SNOWFLAKE_ROLE=CLAUDE_BI_ROLE

# Server Configuration
PORT=3000
WS_PORT=8080

# Activity Configuration
ACTIVITY_CUSTOMER=default_user
`;
      fs.writeFileSync(envExamplePath, envTemplate);
      fs.copyFileSync(envExamplePath, envPath);
      console.log('✅ Created .env and .env.example files');
      console.log('⚠️  Please edit .env with your Snowflake credentials\n');
      process.exit(0);
    }
  }

  // 2. Load environment
  require('dotenv').config({ path: envPath });

  // 3. Check for required environment variables
  const required = [
    'SNOWFLAKE_ACCOUNT',
    'SNOWFLAKE_USERNAME',
    'SNOWFLAKE_PASSWORD',
    'SNOWFLAKE_WAREHOUSE'
  ];

  const missing = required.filter(key => !process.env[key]);
  if (missing.length > 0) {
    console.error('❌ Missing required environment variables:');
    missing.forEach(key => console.error(`   - ${key}`));
    console.error('\n⚠️  Please edit .env file with your credentials');
    process.exit(1);
  }

  // 4. Install dependencies
  console.log('📦 Installing dependencies...');
  try {
    execSync('npm install', { stdio: 'inherit' });
    console.log('✅ Dependencies installed\n');
  } catch (error) {
    console.error('❌ Failed to install dependencies');
    process.exit(1);
  }

  // 5. Create required directories
  const dirs = [
    'generated-dashboards',
    'logs',
    'tests/results'
  ];

  dirs.forEach(dir => {
    const dirPath = path.join(__dirname, '..', dir);
    if (!fs.existsSync(dirPath)) {
      fs.mkdirSync(dirPath, { recursive: true });
      console.log(`✅ Created directory: ${dir}`);
    }
  });

  // 6. Bootstrap Activity views
  console.log('\n📊 Bootstrapping Activity views...');
  console.log('Run this SQL in Snowflake to create Activity views:');
  console.log(`
-- Create Activity views
USE DATABASE CLAUDE_BI;
USE SCHEMA ACTIVITY_CCODE;

CREATE OR REPLACE VIEW VW_ACTIVITY_COUNTS_24H AS
SELECT 
  DATE_TRUNC('hour', ts) as hour,
  activity,
  COUNT(*) as event_count,
  COUNT(DISTINCT customer) as unique_customers
FROM CLAUDE_BI.ACTIVITY.EVENTS
WHERE ts >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
GROUP BY hour, activity;

CREATE OR REPLACE VIEW VW_ACTIVITY_SUMMARY AS
SELECT 
  COUNT(*) as total_events,
  COUNT(DISTINCT customer) as unique_customers,
  COUNT(DISTINCT activity) as unique_activities,
  MAX(ts) as last_event
FROM CLAUDE_BI.ACTIVITY.EVENTS
WHERE ts >= CURRENT_TIMESTAMP - INTERVAL '24 hours';

-- Add other views as needed
`);

  console.log('\n✅ Setup complete!');
  console.log('\n📋 Next steps:');
  console.log('1. Run the SQL above in Snowflake');
  console.log('2. Start the server: npm start');
  console.log('3. Open browser: http://localhost:3000');
}

setup().catch(console.error);