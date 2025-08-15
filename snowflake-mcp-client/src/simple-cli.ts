#!/usr/bin/env node

/**
 * Simple CLI for testing native Snowflake auth
 * No tokens, direct connection to Snowflake
 */

import { Command } from 'commander';
import { SnowflakeSimpleClient } from './simple-client';
import * as fs from 'fs';
import * as path from 'path';

const program = new Command();

program
  .name('snowflake-simple')
  .description('Simple Snowflake client with native auth')
  .version('2.0.0');

/**
 * Status command - check connection and permissions
 */
program
  .command('status')
  .description('Check connection status and user permissions')
  .action(async () => {
    try {
      const client = new SnowflakeSimpleClient();
      
      console.log('🔍 Checking Snowflake connection...\n');
      
      // Get user status
      const status = await client.getUserStatus();
      
      if (status.success) {
        console.log('✅ Connection successful!\n');
        console.log('User Information:');
        console.log(`  Username: ${status.data.username}`);
        console.log(`  Role: ${status.data.role}`);
        console.log(`  Warehouse: ${status.data.warehouse}`);
        console.log(`  Database: ${status.data.database}`);
        console.log(`  Schema: ${status.data.schema}`);
        
        console.log('\nPermissions:');
        console.log(`  Can Read: ${status.data.permissions.can_read ? '✅' : '❌'}`);
        console.log(`  Can Write: ${status.data.permissions.can_write ? '✅' : '❌'}`);
        console.log(`  Is Admin: ${status.data.permissions.is_admin ? '✅' : '❌'}`);
        
        console.log('\nSession Info:');
        console.log(`  Client IP: ${status.data.session_info.client_ip}`);
        console.log(`  Session ID: ${status.data.session_info.session_id}`);
      } else {
        console.error('❌ Connection failed:', status.error);
      }
      
      await client.disconnect();
    } catch (error: any) {
      console.error('❌ Error:', error.message);
      process.exit(1);
    }
  });

/**
 * List sources command
 */
program
  .command('sources')
  .description('List available data sources')
  .action(async () => {
    try {
      const client = new SnowflakeSimpleClient();
      
      console.log('📊 Fetching available data sources...\n');
      
      const result = await client.listSources();
      
      if (result.success) {
        console.log(`Found ${result.data.count} sources:\n`);
        
        result.data.sources.forEach((source: any) => {
          console.log(`  📁 ${source.name}`);
          console.log(`     Type: ${source.type}`);
          console.log(`     Schema: ${source.schema}`);
          if (source.description) {
            console.log(`     Description: ${source.description}`);
          }
          if (source.row_count_estimate) {
            console.log(`     Rows: ~${source.row_count_estimate.toLocaleString()}`);
          }
          console.log();
        });
      } else {
        console.error('❌ Failed to list sources:', result.error);
      }
      
      await client.disconnect();
    } catch (error: any) {
      console.error('❌ Error:', error.message);
      process.exit(1);
    }
  });

/**
 * Query command - natural language queries
 */
program
  .command('query <text>')
  .description('Execute a natural language query')
  .option('-l, --limit <number>', 'Maximum rows to return', '100')
  .action(async (text, options) => {
    try {
      const client = new SnowflakeSimpleClient();
      
      console.log(`🔍 Query: "${text}"\n`);
      
      const result = await client.query(text, parseInt(options.limit));
      
      if (result.success) {
        console.log('✅ Query executed successfully');
        console.log(`⏱️  Execution time: ${result.metadata?.executionTimeMs}ms\n`);
        
        if (result.data?.results) {
          console.log('Results:');
          console.log(JSON.stringify(result.data.results, null, 2));
        } else {
          console.log('Query plan:', JSON.stringify(result.data, null, 2));
        }
      } else {
        console.error('❌ Query failed:', result.error);
      }
      
      await client.disconnect();
    } catch (error: any) {
      console.error('❌ Error:', error.message);
      process.exit(1);
    }
  });

/**
 * Insert event command
 */
program
  .command('insert-event')
  .description('Insert a test event')
  .option('-a, --action <action>', 'Event action', 'test.cli.event')
  .option('-s, --source <source>', 'Source lane', 'CLI')
  .action(async (options) => {
    try {
      const client = new SnowflakeSimpleClient();
      
      const event = {
        event_id: `evt_${Date.now()}`,
        action: options.action,
        actor_id: process.env.SNOWFLAKE_USERNAME || 'cli_user',
        occurred_at: new Date().toISOString(),
        attributes: {
          source: 'simple-cli',
          test: true,
          timestamp: Date.now()
        }
      };
      
      console.log('📝 Inserting event:', JSON.stringify(event, null, 2), '\n');
      
      const result = await client.insertEvent(event, options.source);
      
      if (result.success) {
        console.log('✅ Event inserted successfully');
        console.log('Response:', JSON.stringify(result.data, null, 2));
      } else {
        console.error('❌ Insert failed:', result.error);
      }
      
      await client.disconnect();
    } catch (error: any) {
      console.error('❌ Error:', error.message);
      process.exit(1);
    }
  });

/**
 * SQL command - execute raw SQL
 */
program
  .command('sql <query>')
  .description('Execute raw SQL (for testing)')
  .action(async (query) => {
    try {
      const client = new SnowflakeSimpleClient();
      
      console.log(`🔧 Executing SQL: ${query}\n`);
      
      const result = await client.executeSql(query);
      
      if (result.success) {
        console.log('✅ Query executed successfully');
        console.log(`⏱️  Execution time: ${result.metadata?.executionTimeMs}ms`);
        console.log(`📊 Rows returned: ${result.metadata?.rowCount}\n`);
        
        console.log('Results:');
        console.log(JSON.stringify(result.data, null, 2));
      } else {
        console.error('❌ Query failed:', result.error);
      }
      
      await client.disconnect();
    } catch (error: any) {
      console.error('❌ Error:', error.message);
      process.exit(1);
    }
  });

/**
 * Test command - comprehensive connection test
 */
program
  .command('test')
  .description('Run comprehensive connection and permission tests')
  .action(async () => {
    try {
      const client = new SnowflakeSimpleClient();
      
      console.log('🧪 Running Snowflake Connection Tests\n');
      console.log('=====================================\n');
      
      // Test 1: Connection
      console.log('Test 1: Connection');
      await client.connect();
      console.log('  ✅ Connected successfully\n');
      
      // Test 2: User Status
      console.log('Test 2: User Status');
      const status = await client.getUserStatus();
      if (status.success) {
        console.log(`  ✅ User: ${status.data.username}`);
        console.log(`     Role: ${status.data.role}`);
        console.log(`     Can Read: ${status.data.permissions.can_read}`);
        console.log(`     Can Write: ${status.data.permissions.can_write}\n`);
      } else {
        console.log(`  ❌ Failed: ${status.error}\n`);
      }
      
      // Test 3: List Sources
      console.log('Test 3: List Sources');
      const sources = await client.listSources();
      if (sources.success) {
        console.log(`  ✅ Found ${sources.data.count} sources\n`);
      } else {
        console.log(`  ❌ Failed: ${sources.error}\n`);
      }
      
      // Test 4: Query Composition
      console.log('Test 4: Query Composition');
      const plan = await client.composeQueryPlan('show recent user signups', 10);
      if (plan.success) {
        console.log(`  ✅ Plan composed successfully\n`);
      } else {
        console.log(`  ❌ Failed: ${plan.error}\n`);
      }
      
      // Test 5: Event Insertion (if write permissions)
      if (status.data?.permissions?.can_write) {
        console.log('Test 5: Event Insertion');
        const event = {
          action: 'test.connection.verified',
          actor_id: status.data.username,
          attributes: { test: true }
        };
        const insert = await client.insertEvent(event, 'TEST');
        if (insert.success) {
          console.log(`  ✅ Event inserted: ${insert.data.event_id}\n`);
        } else {
          console.log(`  ❌ Failed: ${insert.error}\n`);
        }
      } else {
        console.log('Test 5: Event Insertion');
        console.log('  ⏭️  Skipped (no write permissions)\n');
      }
      
      console.log('=====================================');
      console.log('✅ All tests completed!\n');
      
      await client.disconnect();
    } catch (error: any) {
      console.error('❌ Test failed:', error.message);
      process.exit(1);
    }
  });

/**
 * Config command - show current configuration
 */
program
  .command('config')
  .description('Show current configuration')
  .action(() => {
    console.log('📋 Current Configuration\n');
    console.log('========================\n');
    
    console.log('Environment Variables:');
    console.log(`  SNOWFLAKE_ACCOUNT: ${process.env.SNOWFLAKE_ACCOUNT || '❌ Not set'}`);
    console.log(`  SNOWFLAKE_USERNAME: ${process.env.SNOWFLAKE_USERNAME || '❌ Not set'}`);
    console.log(`  SNOWFLAKE_PASSWORD: ${process.env.SNOWFLAKE_PASSWORD ? '✅ Set (hidden)' : '❌ Not set'}`);
    console.log(`  SF_PK_PATH: ${process.env.SF_PK_PATH || '❌ Not set'}`);
    console.log(`  SNOWFLAKE_ROLE: ${process.env.SNOWFLAKE_ROLE || '❌ Not set'}`);
    console.log(`  SNOWFLAKE_WAREHOUSE: ${process.env.SNOWFLAKE_WAREHOUSE || 'CLAUDE_WAREHOUSE (default)'}`);
    console.log(`  SNOWFLAKE_DATABASE: ${process.env.SNOWFLAKE_DATABASE || 'CLAUDE_BI (default)'}`);
    console.log(`  SNOWFLAKE_SCHEMA: ${process.env.SNOWFLAKE_SCHEMA || 'MCP (default)'}`);
    
    console.log('\nAuth Mode:');
    if (process.env.SF_PK_PATH) {
      console.log('  🔐 Key-pair authentication');
      if (fs.existsSync(process.env.SF_PK_PATH)) {
        console.log(`  ✅ Private key file exists: ${process.env.SF_PK_PATH}`);
      } else {
        console.log(`  ❌ Private key file not found: ${process.env.SF_PK_PATH}`);
      }
    } else if (process.env.SNOWFLAKE_PASSWORD) {
      console.log('  🔑 Password authentication');
    } else {
      console.log('  ❌ No authentication configured');
    }
  });

// Parse command line arguments
program.parse();

// Show help if no command provided
if (!process.argv.slice(2).length) {
  program.outputHelp();
}