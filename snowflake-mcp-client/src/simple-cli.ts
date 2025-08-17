#!/usr/bin/env node

/**
 * Simple CLI for testing native Snowflake auth
 * No tokens, direct connection to Snowflake
 */

import { Command } from 'commander';
import { SnowflakeSimpleClient, LogEvent } from './simple-client';
import * as fs from 'fs';
import * as path from 'path';

const program = new Command();

/**
 * Robust SQL statement splitter for Snowflake
 * Handles dollar quotes, procedures, comments, and statement markers
 */
function splitStatements(sql: string): string[] {
  const lines = sql.split(/\r?\n/);
  const out: string[] = [];
  let buf: string[] = [];
  let inDollar = false;
  let inBlockComment = false;
  let useMarkers = false;

  // Check if we have statement markers
  for (const line of lines) {
    if (line.trim() === '-- @statement') {
      useMarkers = true;
      break;
    }
  }

  const push = () => { 
    const s = buf.join('\n').trim(); 
    // Only push non-comment statements
    if (s && !s.startsWith('--') && !s.startsWith('/*')) {
      out.push(s); 
    }
    buf = []; 
  };

  for (let raw of lines) {
    const line = raw;

    // If using markers, split ONLY on markers
    if (useMarkers) {
      if (line.trim() === '-- @statement') { 
        if (buf.length) push(); 
        continue; 
      }
      buf.push(line);
      continue;
    }

    // Non-marker mode: traditional splitting
    // Skip pure comment lines when buffer is empty
    if (line.trim().startsWith('--') && buf.length === 0) { 
      continue; 
    }

    // Handle block comments
    if (inBlockComment) {
      buf.push(line);
      if (line.includes('*/')) inBlockComment = false;
      continue;
    }
    if (line.trim().startsWith('/*')) { 
      inBlockComment = !line.includes('*/'); 
      buf.push(line); 
      continue; 
    }

    // Track dollar quote state
    const dollarCount = (line.match(/\$\$/g) || []).length;
    if (dollarCount > 0) {
      inDollar = !inDollar;
    }

    buf.push(line);

    // End-of-statement detection
    const trimmed = line.trim();
    if (!inDollar) {
      // Regular semicolon terminator outside dollar quotes
      if (trimmed.endsWith(';')) {
        push();
      }
    } else if (dollarCount > 0 && trimmed.match(/\$\$\s*;?\s*$/)) {
      // Procedure/function body terminator: $$ with optional semicolon and whitespace
      push();
      inDollar = false;
    }
  }
  
  // Add any remaining statement
  if (buf.length) push();

  return out;
}

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
 * Execute file command - run SQL statements from a file
 */
program
  .command('exec-file <filepath>')
  .description('Execute SQL statements from a file')
  .action(async (filepath) => {
    try {
      const client = new SnowflakeSimpleClient();
      
      // Read the file
      if (!fs.existsSync(filepath)) {
        console.error(`❌ File not found: ${filepath}`);
        process.exit(1);
      }
      
      const sql = fs.readFileSync(filepath, 'utf8');
      
      // Use the robust statement splitter
      const statements = splitStatements(sql);
      
      console.log(`📄 Executing ${statements.length} statements from ${filepath}\n`);
      
      // Connect once
      await client.connect();
      
      // Execute each statement
      for (let i = 0; i < statements.length; i++) {
        const stmt = statements[i];
        if (stmt) {
          console.log(`▶️  Statement ${i+1}/${statements.length}...`);
          
          // Show first 80 chars of statement
          const preview = stmt.substring(0, 80).replace(/\n/g, ' ');
          console.log(`   ${preview}${stmt.length > 80 ? '...' : ''}`);
          
          const result = await client.executeSql(stmt);
          if (result.success) {
            console.log('   ✅ Success');
            if (result.metadata?.rowCount !== undefined) {
              console.log(`   📊 Rows affected: ${result.metadata.rowCount}`);
            }
          } else {
            console.error(`   ❌ Failed: ${result.error}`);
            await client.disconnect();
            process.exit(1);
          }
        }
      }
      
      // Log the file execution
      await client.logEvent({
        action: 'ccode.sql.file_executed',
        object: { type: 'sql_file', id: path.basename(filepath) },
        attributes: { 
          filepath,
          statement_count: statements.length
        }
      });
      
      console.log(`\n✅ All ${statements.length} statements executed successfully`);
      
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
 * Log command - log arbitrary events to ACTIVITY.EVENTS
 */
program
  .command('log')
  .description('Log an event to ACTIVITY.EVENTS')
  .option('--action <action>', 'Event action (e.g., code.edit, git.commit)')
  .option('--object <object>', 'Object reference (e.g., file:README.md, commit:abc123)')
  .option('--attrs <json>', 'Event attributes as JSON')
  .option('--dedupe <key>', 'Deduplication key to prevent duplicates')
  .action(async (options) => {
    try {
      const client = new SnowflakeSimpleClient();
      await client.connect();
      
      // Parse object if provided
      let objectData: { type: string; id: string } | undefined;
      if (options.object) {
        const [type, ...idParts] = options.object.split(':');
        objectData = { type, id: idParts.join(':') };
      }
      
      // Parse attributes if provided
      let attributes = {};
      if (options.attrs) {
        try {
          attributes = JSON.parse(options.attrs);
        } catch (e) {
          console.error('❌ Invalid JSON in --attrs');
          process.exit(1);
        }
      }
      
      // Add dedupe key if provided
      if (options.dedupe) {
        attributes = { ...attributes, dedupe_key: options.dedupe };
      }
      
      // Build event
      const event: LogEvent = {
        action: options.action || 'code.unknown',
        session_id: (client as any).sessionId,  // Access session ID
        object: objectData,
        attributes,
        occurred_at: new Date().toISOString()
      };
      
      // Log the event
      await client.logEvent(event);
      
      console.log(`✅ Logged: ${event.action}`);
      if (objectData) {
        console.log(`   Object: ${objectData.type}:${objectData.id}`);
      }
      
      await client.disconnect();
    } catch (error: any) {
      console.error('❌ Log failed:', error.message);
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

/**
 * Schema discovery commands - events-only approach
 */
program
  .command('schema')
  .description('Show current schema context from events (agent discovery)')
  .action(async () => {
    try {
      const client = new SnowflakeSimpleClient();
      
      console.log('📊 Schema Discovery (Events-Only)\n');
      console.log('=================================\n');
      
      // Get procedures visible to agents
      const procResult = await client.executeSql(`
        SELECT 
          procedure_name,
          category,
          description,
          signature,
          is_mcp_compatible,
          example_call
        FROM MCP.VW_IS_PROCEDURES 
        ORDER BY category, procedure_name
      `);
      
      if (procResult.success && procResult.data.length > 0) {
        console.log('Available Procedures:');
        procResult.data.forEach((proc: any) => {
          const compatible = proc.IS_MCP_COMPATIBLE ? '🔗' : '📄';
          console.log(`  ${compatible} ${proc.PROCEDURE_NAME} (${proc.CATEGORY})`);
          console.log(`     ${proc.DESCRIPTION}`);
          console.log(`     Usage: ${proc.EXAMPLE_CALL}`);
          console.log();
        });
      } else {
        console.log('📭 No procedures discovered from events');
        console.log('   Run "sf schema:publish" to populate schema discovery');
      }
      
      // Get latest schema version
      const schemaResult = await client.executeSql(`
        SELECT 
          COUNT(*) as object_count,
          MAX(schema_version) as latest_version,
          MAX(last_updated) as last_updated
        FROM MCP.VW_LATEST_SCHEMA
      `);
      
      if (schemaResult.success && schemaResult.data.length > 0) {
        const stats = schemaResult.data[0];
        console.log('Schema Statistics:');
        console.log(`  Objects: ${stats.OBJECT_COUNT}`);
        console.log(`  Version: ${stats.LATEST_VERSION || 'None'}`);
        console.log(`  Updated: ${stats.LAST_UPDATED || 'Never'}`);
      }
      
      await client.disconnect();
    } catch (error: any) {
      console.error('❌ Schema discovery failed:', error.message);
      process.exit(1);
    }
  });

program
  .command('schema:publish')
  .description('Capture and publish current schema as events')
  .option('-s, --schema <schema>', 'Schema to snapshot', 'MCP')
  .action(async (options) => {
    try {
      const client = new SnowflakeSimpleClient();
      
      console.log(`📸 Publishing schema snapshot for: ${options.schema}\n`);
      
      const result = await client.executeSql(`
        CALL MCP.PUBLISH_SCHEMA_SNAPSHOT(false, '${options.schema}')
      `);
      
      if (result.success) {
        const data = result.data[0]?.PUBLISH_SCHEMA_SNAPSHOT;
        if (data?.result === 'ok') {
          console.log('✅ Schema snapshot published successfully!');
          console.log(`   Objects: ${data.objects_published} (${data.procedures} procedures, ${data.views} views)`);
          console.log(`   Version: ${data.schema_version}`);
          console.log('\n💡 Agents can now discover schema with "sf schema"');
        } else {
          console.error('❌ Schema publish failed:', data?.error || 'Unknown error');
        }
      } else {
        console.error('❌ Schema publish failed:', result.error);
      }
      
      await client.disconnect();
    } catch (error: any) {
      console.error('❌ Error:', error.message);
      process.exit(1);
    }
  });

program
  .command('schema:procedures')
  .description('List procedures available to agents')
  .option('--category <category>', 'Filter by category (mcp, sdlc, utility)')
  .action(async (options) => {
    try {
      const client = new SnowflakeSimpleClient();
      
      let whereClause = '';
      if (options.category) {
        whereClause = `WHERE category = '${options.category}'`;
      }
      
      console.log('🔧 Agent-Accessible Procedures\n');
      console.log('==============================\n');
      
      const result = await client.executeSql(`
        SELECT 
          procedure_name,
          category,
          description,
          signature,
          is_mcp_compatible,
          parameter_type,
          example_call
        FROM MCP.VW_IS_PROCEDURES 
        ${whereClause}
        ORDER BY category, procedure_name
      `);
      
      if (result.success && result.data.length > 0) {
        let currentCategory = '';
        result.data.forEach((proc: any) => {
          if (proc.CATEGORY !== currentCategory) {
            currentCategory = proc.CATEGORY;
            console.log(`\n📁 ${currentCategory.toUpperCase()} Category:`);
            console.log('─'.repeat(40));
          }
          
          const mcpIcon = proc.IS_MCP_COMPATIBLE ? '🔗 MCP' : '📄 Standard';
          const paramIcon = proc.PARAMETER_TYPE === 'json' ? '🗃️' : '📝';
          
          console.log(`\n  ${mcpIcon} ${proc.PROCEDURE_NAME}`);
          console.log(`  ${paramIcon} ${proc.SIGNATURE}`);
          console.log(`  📋 ${proc.DESCRIPTION}`);
          console.log(`  💡 ${proc.EXAMPLE_CALL}`);
        });
        
        console.log(`\n📊 Total: ${result.data.length} procedures available to agents`);
      } else {
        console.log('📭 No procedures found');
        console.log('   Run "sf schema:publish" first to populate discovery');
      }
      
      await client.disconnect();
    } catch (error: any) {
      console.error('❌ Error:', error.message);
      process.exit(1);
    }
  });

// Parse command line arguments
program.parse();

// Show help if no command provided
if (!process.argv.slice(2).length) {
  program.outputHelp();
}