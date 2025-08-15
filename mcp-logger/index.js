/**
 * MCP Logger Server for Claude Code
 * 
 * This server provides logging infrastructure for Claude Code sessions,
 * with circuit breaker protection, batch processing, and local spooling.
 */

const { Server } = require('@modelcontextprotocol/sdk/server/index.js');
const { StdioServerTransport } = require('@modelcontextprotocol/sdk/server/stdio.js');
const snowflake = require('snowflake-sdk');
const crypto = require('crypto');
const fs = require('fs').promises;
const path = require('path');
const os = require('os');

// Configuration
const CONFIG = {
  maxBatchSize: 500,
  maxEventSizeKB: 100,
  flushIntervalMs: 5000,
  circuitBreakerThreshold: 1000,
  circuitBreakerWindowMs: 60000,
  spoolDirectory: path.join(os.tmpdir(), 'claude-code-spool'),
  compressionThreshold: 10,
  compressionWindowMs: 10000,
  retryAttempts: 3,
  retryDelayMs: 1000
};

// Circuit breaker state
const circuitBreaker = {
  eventCounts: new Map(), // session:action -> count
  windowStart: Date.now(),
  tripped: new Set() // session:action combinations that are blocked
};

// Event buffer for batching
const eventBuffer = [];
let flushTimer = null;

// Snowflake connection
let snowflakeConnection = null;

/**
 * Initialize Snowflake connection
 */
async function initializeSnowflake() {
  return new Promise((resolve, reject) => {
    const connection = snowflake.createConnection({
      account: process.env.SNOWFLAKE_ACCOUNT,
      username: 'MCP_LOGGER_SERVICE',
      password: process.env.MCP_LOGGER_PASSWORD || 'LoggerServicePassword123!',
      database: 'CLAUDE_BI',
      warehouse: 'LOG_XS_WH',
      schema: 'MCP',
      role: 'MCP_LOGGER_ROLE'
    });

    connection.connect((err, conn) => {
      if (err) {
        console.error('Failed to connect to Snowflake:', err);
        reject(err);
      } else {
        console.log('Connected to Snowflake');
        snowflakeConnection = conn;
        resolve(conn);
      }
    });
  });
}

/**
 * Execute Snowflake SQL
 */
async function executeSql(sql, binds = []) {
  return new Promise((resolve, reject) => {
    if (!snowflakeConnection) {
      reject(new Error('Snowflake connection not initialized'));
      return;
    }

    snowflakeConnection.execute({
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
 * Check circuit breaker for a session:action combination
 */
function checkCircuitBreaker(sessionId, action) {
  const key = `${sessionId}:${action}`;
  
  // Reset window if expired
  if (Date.now() - circuitBreaker.windowStart > CONFIG.circuitBreakerWindowMs) {
    circuitBreaker.eventCounts.clear();
    circuitBreaker.tripped.clear();
    circuitBreaker.windowStart = Date.now();
  }
  
  // Check if already tripped
  if (circuitBreaker.tripped.has(key)) {
    return false;
  }
  
  // Increment count
  const count = (circuitBreaker.eventCounts.get(key) || 0) + 1;
  circuitBreaker.eventCounts.set(key, count);
  
  // Trip if threshold exceeded
  if (count >= CONFIG.circuitBreakerThreshold) {
    circuitBreaker.tripped.add(key);
    
    // Log circuit break event
    const breakEvent = {
      event_id: crypto.randomBytes(16).toString('hex'),
      action: 'quality.circuit.broken',
      occurred_at: new Date().toISOString(),
      actor_id: 'system',
      source: 'mcp_logger',
      attributes: {
        session_id: sessionId,
        blocked_action: action,
        event_count: count,
        threshold: CONFIG.circuitBreakerThreshold
      }
    };
    
    // Add to buffer for logging
    eventBuffer.push(breakEvent);
    
    return false;
  }
  
  return true;
}

/**
 * Generate idempotency key for an event
 */
function generateIdempotencyKey(event) {
  const stableFields = [
    event.action,
    event.session_id,
    event.occurred_at,
    JSON.stringify(event.attributes || {})
  ].join('|');
  
  return crypto.createHash('sha256').update(stableFields).digest('hex');
}

/**
 * Validate event structure
 */
function validateEvent(event) {
  const errors = [];
  
  if (!event.action) errors.push('Missing required field: action');
  if (!event.session_id) errors.push('Missing required field: session_id');
  
  // Check event size
  const eventSize = Buffer.byteLength(JSON.stringify(event));
  if (eventSize > CONFIG.maxEventSizeKB * 1024) {
    errors.push(`Event too large: ${eventSize} bytes (max: ${CONFIG.maxEventSizeKB * 1024})`);
  }
  
  // Validate action namespace
  const validNamespaces = ['ccode.', 'quality.', 'system.'];
  if (!validNamespaces.some(ns => event.action.startsWith(ns))) {
    errors.push(`Invalid action namespace: ${event.action}`);
  }
  
  return errors;
}

/**
 * Write events to local spool file (for resilience)
 */
async function spoolEvents(events) {
  try {
    await fs.mkdir(CONFIG.spoolDirectory, { recursive: true });
    
    const filename = `events_${Date.now()}_${crypto.randomBytes(8).toString('hex')}.json`;
    const filepath = path.join(CONFIG.spoolDirectory, filename);
    
    await fs.writeFile(filepath, JSON.stringify(events));
    
    return filepath;
  } catch (error) {
    console.error('Failed to spool events:', error);
    return null;
  }
}

/**
 * Process spooled events on startup
 */
async function processSpooledEvents() {
  try {
    const files = await fs.readdir(CONFIG.spoolDirectory);
    
    for (const file of files) {
      if (!file.endsWith('.json')) continue;
      
      const filepath = path.join(CONFIG.spoolDirectory, file);
      const content = await fs.readFile(filepath, 'utf8');
      const events = JSON.parse(content);
      
      // Try to send to Snowflake
      try {
        await flushToSnowflake(events);
        // Delete file on success
        await fs.unlink(filepath);
        console.log(`Processed spooled file: ${file}`);
      } catch (error) {
        console.error(`Failed to process spooled file ${file}:`, error);
      }
    }
  } catch (error) {
    // Spool directory might not exist yet
    if (error.code !== 'ENOENT') {
      console.error('Error processing spooled events:', error);
    }
  }
}

/**
 * Flush events to Snowflake
 */
async function flushToSnowflake(events = null) {
  const eventsToFlush = events || [...eventBuffer];
  
  if (eventsToFlush.length === 0) return;
  
  // Clear buffer if flushing from buffer
  if (!events) {
    eventBuffer.length = 0;
  }
  
  try {
    // Call the batch logging procedure
    const result = await executeSql(
      'CALL MCP.LOG_DEV_EVENT(?, ?)',
      [JSON.stringify(eventsToFlush), 'CLAUDE_CODE']
    );
    
    const response = JSON.parse(result[0].LOG_DEV_EVENT);
    
    if (response.rejected > 0) {
      console.warn(`Batch processing: ${response.accepted} accepted, ${response.rejected} rejected`);
      
      // Spool rejected events for later retry
      if (response.rejected > response.accepted) {
        await spoolEvents(eventsToFlush);
      }
    }
    
    return response;
  } catch (error) {
    console.error('Failed to flush events to Snowflake:', error);
    
    // Spool events for later retry
    await spoolEvents(eventsToFlush);
    
    throw error;
  }
}

/**
 * Schedule periodic flush
 */
function scheduleFlush() {
  if (flushTimer) {
    clearTimeout(flushTimer);
  }
  
  flushTimer = setTimeout(async () => {
    try {
      await flushToSnowflake();
    } catch (error) {
      console.error('Scheduled flush failed:', error);
    }
    scheduleFlush();
  }, CONFIG.flushIntervalMs);
}

/**
 * Main MCP server setup
 */
async function main() {
  // Initialize Snowflake connection
  try {
    await initializeSnowflake();
    // Process any spooled events from previous sessions
    await processSpooledEvents();
  } catch (error) {
    console.error('Failed to initialize Snowflake, running in spool-only mode:', error);
  }
  
  // Create MCP server
  const server = new Server(
    {
      name: 'claude-code-logger',
      version: '1.0.0',
    },
    {
      capabilities: {
        tools: {},
      },
    }
  );

  // Define logging tools
  server.setRequestHandler('tools/list', async () => ({
    tools: [
      {
        name: 'log_event',
        description: 'Log a single Claude Code event',
        inputSchema: {
          type: 'object',
          properties: {
            action: { type: 'string', description: 'Event action (e.g., ccode.file.read)' },
            session_id: { type: 'string', description: 'Claude Code session ID' },
            attributes: { type: 'object', description: 'Event attributes' },
            occurred_at: { type: 'string', description: 'ISO timestamp' }
          },
          required: ['action', 'session_id']
        }
      },
      {
        name: 'log_batch',
        description: 'Log multiple Claude Code events',
        inputSchema: {
          type: 'object',
          properties: {
            events: {
              type: 'array',
              items: {
                type: 'object',
                properties: {
                  action: { type: 'string' },
                  session_id: { type: 'string' },
                  attributes: { type: 'object' },
                  occurred_at: { type: 'string' }
                },
                required: ['action', 'session_id']
              }
            }
          },
          required: ['events']
        }
      },
      {
        name: 'start_session',
        description: 'Log session start event',
        inputSchema: {
          type: 'object',
          properties: {
            session_id: { type: 'string' },
            version: { type: 'string' },
            platform: { type: 'string' },
            node_version: { type: 'string' }
          },
          required: ['session_id']
        }
      },
      {
        name: 'end_session',
        description: 'Log session end event and flush buffer',
        inputSchema: {
          type: 'object',
          properties: {
            session_id: { type: 'string' },
            exit_code: { type: 'number' },
            duration_ms: { type: 'number' }
          },
          required: ['session_id']
        }
      },
      {
        name: 'get_session_stats',
        description: 'Get statistics for current session',
        inputSchema: {
          type: 'object',
          properties: {
            session_id: { type: 'string' }
          },
          required: ['session_id']
        }
      }
    ]
  }));

  // Handle tool calls
  server.setRequestHandler('tools/call', async (request) => {
    const { name, arguments: args } = request.params;

    try {
      switch (name) {
        case 'log_event': {
          // Check circuit breaker
          if (!checkCircuitBreaker(args.session_id, args.action)) {
            return {
              content: [{
                type: 'text',
                text: JSON.stringify({
                  success: false,
                  error: 'Circuit breaker tripped - too many events',
                  action: args.action,
                  session_id: args.session_id
                })
              }]
            };
          }
          
          // Build event
          const event = {
            event_id: crypto.randomBytes(16).toString('hex'),
            action: args.action,
            session_id: args.session_id,
            idempotency_key: generateIdempotencyKey(args),
            occurred_at: args.occurred_at || new Date().toISOString(),
            attributes: args.attributes || {}
          };
          
          // Validate event
          const errors = validateEvent(event);
          if (errors.length > 0) {
            return {
              content: [{
                type: 'text',
                text: JSON.stringify({
                  success: false,
                  errors: errors
                })
              }]
            };
          }
          
          // Add to buffer
          eventBuffer.push(event);
          
          // Flush if buffer is full
          if (eventBuffer.length >= CONFIG.maxBatchSize) {
            await flushToSnowflake();
          }
          
          return {
            content: [{
              type: 'text',
              text: JSON.stringify({
                success: true,
                event_id: event.event_id,
                buffered: eventBuffer.length
              })
            }]
          };
        }
        
        case 'log_batch': {
          const validEvents = [];
          const rejectedEvents = [];
          
          for (const eventData of args.events) {
            // Check circuit breaker
            if (!checkCircuitBreaker(eventData.session_id, eventData.action)) {
              rejectedEvents.push({
                ...eventData,
                rejection_reason: 'Circuit breaker tripped'
              });
              continue;
            }
            
            // Build event
            const event = {
              event_id: crypto.randomBytes(16).toString('hex'),
              action: eventData.action,
              session_id: eventData.session_id,
              idempotency_key: generateIdempotencyKey(eventData),
              occurred_at: eventData.occurred_at || new Date().toISOString(),
              attributes: eventData.attributes || {}
            };
            
            // Validate
            const errors = validateEvent(event);
            if (errors.length > 0) {
              rejectedEvents.push({
                ...eventData,
                rejection_reason: errors.join('; ')
              });
            } else {
              validEvents.push(event);
            }
          }
          
          // Add valid events to buffer
          eventBuffer.push(...validEvents);
          
          // Flush if needed
          if (eventBuffer.length >= CONFIG.maxBatchSize) {
            await flushToSnowflake();
          }
          
          return {
            content: [{
              type: 'text',
              text: JSON.stringify({
                success: true,
                accepted: validEvents.length,
                rejected: rejectedEvents.length,
                buffered: eventBuffer.length
              })
            }]
          };
        }
        
        case 'start_session': {
          const event = {
            event_id: crypto.randomBytes(16).toString('hex'),
            action: 'ccode.session.started',
            session_id: args.session_id,
            idempotency_key: `session_start_${args.session_id}`,
            occurred_at: new Date().toISOString(),
            attributes: {
              version: args.version || 'unknown',
              platform: args.platform || process.platform,
              node_version: args.node_version || process.version
            }
          };
          
          eventBuffer.push(event);
          
          // Start flush timer if not already running
          if (!flushTimer) {
            scheduleFlush();
          }
          
          return {
            content: [{
              type: 'text',
              text: JSON.stringify({
                success: true,
                session_id: args.session_id,
                event_id: event.event_id
              })
            }]
          };
        }
        
        case 'end_session': {
          const event = {
            event_id: crypto.randomBytes(16).toString('hex'),
            action: 'ccode.session.ended',
            session_id: args.session_id,
            idempotency_key: `session_end_${args.session_id}_${Date.now()}`,
            occurred_at: new Date().toISOString(),
            attributes: {
              exit_code: args.exit_code || 0,
              duration_ms: args.duration_ms || 0
            }
          };
          
          eventBuffer.push(event);
          
          // Flush all remaining events
          const flushResult = await flushToSnowflake();
          
          return {
            content: [{
              type: 'text',
              text: JSON.stringify({
                success: true,
                session_id: args.session_id,
                events_flushed: flushResult ? flushResult.accepted : 0
              })
            }]
          };
        }
        
        case 'get_session_stats': {
          // Get stats from circuit breaker
          const sessionStats = {};
          for (const [key, count] of circuitBreaker.eventCounts) {
            if (key.startsWith(`${args.session_id}:`)) {
              const action = key.substring(args.session_id.length + 1);
              sessionStats[action] = count;
            }
          }
          
          return {
            content: [{
              type: 'text',
              text: JSON.stringify({
                session_id: args.session_id,
                buffered_events: eventBuffer.filter(e => e.session_id === args.session_id).length,
                event_counts: sessionStats,
                circuit_breaker_tripped: Array.from(circuitBreaker.tripped)
                  .filter(key => key.startsWith(`${args.session_id}:`))
              })
            }]
          };
        }
        
        default:
          return {
            content: [{
              type: 'text',
              text: `Unknown tool: ${name}`
            }]
          };
      }
    } catch (error) {
      console.error(`Error executing tool ${name}:`, error);
      return {
        content: [{
          type: 'text',
          text: JSON.stringify({
            success: false,
            error: error.message
          })
        }]
      };
    }
  });

  // Set up graceful shutdown
  process.on('SIGINT', async () => {
    console.log('Shutting down MCP logger...');
    
    // Flush remaining events
    if (eventBuffer.length > 0) {
      try {
        await flushToSnowflake();
      } catch (error) {
        console.error('Failed to flush on shutdown:', error);
        // Spool for next startup
        await spoolEvents(eventBuffer);
      }
    }
    
    // Close Snowflake connection
    if (snowflakeConnection) {
      snowflakeConnection.destroy();
    }
    
    process.exit(0);
  });

  // Start server
  const transport = new StdioServerTransport();
  await server.connect(transport);
  
  console.log('MCP Logger Server started');
}

// Run the server
main().catch((error) => {
  console.error('Fatal error:', error);
  process.exit(1);
});