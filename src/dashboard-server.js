/**
 * Dashboard Server - Thin RSA-auth bridge for dashboard operations
 * Exposes 4 endpoints for dashboard UI interaction
 * All Snowflake access through enforced RSA authentication
 */

const express = require('express');
const bodyParser = require('body-parser');
const { SnowflakeSimpleClient } = require('../snowflake-mcp-client/dist/simple-client.js');
const { getPresetById } = require('./presets.js');
const { v4: uuidv4 } = require('uuid');
const NLCompiler = require('./nl-compiler.js');
const { exec } = require('child_process');
const util = require('util');
const execPromise = util.promisify(exec);

const app = express();
app.use(bodyParser.json());

// Initialize NL compiler for fallback
const nlCompiler = new NLCompiler();

// Enable CORS
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Headers', 'Content-Type');
  res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  next();
});

// Initialize Snowflake client with RSA auth
const sf = new SnowflakeSimpleClient({
  account: process.env.SNOWFLAKE_ACCOUNT || 'uec18397.us-east-1',
  username: process.env.SNOWFLAKE_USERNAME || 'CLAUDE_CODE_AI_AGENT',
  privateKeyPath: process.env.SF_PK_PATH,
  warehouse: process.env.SNOWFLAKE_WAREHOUSE || 'CLAUDE_AGENT_WH',
  database: process.env.SNOWFLAKE_DATABASE || 'CLAUDE_BI',
  schema: process.env.SNOWFLAKE_SCHEMA || 'MCP'
});

// Whitelisted procedures - the ONLY procedures Claude can call
const ALLOWED_PROCS = ["DASH_GET_SERIES", "DASH_GET_TOPN", "DASH_GET_EVENTS", "DASH_GET_METRICS"];

// Allowed values for parameters (hard guardrails)
const ALLOWED_INTERVALS = ["minute", "5 minute", "15 minute", "hour", "day"];
const ALLOWED_DIMENSIONS = ["action", "actor_id", "object_type", "source"];
const ALLOWED_GROUP_BY = ["action", "actor_id", null];

// Helper functions
function ok(res, data) { 
  res.json({ ok: true, data }); 
}

function err(res, e) { 
  console.error('Error:', e);
  res.status(500).json({ ok: false, error: String(e) }); 
}

// Clamp value to allowed list
function clamp(val, list) { 
  return list.includes(val) ? val : null; 
}

// Clamp integer to range
function clampInt(n, min, max, dflt) {
  const v = Number.isFinite(n) ? n : dflt; 
  return Math.min(Math.max(v, min), max);
}

/**
 * Validate and sanitize a query plan from Claude or NL compiler
 * This is our hard guardrail - nothing gets through without validation
 */
function validatePlan(plan) {
  if (!plan || typeof plan !== 'object') {
    throw new Error('Invalid plan: not an object');
  }
  
  // Validate procedure name against whitelist
  if (!ALLOWED_PROCS.includes(plan.proc)) {
    throw new Error(`Invalid procedure: ${plan.proc}. Must be one of: ${ALLOWED_PROCS.join(', ')}`);
  }
  
  const params = plan.params || {};
  const sanitized = { proc: plan.proc, params: {} };
  
  // Canonicalize timestamps to ISO format
  if (params.start_ts) {
    try {
      const date = new Date(params.start_ts);
      sanitized.params.start_ts = date.toISOString();
    } catch (e) {
      // Default to 1 hour ago
      sanitized.params.start_ts = new Date(Date.now() - 3600000).toISOString();
    }
  } else {
    sanitized.params.start_ts = new Date(Date.now() - 3600000).toISOString();
  }
  
  if (params.end_ts) {
    try {
      const date = new Date(params.end_ts);
      sanitized.params.end_ts = date.toISOString();
    } catch (e) {
      sanitized.params.end_ts = new Date().toISOString();
    }
  } else {
    sanitized.params.end_ts = new Date().toISOString();
  }
  
  // Validate procedure-specific parameters
  switch (plan.proc) {
    case 'DASH_GET_SERIES':
      sanitized.params.interval = clamp(params.interval || params.interval_str, ALLOWED_INTERVALS) || '15 minute';
      sanitized.params.group_by = clamp(params.group_by, ALLOWED_GROUP_BY);
      break;
      
    case 'DASH_GET_TOPN':
      sanitized.params.dimension = clamp(params.dimension, ALLOWED_DIMENSIONS) || 'action';
      sanitized.params.n = clampInt(params.n, 1, 50, 10);
      break;
      
    case 'DASH_GET_EVENTS':
      sanitized.params.limit = clampInt(params.limit || params.limit_rows, 1, 5000, 100);
      if (params.cursor_ts) {
        try {
          sanitized.params.cursor_ts = new Date(params.cursor_ts).toISOString();
        } catch (e) {
          sanitized.params.cursor_ts = new Date(Date.now() - 300000).toISOString(); // 5 min ago
        }
      } else {
        sanitized.params.cursor_ts = new Date(Date.now() - 300000).toISOString();
      }
      break;
      
    case 'DASH_GET_METRICS':
      // Metrics just needs time range, already handled above
      break;
  }
  
  // Validate filters if present
  if (params.filters) {
    sanitized.params.filters = {};
    
    // Only allow specific filter fields
    if (params.filters.action && typeof params.filters.action === 'string') {
      sanitized.params.filters.action = params.filters.action.substring(0, 100); // Limit length
    }
    
    if (params.filters.actor_id && typeof params.filters.actor_id === 'string') {
      sanitized.params.filters.actor_id = params.filters.actor_id.substring(0, 100);
    }
    
    // Validate cohort URL starts with s3://
    if (params.filters.cohort_url && typeof params.filters.cohort_url === 'string') {
      if (!params.filters.cohort_url.startsWith('s3://')) {
        throw new Error('Invalid cohort_url: must start with s3://');
      }
      sanitized.params.filters.cohort_url = params.filters.cohort_url;
    }
  }
  
  // Add panels if present (for dashboard generation)
  if (plan.panels && Array.isArray(plan.panels)) {
    sanitized.panels = plan.panels;
  }
  
  return sanitized;
}

/**
 * Ask Claude Code to convert natural language to a structured query plan
 * Returns JSON only - no SQL generation allowed
 * This is Claude Code in action - the intelligent agent that powers the dashboard
 */
async function askClaudeForPlan(prompt, context = {}) {
  const claudePrompt = `You are Claude Code, an intelligent dashboard agent.
Return ONLY valid JSON matching this TypeScript type, with no additional text:

type NLPlan = {
  proc: "DASH_GET_SERIES" | "DASH_GET_TOPN" | "DASH_GET_EVENTS" | "DASH_GET_METRICS",
  params: {
    start_ts?: string,    // ISO8601 timestamp
    end_ts?: string,      // ISO8601 timestamp
    interval?: "minute" | "5 minute" | "15 minute" | "hour" | "day",
    dimension?: "action" | "actor_id" | "object_type" | "source",
    group_by?: "action" | "actor_id" | null,
    n?: number,           // For top-N, between 1-50
    limit?: number,       // For events table, between 1-5000
    cursor_ts?: string,   // For event stream cursor
    filters?: {
      action?: string,
      actor_id?: string,
      cohort_url?: string  // Must start with s3://
    }
  }
}

Rules:
- NEVER generate SQL. Choose from the four procedures only.
- Default time range: last 1 hour if not specified
- Default interval: "15 minute" for time series
- Default n: 10 for top-N queries
- Default limit: 100 for event streams
- If user says "last 6 hours by 15 minutes", use proc="DASH_GET_SERIES", interval="15 minute", start_ts=(now-6h), end_ts=now
- If user says "top 10 actions", use proc="DASH_GET_TOPN", dimension="action", n=10
- If user says "recent events" or "last 50 events", use proc="DASH_GET_EVENTS", limit=50
- If user says "summary" or "metrics", use proc="DASH_GET_METRICS"

User prompt: "${prompt}"

Return ONLY the JSON object, no explanations.`;

  try {
    // Use Claude CLI to get the plan
    // Note: This assumes 'claude' CLI is available. In production, use the SDK.
    const { stdout } = await execPromise(`echo '${claudePrompt.replace(/'/g, "'\\''")}' | claude`, {
      timeout: 5000 // 5 second timeout
    });
    
    // Parse the JSON response
    const plan = JSON.parse(stdout.trim());
    return plan;
  } catch (error) {
    console.error('Claude failed to generate plan:', error);
    throw new Error('Claude unavailable or invalid response');
  }
}

// Connect to Snowflake on startup
async function initializeConnection() {
  try {
    await sf.connect();
    console.log('✅ Connected to Snowflake with RSA authentication');
  } catch (e) {
    console.error('❌ Failed to connect to Snowflake:', e);
    process.exit(1);
  }
}

/**
 * Endpoint 1: Test connection and verify authentication
 */
app.get('/api/test', async (_req, res) => {
  try {
    const result = await sf.executeSql(
      "SELECT CURRENT_USER() AS user, CURRENT_ROLE() AS role, CURRENT_WAREHOUSE() AS warehouse"
    );
    ok(res, result.data);
  } catch (e) {
    err(res, e);
  }
});

/**
 * Endpoint 2: Execute dashboard procedure with parameters
 */
app.post('/api/execute-proc', async (req, res) => {
  try {
    const { proc, params } = req.body;
    
    if (!proc) {
      return res.status(400).json({ ok: false, error: 'Missing procedure name' });
    }
    
    // Set query tag for observability - include Claude Code attribution
    await sf.executeSql(`ALTER SESSION SET QUERY_TAG = 'dash-api|proc:${proc}|agent:claude'`);
    
    // Build procedure call based on proc name
    let sql;
    switch (proc) {
      case 'DASH_GET_SERIES':
        sql = `CALL MCP.DASH_GET_SERIES(
          ${params.start_ts || "DATEADD('hour', -24, CURRENT_TIMESTAMP())"},
          ${params.end_ts || 'CURRENT_TIMESTAMP()'},
          '${params.interval_str || 'hour'}',
          ${params.filters ? `PARSE_JSON('${JSON.stringify(params.filters)}')` : 'NULL'},
          ${params.group_by ? `'${params.group_by}'` : 'NULL'}
        )`;
        break;
        
      case 'DASH_GET_TOPN':
        sql = `CALL MCP.DASH_GET_TOPN(
          ${params.start_ts || "DATEADD('hour', -24, CURRENT_TIMESTAMP())"},
          ${params.end_ts || 'CURRENT_TIMESTAMP()'},
          '${params.dimension || 'action'}',
          ${params.filters ? `PARSE_JSON('${JSON.stringify(params.filters)}')` : 'NULL'},
          ${params.n || 10}
        )`;
        break;
        
      case 'DASH_GET_EVENTS':
        sql = `CALL MCP.DASH_GET_EVENTS(
          ${params.cursor_ts || "DATEADD('minute', -5, CURRENT_TIMESTAMP())"},
          ${params.limit_rows || 50}
        )`;
        break;
        
      case 'DASH_GET_METRICS':
        sql = `CALL MCP.DASH_GET_METRICS(
          ${params.start_ts || "DATEADD('hour', -24, CURRENT_TIMESTAMP())"},
          ${params.end_ts || 'CURRENT_TIMESTAMP()'},
          ${params.filters ? `PARSE_JSON('${JSON.stringify(params.filters)}')` : 'NULL'}
        )`;
        break;
        
      default:
        return res.status(400).json({ ok: false, error: `Unknown procedure: ${proc}` });
    }
    
    const result = await sf.executeSql(sql);
    
    // Parse the procedure result (it returns a VARIANT)
    const procResult = result.data?.[0]?.['DASH_GET_' + proc.split('_').slice(2).join('_')] || result.data?.[0];
    
    ok(res, procResult);
  } catch (e) {
    err(res, e);
  }
});

/**
 * Endpoint 3: Execute preset configuration
 */
app.post('/api/execute-preset', async (req, res) => {
  try {
    const { presetId } = req.body;
    
    if (!presetId) {
      return res.status(400).json({ ok: false, error: 'Missing preset ID' });
    }
    
    // Get preset configuration
    const preset = getPresetById(presetId);
    if (!preset) {
      return res.status(404).json({ ok: false, error: `Preset not found: ${presetId}` });
    }
    
    // Set query tag
    await sf.executeSql(`ALTER SESSION SET QUERY_TAG = 'dash-api|preset:${presetId}'`);
    
    // Forward to execute-proc logic
    req.body = { proc: preset.proc, params: preset.params };
    return app._router.handle(req, res, () => {});
  } catch (e) {
    err(res, e);
  }
});

/**
 * Endpoint 4: Natural Language Query with Claude
 * Converts NL to structured plan, validates, and executes
 */
app.post('/api/nl-query', async (req, res) => {
  try {
    const { prompt, context } = req.body;
    
    if (!prompt) {
      return res.status(400).json({ ok: false, error: 'Missing prompt' });
    }
    
    let plan;
    let usedFallback = false;
    
    // Try Claude first
    try {
      plan = await askClaudeForPlan(prompt, context);
      console.log('Claude generated plan:', JSON.stringify(plan));
    } catch (claudeError) {
      console.log('Claude failed, using fallback NL compiler');
      
      // Fallback to regex-based NL compiler
      try {
        const nlResult = nlCompiler.compile(prompt);
        
        // Convert NL compiler result to our plan format
        plan = {
          proc: nlResult.proc || nlResult.procedure,
          params: nlResult.params
        };
        
        usedFallback = true;
      } catch (nlError) {
        console.error('NL compiler also failed:', nlError);
        return res.status(400).json({ 
          ok: false, 
          error: 'Could not understand query. Please try rephrasing or use preset buttons.' 
        });
      }
    }
    
    // Validate and sanitize the plan (hard guardrail)
    let validatedPlan;
    try {
      validatedPlan = validatePlan(plan);
      console.log('Validated plan:', JSON.stringify(validatedPlan));
    } catch (validationError) {
      console.error('Plan validation failed:', validationError);
      return res.status(400).json({ 
        ok: false, 
        error: `Invalid query plan: ${validationError.message}` 
      });
    }
    
    // Set query tag for observability - attribute to Claude Code
    await sf.executeSql(`ALTER SESSION SET QUERY_TAG = 'dash-nl|proc:${validatedPlan.proc}|agent:claude|fallback:${usedFallback}'`);
    
    // Execute the validated procedure with sanitized parameters
    let result;
    const { proc, params } = validatedPlan;
    
    // Build the procedure call based on the validated proc
    // Since our procedures don't accept VARIANT params yet, we need to pass individual params
    let sql;
    switch (proc) {
      case 'DASH_GET_SERIES':
        sql = `CALL MCP.DASH_GET_SERIES(
          '${params.start_ts}'::TIMESTAMP_TZ,
          '${params.end_ts}'::TIMESTAMP_TZ,
          '${params.interval}',
          ${params.filters ? `'${JSON.stringify(params.filters)}'` : 'NULL'},
          ${params.group_by ? `'${params.group_by}'` : 'NULL'}
        )`;
        break;
        
      case 'DASH_GET_TOPN':
        sql = `CALL MCP.DASH_GET_TOPN(
          '${params.start_ts}'::TIMESTAMP_TZ,
          '${params.end_ts}'::TIMESTAMP_TZ,
          '${params.dimension}',
          ${params.filters ? `'${JSON.stringify(params.filters)}'` : 'NULL'},
          ${params.n}
        )`;
        break;
        
      case 'DASH_GET_EVENTS':
        sql = `CALL MCP.DASH_GET_EVENTS(
          '${params.cursor_ts}'::TIMESTAMP_TZ,
          ${params.limit}
        )`;
        break;
        
      case 'DASH_GET_METRICS':
        sql = `CALL MCP.DASH_GET_METRICS(
          '${params.start_ts}'::TIMESTAMP_TZ,
          '${params.end_ts}'::TIMESTAMP_TZ,
          ${params.filters ? `'${JSON.stringify(params.filters)}'` : 'NULL'}
        )`;
        break;
        
      default:
        throw new Error(`Unknown procedure: ${proc}`);
    }
    
    console.log('Executing SQL:', sql);
    result = await sf.executeSql(sql);
    
    // Log the NL query event
    await sf.executeSql(`
      CALL MCP.LOG_CLAUDE_EVENT(OBJECT_CONSTRUCT(
        'action', 'dashboard.intent_parsed',
        'attributes', OBJECT_CONSTRUCT(
          'prompt', '${prompt.replace(/'/g, "''")}',
          'proc', '${proc}',
          'used_fallback', ${usedFallback},
          'params_hash', MD5('${JSON.stringify(params)}')
        )
      ), 'NL_QUERY')
    `);
    
    // Return the result
    ok(res, {
      result: result.data,
      plan: validatedPlan,
      usedFallback
    });
    
  } catch (error) {
    console.error('NL query error:', error);
    
    // Log error event
    try {
      await sf.executeSql(`
        CALL MCP.LOG_CLAUDE_EVENT(OBJECT_CONSTRUCT(
          'action', 'error.nl_query',
          'attributes', OBJECT_CONSTRUCT(
            'error', '${String(error).replace(/'/g, "''")}'
          )
        ), 'ERROR')
      `);
    } catch (logError) {
      console.error('Failed to log error:', logError);
    }
    
    err(res, error);
  }
});

/**
 * Endpoint 5: Create Snowflake Streamlit dashboard
 */
app.post('/api/create-streamlit', async (req, res) => {
  try {
    const { dashboardId, title, spec } = req.body;
    
    if (!title || !spec) {
      return res.status(400).json({ ok: false, error: 'Missing title or spec' });
    }
    
    const finalDashboardId = dashboardId || `dash_${Date.now()}_${uuidv4().slice(0, 8)}`;
    
    // Set query tag
    await sf.executeSql(`ALTER SESSION SET QUERY_TAG = 'dash-api|make-streamlit|${finalDashboardId}'`);
    
    // 1) Save dashboard spec as an event FIRST (before Streamlit creation that might fail)
    try {
      // Use fully qualified table name and include top-level columns
      const insertSQL = `
        INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS (
          action, 
          actor, 
          occurred_at, 
          dedupe_key,
          payload, 
          _source_lane, 
          _recv_at
        )
        SELECT
          'dashboard.created',
          CURRENT_USER(),
          CURRENT_TIMESTAMP(),
          CONCAT('dash_', '${finalDashboardId}'),
          OBJECT_CONSTRUCT(
            'event_id', UUID_STRING(),
            'action', 'dashboard.created',  
            'actor_id', CURRENT_USER(),
            'object', OBJECT_CONSTRUCT(
              'type', 'dashboard',
              'id', '${finalDashboardId}'
            ),
            'attributes', OBJECT_CONSTRUCT(
              'title', '${title.replace(/'/g, "''")}',
              'spec', PARSE_JSON('${JSON.stringify(spec)}'),
              'refresh_interval_sec', 300,
              'is_active', TRUE,
              'streamlit_enabled', TRUE
            ),
            'occurred_at', CURRENT_TIMESTAMP()
          ),
          'DASHBOARD_SYSTEM',
          CURRENT_TIMESTAMP()
      `;
      
      console.log('📝 Executing INSERT:', insertSQL.substring(0, 200) + '...');
      const insertResult = await sf.executeSql(insertSQL);
      console.log(`✅ Dashboard saved: ${finalDashboardId}, rows affected:`, insertResult.rowCount || insertResult.data?.length || 'unknown');
      
      // Verify it was actually inserted
      const verifyResult = await sf.executeSql(`
        SELECT COUNT(*) as cnt FROM CLAUDE_BI.LANDING.RAW_EVENTS 
        WHERE dedupe_key = CONCAT('dash_', '${finalDashboardId}')
      `);
      console.log('🔍 Verification check:', verifyResult.data);
      
    } catch (insertError) {
      console.error('❌ Failed to save dashboard:', insertError);
      console.error('Full error:', JSON.stringify(insertError, null, 2));
      return res.status(500).json({ 
        ok: false, 
        error: `Failed to save dashboard: ${insertError.message}` 
      });
    }
    
    // 2) Ensure stage exists for Streamlit apps
    await sf.executeSql(`CREATE STAGE IF NOT EXISTS MCP.DASH_APPS COMMENT='Streamlit dashboard apps'`);
    
    // 3) Create the Streamlit app (optional - dashboard is already saved)
    const appName = `DASH_${finalDashboardId.replace(/[^A-Z0-9_]/gi, '_').toUpperCase()}`;
    let streamlitCreated = false;
    let streamlitError = null;
    
    try {
      await sf.executeSql(`
        CREATE OR REPLACE STREAMLIT MCP.${appName}
        ROOT_LOCATION = '@MCP.DASH_APPS'
        MAIN_FILE = 'streamlit_app.py'
        QUERY_WAREHOUSE = 'CLAUDE_AGENT_WH'
        COMMENT = 'Dashboard ${finalDashboardId} - ${title.replace(/'/g, "''")}'
      `);
      streamlitCreated = true;
      console.log(`✅ Streamlit app created: ${appName}`);
    } catch (createError) {
      streamlitError = String(createError);
      console.warn(`⚠️ Streamlit creation failed (dashboard still saved): ${createError}`);
      // Continue - dashboard is saved, just Streamlit failed
    }
    
    // 4) Get the Streamlit URL information
    let urlInfo = {};
    try {
      // Get the url_id from SHOW STREAMLITS
      const showResult = await sf.executeSql(
        `SHOW STREAMLITS LIKE '${appName}' IN SCHEMA MCP`
      );
      
      if (showResult.data && showResult.data.length > 0) {
        const streamlitInfo = showResult.data[0];
        urlInfo = {
          url_id: streamlitInfo.url_id,
          instructions: `To view this dashboard:
1. Log into Snowflake Console (Snowsight)
2. Navigate to Projects → Streamlit
3. Find app: ${appName}
4. Or use url_id: ${streamlitInfo.url_id}
5. Add parameter: ?dashboard_id=${finalDashboardId}`,
          app_name: appName,
          dashboard_id: finalDashboardId
        };
      }
    } catch (urlError) {
      console.warn('Could not get Streamlit URL info:', urlError);
      urlInfo = {
        app_name: appName,
        dashboard_id: finalDashboardId,
        instructions: 'View in Snowflake Console under Streamlit apps'
      };
    }
    
    // Log the dashboard creation
    await sf.logEvent({
      action: 'dashboard.streamlit.created',
      object: {
        type: 'streamlit_app',
        id: appName
      },
      attributes: {
        dashboard_id: finalDashboardId,
        title,
        panels: spec.panels?.length || 0,
        url_id: urlInfo.url_id || 'unknown'
      }
    });
    
    ok(res, {
      dashboardId: finalDashboardId,
      appName,
      urlInfo: urlInfo,
      status: streamlitCreated ? 'created' : 'dashboard_saved',
      streamlitError: streamlitError,
      message: streamlitCreated ? 
        'Dashboard and Streamlit app created successfully' : 
        'Dashboard saved successfully (Streamlit creation failed - see streamlitError)'
    });
    
  } catch (e) {
    err(res, e);
  }
});

/**
 * Endpoint 6: Save dashboard spec to stage
 * Writes spec JSON to stage and logs dashboard.created event
 */
app.post('/api/save-dashboard-spec', async (req, res) => {
  try {
    const { dashboardId, title, panels, slug } = req.body;
    
    if (!panels || !Array.isArray(panels)) {
      return res.status(400).json({ ok: false, error: 'Missing panels array' });
    }
    
    const finalDashboardId = dashboardId || `dash_${Date.now()}_${uuidv4().slice(0, 8)}`;
    const finalSlug = slug || title?.toLowerCase().replace(/\s+/g, '-') || finalDashboardId;
    
    // Create spec object
    const spec = {
      dashboard_id: finalDashboardId,
      title: title || 'Executive Dashboard',
      slug: finalSlug,
      panels: panels,
      created_at: new Date().toISOString(),
      created_by: 'COO_UI'
    };
    
    // Set query tag
    await sf.executeSql(`ALTER SESSION SET QUERY_TAG = 'dash-api|save-spec|${finalDashboardId}'`);
    
    // Ensure stage exists
    await sf.executeSql(`CREATE STAGE IF NOT EXISTS MCP.DASH_SPECS COMMENT='Dashboard specifications'`);
    
    // Write spec to stage (as JSON file)
    const specJson = JSON.stringify(spec, null, 2);
    const specPath = `@MCP.DASH_SPECS/${finalDashboardId}.json`;
    
    // Note: In production, you'd write to a temp file first then PUT
    // For now, we'll just log the event with the spec inline
    
    // Create dedupe key
    const dedupeKey = `dash_${finalDashboardId}_${Date.now()}`;
    const planHash = require('crypto').createHash('md5').update(JSON.stringify(panels)).digest('hex');
    
    // Log dashboard.created event
    const eventSql = `
      CALL MCP.LOG_CLAUDE_EVENT(OBJECT_CONSTRUCT(
        'action', 'dashboard.created',
        'actor_id', CURRENT_USER(),
        'object', OBJECT_CONSTRUCT(
          'type', 'dashboard',
          'id', '${finalDashboardId}'
        ),
        'attributes', OBJECT_CONSTRUCT(
          'title', '${title?.replace(/'/g, "''")}',
          'slug', '${finalSlug}',
          'spec_url', '${specPath}',
          'plan_hash', '${planHash}',
          'panels', PARSE_JSON('${JSON.stringify(panels)}'),
          'dedupe_key', '${dedupeKey}'
        ),
        'occurred_at', CURRENT_TIMESTAMP()
      ), 'COO_UI')
    `;
    
    await sf.executeSql(eventSql);
    
    // Return deep link
    ok(res, {
      dashboardId: finalDashboardId,
      slug: finalSlug,
      deepLink: `/d/${finalSlug}`,
      specPath: specPath,
      message: 'Dashboard spec saved successfully'
    });
    
  } catch (e) {
    err(res, e);
  }
});

/**
 * Endpoint 7: Create dashboard schedule
 * Logs schedule event for external executor to process
 */
app.post('/api/create-schedule', async (req, res) => {
  try {
    const { dashboardId, frequency, time, timezone, displayTz, deliveries } = req.body;
    
    if (!dashboardId) {
      return res.status(400).json({ ok: false, error: 'Missing dashboard ID' });
    }
    
    if (!frequency || !['DAILY', 'WEEKDAYS', 'WEEKLY'].includes(frequency)) {
      return res.status(400).json({ ok: false, error: 'Invalid frequency. Must be DAILY, WEEKDAYS, or WEEKLY' });
    }
    
    if (!time || !timezone) {
      return res.status(400).json({ ok: false, error: 'Missing time or timezone' });
    }
    
    const scheduleId = `sched_${Date.now()}_${uuidv4().slice(0, 8)}`;
    const dedupeKey = `sched_${dashboardId}_${frequency}_${time}_${Date.now()}`;
    
    // Set query tag
    await sf.executeSql(`ALTER SESSION SET QUERY_TAG = 'dash-api|create-schedule|${scheduleId}'`);
    
    // Parse and validate timezone (Olson ID)
    const validTimezones = ['America/New_York', 'America/Chicago', 'America/Denver', 'America/Los_Angeles', 'UTC'];
    if (!validTimezones.includes(timezone)) {
      return res.status(400).json({ ok: false, error: 'Invalid timezone. Use Olson ID like America/Chicago' });
    }
    
    // Calculate next run time
    const now = new Date();
    const [hours, minutes] = time.split(':').map(Number);
    let nextRun = new Date();
    nextRun.setHours(hours, minutes, 0, 0);
    
    // If time has passed today, schedule for tomorrow
    if (nextRun <= now) {
      nextRun.setDate(nextRun.getDate() + 1);
    }
    
    // Adjust for weekdays if needed
    if (frequency === 'WEEKDAYS') {
      const day = nextRun.getDay();
      if (day === 0) nextRun.setDate(nextRun.getDate() + 1); // Sunday -> Monday
      if (day === 6) nextRun.setDate(nextRun.getDate() + 2); // Saturday -> Monday
    }
    
    // Log dashboard.schedule_created event
    const eventSql = `
      CALL MCP.LOG_CLAUDE_EVENT(OBJECT_CONSTRUCT(
        'action', 'dashboard.schedule_created',
        'actor_id', CURRENT_USER(),
        'object', OBJECT_CONSTRUCT(
          'type', 'schedule',
          'id', '${scheduleId}'
        ),
        'attributes', OBJECT_CONSTRUCT(
          'schedule_id', '${scheduleId}',
          'dashboard_id', '${dashboardId}',
          'frequency', '${frequency}',
          'time', '${time}',
          'timezone', '${timezone}',
          'display_tz', '${displayTz || timezone}',
          'deliveries', ARRAY_CONSTRUCT(${deliveries?.map(d => `'${d}'`).join(',') || "'email'"}),
          'next_run', '${nextRun.toISOString()}',
          'dedupe_key', '${dedupeKey}'
        ),
        'occurred_at', CURRENT_TIMESTAMP()
      ), 'COO_UI')
    `;
    
    await sf.executeSql(eventSql);
    
    ok(res, {
      scheduleId,
      dashboardId,
      nextRun: nextRun.toISOString(),
      displayNextRun: `${nextRun.toLocaleDateString()} at ${time} ${displayTz || timezone}`,
      message: 'Schedule created successfully'
    });
    
  } catch (e) {
    err(res, e);
  }
});

// Health check endpoint
app.get('/health', (req, res) => {
  res.json({ ok: true, service: 'dashboard-server', timestamp: new Date().toISOString() });
});

// Start server
const PORT = process.env.DASHBOARD_PORT || 3001;

async function start() {
  await initializeConnection();
  
  app.listen(PORT, () => {
    console.log(`📊 Dashboard server running on http://localhost:${PORT}`);
    console.log('Available endpoints:');
    console.log('  GET  /api/test           - Test connection');
    console.log('  POST /api/execute-proc   - Execute dashboard procedure');
    console.log('  POST /api/execute-preset - Execute preset configuration');
    console.log('  POST /api/create-streamlit - Create Streamlit dashboard');
  });
}

// Handle graceful shutdown
process.on('SIGINT', async () => {
  console.log('\n👋 Shutting down dashboard server...');
  await sf.disconnect();
  process.exit(0);
});

// Start the server
start().catch(err => {
  console.error('Failed to start dashboard server:', err);
  process.exit(1);
});

module.exports = app;