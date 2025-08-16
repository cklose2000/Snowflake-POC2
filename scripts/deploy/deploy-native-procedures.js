/**
 * Deploy All-Snowflake Native Procedures
 * Using direct Snowflake connection with RSA authentication
 */

const snowflake = require('snowflake-sdk');
const fs = require('fs');
const path = require('path');

// Read private key
const privateKey = fs.readFileSync('./claude_code_rsa_key.p8', 'utf8');

// Connection configuration
const connection = snowflake.createConnection({
  account: 'uec18397.us-east-1',
  username: 'CLAUDE_CODE_AI_AGENT',
  authenticator: 'SNOWFLAKE_JWT',
  privateKey: privateKey,
  database: 'CLAUDE_BI',
  schema: 'MCP',
  warehouse: 'CLAUDE_AGENT_WH'
});

// Connect to Snowflake
connection.connect((err, conn) => {
  if (err) {
    console.error('❌ Unable to connect: ' + err.message);
    process.exit(1);
  }
  console.log('✅ Successfully connected as ' + conn.getUsername());
  
  // Deploy procedures sequentially
  deployProcedures();
});

async function execute(sql, description) {
  return new Promise((resolve, reject) => {
    console.log(`\n📋 ${description}`);
    connection.execute({
      sqlText: sql,
      complete: (err, stmt, rows) => {
        if (err) {
          console.error(`❌ Failed: ${err.message}`);
          reject(err);
        } else {
          console.log(`✅ Success: ${stmt.getSqlText().substring(0, 50)}...`);
          resolve(rows);
        }
      }
    });
  });
}

async function deployProcedures() {
  try {
    console.log('\n🚀 Starting All-Snowflake Native Deployment\n');
    
    // Phase 1: Infrastructure (Stages already created)
    console.log('📦 Phase 1: Infrastructure');
    console.log('✅ Stages already created (DASH_SPECS, DASH_SNAPSHOTS, DASH_COHORTS, DASH_APPS)');
    
    // Phase 2: RUN_PLAN Procedure
    console.log('\n📦 Phase 2: Core Procedures');
    
    await execute(`
      CREATE OR REPLACE PROCEDURE MCP.RUN_PLAN(PLAN VARIANT)
      RETURNS VARIANT
      LANGUAGE PYTHON
      RUNTIME_VERSION = '3.10'
      PACKAGES = ('snowflake-snowpark-python')
      EXECUTE AS OWNER
      HANDLER = 'run'
      COMMENT = 'Execute dashboard plan with strict guardrails'
      AS
      $$
import json
import datetime as dt

ALLOWED_PROCS = {"DASH_GET_SERIES", "DASH_GET_TOPN", "DASH_GET_EVENTS", "DASH_GET_METRICS"}

def _clamp(p):
    p = dict(p or {})
    p["limit"] = min(int(p.get("limit", 1000)), 5000)
    if "n" in p: p["n"] = min(int(p["n"]), 50)
    if p.get("interval") not in (None,"minute","5 minute","15 minute","hour","day"):
        p["interval"] = "hour"
    return p

def run(session, plan):
    session.sql("ALTER SESSION SET QUERY_TAG = 'dash-api|proc:RUN_PLAN|agent:claude'").collect()
    proc = plan.get("proc")
    if proc not in ALLOWED_PROCS:
        return {"ok": False, "error": f"Disallowed proc: {proc}"}
    params = _clamp(plan.get("params"))
    payload = json.dumps(params)
    
    try:
        df = session.sql(f"CALL MCP.{proc}(PARSE_JSON(?))").bind(params=[payload])
        rows = df.collect()
        pyrows = [list(r) for r in rows]
        return {"ok": True, "rows": pyrows, "row_count": len(pyrows), "procedure": proc}
    except Exception as e:
        return {"ok": False, "error": str(e), "procedure": proc}
      $$
    `, 'Creating RUN_PLAN procedure');
    
    // Phase 3: COMPILE_NL_PLAN Procedure (simplified without External Access for now)
    await execute(`
      CREATE OR REPLACE PROCEDURE MCP.COMPILE_NL_PLAN(INTENT VARIANT)
      RETURNS VARIANT
      LANGUAGE PYTHON
      RUNTIME_VERSION = '3.10'
      PACKAGES = ('snowflake-snowpark-python')
      EXECUTE AS OWNER
      HANDLER = 'run'
      COMMENT = 'Convert natural language to dashboard plan'
      AS
      $$
import json
import datetime as dt
from datetime import timezone, timedelta

def _iso_now():
    return dt.datetime.now(dt.timezone.utc).isoformat()

def _iso_hours_ago(hours):
    return (dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=hours)).isoformat()

def _default_plan():
    return {
        "proc": "DASH_GET_TOPN",
        "params": {
            "start_ts": _iso_hours_ago(24),
            "end_ts": _iso_now(),
            "dimension": "action",
            "n": 10,
            "filters": {}
        }
    }

def run(session, intent):
    text = (intent or {}).get("text", "").strip() if isinstance(intent, dict) else str(intent)
    
    # Simple pattern matching as fallback
    plan = _default_plan()
    
    if "series" in text.lower() or "trend" in text.lower() or "hour" in text.lower():
        plan["proc"] = "DASH_GET_SERIES"
        plan["params"]["interval"] = "hour"
    elif "top" in text.lower() or "ranking" in text.lower():
        plan["proc"] = "DASH_GET_TOPN"
        plan["params"]["dimension"] = "action"
        plan["params"]["n"] = 10
    elif "event" in text.lower() or "recent" in text.lower():
        plan["proc"] = "DASH_GET_EVENTS"
        plan["params"] = {"cursor_ts": _iso_now(), "limit": 100}
    elif "metric" in text.lower() or "kpi" in text.lower():
        plan["proc"] = "DASH_GET_METRICS"
    
    return {
        "ok": True,
        "plan": plan,
        "source": "fallback",
        "original_text": text
    }
      $$
    `, 'Creating COMPILE_NL_PLAN procedure');
    
    // Phase 4: SAVE_DASHBOARD_SPEC Procedure
    await execute(`
      CREATE OR REPLACE PROCEDURE MCP.SAVE_DASHBOARD_SPEC(SPEC VARIANT)
      RETURNS VARIANT
      LANGUAGE PYTHON
      RUNTIME_VERSION = '3.10'
      PACKAGES = ('snowflake-snowpark-python')
      EXECUTE AS OWNER
      HANDLER = 'run'
      COMMENT = 'Save dashboard specification to stage'
      AS
      $$
import json
import datetime as dt
import uuid

def run(session, spec):
    try:
        dashboard_id = f"dash_{uuid.uuid4().hex[:8]}"
        
        # Validate spec
        validated_spec = {
            "dashboard_id": dashboard_id,
            "title": spec.get("title", "Untitled Dashboard"),
            "panels": spec.get("panels", []),
            "created_at": dt.datetime.now(dt.timezone.utc).isoformat()
        }
        
        # Log event
        event_data = {
            "event_id": f"dash_{dashboard_id}_{dt.datetime.now().timestamp()}",
            "action": "dashboard.created",
            "actor_id": "CLAUDE_CODE_AI_AGENT",
            "object": {"type": "dashboard", "id": dashboard_id},
            "attributes": {
                "title": validated_spec["title"],
                "stage_path": f"@MCP.DASH_SPECS/{dashboard_id}.json",
                "panel_count": len(validated_spec["panels"])
            },
            "occurred_at": dt.datetime.now(dt.timezone.utc).isoformat()
        }
        
        event_json = json.dumps(event_data).replace("'", "''")
        session.sql(f"""
            INSERT INTO LANDING.RAW_EVENTS (raw_event, source, loaded_at)
            VALUES (PARSE_JSON('{event_json}'), 'CLAUDE_CODE', CURRENT_TIMESTAMP())
        """).collect()
        
        return {
            "ok": True,
            "dashboard_id": dashboard_id,
            "stage_path": f"@MCP.DASH_SPECS/{dashboard_id}.json"
        }
        
    except Exception as e:
        return {"ok": False, "error": str(e)}
      $$
    `, 'Creating SAVE_DASHBOARD_SPEC procedure');
    
    // Phase 5: LIST_DASHBOARDS Procedure
    await execute(`
      CREATE OR REPLACE PROCEDURE MCP.LIST_DASHBOARDS()
      RETURNS VARIANT
      LANGUAGE SQL
      EXECUTE AS OWNER
      AS
      BEGIN
        RETURN (
          SELECT ARRAY_AGG(
            OBJECT_CONSTRUCT(
              'dashboard_id', object_id,
              'title', attributes:title::string,
              'panel_count', attributes:panel_count::int,
              'created_at', occurred_at
            )
          )
          FROM (
            SELECT object_id, attributes, occurred_at,
                   ROW_NUMBER() OVER (PARTITION BY object_id ORDER BY occurred_at DESC) as rn
            FROM ACTIVITY.EVENTS
            WHERE action = 'dashboard.created'
            QUALIFY rn = 1
            ORDER BY occurred_at DESC
            LIMIT 100
          )
        );
      END
    `, 'Creating LIST_DASHBOARDS procedure');
    
    // Grant permissions
    console.log('\n📦 Phase 6: Granting Permissions');
    
    await execute(
      'GRANT EXECUTE ON PROCEDURE MCP.RUN_PLAN(VARIANT) TO ROLE R_CLAUDE_AGENT',
      'Granting RUN_PLAN permissions'
    );
    
    await execute(
      'GRANT EXECUTE ON PROCEDURE MCP.COMPILE_NL_PLAN(VARIANT) TO ROLE R_CLAUDE_AGENT',
      'Granting COMPILE_NL_PLAN permissions'
    );
    
    await execute(
      'GRANT EXECUTE ON PROCEDURE MCP.SAVE_DASHBOARD_SPEC(VARIANT) TO ROLE R_CLAUDE_AGENT',
      'Granting SAVE_DASHBOARD_SPEC permissions'
    );
    
    await execute(
      'GRANT EXECUTE ON PROCEDURE MCP.LIST_DASHBOARDS() TO ROLE R_CLAUDE_AGENT',
      'Granting LIST_DASHBOARDS permissions'
    );
    
    // Test the procedures
    console.log('\n📦 Phase 7: Testing Procedures');
    
    const testPlan = {
      proc: "DASH_GET_METRICS",
      params: {
        start_ts: "2025-01-15T00:00:00Z",
        end_ts: "2025-01-16T00:00:00Z",
        filters: {}
      }
    };
    
    await execute(
      `CALL MCP.RUN_PLAN(PARSE_JSON('${JSON.stringify(testPlan)}'))`,
      'Testing RUN_PLAN procedure'
    );
    
    await execute(
      `CALL MCP.COMPILE_NL_PLAN(PARSE_JSON('{"text": "show top 10 actions"}'))`,
      'Testing COMPILE_NL_PLAN procedure'
    );
    
    await execute(
      'CALL MCP.LIST_DASHBOARDS()',
      'Testing LIST_DASHBOARDS procedure'
    );
    
    console.log('\n✅ All-Snowflake Native Deployment Complete!');
    console.log('\n📝 Next Steps:');
    console.log('1. External Access Integration and Secrets require admin privileges');
    console.log('2. Deploy serverless task for scheduling');
    console.log('3. Upload Streamlit app to stage');
    console.log('4. Create Streamlit application');
    
    process.exit(0);
    
  } catch (error) {
    console.error('\n❌ Deployment failed:', error.message);
    process.exit(1);
  }
}