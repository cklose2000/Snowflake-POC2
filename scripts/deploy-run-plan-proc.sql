-- Deploy RUN_PLAN procedure
USE DATABASE CLAUDE_BI;
USE SCHEMA MCP;

CREATE OR REPLACE PROCEDURE MCP.RUN_PLAN(PLAN VARIANT)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
EXECUTE AS OWNER
HANDLER = 'run'
COMMENT = 'Execute dashboard plan with strict guardrails and clamping'
AS
$$
import json
import datetime as dt
import hashlib

# Whitelisted procedures - the ONLY procedures Claude can call
ALLOWED_PROCS = {"DASH_GET_SERIES", "DASH_GET_TOPN", "DASH_GET_EVENTS", "DASH_GET_METRICS"}

# Allowed parameter values for safety
ALLOWED_INTERVALS = {"minute", "5 minute", "15 minute", "hour", "day"}
ALLOWED_DIMENSIONS = {"action", "actor_id", "object_type", "source"}

def _clamp_params(params):
    """Apply strict parameter clamping and validation"""
    if not params:
        params = {}
    
    # Make a copy to avoid mutating input
    clamped = dict(params)
    
    # Hard limits on row counts
    clamped["limit"] = min(int(clamped.get("limit", 1000)), 5000)
    if "n" in clamped:
        clamped["n"] = min(int(clamped["n"]), 50)
    
    # Validate interval parameter
    if clamped.get("interval") not in ALLOWED_INTERVALS:
        clamped["interval"] = "hour"  # Safe default
    
    # Validate dimension parameter
    if clamped.get("dimension") not in ALLOWED_DIMENSIONS:
        clamped["dimension"] = "action"  # Safe default
    
    # Ensure filters is an object
    if "filters" not in clamped or not isinstance(clamped["filters"], dict):
        clamped["filters"] = {}
    
    return clamped

def _generate_plan_hash(plan):
    """Generate deterministic hash for deduplication"""
    plan_str = json.dumps(plan, sort_keys=True)
    return hashlib.sha256(plan_str.encode()).hexdigest()[:16]

def _log_event(session, action, plan_hash, status, details=None):
    """Log agent event for audit trail"""
    event_data = {
        "event_id": f"agent_{plan_hash}_{dt.datetime.now().timestamp()}",
        "action": action,
        "actor_id": "CLAUDE_CODE_AI_AGENT",
        "object": {"type": "plan", "id": plan_hash},
        "attributes": {
            "status": status,
            "plan_hash": plan_hash,
            "details": details or {},
            "timestamp": dt.datetime.now(dt.timezone.utc).isoformat()
        },
        "occurred_at": dt.datetime.now(dt.timezone.utc).isoformat()
    }
    
    event_json = json.dumps(event_data).replace("'", "''")
    session.sql(f"""
        INSERT INTO LANDING.RAW_EVENTS (raw_event, source, loaded_at)
        VALUES (PARSE_JSON('{event_json}'), 'CLAUDE_CODE', CURRENT_TIMESTAMP())
    """).collect()

def run(session, plan):
    """Main execution function"""
    try:
        # Enforce role and database constraints
        session.sql("USE ROLE R_CLAUDE_AGENT").collect()
        session.sql("USE DATABASE CLAUDE_BI").collect()
        session.sql("USE SCHEMA MCP").collect()
        
        # Generate plan hash for deduplication and audit
        plan_hash = _generate_plan_hash(plan)
        
        # Set query tag for attribution
        session.sql(f"ALTER SESSION SET QUERY_TAG = 'dash-api|proc:RUN_PLAN|agent:claude|hash:{plan_hash}'").collect()
        
        # Validate procedure name
        proc_name = plan.get("proc")
        if not proc_name:
            raise ValueError("Missing 'proc' in plan")
        
        if proc_name not in ALLOWED_PROCS:
            _log_event(session, "agent.plan_rejected", plan_hash, "error", 
                      {"reason": f"Disallowed procedure: {proc_name}"})
            raise ValueError(f"Disallowed procedure: {proc_name}. Allowed: {list(ALLOWED_PROCS)}")
        
        # Apply parameter clamping
        raw_params = plan.get("params", {})
        clamped_params = _clamp_params(raw_params)
        
        # Log plan execution start
        _log_event(session, "agent.proc_called", plan_hash, "started", {
            "procedure": proc_name,
            "clamped_params": clamped_params,
            "original_params": raw_params
        })
        
        # Execute the procedure with clamped parameters
        params_json = json.dumps(clamped_params)
        sql = f"CALL MCP.{proc_name}(PARSE_JSON(?))"
        
        df = session.sql(sql).bind(params=[params_json])
        rows = df.collect()
        
        # Convert to Python-native types (avoid Snowflake types in return)
        result_rows = []
        for row in rows:
            # Convert each Row to a list of values
            result_rows.append(list(row))
        
        # Log successful execution
        _log_event(session, "agent.proc_completed", plan_hash, "success", {
            "procedure": proc_name,
            "row_count": len(result_rows),
            "execution_time_ms": None  # Could add timing if needed
        })
        
        return {
            "ok": True,
            "plan_hash": plan_hash,
            "procedure": proc_name,
            "rows": result_rows,
            "row_count": len(result_rows),
            "clamped_params": clamped_params,
            "metadata": {
                "executed_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                "query_tag": f"dash-api|proc:RUN_PLAN|agent:claude|hash:{plan_hash}"
            }
        }
        
    except Exception as e:
        # Log execution failure
        plan_hash = _generate_plan_hash(plan) if plan else "unknown"
        _log_event(session, "agent.proc_failed", plan_hash, "error", {
            "error": str(e),
            "procedure": plan.get("proc") if plan else None
        })
        
        return {
            "ok": False,
            "error": str(e),
            "plan_hash": plan_hash,
            "procedure": plan.get("proc") if plan else None
        }
$$;

-- Grant execution privileges
GRANT EXECUTE ON PROCEDURE MCP.RUN_PLAN(VARIANT) TO ROLE R_CLAUDE_AGENT;