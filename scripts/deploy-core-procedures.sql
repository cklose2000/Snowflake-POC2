-- ===================================================================
-- ALL-SNOWFLAKE NATIVE DASHBOARD PROCEDURES
-- Phase 2: Core RUN_PLAN and COMPILE_NL_PLAN Implementation
-- ===================================================================

USE DATABASE CLAUDE_BI;
USE SCHEMA MCP;

-- ===================================================================
-- 1. RUN_PLAN - Execute dashboard queries with guardrails
-- ===================================================================

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
    
    event_json = json.dumps(event_data)
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

-- ===================================================================
-- 2. COMPILE_NL_PLAN - Convert natural language to plan via Claude API
-- ===================================================================

CREATE OR REPLACE PROCEDURE MCP.COMPILE_NL_PLAN(INTENT VARIANT)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('requests')
EXTERNAL_ACCESS_INTEGRATIONS = (MCP.CLAUDE_EAI)
SECRETS = ('CLAUDE_API_KEY'='MCP.CLAUDE_API_KEY')
EXECUTE AS OWNER
HANDLER = 'run'
COMMENT = 'Convert natural language to dashboard plan via Claude API'
AS
$$
import os
import json
import datetime as dt
import hashlib
import requests
from datetime import timezone, timedelta

# Get Claude API key from Snowflake Secret
CLAUDE_API_KEY = os.environ.get("CLAUDE_API_KEY")

def _iso_now():
    """Get current timestamp in ISO format"""
    return dt.datetime.now(dt.timezone.utc).isoformat()

def _iso_hours_ago(hours):
    """Get timestamp N hours ago in ISO format"""
    return (dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=hours)).isoformat()

def _default_plan():
    """Fallback plan when Claude API fails"""
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

def _validate_plan(plan):
    """Validate and sanitize the plan returned by Claude"""
    if not plan or not isinstance(plan, dict):
        return _default_plan()
    
    # Ensure required fields
    if "proc" not in plan or "params" not in plan:
        return _default_plan()
    
    # Validate procedure name
    allowed_procs = {"DASH_GET_SERIES", "DASH_GET_TOPN", "DASH_GET_EVENTS", "DASH_GET_METRICS"}
    if plan["proc"] not in allowed_procs:
        plan["proc"] = "DASH_GET_TOPN"
    
    # Ensure params is a dict
    if not isinstance(plan["params"], dict):
        plan["params"] = {}
    
    # Add default timestamps if missing
    params = plan["params"]
    if "start_ts" not in params:
        params["start_ts"] = _iso_hours_ago(24)
    if "end_ts" not in params:
        params["end_ts"] = _iso_now()
    
    return plan

def _call_claude_api(text, timeout=10):
    """Call Claude API with minimal payload and timeout"""
    if not CLAUDE_API_KEY or CLAUDE_API_KEY == "sk-placeholder-set-manually-in-prod":
        raise Exception("Claude API key not configured")
    
    # Minimal prompt for plan generation
    prompt = f"""Convert this request to a dashboard plan JSON:
"{text}"

Return only valid JSON with this structure:
{{"proc": "DASH_GET_SERIES|DASH_GET_TOPN|DASH_GET_EVENTS|DASH_GET_METRICS", "params": {{"start_ts": "ISO timestamp", "end_ts": "ISO timestamp", "dimension": "action|actor_id|source", "n": 10, "interval": "hour|day", "filters": {{}}}}}}

Examples:
- "show last 24h by hour" → {{"proc": "DASH_GET_SERIES", "params": {{"start_ts": "{_iso_hours_ago(24)}", "end_ts": "{_iso_now()}", "interval": "hour", "filters": {{}}}}}}
- "top 10 actions" → {{"proc": "DASH_GET_TOPN", "params": {{"start_ts": "{_iso_hours_ago(24)}", "end_ts": "{_iso_now()}", "dimension": "action", "n": 10, "filters": {{}}}}}}

Request: {text}
JSON:"""

    headers = {
        "Content-Type": "application/json",
        "X-API-Key": CLAUDE_API_KEY,
        "anthropic-version": "2023-06-01"
    }
    
    payload = {
        "model": "claude-3-haiku-20240307",  # Fast, small model
        "max_tokens": 256,  # Keep response tiny
        "temperature": 0.1,  # Low creativity for structured output
        "messages": [{"role": "user", "content": prompt}]
    }
    
    response = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers=headers,
        json=payload,
        timeout=timeout
    )
    
    if response.status_code != 200:
        raise Exception(f"Claude API error: {response.status_code}")
    
    result = response.json()
    content = result.get("content", [])
    if content and content[0].get("type") == "text":
        return content[0]["text"].strip()
    
    raise Exception("Invalid Claude API response format")

def _log_event(session, action, intent_hash, status, details=None):
    """Log agent event for audit trail"""
    event_data = {
        "event_id": f"agent_{intent_hash}_{dt.datetime.now().timestamp()}",
        "action": action,
        "actor_id": "CLAUDE_CODE_AI_AGENT",
        "object": {"type": "intent", "id": intent_hash},
        "attributes": {
            "status": status,
            "intent_hash": intent_hash,
            "details": details or {},
            "timestamp": dt.datetime.now(dt.timezone.utc).isoformat()
        },
        "occurred_at": dt.datetime.now(dt.timezone.utc).isoformat()
    }
    
    event_json = json.dumps(event_data)
    session.sql(f"""
        INSERT INTO LANDING.RAW_EVENTS (raw_event, source, loaded_at)
        VALUES (PARSE_JSON('{event_json}'), 'CLAUDE_CODE', CURRENT_TIMESTAMP())
    """).collect()

def run(session, intent):
    """Main execution function"""
    try:
        # Enforce role and database constraints
        session.sql("USE ROLE R_CLAUDE_AGENT").collect()
        session.sql("USE DATABASE CLAUDE_BI").collect()
        session.sql("USE SCHEMA MCP").collect()
        
        # Extract text from intent
        text = ""
        if intent and isinstance(intent, dict):
            text = intent.get("text", "").strip()
        if not text:
            text = str(intent or "").strip()
        
        # Generate intent hash for deduplication
        intent_hash = hashlib.sha256(text.encode()).hexdigest()[:16]
        
        # Set query tag
        session.sql(f"ALTER SESSION SET QUERY_TAG = 'dash-nl|proc:COMPILE_NL_PLAN|agent:claude|hash:{intent_hash}'").collect()
        
        # Log intent received
        _log_event(session, "agent.intent_received", intent_hash, "started", {
            "text": text,
            "length": len(text)
        })
        
        try:
            # Try Claude API call
            claude_response = _call_claude_api(text, timeout=8)
            
            # Parse JSON response
            try:
                plan = json.loads(claude_response)
            except json.JSONDecodeError:
                # Try to extract JSON from response
                import re
                json_match = re.search(r'\{.*\}', claude_response, re.DOTALL)
                if json_match:
                    plan = json.loads(json_match.group())
                else:
                    raise Exception("No valid JSON found in Claude response")
            
            # Validate and sanitize plan
            validated_plan = _validate_plan(plan)
            
            # Log successful compilation
            _log_event(session, "agent.plan_compiled", intent_hash, "success", {
                "claude_response": claude_response[:200],  # Truncate for storage
                "validated_plan": validated_plan
            })
            
            return {
                "ok": True,
                "intent_hash": intent_hash,
                "plan": validated_plan,
                "source": "claude_api",
                "original_text": text,
                "metadata": {
                    "compiled_at": _iso_now(),
                    "model": "claude-3-haiku-20240307"
                }
            }
            
        except Exception as api_error:
            # Log API failure and use fallback
            _log_event(session, "agent.claude_api_failed", intent_hash, "fallback", {
                "error": str(api_error),
                "fallback_used": True
            })
            
            # Use default plan as fallback
            fallback_plan = _default_plan()
            
            return {
                "ok": True,
                "intent_hash": intent_hash,
                "plan": fallback_plan,
                "source": "fallback",
                "original_text": text,
                "fallback_reason": str(api_error),
                "metadata": {
                    "compiled_at": _iso_now(),
                    "model": "fallback"
                }
            }
            
    except Exception as e:
        # Log compilation failure
        intent_hash = hashlib.sha256(str(intent or "").encode()).hexdigest()[:16]
        _log_event(session, "agent.compilation_failed", intent_hash, "error", {
            "error": str(e)
        })
        
        return {
            "ok": False,
            "error": str(e),
            "intent_hash": intent_hash,
            "original_text": str(intent or "")
        }
$$;

-- ===================================================================
-- 3. GRANTS - Secure access for procedures
-- ===================================================================

-- Grant execution privileges to Claude agent role
GRANT EXECUTE ON PROCEDURE MCP.RUN_PLAN(VARIANT) TO ROLE R_CLAUDE_AGENT;
GRANT EXECUTE ON PROCEDURE MCP.COMPILE_NL_PLAN(VARIANT) TO ROLE R_CLAUDE_AGENT;

-- Grant usage on external access integration (already done in Phase 1, but ensuring)
GRANT USAGE ON INTEGRATION MCP.CLAUDE_EAI TO ROLE R_CLAUDE_AGENT;

-- ===================================================================
-- 4. VALIDATION TESTS
-- ===================================================================

-- Test RUN_PLAN with a simple plan
SELECT 'RUN_PLAN Test' as test_name,
       MCP.RUN_PLAN(PARSE_JSON('{"proc": "DASH_GET_METRICS", "params": {"start_ts": "2025-01-15T00:00:00Z", "end_ts": "2025-01-16T00:00:00Z", "filters": {}}}')) as result;

-- Test COMPILE_NL_PLAN with fallback (Claude API key not set yet)
SELECT 'COMPILE_NL_PLAN Test' as test_name,
       MCP.COMPILE_NL_PLAN(PARSE_JSON('{"text": "show me top 10 actions from last 24 hours"}')) as result;

-- ===================================================================
-- DEPLOYMENT NOTES  
-- ===================================================================

/*
PRODUCTION DEPLOYMENT CHECKLIST:

1. UPDATE CLAUDE API KEY:
   ALTER SECRET MCP.CLAUDE_API_KEY SET VALUE = 'sk-ant-api03-your-real-key';

2. TEST EXTERNAL ACCESS:
   - Verify network rules allow api.anthropic.com:443
   - Test with a simple NL query through COMPILE_NL_PLAN
   - Monitor query tags in QUERY_HISTORY

3. MONITOR PROCEDURE EXECUTION:
   - Check ACTIVITY.EVENTS for agent.* events
   - Monitor procedure costs and execution times
   - Verify guardrails are working (n≤50, limit≤5000)

4. SECURITY VALIDATION:
   - Ensure procedures only run as R_CLAUDE_AGENT role
   - Verify no direct table access granted
   - Test procedure parameter clamping

5. ERROR HANDLING:
   - Test API timeout scenarios
   - Verify fallback plans work correctly
   - Monitor error events and logging

Ready for Phase 3: Dashboard Management Procedures
*/