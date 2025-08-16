-- ===================================================================
-- ALL-SNOWFLAKE NATIVE DASHBOARD MANAGEMENT
-- Phase 3: SAVE_DASHBOARD_SPEC and CREATE_DASHBOARD_SCHEDULE procedures
-- ===================================================================

USE DATABASE CLAUDE_BI;
USE SCHEMA MCP;

-- ===================================================================
-- 1. SAVE_DASHBOARD_SPEC - Persist dashboard specifications to stages
-- ===================================================================

CREATE OR REPLACE PROCEDURE MCP.SAVE_DASHBOARD_SPEC(SPEC VARIANT)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
EXECUTE AS OWNER
HANDLER = 'run'
COMMENT = 'Save dashboard specification to stage with event logging'
AS
$$
import json
import datetime as dt
import hashlib
import uuid

def _generate_dashboard_id():
    """Generate unique dashboard ID"""
    return f"dash_{uuid.uuid4().hex[:8]}"

def _generate_spec_hash(spec):
    """Generate deterministic hash for deduplication"""
    spec_str = json.dumps(spec, sort_keys=True)
    return hashlib.sha256(spec_str.encode()).hexdigest()[:16]

def _validate_spec(spec):
    """Validate and sanitize dashboard specification"""
    if not spec or not isinstance(spec, dict):
        raise ValueError("Spec must be a valid JSON object")
    
    # Ensure required fields
    validated_spec = {
        "title": spec.get("title", "Untitled Dashboard"),
        "description": spec.get("description", ""),
        "panels": spec.get("panels", []),
        "refresh_interval_sec": min(int(spec.get("refresh_interval_sec", 300)), 3600),  # Max 1 hour
        "created_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "created_by": "CLAUDE_CODE_AI_AGENT"
    }
    
    # Validate panels
    valid_panel_types = {"metrics", "series", "topn", "events"}
    validated_panels = []
    
    for panel in validated_spec["panels"]:
        if isinstance(panel, dict) and panel.get("type") in valid_panel_types:
            validated_panel = {
                "type": panel["type"],
                "title": panel.get("title", f"{panel['type'].title()} Panel"),
                "params": panel.get("params", {}),
                "position": panel.get("position", {"row": 0, "col": 0, "width": 12, "height": 6})
            }
            validated_panels.append(validated_panel)
    
    validated_spec["panels"] = validated_panels
    
    if not validated_panels:
        raise ValueError("Dashboard must have at least one valid panel")
    
    return validated_spec

def _log_event(session, action, dashboard_id, spec_hash, status, details=None):
    """Log dashboard event for audit trail"""
    event_data = {
        "event_id": f"dash_{dashboard_id}_{dt.datetime.now().timestamp()}",
        "action": action,
        "actor_id": "CLAUDE_CODE_AI_AGENT",
        "object": {"type": "dashboard", "id": dashboard_id},
        "attributes": {
            "status": status,
            "spec_hash": spec_hash,
            "stage_path": f"@MCP.DASH_SPECS/{dashboard_id}.json",
            "details": details or {},
            "timestamp": dt.datetime.now(dt.timezone.utc).isoformat(),
            "dedupe_key": f"dashboard.created|{dashboard_id}|{spec_hash}"
        },
        "occurred_at": dt.datetime.now(dt.timezone.utc).isoformat()
    }
    
    event_json = json.dumps(event_data)
    session.sql(f"""
        INSERT INTO LANDING.RAW_EVENTS (raw_event, source, loaded_at)
        VALUES (PARSE_JSON('{event_json}'), 'CLAUDE_CODE', CURRENT_TIMESTAMP())
    """).collect()

def run(session, spec):
    """Main execution function"""
    try:
        # Enforce role and database constraints
        session.sql("USE ROLE R_CLAUDE_AGENT").collect()
        session.sql("USE DATABASE CLAUDE_BI").collect()
        session.sql("USE SCHEMA MCP").collect()
        
        # Generate dashboard ID and spec hash
        dashboard_id = _generate_dashboard_id()
        spec_hash = _generate_spec_hash(spec)
        
        # Set query tag
        session.sql(f"ALTER SESSION SET QUERY_TAG = 'dash-save|proc:SAVE_DASHBOARD_SPEC|agent:claude|id:{dashboard_id}'").collect()
        
        # Validate specification
        validated_spec = _validate_spec(spec)
        validated_spec["dashboard_id"] = dashboard_id
        validated_spec["spec_hash"] = spec_hash
        
        # Save to stage
        spec_json = json.dumps(validated_spec, indent=2)
        stage_path = f"@MCP.DASH_SPECS/{dashboard_id}.json"
        
        # Use PUT command to upload to stage
        session.sql(f"""
            SELECT PUT_JSON('{spec_json}') as uploaded_content
        """).collect()
        
        # Create a temporary file and upload it
        temp_sql = f"""
            COPY INTO {stage_path}
            FROM (
                SELECT PARSE_JSON('{spec_json}') as content
            )
            FILE_FORMAT = (TYPE = JSON)
            OVERWRITE = TRUE
        """
        
        # Alternative: Use direct stage write
        session.sql(f"""
            CREATE OR REPLACE TEMPORARY TABLE temp_dashboard_spec AS
            SELECT '{dashboard_id}' as dashboard_id, PARSE_JSON('{spec_json}') as spec_content
        """).collect()
        
        session.sql(f"""
            COPY INTO {stage_path}
            FROM (SELECT spec_content FROM temp_dashboard_spec)
            FILE_FORMAT = (TYPE = JSON)
            OVERWRITE = TRUE
        """).collect()
        
        # Log successful save
        _log_event(session, "dashboard.created", dashboard_id, spec_hash, "success", {
            "title": validated_spec["title"],
            "panel_count": len(validated_spec["panels"]),
            "stage_path": stage_path
        })
        
        return {
            "ok": True,
            "dashboard_id": dashboard_id,
            "spec_hash": spec_hash,
            "stage_path": stage_path,
            "validated_spec": validated_spec,
            "metadata": {
                "created_at": validated_spec["created_at"],
                "panel_count": len(validated_spec["panels"])
            }
        }
        
    except Exception as e:
        # Log save failure
        dashboard_id = _generate_dashboard_id()
        spec_hash = _generate_spec_hash(spec) if spec else "unknown"
        
        _log_event(session, "dashboard.save_failed", dashboard_id, spec_hash, "error", {
            "error": str(e)
        })
        
        return {
            "ok": False,
            "error": str(e),
            "dashboard_id": dashboard_id,
            "spec_hash": spec_hash
        }
$$;

-- ===================================================================
-- 2. CREATE_DASHBOARD_SCHEDULE - Schedule dashboard generation and delivery
-- ===================================================================

CREATE OR REPLACE PROCEDURE MCP.CREATE_DASHBOARD_SCHEDULE(SCHEDULE_SPEC VARIANT)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
EXECUTE AS OWNER
HANDLER = 'run'
COMMENT = 'Create dashboard delivery schedule with timezone handling'
AS
$$
import json
import datetime as dt
import hashlib
import uuid
import re

def _generate_schedule_id():
    """Generate unique schedule ID"""
    return f"sched_{uuid.uuid4().hex[:8]}"

def _validate_timezone(tz_string):
    """Validate Olson timezone identifier"""
    # Basic validation for common timezone patterns
    valid_patterns = [
        r'^UTC$',
        r'^America/[A-Za-z_]+$',
        r'^Europe/[A-Za-z_]+$',
        r'^Asia/[A-Za-z_]+$',
        r'^Australia/[A-Za-z_]+$'
    ]
    
    for pattern in valid_patterns:
        if re.match(pattern, tz_string):
            return True
    
    return False

def _validate_schedule_spec(spec):
    """Validate and sanitize schedule specification"""
    if not spec or not isinstance(spec, dict):
        raise ValueError("Schedule spec must be a valid JSON object")
    
    # Validate dashboard_id
    dashboard_id = spec.get("dashboard_id")
    if not dashboard_id or not isinstance(dashboard_id, str):
        raise ValueError("Valid dashboard_id is required")
    
    # Validate frequency
    valid_frequencies = {"DAILY", "WEEKDAYS", "WEEKLY", "HOURLY"}
    frequency = spec.get("frequency", "DAILY").upper()
    if frequency not in valid_frequencies:
        frequency = "DAILY"
    
    # Validate time
    time_str = spec.get("time", "09:00")
    if not re.match(r'^\d{1,2}:\d{2}$', time_str):
        time_str = "09:00"
    
    # Parse hour and minute
    try:
        hour, minute = map(int, time_str.split(':'))
        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            hour, minute = 9, 0
    except:
        hour, minute = 9, 0
    
    # Validate timezone
    timezone = spec.get("timezone", "UTC")
    if not _validate_timezone(timezone):
        timezone = "UTC"
    
    # Validate delivery methods
    deliveries = spec.get("deliveries", ["email"])
    if not isinstance(deliveries, list) or not deliveries:
        deliveries = ["email"]
    
    valid_delivery_methods = {"email", "slack", "webhook"}
    deliveries = [d for d in deliveries if d in valid_delivery_methods]
    if not deliveries:
        deliveries = ["email"]
    
    # Validate recipients
    recipients = spec.get("recipients", [])
    if not isinstance(recipients, list):
        recipients = []
    
    validated_spec = {
        "schedule_id": _generate_schedule_id(),
        "dashboard_id": dashboard_id,
        "frequency": frequency,
        "hour": hour,
        "minute": minute,
        "timezone": timezone,
        "deliveries": deliveries,
        "recipients": recipients,
        "enabled": bool(spec.get("enabled", True)),
        "created_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "created_by": "CLAUDE_CODE_AI_AGENT",
        "next_run_at": None  # Will be computed at execution time
    }
    
    return validated_spec

def _generate_schedule_hash(spec):
    """Generate deterministic hash for deduplication"""
    spec_str = json.dumps(spec, sort_keys=True)
    return hashlib.sha256(spec_str.encode()).hexdigest()[:16]

def _log_event(session, action, schedule_id, schedule_hash, status, details=None):
    """Log schedule event for audit trail"""
    event_data = {
        "event_id": f"sched_{schedule_id}_{dt.datetime.now().timestamp()}",
        "action": action,
        "actor_id": "CLAUDE_CODE_AI_AGENT",
        "object": {"type": "schedule", "id": schedule_id},
        "attributes": {
            "status": status,
            "schedule_hash": schedule_hash,
            "stage_path": f"@MCP.DASH_SPECS/{schedule_id}_schedule.json",
            "details": details or {},
            "timestamp": dt.datetime.now(dt.timezone.utc).isoformat(),
            "dedupe_key": f"schedule.created|{schedule_id}|{schedule_hash}"
        },
        "occurred_at": dt.datetime.now(dt.timezone.utc).isoformat()
    }
    
    event_json = json.dumps(event_data)
    session.sql(f"""
        INSERT INTO LANDING.RAW_EVENTS (raw_event, source, loaded_at)
        VALUES (PARSE_JSON('{event_json}'), 'CLAUDE_CODE', CURRENT_TIMESTAMP())
    """).collect()

def run(session, schedule_spec):
    """Main execution function"""
    try:
        # Enforce role and database constraints
        session.sql("USE ROLE R_CLAUDE_AGENT").collect()
        session.sql("USE DATABASE CLAUDE_BI").collect()
        session.sql("USE SCHEMA MCP").collect()
        
        # Validate schedule specification
        validated_spec = _validate_schedule_spec(schedule_spec)
        schedule_id = validated_spec["schedule_id"]
        schedule_hash = _generate_schedule_hash(validated_spec)
        
        # Set query tag
        session.sql(f"ALTER SESSION SET QUERY_TAG = 'dash-schedule|proc:CREATE_DASHBOARD_SCHEDULE|agent:claude|id:{schedule_id}'").collect()
        
        # Save schedule to stage
        schedule_json = json.dumps(validated_spec, indent=2)
        stage_path = f"@MCP.DASH_SPECS/{schedule_id}_schedule.json"
        
        # Save to stage using temporary table approach
        session.sql(f"""
            CREATE OR REPLACE TEMPORARY TABLE temp_schedule_spec AS
            SELECT '{schedule_id}' as schedule_id, PARSE_JSON('{schedule_json}') as schedule_content
        """).collect()
        
        session.sql(f"""
            COPY INTO {stage_path}
            FROM (SELECT schedule_content FROM temp_schedule_spec)
            FILE_FORMAT = (TYPE = JSON)
            OVERWRITE = TRUE
        """).collect()
        
        # Log schedule creation event for task consumption
        _log_event(session, "dashboard.schedule_created", schedule_id, schedule_hash, "success", {
            "dashboard_id": validated_spec["dashboard_id"],
            "frequency": validated_spec["frequency"],
            "time": f"{validated_spec['hour']:02d}:{validated_spec['minute']:02d}",
            "timezone": validated_spec["timezone"],
            "deliveries": validated_spec["deliveries"],
            "stage_path": stage_path
        })
        
        return {
            "ok": True,
            "schedule_id": schedule_id,
            "schedule_hash": schedule_hash,
            "stage_path": stage_path,
            "validated_spec": validated_spec,
            "metadata": {
                "created_at": validated_spec["created_at"],
                "frequency": validated_spec["frequency"],
                "timezone": validated_spec["timezone"]
            }
        }
        
    except Exception as e:
        # Log schedule creation failure
        schedule_id = _generate_schedule_id()
        schedule_hash = _generate_schedule_hash(schedule_spec) if schedule_spec else "unknown"
        
        _log_event(session, "schedule.creation_failed", schedule_id, schedule_hash, "error", {
            "error": str(e)
        })
        
        return {
            "ok": False,
            "error": str(e),
            "schedule_id": schedule_id,
            "schedule_hash": schedule_hash
        }
$$;

-- ===================================================================
-- 3. LIST_DASHBOARDS - Retrieve available dashboards from events
-- ===================================================================

CREATE OR REPLACE PROCEDURE MCP.LIST_DASHBOARDS()
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
EXECUTE AS OWNER
HANDLER = 'run'
COMMENT = 'List available dashboards from activity events'
AS
$$
import json
import datetime as dt

def run(session):
    """List all created dashboards from events"""
    try:
        # Enforce role and database constraints
        session.sql("USE ROLE R_CLAUDE_AGENT").collect()
        session.sql("USE DATABASE CLAUDE_BI").collect()
        session.sql("USE SCHEMA MCP").collect()
        
        # Set query tag
        session.sql("ALTER SESSION SET QUERY_TAG = 'dash-list|proc:LIST_DASHBOARDS|agent:claude'").collect()
        
        # Query dashboard events from ACTIVITY.EVENTS
        df = session.sql("""
            SELECT 
                object_id as dashboard_id,
                attributes:title::string as title,
                attributes:panel_count::int as panel_count,
                attributes:stage_path::string as stage_path,
                attributes:spec_hash::string as spec_hash,
                occurred_at,
                ROW_NUMBER() OVER (PARTITION BY object_id ORDER BY occurred_at DESC) as rn
            FROM ACTIVITY.EVENTS
            WHERE action = 'dashboard.created'
              AND attributes:status::string = 'success'
            QUALIFY rn = 1
            ORDER BY occurred_at DESC
            LIMIT 100
        """)
        
        rows = df.collect()
        
        dashboards = []
        for row in rows:
            dashboard = {
                "dashboard_id": row["DASHBOARD_ID"],
                "title": row["TITLE"],
                "panel_count": row["PANEL_COUNT"],
                "stage_path": row["STAGE_PATH"],
                "spec_hash": row["SPEC_HASH"],
                "created_at": row["OCCURRED_AT"].isoformat() if row["OCCURRED_AT"] else None
            }
            dashboards.append(dashboard)
        
        return {
            "ok": True,
            "dashboards": dashboards,
            "count": len(dashboards),
            "metadata": {
                "retrieved_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                "source": "activity_events"
            }
        }
        
    except Exception as e:
        return {
            "ok": False,
            "error": str(e),
            "dashboards": [],
            "count": 0
        }
$$;

-- ===================================================================
-- 4. GRANTS - Secure access for dashboard management procedures
-- ===================================================================

-- Grant execution privileges to Claude agent role
GRANT EXECUTE ON PROCEDURE MCP.SAVE_DASHBOARD_SPEC(VARIANT) TO ROLE R_CLAUDE_AGENT;
GRANT EXECUTE ON PROCEDURE MCP.CREATE_DASHBOARD_SCHEDULE(VARIANT) TO ROLE R_CLAUDE_AGENT;
GRANT EXECUTE ON PROCEDURE MCP.LIST_DASHBOARDS() TO ROLE R_CLAUDE_AGENT;

-- ===================================================================
-- 5. VALIDATION TESTS
-- ===================================================================

-- Test dashboard spec saving
SELECT 'SAVE_DASHBOARD_SPEC Test' as test_name,
       MCP.SAVE_DASHBOARD_SPEC(PARSE_JSON('{
         "title": "Test Executive Dashboard",
         "description": "Test dashboard for validation",
         "panels": [
           {"type": "metrics", "title": "KPIs", "params": {}},
           {"type": "series", "title": "Trends", "params": {"interval": "hour"}}
         ],
         "refresh_interval_sec": 300
       }')) as result;

-- Test schedule creation
SELECT 'CREATE_DASHBOARD_SCHEDULE Test' as test_name,
       MCP.CREATE_DASHBOARD_SCHEDULE(PARSE_JSON('{
         "dashboard_id": "dash_test_001",
         "frequency": "DAILY",
         "time": "09:00",
         "timezone": "America/New_York",
         "deliveries": ["email"],
         "recipients": ["exec@company.com"],
         "enabled": true
       }')) as result;

-- Test dashboard listing
SELECT 'LIST_DASHBOARDS Test' as test_name,
       MCP.LIST_DASHBOARDS() as result;

-- ===================================================================
-- DEPLOYMENT NOTES
-- ===================================================================

/*
PRODUCTION CHECKLIST FOR DASHBOARD MANAGEMENT:

1. STAGE ACCESS VALIDATION:
   - Verify @MCP.DASH_SPECS stage is accessible
   - Test file upload/download permissions
   - Monitor stage storage usage

2. EVENT LOGGING VERIFICATION:
   - Check dashboard.created events in ACTIVITY.EVENTS
   - Verify schedule.created events are logged
   - Monitor for event deduplication

3. TIMEZONE HANDLING:
   - Test with various Olson timezone identifiers
   - Verify schedule computation accuracy
   - Monitor for DST transition issues

4. SPEC VALIDATION:
   - Test invalid panel types
   - Verify parameter clamping works
   - Test malformed JSON handling

5. SECURITY CHECKS:
   - Ensure only R_CLAUDE_AGENT can execute
   - Verify stage access is restricted
   - Test procedure parameter validation

6. PERFORMANCE MONITORING:
   - Monitor stage write operations
   - Track procedure execution times
   - Watch for JSON parsing overhead

Ready for Phase 4: Serverless Task Implementation
*/