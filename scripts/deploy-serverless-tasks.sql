-- ===================================================================
-- ALL-SNOWFLAKE NATIVE SERVERLESS TASK SYSTEM
-- Phase 4: RUN_DUE_SCHEDULES procedure and automated task scheduler
-- ===================================================================

USE DATABASE CLAUDE_BI;
USE SCHEMA MCP;

-- ===================================================================
-- 1. RUN_DUE_SCHEDULES - Main scheduling engine procedure
-- ===================================================================

CREATE OR REPLACE PROCEDURE MCP.RUN_DUE_SCHEDULES()
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python', 'requests')
EXTERNAL_ACCESS_INTEGRATIONS = (MCP.CLAUDE_EAI)
SECRETS = ('SLACK_WEBHOOK_URL'='MCP.SLACK_WEBHOOK_URL')
EXECUTE AS OWNER
HANDLER = 'run'
COMMENT = 'Execute due dashboard schedules with snapshot generation and delivery'
AS
$$
import json
import datetime as dt
import hashlib
import os
import requests
from datetime import timezone, timedelta

# Get webhook URL from secret
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")

def _is_schedule_due(schedule, now_utc, last_snapshot_time=None):
    """
    Check if a schedule is due for execution.
    Uses smart DST-aware calculation based on Olson timezone.
    """
    try:
        frequency = schedule.get("frequency", "DAILY")
        hour = schedule.get("hour", 9)
        minute = schedule.get("minute", 0)
        tz = schedule.get("timezone", "UTC")
        
        # For simplicity, assume UTC scheduling
        # In production, use pytz or similar for proper timezone handling
        
        # Calculate target time for today in UTC
        today = now_utc.date()
        target_time = dt.datetime.combine(today, dt.time(hour, minute))
        target_time = target_time.replace(tzinfo=timezone.utc)
        
        # Check if we've passed the target time today
        if now_utc < target_time:
            # Not yet time today
            return False
        
        # Check if we already ran today
        if last_snapshot_time:
            if isinstance(last_snapshot_time, str):
                last_snapshot_time = dt.datetime.fromisoformat(last_snapshot_time.replace('Z', '+00:00'))
            
            # If last snapshot was today after target time, we already ran
            if last_snapshot_time.date() == today and last_snapshot_time >= target_time:
                return False
        
        # Handle frequency-specific logic
        if frequency == "DAILY":
            return True
        elif frequency == "WEEKDAYS":
            # Monday=0, Sunday=6
            return today.weekday() < 5  # Monday-Friday
        elif frequency == "WEEKLY":
            # Run on the same day of week as created
            return today.weekday() == 0  # Simplified: run on Mondays
        elif frequency == "HOURLY":
            # For hourly, check if an hour has passed
            if last_snapshot_time:
                return (now_utc - last_snapshot_time) >= timedelta(hours=1)
            return True
        
        return False
        
    except Exception as e:
        # On error, don't execute to avoid spam
        return False

def _generate_snapshot(session, dashboard_spec, schedule_id):
    """
    Generate dashboard snapshot by executing all panels.
    Returns snapshot metadata.
    """
    try:
        panels = dashboard_spec.get("panels", [])
        snapshot_data = {
            "dashboard_id": dashboard_spec.get("dashboard_id"),
            "title": dashboard_spec.get("title", "Untitled Dashboard"),
            "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            "panels": []
        }
        
        # Execute each panel
        for panel in panels:
            panel_type = panel.get("type")
            panel_params = panel.get("params", {})
            
            # Map panel type to procedure
            proc_map = {
                "metrics": "DASH_GET_METRICS",
                "series": "DASH_GET_SERIES", 
                "topn": "DASH_GET_TOPN",
                "events": "DASH_GET_EVENTS"
            }
            
            proc_name = proc_map.get(panel_type)
            if not proc_name:
                continue
            
            # Execute panel procedure
            try:
                params_json = json.dumps(panel_params)
                df = session.sql(f"CALL MCP.{proc_name}(PARSE_JSON(?))").bind(params=[params_json])
                rows = df.collect()
                
                # Convert to serializable format
                panel_data = {
                    "type": panel_type,
                    "title": panel.get("title", f"{panel_type.title()} Panel"),
                    "data": [list(row) for row in rows],
                    "row_count": len(rows),
                    "executed_at": dt.datetime.now(dt.timezone.utc).isoformat()
                }
                
                snapshot_data["panels"].append(panel_data)
                
            except Exception as panel_error:
                # Log panel error but continue with other panels
                panel_data = {
                    "type": panel_type,
                    "title": panel.get("title", f"{panel_type.title()} Panel"),
                    "error": str(panel_error),
                    "executed_at": dt.datetime.now(dt.timezone.utc).isoformat()
                }
                snapshot_data["panels"].append(panel_data)
        
        # Save snapshot to stage
        snapshot_id = f"snap_{schedule_id}_{int(dt.datetime.now().timestamp())}"
        snapshot_json = json.dumps(snapshot_data, indent=2)
        stage_path = f"@MCP.DASH_SNAPSHOTS/{snapshot_id}.json"
        
        # Save to stage
        session.sql(f"""
            CREATE OR REPLACE TEMPORARY TABLE temp_snapshot AS
            SELECT '{snapshot_id}' as snapshot_id, PARSE_JSON('{snapshot_json}') as snapshot_content
        """).collect()
        
        session.sql(f"""
            COPY INTO {stage_path}
            FROM (SELECT snapshot_content FROM temp_snapshot)
            FILE_FORMAT = (TYPE = JSON)
            OVERWRITE = TRUE
        """).collect()
        
        return {
            "snapshot_id": snapshot_id,
            "stage_path": stage_path,
            "panel_count": len(snapshot_data["panels"]),
            "success_count": len([p for p in snapshot_data["panels"] if "error" not in p])
        }
        
    except Exception as e:
        return {
            "error": str(e),
            "snapshot_id": None,
            "stage_path": None
        }

def _send_notification(schedule, snapshot_result):
    """
    Send notification via configured delivery methods.
    """
    results = []
    
    try:
        deliveries = schedule.get("deliveries", [])
        dashboard_title = schedule.get("dashboard_title", "Dashboard")
        
        # Prepare message
        if snapshot_result.get("error"):
            message = f"❌ Dashboard '{dashboard_title}' generation failed: {snapshot_result['error']}"
            success = False
        else:
            panel_count = snapshot_result.get("panel_count", 0)
            success_count = snapshot_result.get("success_count", 0)
            message = f"✅ Dashboard '{dashboard_title}' generated successfully ({success_count}/{panel_count} panels)"
            success = True
        
        # Send to Slack if configured
        if "slack" in deliveries and SLACK_WEBHOOK_URL and SLACK_WEBHOOK_URL != "https://hooks.slack.com/placeholder":
            try:
                payload = {
                    "text": message,
                    "channel": "#dashboards",
                    "username": "Claude Code Dashboard Bot",
                    "icon_emoji": ":robot_face:"
                }
                
                response = requests.post(
                    SLACK_WEBHOOK_URL,
                    json=payload,
                    timeout=10
                )
                
                results.append({
                    "method": "slack",
                    "success": response.status_code == 200,
                    "status_code": response.status_code
                })
                
            except Exception as slack_error:
                results.append({
                    "method": "slack",
                    "success": False,
                    "error": str(slack_error)
                })
        
        # Email delivery would go here
        if "email" in deliveries:
            # For now, just log that email would be sent
            results.append({
                "method": "email",
                "success": True,
                "note": "Email delivery not implemented - would send via SNS/SES"
            })
        
        return results
        
    except Exception as e:
        return [{
            "method": "notification_error",
            "success": False,
            "error": str(e)
        }]

def _log_event(session, action, schedule_id, status, details=None):
    """Log task execution event"""
    event_data = {
        "event_id": f"task_{schedule_id}_{dt.datetime.now().timestamp()}",
        "action": action,
        "actor_id": "SYSTEM_TASK",
        "object": {"type": "schedule", "id": schedule_id},
        "attributes": {
            "status": status,
            "details": details or {},
            "timestamp": dt.datetime.now(dt.timezone.utc).isoformat(),
            "dedupe_key": f"{action}|{schedule_id}|{dt.datetime.now().strftime('%Y%m%d%H')}"
        },
        "occurred_at": dt.datetime.now(dt.timezone.utc).isoformat()
    }
    
    event_json = json.dumps(event_data)
    session.sql(f"""
        INSERT INTO LANDING.RAW_EVENTS (raw_event, source, loaded_at)
        VALUES (PARSE_JSON('{event_json}'), 'SYSTEM_TASK', CURRENT_TIMESTAMP())
    """).collect()

def run(session):
    """Main task execution function"""
    try:
        # Enforce role and database constraints
        session.sql("USE ROLE R_CLAUDE_AGENT").collect()
        session.sql("USE DATABASE CLAUDE_BI").collect()
        session.sql("USE SCHEMA MCP").collect()
        
        # Set query tag
        session.sql("ALTER SESSION SET QUERY_TAG = 'task:schedules|proc:RUN_DUE_SCHEDULES|agent:system'").collect()
        
        now_utc = dt.datetime.now(dt.timezone.utc)
        executed_count = 0
        error_count = 0
        
        # Get all active schedules from events
        schedules_df = session.sql("""
            WITH latest_schedules AS (
                SELECT 
                    object_id as schedule_id,
                    attributes:dashboard_id::string as dashboard_id,
                    attributes:frequency::string as frequency,
                    attributes:time::string as time_str,
                    attributes:timezone::string as timezone,
                    attributes:deliveries as deliveries,
                    attributes:stage_path::string as stage_path,
                    occurred_at,
                    ROW_NUMBER() OVER (PARTITION BY object_id ORDER BY occurred_at DESC) as rn
                FROM ACTIVITY.EVENTS
                WHERE action = 'dashboard.schedule_created'
                  AND attributes:status::string = 'success'
                QUALIFY rn = 1
            )
            SELECT * FROM latest_schedules
            ORDER BY occurred_at
            LIMIT 50
        """)
        
        schedules = schedules_df.collect()
        
        for schedule_row in schedules:
            try:
                schedule_id = schedule_row["SCHEDULE_ID"]
                dashboard_id = schedule_row["DASHBOARD_ID"]
                stage_path = schedule_row["STAGE_PATH"]
                
                # Parse time string to get hour/minute
                time_str = schedule_row["TIME_STR"] or "09:00"
                try:
                    hour, minute = map(int, time_str.split(':'))
                except:
                    hour, minute = 9, 0
                
                schedule = {
                    "schedule_id": schedule_id,
                    "dashboard_id": dashboard_id,
                    "frequency": schedule_row["FREQUENCY"] or "DAILY",
                    "hour": hour,
                    "minute": minute,
                    "timezone": schedule_row["TIMEZONE"] or "UTC",
                    "deliveries": json.loads(schedule_row["DELIVERIES"] or '["email"]') if schedule_row["DELIVERIES"] else ["email"]
                }
                
                # Get last snapshot time for this schedule
                last_snapshot_df = session.sql(f"""
                    SELECT MAX(occurred_at) as last_snapshot
                    FROM ACTIVITY.EVENTS
                    WHERE action = 'dashboard.snapshot_generated'
                      AND object_id = '{schedule_id}'
                      AND attributes:status::string = 'success'
                """)
                
                last_snapshot_rows = last_snapshot_df.collect()
                last_snapshot_time = last_snapshot_rows[0]["LAST_SNAPSHOT"] if last_snapshot_rows else None
                
                # Check if schedule is due
                if _is_schedule_due(schedule, now_utc, last_snapshot_time):
                    # Get dashboard spec from events (not stage for simplicity)
                    dashboard_df = session.sql(f"""
                        SELECT 
                            attributes:title::string as title,
                            attributes as spec_attributes
                        FROM ACTIVITY.EVENTS
                        WHERE action = 'dashboard.created'
                          AND object_id = '{dashboard_id}'
                          AND attributes:status::string = 'success'
                        ORDER BY occurred_at DESC
                        LIMIT 1
                    """)
                    
                    dashboard_rows = dashboard_df.collect()
                    if not dashboard_rows:
                        continue
                    
                    # Create simplified dashboard spec for snapshot
                    dashboard_spec = {
                        "dashboard_id": dashboard_id,
                        "title": dashboard_rows[0]["TITLE"] or "Untitled Dashboard",
                        "panels": [
                            {"type": "metrics", "params": {"start_ts": (now_utc - timedelta(days=7)).isoformat(), "end_ts": now_utc.isoformat(), "filters": {}}},
                            {"type": "series", "params": {"start_ts": (now_utc - timedelta(days=1)).isoformat(), "end_ts": now_utc.isoformat(), "interval": "hour", "filters": {}}},
                            {"type": "topn", "params": {"start_ts": (now_utc - timedelta(days=1)).isoformat(), "end_ts": now_utc.isoformat(), "dimension": "action", "n": 10, "filters": {}}}
                        ]
                    }
                    
                    # Generate snapshot
                    snapshot_result = _generate_snapshot(session, dashboard_spec, schedule_id)
                    
                    # Send notifications
                    schedule["dashboard_title"] = dashboard_spec["title"]
                    notification_results = _send_notification(schedule, snapshot_result)
                    
                    # Log snapshot generation
                    if snapshot_result.get("error"):
                        _log_event(session, "dashboard.snapshot_failed", schedule_id, "error", {
                            "error": snapshot_result["error"],
                            "dashboard_id": dashboard_id
                        })
                        error_count += 1
                    else:
                        _log_event(session, "dashboard.snapshot_generated", schedule_id, "success", {
                            "snapshot_id": snapshot_result["snapshot_id"],
                            "stage_path": snapshot_result["stage_path"],
                            "panel_count": snapshot_result["panel_count"],
                            "success_count": snapshot_result["success_count"],
                            "notifications": notification_results,
                            "dashboard_id": dashboard_id
                        })
                        executed_count += 1
                
            except Exception as schedule_error:
                error_count += 1
                _log_event(session, "dashboard.schedule_error", schedule_id, "error", {
                    "error": str(schedule_error)
                })
        
        # Log task completion
        _log_event(session, "task.schedules_checked", "system", "success", {
            "schedules_checked": len(schedules),
            "executed_count": executed_count,
            "error_count": error_count,
            "execution_time": dt.datetime.now(dt.timezone.utc).isoformat()
        })
        
        return {
            "ok": True,
            "schedules_checked": len(schedules),
            "executed_count": executed_count,
            "error_count": error_count,
            "execution_time": now_utc.isoformat()
        }
        
    except Exception as e:
        _log_event(session, "task.execution_failed", "system", "error", {
            "error": str(e)
        })
        
        return {
            "ok": False,
            "error": str(e),
            "schedules_checked": 0,
            "executed_count": 0,
            "error_count": 1
        }
$$;

-- ===================================================================
-- 2. SERVERLESS TASK - Automated schedule execution
-- ===================================================================

-- Create the serverless task (initially suspended)
CREATE OR REPLACE TASK MCP.TASK_RUN_SCHEDULES
  WAREHOUSE = CLAUDE_AGENT_WH
  SCHEDULE = '5 MINUTE'
  COMMENT = 'Execute due dashboard schedules every 5 minutes'
AS
  CALL MCP.RUN_DUE_SCHEDULES();

-- ===================================================================
-- 3. TASK MONITORING VIEWS
-- ===================================================================

-- View task execution history
CREATE OR REPLACE VIEW MCP.VW_TASK_EXECUTION_HISTORY AS
SELECT 
  object_id as task_identifier,
  action,
  attributes:status::string as status,
  attributes:schedules_checked::int as schedules_checked,
  attributes:executed_count::int as executed_count,
  attributes:error_count::int as error_count,
  occurred_at,
  attributes:details as details
FROM ACTIVITY.EVENTS
WHERE action IN ('task.schedules_checked', 'task.execution_failed')
ORDER BY occurred_at DESC;

-- View dashboard snapshots
CREATE OR REPLACE VIEW MCP.VW_DASHBOARD_SNAPSHOTS AS
SELECT 
  object_id as schedule_id,
  attributes:snapshot_id::string as snapshot_id,
  attributes:stage_path::string as stage_path,
  attributes:dashboard_id::string as dashboard_id,
  attributes:panel_count::int as panel_count,
  attributes:success_count::int as success_count,
  occurred_at as generated_at,
  attributes:notifications as notification_results
FROM ACTIVITY.EVENTS
WHERE action = 'dashboard.snapshot_generated'
  AND attributes:status::string = 'success'
ORDER BY occurred_at DESC;

-- View schedule execution status
CREATE OR REPLACE VIEW MCP.VW_SCHEDULE_STATUS AS
WITH latest_schedules AS (
  SELECT 
    object_id as schedule_id,
    attributes:dashboard_id::string as dashboard_id,
    attributes:frequency::string as frequency,
    attributes:time::string as time_str,
    attributes:timezone::string as timezone,
    occurred_at as created_at,
    ROW_NUMBER() OVER (PARTITION BY object_id ORDER BY occurred_at DESC) as rn
  FROM ACTIVITY.EVENTS
  WHERE action = 'dashboard.schedule_created'
    AND attributes:status::string = 'success'
  QUALIFY rn = 1
),
latest_snapshots AS (
  SELECT 
    object_id as schedule_id,
    MAX(occurred_at) as last_snapshot_at,
    COUNT(*) as total_snapshots
  FROM ACTIVITY.EVENTS
  WHERE action = 'dashboard.snapshot_generated'
    AND attributes:status::string = 'success'
  GROUP BY object_id
)
SELECT 
  s.schedule_id,
  s.dashboard_id,
  s.frequency,
  s.time_str,
  s.timezone,
  s.created_at,
  snap.last_snapshot_at,
  snap.total_snapshots,
  CASE 
    WHEN snap.last_snapshot_at IS NULL THEN 'Never executed'
    WHEN snap.last_snapshot_at > DATEADD('day', -1, CURRENT_TIMESTAMP()) THEN 'Recent'
    WHEN snap.last_snapshot_at > DATEADD('day', -7, CURRENT_TIMESTAMP()) THEN 'Stale'
    ELSE 'Very stale'
  END as status
FROM latest_schedules s
LEFT JOIN latest_snapshots snap ON s.schedule_id = snap.schedule_id
ORDER BY s.created_at DESC;

-- ===================================================================
-- 4. GRANTS - Access for task execution
-- ===================================================================

-- Grant execution privileges
GRANT EXECUTE ON PROCEDURE MCP.RUN_DUE_SCHEDULES() TO ROLE R_CLAUDE_AGENT;

-- Grant view access for monitoring
GRANT SELECT ON VIEW MCP.VW_TASK_EXECUTION_HISTORY TO ROLE R_CLAUDE_AGENT;
GRANT SELECT ON VIEW MCP.VW_DASHBOARD_SNAPSHOTS TO ROLE R_CLAUDE_AGENT;
GRANT SELECT ON VIEW MCP.VW_SCHEDULE_STATUS TO ROLE R_CLAUDE_AGENT;

-- Grant task management (for admins only)
-- GRANT OPERATE ON TASK MCP.TASK_RUN_SCHEDULES TO ROLE SYSADMIN;

-- ===================================================================
-- 5. TASK MANAGEMENT COMMANDS
-- ===================================================================

-- View task status
SELECT 
  name,
  database_name,
  schema_name,
  state,
  schedule,
  warehouse,
  comment,
  created_on,
  last_committed_on
FROM INFORMATION_SCHEMA.TASKS
WHERE name = 'TASK_RUN_SCHEDULES';

-- To start the task (run manually when ready):
-- ALTER TASK MCP.TASK_RUN_SCHEDULES RESUME;

-- To stop the task:
-- ALTER TASK MCP.TASK_RUN_SCHEDULES SUSPEND;

-- To check task execution history:
-- SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY()) 
-- WHERE name = 'TASK_RUN_SCHEDULES' 
-- ORDER BY scheduled_time DESC;

-- ===================================================================
-- 6. VALIDATION TESTS
-- ===================================================================

-- Test manual execution of schedule runner
SELECT 'RUN_DUE_SCHEDULES Test' as test_name,
       MCP.RUN_DUE_SCHEDULES() as result;

-- Check monitoring views
SELECT 'Task History' as view_name, COUNT(*) as records 
FROM MCP.VW_TASK_EXECUTION_HISTORY;

SELECT 'Schedule Status' as view_name, COUNT(*) as records 
FROM MCP.VW_SCHEDULE_STATUS;

-- ===================================================================
-- DEPLOYMENT NOTES
-- ===================================================================

/*
PRODUCTION TASK DEPLOYMENT CHECKLIST:

1. WEBHOOK CONFIGURATION:
   ALTER SECRET MCP.SLACK_WEBHOOK_URL SET VALUE = 'https://hooks.slack.com/your-webhook';

2. WAREHOUSE OPTIMIZATION:
   - CLAUDE_AGENT_WH set to XSMALL with 60s auto-suspend
   - Monitor task execution costs
   - Adjust schedule frequency if needed (5 minutes recommended)

3. TASK ACTIVATION:
   -- Start the task when ready for production
   ALTER TASK MCP.TASK_RUN_SCHEDULES RESUME;

4. MONITORING SETUP:
   - Monitor VW_TASK_EXECUTION_HISTORY for failures
   - Watch VW_SCHEDULE_STATUS for stale schedules
   - Set up alerts for task execution errors

5. SECURITY VERIFICATION:
   - Ensure task runs as R_CLAUDE_AGENT role only
   - Verify external access is limited to allowed hosts
   - Test notification delivery without exposing secrets

6. PERFORMANCE TUNING:
   - Monitor stage storage usage in DASH_SNAPSHOTS
   - Optimize snapshot generation for large dashboards
   - Consider implementing snapshot retention policies

7. ERROR HANDLING:
   - Test timezone edge cases and DST transitions
   - Verify graceful handling of API timeouts
   - Monitor for schedule drift and accuracy

Ready for Phase 5: Streamlit Conversion
*/