-- ============================================================================
-- JSON Metadata Annotations for Existing Views and Procedures
-- This script adds semantic metadata to existing database objects
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE CLAUDE_BI;

-- ============================================================================
-- View Annotations: Business Intelligence Subjects
-- ============================================================================

-- Dashboard and Analytics Views
COMMENT ON VIEW MCP.V_DAILY_SUMMARY IS $${
  "subject": "daily_analytics",
  "title": "Daily Business Summary",
  "grain": "date",
  "dimensions": ["date", "metric_type", "source"],
  "measures": ["event_count", "user_count", "error_rate"],
  "time_column": "date",
  "tags": ["analytics", "summary", "daily", "business"],
  "sensitivity": "public"
}$$;

COMMENT ON VIEW MCP.V_CLAUDE_CODE_ACTIVITY IS $${
  "subject": "claude_activity",
  "title": "Claude Code Activity Stream",
  "grain": "event_id",
  "dimensions": ["actor_id", "action", "source", "occurred_at"],
  "measures": ["event_count", "session_count"],
  "time_column": "occurred_at",
  "tags": ["claude", "activity", "development", "audit"],
  "sensitivity": "public"
}$$;

COMMENT ON VIEW MCP.V_QUERY_PATTERNS IS $${
  "subject": "query_patterns",
  "title": "SQL Query Patterns Analysis",
  "grain": "query_hash",
  "dimensions": ["query_type", "warehouse", "role", "timestamp"],
  "measures": ["execution_count", "avg_duration", "total_cost"],
  "time_column": "timestamp",
  "tags": ["performance", "sql", "analysis", "cost"],
  "sensitivity": "public"
}$$;

COMMENT ON VIEW MCP.V_RESOURCE_USAGE IS $${
  "subject": "resource_usage",
  "title": "Warehouse Resource Usage",
  "grain": "warehouse_hour",
  "dimensions": ["warehouse", "hour", "role", "query_type"],
  "measures": ["compute_hours", "credits_used", "query_count"],
  "time_column": "hour",
  "tags": ["resources", "cost", "warehouse", "performance"],
  "sensitivity": "restricted"
}$$;

COMMENT ON VIEW MCP.V_SESSION_PERFORMANCE IS $${
  "subject": "session_performance",
  "title": "User Session Performance Metrics",
  "grain": "session_id",
  "dimensions": ["user_id", "role", "warehouse", "session_start"],
  "measures": ["query_count", "avg_response_time", "error_count"],
  "time_column": "session_start",
  "tags": ["performance", "sessions", "users", "monitoring"],
  "sensitivity": "restricted"
}$$;

-- SDLC and Work Management Views
COMMENT ON VIEW MCP.VW_WORK_ITEMS IS $${
  "subject": "work_items",
  "title": "SDLC Work Items Tracking",
  "grain": "work_id",
  "dimensions": ["work_id", "display_id", "status", "type", "assignee", "created_at"],
  "measures": ["story_points", "hours_logged", "days_open"],
  "time_column": "created_at",
  "tags": ["sdlc", "work", "tickets", "project-management"],
  "sensitivity": "public"
}$$;

-- System Health and Monitoring Views
COMMENT ON VIEW MCP.VW_DT_HEALTH IS $${
  "subject": "dynamic_table_health",
  "title": "Dynamic Table Health Status",
  "grain": "table_name",
  "dimensions": ["table_name", "refresh_mode", "status", "last_refresh"],
  "measures": ["lag_seconds", "refresh_count", "error_count"],
  "time_column": "last_refresh",
  "tags": ["monitoring", "dynamic-tables", "health", "infrastructure"],
  "sensitivity": "public"
}$$;

COMMENT ON VIEW MCP.PIPELINE_METRICS IS $${
  "subject": "pipeline_metrics",
  "title": "Data Pipeline Performance",
  "grain": "pipeline_run",
  "dimensions": ["pipeline_name", "stage", "status", "run_timestamp"],
  "measures": ["rows_processed", "duration_seconds", "error_count"],
  "time_column": "run_timestamp",
  "tags": ["pipelines", "etl", "performance", "monitoring"],
  "sensitivity": "public"
}$$;

-- Quality and Validation Views
COMMENT ON VIEW ACTIVITY.QUALITY_EVENTS IS $${
  "subject": "data_quality",
  "title": "Data Quality Events",
  "grain": "event_id",
  "dimensions": ["validation_type", "status", "source_table", "detected_at"],
  "measures": ["error_count", "records_affected"],
  "time_column": "detected_at",
  "tags": ["quality", "validation", "data-health", "monitoring"],
  "sensitivity": "public"
}$$;

COMMENT ON VIEW MCP.VALIDATION_FAILURES IS $${
  "subject": "validation_failures",
  "title": "Data Validation Failures",
  "grain": "failure_id",
  "dimensions": ["rule_name", "table_name", "column_name", "failure_time"],
  "measures": ["failure_count", "impact_score"],
  "time_column": "failure_time",
  "tags": ["quality", "failures", "validation", "alerts"],
  "sensitivity": "public"
}$$;

-- Security and Access Views
COMMENT ON VIEW SECURITY.DASHBOARD IS $${
  "subject": "security_dashboard",
  "title": "Security Access Dashboard",
  "grain": "access_event",
  "dimensions": ["user_id", "resource", "action", "timestamp"],
  "measures": ["access_count", "denied_count"],
  "time_column": "timestamp",
  "tags": ["security", "access", "audit", "compliance"],
  "sensitivity": "restricted"
}$$;

-- Development Activity Views
COMMENT ON VIEW MCP.VW_DEV_ACTIVITY IS $${
  "subject": "development_activity",
  "title": "Development Gateway Activity",
  "grain": "event_id",
  "dimensions": ["agent_id", "action", "namespace", "timestamp"],
  "measures": ["deployment_count", "error_count"],
  "time_column": "timestamp",
  "tags": ["development", "gateway", "deployment", "activity"],
  "sensitivity": "public"
}$$;

-- DDL and Schema Management Views
COMMENT ON VIEW MCP.VW_DDL_CATALOG IS $${
  "subject": "ddl_catalog",
  "title": "DDL Object Catalog",
  "grain": "object_name",
  "dimensions": ["object_name", "object_type", "version", "last_modified"],
  "measures": ["version_count", "change_frequency"],
  "time_column": "last_modified",
  "tags": ["ddl", "schema", "versioning", "catalog"],
  "sensitivity": "public"
}$$;

COMMENT ON VIEW MCP.VW_LATEST_SCHEMA IS $${
  "subject": "latest_schema",
  "title": "Latest Schema Versions",
  "grain": "object_name",
  "dimensions": ["object_name", "object_type", "schema_name", "version"],
  "measures": [],
  "time_column": "last_updated",
  "tags": ["schema", "versioning", "latest", "catalog"],
  "sensitivity": "public"
}$$;

-- ============================================================================
-- Procedure Annotations: Workflow Intents
-- ============================================================================

-- Dashboard and Analytics Workflows
COMMENT ON PROCEDURE MCP.DASH_GET_SERIES(VARIANT) IS $${
  "intent": "get_time_series",
  "title": "Get Time Series Data",
  "inputs": [
    {"name": "params", "type": "variant", "description": "Time series parameters including start_time, end_time, metric"}
  ],
  "outputs": [
    {"name": "series_data", "type": "array", "description": "Time series data points"}
  ],
  "tags": ["dashboard", "time-series", "analytics", "visualization"],
  "requires_secret": false,
  "min_role": "PUBLIC",
  "idempotent": true
}$$;

COMMENT ON PROCEDURE MCP.DASH_GET_METRICS(VARIANT) IS $${
  "intent": "get_metrics",
  "title": "Get Business Metrics",
  "inputs": [
    {"name": "params", "type": "variant", "description": "Metrics parameters including time range and filters"}
  ],
  "outputs": [
    {"name": "metrics", "type": "variant", "description": "Business metrics summary"}
  ],
  "tags": ["dashboard", "metrics", "kpi", "business"],
  "requires_secret": false,
  "min_role": "PUBLIC",
  "idempotent": true
}$$;

COMMENT ON PROCEDURE MCP.DASH_GET_TOPN(VARIANT) IS $${
  "intent": "get_top_rankings",
  "title": "Get Top N Rankings",
  "inputs": [
    {"name": "params", "type": "variant", "description": "Ranking parameters including metric and count"}
  ],
  "outputs": [
    {"name": "rankings", "type": "array", "description": "Ranked list of items"}
  ],
  "tags": ["dashboard", "ranking", "topn", "analytics"],
  "requires_secret": false,
  "min_role": "PUBLIC",
  "idempotent": true
}$$;

COMMENT ON PROCEDURE MCP.DASH_GET_EVENTS(VARIANT) IS $${
  "intent": "get_events",
  "title": "Get Event Stream Data",
  "inputs": [
    {"name": "params", "type": "variant", "description": "Event filters including time range and event types"}
  ],
  "outputs": [
    {"name": "events", "type": "array", "description": "Filtered event stream"}
  ],
  "tags": ["dashboard", "events", "stream", "activity"],
  "requires_secret": false,
  "min_role": "PUBLIC",
  "idempotent": true
}$$;

-- SDLC and Work Management Workflows
COMMENT ON PROCEDURE MCP.SDLC_CREATE_WORK(VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, FLOAT, BOOLEAN, VARCHAR) IS $${
  "intent": "create_work_item",
  "title": "Create SDLC Work Item",
  "inputs": [
    {"name": "title", "type": "string", "description": "Work item title"},
    {"name": "work_type", "type": "string", "description": "Type of work (bug, feature, enhancement)"},
    {"name": "severity", "type": "string", "description": "Priority level (p0, p1, p2, p3)"},
    {"name": "description", "type": "string", "description": "Detailed description"},
    {"name": "reporter_id", "type": "string", "description": "Who reported this work"},
    {"name": "business_value", "type": "number", "description": "Business value score 1-10"},
    {"name": "customer_impact", "type": "boolean", "description": "Has customer impact"},
    {"name": "idempotency_key", "type": "string", "description": "Optional idempotency key"}
  ],
  "outputs": [
    {"name": "work_id", "type": "string", "description": "Generated work ID"},
    {"name": "display_id", "type": "string", "description": "Human-readable ID (WORK-00001)"}
  ],
  "tags": ["sdlc", "work-management", "ticketing", "project"],
  "requires_secret": false,
  "min_role": "PUBLIC",
  "idempotent": true
}$$;

-- Development Gateway Workflows
COMMENT ON PROCEDURE MCP.DEV(VARCHAR, VARIANT) IS $${
  "intent": "development_gateway",
  "title": "Development Gateway Router",
  "inputs": [
    {"name": "action", "type": "string", "description": "Gateway action (claim, deploy, validate, etc.)"},
    {"name": "params", "type": "variant", "description": "Action-specific parameters"}
  ],
  "outputs": [
    {"name": "result", "type": "variant", "description": "Action result with status and details"}
  ],
  "tags": ["development", "gateway", "deployment", "automation"],
  "requires_secret": false,
  "min_role": "PUBLIC",
  "idempotent": false
}$$;

-- DDL and Schema Management Workflows
COMMENT ON PROCEDURE MCP.DDL_DEPLOY(VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR) IS $${
  "intent": "deploy_ddl",
  "title": "Deploy DDL with Versioning",
  "inputs": [
    {"name": "object_type", "type": "string", "description": "Type of object (VIEW, PROCEDURE, FUNCTION)"},
    {"name": "object_name", "type": "string", "description": "Fully qualified object name"},
    {"name": "ddl", "type": "string", "description": "DDL statement to execute"},
    {"name": "provenance", "type": "string", "description": "Who is making this change"},
    {"name": "reason", "type": "string", "description": "Why this change is being made"},
    {"name": "expected_version", "type": "string", "description": "Expected current version for optimistic locking"}
  ],
  "outputs": [
    {"name": "result", "type": "variant", "description": "Deployment result with new version"}
  ],
  "tags": ["ddl", "deployment", "versioning", "schema"],
  "requires_secret": false,
  "min_role": "SYSADMIN",
  "idempotent": false
}$$;

COMMENT ON PROCEDURE MCP.DDL_DEPLOY_FROM_STAGE(VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR) IS $${
  "intent": "deploy_ddl_from_stage",
  "title": "Deploy DDL from Stage with Checksums",
  "inputs": [
    {"name": "object_type", "type": "string", "description": "Type of object"},
    {"name": "object_name", "type": "string", "description": "Fully qualified object name"},
    {"name": "stage_url", "type": "string", "description": "Stage URL for DDL file"},
    {"name": "provenance", "type": "string", "description": "Who is deploying"},
    {"name": "reason", "type": "string", "description": "Reason for deployment"},
    {"name": "expected_version", "type": "string", "description": "Expected current version"},
    {"name": "expected_md5", "type": "string", "description": "Expected file checksum"}
  ],
  "outputs": [
    {"name": "result", "type": "variant", "description": "Deployment result with validation status"}
  ],
  "tags": ["ddl", "deployment", "stage", "checksum", "security"],
  "requires_secret": false,
  "min_role": "SYSADMIN",
  "idempotent": false
}$$;

-- Testing and Validation Workflows
COMMENT ON PROCEDURE MCP.DDL_ADD_TEST(VARCHAR, VARCHAR, VARCHAR, VARIANT) IS $${
  "intent": "add_ddl_test",
  "title": "Add Test for DDL Object",
  "inputs": [
    {"name": "object_name", "type": "string", "description": "Object to test"},
    {"name": "test_name", "type": "string", "description": "Name of the test"},
    {"name": "test_sql", "type": "string", "description": "SQL to execute for testing"},
    {"name": "expected_result", "type": "variant", "description": "Expected test result"}
  ],
  "outputs": [
    {"name": "test_id", "type": "string", "description": "Generated test ID"}
  ],
  "tags": ["testing", "ddl", "validation", "quality"],
  "requires_secret": false,
  "min_role": "SYSADMIN",
  "idempotent": true
}$$;

-- Event and Data Management Workflows
COMMENT ON PROCEDURE MCP.SAFE_INSERT_EVENT(VARIANT, VARCHAR) IS $${
  "intent": "insert_event",
  "title": "Safely Insert Event to Stream",
  "inputs": [
    {"name": "event_payload", "type": "variant", "description": "Event data to insert"},
    {"name": "source_lane", "type": "string", "description": "Source lane for the event"}
  ],
  "outputs": [
    {"name": "event_id", "type": "string", "description": "Generated event ID"}
  ],
  "tags": ["events", "insert", "stream", "data"],
  "requires_secret": false,
  "min_role": "PUBLIC",
  "idempotent": true
}$$;

COMMENT ON PROCEDURE MCP.LOG_CLAUDE_EVENT(VARIANT) IS $${
  "intent": "log_claude_activity",
  "title": "Log Claude Code Activity",
  "inputs": [
    {"name": "event_data", "type": "variant", "description": "Claude activity event data"}
  ],
  "outputs": [
    {"name": "logged", "type": "boolean", "description": "Whether event was logged successfully"}
  ],
  "tags": ["claude", "logging", "activity", "audit"],
  "requires_secret": false,
  "min_role": "PUBLIC",
  "idempotent": true
}$$;

-- Quality and Monitoring Workflows
COMMENT ON PROCEDURE MCP.EMIT_QUALITY_EVENT(VARCHAR, VARCHAR, VARIANT) IS $${
  "intent": "emit_quality_event",
  "title": "Emit Data Quality Event",
  "inputs": [
    {"name": "validation_status", "type": "string", "description": "Status of validation (pass, fail, warning)"},
    {"name": "error_message", "type": "string", "description": "Error details if validation failed"},
    {"name": "affected_payload", "type": "variant", "description": "Data that was being validated"}
  ],
  "outputs": [
    {"name": "quality_event_id", "type": "string", "description": "Generated quality event ID"}
  ],
  "tags": ["quality", "validation", "monitoring", "data-health"],
  "requires_secret": false,
  "min_role": "PUBLIC",
  "idempotent": true
}$$;

-- System Administration Workflows
COMMENT ON PROCEDURE MCP.VALIDATE_TOKEN(VARCHAR) IS $${
  "intent": "validate_access_token",
  "title": "Validate User Access Token",
  "inputs": [
    {"name": "user_token", "type": "string", "description": "Token to validate"}
  ],
  "outputs": [
    {"name": "valid", "type": "boolean", "description": "Whether token is valid"},
    {"name": "user_info", "type": "variant", "description": "User information if valid"}
  ],
  "tags": ["security", "authentication", "tokens", "access"],
  "requires_secret": true,
  "min_role": "SYSADMIN",
  "idempotent": true
}$$;

-- Schema Discovery and Documentation Workflows
COMMENT ON PROCEDURE MCP.PUBLISH_SCHEMA_SNAPSHOT(BOOLEAN, VARCHAR) IS $${
  "intent": "publish_schema_snapshot",
  "title": "Publish Schema Documentation Snapshot",
  "inputs": [
    {"name": "include_ddl", "type": "boolean", "description": "Include DDL definitions"},
    {"name": "schema_filter", "type": "string", "description": "Schema name filter"}
  ],
  "outputs": [
    {"name": "snapshot_id", "type": "string", "description": "Generated snapshot ID"},
    {"name": "objects_included", "type": "number", "description": "Number of objects documented"}
  ],
  "tags": ["schema", "documentation", "snapshot", "discovery"],
  "requires_secret": false,
  "min_role": "PUBLIC",
  "idempotent": true
}$$;

-- ============================================================================
-- Verification: Check Annotations
-- ============================================================================

-- Show annotated views
SELECT 
  table_schema,
  table_name,
  CASE 
    WHEN TRY_PARSE_JSON(comment) IS NOT NULL THEN '✅ Valid JSON'
    WHEN comment IS NOT NULL THEN '⚠️ Has comment but invalid JSON'
    ELSE '❌ No annotation'
  END AS annotation_status,
  TRY_PARSE_JSON(comment):subject::STRING AS subject,
  TRY_PARSE_JSON(comment):title::STRING AS title
FROM INFORMATION_SCHEMA.VIEWS
WHERE table_schema IN ('MCP', 'ACTIVITY', 'SECURITY')
ORDER BY table_schema, table_name;

-- Show annotated procedures
SELECT 
  procedure_schema,
  procedure_name,
  CASE 
    WHEN TRY_PARSE_JSON(comment) IS NOT NULL THEN '✅ Valid JSON'
    WHEN comment IS NOT NULL THEN '⚠️ Has comment but invalid JSON'
    ELSE '❌ No annotation'
  END AS annotation_status,
  TRY_PARSE_JSON(comment):intent::STRING AS intent,
  TRY_PARSE_JSON(comment):title::STRING AS title
FROM INFORMATION_SCHEMA.PROCEDURES
WHERE procedure_schema = 'MCP'
ORDER BY procedure_name;