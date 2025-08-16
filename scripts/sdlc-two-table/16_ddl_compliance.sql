-- ============================================================================
-- 16_ddl_compliance.sql
-- Compliance monitoring to detect and alert on DDL bypass attempts
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE CLAUDE_BI;
USE SCHEMA MCP;

-- ============================================================================
-- Compliance Detection Procedure
-- ============================================================================
CREATE OR REPLACE PROCEDURE MCP.DDL_COMPLIANCE_CHECK()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
COMMENT = 'Detect direct DDL operations bypassing SAFE_DDL'
AS
$$
DECLARE
  violations_found INTEGER DEFAULT 0;
  violation_details ARRAY DEFAULT ARRAY_CONSTRUCT();
BEGIN
  -- Check for direct DDL in ACCOUNT_USAGE (last 24 hours)
  -- Note: ACCOUNT_USAGE has latency, typically 45 minutes
  CREATE OR REPLACE TEMPORARY TABLE DDL_VIOLATIONS AS
  SELECT 
    QUERY_ID,
    QUERY_TEXT,
    USER_NAME,
    ROLE_NAME,
    QUERY_TAG,
    START_TIME,
    END_TIME,
    EXECUTION_STATUS,
    ERROR_MESSAGE
  FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
  WHERE START_TIME >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
    AND DATABASE_NAME = 'CLAUDE_BI'
    AND SCHEMA_NAME = 'MCP'
    AND QUERY_TYPE IN ('CREATE', 'CREATE_VIEW', 'CREATE_PROCEDURE', 'CREATE_FUNCTION',
                       'ALTER', 'ALTER_VIEW', 'ALTER_PROCEDURE', 'ALTER_FUNCTION',
                       'DROP', 'DROP_VIEW', 'DROP_PROCEDURE', 'DROP_FUNCTION',
                       'REPLACE', 'CREATE_OR_REPLACE')
    -- Exclude SAFE_DDL calls
    AND QUERY_TEXT NOT ILIKE '%CALL%SAFE_DDL%'
    AND QUERY_TEXT NOT ILIKE '%DDL_DEPLOY%'
    -- Exclude system/admin operations
    AND ROLE_NAME NOT IN ('ACCOUNTADMIN', 'SYSADMIN', 'DDL_OWNER_ROLE')
    -- Focus on agent users
    AND (QUERY_TAG LIKE 'agent:%' OR USER_NAME LIKE '%AGENT%' OR USER_NAME LIKE '%CLAUDE%');
  
  -- Count violations
  SELECT COUNT(*) INTO :violations_found FROM DDL_VIOLATIONS;
  
  IF (:violations_found > 0) THEN
    -- Collect violation details
    LET rs RESULTSET := (
      SELECT 
        OBJECT_CONSTRUCT(
          'query_id', QUERY_ID,
          'user', USER_NAME,
          'role', ROLE_NAME,
          'query_tag', QUERY_TAG,
          'time', START_TIME,
          'query_snippet', LEFT(QUERY_TEXT, 200)
        ) as violation
      FROM DDL_VIOLATIONS
      LIMIT 10
    );
    
    LET c CURSOR FOR rs;
    FOR row_var IN c DO
      SET violation_details = ARRAY_APPEND(:violation_details, row_var.violation);
    END FOR;
    
    -- Log compliance violation event
    INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS (PAYLOAD, _SOURCE_LANE, _RECV_AT)
    SELECT 
      OBJECT_CONSTRUCT(
        'event_id', UUID_STRING(),
        'action', 'ddl.compliance.violation',
        'occurred_at', CURRENT_TIMESTAMP(),
        'actor_id', 'COMPLIANCE_MONITOR',
        'source', 'DDL_COMPLIANCE_CHECK',
        'object', OBJECT_CONSTRUCT(
          'type', 'COMPLIANCE_REPORT',
          'violations', :violations_found
        ),
        'attributes', OBJECT_CONSTRUCT(
          'check_time', CURRENT_TIMESTAMP(),
          'violations_found', :violations_found,
          'violation_details', :violation_details,
          'severity', 'HIGH',
          'alert_required', true
        )
      ),
      'COMPLIANCE',
      CURRENT_TIMESTAMP();
  ELSE
    -- Log compliance success
    INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS (PAYLOAD, _SOURCE_LANE, _RECV_AT)
    SELECT 
      OBJECT_CONSTRUCT(
        'event_id', UUID_STRING(),
        'action', 'ddl.compliance.passed',
        'occurred_at', CURRENT_TIMESTAMP(),
        'actor_id', 'COMPLIANCE_MONITOR',
        'source', 'DDL_COMPLIANCE_CHECK',
        'object', OBJECT_CONSTRUCT(
          'type', 'COMPLIANCE_REPORT',
          'status', 'COMPLIANT'
        ),
        'attributes', OBJECT_CONSTRUCT(
          'check_time', CURRENT_TIMESTAMP(),
          'violations_found', 0,
          'message', 'No DDL bypass attempts detected'
        )
      ),
      'COMPLIANCE',
      CURRENT_TIMESTAMP();
  END IF;
  
  DROP TABLE IF EXISTS DDL_VIOLATIONS;
  
  RETURN OBJECT_CONSTRUCT(
    'result', IFF(:violations_found > 0, 'violations_detected', 'compliant'),
    'violations_found', :violations_found,
    'violation_details', :violation_details,
    'check_time', CURRENT_TIMESTAMP()
  );
END;
$$;

-- ============================================================================
-- Real-time Compliance Monitoring View
-- ============================================================================
CREATE OR REPLACE VIEW MCP.VW_DDL_COMPLIANCE_MONITOR AS
WITH recent_ddl AS (
  -- DDL operations from query history (has latency)
  SELECT 
    QUERY_ID,
    QUERY_TEXT,
    USER_NAME,
    ROLE_NAME,
    QUERY_TAG,
    START_TIME,
    QUERY_TYPE,
    EXECUTION_STATUS,
    CASE 
      WHEN QUERY_TEXT ILIKE '%CALL%SAFE_DDL%' THEN 'SAFE_DDL'
      WHEN ROLE_NAME IN ('ACCOUNTADMIN', 'SYSADMIN', 'DDL_OWNER_ROLE') THEN 'ADMIN'
      ELSE 'DIRECT_DDL'
    END as ddl_method
  FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
  WHERE START_TIME >= DATEADD('hour', -6, CURRENT_TIMESTAMP())
    AND DATABASE_NAME = 'CLAUDE_BI'
    AND SCHEMA_NAME = 'MCP'
    AND QUERY_TYPE IN ('CREATE', 'CREATE_VIEW', 'CREATE_PROCEDURE', 'CREATE_FUNCTION',
                       'ALTER', 'ALTER_VIEW', 'ALTER_PROCEDURE', 'ALTER_FUNCTION',
                       'DROP', 'DROP_VIEW', 'DROP_PROCEDURE', 'DROP_FUNCTION',
                       'REPLACE', 'CREATE_OR_REPLACE')
),
compliance_status AS (
  SELECT 
    USER_NAME,
    ROLE_NAME,
    ddl_method,
    COUNT(*) as operation_count,
    MAX(START_TIME) as last_operation,
    CASE 
      WHEN ddl_method = 'DIRECT_DDL' 
        AND ROLE_NAME NOT IN ('ACCOUNTADMIN', 'SYSADMIN', 'DDL_OWNER_ROLE')
        AND (QUERY_TAG LIKE 'agent:%' OR USER_NAME LIKE '%AGENT%' OR USER_NAME LIKE '%CLAUDE%')
      THEN 'VIOLATION'
      WHEN ddl_method = 'SAFE_DDL' THEN 'COMPLIANT'
      WHEN ddl_method = 'ADMIN' THEN 'AUTHORIZED'
      ELSE 'REVIEW'
    END as compliance_status
  FROM recent_ddl
  GROUP BY USER_NAME, ROLE_NAME, ddl_method
)
SELECT 
  USER_NAME,
  ROLE_NAME,
  ddl_method,
  operation_count,
  last_operation,
  compliance_status,
  CASE compliance_status
    WHEN 'VIOLATION' THEN 'ALERT: Direct DDL bypass detected!'
    WHEN 'COMPLIANT' THEN 'Good: Using SAFE_DDL'
    WHEN 'AUTHORIZED' THEN 'Admin operation'
    ELSE 'Review required'
  END as status_message
FROM compliance_status
ORDER BY 
  CASE compliance_status 
    WHEN 'VIOLATION' THEN 1 
    WHEN 'REVIEW' THEN 2
    WHEN 'COMPLIANT' THEN 3
    ELSE 4 
  END,
  last_operation DESC;

-- ============================================================================
-- Scheduled Compliance Check Task
-- ============================================================================
CREATE OR REPLACE TASK MCP.TASK_DDL_COMPLIANCE_CHECK
  WAREHOUSE = CLAUDE_AGENT_WH
  SCHEDULE = 'USING CRON 0 */4 * * * UTC'  -- Every 4 hours
  COMMENT = 'Regular compliance check for DDL bypass attempts'
AS
  CALL MCP.DDL_COMPLIANCE_CHECK();

-- Initially suspended - activate with:
-- ALTER TASK MCP.TASK_DDL_COMPLIANCE_CHECK RESUME;

-- ============================================================================
-- Alert View for Active Violations
-- ============================================================================
CREATE OR REPLACE VIEW MCP.VW_DDL_COMPLIANCE_ALERTS AS
SELECT 
  occurred_at,
  attributes:violations_found::integer as violations_found,
  attributes:severity::string as severity,
  attributes:violation_details as violation_details,
  attributes:alert_required::boolean as alert_required
FROM CLAUDE_BI.ACTIVITY.EVENTS
WHERE action = 'ddl.compliance.violation'
  AND occurred_at >= DATEADD('day', -7, CURRENT_TIMESTAMP())
ORDER BY occurred_at DESC;

-- ============================================================================
-- Compliance Dashboard View
-- ============================================================================
CREATE OR REPLACE VIEW MCP.VW_DDL_COMPLIANCE_DASHBOARD AS
WITH compliance_events AS (
  SELECT 
    DATE_TRUNC('hour', occurred_at) as check_hour,
    action,
    attributes:violations_found::integer as violations_found
  FROM CLAUDE_BI.ACTIVITY.EVENTS
  WHERE action IN ('ddl.compliance.violation', 'ddl.compliance.passed')
    AND occurred_at >= DATEADD('day', -7, CURRENT_TIMESTAMP())
),
hourly_stats AS (
  SELECT 
    check_hour,
    SUM(CASE WHEN action = 'ddl.compliance.violation' THEN violations_found ELSE 0 END) as violations,
    COUNT(CASE WHEN action = 'ddl.compliance.passed' THEN 1 END) as compliant_checks,
    COUNT(*) as total_checks
  FROM compliance_events
  GROUP BY check_hour
)
SELECT 
  check_hour,
  violations,
  compliant_checks,
  total_checks,
  ROUND(100.0 * compliant_checks / NULLIF(total_checks, 0), 2) as compliance_rate,
  CASE 
    WHEN violations > 0 THEN 'VIOLATION DETECTED'
    WHEN compliant_checks = total_checks THEN 'FULLY COMPLIANT'
    ELSE 'PARTIAL COMPLIANCE'
  END as compliance_status
FROM hourly_stats
ORDER BY check_hour DESC;

-- ============================================================================
-- Grant Permissions
-- ============================================================================
GRANT USAGE ON PROCEDURE MCP.DDL_COMPLIANCE_CHECK() TO ROLE MCP_USER_ROLE;
GRANT SELECT ON VIEW MCP.VW_DDL_COMPLIANCE_MONITOR TO ROLE MCP_USER_ROLE;
GRANT SELECT ON VIEW MCP.VW_DDL_COMPLIANCE_MONITOR TO ROLE MCP_AGENT_ROLE;
GRANT SELECT ON VIEW MCP.VW_DDL_COMPLIANCE_ALERTS TO ROLE MCP_USER_ROLE;
GRANT SELECT ON VIEW MCP.VW_DDL_COMPLIANCE_DASHBOARD TO ROLE MCP_USER_ROLE;
GRANT SELECT ON VIEW MCP.VW_DDL_COMPLIANCE_DASHBOARD TO ROLE MCP_AGENT_ROLE;

-- ============================================================================
-- Manual Compliance Check Examples
-- ============================================================================
/*
-- Run manual compliance check
CALL MCP.DDL_COMPLIANCE_CHECK();

-- View current compliance status
SELECT * FROM MCP.VW_DDL_COMPLIANCE_MONITOR;

-- Check for active alerts
SELECT * FROM MCP.VW_DDL_COMPLIANCE_ALERTS 
WHERE alert_required = true;

-- View compliance dashboard
SELECT * FROM MCP.VW_DDL_COMPLIANCE_DASHBOARD
WHERE check_hour >= DATEADD('day', -1, CURRENT_TIMESTAMP());

-- Enable automated compliance monitoring
ALTER TASK MCP.TASK_DDL_COMPLIANCE_CHECK RESUME;
*/