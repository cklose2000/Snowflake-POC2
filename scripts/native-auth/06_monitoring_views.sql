-- ============================================================================
-- 06_monitoring_views.sql
-- Production monitoring views with null coalescing and type guards
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE CLAUDE_BI;
USE SCHEMA MCP;

-- ============================================================================
-- V_CLAUDE_CODE_ACTIVITY - Activity monitoring with null safety
-- ============================================================================
CREATE OR REPLACE SECURE VIEW V_CLAUDE_CODE_ACTIVITY
AS
SELECT 
  -- Time dimensions
  DATE_TRUNC('hour', occurred_at) AS hour,
  DATE_TRUNC('day', occurred_at) AS day,
  
  -- Event details  
  action,
  COUNT(*) AS event_count,
  COUNT(DISTINCT attributes:session_id::STRING) AS unique_sessions,
  COUNT(DISTINCT actor_id) AS unique_actors,
  
  -- Performance metrics with null safety
  COALESCE(AVG(attributes:execution_time_ms::NUMBER), 0) AS avg_execution_ms,
  COALESCE(MAX(attributes:execution_time_ms::NUMBER), 0) AS max_execution_ms,
  COALESCE(MIN(attributes:execution_time_ms::NUMBER), 0) AS min_execution_ms,
  
  -- Resource usage
  COUNT(DISTINCT attributes:warehouse::STRING) AS warehouses_used,
  COUNT(DISTINCT attributes:ip::STRING) AS unique_ips,
  
  -- Event categories
  CASE 
    WHEN action LIKE 'ccode.%' THEN 'claude_code'
    WHEN action LIKE 'query.%' THEN 'query'
    WHEN action LIKE 'system.%' THEN 'system'
    WHEN action LIKE 'security.%' THEN 'security'
    WHEN action LIKE 'quality.%' THEN 'quality'
    ELSE 'other'
  END AS category,
  
  -- Latest event time
  MAX(occurred_at) AS last_event_at
  
FROM CLAUDE_BI.ACTIVITY.EVENTS
WHERE occurred_at >= DATEADD('day', -30, CURRENT_TIMESTAMP())
GROUP BY 1, 2, 3
ORDER BY 1 DESC, event_count DESC;

-- ============================================================================
-- V_CLAUDE_CODE_ERRORS - Error tracking with type guards
-- ============================================================================
CREATE OR REPLACE SECURE VIEW V_CLAUDE_CODE_ERRORS
AS
SELECT 
  occurred_at,
  attributes:session_id::STRING AS session_id,
  actor_id,
  action,
  
  -- Error details with type guards
  TRY_TO_VARCHAR(attributes:error) AS error_message,
  TRY_TO_VARCHAR(attributes:error_code) AS error_code,
  TRY_TO_VARCHAR(attributes:stack_trace) AS stack_trace,
  
  -- Context with null safety
  COALESCE(TRY_TO_VARCHAR(attributes:operation), 'unknown') AS operation,
  COALESCE(TRY_TO_VARCHAR(attributes:file_path), '') AS file_path,
  COALESCE(TRY_TO_NUMBER(attributes:line_number), 0) AS line_number,
  
  -- Metadata from attributes
  attributes:warehouse::STRING AS warehouse,
  attributes:query_tag::STRING AS query_tag,
  attributes:ip::STRING AS source_ip,
  
  -- Full attributes for debugging
  attributes AS full_attributes
  
FROM CLAUDE_BI.ACTIVITY.EVENTS
WHERE 1=1
  AND (
    action LIKE '%error%' 
    OR action LIKE '%fail%'
    OR attributes:status::STRING = 'error'
    OR attributes:success::BOOLEAN = FALSE
  )
  -- Filter out null/empty errors
  AND TRY_TO_VARCHAR(attributes:error) IS NOT NULL 
  AND TRY_TO_VARCHAR(attributes:error) != ''
ORDER BY occurred_at DESC
LIMIT 1000;

-- ============================================================================
-- V_SESSION_PERFORMANCE - Session-level performance metrics
-- ============================================================================
CREATE OR REPLACE SECURE VIEW V_SESSION_PERFORMANCE
AS
WITH session_metrics AS (
  SELECT 
    attributes:session_id::STRING AS session_id,
    MIN(occurred_at) AS session_start,
    MAX(occurred_at) AS session_end,
    COUNT(*) AS total_events,
    COUNT(DISTINCT action) AS unique_actions,
    
    -- Performance metrics with null safety
    COALESCE(AVG(CASE 
      WHEN attributes:execution_time_ms IS NOT NULL 
      THEN attributes:execution_time_ms::NUMBER 
    END), 0) AS avg_execution_ms,
    
    COALESCE(SUM(CASE 
      WHEN action LIKE '%error%' THEN 1 
      ELSE 0 
    END), 0) AS error_count,
    
    -- Resource usage
    COUNT(DISTINCT attributes:warehouse::STRING) AS warehouses_used,
    LISTAGG(DISTINCT attributes:warehouse::STRING, ', ') AS warehouse_list,
    
    -- Actor info
    MAX(actor_id) AS primary_actor
    
  FROM CLAUDE_BI.ACTIVITY.EVENTS
  WHERE attributes:session_id IS NOT NULL
    AND occurred_at >= DATEADD('day', -7, CURRENT_TIMESTAMP())
  GROUP BY attributes:session_id::STRING
)
SELECT 
  session_id,
  session_start,
  session_end,
  DATEDIFF('minute', session_start, session_end) AS duration_minutes,
  total_events,
  unique_actions,
  avg_execution_ms,
  error_count,
  COALESCE(ROUND(100.0 * error_count / NULLIF(total_events, 0), 2), 0) AS error_rate,
  warehouses_used,
  warehouse_list,
  primary_actor,
  
  -- Session status
  CASE 
    WHEN session_end < DATEADD('minute', -5, CURRENT_TIMESTAMP()) THEN 'completed'
    ELSE 'active'
  END AS session_status
  
FROM session_metrics
ORDER BY session_start DESC;

-- ============================================================================
-- V_QUERY_PATTERNS - Query pattern analysis
-- ============================================================================
CREATE OR REPLACE SECURE VIEW V_QUERY_PATTERNS
AS
SELECT 
  DATE_TRUNC('hour', occurred_at) AS hour,
  
  -- Query type extraction with null safety
  COALESCE(attributes:intent::STRING, 'unknown') AS query_intent,
  COALESCE(attributes:limit::NUMBER, 0) AS query_limit,
  
  COUNT(*) AS query_count,
  COUNT(DISTINCT attributes:session_id::STRING) AS unique_sessions,
  
  -- Performance with null safety
  COALESCE(AVG(attributes:execution_time_ms::NUMBER), 0) AS avg_query_time_ms,
  COALESCE(MAX(attributes:execution_time_ms::NUMBER), 0) AS max_query_time_ms,
  
  -- Success metrics
  SUM(CASE WHEN attributes:success::BOOLEAN = TRUE THEN 1 ELSE 0 END) AS successful_queries,
  SUM(CASE WHEN attributes:success::BOOLEAN = FALSE THEN 1 ELSE 0 END) AS failed_queries,
  
  -- Row counts with null safety
  COALESCE(AVG(attributes:row_count::NUMBER), 0) AS avg_rows_returned,
  COALESCE(MAX(attributes:row_count::NUMBER), 0) AS max_rows_returned
  
FROM CLAUDE_BI.ACTIVITY.EVENTS
WHERE action LIKE 'query.%'
  AND occurred_at >= DATEADD('day', -7, CURRENT_TIMESTAMP())
GROUP BY 1, 2, 3
ORDER BY 1 DESC, query_count DESC;

-- ============================================================================
-- V_SECURITY_EVENTS - Security-related events
-- ============================================================================
CREATE OR REPLACE SECURE VIEW V_SECURITY_EVENTS
AS
SELECT 
  occurred_at,
  action,
  actor_id,
  attributes:session_id::STRING AS session_id,
  
  -- Security details with type guards
  COALESCE(TRY_TO_VARCHAR(object_type), '') AS object_type,
  COALESCE(TRY_TO_VARCHAR(object_id), '') AS object_id,
  
  -- Authentication info
  COALESCE(TRY_TO_VARCHAR(attributes:auth_method), 'unknown') AS auth_method,
  TRY_TO_BOOLEAN(attributes:success) AS auth_success,
  TRY_TO_VARCHAR(attributes:failure_reason) AS failure_reason,
  
  -- Source info
  attributes:ip::STRING AS source_ip,
  attributes:user::STRING AS authenticated_user,
  
  -- Full context
  attributes AS full_attributes
  
FROM CLAUDE_BI.ACTIVITY.EVENTS
WHERE action LIKE 'security.%'
   OR action LIKE 'auth.%'
   OR action LIKE 'system.permission.%'
ORDER BY occurred_at DESC
LIMIT 1000;

-- ============================================================================
-- V_RESOURCE_USAGE - Resource consumption metrics
-- ============================================================================
CREATE OR REPLACE SECURE VIEW V_RESOURCE_USAGE
AS
SELECT 
  DATE_TRUNC('hour', occurred_at) AS hour,
  attributes:warehouse::STRING AS warehouse,
  
  COUNT(*) AS operation_count,
  COUNT(DISTINCT attributes:session_id::STRING) AS unique_sessions,
  COUNT(DISTINCT actor_id) AS unique_users,
  
  -- Operation breakdown
  SUM(CASE WHEN action LIKE 'ccode.%' THEN 1 ELSE 0 END) AS claude_operations,
  SUM(CASE WHEN action LIKE 'query.%' THEN 1 ELSE 0 END) AS query_operations,
  SUM(CASE WHEN action LIKE 'system.%' THEN 1 ELSE 0 END) AS system_operations,
  
  -- Performance metrics
  COALESCE(AVG(attributes:execution_time_ms::NUMBER), 0) AS avg_execution_ms,
  COALESCE(MAX(attributes:execution_time_ms::NUMBER), 0) AS peak_execution_ms,
  
  -- Latest activity
  MAX(occurred_at) AS last_activity
  
FROM CLAUDE_BI.ACTIVITY.EVENTS
WHERE attributes:warehouse IS NOT NULL
  AND occurred_at >= DATEADD('day', -7, CURRENT_TIMESTAMP())
GROUP BY 1, 2
ORDER BY 1 DESC, operation_count DESC;

-- ============================================================================
-- V_DAILY_SUMMARY - Executive daily summary
-- ============================================================================
CREATE OR REPLACE SECURE VIEW V_DAILY_SUMMARY
AS
SELECT 
  DATE_TRUNC('day', occurred_at) AS day,
  
  -- Volume metrics
  COUNT(*) AS total_events,
  COUNT(DISTINCT attributes:session_id::STRING) AS unique_sessions,
  COUNT(DISTINCT actor_id) AS unique_actors,
  
  -- Event breakdown
  SUM(CASE WHEN action LIKE 'ccode.%' THEN 1 ELSE 0 END) AS claude_events,
  SUM(CASE WHEN action LIKE 'query.%' THEN 1 ELSE 0 END) AS query_events,
  SUM(CASE WHEN action LIKE 'system.%' THEN 1 ELSE 0 END) AS system_events,
  SUM(CASE WHEN action LIKE 'security.%' THEN 1 ELSE 0 END) AS security_events,
  SUM(CASE WHEN action LIKE '%error%' THEN 1 ELSE 0 END) AS error_events,
  
  -- Performance
  COALESCE(AVG(attributes:execution_time_ms::NUMBER), 0) AS avg_execution_ms,
  COALESCE(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY attributes:execution_time_ms::NUMBER), 0) AS p95_execution_ms,
  COALESCE(MAX(attributes:execution_time_ms::NUMBER), 0) AS max_execution_ms,
  
  -- Resource usage
  COUNT(DISTINCT attributes:warehouse::STRING) AS warehouses_used,
  
  -- Data quality
  SUM(CASE WHEN attributes:session_id IS NULL THEN 1 ELSE 0 END) AS events_without_session,
  SUM(CASE WHEN actor_id IS NULL OR actor_id = 'system' THEN 1 ELSE 0 END) AS system_events_count
  
FROM CLAUDE_BI.ACTIVITY.EVENTS
WHERE occurred_at >= DATEADD('day', -90, CURRENT_TIMESTAMP())
GROUP BY 1
ORDER BY 1 DESC;

-- ============================================================================
-- Grant view access to appropriate roles
-- ============================================================================

-- Grant all monitoring views to R_APP_READ
GRANT SELECT ON VIEW V_CLAUDE_CODE_ACTIVITY TO ROLE R_APP_READ;
GRANT SELECT ON VIEW V_CLAUDE_CODE_ERRORS TO ROLE R_APP_READ;
GRANT SELECT ON VIEW V_SESSION_PERFORMANCE TO ROLE R_APP_READ;
GRANT SELECT ON VIEW V_QUERY_PATTERNS TO ROLE R_APP_READ;
GRANT SELECT ON VIEW V_SECURITY_EVENTS TO ROLE R_APP_READ;
GRANT SELECT ON VIEW V_RESOURCE_USAGE TO ROLE R_APP_READ;
GRANT SELECT ON VIEW V_DAILY_SUMMARY TO ROLE R_APP_READ;

-- Also grant to R_APP_ADMIN for monitoring
GRANT SELECT ON VIEW V_CLAUDE_CODE_ACTIVITY TO ROLE R_APP_ADMIN;
GRANT SELECT ON VIEW V_CLAUDE_CODE_ERRORS TO ROLE R_APP_ADMIN;
GRANT SELECT ON VIEW V_SESSION_PERFORMANCE TO ROLE R_APP_ADMIN;
GRANT SELECT ON VIEW V_QUERY_PATTERNS TO ROLE R_APP_ADMIN;
GRANT SELECT ON VIEW V_SECURITY_EVENTS TO ROLE R_APP_ADMIN;
GRANT SELECT ON VIEW V_RESOURCE_USAGE TO ROLE R_APP_ADMIN;
GRANT SELECT ON VIEW V_DAILY_SUMMARY TO ROLE R_APP_ADMIN;

-- ============================================================================
-- Success message
-- ============================================================================
SELECT 'Monitoring views created with null safety!' AS status,
       '7 views available for observability' AS detail,
       'All views include COALESCE and TRY_TO_* guards' AS feature;