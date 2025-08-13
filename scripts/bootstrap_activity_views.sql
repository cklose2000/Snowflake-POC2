-- Bootstrap Activity Views for Dashboard Factory
-- Creates typed views in ACTIVITY_CCODE schema for Activity-native dashboards
-- These views are the ONLY data source for v1 dashboards (no fake tables)

-- Note: Replace {{DB}} with actual database name during deployment (e.g., CLAUDE_BI)

-- Set context
USE DATABASE CLAUDE_BI;
USE SCHEMA ACTIVITY_CCODE;

-- =============================================================================
-- 1. Activity counts by customer/type (24h fixed window)
-- =============================================================================
CREATE OR REPLACE VIEW ACTIVITY_CCODE.VW_ACTIVITY_COUNTS_24H AS
SELECT
  e.activity,
  e.customer,
  COUNT(*) AS events_24h,
  MIN(e.ts) AS first_seen,
  MAX(e.ts) AS last_seen
FROM CLAUDE_BI.ACTIVITY.EVENTS e
WHERE e.ts >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
GROUP BY e.activity, e.customer
ORDER BY events_24h DESC;

COMMENT ON VIEW ACTIVITY_CCODE.VW_ACTIVITY_COUNTS_24H IS 
  'Activity counts by type and customer for last 24 hours. Fixed window, no parameters.';

-- =============================================================================
-- 2. LLM telemetry (tokens, latency, model usage)
-- =============================================================================
CREATE OR REPLACE VIEW ACTIVITY_CCODE.VW_LLM_TELEMETRY AS
SELECT
  e.customer,
  e.activity,
  e.ts,
  e._feature_json:model::STRING AS model,
  TRY_TO_NUMBER(e._feature_json:prompt_tokens) AS prompt_tokens,
  TRY_TO_NUMBER(e._feature_json:completion_tokens) AS completion_tokens,
  TRY_TO_NUMBER(e._feature_json:total_tokens) AS total_tokens,
  TRY_TO_NUMBER(e._feature_json:latency_ms) AS latency_ms,
  e._feature_json:template::STRING AS template_used,
  e._session_id AS session_id
FROM CLAUDE_BI.ACTIVITY.EVENTS e
WHERE e.activity IN ('ccode.user_asked', 'ccode.claude_responded', 'ccode.llm_invoked')
  AND e.ts >= DATEADD('day', -7, CURRENT_TIMESTAMP())
ORDER BY e.ts DESC;

COMMENT ON VIEW ACTIVITY_CCODE.VW_LLM_TELEMETRY IS 
  'LLM usage telemetry including tokens, latency, and model. 7-day window.';

-- =============================================================================
-- 3. SQL executions with cost and performance metrics
-- =============================================================================
CREATE OR REPLACE VIEW ACTIVITY_CCODE.VW_SQL_EXECUTIONS AS
WITH sql_events AS (
  SELECT
    e.customer,
    e.ts,
    e._feature_json:query_id::STRING AS query_id,
    e._query_tag AS query_tag,
    e._feature_json:template::STRING AS template,
    e._feature_json:row_count::NUMBER AS row_count,
    e._session_id AS session_id
  FROM CLAUDE_BI.ACTIVITY.EVENTS e
  WHERE e.activity = 'ccode.sql_executed'
    AND e.ts >= DATEADD('day', -7, CURRENT_TIMESTAMP())
)
SELECT
  se.customer,
  se.ts,
  se.query_id,
  se.query_tag,
  se.template,
  se.row_count,
  se.session_id,
  -- Note: Query history join requires appropriate privileges
  -- If QUERY_ID not available, falls back to NULL for these metrics
  qh.BYTES_SCANNED AS bytes_scanned,
  qh.TOTAL_ELAPSED_TIME AS duration_ms,
  qh.CREDITS_USED_CLOUD_SERVICES AS credits_used,
  (qh.ERROR_MESSAGE IS NULL) AS success,
  qh.ERROR_MESSAGE AS error_message
FROM sql_events se
LEFT JOIN TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(
  END_TIME_RANGE_START => DATEADD('day', -7, CURRENT_TIMESTAMP()),
  END_TIME_RANGE_END => CURRENT_TIMESTAMP()
)) qh
  ON qh.QUERY_ID = se.query_id
ORDER BY se.ts DESC;

COMMENT ON VIEW ACTIVITY_CCODE.VW_SQL_EXECUTIONS IS 
  'SQL execution telemetry with cost and performance metrics. Joins on QUERY_ID when available. 7-day window.';

-- =============================================================================
-- 4. Dashboard operations (create/refresh/destroy lifecycle)
-- =============================================================================
CREATE OR REPLACE VIEW ACTIVITY_CCODE.VW_DASHBOARD_OPERATIONS AS
SELECT
  e.activity,
  e.customer,
  e.ts,
  e.link AS streamlit_url,
  e._feature_json:spec_id::STRING AS spec_id,
  e._feature_json:panels::NUMBER AS panel_count,
  e._feature_json:schedule::STRING AS schedule_mode,
  e._feature_json:creation_time_ms::NUMBER AS creation_time_ms,
  e._feature_json:error::STRING AS error_message,
  e._session_id AS session_id,
  -- Calculate time since last refresh for monitoring
  LAG(e.ts) OVER (PARTITION BY e._feature_json:spec_id ORDER BY e.ts) AS previous_operation_ts,
  DATEDIFF('minute', LAG(e.ts) OVER (PARTITION BY e._feature_json:spec_id ORDER BY e.ts), e.ts) AS minutes_since_last_op
FROM CLAUDE_BI.ACTIVITY.EVENTS e
WHERE e.activity IN (
  'ccode.dashboard_created',
  'ccode.dashboard_refreshed',
  'ccode.dashboard_destroyed',
  'ccode.dashboard_failed'
)
ORDER BY e.ts DESC;

COMMENT ON VIEW ACTIVITY_CCODE.VW_DASHBOARD_OPERATIONS IS 
  'Dashboard lifecycle events including create, refresh, destroy operations. All time.';

-- =============================================================================
-- 5. SafeSQL template usage patterns
-- =============================================================================
CREATE OR REPLACE VIEW ACTIVITY_CCODE.VW_SAFESQL_TEMPLATES AS
SELECT
  e.customer,
  e.ts,
  e._feature_json:template::STRING AS template,
  e._feature_json:params::VARIANT AS params,
  e._feature_json:row_count::NUMBER AS rows_returned,
  e._feature_json:execution_time_ms::NUMBER AS execution_time_ms,
  -- Running count of template usage
  COUNT(*) OVER (
    PARTITION BY e._feature_json:template::STRING
    ORDER BY e.ts
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS template_running_count,
  -- Daily usage rank
  DENSE_RANK() OVER (
    PARTITION BY DATE_TRUNC('day', e.ts)
    ORDER BY COUNT(*) OVER (
      PARTITION BY DATE_TRUNC('day', e.ts), e._feature_json:template::STRING
    ) DESC
  ) AS daily_usage_rank
FROM CLAUDE_BI.ACTIVITY.EVENTS e
WHERE e.activity = 'ccode.sql_executed'
  AND e._feature_json:template IS NOT NULL
  AND e.ts >= DATEADD('day', -30, CURRENT_TIMESTAMP())
ORDER BY e.ts DESC;

COMMENT ON VIEW ACTIVITY_CCODE.VW_SAFESQL_TEMPLATES IS 
  'SafeSQL template usage patterns with running counts and daily rankings. 30-day window.';

-- =============================================================================
-- 6. BONUS: Activity Summary (high-level metrics for overview panel)
-- =============================================================================
CREATE OR REPLACE VIEW ACTIVITY_CCODE.VW_ACTIVITY_SUMMARY AS
WITH hourly_stats AS (
  SELECT
    DATE_TRUNC('hour', ts) AS hour,
    COUNT(*) AS events_per_hour,
    COUNT(DISTINCT customer) AS unique_customers_per_hour,
    COUNT(DISTINCT activity) AS unique_activities_per_hour
  FROM CLAUDE_BI.ACTIVITY.EVENTS
  WHERE ts >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
  GROUP BY 1
)
SELECT
  CURRENT_TIMESTAMP() AS as_of_time,
  -- 24h totals
  (SELECT COUNT(*) FROM CLAUDE_BI.ACTIVITY.EVENTS WHERE ts >= DATEADD('hour', -24, CURRENT_TIMESTAMP())) AS total_events_24h,
  (SELECT COUNT(DISTINCT customer) FROM CLAUDE_BI.ACTIVITY.EVENTS WHERE ts >= DATEADD('hour', -24, CURRENT_TIMESTAMP())) AS unique_customers_24h,
  (SELECT COUNT(DISTINCT activity) FROM CLAUDE_BI.ACTIVITY.EVENTS WHERE ts >= DATEADD('hour', -24, CURRENT_TIMESTAMP())) AS unique_activities_24h,
  -- Peak hour
  (SELECT MAX(events_per_hour) FROM hourly_stats) AS peak_events_per_hour,
  (SELECT hour FROM hourly_stats WHERE events_per_hour = (SELECT MAX(events_per_hour) FROM hourly_stats) LIMIT 1) AS peak_hour,
  -- Most active customer
  (SELECT customer FROM CLAUDE_BI.ACTIVITY.EVENTS 
   WHERE ts >= DATEADD('hour', -24, CURRENT_TIMESTAMP()) 
   GROUP BY customer 
   ORDER BY COUNT(*) DESC 
   LIMIT 1) AS most_active_customer,
  -- Most common activity
  (SELECT activity FROM CLAUDE_BI.ACTIVITY.EVENTS 
   WHERE ts >= DATEADD('hour', -24, CURRENT_TIMESTAMP()) 
   GROUP BY activity 
   ORDER BY COUNT(*) DESC 
   LIMIT 1) AS most_common_activity;

COMMENT ON VIEW ACTIVITY_CCODE.VW_ACTIVITY_SUMMARY IS 
  'High-level activity metrics for overview dashboard panel. Single row, 24h window.';

-- =============================================================================
-- Validation: Ensure views are created and have expected structure
-- =============================================================================
SHOW VIEWS IN SCHEMA ACTIVITY_CCODE;

-- Test that views return data (or at least don't error)
SELECT 'VW_ACTIVITY_COUNTS_24H' AS view_name, COUNT(*) AS row_count FROM ACTIVITY_CCODE.VW_ACTIVITY_COUNTS_24H LIMIT 1
UNION ALL
SELECT 'VW_LLM_TELEMETRY', COUNT(*) FROM ACTIVITY_CCODE.VW_LLM_TELEMETRY LIMIT 1
UNION ALL
SELECT 'VW_SQL_EXECUTIONS', COUNT(*) FROM ACTIVITY_CCODE.VW_SQL_EXECUTIONS LIMIT 1
UNION ALL
SELECT 'VW_DASHBOARD_OPERATIONS', COUNT(*) FROM ACTIVITY_CCODE.VW_DASHBOARD_OPERATIONS LIMIT 1
UNION ALL
SELECT 'VW_SAFESQL_TEMPLATES', COUNT(*) FROM ACTIVITY_CCODE.VW_SAFESQL_TEMPLATES LIMIT 1
UNION ALL
SELECT 'VW_ACTIVITY_SUMMARY', COUNT(*) FROM ACTIVITY_CCODE.VW_ACTIVITY_SUMMARY LIMIT 1;

-- =============================================================================
-- 7. DESTROY_DASHBOARD Stored Procedure (Idempotent cleanup)
-- =============================================================================
CREATE OR REPLACE PROCEDURE ACTIVITY_CCODE.DESTROY_DASHBOARD(dashboard_name VARCHAR, spec_hash VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
  objects_dropped INTEGER DEFAULT 0;
  error_count INTEGER DEFAULT 0;
  result_message VARCHAR;
BEGIN
  -- Log the destroy operation start
  INSERT INTO CLAUDE_BI.ACTIVITY.EVENTS (
    activity_id,
    ts,
    customer,
    activity,
    feature_json,
    _source_system
  ) VALUES (
    'destroy_' || UUID_STRING(),
    CURRENT_TIMESTAMP(),
    CURRENT_USER(),
    'ccode.dashboard_destroy_started',
    OBJECT_CONSTRUCT(
      'dashboard_name', :dashboard_name,
      'spec_hash', :spec_hash,
      'started_at', CURRENT_TIMESTAMP()
    ),
    'dashboard_factory'
  );
  
  -- Drop Tasks (must be done before views they depend on)
  BEGIN
    EXECUTE IMMEDIATE 'DROP TASK IF EXISTS ACTIVITY_CCODE.' || :dashboard_name || '__*__' || :spec_hash;
    objects_dropped := objects_dropped + 1;
  EXCEPTION
    WHEN OTHER THEN
      error_count := error_count + 1;
  END;
  
  -- Drop Dynamic Tables
  BEGIN
    EXECUTE IMMEDIATE 'DROP DYNAMIC TABLE IF EXISTS ACTIVITY_CCODE.DT_' || :dashboard_name || '__*__' || :spec_hash;
    objects_dropped := objects_dropped + 1;
  EXCEPTION
    WHEN OTHER THEN
      error_count := error_count + 1;
  END;
  
  -- Drop Views (in reverse dependency order)
  -- First drop top views
  BEGIN
    EXECUTE IMMEDIATE 'DROP VIEW IF EXISTS ACTIVITY_CCODE.TOP_' || :dashboard_name || '__*__' || :spec_hash;
    objects_dropped := objects_dropped + 1;
  EXCEPTION
    WHEN OTHER THEN
      error_count := error_count + 1;
  END;
  
  -- Then drop base views
  BEGIN
    EXECUTE IMMEDIATE 'DROP VIEW IF EXISTS ACTIVITY_CCODE.' || :dashboard_name || '__*__' || :spec_hash;
    objects_dropped := objects_dropped + 1;
  EXCEPTION
    WHEN OTHER THEN
      error_count := error_count + 1;
  END;
  
  -- Drop associated warehouse (if dedicated)
  BEGIN
    EXECUTE IMMEDIATE 'DROP WAREHOUSE IF EXISTS WH_' || :dashboard_name;
    objects_dropped := objects_dropped + 1;
  EXCEPTION
    WHEN OTHER THEN
      -- Warehouse might be shared, so ignore errors
      NULL;
  END;
  
  -- Drop resource monitor (if exists)
  BEGIN
    EXECUTE IMMEDIATE 'DROP RESOURCE MONITOR IF EXISTS RM_' || :dashboard_name;
    objects_dropped := objects_dropped + 1;
  EXCEPTION
    WHEN OTHER THEN
      -- Resource monitor might not exist
      NULL;
  END;
  
  -- Build result message
  IF error_count = 0 THEN
    result_message := 'Successfully dropped ' || objects_dropped || ' objects for dashboard ' || dashboard_name;
  ELSE
    result_message := 'Dropped ' || objects_dropped || ' objects with ' || error_count || ' errors for dashboard ' || dashboard_name;
  END IF;
  
  -- Log the destroy operation completion
  INSERT INTO CLAUDE_BI.ACTIVITY.EVENTS (
    activity_id,
    ts,
    customer,
    activity,
    feature_json,
    _source_system
  ) VALUES (
    'destroy_' || UUID_STRING(),
    CURRENT_TIMESTAMP(),
    CURRENT_USER(),
    'ccode.dashboard_destroyed',
    OBJECT_CONSTRUCT(
      'dashboard_name', :dashboard_name,
      'spec_hash', :spec_hash,
      'objects_dropped', :objects_dropped,
      'errors', :error_count,
      'completed_at', CURRENT_TIMESTAMP()
    ),
    'dashboard_factory'
  );
  
  RETURN result_message;
END;
$$;

COMMENT ON PROCEDURE ACTIVITY_CCODE.DESTROY_DASHBOARD IS 
  'Idempotent cleanup procedure for removing all dashboard objects. Uses spec_hash for precise targeting.';

-- =============================================================================
-- 8. LIST_DASHBOARDS Function (Show active dashboards)
-- =============================================================================
CREATE OR REPLACE FUNCTION ACTIVITY_CCODE.LIST_DASHBOARDS()
RETURNS TABLE (
  dashboard_name VARCHAR,
  created_at TIMESTAMP_NTZ,
  last_refreshed TIMESTAMP_NTZ,
  panel_count NUMBER,
  schedule_mode VARCHAR,
  streamlit_url VARCHAR
)
AS
$$
  SELECT DISTINCT
    feature_json:spec_id::VARCHAR AS dashboard_name,
    MIN(ts) AS created_at,
    MAX(ts) AS last_refreshed,
    MAX(feature_json:panels::NUMBER) AS panel_count,
    MAX(feature_json:schedule::VARCHAR) AS schedule_mode,
    MAX(link) AS streamlit_url
  FROM CLAUDE_BI.ACTIVITY.EVENTS
  WHERE activity IN ('ccode.dashboard_created', 'ccode.dashboard_refreshed')
    AND ts >= DATEADD('day', -30, CURRENT_TIMESTAMP())
  GROUP BY dashboard_name
  ORDER BY last_refreshed DESC
$$;

COMMENT ON FUNCTION ACTIVITY_CCODE.LIST_DASHBOARDS IS 
  'Returns list of active dashboards created in last 30 days with their metadata.';

-- Success message
SELECT 'Activity views and procedures created successfully! Ready for Activity-native dashboards.' AS status;