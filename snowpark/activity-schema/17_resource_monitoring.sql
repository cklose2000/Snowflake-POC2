-- ============================================================================
-- 17_resource_monitoring.sql
-- Cost monitoring and resource controls
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE CLAUDE_BI;
USE SCHEMA MCP;

-- ============================================================================
-- Warehouse Cost Monitoring
-- ============================================================================

CREATE OR REPLACE VIEW CLAUDE_BI.MCP.WAREHOUSE_COSTS AS
WITH warehouse_usage AS (
  SELECT 
    WAREHOUSE_NAME,
    DATE_TRUNC('day', START_TIME) AS usage_date,
    COUNT(*) AS query_count,
    SUM(CREDITS_USED) AS credits_used,
    AVG(EXECUTION_TIME) / 1000 AS avg_execution_seconds,
    MAX(EXECUTION_TIME) / 1000 AS max_execution_seconds,
    SUM(BYTES_SCANNED) / POW(1024, 3) AS gb_scanned
  FROM TABLE(INFORMATION_SCHEMA.WAREHOUSE_METERING_HISTORY(
    DATE_RANGE_START => DATEADD('day', -30, CURRENT_DATE()),
    DATE_RANGE_END => CURRENT_DATE()
  ))
  WHERE WAREHOUSE_NAME IN ('DT_XS_WH', 'ALERT_WH', 'MCP_XS_WH', 'CLAUDE_WAREHOUSE')
  GROUP BY WAREHOUSE_NAME, usage_date
)
SELECT 
  usage_date,
  WAREHOUSE_NAME,
  query_count,
  credits_used,
  ROUND(credits_used * 3, 2) AS estimated_cost_usd,  -- ~$3 per credit
  avg_execution_seconds,
  max_execution_seconds,
  ROUND(gb_scanned, 2) AS gb_scanned,
  ROUND(credits_used / NULLIF(query_count, 0), 4) AS credits_per_query
FROM warehouse_usage
ORDER BY usage_date DESC, credits_used DESC;

-- ============================================================================
-- Storage Cost Monitoring
-- ============================================================================

CREATE OR REPLACE VIEW CLAUDE_BI.MCP.STORAGE_COSTS AS
SELECT 
  TABLE_CATALOG AS database_name,
  TABLE_SCHEMA AS schema_name,
  TABLE_NAME,
  TABLE_TYPE,
  ROW_COUNT,
  BYTES / POW(1024, 3) AS size_gb,
  BYTES / POW(1024, 4) AS size_tb,
  -- Approximate monthly storage cost: $23/TB/month
  ROUND((BYTES / POW(1024, 4)) * 23, 2) AS monthly_storage_cost_usd,
  ROUND((BYTES / POW(1024, 4)) * 23 / 30, 4) AS daily_storage_cost_usd,
  LAST_ALTERED
FROM CLAUDE_BI.INFORMATION_SCHEMA.TABLES
WHERE TABLE_CATALOG = 'CLAUDE_BI'
  AND TABLE_TYPE IN ('BASE TABLE', 'DYNAMIC TABLE')
ORDER BY BYTES DESC;

-- ============================================================================
-- User Runtime Budget Tracking
-- ============================================================================

CREATE OR REPLACE VIEW CLAUDE_BI.MCP.USER_RUNTIME_BUDGET AS
WITH user_permissions AS (
  -- Get current runtime budget per user
  SELECT 
    object_id AS username,
    attributes:daily_runtime_seconds::NUMBER AS daily_budget_seconds,
    attributes:expires_at::TIMESTAMP_TZ AS expires_at
  FROM CLAUDE_BI.ACTIVITY.EVENTS
  WHERE object_type = 'user'
    AND action = 'system.permission.granted'
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY object_id 
    ORDER BY occurred_at DESC
  ) = 1
),
user_usage AS (
  -- Calculate actual usage in last 24 hours
  SELECT 
    actor_id AS username,
    COUNT(*) AS request_count_24h,
    SUM(attributes:execution_ms::NUMBER) / 1000 AS runtime_seconds_24h,
    AVG(attributes:execution_ms::NUMBER) AS avg_runtime_ms,
    MAX(occurred_at) AS last_request_time
  FROM CLAUDE_BI.ACTIVITY.EVENTS
  WHERE action = 'mcp.request.processed'
    AND occurred_at >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
  GROUP BY actor_id
)
SELECT 
  p.username,
  p.daily_budget_seconds,
  COALESCE(u.runtime_seconds_24h, 0) AS runtime_used_seconds,
  p.daily_budget_seconds - COALESCE(u.runtime_seconds_24h, 0) AS runtime_remaining_seconds,
  ROUND(COALESCE(u.runtime_seconds_24h, 0) * 100.0 / NULLIF(p.daily_budget_seconds, 0), 2) AS budget_used_percent,
  COALESCE(u.request_count_24h, 0) AS requests_24h,
  COALESCE(u.avg_runtime_ms, 0) AS avg_runtime_ms,
  u.last_request_time,
  p.expires_at,
  CASE 
    WHEN COALESCE(u.runtime_seconds_24h, 0) >= p.daily_budget_seconds THEN 'EXCEEDED'
    WHEN COALESCE(u.runtime_seconds_24h, 0) >= p.daily_budget_seconds * 0.8 THEN 'WARNING'
    ELSE 'OK'
  END AS budget_status
FROM user_permissions p
LEFT JOIN user_usage u ON p.username = u.username
ORDER BY budget_used_percent DESC;

-- ============================================================================
-- Query Performance Monitoring
-- ============================================================================

CREATE OR REPLACE VIEW CLAUDE_BI.MCP.QUERY_PERFORMANCE AS
SELECT 
  QUERY_ID,
  QUERY_TEXT,
  DATABASE_NAME,
  SCHEMA_NAME,
  QUERY_TYPE,
  USER_NAME,
  ROLE_NAME,
  WAREHOUSE_NAME,
  WAREHOUSE_SIZE,
  START_TIME,
  END_TIME,
  TOTAL_ELAPSED_TIME / 1000 AS total_elapsed_seconds,
  EXECUTION_TIME / 1000 AS execution_seconds,
  COMPILATION_TIME / 1000 AS compilation_seconds,
  QUEUED_PROVISIONING_TIME / 1000 AS queue_seconds,
  BYTES_SCANNED / POW(1024, 3) AS gb_scanned,
  ROWS_PRODUCED,
  CREDITS_USED_CLOUD_SERVICES,
  ERROR_CODE,
  ERROR_MESSAGE,
  QUERY_TAG
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(
  END_TIME_RANGE_START => DATEADD('hour', -24, CURRENT_TIMESTAMP()),
  END_TIME_RANGE_END => CURRENT_TIMESTAMP()
))
WHERE DATABASE_NAME = 'CLAUDE_BI'
  AND (
    QUERY_TAG LIKE '%mcp%'
    OR USER_NAME LIKE 'MCP_%'
    OR QUERY_TEXT LIKE 'CALL MCP.%'
  )
ORDER BY START_TIME DESC;

-- ============================================================================
-- Resource Usage Alerts Configuration
-- ============================================================================

-- Alert for user budget exceeded
CREATE OR REPLACE ALERT CLAUDE_BI.MCP.BUDGET_EXCEEDED_ALERT
  WAREHOUSE = ALERT_WH
  SCHEDULE = '15 MINUTE'
  IF (EXISTS (
    SELECT 1 FROM CLAUDE_BI.MCP.USER_RUNTIME_BUDGET
    WHERE budget_status = 'EXCEEDED'
  ))
  THEN CALL SYSTEM$SEND_EMAIL(
    'admin@company.com',
    'MCP Budget Alert',
    'One or more users have exceeded their daily runtime budget. Check USER_RUNTIME_BUDGET view.'
  );

-- Alert for high warehouse costs
CREATE OR REPLACE ALERT CLAUDE_BI.MCP.COST_ALERT
  WAREHOUSE = ALERT_WH
  SCHEDULE = 'USING CRON 0 8 * * * UTC'  -- Daily at 8 AM UTC
  IF (EXISTS (
    SELECT 1 FROM CLAUDE_BI.MCP.WAREHOUSE_COSTS
    WHERE usage_date = CURRENT_DATE() - 1
      AND estimated_cost_usd > 100  -- Alert if daily cost > $100
  ))
  THEN CALL SYSTEM$SEND_EMAIL(
    'admin@company.com',
    'MCP Cost Alert',
    'Daily warehouse costs exceeded $100. Check WAREHOUSE_COSTS view for details.'
  );

-- Resume alerts
ALTER ALERT CLAUDE_BI.MCP.BUDGET_EXCEEDED_ALERT RESUME;
ALTER ALERT CLAUDE_BI.MCP.COST_ALERT RESUME;

-- ============================================================================
-- Cost Optimization Recommendations View
-- ============================================================================

CREATE OR REPLACE VIEW CLAUDE_BI.MCP.COST_OPTIMIZATION AS
WITH warehouse_stats AS (
  SELECT 
    WAREHOUSE_NAME,
    AVG(CREDITS_USED) AS avg_daily_credits,
    AVG(query_count) AS avg_daily_queries,
    AVG(avg_execution_seconds) AS avg_execution_seconds
  FROM CLAUDE_BI.MCP.WAREHOUSE_COSTS
  WHERE usage_date >= DATEADD('day', -7, CURRENT_DATE())
  GROUP BY WAREHOUSE_NAME
),
storage_waste AS (
  SELECT 
    TABLE_NAME,
    size_gb,
    ROW_COUNT,
    DATEDIFF('day', LAST_ALTERED, CURRENT_DATE()) AS days_since_modified
  FROM CLAUDE_BI.MCP.STORAGE_COSTS
  WHERE ROW_COUNT = 0 OR days_since_modified > 90
)
SELECT 
  'WAREHOUSE' AS category,
  'Consider downsizing ' || WAREHOUSE_NAME AS recommendation,
  'Low utilization detected' AS reason,
  ROUND(avg_daily_credits * 3 * 30, 2) AS potential_monthly_savings_usd
FROM warehouse_stats
WHERE avg_daily_queries < 10 AND avg_execution_seconds < 5

UNION ALL

SELECT 
  'STORAGE' AS category,
  'Consider dropping table ' || TABLE_NAME AS recommendation,
  CASE 
    WHEN ROW_COUNT = 0 THEN 'Empty table'
    ELSE 'Not accessed in ' || days_since_modified || ' days'
  END AS reason,
  ROUND(size_gb * 0.023, 2) AS potential_monthly_savings_usd
FROM storage_waste
WHERE size_gb > 0.1

ORDER BY potential_monthly_savings_usd DESC;

-- ============================================================================
-- Monthly Cost Summary
-- ============================================================================

CREATE OR REPLACE VIEW CLAUDE_BI.MCP.MONTHLY_COST_SUMMARY AS
WITH compute_costs AS (
  SELECT 
    DATE_TRUNC('month', usage_date) AS month,
    SUM(credits_used) AS total_credits,
    SUM(estimated_cost_usd) AS total_compute_cost_usd
  FROM CLAUDE_BI.MCP.WAREHOUSE_COSTS
  GROUP BY month
),
storage_costs AS (
  SELECT 
    DATE_TRUNC('month', CURRENT_DATE()) AS month,
    SUM(monthly_storage_cost_usd) AS total_storage_cost_usd
  FROM CLAUDE_BI.MCP.STORAGE_COSTS
)
SELECT 
  c.month,
  c.total_credits,
  c.total_compute_cost_usd,
  s.total_storage_cost_usd,
  c.total_compute_cost_usd + COALESCE(s.total_storage_cost_usd, 0) AS total_cost_usd,
  ROUND((c.total_compute_cost_usd + COALESCE(s.total_storage_cost_usd, 0)) / 
    DATE_PART('day', LAST_DAY(c.month)), 2) AS avg_daily_cost_usd
FROM compute_costs c
LEFT JOIN storage_costs s ON c.month = s.month
ORDER BY c.month DESC;

-- ============================================================================
-- Grant permissions
-- ============================================================================

GRANT SELECT ON VIEW CLAUDE_BI.MCP.WAREHOUSE_COSTS TO ROLE MCP_ADMIN_ROLE;
GRANT SELECT ON VIEW CLAUDE_BI.MCP.STORAGE_COSTS TO ROLE MCP_ADMIN_ROLE;
GRANT SELECT ON VIEW CLAUDE_BI.MCP.USER_RUNTIME_BUDGET TO ROLE MCP_ADMIN_ROLE;
GRANT SELECT ON VIEW CLAUDE_BI.MCP.QUERY_PERFORMANCE TO ROLE MCP_ADMIN_ROLE;
GRANT SELECT ON VIEW CLAUDE_BI.MCP.COST_OPTIMIZATION TO ROLE MCP_ADMIN_ROLE;
GRANT SELECT ON VIEW CLAUDE_BI.MCP.MONTHLY_COST_SUMMARY TO ROLE MCP_ADMIN_ROLE;

-- Users can see their own budget status
GRANT SELECT ON VIEW CLAUDE_BI.MCP.USER_RUNTIME_BUDGET TO ROLE MCP_USER_ROLE;