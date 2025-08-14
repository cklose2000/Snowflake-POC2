-- ============================================================================
-- 09_monitoring_queries.sql
-- Dashboard queries for monitoring the Activity Schema system
-- ============================================================================

USE DATABASE CLAUDE_BI;
USE SCHEMA MCP;

-- ============================================================================
-- 1. Current Active User Permissions
-- ============================================================================
SELECT 
  username,
  status,
  ARRAY_SIZE(allowed_actions) AS action_count,
  allowed_actions[0:3] AS sample_actions,  -- Show first 3 actions
  max_rows,
  daily_runtime_budget_s,
  can_export,
  expires_at,
  DATEDIFF('day', CURRENT_DATE(), expires_at) AS days_until_expiry,
  granted_by,
  granted_at
FROM CLAUDE_BI.MCP.CURRENT_USER_PERMISSIONS
WHERE status = 'ACTIVE'
ORDER BY username;

-- ============================================================================
-- 2. Query Activity by User (Last 24 Hours)
-- ============================================================================
SELECT 
  user,
  COUNT(*) AS query_count,
  SUM(CASE WHEN action = 'mcp.query.executed' THEN 1 ELSE 0 END) AS successful,
  SUM(CASE WHEN action = 'mcp.query.rejected' THEN 1 ELSE 0 END) AS rejected,
  SUM(rows_requested) AS total_rows_requested,
  ROUND(AVG(execution_time_ms), 2) AS avg_execution_ms,
  MAX(execution_time_ms) AS max_execution_ms,
  MIN(occurred_at) AS first_query,
  MAX(occurred_at) AS last_query
FROM CLAUDE_BI.MCP.QUERY_ACTIVITY_LAST_24H
GROUP BY user
ORDER BY query_count DESC;

-- ============================================================================
-- 3. Runtime Budget Usage (Rate Limiting Status)
-- ============================================================================
WITH user_limits AS (
  SELECT 
    username,
    daily_runtime_budget_s
  FROM CLAUDE_BI.MCP.CURRENT_USER_PERMISSIONS
  WHERE status = 'ACTIVE'
)
SELECT 
  ul.username,
  ul.daily_runtime_budget_s AS budget_seconds,
  COALESCE(ur.seconds_used, 0) AS used_seconds,
  ul.daily_runtime_budget_s - COALESCE(ur.seconds_used, 0) AS remaining_seconds,
  ROUND(100 * COALESCE(ur.seconds_used, 0) / ul.daily_runtime_budget_s, 1) AS percent_used,
  CASE 
    WHEN COALESCE(ur.seconds_used, 0) >= ul.daily_runtime_budget_s THEN 'EXHAUSTED'
    WHEN COALESCE(ur.seconds_used, 0) >= ul.daily_runtime_budget_s * 0.8 THEN 'WARNING'
    ELSE 'OK'
  END AS budget_status,
  ur.queries_executed
FROM user_limits ul
LEFT JOIN CLAUDE_BI.MCP.USER_RUNTIME_LAST_24H ur
  ON ul.username = ur.user
ORDER BY percent_used DESC;

-- ============================================================================
-- 4. Permission Changes Audit Trail (Last 7 Days)
-- ============================================================================
SELECT 
  occurred_at,
  change_type,
  user_affected,
  changed_by,
  CASE 
    WHEN change_type = 'GRANTED' THEN 
      'Actions: ' || ARRAY_SIZE(allowed_actions) || ', Rows: ' || max_rows || ', Budget: ' || runtime_budget_s || 's'
    ELSE 
      'Reason: ' || COALESCE(reason, 'Not specified')
  END AS details,
  expires_at
FROM CLAUDE_BI.MCP.PERMISSION_CHANGES_LAST_30D
WHERE occurred_at > DATEADD('day', -7, CURRENT_TIMESTAMP())
ORDER BY occurred_at DESC
LIMIT 20;

-- ============================================================================
-- 5. Query Rejection Analysis
-- ============================================================================
SELECT 
  user,
  COUNT(*) AS rejection_count,
  LISTAGG(DISTINCT 
    CASE 
      WHEN rejection_reason LIKE '%budget exceeded%' THEN 'Rate limit'
      WHEN rejection_reason LIKE '%not allowed%' THEN 'Permission denied'
      WHEN rejection_reason LIKE '%expired%' THEN 'Expired permission'
      WHEN rejection_reason LIKE '%rows%' THEN 'Row limit'
      ELSE 'Other'
    END, ', '
  ) AS rejection_types,
  MAX(occurred_at) AS last_rejection
FROM CLAUDE_BI.MCP.QUERY_REJECTIONS_LAST_7D
GROUP BY user
ORDER BY rejection_count DESC;

-- ============================================================================
-- 6. System Health Overview (Hourly)
-- ============================================================================
SELECT 
  hour,
  total_queries,
  unique_users,
  successful_queries,
  rejected_queries,
  ROUND(100.0 * successful_queries / NULLIF(total_queries, 0), 1) AS success_rate,
  ROUND(avg_execution_ms, 2) AS avg_ms,
  ROUND(max_execution_ms, 2) AS max_ms
FROM CLAUDE_BI.MCP.SYSTEM_HEALTH_HOURLY
WHERE hour >= DATEADD('hour', -24, DATE_TRUNC('hour', CURRENT_TIMESTAMP()))
ORDER BY hour DESC;

-- ============================================================================
-- 7. Business Event Distribution (What data is being queried)
-- ============================================================================
SELECT 
  action,
  source,
  COUNT(*) AS event_count,
  COUNT(DISTINCT actor_id) AS unique_actors,
  MIN(occurred_at) AS earliest_event,
  MAX(occurred_at) AS latest_event,
  DATEDIFF('hour', MIN(occurred_at), MAX(occurred_at)) AS span_hours
FROM CLAUDE_BI.ACTIVITY.EVENTS
WHERE occurred_at > DATEADD('day', -7, CURRENT_TIMESTAMP())
  AND source NOT IN ('system', 'mcp')  -- Exclude system events
GROUP BY action, source
ORDER BY event_count DESC
LIMIT 20;

-- ============================================================================
-- 8. User Lifecycle (Creation to Activity)
-- ============================================================================
WITH user_creation AS (
  SELECT 
    object_id AS username,
    occurred_at AS created_at,
    attributes:department::string AS department
  FROM CLAUDE_BI.ACTIVITY.EVENTS
  WHERE action = 'system.user.created'
),
user_first_query AS (
  SELECT 
    actor_id AS username,
    MIN(occurred_at) AS first_query_at
  FROM CLAUDE_BI.ACTIVITY.EVENTS
  WHERE action = 'mcp.query.executed'
  GROUP BY actor_id
)
SELECT 
  uc.username,
  uc.department,
  uc.created_at,
  ufq.first_query_at,
  DATEDIFF('minute', uc.created_at, ufq.first_query_at) AS minutes_to_first_query,
  CASE 
    WHEN ufq.first_query_at IS NULL THEN 'Never queried'
    WHEN DATEDIFF('hour', uc.created_at, ufq.first_query_at) < 1 THEN 'Immediate'
    WHEN DATEDIFF('hour', uc.created_at, ufq.first_query_at) < 24 THEN 'Same day'
    ELSE 'Later'
  END AS activation_speed
FROM user_creation uc
LEFT JOIN user_first_query ufq ON uc.username = ufq.username
ORDER BY uc.created_at DESC;

-- ============================================================================
-- 9. Cost Tracking Summary (Estimated)
-- ============================================================================
SELECT 
  DATE_TRUNC('day', occurred_at) AS day,
  COUNT(DISTINCT actor_id) AS unique_users,
  COUNT(*) AS total_queries,
  SUM(attributes:execution_time_ms::number) / 1000 AS total_runtime_seconds,
  -- Rough cost estimate: $0.00003 per second for XS warehouse
  ROUND(SUM(attributes:execution_time_ms::number) / 1000 * 0.00003, 4) AS estimated_cost_usd
FROM CLAUDE_BI.ACTIVITY.EVENTS
WHERE action = 'mcp.query.executed'
  AND occurred_at > DATEADD('day', -30, CURRENT_TIMESTAMP())
GROUP BY day
ORDER BY day DESC;

-- ============================================================================
-- 10. Data Quality Check - Event Dependencies
-- ============================================================================
WITH dependency_check AS (
  SELECT 
    e1.event_id,
    e1.depends_on_event_id,
    e1.occurred_at,
    e2.event_id AS parent_exists,
    CASE 
      WHEN e1.depends_on_event_id IS NULL THEN 'No dependency'
      WHEN e2.event_id IS NOT NULL THEN 'Valid'
      ELSE 'Missing parent'
    END AS dependency_status
  FROM CLAUDE_BI.ACTIVITY.EVENTS e1
  LEFT JOIN CLAUDE_BI.ACTIVITY.EVENTS e2 
    ON e1.depends_on_event_id = e2.event_id
  WHERE e1.occurred_at > DATEADD('day', -1, CURRENT_TIMESTAMP())
)
SELECT 
  dependency_status,
  COUNT(*) AS event_count,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) AS percentage
FROM dependency_check
GROUP BY dependency_status
ORDER BY event_count DESC;