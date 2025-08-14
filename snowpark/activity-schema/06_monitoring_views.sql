-- ============================================================================
-- 06_monitoring_views.sql
-- Views to monitor permissions and activity (derived from events)
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE CLAUDE_BI;
USE SCHEMA MCP;

-- ============================================================================
-- View: Current user permissions (latest event per user)
-- ============================================================================
CREATE OR REPLACE VIEW CLAUDE_BI.MCP.CURRENT_USER_PERMISSIONS AS
WITH latest_permissions AS (
  SELECT 
    object_id AS username,
    action,
    attributes,
    occurred_at,
    actor_id AS granted_by,
    ROW_NUMBER() OVER (
      PARTITION BY object_id 
      ORDER BY occurred_at DESC
    ) AS rn
  FROM CLAUDE_BI.ACTIVITY.EVENTS
  WHERE action IN ('system.permission.granted', 'system.permission.revoked')
    AND object_type = 'user'
)
SELECT 
  username,
  granted_by,
  occurred_at AS granted_at,
  attributes:allowed_actions::array AS allowed_actions,
  attributes:max_rows::number AS max_rows,
  attributes:daily_runtime_budget_s::number AS daily_runtime_budget_s,
  attributes:can_export::boolean AS can_export,
  attributes:expires_at::timestamp_tz AS expires_at,
  CASE 
    WHEN attributes:expires_at::timestamp_tz < CURRENT_TIMESTAMP() THEN 'EXPIRED'
    WHEN action = 'system.permission.revoked' THEN 'REVOKED'
    ELSE 'ACTIVE'
  END AS status,
  attributes:reason::string AS revocation_reason
FROM latest_permissions
WHERE rn = 1
ORDER BY username;

-- ============================================================================
-- View: MCP query activity in last 24 hours
-- ============================================================================
CREATE OR REPLACE VIEW CLAUDE_BI.MCP.QUERY_ACTIVITY_LAST_24H AS
SELECT 
  actor_id AS user,
  occurred_at,
  attributes:window::string AS query_window,
  attributes:rows_requested::number AS rows_requested,
  attributes:execution_time_ms::number AS execution_time_ms,
  attributes:plan AS query_plan,
  object_id AS query_id,
  action
FROM CLAUDE_BI.ACTIVITY.EVENTS
WHERE action IN ('mcp.query.executed', 'mcp.query.rejected')
  AND occurred_at > DATEADD('hour', -24, CURRENT_TIMESTAMP())
ORDER BY occurred_at DESC;

-- ============================================================================
-- View: User runtime usage in last 24 hours (for rate limiting)
-- ============================================================================
CREATE OR REPLACE VIEW CLAUDE_BI.MCP.USER_RUNTIME_LAST_24H AS
SELECT 
  actor_id AS user,
  COUNT(*) AS queries_executed,
  SUM(attributes:execution_time_ms::number)/1000 AS seconds_used,
  MAX(attributes:execution_time_ms::number)/1000 AS max_query_seconds,
  AVG(attributes:execution_time_ms::number)/1000 AS avg_query_seconds
FROM CLAUDE_BI.ACTIVITY.EVENTS
WHERE action = 'mcp.query.executed'
  AND occurred_at >= DATEADD('day', -1, CURRENT_TIMESTAMP())
GROUP BY 1
ORDER BY seconds_used DESC;

-- ============================================================================
-- View: Permission change audit trail (last 30 days)
-- ============================================================================
CREATE OR REPLACE VIEW CLAUDE_BI.MCP.PERMISSION_CHANGES_LAST_30D AS
SELECT 
  occurred_at,
  actor_id AS changed_by,
  action,
  object_id AS user_affected,
  CASE 
    WHEN action = 'system.permission.granted' THEN 'GRANTED'
    WHEN action = 'system.permission.revoked' THEN 'REVOKED'
    ELSE 'UNKNOWN'
  END AS change_type,
  attributes:allowed_actions AS allowed_actions,
  attributes:max_rows AS max_rows,
  attributes:daily_runtime_budget_s AS runtime_budget_s,
  attributes:expires_at AS expires_at,
  attributes:reason AS reason
FROM CLAUDE_BI.ACTIVITY.EVENTS
WHERE action IN ('system.permission.granted', 'system.permission.revoked')
  AND occurred_at > DATEADD('day', -30, CURRENT_TIMESTAMP())
ORDER BY occurred_at DESC;

-- ============================================================================
-- View: Query rejection reasons
-- ============================================================================
CREATE OR REPLACE VIEW CLAUDE_BI.MCP.QUERY_REJECTIONS_LAST_7D AS
SELECT 
  occurred_at,
  actor_id AS user,
  attributes:error::string AS rejection_reason,
  attributes:plan AS attempted_plan,
  attributes:execution_time_ms::number AS processing_time_ms
FROM CLAUDE_BI.ACTIVITY.EVENTS
WHERE action = 'mcp.query.rejected'
  AND occurred_at > DATEADD('day', -7, CURRENT_TIMESTAMP())
ORDER BY occurred_at DESC;

-- ============================================================================
-- View: System health - queries per hour
-- ============================================================================
CREATE OR REPLACE VIEW CLAUDE_BI.MCP.SYSTEM_HEALTH_HOURLY AS
SELECT 
  DATE_TRUNC('hour', occurred_at) AS hour,
  COUNT(*) AS total_queries,
  COUNT(DISTINCT actor_id) AS unique_users,
  SUM(CASE WHEN action = 'mcp.query.executed' THEN 1 ELSE 0 END) AS successful_queries,
  SUM(CASE WHEN action = 'mcp.query.rejected' THEN 1 ELSE 0 END) AS rejected_queries,
  AVG(attributes:execution_time_ms::number) AS avg_execution_ms,
  MAX(attributes:execution_time_ms::number) AS max_execution_ms
FROM CLAUDE_BI.ACTIVITY.EVENTS
WHERE action LIKE 'mcp.query.%'
  AND occurred_at > DATEADD('day', -7, CURRENT_TIMESTAMP())
GROUP BY 1
ORDER BY 1 DESC;

-- ============================================================================
-- Grant view permissions
-- ============================================================================
GRANT SELECT ON ALL VIEWS IN SCHEMA CLAUDE_BI.MCP TO ROLE MCP_ADMIN_ROLE;
GRANT SELECT ON VIEW CLAUDE_BI.MCP.QUERY_ACTIVITY_LAST_24H TO ROLE MCP_SERVICE_ROLE;
GRANT SELECT ON VIEW CLAUDE_BI.MCP.USER_RUNTIME_LAST_24H TO ROLE MCP_SERVICE_ROLE;