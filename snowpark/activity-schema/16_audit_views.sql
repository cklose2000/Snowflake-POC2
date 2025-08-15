-- ============================================================================
-- 16_audit_views.sql
-- Audit and monitoring views without exposing raw tokens
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE CLAUDE_BI;
USE SCHEMA MCP;

-- ============================================================================
-- MCP Request Activity (no tokens exposed)
-- ============================================================================

CREATE OR REPLACE VIEW CLAUDE_BI.MCP.REQUEST_ACTIVITY AS
SELECT 
  occurred_at,
  actor_id AS username,
  attributes:endpoint::STRING AS endpoint,
  attributes:tool::STRING AS tool,
  attributes:nonce::STRING AS request_nonce,
  attributes:execution_ms::NUMBER AS runtime_ms,
  attributes:rows_returned::NUMBER AS rows_returned,
  CASE 
    WHEN action = 'mcp.request.processed' THEN 'SUCCESS'
    WHEN action = 'mcp.request.failed' THEN 'FAILED'
    ELSE 'UNKNOWN'
  END AS status,
  attributes:error::STRING AS error_message
FROM CLAUDE_BI.ACTIVITY.EVENTS
WHERE action LIKE 'mcp.request.%'
ORDER BY occurred_at DESC;

-- ============================================================================
-- Permission Timeline (token hashes only, no raw tokens)
-- ============================================================================

CREATE OR REPLACE VIEW CLAUDE_BI.MCP.PERMISSION_TIMELINE AS
WITH perm_events AS (
  SELECT 
    object_id AS username,
    action,
    occurred_at,
    SUBSTR(attributes:token_hash::STRING, 1, 8) AS token_prefix,  -- Only first 8 chars
    attributes:allowed_tools::ARRAY AS allowed_tools,
    attributes:max_rows::NUMBER AS max_rows,
    attributes:daily_runtime_seconds::NUMBER AS runtime_budget,
    attributes:expires_at::TIMESTAMP_TZ AS expires_at,
    attributes:granted_by::STRING AS granted_by,
    attributes:revoked_by::STRING AS revoked_by,
    attributes:reason::STRING AS reason,
    LAG(action) OVER (
      PARTITION BY object_id 
      ORDER BY occurred_at
    ) AS prev_action,
    LEAD(action) OVER (
      PARTITION BY object_id 
      ORDER BY occurred_at
    ) AS next_action
  FROM CLAUDE_BI.ACTIVITY.EVENTS
  WHERE object_type = 'user'
    AND action LIKE 'system.permission.%'
)
SELECT 
  username,
  occurred_at,
  action,
  token_prefix || '...' AS token_identifier,
  CASE 
    WHEN action = 'system.permission.granted' THEN 'GRANTED'
    WHEN action = 'system.permission.revoked' THEN 'REVOKED'
    ELSE 'UNKNOWN'
  END AS permission_state,
  allowed_tools,
  max_rows,
  runtime_budget,
  expires_at,
  COALESCE(granted_by, revoked_by) AS changed_by,
  reason,
  DATEDIFF('hour', 
    LAG(occurred_at) OVER (PARTITION BY username ORDER BY occurred_at),
    occurred_at
  ) AS hours_since_last_change
FROM perm_events
ORDER BY username, occurred_at DESC;

-- ============================================================================
-- Security Events Audit
-- ============================================================================

CREATE OR REPLACE VIEW CLAUDE_BI.MCP.SECURITY_AUDIT AS
SELECT 
  occurred_at,
  action,
  actor_id,
  CASE
    WHEN action = 'security.replay_detected' THEN 'REPLAY_ATTACK'
    WHEN action = 'system.token.revoked' THEN 'TOKEN_REVOKED'
    WHEN action = 'mcp.request.failed' AND attributes:error LIKE '%token%' THEN 'INVALID_TOKEN'
    WHEN action = 'mcp.request.failed' AND attributes:error LIKE '%budget%' THEN 'BUDGET_EXCEEDED'
    WHEN action = 'mcp.request.failed' AND attributes:error LIKE '%expired%' THEN 'TOKEN_EXPIRED'
    ELSE 'OTHER'
  END AS security_event_type,
  attributes:nonce::STRING AS nonce,
  SUBSTR(attributes:token_hash::STRING, 1, 8) AS token_prefix,
  attributes:error::STRING AS error_detail,
  attributes:reason::STRING AS reason
FROM CLAUDE_BI.ACTIVITY.EVENTS
WHERE action IN (
  'security.replay_detected',
  'system.token.revoked',
  'mcp.request.failed'
)
  OR action LIKE 'security.%'
ORDER BY occurred_at DESC;

-- ============================================================================
-- User Session Analysis
-- ============================================================================

CREATE OR REPLACE VIEW CLAUDE_BI.MCP.USER_SESSIONS AS
WITH session_events AS (
  SELECT 
    actor_id AS username,
    occurred_at,
    action,
    attributes:nonce::STRING AS nonce,
    LAG(occurred_at) OVER (
      PARTITION BY actor_id 
      ORDER BY occurred_at
    ) AS prev_event_time,
    DATEDIFF('minute', 
      LAG(occurred_at) OVER (PARTITION BY actor_id ORDER BY occurred_at),
      occurred_at
    ) AS minutes_since_last
  FROM CLAUDE_BI.ACTIVITY.EVENTS
  WHERE action = 'mcp.request.processed'
),
session_boundaries AS (
  SELECT 
    username,
    occurred_at,
    -- New session if > 30 minutes since last activity
    CASE 
      WHEN minutes_since_last > 30 OR minutes_since_last IS NULL THEN 1
      ELSE 0
    END AS is_new_session
  FROM session_events
),
sessions AS (
  SELECT 
    username,
    occurred_at,
    SUM(is_new_session) OVER (
      PARTITION BY username 
      ORDER BY occurred_at
    ) AS session_id
  FROM session_boundaries
)
SELECT 
  username,
  session_id,
  MIN(occurred_at) AS session_start,
  MAX(occurred_at) AS session_end,
  COUNT(*) AS request_count,
  DATEDIFF('minute', MIN(occurred_at), MAX(occurred_at)) AS session_duration_minutes
FROM sessions
GROUP BY username, session_id
HAVING request_count > 1  -- Only show actual sessions, not single requests
ORDER BY session_start DESC;

-- ============================================================================
-- Tool Usage Statistics
-- ============================================================================

CREATE OR REPLACE VIEW CLAUDE_BI.MCP.TOOL_USAGE_STATS AS
WITH tool_usage AS (
  SELECT 
    attributes:tool::STRING AS tool_name,
    actor_id AS username,
    DATE_TRUNC('day', occurred_at) AS usage_date,
    COUNT(*) AS call_count,
    AVG(attributes:execution_ms::NUMBER) AS avg_runtime_ms,
    SUM(attributes:execution_ms::NUMBER) / 1000 AS total_runtime_seconds,
    SUM(COALESCE(attributes:rows_returned::NUMBER, 0)) AS total_rows_returned
  FROM CLAUDE_BI.ACTIVITY.EVENTS
  WHERE action = 'mcp.request.processed'
    AND attributes:tool IS NOT NULL
  GROUP BY tool_name, username, usage_date
)
SELECT 
  usage_date,
  tool_name,
  COUNT(DISTINCT username) AS unique_users,
  SUM(call_count) AS total_calls,
  AVG(avg_runtime_ms) AS avg_runtime_ms,
  SUM(total_runtime_seconds) AS total_runtime_seconds,
  SUM(total_rows_returned) AS total_rows_returned,
  SUM(call_count) / COUNT(DISTINCT username) AS avg_calls_per_user
FROM tool_usage
GROUP BY usage_date, tool_name
ORDER BY usage_date DESC, total_calls DESC;

-- ============================================================================
-- Failed Request Analysis
-- ============================================================================

CREATE OR REPLACE VIEW CLAUDE_BI.MCP.FAILED_REQUESTS AS
SELECT 
  occurred_at,
  actor_id AS username,
  attributes:endpoint::STRING AS endpoint,
  attributes:tool::STRING AS tool,
  attributes:error::STRING AS error_message,
  CASE
    WHEN attributes:error LIKE '%token%' THEN 'AUTH_ERROR'
    WHEN attributes:error LIKE '%permission%' THEN 'PERMISSION_ERROR'
    WHEN attributes:error LIKE '%budget%' THEN 'BUDGET_ERROR'
    WHEN attributes:error LIKE '%replay%' THEN 'REPLAY_ERROR'
    WHEN attributes:error LIKE '%expired%' THEN 'EXPIRY_ERROR'
    WHEN attributes:error LIKE '%timeout%' THEN 'TIMEOUT_ERROR'
    ELSE 'OTHER_ERROR'
  END AS error_category,
  attributes:nonce::STRING AS request_nonce
FROM CLAUDE_BI.ACTIVITY.EVENTS
WHERE action = 'mcp.request.failed'
ORDER BY occurred_at DESC;

-- ============================================================================
-- Daily Activity Summary
-- ============================================================================

CREATE OR REPLACE VIEW CLAUDE_BI.MCP.DAILY_ACTIVITY_SUMMARY AS
WITH daily_stats AS (
  SELECT 
    DATE(occurred_at) AS activity_date,
    COUNT(DISTINCT actor_id) AS unique_users,
    COUNT(*) AS total_requests,
    COUNT(CASE WHEN action = 'mcp.request.processed' THEN 1 END) AS successful_requests,
    COUNT(CASE WHEN action = 'mcp.request.failed' THEN 1 END) AS failed_requests,
    AVG(CASE 
      WHEN action = 'mcp.request.processed' 
      THEN attributes:execution_ms::NUMBER 
    END) AS avg_runtime_ms,
    SUM(CASE 
      WHEN action = 'mcp.request.processed' 
      THEN attributes:execution_ms::NUMBER 
    END) / 1000 AS total_runtime_seconds
  FROM CLAUDE_BI.ACTIVITY.EVENTS
  WHERE action LIKE 'mcp.request.%'
  GROUP BY activity_date
),
user_events AS (
  SELECT 
    DATE(occurred_at) AS activity_date,
    COUNT(CASE WHEN action = 'system.user.created' THEN 1 END) AS users_created,
    COUNT(CASE WHEN action = 'system.permission.granted' THEN 1 END) AS permissions_granted,
    COUNT(CASE WHEN action = 'system.permission.revoked' THEN 1 END) AS permissions_revoked
  FROM CLAUDE_BI.ACTIVITY.EVENTS
  WHERE action LIKE 'system.%'
  GROUP BY activity_date
)
SELECT 
  COALESCE(d.activity_date, u.activity_date) AS date,
  COALESCE(d.unique_users, 0) AS unique_users,
  COALESCE(d.total_requests, 0) AS total_requests,
  COALESCE(d.successful_requests, 0) AS successful_requests,
  COALESCE(d.failed_requests, 0) AS failed_requests,
  ROUND(COALESCE(d.successful_requests, 0) * 100.0 / NULLIF(d.total_requests, 0), 2) AS success_rate,
  COALESCE(d.avg_runtime_ms, 0) AS avg_runtime_ms,
  COALESCE(d.total_runtime_seconds, 0) AS total_runtime_seconds,
  COALESCE(u.users_created, 0) AS users_created,
  COALESCE(u.permissions_granted, 0) AS permissions_granted,
  COALESCE(u.permissions_revoked, 0) AS permissions_revoked
FROM daily_stats d
FULL OUTER JOIN user_events u ON d.activity_date = u.activity_date
ORDER BY date DESC;

-- ============================================================================
-- Grant permissions
-- ============================================================================

GRANT SELECT ON VIEW CLAUDE_BI.MCP.REQUEST_ACTIVITY TO ROLE MCP_ADMIN_ROLE;
GRANT SELECT ON VIEW CLAUDE_BI.MCP.PERMISSION_TIMELINE TO ROLE MCP_ADMIN_ROLE;
GRANT SELECT ON VIEW CLAUDE_BI.MCP.SECURITY_AUDIT TO ROLE MCP_ADMIN_ROLE;
GRANT SELECT ON VIEW CLAUDE_BI.MCP.USER_SESSIONS TO ROLE MCP_ADMIN_ROLE;
GRANT SELECT ON VIEW CLAUDE_BI.MCP.TOOL_USAGE_STATS TO ROLE MCP_ADMIN_ROLE;
GRANT SELECT ON VIEW CLAUDE_BI.MCP.FAILED_REQUESTS TO ROLE MCP_ADMIN_ROLE;
GRANT SELECT ON VIEW CLAUDE_BI.MCP.DAILY_ACTIVITY_SUMMARY TO ROLE MCP_ADMIN_ROLE;

-- Limited views for users to see their own activity
GRANT SELECT ON VIEW CLAUDE_BI.MCP.REQUEST_ACTIVITY TO ROLE MCP_USER_ROLE;
-- Users can only see their own activity via filtered queries