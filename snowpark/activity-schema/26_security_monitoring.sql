-- ============================================================================
-- 26_security_monitoring.sql
-- Comprehensive security monitoring views and alerts for token-based auth
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE CLAUDE_BI;

-- Create security schema if not exists
CREATE SCHEMA IF NOT EXISTS CLAUDE_BI.SECURITY
  COMMENT = 'Security monitoring and audit views';

USE SCHEMA CLAUDE_BI.SECURITY;

-- ============================================================================
-- Token usage audit trail
-- ============================================================================

CREATE OR REPLACE VIEW SECURITY.TOKEN_USAGE AS
WITH token_events AS (
  SELECT 
    occurred_at,
    action,
    actor_id AS username,
    attributes:token_prefix::STRING AS token_prefix,
    attributes:client_ip::STRING AS client_ip,
    attributes:endpoint::STRING AS endpoint,
    attributes:tool::STRING AS tool,
    attributes:execution_ms::NUMBER AS execution_ms,
    attributes:error::STRING AS error
  FROM CLAUDE_BI.ACTIVITY.EVENTS
  WHERE action IN (
    'system.token.used',
    'mcp.request.processed',
    'mcp.request.failed',
    'security.auth.failed'
  )
    AND occurred_at >= DATEADD('day', -30, CURRENT_TIMESTAMP())
)
SELECT 
  occurred_at,
  username,
  token_prefix,
  client_ip,
  endpoint,
  tool,
  execution_ms,
  CASE 
    WHEN action = 'system.token.used' THEN 'TOKEN_USED'
    WHEN action = 'mcp.request.processed' THEN 'REQUEST_SUCCESS'
    WHEN action = 'mcp.request.failed' THEN 'REQUEST_FAILED'
    WHEN action = 'security.auth.failed' THEN 'AUTH_FAILED'
  END AS event_type,
  error,
  DATE_TRUNC('hour', occurred_at) AS hour,
  DATE_TRUNC('day', occurred_at) AS day
FROM token_events
ORDER BY occurred_at DESC;

-- ============================================================================
-- Failed authentication attempts
-- ============================================================================

CREATE OR REPLACE VIEW SECURITY.FAILED_AUTH_ATTEMPTS AS
WITH failed_auth AS (
  SELECT 
    occurred_at,
    attributes:token_prefix::STRING AS token_prefix,
    attributes:client_ip::STRING AS client_ip,
    attributes:endpoint::STRING AS endpoint,
    attributes:reason::STRING AS failure_reason,
    DATE_TRUNC('hour', occurred_at) AS hour
  FROM CLAUDE_BI.ACTIVITY.EVENTS
  WHERE action = 'security.auth.failed'
    AND occurred_at >= DATEADD('day', -7, CURRENT_TIMESTAMP())
),
aggregated AS (
  SELECT 
    client_ip,
    token_prefix,
    COUNT(*) AS failure_count,
    MIN(occurred_at) AS first_attempt,
    MAX(occurred_at) AS last_attempt,
    ARRAY_AGG(DISTINCT failure_reason) AS failure_reasons,
    ARRAY_AGG(DISTINCT endpoint) AS attempted_endpoints
  FROM failed_auth
  GROUP BY client_ip, token_prefix
)
SELECT 
  client_ip,
  token_prefix,
  failure_count,
  first_attempt,
  last_attempt,
  DATEDIFF('minute', first_attempt, last_attempt) AS attack_duration_minutes,
  failure_reasons,
  attempted_endpoints,
  CASE 
    WHEN failure_count > 50 THEN 'CRITICAL'
    WHEN failure_count > 20 THEN 'HIGH'
    WHEN failure_count > 10 THEN 'MEDIUM'
    ELSE 'LOW'
  END AS threat_level,
  CASE 
    WHEN failure_count > 20 THEN 'Block IP and rotate token'
    WHEN failure_count > 10 THEN 'Investigate and monitor'
    ELSE 'Monitor'
  END AS recommended_action
FROM aggregated
ORDER BY failure_count DESC;

-- ============================================================================
-- Multi-IP token usage detection
-- ============================================================================

CREATE OR REPLACE VIEW SECURITY.MULTI_IP_TOKENS AS
WITH token_ips AS (
  SELECT 
    attributes:token_prefix::STRING AS token_prefix,
    actor_id AS username,
    attributes:client_ip::STRING AS client_ip,
    occurred_at
  FROM CLAUDE_BI.ACTIVITY.EVENTS
  WHERE action = 'system.token.used'
    AND occurred_at >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
),
ip_analysis AS (
  SELECT 
    token_prefix,
    username,
    COUNT(DISTINCT client_ip) AS unique_ip_count,
    ARRAY_AGG(DISTINCT client_ip) AS ip_list,
    MIN(occurred_at) AS first_use,
    MAX(occurred_at) AS last_use,
    COUNT(*) AS total_uses
  FROM token_ips
  GROUP BY token_prefix, username
  HAVING unique_ip_count > 1
)
SELECT 
  username,
  token_prefix,
  unique_ip_count,
  ip_list,
  first_use,
  last_use,
  total_uses,
  DATEDIFF('minute', first_use, last_use) AS usage_span_minutes,
  CASE 
    WHEN unique_ip_count > 5 THEN 'CRITICAL - Possible token compromise'
    WHEN unique_ip_count > 3 THEN 'HIGH - Suspicious activity'
    WHEN unique_ip_count > 2 THEN 'MEDIUM - Multiple locations'
    ELSE 'LOW - Normal roaming'
  END AS risk_assessment,
  CASE 
    WHEN unique_ip_count > 3 THEN 'Immediate token rotation required'
    ELSE 'Monitor activity'
  END AS action_required
FROM ip_analysis
ORDER BY unique_ip_count DESC;

-- ============================================================================
-- Replay attack monitoring
-- ============================================================================

CREATE OR REPLACE VIEW SECURITY.REPLAY_ATTACKS AS
SELECT 
  occurred_at AS detected_at,
  attributes:nonce::STRING AS replay_nonce,
  attributes:token_prefix::STRING AS token_prefix,
  attributes:client_ip::STRING AS attacker_ip,
  COUNT(*) OVER (
    PARTITION BY attributes:client_ip 
    ORDER BY occurred_at 
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS cumulative_attempts_from_ip
FROM CLAUDE_BI.ACTIVITY.EVENTS
WHERE action = 'security.replay_detected'
  AND occurred_at >= DATEADD('day', -7, CURRENT_TIMESTAMP())
ORDER BY detected_at DESC;

-- ============================================================================
-- Token lifecycle audit
-- ============================================================================

CREATE OR REPLACE VIEW SECURITY.TOKEN_LIFECYCLE AS
WITH token_events AS (
  SELECT 
    object_id AS username,
    action,
    occurred_at,
    attributes:token_prefix::STRING AS token_prefix,
    attributes:token_suffix::STRING AS token_suffix,
    attributes:reason::STRING AS reason,
    attributes:granted_by::STRING AS granted_by,
    attributes:revoked_by::STRING AS revoked_by,
    attributes:expires_at::TIMESTAMP_TZ AS expires_at
  FROM CLAUDE_BI.ACTIVITY.EVENTS
  WHERE object_type = 'user'
    AND action IN (
      'system.permission.granted',
      'system.permission.revoked',
      'system.token.rotated'
    )
),
lifecycle AS (
  SELECT 
    username,
    token_prefix || '...' || token_suffix AS token_hint,
    action AS event_type,
    occurred_at AS event_time,
    reason,
    COALESCE(granted_by, revoked_by) AS actor,
    expires_at,
    LAG(occurred_at) OVER (
      PARTITION BY username 
      ORDER BY occurred_at
    ) AS previous_event_time,
    LAG(action) OVER (
      PARTITION BY username 
      ORDER BY occurred_at
    ) AS previous_event_type
  FROM token_events
)
SELECT 
  username,
  token_hint,
  event_type,
  event_time,
  reason,
  actor,
  expires_at,
  DATEDIFF('day', previous_event_time, event_time) AS days_since_last_change,
  previous_event_type,
  CASE 
    WHEN event_type = 'system.permission.granted' AND previous_event_type = 'system.permission.revoked' 
      THEN 'Token reissued after revocation'
    WHEN event_type = 'system.token.rotated' 
      THEN 'Token rotated'
    WHEN event_type = 'system.permission.revoked' 
      THEN 'Access revoked'
    WHEN event_type = 'system.permission.granted' 
      THEN 'Initial grant'
    ELSE 'Other'
  END AS lifecycle_event
FROM lifecycle
ORDER BY username, event_time DESC;

-- ============================================================================
-- Activation tracking
-- ============================================================================

CREATE OR REPLACE VIEW SECURITY.ACTIVATION_AUDIT AS
WITH activation_events AS (
  SELECT 
    occurred_at,
    action,
    attributes:activation_code::STRING AS activation_code,
    attributes:username::STRING AS username,
    attributes:status::STRING AS status,
    attributes:created_by::STRING AS created_by,
    attributes:used_from_ip::STRING AS used_from_ip,
    attributes:activation_expires_at::TIMESTAMP_TZ AS expires_at
  FROM CLAUDE_BI.ACTIVITY.EVENTS
  WHERE action IN (
    'system.activation.created',
    'system.activation.used',
    'system.activation.expired'
  )
    AND occurred_at >= DATEADD('day', -30, CURRENT_TIMESTAMP())
)
SELECT 
  activation_code,
  username,
  MIN(CASE WHEN action = 'system.activation.created' THEN occurred_at END) AS created_at,
  MIN(CASE WHEN action = 'system.activation.created' THEN created_by END) AS created_by,
  MIN(CASE WHEN action = 'system.activation.used' THEN occurred_at END) AS used_at,
  MIN(CASE WHEN action = 'system.activation.used' THEN used_from_ip END) AS used_from_ip,
  MIN(CASE WHEN action = 'system.activation.expired' THEN occurred_at END) AS expired_at,
  expires_at,
  CASE 
    WHEN MIN(CASE WHEN action = 'system.activation.used' THEN occurred_at END) IS NOT NULL THEN 'USED'
    WHEN MIN(CASE WHEN action = 'system.activation.expired' THEN occurred_at END) IS NOT NULL THEN 'EXPIRED'
    WHEN expires_at < CURRENT_TIMESTAMP() THEN 'EXPIRED'
    ELSE 'PENDING'
  END AS final_status,
  DATEDIFF(
    'minute',
    MIN(CASE WHEN action = 'system.activation.created' THEN occurred_at END),
    MIN(CASE WHEN action = 'system.activation.used' THEN occurred_at END)
  ) AS time_to_activation_minutes
FROM activation_events
GROUP BY activation_code, username, expires_at
ORDER BY created_at DESC;

-- ============================================================================
-- Security dashboard summary
-- ============================================================================

CREATE OR REPLACE VIEW SECURITY.DASHBOARD AS
WITH metrics AS (
  SELECT 
    -- Failed auth attempts
    (SELECT COUNT(*) 
     FROM SECURITY.FAILED_AUTH_ATTEMPTS 
     WHERE last_attempt >= DATEADD('hour', -1, CURRENT_TIMESTAMP())) AS failed_auth_1h,
    
    -- Multi-IP tokens
    (SELECT COUNT(*) 
     FROM SECURITY.MULTI_IP_TOKENS 
     WHERE unique_ip_count > 3) AS suspicious_tokens,
    
    -- Replay attacks
    (SELECT COUNT(*) 
     FROM SECURITY.REPLAY_ATTACKS 
     WHERE detected_at >= DATEADD('hour', -24, CURRENT_TIMESTAMP())) AS replay_attempts_24h,
    
    -- Active tokens
    (SELECT COUNT(*) 
     FROM ADMIN.ACTIVE_TOKENS 
     WHERE status = 'ACTIVE') AS active_tokens,
    
    -- Expiring tokens
    (SELECT COUNT(*) 
     FROM ADMIN.EXPIRING_TOKENS 
     WHERE expiry_status = 'EXPIRING_SOON') AS expiring_soon,
    
    -- Pending activations
    (SELECT COUNT(*) 
     FROM ADMIN.PENDING_ACTIVATIONS 
     WHERE status = 'PENDING') AS pending_activations
)
SELECT 
  failed_auth_1h,
  suspicious_tokens,
  replay_attempts_24h,
  active_tokens,
  expiring_soon,
  pending_activations,
  CASE 
    WHEN failed_auth_1h > 100 OR suspicious_tokens > 5 OR replay_attempts_24h > 10 THEN 'CRITICAL'
    WHEN failed_auth_1h > 50 OR suspicious_tokens > 2 OR replay_attempts_24h > 5 THEN 'HIGH'
    WHEN failed_auth_1h > 20 OR suspicious_tokens > 0 OR replay_attempts_24h > 0 THEN 'MEDIUM'
    ELSE 'LOW'
  END AS overall_threat_level,
  CURRENT_TIMESTAMP() AS dashboard_updated_at
FROM metrics;

-- ============================================================================
-- Create security alerts
-- ============================================================================

-- Alert for suspicious token usage
CREATE OR REPLACE ALERT SECURITY.SUSPICIOUS_TOKEN_ALERT
  WAREHOUSE = ALERT_WH
  SCHEDULE = '5 MINUTE'
  IF (EXISTS (
    SELECT 1 FROM SECURITY.MULTI_IP_TOKENS
    WHERE unique_ip_count > 3
      AND last_use >= DATEADD('minute', -5, CURRENT_TIMESTAMP())
  ))
  THEN CALL SYSTEM$SEND_EMAIL(
    'security@company.com',
    'CRITICAL: Suspicious Token Usage Detected',
    'A token has been used from more than 3 different IPs in the last 5 minutes. Check SECURITY.MULTI_IP_TOKENS for details.'
  );

-- Alert for high volume of failed auth
CREATE OR REPLACE ALERT SECURITY.FAILED_AUTH_ALERT
  WAREHOUSE = ALERT_WH
  SCHEDULE = '15 MINUTE'
  IF (EXISTS (
    SELECT 1 
    FROM SECURITY.FAILED_AUTH_ATTEMPTS
    WHERE failure_count > 20
      AND last_attempt >= DATEADD('minute', -15, CURRENT_TIMESTAMP())
  ))
  THEN CALL SYSTEM$SEND_EMAIL(
    'security@company.com',
    'HIGH: Multiple Failed Authentication Attempts',
    'More than 20 failed authentication attempts detected. Check SECURITY.FAILED_AUTH_ATTEMPTS for details.'
  );

-- Resume alerts
ALTER ALERT SECURITY.SUSPICIOUS_TOKEN_ALERT RESUME;
ALTER ALERT SECURITY.FAILED_AUTH_ALERT RESUME;

-- ============================================================================
-- Grant permissions
-- ============================================================================

-- Admin role can view all security monitoring
GRANT SELECT ON ALL VIEWS IN SCHEMA CLAUDE_BI.SECURITY TO ROLE MCP_ADMIN_ROLE;

-- Create read-only security monitoring role
CREATE ROLE IF NOT EXISTS SECURITY_MONITOR_ROLE
  COMMENT = 'Read-only access to security monitoring views';

GRANT USAGE ON DATABASE CLAUDE_BI TO ROLE SECURITY_MONITOR_ROLE;
GRANT USAGE ON SCHEMA CLAUDE_BI.SECURITY TO ROLE SECURITY_MONITOR_ROLE;
GRANT SELECT ON ALL VIEWS IN SCHEMA CLAUDE_BI.SECURITY TO ROLE SECURITY_MONITOR_ROLE;

-- ============================================================================
-- Summary
-- ============================================================================

SELECT 
  'Security monitoring configured' AS status,
  (SELECT COUNT(*) FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA = 'SECURITY') AS monitoring_views,
  (SELECT overall_threat_level FROM SECURITY.DASHBOARD) AS current_threat_level,
  (SELECT COUNT(*) FROM INFORMATION_SCHEMA.ALERTS WHERE SCHEMA_NAME = 'SECURITY' AND STATE = 'STARTED') AS active_alerts;