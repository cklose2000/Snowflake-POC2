-- ============================================================================
-- 25_token_lifecycle.sql
-- Token lifecycle management with session tracking and suspicious activity detection
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE CLAUDE_BI;
USE SCHEMA MCP;

-- ============================================================================
-- Enhanced MCP handler with token usage tracking
-- ============================================================================

CREATE OR REPLACE PROCEDURE MCP.HANDLE_REQUEST_WITH_TRACKING(
  endpoint STRING,
  payload VARIANT,
  auth_token STRING
)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
const SF = snowflake;

try {
  const startTime = Date.now();
  
  // 1. Hash the provided token
  const tokenHash = SF.createStatement({
    sqlText: "SELECT MCP.HASH_TOKEN_WITH_PEPPER(?)",
    binds: [AUTH_TOKEN]
  }).execute().next() ? 
    SF.createStatement({
      sqlText: "SELECT MCP.HASH_TOKEN_WITH_PEPPER(?)",
      binds: [AUTH_TOKEN]
    }).execute().getColumnValue(1) : null;
  
  if (!tokenHash) {
    throw new Error('Failed to hash token');
  }
  
  // 2. Extract token prefix for tracking
  const tokenPrefix = AUTH_TOKEN.substring(0, 8);
  
  // 3. Get client IP for security tracking
  const clientIP = SF.createStatement({
    sqlText: "SELECT CURRENT_IP_ADDRESS()"
  }).execute().next() ? 
    SF.createStatement({
      sqlText: "SELECT CURRENT_IP_ADDRESS()"
    }).execute().getColumnValue(1) : 'unknown';
  
  // 4. Validate nonce for replay protection
  const nonce = PAYLOAD.nonce;
  if (!nonce) {
    throw new Error('Missing nonce in request');
  }
  
  // Check nonce uniqueness
  const nonceCheckSQL = `
    SELECT COUNT(*) AS used_count
    FROM CLAUDE_BI.ACTIVITY.EVENTS
    WHERE action = 'mcp.request.processed'
      AND attributes:nonce::STRING = :1
      AND occurred_at >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
  `;
  
  const nonceRS = SF.createStatement({
    sqlText: nonceCheckSQL,
    binds: [nonce]
  }).execute();
  nonceRS.next();
  
  if (nonceRS.getColumnValue('USED_COUNT') > 0) {
    // Log replay attempt
    SF.createStatement({
      sqlText: `
        INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS VALUES (
          OBJECT_CONSTRUCT(
            'event_id', SHA2(CONCAT_WS('|', 'security.replay', :1, :2), 256),
            'action', 'security.replay_detected',
            'occurred_at', CURRENT_TIMESTAMP(),
            'attributes', OBJECT_CONSTRUCT(
              'nonce', :2,
              'token_prefix', :3,
              'client_ip', :4
            )
          ),
          'SECURITY',
          CURRENT_TIMESTAMP()
        )
      `,
      binds: [tokenHash, nonce, tokenPrefix, clientIP]
    }).execute();
    
    throw new Error('Replay detected: nonce already used');
  }
  
  // 5. Get user permissions with enhanced validation
  const permSQL = `
    WITH latest_perm AS (
      SELECT
        object_id AS username,
        action,
        attributes:allowed_tools::ARRAY AS allowed_tools,
        attributes:max_rows::NUMBER AS max_rows,
        attributes:daily_runtime_seconds::NUMBER AS runtime_budget,
        attributes:expires_at::TIMESTAMP_TZ AS expires_at,
        attributes:token_prefix::STRING AS token_prefix,
        occurred_at
      FROM CLAUDE_BI.ACTIVITY.EVENTS
      WHERE object_type = 'user'
        AND action IN ('system.permission.granted', 'system.permission.revoked')
        AND attributes:token_hash::STRING = :1
      QUALIFY ROW_NUMBER() OVER (
        ORDER BY occurred_at DESC
      ) = 1
    )
    SELECT * FROM latest_perm
  `;
  
  const permRS = SF.createStatement({
    sqlText: permSQL,
    binds: [tokenHash]
  }).execute();
  
  if (!permRS.next()) {
    // Log failed auth attempt
    SF.createStatement({
      sqlText: `
        INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS VALUES (
          OBJECT_CONSTRUCT(
            'event_id', SHA2(CONCAT_WS('|', 'security.auth.failed', :1, CURRENT_TIMESTAMP()::STRING), 256),
            'action', 'security.auth.failed',
            'occurred_at', CURRENT_TIMESTAMP(),
            'attributes', OBJECT_CONSTRUCT(
              'token_prefix', :1,
              'client_ip', :2,
              'endpoint', :3,
              'reason', 'Invalid token'
            )
          ),
          'SECURITY',
          CURRENT_TIMESTAMP()
        )
      `,
      binds: [tokenPrefix, clientIP, ENDPOINT]
    }).execute();
    
    throw new Error('Invalid token - no permissions found');
  }
  
  const userPerms = {
    username: permRS.getColumnValue('USERNAME'),
    action: permRS.getColumnValue('ACTION'),
    allowedTools: permRS.getColumnValue('ALLOWED_TOOLS'),
    maxRows: permRS.getColumnValue('MAX_ROWS') || 1000,
    runtimeBudget: permRS.getColumnValue('RUNTIME_BUDGET') || 3600,
    expiresAt: permRS.getColumnValue('EXPIRES_AT'),
    tokenPrefix: permRS.getColumnValue('TOKEN_PREFIX')
  };
  
  // 6. Validate permission state
  if (userPerms.action === 'system.permission.revoked') {
    throw new Error('Access revoked for user: ' + userPerms.username);
  }
  
  if (userPerms.expiresAt && new Date(userPerms.expiresAt) < new Date()) {
    throw new Error('Token expired for user: ' + userPerms.username);
  }
  
  // 7. Log token usage for security monitoring
  SF.createStatement({
    sqlText: `
      INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS VALUES (
        OBJECT_CONSTRUCT(
          'event_id', SHA2(CONCAT_WS('|', 'token.used', :1, :2, CURRENT_TIMESTAMP()::STRING), 256),
          'action', 'system.token.used',
          'occurred_at', CURRENT_TIMESTAMP(),
          'actor_id', :3,
          'attributes', OBJECT_CONSTRUCT(
            'token_prefix', :1,
            'client_ip', :2,
            'endpoint', :4,
            'tool', :5
          )
        ),
        'SYSTEM',
        CURRENT_TIMESTAMP()
      )
    `,
    binds: [
      userPerms.tokenPrefix,
      clientIP,
      userPerms.username,
      ENDPOINT,
      PAYLOAD.name || null
    ]
  }).execute();
  
  // 8. Check for suspicious activity (multiple IPs)
  const ipCheckSQL = `
    SELECT 
      COUNT(DISTINCT attributes:client_ip) AS unique_ips,
      ARRAY_AGG(DISTINCT attributes:client_ip) AS ip_list
    FROM CLAUDE_BI.ACTIVITY.EVENTS
    WHERE action = 'system.token.used'
      AND attributes:token_prefix = :1
      AND occurred_at >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
  `;
  
  const ipRS = SF.createStatement({
    sqlText: ipCheckSQL,
    binds: [userPerms.tokenPrefix]
  }).execute();
  ipRS.next();
  
  const uniqueIPs = ipRS.getColumnValue('UNIQUE_IPS');
  
  if (uniqueIPs > 3) {
    // Token used from too many IPs - possible compromise
    SF.createStatement({
      sqlText: `
        INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS VALUES (
          OBJECT_CONSTRUCT(
            'event_id', SHA2(CONCAT_WS('|', 'security.suspicious', :1, CURRENT_TIMESTAMP()::STRING), 256),
            'action', 'security.token.suspicious',
            'occurred_at', CURRENT_TIMESTAMP(),
            'actor_id', :2,
            'attributes', OBJECT_CONSTRUCT(
              'token_prefix', :1,
              'unique_ips_1h', :3,
              'ip_list', :4,
              'alert_level', 'HIGH'
            )
          ),
          'SECURITY',
          CURRENT_TIMESTAMP()
        )
      `,
      binds: [
        userPerms.tokenPrefix,
        userPerms.username,
        uniqueIPs,
        ipRS.getColumnValue('IP_LIST')
      ]
    }).execute();
  }
  
  // 9. Execute the actual request (existing logic)
  // ... [Rest of the existing HANDLE_REQUEST logic] ...
  
  // For this example, return success
  const result = {
    success: true,
    user: userPerms.username,
    endpoint: ENDPOINT
  };
  
  // 10. Audit successful request
  const executionMs = Date.now() - startTime;
  
  SF.createStatement({
    sqlText: `
      INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS VALUES (
        OBJECT_CONSTRUCT(
          'event_id', SHA2(CONCAT_WS('|', 'mcp.exec', :1, :2), 256),
          'action', 'mcp.request.processed',
          'occurred_at', CURRENT_TIMESTAMP(),
          'actor_id', :1,
          'attributes', OBJECT_CONSTRUCT(
            'endpoint', :3,
            'tool', :4,
            'nonce', :2,
            'execution_ms', :5,
            'client_ip', :6,
            'token_prefix', :7
          )
        ),
        'MCP',
        CURRENT_TIMESTAMP()
      )
    `,
    binds: [
      userPerms.username,
      nonce,
      ENDPOINT,
      PAYLOAD.name || null,
      executionMs,
      clientIP,
      userPerms.tokenPrefix
    ]
  }).execute();
  
  return result;
  
} catch (err) {
  // Audit failed request
  SF.createStatement({
    sqlText: `
      INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS VALUES (
        OBJECT_CONSTRUCT(
          'event_id', SHA2(CONCAT_WS('|', 'mcp.fail', CURRENT_TIMESTAMP()::STRING), 256),
          'action', 'mcp.request.failed',
          'occurred_at', CURRENT_TIMESTAMP(),
          'attributes', OBJECT_CONSTRUCT(
            'endpoint', :1,
            'error', :2,
            'nonce', :3
          )
        ),
        'MCP',
        CURRENT_TIMESTAMP()
      )
    `,
    binds: [ENDPOINT, err.toString(), PAYLOAD.nonce || null]
  }).execute();
  
  return {
    success: false,
    error: err.toString()
  };
}
$$;

-- ============================================================================
-- Create session management views
-- ============================================================================

CREATE OR REPLACE VIEW MCP.TOKEN_SESSIONS AS
WITH token_usage AS (
  SELECT 
    attributes:token_prefix::STRING AS token_prefix,
    actor_id AS username,
    attributes:client_ip::STRING AS client_ip,
    occurred_at AS used_at,
    attributes:endpoint::STRING AS endpoint,
    attributes:tool::STRING AS tool
  FROM CLAUDE_BI.ACTIVITY.EVENTS
  WHERE action = 'system.token.used'
    AND occurred_at >= DATEADD('day', -7, CURRENT_TIMESTAMP())
),
session_summary AS (
  SELECT 
    token_prefix,
    username,
    MIN(used_at) AS session_start,
    MAX(used_at) AS session_end,
    COUNT(*) AS request_count,
    COUNT(DISTINCT client_ip) AS unique_ips,
    ARRAY_AGG(DISTINCT client_ip) AS ip_list,
    COUNT(DISTINCT endpoint) AS unique_endpoints,
    COUNT(DISTINCT tool) AS unique_tools
  FROM token_usage
  GROUP BY token_prefix, username
)
SELECT 
  username,
  token_prefix,
  session_start,
  session_end,
  DATEDIFF('minute', session_start, session_end) AS session_duration_minutes,
  request_count,
  unique_ips,
  ip_list,
  unique_endpoints,
  unique_tools,
  ROUND(request_count::FLOAT / NULLIF(DATEDIFF('minute', session_start, session_end), 0), 2) AS requests_per_minute,
  CASE 
    WHEN unique_ips > 3 THEN 'SUSPICIOUS'
    WHEN unique_ips > 1 THEN 'MULTI_IP'
    ELSE 'NORMAL'
  END AS session_status
FROM session_summary
ORDER BY session_end DESC;

-- ============================================================================
-- Create automated token rotation procedure
-- ============================================================================

CREATE OR REPLACE PROCEDURE ADMIN.BULK_ROTATE_TOKENS(
  age_threshold_days INT DEFAULT 90,
  dry_run BOOLEAN DEFAULT TRUE
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
  tokens_to_rotate ARRAY;
  rotated_count INT DEFAULT 0;
  failed_count INT DEFAULT 0;
  username STRING;
  result VARIANT;
BEGIN
  -- Find tokens that need rotation
  tokens_to_rotate := (
    SELECT ARRAY_AGG(username)
    FROM ADMIN.ACTIVE_TOKENS
    WHERE status IN ('SHOULD_ROTATE', 'AGING')
      AND age_days >= age_threshold_days
  );
  
  IF (dry_run) THEN
    RETURN OBJECT_CONSTRUCT(
      'dry_run', TRUE,
      'tokens_to_rotate', tokens_to_rotate,
      'count', ARRAY_SIZE(tokens_to_rotate)
    );
  END IF;
  
  -- Rotate each token
  FOR i IN 0 TO ARRAY_SIZE(tokens_to_rotate) - 1 DO
    username := tokens_to_rotate[i];
    
    BEGIN
      CALL ADMIN.ROTATE_USER_TOKEN(
        username,
        'Automated rotation - token age > ' || age_threshold_days || ' days'
      ) INTO result;
      
      IF (result:success = TRUE) THEN
        rotated_count := rotated_count + 1;
      ELSE
        failed_count := failed_count + 1;
      END IF;
    EXCEPTION
      WHEN OTHER THEN
        failed_count := failed_count + 1;
    END;
  END FOR;
  
  RETURN OBJECT_CONSTRUCT(
    'success', TRUE,
    'rotated', rotated_count,
    'failed', failed_count,
    'total', ARRAY_SIZE(tokens_to_rotate)
  );
END;
$$;

-- ============================================================================
-- Create suspicious activity monitoring
-- ============================================================================

CREATE OR REPLACE VIEW SECURITY.SUSPICIOUS_ACTIVITY AS
WITH recent_activity AS (
  SELECT 
    action,
    actor_id AS username,
    attributes:token_prefix::STRING AS token_prefix,
    attributes:client_ip::STRING AS client_ip,
    attributes:unique_ips_1h::NUMBER AS unique_ips,
    attributes:alert_level::STRING AS alert_level,
    occurred_at
  FROM CLAUDE_BI.ACTIVITY.EVENTS
  WHERE action IN (
    'security.token.suspicious',
    'security.replay_detected',
    'security.auth.failed'
  )
    AND occurred_at >= DATEADD('day', -7, CURRENT_TIMESTAMP())
),
aggregated AS (
  SELECT 
    username,
    token_prefix,
    action,
    COUNT(*) AS incident_count,
    MAX(occurred_at) AS last_incident,
    ARRAY_AGG(DISTINCT client_ip) AS client_ips,
    MAX(alert_level) AS max_alert_level
  FROM recent_activity
  GROUP BY username, token_prefix, action
)
SELECT 
  username,
  token_prefix,
  action AS incident_type,
  incident_count,
  last_incident,
  client_ips,
  max_alert_level,
  CASE 
    WHEN incident_count > 10 THEN 'CRITICAL'
    WHEN incident_count > 5 THEN 'HIGH'
    WHEN incident_count > 2 THEN 'MEDIUM'
    ELSE 'LOW'
  END AS risk_level,
  CASE 
    WHEN incident_count > 10 THEN 'Immediate token rotation required'
    WHEN incident_count > 5 THEN 'Investigate and consider rotation'
    WHEN incident_count > 2 THEN 'Monitor closely'
    ELSE 'Normal monitoring'
  END AS recommended_action
FROM aggregated
ORDER BY 
  CASE max_alert_level
    WHEN 'CRITICAL' THEN 1
    WHEN 'HIGH' THEN 2
    WHEN 'MEDIUM' THEN 3
    ELSE 4
  END,
  incident_count DESC;

-- ============================================================================
-- Create token expiration monitoring
-- ============================================================================

CREATE OR REPLACE VIEW ADMIN.EXPIRING_TOKENS AS
SELECT 
  username,
  token_prefix || '...' || token_suffix AS token_hint,
  expires_at,
  DATEDIFF('day', CURRENT_TIMESTAMP(), expires_at) AS days_until_expiry,
  granted_by,
  CASE 
    WHEN expires_at < CURRENT_TIMESTAMP() THEN 'EXPIRED'
    WHEN DATEDIFF('day', CURRENT_TIMESTAMP(), expires_at) <= 7 THEN 'EXPIRING_SOON'
    WHEN DATEDIFF('day', CURRENT_TIMESTAMP(), expires_at) <= 30 THEN 'EXPIRING'
    ELSE 'ACTIVE'
  END AS expiry_status,
  'Rotate token for ' || username AS action_required
FROM ADMIN.ACTIVE_TOKENS
WHERE status = 'ACTIVE'
  AND expires_at < DATEADD('day', 30, CURRENT_TIMESTAMP())
ORDER BY expires_at;

-- ============================================================================
-- Create scheduled tasks for lifecycle management
-- ============================================================================

-- Task for automated token rotation
CREATE OR REPLACE TASK ADMIN.AUTO_ROTATE_TOKENS_TASK
  WAREHOUSE = ALERT_WH
  SCHEDULE = 'USING CRON 0 2 * * 0 UTC'  -- Weekly on Sunday at 2 AM UTC
  COMMENT = 'Automatically rotate old tokens'
AS
  CALL ADMIN.BULK_ROTATE_TOKENS(90, FALSE);  -- Rotate tokens older than 90 days

-- Task for expiration notifications
CREATE OR REPLACE TASK ADMIN.TOKEN_EXPIRY_ALERT_TASK
  WAREHOUSE = ALERT_WH
  SCHEDULE = 'USING CRON 0 9 * * * UTC'  -- Daily at 9 AM UTC
  COMMENT = 'Alert on expiring tokens'
AS
  INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS
  SELECT 
    OBJECT_CONSTRUCT(
      'event_id', SHA2(CONCAT_WS('|', 'alert.expiry', username, CURRENT_TIMESTAMP()::STRING), 256),
      'action', 'system.alert.token_expiring',
      'occurred_at', CURRENT_TIMESTAMP(),
      'actor_id', 'system',
      'attributes', OBJECT_CONSTRUCT(
        'username', username,
        'token_prefix', SPLIT_PART(token_hint, '...', 1),
        'days_until_expiry', days_until_expiry,
        'expires_at', expires_at
      )
    ),
    'SYSTEM',
    CURRENT_TIMESTAMP()
  FROM ADMIN.EXPIRING_TOKENS
  WHERE expiry_status = 'EXPIRING_SOON';

-- Resume tasks
ALTER TASK ADMIN.AUTO_ROTATE_TOKENS_TASK RESUME;
ALTER TASK ADMIN.TOKEN_EXPIRY_ALERT_TASK RESUME;

-- ============================================================================
-- Grant permissions
-- ============================================================================

GRANT EXECUTE ON PROCEDURE MCP.HANDLE_REQUEST_WITH_TRACKING(STRING, VARIANT, STRING) TO ROLE MCP_SERVICE_ROLE;
GRANT EXECUTE ON PROCEDURE ADMIN.BULK_ROTATE_TOKENS(INT, BOOLEAN) TO ROLE MCP_ADMIN_ROLE;

GRANT SELECT ON VIEW MCP.TOKEN_SESSIONS TO ROLE MCP_ADMIN_ROLE;
GRANT SELECT ON VIEW SECURITY.SUSPICIOUS_ACTIVITY TO ROLE MCP_ADMIN_ROLE;
GRANT SELECT ON VIEW ADMIN.EXPIRING_TOKENS TO ROLE MCP_ADMIN_ROLE;

-- ============================================================================
-- Summary
-- ============================================================================

SELECT 
  'Token lifecycle management configured' AS status,
  (SELECT COUNT(*) FROM ADMIN.ACTIVE_TOKENS WHERE status = 'ACTIVE') AS active_tokens,
  (SELECT COUNT(*) FROM ADMIN.EXPIRING_TOKENS WHERE expiry_status = 'EXPIRING_SOON') AS expiring_soon,
  (SELECT COUNT(*) FROM SECURITY.SUSPICIOUS_ACTIVITY WHERE risk_level IN ('HIGH', 'CRITICAL')) AS suspicious_tokens;