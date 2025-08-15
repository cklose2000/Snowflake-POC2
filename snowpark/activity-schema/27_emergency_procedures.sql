-- ============================================================================
-- 27_emergency_procedures.sql
-- Emergency response procedures for security incidents
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE CLAUDE_BI;
USE SCHEMA MCP;

-- ============================================================================
-- Emergency kill switch - revoke all tokens
-- ============================================================================

CREATE OR REPLACE PROCEDURE ADMIN.EMERGENCY_REVOKE_ALL(
  reason STRING,
  confirm_code STRING
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
  revoked_count INT DEFAULT 0;
  active_users ARRAY;
  username STRING;
  event_id STRING;
BEGIN
  -- Safety check - require confirmation code
  IF (confirm_code != 'EMERGENCY-REVOKE-ALL-' || TO_CHAR(CURRENT_DATE(), 'YYYYMMDD')) THEN
    RETURN OBJECT_CONSTRUCT(
      'success', FALSE,
      'error', 'Invalid confirmation code. Use: EMERGENCY-REVOKE-ALL-' || TO_CHAR(CURRENT_DATE(), 'YYYYMMDD')
    );
  END IF;
  
  -- Get all active users
  active_users := (
    SELECT ARRAY_AGG(username)
    FROM ADMIN.ACTIVE_TOKENS
    WHERE status IN ('ACTIVE', 'AGING', 'SHOULD_ROTATE')
  );
  
  -- Revoke each user's permissions
  FOR i IN 0 TO ARRAY_SIZE(active_users) - 1 DO
    username := active_users[i];
    
    -- Insert revocation event
    event_id := SHA2(CONCAT_WS('|', 'emergency.revoke', username, CURRENT_TIMESTAMP()::STRING), 256);
    
    INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS VALUES (
      OBJECT_CONSTRUCT(
        'event_id', event_id,
        'action', 'system.permission.revoked',
        'occurred_at', CURRENT_TIMESTAMP(),
        'actor_id', CURRENT_USER(),
        'source', 'emergency',
        'object', OBJECT_CONSTRUCT(
          'type', 'user',
          'id', username
        ),
        'attributes', OBJECT_CONSTRUCT(
          'reason', 'EMERGENCY: ' || reason,
          'revoked_by', CURRENT_USER(),
          'emergency_action', TRUE
        )
      ),
      'EMERGENCY',
      CURRENT_TIMESTAMP()
    );
    
    revoked_count := revoked_count + 1;
  END FOR;
  
  -- Log emergency action
  INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS VALUES (
    OBJECT_CONSTRUCT(
      'event_id', SHA2(CONCAT_WS('|', 'emergency.killswitch', CURRENT_TIMESTAMP()::STRING), 256),
      'action', 'security.emergency.killswitch',
      'occurred_at', CURRENT_TIMESTAMP(),
      'actor_id', CURRENT_USER(),
      'source', 'emergency',
      'attributes', OBJECT_CONSTRUCT(
        'reason', reason,
        'users_revoked', revoked_count,
        'confirmation_code', confirm_code,
        'executed_from_ip', CURRENT_IP_ADDRESS()
      )
    ),
    'EMERGENCY',
    CURRENT_TIMESTAMP()
  );
  
  RETURN OBJECT_CONSTRUCT(
    'success', TRUE,
    'action', 'EMERGENCY_REVOKE_ALL',
    'users_revoked', revoked_count,
    'reason', reason,
    'executed_by', CURRENT_USER(),
    'executed_at', CURRENT_TIMESTAMP(),
    'message', 'All user tokens have been revoked. System is now locked down.'
  );
END;
$$;

-- ============================================================================
-- Emergency revoke specific user
-- ============================================================================

CREATE OR REPLACE PROCEDURE ADMIN.EMERGENCY_REVOKE_USER(
  username STRING,
  reason STRING,
  block_ip STRING DEFAULT NULL
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
  token_hash STRING;
  event_id STRING;
BEGIN
  -- Get current token hash
  SELECT attributes:token_hash::STRING INTO token_hash
  FROM CLAUDE_BI.ACTIVITY.EVENTS
  WHERE object_type = 'user'
    AND object_id = username
    AND action = 'system.permission.granted'
  ORDER BY occurred_at DESC
  LIMIT 1;
  
  IF (token_hash IS NULL) THEN
    RETURN OBJECT_CONSTRUCT(
      'success', FALSE,
      'error', 'User not found or no active permissions: ' || username
    );
  END IF;
  
  -- Revoke immediately
  event_id := SHA2(CONCAT_WS('|', 'emergency.revoke.user', username, CURRENT_TIMESTAMP()::STRING), 256);
  
  INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS VALUES (
    OBJECT_CONSTRUCT(
      'event_id', event_id,
      'action', 'system.permission.revoked',
      'occurred_at', CURRENT_TIMESTAMP(),
      'actor_id', CURRENT_USER(),
      'source', 'emergency',
      'object', OBJECT_CONSTRUCT(
        'type', 'user',
        'id', username
      ),
      'attributes', OBJECT_CONSTRUCT(
        'token_hash', token_hash,
        'reason', 'EMERGENCY: ' || reason,
        'revoked_by', CURRENT_USER(),
        'emergency_action', TRUE,
        'blocked_ip', block_ip
      )
    ),
    'EMERGENCY',
    CURRENT_TIMESTAMP()
  );
  
  -- If IP provided, add to blocklist
  IF (block_ip IS NOT NULL) THEN
    INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS VALUES (
      OBJECT_CONSTRUCT(
        'event_id', SHA2(CONCAT_WS('|', 'security.ip.blocked', block_ip, CURRENT_TIMESTAMP()::STRING), 256),
        'action', 'security.ip.blocked',
        'occurred_at', CURRENT_TIMESTAMP(),
        'actor_id', CURRENT_USER(),
        'source', 'emergency',
        'attributes', OBJECT_CONSTRUCT(
          'blocked_ip', block_ip,
          'reason', reason,
          'associated_user', username
        )
      ),
      'EMERGENCY',
      CURRENT_TIMESTAMP()
    );
  END IF;
  
  RETURN OBJECT_CONSTRUCT(
    'success', TRUE,
    'action', 'EMERGENCY_REVOKE_USER',
    'username', username,
    'reason', reason,
    'blocked_ip', block_ip,
    'executed_by', CURRENT_USER(),
    'executed_at', CURRENT_TIMESTAMP()
  );
END;
$$;

-- ============================================================================
-- Restore access after emergency
-- ============================================================================

CREATE OR REPLACE PROCEDURE ADMIN.RESTORE_ACCESS(
  usernames ARRAY,
  reason STRING,
  new_role_template STRING DEFAULT 'VIEWER'
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
  restored_count INT DEFAULT 0;
  failed_count INT DEFAULT 0;
  username STRING;
  result VARIANT;
  activation_results ARRAY;
BEGIN
  activation_results := ARRAY_CONSTRUCT();
  
  -- Create new activations for each user
  FOR i IN 0 TO ARRAY_SIZE(usernames) - 1 DO
    username := usernames[i];
    
    BEGIN
      -- Create activation (not direct token)
      CALL ADMIN.CREATE_ACTIVATION(
        username,
        MCP.GET_TEMPLATE_TOOLS(new_role_template),
        MCP.GET_TEMPLATE_ROWS(new_role_template),
        MCP.GET_TEMPLATE_RUNTIME(new_role_template),
        30,  -- 30 day token TTL after emergency
        1440 -- 24 hour activation window
      ) INTO result;
      
      IF (result:success = TRUE) THEN
        restored_count := restored_count + 1;
        activation_results := ARRAY_APPEND(
          activation_results,
          OBJECT_CONSTRUCT(
            'username', username,
            'activation_url', result:activation_url
          )
        );
      ELSE
        failed_count := failed_count + 1;
      END IF;
    EXCEPTION
      WHEN OTHER THEN
        failed_count := failed_count + 1;
    END;
  END FOR;
  
  -- Log restoration event
  INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS VALUES (
    OBJECT_CONSTRUCT(
      'event_id', SHA2(CONCAT_WS('|', 'emergency.restore', CURRENT_TIMESTAMP()::STRING), 256),
      'action', 'security.emergency.restore',
      'occurred_at', CURRENT_TIMESTAMP(),
      'actor_id', CURRENT_USER(),
      'source', 'emergency',
      'attributes', OBJECT_CONSTRUCT(
        'reason', reason,
        'users_restored', restored_count,
        'failed', failed_count,
        'role_template', new_role_template
      )
    ),
    'EMERGENCY',
    CURRENT_TIMESTAMP()
  );
  
  RETURN OBJECT_CONSTRUCT(
    'success', TRUE,
    'action', 'RESTORE_ACCESS',
    'restored', restored_count,
    'failed', failed_count,
    'activations', activation_results,
    'reason', reason,
    'message', 'Activation links generated. Send to users for re-authentication.'
  );
END;
$$;

-- ============================================================================
-- Audit security incident
-- ============================================================================

CREATE OR REPLACE PROCEDURE ADMIN.AUDIT_SECURITY_INCIDENT(
  incident_type STRING,
  severity STRING,
  description STRING,
  affected_users ARRAY DEFAULT NULL,
  affected_ips ARRAY DEFAULT NULL,
  actions_taken ARRAY DEFAULT NULL
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
  incident_id STRING;
BEGIN
  incident_id := 'INC_' || TO_CHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MISS');
  
  -- Log security incident
  INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS VALUES (
    OBJECT_CONSTRUCT(
      'event_id', SHA2(CONCAT_WS('|', 'security.incident', incident_id), 256),
      'action', 'security.incident.reported',
      'occurred_at', CURRENT_TIMESTAMP(),
      'actor_id', CURRENT_USER(),
      'source', 'security',
      'object', OBJECT_CONSTRUCT(
        'type', 'incident',
        'id', incident_id
      ),
      'attributes', OBJECT_CONSTRUCT(
        'incident_type', incident_type,
        'severity', severity,
        'description', description,
        'affected_users', affected_users,
        'affected_ips', affected_ips,
        'actions_taken', actions_taken,
        'reported_by', CURRENT_USER(),
        'reported_from_ip', CURRENT_IP_ADDRESS()
      )
    ),
    'SECURITY',
    CURRENT_TIMESTAMP()
  );
  
  RETURN 'Security incident logged: ' || incident_id;
END;
$$;

-- ============================================================================
-- Lock down specific IP address
-- ============================================================================

CREATE OR REPLACE PROCEDURE ADMIN.BLOCK_IP_ADDRESS(
  ip_address STRING,
  reason STRING,
  duration_hours INT DEFAULT 24
)
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
  -- Add IP to blocklist
  INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS VALUES (
    OBJECT_CONSTRUCT(
      'event_id', SHA2(CONCAT_WS('|', 'security.ip.blocked', ip_address, CURRENT_TIMESTAMP()::STRING), 256),
      'action', 'security.ip.blocked',
      'occurred_at', CURRENT_TIMESTAMP(),
      'actor_id', CURRENT_USER(),
      'source', 'security',
      'attributes', OBJECT_CONSTRUCT(
        'blocked_ip', ip_address,
        'reason', reason,
        'duration_hours', duration_hours,
        'expires_at', DATEADD('hour', duration_hours, CURRENT_TIMESTAMP()),
        'blocked_by', CURRENT_USER()
      )
    ),
    'SECURITY',
    CURRENT_TIMESTAMP()
  );
  
  RETURN 'IP address blocked: ' || ip_address || ' for ' || duration_hours || ' hours';
END;
$$;

-- ============================================================================
-- Emergency status view
-- ============================================================================

CREATE OR REPLACE VIEW ADMIN.EMERGENCY_STATUS AS
WITH recent_emergencies AS (
  SELECT 
    occurred_at,
    action,
    actor_id,
    attributes:reason::STRING AS reason,
    attributes:users_revoked::NUMBER AS users_affected,
    attributes:confirmation_code::STRING AS confirmation_code
  FROM CLAUDE_BI.ACTIVITY.EVENTS
  WHERE action IN (
    'security.emergency.killswitch',
    'security.emergency.restore',
    'security.incident.reported'
  )
    AND occurred_at >= DATEADD('day', -7, CURRENT_TIMESTAMP())
),
blocked_ips AS (
  SELECT 
    attributes:blocked_ip::STRING AS ip_address,
    attributes:reason::STRING AS block_reason,
    attributes:expires_at::TIMESTAMP_TZ AS expires_at,
    occurred_at AS blocked_at
  FROM CLAUDE_BI.ACTIVITY.EVENTS
  WHERE action = 'security.ip.blocked'
    AND attributes:expires_at > CURRENT_TIMESTAMP()
)
SELECT 
  'Emergency Actions' AS category,
  COUNT(*) AS count,
  MAX(occurred_at) AS last_action
FROM recent_emergencies

UNION ALL

SELECT 
  'Blocked IPs' AS category,
  COUNT(*) AS count,
  MAX(blocked_at) AS last_action
FROM blocked_ips

UNION ALL

SELECT 
  'Active Tokens' AS category,
  COUNT(*) AS count,
  NULL AS last_action
FROM ADMIN.ACTIVE_TOKENS
WHERE status = 'ACTIVE';

-- ============================================================================
-- Create emergency response checklist view
-- ============================================================================

CREATE OR REPLACE VIEW ADMIN.EMERGENCY_CHECKLIST AS
SELECT 
  'Security Incident Response Checklist' AS title,
  ARRAY_CONSTRUCT(
    '1. Identify scope of compromise',
    '2. Run SECURITY.DASHBOARD to assess threat level',
    '3. Check SECURITY.MULTI_IP_TOKENS for compromised tokens',
    '4. Review SECURITY.FAILED_AUTH_ATTEMPTS for attack patterns',
    '5. For compromised user: CALL ADMIN.EMERGENCY_REVOKE_USER(username, reason)',
    '6. For system-wide breach: CALL ADMIN.EMERGENCY_REVOKE_ALL(reason, confirmation)',
    '7. Block attacking IPs: CALL ADMIN.BLOCK_IP_ADDRESS(ip, reason)',
    '8. Document incident: CALL ADMIN.AUDIT_SECURITY_INCIDENT(...)',
    '9. After resolution: CALL ADMIN.RESTORE_ACCESS(users, reason)',
    '10. Review and update security procedures'
  ) AS steps,
  OBJECT_CONSTRUCT(
    'emergency_revoke_all_code', 'EMERGENCY-REVOKE-ALL-' || TO_CHAR(CURRENT_DATE(), 'YYYYMMDD'),
    'current_threat_level', (SELECT overall_threat_level FROM SECURITY.DASHBOARD),
    'suspicious_tokens', (SELECT COUNT(*) FROM SECURITY.MULTI_IP_TOKENS WHERE unique_ip_count > 3),
    'active_incidents', (SELECT COUNT(*) FROM ADMIN.EMERGENCY_STATUS WHERE category = 'Emergency Actions')
  ) AS current_status;

-- ============================================================================
-- Grant permissions
-- ============================================================================

-- Only ACCOUNTADMIN can execute emergency procedures
GRANT EXECUTE ON PROCEDURE ADMIN.EMERGENCY_REVOKE_ALL(STRING, STRING) TO ROLE ACCOUNTADMIN;
GRANT EXECUTE ON PROCEDURE ADMIN.EMERGENCY_REVOKE_USER(STRING, STRING, STRING) TO ROLE ACCOUNTADMIN;
GRANT EXECUTE ON PROCEDURE ADMIN.RESTORE_ACCESS(ARRAY, STRING, STRING) TO ROLE ACCOUNTADMIN;
GRANT EXECUTE ON PROCEDURE ADMIN.AUDIT_SECURITY_INCIDENT(STRING, STRING, STRING, ARRAY, ARRAY, ARRAY) TO ROLE MCP_ADMIN_ROLE;
GRANT EXECUTE ON PROCEDURE ADMIN.BLOCK_IP_ADDRESS(STRING, STRING, INT) TO ROLE MCP_ADMIN_ROLE;

GRANT SELECT ON VIEW ADMIN.EMERGENCY_STATUS TO ROLE MCP_ADMIN_ROLE;
GRANT SELECT ON VIEW ADMIN.EMERGENCY_CHECKLIST TO ROLE MCP_ADMIN_ROLE;

-- ============================================================================
-- Test emergency procedures (with safety)
-- ============================================================================

-- View emergency checklist
SELECT * FROM ADMIN.EMERGENCY_CHECKLIST;

-- Check emergency status
SELECT * FROM ADMIN.EMERGENCY_STATUS;

-- Example: How to use emergency revoke (DO NOT RUN IN PRODUCTION)
-- CALL ADMIN.EMERGENCY_REVOKE_ALL('Test emergency', 'EMERGENCY-REVOKE-ALL-20240115');

-- ============================================================================
-- Summary
-- ============================================================================

SELECT 
  'Emergency procedures configured' AS status,
  'Use ADMIN.EMERGENCY_CHECKLIST for incident response' AS checklist,
  'Confirmation code for today: EMERGENCY-REVOKE-ALL-' || TO_CHAR(CURRENT_DATE(), 'YYYYMMDD') AS killswitch_code;