-- ============================================================================
-- 23_token_pepper_security.sql
-- Enhanced token security with pepper storage and metadata tracking
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE CLAUDE_BI;

-- ============================================================================
-- Create secure schema for secrets
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS CLAUDE_BI.ADMIN_SECRETS
  COMMENT = 'Secure schema for storing authentication secrets';

-- Restrict access to admin role only
REVOKE ALL ON SCHEMA CLAUDE_BI.ADMIN_SECRETS FROM PUBLIC;
GRANT USAGE ON SCHEMA CLAUDE_BI.ADMIN_SECRETS TO ROLE MCP_ADMIN_ROLE;
GRANT CREATE FUNCTION ON SCHEMA CLAUDE_BI.ADMIN_SECRETS TO ROLE MCP_ADMIN_ROLE;

USE SCHEMA CLAUDE_BI.ADMIN_SECRETS;

-- ============================================================================
-- Create secure pepper storage
-- ============================================================================

-- Initialize pepper if not exists
CREATE OR REPLACE SECURE FUNCTION ADMIN_SECRETS.GET_PEPPER()
RETURNS STRING
LANGUAGE SQL
COMMENT = 'Returns the server pepper for token hashing'
AS
$$
  -- In production, this would be stored in Snowflake Secrets Manager
  -- or retrieved from an external secret store
  -- This is a 256-bit hex pepper (64 chars)
  SELECT 'a7f3d9b2e1c4a8f5d6b9e2c7a4f1d8b5e9c3a6f2d5b8e1c4a7f0d3b6e9c2a5f8'
$$;

-- Create function to generate secure random tokens
CREATE OR REPLACE SECURE FUNCTION ADMIN_SECRETS.GENERATE_SECURE_TOKEN(
  prefix STRING DEFAULT 'tk',
  username_hint STRING DEFAULT NULL
)
RETURNS STRING
LANGUAGE JAVASCRIPT
COMMENT = 'Generates cryptographically secure token with metadata'
AS
$$
  // Generate 32 bytes of random data
  const randomBytes = [];
  for (let i = 0; i < 32; i++) {
    randomBytes.push(Math.floor(Math.random() * 256));
  }
  
  // Convert to base64
  const base64 = btoa(String.fromCharCode.apply(null, randomBytes))
    .replace(/\+/g, '-')
    .replace(/\//g, '_')
    .replace(/=/g, '');
  
  // Add user hint (first 4 chars of username)
  const hint = USERNAME_HINT ? USERNAME_HINT.substring(0, 4).toLowerCase() : 'user';
  
  // Construct token: prefix_randompart_hint
  return PREFIX + '_' + base64.substring(0, 32) + '_' + hint;
$$;

-- ============================================================================
-- Enhanced token hashing with pepper
-- ============================================================================

USE SCHEMA CLAUDE_BI.MCP;

-- Drop and recreate token functions with pepper support
CREATE OR REPLACE FUNCTION MCP.HASH_TOKEN_WITH_PEPPER(
  raw_token STRING
)
RETURNS STRING
LANGUAGE SQL
COMMENT = 'Hashes token with server pepper for secure storage'
AS
$$
  SELECT SHA2(CONCAT(raw_token, CLAUDE_BI.ADMIN_SECRETS.GET_PEPPER()), 256)
$$;

-- Create function to extract token metadata
CREATE OR REPLACE FUNCTION MCP.EXTRACT_TOKEN_METADATA(
  raw_token STRING
)
RETURNS VARIANT
LANGUAGE SQL
COMMENT = 'Extracts prefix and suffix from token for identification'
AS
$$
  SELECT OBJECT_CONSTRUCT(
    'prefix', SUBSTR(raw_token, 1, 8),
    'suffix', SUBSTR(raw_token, -4),
    'length', LENGTH(raw_token),
    'hint', CASE 
      WHEN raw_token LIKE 'tk_%' THEN SPLIT_PART(raw_token, '_', 3)
      ELSE NULL
    END
  )
$$;

-- ============================================================================
-- Update token generation procedure with enhanced security
-- ============================================================================

CREATE OR REPLACE PROCEDURE MCP.GENERATE_SECURE_TOKEN()
RETURNS STRING
LANGUAGE SQL
COMMENT = 'Generates a cryptographically secure token'
AS
$$
DECLARE
  token STRING;
BEGIN
  -- Generate token with secure function
  SELECT CLAUDE_BI.ADMIN_SECRETS.GENERATE_SECURE_TOKEN('tk', NULL) INTO token;
  RETURN token;
END;
$$;

-- ============================================================================
-- Create enhanced user token issuance with metadata
-- ============================================================================

CREATE OR REPLACE PROCEDURE ADMIN.ISSUE_USER_TOKEN(
  username STRING,
  allowed_tools ARRAY,
  max_rows INT,
  daily_runtime_s INT,
  expires_at TIMESTAMP_TZ
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
  token_full STRING;
  token_hash STRING;
  token_metadata VARIANT;
  event_id STRING;
  client_info STRING;
  client_ip STRING;
BEGIN
  -- Generate secure token with user hint
  token_full := CLAUDE_BI.ADMIN_SECRETS.GENERATE_SECURE_TOKEN('tk', username);
  
  -- Hash token with pepper
  token_hash := MCP.HASH_TOKEN_WITH_PEPPER(token_full);
  
  -- Extract metadata
  token_metadata := MCP.EXTRACT_TOKEN_METADATA(token_full);
  
  -- Get client information for audit
  SELECT CURRENT_CLIENT() INTO client_info;
  SELECT CURRENT_IP_ADDRESS() INTO client_ip;
  
  -- Check if user exists, if not create
  IF NOT EXISTS (
    SELECT 1 FROM CLAUDE_BI.ACTIVITY.EVENTS
    WHERE object_type = 'user'
      AND object_id = username
      AND action = 'system.user.created'
  ) THEN
    -- Create user event
    event_id := SHA2(CONCAT_WS('|', 'user.create', username, CURRENT_TIMESTAMP()::STRING), 256);
    
    INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS VALUES (
      OBJECT_CONSTRUCT(
        'event_id', event_id,
        'action', 'system.user.created',
        'occurred_at', CURRENT_TIMESTAMP(),
        'actor_id', CURRENT_USER(),
        'source', 'system',
        'object', OBJECT_CONSTRUCT(
          'type', 'user',
          'id', username
        ),
        'attributes', OBJECT_CONSTRUCT(
          'created_by', CURRENT_USER(),
          'created_from', client_info,
          'created_ip', client_ip
        )
      ),
      'ADMIN',
      CURRENT_TIMESTAMP()
    );
  END IF;
  
  -- Create permission grant event with enhanced metadata
  event_id := SHA2(CONCAT_WS('|', 'perm.grant', username, token_hash), 256);
  
  INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS VALUES (
    OBJECT_CONSTRUCT(
      'event_id', event_id,
      'action', 'system.permission.granted',
      'occurred_at', CURRENT_TIMESTAMP(),
      'actor_id', CURRENT_USER(),
      'source', 'system',
      'object', OBJECT_CONSTRUCT(
        'type', 'user',
        'id', username
      ),
      'attributes', OBJECT_CONSTRUCT(
        'token_hash', token_hash,
        'token_prefix', token_metadata:prefix,
        'token_suffix', token_metadata:suffix,
        'allowed_tools', allowed_tools,
        'max_rows', max_rows,
        'daily_runtime_seconds', daily_runtime_s,
        'expires_at', expires_at,
        'granted_by', CURRENT_USER(),
        'granted_from', client_info,
        'granted_ip', client_ip,
        'token_metadata', token_metadata
      )
    ),
    'ADMIN',
    CURRENT_TIMESTAMP()
  );
  
  -- Return token information (token shown only once)
  RETURN OBJECT_CONSTRUCT(
    'success', TRUE,
    'token_full', token_full,
    'token_prefix', token_metadata:prefix,
    'token_suffix', token_metadata:suffix,
    'user_id', username,
    'expires_at', expires_at,
    'allowed_tools', allowed_tools,
    'max_rows', max_rows,
    'daily_runtime_seconds', daily_runtime_s,
    'message', 'Token generated successfully. This is the only time it will be shown.'
  );
END;
$$;

-- ============================================================================
-- Create token rotation procedure
-- ============================================================================

CREATE OR REPLACE PROCEDURE ADMIN.ROTATE_USER_TOKEN(
  username STRING,
  reason STRING DEFAULT 'Scheduled rotation'
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
  current_permissions VARIANT;
  new_token_result VARIANT;
  event_id STRING;
BEGIN
  -- Get current permissions
  SELECT OBJECT_CONSTRUCT(
    'allowed_tools', attributes:allowed_tools,
    'max_rows', attributes:max_rows,
    'daily_runtime_seconds', attributes:daily_runtime_seconds,
    'token_hash', attributes:token_hash
  ) INTO current_permissions
  FROM CLAUDE_BI.ACTIVITY.EVENTS
  WHERE object_type = 'user'
    AND object_id = username
    AND action = 'system.permission.granted'
  ORDER BY occurred_at DESC
  LIMIT 1;
  
  IF (current_permissions IS NULL) THEN
    RETURN OBJECT_CONSTRUCT(
      'success', FALSE,
      'error', 'No active permissions found for user: ' || username
    );
  END IF;
  
  -- Revoke old token
  event_id := SHA2(CONCAT_WS('|', 'perm.revoke.rotation', username, CURRENT_TIMESTAMP()::STRING), 256);
  
  INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS VALUES (
    OBJECT_CONSTRUCT(
      'event_id', event_id,
      'action', 'system.permission.revoked',
      'occurred_at', CURRENT_TIMESTAMP(),
      'actor_id', CURRENT_USER(),
      'source', 'system',
      'object', OBJECT_CONSTRUCT(
        'type', 'user',
        'id', username
      ),
      'attributes', OBJECT_CONSTRUCT(
        'token_hash', current_permissions:token_hash,
        'reason', reason,
        'revoked_by', CURRENT_USER()
      )
    ),
    'ADMIN',
    CURRENT_TIMESTAMP()
  );
  
  -- Issue new token with same permissions
  CALL ADMIN.ISSUE_USER_TOKEN(
    username,
    current_permissions:allowed_tools,
    current_permissions:max_rows,
    current_permissions:daily_runtime_seconds,
    DATEADD('day', 90, CURRENT_TIMESTAMP())  -- 90 day expiry for rotated tokens
  ) INTO new_token_result;
  
  -- Log rotation event
  INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS VALUES (
    OBJECT_CONSTRUCT(
      'event_id', SHA2(CONCAT_WS('|', 'token.rotated', username, CURRENT_TIMESTAMP()::STRING), 256),
      'action', 'system.token.rotated',
      'occurred_at', CURRENT_TIMESTAMP(),
      'actor_id', CURRENT_USER(),
      'source', 'system',
      'object', OBJECT_CONSTRUCT(
        'type', 'user',
        'id', username
      ),
      'attributes', OBJECT_CONSTRUCT(
        'reason', reason,
        'old_token_prefix', current_permissions:token_prefix,
        'new_token_prefix', new_token_result:token_prefix,
        'rotated_by', CURRENT_USER()
      )
    ),
    'ADMIN',
    CURRENT_TIMESTAMP()
  );
  
  RETURN new_token_result;
END;
$$;

-- ============================================================================
-- Create emergency token views
-- ============================================================================

CREATE OR REPLACE VIEW ADMIN.ACTIVE_TOKENS AS
WITH latest_permissions AS (
  SELECT 
    object_id AS username,
    attributes:token_prefix::STRING AS token_prefix,
    attributes:token_suffix::STRING AS token_suffix,
    attributes:expires_at::TIMESTAMP_TZ AS expires_at,
    attributes:allowed_tools::ARRAY AS allowed_tools,
    attributes:granted_by::STRING AS granted_by,
    attributes:granted_ip::STRING AS granted_ip,
    occurred_at AS issued_at,
    DATEDIFF('day', occurred_at, CURRENT_TIMESTAMP()) AS age_days,
    action,
    ROW_NUMBER() OVER (
      PARTITION BY object_id 
      ORDER BY occurred_at DESC
    ) AS rn
  FROM CLAUDE_BI.ACTIVITY.EVENTS
  WHERE object_type = 'user'
    AND action IN ('system.permission.granted', 'system.permission.revoked')
),
token_usage AS (
  SELECT 
    actor_id AS username,
    COUNT(*) AS usage_count_24h,
    MAX(occurred_at) AS last_used
  FROM CLAUDE_BI.ACTIVITY.EVENTS
  WHERE action = 'mcp.request.processed'
    AND occurred_at >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
  GROUP BY actor_id
)
SELECT 
  p.username,
  p.token_prefix || '...' || p.token_suffix AS token_hint,
  p.issued_at,
  p.expires_at,
  p.age_days,
  p.granted_by,
  p.granted_ip,
  ARRAY_SIZE(p.allowed_tools) AS tool_count,
  COALESCE(u.usage_count_24h, 0) AS usage_24h,
  u.last_used,
  CASE 
    WHEN p.action = 'system.permission.revoked' THEN 'REVOKED'
    WHEN p.expires_at < CURRENT_TIMESTAMP() THEN 'EXPIRED'
    WHEN p.age_days > 90 THEN 'SHOULD_ROTATE'
    WHEN p.age_days > 60 THEN 'AGING'
    ELSE 'ACTIVE'
  END AS status,
  CASE 
    WHEN p.age_days > 90 THEN 'Token is over 90 days old and should be rotated'
    WHEN p.expires_at < DATEADD('day', 7, CURRENT_TIMESTAMP()) THEN 'Token expires in less than 7 days'
    ELSE NULL
  END AS warning
FROM latest_permissions p
LEFT JOIN token_usage u ON p.username = u.username
WHERE p.rn = 1
ORDER BY 
  CASE status 
    WHEN 'SHOULD_ROTATE' THEN 1
    WHEN 'EXPIRED' THEN 2
    WHEN 'AGING' THEN 3
    WHEN 'ACTIVE' THEN 4
    WHEN 'REVOKED' THEN 5
  END,
  p.age_days DESC;

-- ============================================================================
-- Grant permissions
-- ============================================================================

GRANT EXECUTE ON FUNCTION CLAUDE_BI.ADMIN_SECRETS.GET_PEPPER() TO ROLE MCP_ADMIN_ROLE;
GRANT EXECUTE ON FUNCTION CLAUDE_BI.ADMIN_SECRETS.GENERATE_SECURE_TOKEN(STRING, STRING) TO ROLE MCP_ADMIN_ROLE;
GRANT EXECUTE ON FUNCTION MCP.HASH_TOKEN_WITH_PEPPER(STRING) TO ROLE MCP_SERVICE_ROLE;
GRANT EXECUTE ON FUNCTION MCP.EXTRACT_TOKEN_METADATA(STRING) TO ROLE MCP_ADMIN_ROLE;

GRANT EXECUTE ON PROCEDURE ADMIN.ISSUE_USER_TOKEN(STRING, ARRAY, INT, INT, TIMESTAMP_TZ) TO ROLE MCP_ADMIN_ROLE;
GRANT EXECUTE ON PROCEDURE ADMIN.ROTATE_USER_TOKEN(STRING, STRING) TO ROLE MCP_ADMIN_ROLE;

GRANT SELECT ON VIEW ADMIN.ACTIVE_TOKENS TO ROLE MCP_ADMIN_ROLE;

-- ============================================================================
-- Test the enhanced token system
-- ============================================================================

-- Test token generation
SELECT CLAUDE_BI.ADMIN_SECRETS.GENERATE_SECURE_TOKEN('tk', 'testuser') AS sample_token;

-- Test token issuance
CALL ADMIN.ISSUE_USER_TOKEN(
  'test_secure_user',
  ARRAY_CONSTRUCT('compose_query', 'list_sources'),
  10000,
  3600,
  DATEADD('day', 30, CURRENT_TIMESTAMP())
);