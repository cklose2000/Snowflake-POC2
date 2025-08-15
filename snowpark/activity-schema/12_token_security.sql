-- ============================================================================
-- 12_token_security.sql
-- Secure token management with hashing and pepper
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE CLAUDE_BI;
USE SCHEMA MCP;

-- ============================================================================
-- Create secure context for storing pepper and other secrets
-- ============================================================================

-- Create context for security settings
CREATE OR REPLACE CONTEXT MCP_SECURITY_CTX;

-- Initialize server pepper (should be rotated periodically)
-- In production, this would come from KMS/external secret manager
ALTER CONTEXT MCP_SECURITY_CTX SET 
  server_pepper = SHA2(CONCAT(
    'SNOWFLAKE_MCP_PEPPER_v1_',
    UUID_STRING(),
    CURRENT_TIMESTAMP()::STRING
  ), 512);

-- Set token TTL and other security parameters
ALTER CONTEXT MCP_SECURITY_CTX SET
  token_ttl_days = 90,
  max_nonce_age_minutes = 60,
  one_time_url_ttl_seconds = 300;

-- ============================================================================
-- Helper functions for token operations
-- ============================================================================

-- Generate a secure random token
CREATE OR REPLACE FUNCTION MCP.GENERATE_SECURE_TOKEN()
RETURNS STRING
LANGUAGE SQL
AS
$$
  'tk_' || 
  REPLACE(UUID_STRING(), '-', '') || 
  '_' || 
  SUBSTR(SHA2(CONCAT(RANDOM()::STRING, CURRENT_TIMESTAMP()::STRING), 256), 1, 16)
$$;

-- Hash a token with the server pepper
CREATE OR REPLACE FUNCTION MCP.HASH_TOKEN(raw_token STRING)
RETURNS STRING
LANGUAGE SQL
AS
$$
  SHA2(
    raw_token || 
    SYSTEM$GET_CONTEXT('MCP_SECURITY_CTX', 'server_pepper'),
    256
  )
$$;

-- Generate a one-time URL for token delivery
CREATE OR REPLACE FUNCTION MCP.GENERATE_ONE_TIME_URL(
  token STRING,
  username STRING
)
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
  -- In production, this would integrate with a secure delivery system
  -- For now, return a conceptual URL structure
  LET url_token := SHA2(CONCAT(token, username, CURRENT_TIMESTAMP()::STRING), 256);
  LET base_url := 'https://secure.company.com/claim-token/';
  
  -- Store the URL token in events for validation
  INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS VALUES (
    OBJECT_CONSTRUCT(
      'event_id', SHA2(CONCAT_WS('|', 'token.url.created', username, url_token), 256),
      'action', 'system.token.url_created',
      'occurred_at', CURRENT_TIMESTAMP(),
      'actor_id', CURRENT_USER(),
      'source', 'system',
      'schema_version', '2.1.0',
      'object', OBJECT_CONSTRUCT(
        'type', 'token_url',
        'id', url_token
      ),
      'attributes', OBJECT_CONSTRUCT(
        'username', username,
        'token_hash', MCP.HASH_TOKEN(token),
        'expires_at', DATEADD(
          'second',
          SYSTEM$GET_CONTEXT('MCP_SECURITY_CTX', 'one_time_url_ttl_seconds')::NUMBER,
          CURRENT_TIMESTAMP()
        ),
        'claimed', FALSE
      )
    ),
    'SYSTEM',
    CURRENT_TIMESTAMP()
  );
  
  RETURN base_url || url_token;
END;
$$;

-- ============================================================================
-- Permission template functions
-- ============================================================================

-- Get allowed tools for a role template
CREATE OR REPLACE FUNCTION MCP.GET_TEMPLATE_TOOLS(template STRING)
RETURNS ARRAY
LANGUAGE SQL
AS
$$
  CASE UPPER(template)
    WHEN 'VIEWER' THEN 
      ARRAY_CONSTRUCT('list_sources', 'compose_query')
    WHEN 'ANALYST' THEN 
      ARRAY_CONSTRUCT('list_sources', 'compose_query', 'export_data', 'create_dashboard')
    WHEN 'ADMIN' THEN 
      ARRAY_CONSTRUCT('list_sources', 'compose_query', 'export_data', 'create_dashboard', 
                     'manage_users', 'view_audit')
    ELSE 
      ARRAY_CONSTRUCT('list_sources')  -- Minimal default
  END
$$;

-- Get row limit for a role template
CREATE OR REPLACE FUNCTION MCP.GET_TEMPLATE_ROWS(template STRING)
RETURNS NUMBER
LANGUAGE SQL
AS
$$
  CASE UPPER(template)
    WHEN 'VIEWER' THEN 1000
    WHEN 'ANALYST' THEN 10000
    WHEN 'ADMIN' THEN 100000
    ELSE 100  -- Minimal default
  END
$$;

-- Get runtime budget for a role template (seconds per day)
CREATE OR REPLACE FUNCTION MCP.GET_TEMPLATE_RUNTIME(template STRING)
RETURNS NUMBER
LANGUAGE SQL
AS
$$
  CASE UPPER(template)
    WHEN 'VIEWER' THEN 1800     -- 30 minutes
    WHEN 'ANALYST' THEN 7200    -- 2 hours
    WHEN 'ADMIN' THEN 28800     -- 8 hours
    ELSE 300  -- 5 minutes default
  END
$$;

-- ============================================================================
-- Token rotation procedure
-- ============================================================================

CREATE OR REPLACE PROCEDURE MCP.ROTATE_USER_TOKEN(
  username STRING,
  reason STRING DEFAULT 'Scheduled rotation'
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
  new_token STRING;
  token_hash STRING;
  old_token_hash STRING;
BEGIN
  -- Get current token hash to revoke
  SELECT attributes:token_hash::STRING INTO old_token_hash
  FROM CLAUDE_BI.ACTIVITY.EVENTS
  WHERE object_type = 'user'
    AND object_id = :username
    AND action = 'system.permission.granted'
  ORDER BY occurred_at DESC
  LIMIT 1;
  
  IF (old_token_hash IS NULL) THEN
    RETURN OBJECT_CONSTRUCT(
      'success', FALSE,
      'error', 'No active token found for user'
    );
  END IF;
  
  -- Revoke old token
  INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS VALUES (
    OBJECT_CONSTRUCT(
      'event_id', SHA2(CONCAT_WS('|', 'token.revoked', username, old_token_hash), 256),
      'action', 'system.token.revoked',
      'occurred_at', CURRENT_TIMESTAMP(),
      'actor_id', CURRENT_USER(),
      'source', 'system',
      'schema_version', '2.1.0',
      'object', OBJECT_CONSTRUCT(
        'type', 'token',
        'id', old_token_hash
      ),
      'attributes', OBJECT_CONSTRUCT(
        'username', :username,
        'reason', :reason,
        'revoked_by', CURRENT_USER()
      )
    ),
    'SYSTEM',
    CURRENT_TIMESTAMP()
  );
  
  -- Generate new token
  new_token := MCP.GENERATE_SECURE_TOKEN();
  token_hash := MCP.HASH_TOKEN(new_token);
  
  -- Copy existing permissions with new token
  INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS
  SELECT OBJECT_CONSTRUCT(
    'event_id', SHA2(CONCAT_WS('|', 'perm.rotated', username, token_hash), 256),
    'action', 'system.permission.granted',
    'occurred_at', CURRENT_TIMESTAMP(),
    'actor_id', CURRENT_USER(),
    'source', 'system',
    'schema_version', '2.1.0',
    'object', OBJECT_CONSTRUCT(
      'type', 'user',
      'id', :username
    ),
    'attributes', OBJECT_INSERT(
      attributes,
      'token_hash', :token_hash,
      TRUE
    )
  ),
  'SYSTEM',
  CURRENT_TIMESTAMP()
  FROM CLAUDE_BI.ACTIVITY.EVENTS
  WHERE object_type = 'user'
    AND object_id = :username
    AND action = 'system.permission.granted'
    AND attributes:token_hash = :old_token_hash
  ORDER BY occurred_at DESC
  LIMIT 1;
  
  RETURN OBJECT_CONSTRUCT(
    'success', TRUE,
    'username', :username,
    'new_token', new_token,
    'delivery_url', MCP.GENERATE_ONE_TIME_URL(new_token, :username),
    'expires_in_seconds', SYSTEM$GET_CONTEXT('MCP_SECURITY_CTX', 'one_time_url_ttl_seconds')::NUMBER
  );
END;
$$;

-- ============================================================================
-- Nonce management for replay protection
-- ============================================================================

-- Validate and record a nonce
CREATE OR REPLACE FUNCTION MCP.VALIDATE_NONCE(
  nonce STRING,
  username STRING
)
RETURNS BOOLEAN
LANGUAGE SQL
AS
$$
BEGIN
  -- Check if nonce was already used within the time window
  LET max_age_minutes := SYSTEM$GET_CONTEXT('MCP_SECURITY_CTX', 'max_nonce_age_minutes')::NUMBER;
  
  IF EXISTS (
    SELECT 1 FROM CLAUDE_BI.ACTIVITY.EVENTS
    WHERE action = 'mcp.request.processed'
      AND attributes:nonce = :nonce
      AND occurred_at >= DATEADD('minute', -max_age_minutes, CURRENT_TIMESTAMP())
  ) THEN
    -- Nonce already used - potential replay attack
    INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS VALUES (
      OBJECT_CONSTRUCT(
        'event_id', SHA2(CONCAT_WS('|', 'security.replay', username, nonce), 256),
        'action', 'security.replay_detected',
        'occurred_at', CURRENT_TIMESTAMP(),
        'actor_id', :username,
        'source', 'security',
        'schema_version', '2.1.0',
        'attributes', OBJECT_CONSTRUCT(
          'nonce', :nonce,
          'detection_time', CURRENT_TIMESTAMP()
        )
      ),
      'SECURITY',
      CURRENT_TIMESTAMP()
    );
    RETURN FALSE;
  END IF;
  
  -- Nonce is valid
  RETURN TRUE;
END;
$$;

-- ============================================================================
-- Security monitoring views
-- ============================================================================

-- Monitor token operations (no raw tokens exposed)
CREATE OR REPLACE VIEW MCP.TOKEN_OPERATIONS AS
SELECT 
  occurred_at,
  action,
  object_id AS username,
  attributes:reason AS reason,
  attributes:revoked_by AS revoked_by,
  attributes:expires_at AS expires_at
FROM CLAUDE_BI.ACTIVITY.EVENTS
WHERE action IN (
  'system.token.revoked',
  'system.token.url_created',
  'security.replay_detected'
)
ORDER BY occurred_at DESC;

-- Active tokens summary (only hashes)
CREATE OR REPLACE VIEW MCP.ACTIVE_TOKENS AS
WITH latest_tokens AS (
  SELECT 
    object_id AS username,
    attributes:token_hash AS token_hash,
    attributes:expires_at AS expires_at,
    attributes:allowed_tools AS allowed_tools,
    occurred_at AS granted_at,
    ROW_NUMBER() OVER (
      PARTITION BY object_id 
      ORDER BY occurred_at DESC
    ) AS rn
  FROM CLAUDE_BI.ACTIVITY.EVENTS
  WHERE action = 'system.permission.granted'
    AND attributes:expires_at > CURRENT_TIMESTAMP()
)
SELECT 
  username,
  SUBSTR(token_hash, 1, 8) || '...' AS token_prefix,  -- Only show prefix
  expires_at,
  ARRAY_SIZE(allowed_tools) AS tool_count,
  granted_at,
  DATEDIFF('day', CURRENT_DATE(), expires_at) AS days_until_expiry
FROM latest_tokens
WHERE rn = 1
ORDER BY username;

-- ============================================================================
-- Grant permissions
-- ============================================================================

GRANT EXECUTE ON FUNCTION MCP.GENERATE_SECURE_TOKEN() TO ROLE MCP_ADMIN_ROLE;
GRANT EXECUTE ON FUNCTION MCP.HASH_TOKEN(STRING) TO ROLE MCP_ADMIN_ROLE;
GRANT EXECUTE ON FUNCTION MCP.GENERATE_ONE_TIME_URL(STRING, STRING) TO ROLE MCP_ADMIN_ROLE;
GRANT EXECUTE ON FUNCTION MCP.GET_TEMPLATE_TOOLS(STRING) TO ROLE MCP_ADMIN_ROLE;
GRANT EXECUTE ON FUNCTION MCP.GET_TEMPLATE_ROWS(STRING) TO ROLE MCP_ADMIN_ROLE;
GRANT EXECUTE ON FUNCTION MCP.GET_TEMPLATE_RUNTIME(STRING) TO ROLE MCP_ADMIN_ROLE;
GRANT EXECUTE ON PROCEDURE MCP.ROTATE_USER_TOKEN(STRING, STRING) TO ROLE MCP_ADMIN_ROLE;
GRANT EXECUTE ON FUNCTION MCP.VALIDATE_NONCE(STRING, STRING) TO ROLE MCP_SERVICE_ROLE;

GRANT SELECT ON VIEW MCP.TOKEN_OPERATIONS TO ROLE MCP_ADMIN_ROLE;
GRANT SELECT ON VIEW MCP.ACTIVE_TOKENS TO ROLE MCP_ADMIN_ROLE;