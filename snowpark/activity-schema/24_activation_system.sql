-- ============================================================================
-- 24_activation_system.sql  
-- One-click activation system for token delivery without human token handling
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE CLAUDE_BI;
USE SCHEMA MCP;

-- ============================================================================
-- Create activation tracking view (uses events, not a table!)
-- ============================================================================

CREATE OR REPLACE VIEW ADMIN.PENDING_ACTIVATIONS AS
WITH activation_events AS (
  SELECT 
    attributes:activation_code::STRING AS activation_code,
    attributes:username::STRING AS username,
    attributes:allowed_tools::ARRAY AS allowed_tools,
    attributes:max_rows::NUMBER AS max_rows,
    attributes:daily_runtime_seconds::NUMBER AS daily_runtime_seconds,
    attributes:expires_at::TIMESTAMP_TZ AS token_expires_at,
    attributes:activation_expires_at::TIMESTAMP_TZ AS activation_expires_at,
    attributes:activation_url::STRING AS activation_url,
    attributes:status::STRING AS status,
    occurred_at AS created_at,
    actor_id AS created_by,
    ROW_NUMBER() OVER (
      PARTITION BY attributes:activation_code
      ORDER BY occurred_at DESC
    ) AS rn
  FROM CLAUDE_BI.ACTIVITY.EVENTS
  WHERE action IN ('system.activation.created', 'system.activation.used', 'system.activation.expired')
)
SELECT 
  activation_code,
  username,
  allowed_tools,
  max_rows,
  daily_runtime_seconds,
  token_expires_at,
  activation_expires_at,
  activation_url,
  created_at,
  created_by,
  CASE 
    WHEN status = 'used' THEN 'USED'
    WHEN activation_expires_at < CURRENT_TIMESTAMP() THEN 'EXPIRED'
    ELSE 'PENDING'
  END AS status,
  DATEDIFF('minute', CURRENT_TIMESTAMP(), activation_expires_at) AS minutes_remaining
FROM activation_events
WHERE rn = 1
ORDER BY created_at DESC;

-- ============================================================================
-- Create activation generation procedure
-- ============================================================================

CREATE OR REPLACE PROCEDURE ADMIN.CREATE_ACTIVATION(
  username STRING,
  allowed_tools ARRAY,
  max_rows INT DEFAULT 10000,
  daily_runtime_s INT DEFAULT 3600,
  token_ttl_days INT DEFAULT 90,
  activation_ttl_minutes INT DEFAULT 30
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
  activation_code STRING;
  activation_url STRING;
  activation_expires_at TIMESTAMP_TZ;
  token_expires_at TIMESTAMP_TZ;
  event_id STRING;
  gateway_base_url STRING;
BEGIN
  -- Validate inputs
  IF (username IS NULL OR TRIM(username) = '') THEN
    RETURN OBJECT_CONSTRUCT(
      'success', FALSE,
      'error', 'Username cannot be empty'
    );
  END IF;
  
  IF (ARRAY_SIZE(allowed_tools) = 0) THEN
    RETURN OBJECT_CONSTRUCT(
      'success', FALSE,
      'error', 'At least one tool must be allowed'
    );
  END IF;
  
  -- Generate activation code (not the actual token yet)
  activation_code := 'act_' || REPLACE(UUID_STRING(), '-', '');
  
  -- Calculate expiration times
  activation_expires_at := DATEADD('minute', activation_ttl_minutes, CURRENT_TIMESTAMP());
  token_expires_at := DATEADD('day', token_ttl_days, CURRENT_TIMESTAMP());
  
  -- Get gateway URL from context (or use default)
  gateway_base_url := COALESCE(
    SYSTEM$GET_CONTEXT('MCP_CONFIG', 'activation_gateway_url'),
    'https://mcp.example.com/activate'
  );
  
  activation_url := gateway_base_url || '/' || activation_code;
  
  -- Create activation event (not the token yet!)
  event_id := SHA2(CONCAT_WS('|', 'activation.create', activation_code, username), 256);
  
  INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS VALUES (
    OBJECT_CONSTRUCT(
      'event_id', event_id,
      'action', 'system.activation.created',
      'occurred_at', CURRENT_TIMESTAMP(),
      'actor_id', CURRENT_USER(),
      'source', 'system',
      'object', OBJECT_CONSTRUCT(
        'type', 'activation',
        'id', activation_code
      ),
      'attributes', OBJECT_CONSTRUCT(
        'activation_code', activation_code,
        'username', username,
        'allowed_tools', allowed_tools,
        'max_rows', max_rows,
        'daily_runtime_seconds', daily_runtime_s,
        'token_expires_at', token_expires_at,
        'activation_expires_at', activation_expires_at,
        'activation_url', activation_url,
        'status', 'pending',
        'created_by', CURRENT_USER(),
        'created_from_ip', CURRENT_IP_ADDRESS()
      )
    ),
    'ADMIN',
    CURRENT_TIMESTAMP()
  );
  
  -- Return activation details (no token yet!)
  RETURN OBJECT_CONSTRUCT(
    'success', TRUE,
    'activation_code', activation_code,
    'activation_url', activation_url,
    'valid_for_minutes', activation_ttl_minutes,
    'expires_at', activation_expires_at,
    'username', username,
    'allowed_tools', allowed_tools,
    'message', 'Send the activation URL to the user. It will expire in ' || activation_ttl_minutes || ' minutes.'
  );
END;
$$;

-- ============================================================================
-- Create activation finalization procedure
-- ============================================================================

CREATE OR REPLACE PROCEDURE ADMIN.FINALIZE_ACTIVATION(
  activation_code STRING
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
  activation_data VARIANT;
  token_full STRING;
  token_hash STRING;
  token_metadata VARIANT;
  event_id STRING;
  deeplink STRING;
BEGIN
  -- Validate activation code format
  IF (activation_code NOT LIKE 'act_%') THEN
    RETURN OBJECT_CONSTRUCT(
      'success', FALSE,
      'error', 'Invalid activation code format'
    );
  END IF;
  
  -- Get activation details
  SELECT OBJECT_CONSTRUCT(
    'username', attributes:username,
    'allowed_tools', attributes:allowed_tools,
    'max_rows', attributes:max_rows,
    'daily_runtime_seconds', attributes:daily_runtime_seconds,
    'token_expires_at', attributes:token_expires_at,
    'activation_expires_at', attributes:activation_expires_at,
    'status', attributes:status
  ) INTO activation_data
  FROM CLAUDE_BI.ACTIVITY.EVENTS
  WHERE action = 'system.activation.created'
    AND attributes:activation_code = activation_code
  ORDER BY occurred_at DESC
  LIMIT 1;
  
  -- Validate activation exists
  IF (activation_data IS NULL) THEN
    RETURN OBJECT_CONSTRUCT(
      'success', FALSE,
      'error', 'Activation code not found'
    );
  END IF;
  
  -- Check if already used
  IF EXISTS (
    SELECT 1 FROM CLAUDE_BI.ACTIVITY.EVENTS
    WHERE action = 'system.activation.used'
      AND attributes:activation_code = activation_code
  ) THEN
    RETURN OBJECT_CONSTRUCT(
      'success', FALSE,
      'error', 'Activation code already used'
    );
  END IF;
  
  -- Check if expired
  IF (activation_data:activation_expires_at < CURRENT_TIMESTAMP()) THEN
    -- Mark as expired
    INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS VALUES (
      OBJECT_CONSTRUCT(
        'event_id', SHA2(CONCAT_WS('|', 'activation.expired', activation_code), 256),
        'action', 'system.activation.expired',
        'occurred_at', CURRENT_TIMESTAMP(),
        'actor_id', 'system',
        'source', 'system',
        'object', OBJECT_CONSTRUCT(
          'type', 'activation',
          'id', activation_code
        ),
        'attributes', OBJECT_CONSTRUCT(
          'activation_code', activation_code,
          'status', 'expired'
        )
      ),
      'SYSTEM',
      CURRENT_TIMESTAMP()
    );
    
    RETURN OBJECT_CONSTRUCT(
      'success', FALSE,
      'error', 'Activation code expired'
    );
  END IF;
  
  -- Generate the actual token now
  token_full := CLAUDE_BI.ADMIN_SECRETS.GENERATE_SECURE_TOKEN('tk', activation_data:username);
  token_hash := MCP.HASH_TOKEN_WITH_PEPPER(token_full);
  token_metadata := MCP.EXTRACT_TOKEN_METADATA(token_full);
  
  -- Create user if not exists
  IF NOT EXISTS (
    SELECT 1 FROM CLAUDE_BI.ACTIVITY.EVENTS
    WHERE object_type = 'user'
      AND object_id = activation_data:username
      AND action = 'system.user.created'
  ) THEN
    event_id := SHA2(CONCAT_WS('|', 'user.create', activation_data:username, CURRENT_TIMESTAMP()::STRING), 256);
    
    INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS VALUES (
      OBJECT_CONSTRUCT(
        'event_id', event_id,
        'action', 'system.user.created',
        'occurred_at', CURRENT_TIMESTAMP(),
        'actor_id', 'activation_gateway',
        'source', 'system',
        'object', OBJECT_CONSTRUCT(
          'type', 'user',
          'id', activation_data:username
        ),
        'attributes', OBJECT_CONSTRUCT(
          'created_via', 'activation',
          'activation_code', activation_code
        )
      ),
      'ADMIN',
      CURRENT_TIMESTAMP()
    );
  END IF;
  
  -- Grant permissions with token hash
  event_id := SHA2(CONCAT_WS('|', 'perm.grant', activation_data:username, token_hash), 256);
  
  INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS VALUES (
    OBJECT_CONSTRUCT(
      'event_id', event_id,
      'action', 'system.permission.granted',
      'occurred_at', CURRENT_TIMESTAMP(),
      'actor_id', 'activation_gateway',
      'source', 'system',
      'object', OBJECT_CONSTRUCT(
        'type', 'user',
        'id', activation_data:username
      ),
      'attributes', OBJECT_CONSTRUCT(
        'token_hash', token_hash,
        'token_prefix', token_metadata:prefix,
        'token_suffix', token_metadata:suffix,
        'allowed_tools', activation_data:allowed_tools,
        'max_rows', activation_data:max_rows,
        'daily_runtime_seconds', activation_data:daily_runtime_seconds,
        'expires_at', activation_data:token_expires_at,
        'granted_via', 'activation',
        'activation_code', activation_code,
        'token_metadata', token_metadata
      )
    ),
    'ADMIN',
    CURRENT_TIMESTAMP()
  );
  
  -- Mark activation as used
  INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS VALUES (
    OBJECT_CONSTRUCT(
      'event_id', SHA2(CONCAT_WS('|', 'activation.used', activation_code), 256),
      'action', 'system.activation.used',
      'occurred_at', CURRENT_TIMESTAMP(),
      'actor_id', 'activation_gateway',
      'source', 'system',
      'object', OBJECT_CONSTRUCT(
        'type', 'activation',
        'id', activation_code
      ),
      'attributes', OBJECT_CONSTRUCT(
        'activation_code', activation_code,
        'username', activation_data:username,
        'token_prefix', token_metadata:prefix,
        'status', 'used',
        'used_from_ip', CURRENT_IP_ADDRESS()
      )
    ),
    'ADMIN',
    CURRENT_TIMESTAMP()
  );
  
  -- Generate deeplink for Claude Code
  deeplink := 'claudecode://activate?token=' || token_full;
  
  -- Return token and deeplink (gateway will redirect, user never sees token)
  RETURN OBJECT_CONSTRUCT(
    'success', TRUE,
    'token_full', token_full,
    'token_prefix', token_metadata:prefix,
    'deeplink', deeplink,
    'username', activation_data:username,
    'expires_at', activation_data:token_expires_at,
    'message', 'Activation successful. Redirect to deeplink.'
  );
END;
$$;

-- ============================================================================
-- Create bulk activation procedure for onboarding
-- ============================================================================

CREATE OR REPLACE PROCEDURE ADMIN.CREATE_BULK_ACTIVATIONS(
  usernames ARRAY,
  role_template STRING DEFAULT 'VIEWER',
  activation_ttl_minutes INT DEFAULT 1440  -- 24 hours for bulk
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
  allowed_tools ARRAY;
  max_rows INT;
  daily_runtime_s INT;
  created_count INT DEFAULT 0;
  failed_count INT DEFAULT 0;
  activation_urls ARRAY;
  username STRING;
  result VARIANT;
BEGIN
  -- Get template settings
  allowed_tools := MCP.GET_TEMPLATE_TOOLS(role_template);
  max_rows := MCP.GET_TEMPLATE_ROWS(role_template);
  daily_runtime_s := MCP.GET_TEMPLATE_RUNTIME(role_template);
  
  activation_urls := ARRAY_CONSTRUCT();
  
  -- Process each username
  FOR i IN 0 TO ARRAY_SIZE(usernames) - 1 DO
    username := usernames[i];
    
    BEGIN
      CALL ADMIN.CREATE_ACTIVATION(
        username,
        allowed_tools,
        max_rows,
        daily_runtime_s,
        90,  -- 90 day token TTL
        activation_ttl_minutes
      ) INTO result;
      
      IF (result:success = TRUE) THEN
        created_count := created_count + 1;
        activation_urls := ARRAY_APPEND(
          activation_urls,
          OBJECT_CONSTRUCT(
            'username', username,
            'url', result:activation_url
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
  
  RETURN OBJECT_CONSTRUCT(
    'success', TRUE,
    'created', created_count,
    'failed', failed_count,
    'total', ARRAY_SIZE(usernames),
    'activation_urls', activation_urls,
    'valid_for_hours', activation_ttl_minutes / 60
  );
END;
$$;

-- ============================================================================
-- Create activation cleanup task
-- ============================================================================

CREATE OR REPLACE PROCEDURE ADMIN.CLEANUP_EXPIRED_ACTIVATIONS()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
  -- Mark expired activations
  INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS
  SELECT 
    OBJECT_CONSTRUCT(
      'event_id', SHA2(CONCAT_WS('|', 'activation.expired', activation_code, CURRENT_TIMESTAMP()::STRING), 256),
      'action', 'system.activation.expired',
      'occurred_at', CURRENT_TIMESTAMP(),
      'actor_id', 'system',
      'source', 'system',
      'object', OBJECT_CONSTRUCT(
        'type', 'activation',
        'id', activation_code
      ),
      'attributes', OBJECT_CONSTRUCT(
        'activation_code', activation_code,
        'status', 'expired',
        'expired_at', CURRENT_TIMESTAMP()
      )
    ),
    'SYSTEM',
    CURRENT_TIMESTAMP()
  FROM ADMIN.PENDING_ACTIVATIONS
  WHERE status = 'PENDING'
    AND activation_expires_at < CURRENT_TIMESTAMP();
  
  RETURN 'Expired activations marked';
END;
$$;

-- Create scheduled task for cleanup
CREATE OR REPLACE TASK ADMIN.CLEANUP_ACTIVATIONS_TASK
  WAREHOUSE = ALERT_WH
  SCHEDULE = '60 MINUTE'
  COMMENT = 'Cleanup expired activation codes'
AS
  CALL ADMIN.CLEANUP_EXPIRED_ACTIVATIONS();

ALTER TASK ADMIN.CLEANUP_ACTIVATIONS_TASK RESUME;

-- ============================================================================
-- Grant permissions
-- ============================================================================

GRANT EXECUTE ON PROCEDURE ADMIN.CREATE_ACTIVATION(STRING, ARRAY, INT, INT, INT, INT) TO ROLE MCP_ADMIN_ROLE;
GRANT EXECUTE ON PROCEDURE ADMIN.FINALIZE_ACTIVATION(STRING) TO ROLE MCP_SERVICE_ROLE;  -- Gateway needs this
GRANT EXECUTE ON PROCEDURE ADMIN.CREATE_BULK_ACTIVATIONS(ARRAY, STRING, INT) TO ROLE MCP_ADMIN_ROLE;
GRANT EXECUTE ON PROCEDURE ADMIN.CLEANUP_EXPIRED_ACTIVATIONS() TO ROLE MCP_ADMIN_ROLE;

GRANT SELECT ON VIEW ADMIN.PENDING_ACTIVATIONS TO ROLE MCP_ADMIN_ROLE;

-- ============================================================================
-- Test activation flow
-- ============================================================================

-- Create an activation
CALL ADMIN.CREATE_ACTIVATION(
  'test_activation_user',
  ARRAY_CONSTRUCT('compose_query', 'list_sources'),
  5000,
  1800,
  30,
  30
);

-- View pending activations
SELECT * FROM ADMIN.PENDING_ACTIVATIONS WHERE status = 'PENDING';