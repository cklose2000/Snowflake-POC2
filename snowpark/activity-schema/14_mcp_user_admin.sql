-- ============================================================================
-- 14_mcp_user_admin.sql
-- User creation and management with secure token generation
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE CLAUDE_BI;
USE SCHEMA MCP;

-- ============================================================================
-- Create MCP User with secure token generation
-- ============================================================================

CREATE OR REPLACE PROCEDURE MCP.CREATE_MCP_USER(
  username STRING,
  email STRING,
  role_template STRING DEFAULT 'VIEWER'
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
  raw_token STRING;
  token_hash STRING;
  allowed_tools ARRAY;
  max_rows NUMBER;
  runtime_budget NUMBER;
  token_ttl_days NUMBER;
  delivery_url STRING;
  event_id STRING;
BEGIN
  -- Validate inputs
  IF (username IS NULL OR TRIM(username) = '') THEN
    RETURN OBJECT_CONSTRUCT(
      'success', FALSE,
      'error', 'Username cannot be empty'
    );
  END IF;
  
  IF (email IS NULL OR TRIM(email) = '') THEN
    RETURN OBJECT_CONSTRUCT(
      'success', FALSE,
      'error', 'Email cannot be empty'
    );
  END IF;
  
  -- Check if user already exists
  IF EXISTS (
    SELECT 1 FROM CLAUDE_BI.ACTIVITY.EVENTS
    WHERE object_type = 'user'
      AND object_id = :username
      AND action = 'system.user.created'
  ) THEN
    RETURN OBJECT_CONSTRUCT(
      'success', FALSE,
      'error', 'User already exists: ' || :username
    );
  END IF;
  
  -- Generate secure token
  raw_token := MCP.GENERATE_SECURE_TOKEN();
  token_hash := MCP.HASH_TOKEN(raw_token);
  
  -- Get template settings
  allowed_tools := MCP.GET_TEMPLATE_TOOLS(:role_template);
  max_rows := MCP.GET_TEMPLATE_ROWS(:role_template);
  runtime_budget := MCP.GET_TEMPLATE_RUNTIME(:role_template);
  
  -- Get TTL from context
  SELECT SYSTEM$GET_CONTEXT('MCP_SECURITY_CTX', 'token_ttl_days')::NUMBER 
  INTO token_ttl_days;
  
  -- Create user event
  event_id := SHA2(CONCAT_WS('|', 'user.create', username, CURRENT_TIMESTAMP()::STRING), 256);
  
  INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS VALUES (
    OBJECT_CONSTRUCT(
      'event_id', event_id,
      'action', 'system.user.created',
      'occurred_at', CURRENT_TIMESTAMP(),
      'actor_id', CURRENT_USER(),
      'source', 'system',
      'schema_version', '2.1.0',
      'object', OBJECT_CONSTRUCT(
        'type', 'user',
        'id', :username
      ),
      'attributes', OBJECT_CONSTRUCT(
        'email', :email,
        'role_template', :role_template,
        'created_by', CURRENT_USER(),
        'created_at', CURRENT_TIMESTAMP()
      )
    ),
    'ADMIN',
    CURRENT_TIMESTAMP()
  );
  
  -- Grant permissions with hashed token
  event_id := SHA2(CONCAT_WS('|', 'perm.grant', username, token_hash), 256);
  
  INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS VALUES (
    OBJECT_CONSTRUCT(
      'event_id', event_id,
      'action', 'system.permission.granted',
      'occurred_at', CURRENT_TIMESTAMP(),
      'actor_id', CURRENT_USER(),
      'source', 'system',
      'schema_version', '2.1.0',
      'object', OBJECT_CONSTRUCT(
        'type', 'user',
        'id', :username
      ),
      'attributes', OBJECT_CONSTRUCT(
        'token_hash', :token_hash,  -- Only hash stored!
        'allowed_tools', :allowed_tools,
        'max_rows', :max_rows,
        'daily_runtime_seconds', :runtime_budget,
        'expires_at', DATEADD('day', token_ttl_days, CURRENT_TIMESTAMP()),
        'granted_by', CURRENT_USER()
      )
    ),
    'ADMIN',
    CURRENT_TIMESTAMP()
  );
  
  -- Generate one-time delivery URL
  delivery_url := MCP.GENERATE_ONE_TIME_URL(raw_token, :username);
  
  -- Return token for ONE-TIME delivery
  RETURN OBJECT_CONSTRUCT(
    'success', TRUE,
    'username', :username,
    'email', :email,
    'role_template', :role_template,
    'token', raw_token,  -- Only returned once, never stored
    'delivery_url', delivery_url,
    'delivery_expires_seconds', SYSTEM$GET_CONTEXT('MCP_SECURITY_CTX', 'one_time_url_ttl_seconds')::NUMBER,
    'token_expires_days', token_ttl_days,
    'allowed_tools', :allowed_tools,
    'max_rows', :max_rows,
    'daily_runtime_seconds', :runtime_budget,
    'message', 'User created successfully. Share the delivery URL with the user immediately.'
  );
END;
$$;

-- ============================================================================
-- Revoke user permissions
-- ============================================================================

CREATE OR REPLACE PROCEDURE MCP.REVOKE_MCP_USER(
  username STRING,
  reason STRING DEFAULT 'Administrative action'
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
  current_token_hash STRING;
  event_id STRING;
BEGIN
  -- Get current token hash
  SELECT attributes:token_hash::STRING INTO current_token_hash
  FROM CLAUDE_BI.ACTIVITY.EVENTS
  WHERE object_type = 'user'
    AND object_id = :username
    AND action = 'system.permission.granted'
  ORDER BY occurred_at DESC
  LIMIT 1;
  
  IF (current_token_hash IS NULL) THEN
    RETURN OBJECT_CONSTRUCT(
      'success', FALSE,
      'error', 'No active permissions found for user: ' || :username
    );
  END IF;
  
  -- Insert revocation event
  event_id := SHA2(CONCAT_WS('|', 'perm.revoke', username, current_token_hash), 256);
  
  INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS VALUES (
    OBJECT_CONSTRUCT(
      'event_id', event_id,
      'action', 'system.permission.revoked',
      'occurred_at', CURRENT_TIMESTAMP(),
      'actor_id', CURRENT_USER(),
      'source', 'system',
      'schema_version', '2.1.0',
      'object', OBJECT_CONSTRUCT(
        'type', 'user',
        'id', :username
      ),
      'attributes', OBJECT_CONSTRUCT(
        'token_hash', :current_token_hash,
        'reason', :reason,
        'revoked_by', CURRENT_USER(),
        'revoked_at', CURRENT_TIMESTAMP()
      )
    ),
    'ADMIN',
    CURRENT_TIMESTAMP()
  );
  
  RETURN OBJECT_CONSTRUCT(
    'success', TRUE,
    'username', :username,
    'action', 'revoked',
    'reason', :reason,
    'revoked_by', CURRENT_USER()
  );
END;
$$;

-- ============================================================================
-- Update user permissions (creates new permission event)
-- ============================================================================

CREATE OR REPLACE PROCEDURE MCP.UPDATE_USER_PERMISSIONS(
  username STRING,
  allowed_tools ARRAY,
  max_rows NUMBER DEFAULT NULL,
  runtime_budget NUMBER DEFAULT NULL
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
  current_token_hash STRING;
  current_expires_at TIMESTAMP_TZ;
  new_max_rows NUMBER;
  new_runtime_budget NUMBER;
  event_id STRING;
BEGIN
  -- Get current token hash and settings
  SELECT 
    attributes:token_hash::STRING,
    attributes:expires_at::TIMESTAMP_TZ,
    attributes:max_rows::NUMBER,
    attributes:daily_runtime_seconds::NUMBER
  INTO 
    current_token_hash,
    current_expires_at,
    new_max_rows,
    new_runtime_budget
  FROM CLAUDE_BI.ACTIVITY.EVENTS
  WHERE object_type = 'user'
    AND object_id = :username
    AND action = 'system.permission.granted'
  ORDER BY occurred_at DESC
  LIMIT 1;
  
  IF (current_token_hash IS NULL) THEN
    RETURN OBJECT_CONSTRUCT(
      'success', FALSE,
      'error', 'No active permissions found for user: ' || :username
    );
  END IF;
  
  -- Use provided values or keep existing
  new_max_rows := COALESCE(:max_rows, new_max_rows, 1000);
  new_runtime_budget := COALESCE(:runtime_budget, new_runtime_budget, 3600);
  
  -- Create new permission event with updated settings
  event_id := SHA2(CONCAT_WS('|', 'perm.update', username, current_token_hash), 256);
  
  INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS VALUES (
    OBJECT_CONSTRUCT(
      'event_id', event_id,
      'action', 'system.permission.granted',
      'occurred_at', CURRENT_TIMESTAMP(),
      'actor_id', CURRENT_USER(),
      'source', 'system',
      'schema_version', '2.1.0',
      'object', OBJECT_CONSTRUCT(
        'type', 'user',
        'id', :username
      ),
      'attributes', OBJECT_CONSTRUCT(
        'token_hash', :current_token_hash,  -- Keep same token
        'allowed_tools', :allowed_tools,
        'max_rows', new_max_rows,
        'daily_runtime_seconds', new_runtime_budget,
        'expires_at', :current_expires_at,  -- Keep same expiry
        'updated_by', CURRENT_USER(),
        'updated_at', CURRENT_TIMESTAMP()
      )
    ),
    'ADMIN',
    CURRENT_TIMESTAMP()
  );
  
  RETURN OBJECT_CONSTRUCT(
    'success', TRUE,
    'username', :username,
    'action', 'permissions_updated',
    'allowed_tools', :allowed_tools,
    'max_rows', new_max_rows,
    'daily_runtime_seconds', new_runtime_budget,
    'updated_by', CURRENT_USER()
  );
END;
$$;

-- ============================================================================
-- Bulk user creation from stage
-- ============================================================================

CREATE OR REPLACE PROCEDURE MCP.IMPORT_USERS_FROM_STAGE(
  stage_path STRING,  -- e.g., '@user_imports/users.csv'
  role_template STRING DEFAULT 'VIEWER'
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
  created_count NUMBER DEFAULT 0;
  failed_count NUMBER DEFAULT 0;
  user_record VARIANT;
  result VARIANT;
BEGIN
  -- Create temporary table from CSV
  CREATE OR REPLACE TEMPORARY TABLE temp_user_import (
    username STRING,
    email STRING,
    department STRING
  );
  
  -- Copy from stage
  EXECUTE IMMEDIATE 'COPY INTO temp_user_import FROM ' || :stage_path || 
    ' FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1)';
  
  -- Process each user
  FOR user_record IN (SELECT * FROM temp_user_import) DO
    BEGIN
      CALL MCP.CREATE_MCP_USER(
        user_record.username,
        user_record.email,
        :role_template
      ) INTO result;
      
      IF (result:success = TRUE) THEN
        created_count := created_count + 1;
      ELSE
        failed_count := failed_count + 1;
      END IF;
    EXCEPTION
      WHEN OTHER THEN
        failed_count := failed_count + 1;
    END;
  END FOR;
  
  DROP TABLE temp_user_import;
  
  RETURN OBJECT_CONSTRUCT(
    'success', TRUE,
    'created_count', created_count,
    'failed_count', failed_count,
    'total_processed', created_count + failed_count
  );
END;
$$;

-- ============================================================================
-- User management views
-- ============================================================================

-- Current users and their permission status
CREATE OR REPLACE VIEW MCP.CURRENT_USERS AS
WITH latest_perms AS (
  SELECT 
    object_id AS username,
    action,
    attributes:allowed_tools AS allowed_tools,
    attributes:max_rows AS max_rows,
    attributes:daily_runtime_seconds AS runtime_budget,
    attributes:expires_at AS expires_at,
    occurred_at AS last_updated,
    ROW_NUMBER() OVER (
      PARTITION BY object_id 
      ORDER BY occurred_at DESC
    ) AS rn
  FROM CLAUDE_BI.ACTIVITY.EVENTS
  WHERE object_type = 'user'
    AND action IN ('system.permission.granted', 'system.permission.revoked')
),
user_info AS (
  SELECT 
    object_id AS username,
    attributes:email AS email,
    attributes:role_template AS role_template,
    occurred_at AS created_at
  FROM CLAUDE_BI.ACTIVITY.EVENTS
  WHERE action = 'system.user.created'
)
SELECT 
  u.username,
  u.email,
  u.role_template,
  u.created_at,
  CASE 
    WHEN p.action = 'system.permission.granted' THEN 'ACTIVE'
    WHEN p.action = 'system.permission.revoked' THEN 'REVOKED'
    ELSE 'NO_PERMISSIONS'
  END AS status,
  p.allowed_tools,
  p.max_rows,
  p.runtime_budget,
  p.expires_at,
  p.last_updated
FROM user_info u
LEFT JOIN latest_perms p ON u.username = p.username AND p.rn = 1
ORDER BY u.username;

-- User activity summary
CREATE OR REPLACE VIEW MCP.USER_ACTIVITY_SUMMARY AS
WITH activity AS (
  SELECT 
    actor_id AS username,
    COUNT(*) AS request_count,
    SUM(attributes:execution_ms::NUMBER) / 1000 AS total_runtime_seconds,
    AVG(attributes:execution_ms::NUMBER) AS avg_runtime_ms,
    MAX(occurred_at) AS last_activity,
    MIN(occurred_at) AS first_activity
  FROM CLAUDE_BI.ACTIVITY.EVENTS
  WHERE action = 'mcp.request.processed'
    AND occurred_at >= DATEADD('day', -30, CURRENT_DATE())
  GROUP BY actor_id
)
SELECT 
  u.username,
  u.email,
  u.status,
  COALESCE(a.request_count, 0) AS requests_30d,
  COALESCE(a.total_runtime_seconds, 0) AS runtime_seconds_30d,
  COALESCE(a.avg_runtime_ms, 0) AS avg_runtime_ms,
  a.last_activity,
  DATEDIFF('day', a.last_activity, CURRENT_TIMESTAMP()) AS days_since_active
FROM MCP.CURRENT_USERS u
LEFT JOIN activity a ON u.username = a.username
ORDER BY requests_30d DESC;

-- ============================================================================
-- Grant permissions
-- ============================================================================

GRANT EXECUTE ON PROCEDURE MCP.CREATE_MCP_USER(STRING, STRING, STRING) TO ROLE MCP_ADMIN_ROLE;
GRANT EXECUTE ON PROCEDURE MCP.REVOKE_MCP_USER(STRING, STRING) TO ROLE MCP_ADMIN_ROLE;
GRANT EXECUTE ON PROCEDURE MCP.UPDATE_USER_PERMISSIONS(STRING, ARRAY, NUMBER, NUMBER) TO ROLE MCP_ADMIN_ROLE;
GRANT EXECUTE ON PROCEDURE MCP.IMPORT_USERS_FROM_STAGE(STRING, STRING) TO ROLE MCP_ADMIN_ROLE;

GRANT SELECT ON VIEW MCP.CURRENT_USERS TO ROLE MCP_ADMIN_ROLE;
GRANT SELECT ON VIEW MCP.USER_ACTIVITY_SUMMARY TO ROLE MCP_ADMIN_ROLE;