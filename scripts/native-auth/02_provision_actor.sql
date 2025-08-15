-- ============================================================================
-- 02_provision_actor.sql
-- Main provisioning procedure for creating users with proper roles
-- Returns one-pager with exact .env configuration
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE CLAUDE_BI;
USE SCHEMA ADMIN;

-- ============================================================================
-- Helper function to validate email format
-- ============================================================================
CREATE OR REPLACE FUNCTION VALIDATE_EMAIL(email STRING)
RETURNS BOOLEAN
LANGUAGE SQL
AS $$
  SELECT email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'
$$;

-- ============================================================================
-- Main provisioning procedure with comprehensive validation and output
-- ============================================================================
CREATE OR REPLACE PROCEDURE PROVISION_ACTOR(
  actor_email STRING,
  actor_type STRING,       -- 'HUMAN' or 'AGENT'
  can_write BOOLEAN,       -- Grant write permissions
  auth_mode STRING,        -- 'PASSWORD' or 'KEYPAIR'
  initial_secret STRING    -- Password for humans, NULL for agents
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
COMMENT = 'Provision a new actor (human or agent) with proper role and authentication'
AS $$
DECLARE
  uname STRING;
  rname STRING;
  key_deadline TIMESTAMP_NTZ;
  provision_result VARIANT;
  existing_user BOOLEAN;
BEGIN
  -- ============================================================================
  -- VALIDATION
  -- ============================================================================
  
  -- Validate email
  IF (NOT VALIDATE_EMAIL(actor_email)) THEN
    RETURN OBJECT_CONSTRUCT(
      'success', FALSE,
      'error', 'Invalid email format: ' || actor_email
    );
  END IF;
  
  -- Validate actor type
  IF (actor_type NOT IN ('HUMAN', 'AGENT')) THEN
    RETURN OBJECT_CONSTRUCT(
      'success', FALSE,
      'error', 'Invalid actor_type: must be HUMAN or AGENT'
    );
  END IF;
  
  -- Validate auth mode
  IF (auth_mode NOT IN ('PASSWORD', 'KEYPAIR')) THEN
    RETURN OBJECT_CONSTRUCT(
      'success', FALSE,
      'error', 'Invalid auth_mode: must be PASSWORD or KEYPAIR'
    );
  END IF;
  
  -- Validate auth mode matches actor type
  IF (actor_type = 'HUMAN' AND auth_mode = 'KEYPAIR') THEN
    RETURN OBJECT_CONSTRUCT(
      'success', FALSE,
      'error', 'Humans should use PASSWORD auth (KEYPAIR is for agents)'
    );
  END IF;
  
  -- Validate password provided for PASSWORD mode
  IF (auth_mode = 'PASSWORD' AND (initial_secret IS NULL OR LENGTH(initial_secret) < 14)) THEN
    RETURN OBJECT_CONSTRUCT(
      'success', FALSE,
      'error', 'Password required and must be at least 14 characters'
    );
  END IF;
  
  -- ============================================================================
  -- GENERATE NAMES
  -- ============================================================================
  
  -- Generate deterministic username (sanitized email)
  uname := UPPER(REGEXP_REPLACE(actor_email, '[^A-Za-z0-9]', '_'));
  
  -- Generate role name with type prefix and hash
  rname := CASE 
    WHEN actor_type = 'AGENT' THEN 'R_ACTOR_AGT_' || SUBSTR(SHA2(uname), 1, 8)
    ELSE 'R_ACTOR_HUM_' || SUBSTR(SHA2(uname), 1, 8)
  END;
  
  -- Check if user already exists
  SELECT COUNT(*) > 0 INTO existing_user
  FROM SNOWFLAKE.ACCOUNT_USAGE.USERS
  WHERE NAME = :uname AND DELETED_ON IS NULL;
  
  IF (existing_user) THEN
    RETURN OBJECT_CONSTRUCT(
      'success', FALSE,
      'error', 'User already exists: ' || uname,
      'hint', 'Use ROTATE_ACTOR_KEY for key rotation or DROP USER to reprovision'
    );
  END IF;
  
  -- ============================================================================
  -- CREATE ROLE WITH GRANTS
  -- ============================================================================
  
  -- Create actor-specific role
  EXECUTE IMMEDIATE 'CREATE ROLE IF NOT EXISTS ' || :rname || 
    ' COMMENT = ''Actor role for ' || :actor_email || ' (' || :actor_type || ')''';
  
  -- Grant base roles
  EXECUTE IMMEDIATE 'GRANT ROLE R_APP_READ TO ROLE ' || :rname;
  
  IF (can_write) THEN 
    EXECUTE IMMEDIATE 'GRANT ROLE R_APP_WRITE TO ROLE ' || :rname;
  END IF;
  
  -- ============================================================================
  -- CREATE USER
  -- ============================================================================
  
  IF (auth_mode = 'PASSWORD') THEN
    -- Create human user with password
    EXECUTE IMMEDIATE 
      'CREATE USER ' || :uname ||
      ' PASSWORD = ''' || :initial_secret || '''
        EMAIL = ''' || :actor_email || '''
        DEFAULT_ROLE = ' || :rname ||
        ' DEFAULT_WAREHOUSE = ''CLAUDE_WAREHOUSE''
        DEFAULT_NAMESPACE = ''CLAUDE_BI.MCP''
        MUST_CHANGE_PASSWORD = TRUE
        PASSWORD_POLICY = PP_HUMANS
        SESSION_POLICY = SP_STANDARD
        COMMENT = ''Human user provisioned on ' || CURRENT_TIMESTAMP() || '''';
  ELSE
    -- Create agent user for key-pair auth
    key_deadline := DATEADD('minute', 10, CURRENT_TIMESTAMP());
    
    EXECUTE IMMEDIATE
      'CREATE USER ' || :uname ||
      ' EMAIL = ''' || :actor_email || '''
        DEFAULT_ROLE = ' || :rname ||
        ' DEFAULT_WAREHOUSE = ''CLAUDE_AGENT_WH''
        DEFAULT_NAMESPACE = ''CLAUDE_BI.MCP''
        SESSION_POLICY = SP_STANDARD
        COMMENT = ''Agent user provisioned on ' || CURRENT_TIMESTAMP() || 
        ' | RSA_KEY_DEADLINE: ' || :key_deadline || '''';
  END IF;
  
  -- Grant role to user
  EXECUTE IMMEDIATE 'GRANT ROLE ' || :rname || ' TO USER ' || :uname;
  
  -- ============================================================================
  -- TRACK IN REGISTRY
  -- ============================================================================
  
  INSERT INTO CLAUDE_BI.ADMIN.ACTOR_REGISTRY (
    username,
    email,
    actor_type,
    auth_mode,
    role_name,
    key_deadline,
    next_rotation_due,
    metadata
  )
  VALUES (
    :uname,
    :actor_email,
    :actor_type,
    :auth_mode,
    :rname,
    :key_deadline,
    CASE 
      WHEN :auth_mode = 'KEYPAIR' THEN DATEADD('day', 30, CURRENT_TIMESTAMP())
      ELSE NULL 
    END,
    OBJECT_CONSTRUCT(
      'can_write', :can_write,
      'provisioned_from', CURRENT_CLIENT(),
      'provisioned_session', CURRENT_SESSION()
    )
  );
  
  -- ============================================================================
  -- AUDIT EVENT
  -- ============================================================================
  
  INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS
  SELECT OBJECT_CONSTRUCT(
    'event_id', UUID_STRING(),
    'action', 'system.actor.provisioned',
    'actor_id', CURRENT_USER(),
    'occurred_at', CURRENT_TIMESTAMP(),
    'object', OBJECT_CONSTRUCT(
      'type', 'user',
      'id', :uname
    ),
    'attributes', OBJECT_CONSTRUCT(
      'email', :actor_email,
      'actor_type', :actor_type,
      'role', :rname,
      'auth_mode', :auth_mode,
      'can_write', :can_write,
      'key_deadline', :key_deadline,
      'warehouse', IFF(:actor_type = 'AGENT', 'CLAUDE_AGENT_WH', 'CLAUDE_WAREHOUSE')
    )
  ), 'ADMIN', CURRENT_TIMESTAMP();
  
  -- ============================================================================
  -- RETURN ONE-PAGER
  -- ============================================================================
  
  RETURN OBJECT_CONSTRUCT(
    'success', TRUE,
    'username', :uname,
    'role', :rname,
    'actor_type', :actor_type,
    'email', :actor_email,
    
    -- Next action instructions
    'next_action', CASE 
      WHEN :auth_mode = 'KEYPAIR' THEN 
        'Upload RSA public key within 10 minutes using: ALTER USER ' || :uname || ' SET RSA_PUBLIC_KEY = ''<YOUR_PUBLIC_KEY>'';'
      ELSE 
        'User must change password on first login'
    END,
    
    -- Key deadline (for agents)
    'key_deadline', :key_deadline,
    
    -- Complete .env configuration
    'env_config', OBJECT_CONSTRUCT(
      '_comment', 'Add these to your .env file',
      'SNOWFLAKE_ACCOUNT', CURRENT_ACCOUNT() || '.' || CURRENT_REGION(),
      'SNOWFLAKE_USERNAME', :uname,
      'SNOWFLAKE_PASSWORD', IFF(:auth_mode = 'PASSWORD', '<password_here>', NULL),
      'SF_PK_PATH', IFF(:auth_mode = 'KEYPAIR', '/path/to/private_key.p8', NULL),
      'SNOWFLAKE_ROLE', :rname,
      'SNOWFLAKE_WAREHOUSE', IFF(:actor_type = 'AGENT', 'CLAUDE_AGENT_WH', 'CLAUDE_WAREHOUSE'),
      'SNOWFLAKE_DATABASE', 'CLAUDE_BI',
      'SNOWFLAKE_SCHEMA', 'MCP'
    ),
    
    -- Connection test command
    'test_command', 'snowsql -a ' || CURRENT_ACCOUNT() || ' -u ' || :uname || ' -r ' || :rname ||
                   ' -w ' || IFF(:actor_type = 'AGENT', 'CLAUDE_AGENT_WH', 'CLAUDE_WAREHOUSE') ||
                   ' -d CLAUDE_BI -s MCP -q "SELECT CURRENT_USER(), CURRENT_ROLE();"',
    
    -- Permissions summary
    'permissions', OBJECT_CONSTRUCT(
      'can_read', TRUE,
      'can_write', :can_write,
      'warehouse', IFF(:actor_type = 'AGENT', 'CLAUDE_AGENT_WH', 'CLAUDE_WAREHOUSE'),
      'resource_monitor', IFF(:actor_type = 'AGENT', 'RM_AGENT_DAILY (10 credits/day)', 'None')
    )
  );
  
EXCEPTION
  WHEN OTHER THEN
    RETURN OBJECT_CONSTRUCT(
      'success', FALSE,
      'error', 'Provisioning failed',
      'details', SQLERRM,
      'code', SQLCODE
    );
END;
$$;

-- ============================================================================
-- Key rotation helper for agents
-- ============================================================================
CREATE OR REPLACE PROCEDURE ROTATE_AGENT_KEY(
  agent_username STRING,
  new_public_key STRING
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
COMMENT = 'Rotate RSA key pair for an agent user'
AS $$
BEGIN
  -- Validate user exists and is an agent
  LET user_check VARIANT := (
    SELECT OBJECT_CONSTRUCT(
      'exists', COUNT(*) > 0,
      'actor_type', MAX(actor_type)
    )
    FROM CLAUDE_BI.ADMIN.ACTOR_REGISTRY
    WHERE username = UPPER(:agent_username)
      AND is_active = TRUE
  );
  
  IF (NOT user_check:exists) THEN
    RETURN OBJECT_CONSTRUCT(
      'success', FALSE,
      'error', 'User not found in registry: ' || agent_username
    );
  END IF;
  
  IF (user_check:actor_type != 'AGENT') THEN
    RETURN OBJECT_CONSTRUCT(
      'success', FALSE,
      'error', 'Key rotation is only for AGENT users'
    );
  END IF;
  
  -- Update the RSA key
  EXECUTE IMMEDIATE 
    'ALTER USER ' || :agent_username || 
    ' SET RSA_PUBLIC_KEY = ''' || :new_public_key || '''';
  
  -- Update registry
  UPDATE CLAUDE_BI.ADMIN.ACTOR_REGISTRY
  SET last_rotation = CURRENT_TIMESTAMP(),
      next_rotation_due = DATEADD('day', 30, CURRENT_TIMESTAMP()),
      key_uploaded_at = CURRENT_TIMESTAMP()
  WHERE username = UPPER(:agent_username);
  
  -- Audit event
  INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS
  SELECT OBJECT_CONSTRUCT(
    'event_id', UUID_STRING(),
    'action', 'system.key.rotated',
    'actor_id', CURRENT_USER(),
    'occurred_at', CURRENT_TIMESTAMP(),
    'object', OBJECT_CONSTRUCT(
      'type', 'user',
      'id', :agent_username
    ),
    'attributes', OBJECT_CONSTRUCT(
      'rotated_by', CURRENT_USER(),
      'next_rotation_due', DATEADD('day', 30, CURRENT_TIMESTAMP())
    )
  ), 'ADMIN', CURRENT_TIMESTAMP();
  
  RETURN OBJECT_CONSTRUCT(
    'success', TRUE,
    'username', :agent_username,
    'rotated_at', CURRENT_TIMESTAMP(),
    'next_rotation_due', DATEADD('day', 30, CURRENT_TIMESTAMP())
  );
END;
$$;

-- ============================================================================
-- Check and enforce key deadlines (run periodically)
-- ============================================================================
CREATE OR REPLACE PROCEDURE ENFORCE_KEY_DEADLINES()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
COMMENT = 'Disable users who missed their RSA key upload deadline'
AS $$
DECLARE
  disabled_count INTEGER DEFAULT 0;
  disabled_users ARRAY DEFAULT ARRAY_CONSTRUCT();
BEGIN
  -- Find users past their key deadline
  FOR record IN (
    SELECT username
    FROM CLAUDE_BI.ADMIN.ACTOR_REGISTRY
    WHERE auth_mode = 'KEYPAIR'
      AND key_deadline < CURRENT_TIMESTAMP()
      AND key_uploaded_at IS NULL
      AND is_active = TRUE
  ) DO
    -- Disable the user
    EXECUTE IMMEDIATE 'ALTER USER ' || record.username || ' SET DISABLED = TRUE';
    
    -- Update registry
    UPDATE CLAUDE_BI.ADMIN.ACTOR_REGISTRY
    SET is_active = FALSE,
        metadata = OBJECT_INSERT(metadata, 'disabled_reason', 'Key deadline missed')
    WHERE username = record.username;
    
    disabled_count := disabled_count + 1;
    disabled_users := ARRAY_APPEND(disabled_users, record.username);
  END FOR;
  
  RETURN OBJECT_CONSTRUCT(
    'success', TRUE,
    'disabled_count', disabled_count,
    'disabled_users', disabled_users,
    'checked_at', CURRENT_TIMESTAMP()
  );
END;
$$;

-- ============================================================================
-- Grant permissions
-- ============================================================================
GRANT USAGE ON PROCEDURE PROVISION_ACTOR(STRING, STRING, BOOLEAN, STRING, STRING) TO ROLE R_APP_ADMIN;
GRANT USAGE ON PROCEDURE ROTATE_AGENT_KEY(STRING, STRING) TO ROLE R_APP_ADMIN;
GRANT USAGE ON PROCEDURE ENFORCE_KEY_DEADLINES() TO ROLE R_APP_ADMIN;

-- ============================================================================
-- Success message
-- ============================================================================
SELECT 'Provisioning procedures created successfully!' AS status,
       'Next step: Run 03_workload_procedures.sql' AS next_action;