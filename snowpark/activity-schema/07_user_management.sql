-- ============================================================================
-- 07_user_management.sql
-- Procedures to create and manage MCP users
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE CLAUDE_BI;
USE SCHEMA MCP;

-- ============================================================================
-- Create a new MCP user with permissions
-- ============================================================================
CREATE OR REPLACE PROCEDURE CLAUDE_BI.MCP.CREATE_MCP_USER(
  username STRING,
  user_email STRING,
  department STRING,
  allowed_actions ARRAY,
  max_rows NUMBER DEFAULT 10000,
  daily_runtime_budget_s NUMBER DEFAULT 120
)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
BEGIN
  -- Validate inputs
  IF (username IS NULL OR TRIM(username) = '') THEN
    RETURN 'Error: Username cannot be empty';
  END IF;
  
  -- Create Snowflake user with temporary password
  LET create_user_sql STRING := 'CREATE USER IF NOT EXISTS ' || :username || 
    ' PASSWORD = ''TempPassword123!'' ' ||
    ' DEFAULT_ROLE = MCP_USER_ROLE ' ||
    ' DEFAULT_WAREHOUSE = MCP_XS_WH ' ||
    ' MUST_CHANGE_PASSWORD = TRUE ' ||
    ' COMMENT = ''' || :user_email || ' - ' || :department || '''';
  
  EXECUTE IMMEDIATE :create_user_sql;
  
  -- Grant the MCP_USER_ROLE to the new user
  LET grant_role_sql STRING := 'GRANT ROLE MCP_USER_ROLE TO USER ' || :username;
  EXECUTE IMMEDIATE :grant_role_sql;
  
  -- Grant permissions via event (expires in 1 year by default)
  CALL CLAUDE_BI.MCP.GRANT_USER_PERMISSION(
    :username,
    :allowed_actions,
    :max_rows,
    :daily_runtime_budget_s,
    FALSE,  -- can_export default to false
    DATEADD('year', 1, CURRENT_TIMESTAMP())  -- expires in 1 year
  );
  
  -- Log user creation as an event
  INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS
  SELECT OBJECT_CONSTRUCT(
    'event_id', UUID_STRING(),
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
      'email', :user_email,
      'department', :department,
      'allowed_actions', :allowed_actions,
      'max_rows', :max_rows,
      'daily_runtime_budget_s', :daily_runtime_budget_s,
      'created_by', CURRENT_USER()
    )
  ), 'ADMIN', CURRENT_TIMESTAMP();
  
  RETURN 'User ' || :username || ' created successfully with temporary password TempPassword123!';
END;
$$;

-- ============================================================================
-- Disable a user (revokes permissions and disables login)
-- ============================================================================
CREATE OR REPLACE PROCEDURE CLAUDE_BI.MCP.DISABLE_MCP_USER(
  username STRING,
  reason STRING
)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
BEGIN
  -- Revoke permissions via event
  CALL CLAUDE_BI.MCP.REVOKE_USER_PERMISSION(:username, :reason);
  
  -- Disable the Snowflake user
  LET disable_sql STRING := 'ALTER USER ' || :username || ' SET DISABLED = TRUE';
  EXECUTE IMMEDIATE :disable_sql;
  
  -- Log user disable as an event
  INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS
  SELECT OBJECT_CONSTRUCT(
    'event_id', UUID_STRING(),
    'action', 'system.user.disabled',
    'occurred_at', CURRENT_TIMESTAMP(),
    'actor_id', CURRENT_USER(),
    'source', 'system',
    'schema_version', '2.1.0',
    'object', OBJECT_CONSTRUCT(
      'type', 'user',
      'id', :username
    ),
    'attributes', OBJECT_CONSTRUCT(
      'reason', :reason,
      'disabled_by', CURRENT_USER()
    )
  ), 'ADMIN', CURRENT_TIMESTAMP();
  
  RETURN 'User ' || :username || ' has been disabled';
END;
$$;

-- ============================================================================
-- Re-enable a user
-- ============================================================================
CREATE OR REPLACE PROCEDURE CLAUDE_BI.MCP.ENABLE_MCP_USER(
  username STRING,
  allowed_actions ARRAY,
  max_rows NUMBER DEFAULT 10000,
  daily_runtime_budget_s NUMBER DEFAULT 120
)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
BEGIN
  -- Re-enable the Snowflake user
  LET enable_sql STRING := 'ALTER USER ' || :username || ' SET DISABLED = FALSE';
  EXECUTE IMMEDIATE :enable_sql;
  
  -- Grant new permissions
  CALL CLAUDE_BI.MCP.GRANT_USER_PERMISSION(
    :username,
    :allowed_actions,
    :max_rows,
    :daily_runtime_budget_s,
    FALSE,
    DATEADD('year', 1, CURRENT_TIMESTAMP())
  );
  
  -- Log user enable as an event
  INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS
  SELECT OBJECT_CONSTRUCT(
    'event_id', UUID_STRING(),
    'action', 'system.user.enabled',
    'occurred_at', CURRENT_TIMESTAMP(),
    'actor_id', CURRENT_USER(),
    'source', 'system',
    'schema_version', '2.1.0',
    'object', OBJECT_CONSTRUCT(
      'type', 'user',
      'id', :username
    ),
    'attributes', OBJECT_CONSTRUCT(
      'enabled_by', CURRENT_USER(),
      'new_permissions', OBJECT_CONSTRUCT(
        'allowed_actions', :allowed_actions,
        'max_rows', :max_rows,
        'daily_runtime_budget_s', :daily_runtime_budget_s
      )
    )
  ), 'ADMIN', CURRENT_TIMESTAMP();
  
  RETURN 'User ' || :username || ' has been re-enabled with new permissions';
END;
$$;

-- ============================================================================
-- Update user permissions (creates new permission event)
-- ============================================================================
CREATE OR REPLACE PROCEDURE CLAUDE_BI.MCP.UPDATE_USER_PERMISSIONS(
  username STRING,
  allowed_actions ARRAY,
  max_rows NUMBER,
  daily_runtime_budget_s NUMBER,
  can_export BOOLEAN,
  expires_at TIMESTAMP_TZ
)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
BEGIN
  -- Simply grant new permissions - latest event wins
  CALL CLAUDE_BI.MCP.GRANT_USER_PERMISSION(
    :username,
    :allowed_actions,
    :max_rows,
    :daily_runtime_budget_s,
    :can_export,
    :expires_at
  );
  
  RETURN 'Permissions updated for ' || :username;
END;
$$;

-- ============================================================================
-- Grant procedure permissions to admin role
-- ============================================================================
GRANT USAGE ON PROCEDURE CREATE_MCP_USER(STRING, STRING, STRING, ARRAY, NUMBER, NUMBER) TO ROLE MCP_ADMIN_ROLE;
GRANT USAGE ON PROCEDURE DISABLE_MCP_USER(STRING, STRING) TO ROLE MCP_ADMIN_ROLE;
GRANT USAGE ON PROCEDURE ENABLE_MCP_USER(STRING, ARRAY, NUMBER, NUMBER) TO ROLE MCP_ADMIN_ROLE;
GRANT USAGE ON PROCEDURE UPDATE_USER_PERMISSIONS(STRING, ARRAY, NUMBER, NUMBER, BOOLEAN, TIMESTAMP_TZ) TO ROLE MCP_ADMIN_ROLE;