-- Test Authentication Deployment
-- Run this manually in Snowflake to verify the system works

USE ROLE ACCOUNTADMIN;
USE DATABASE CLAUDE_BI;
USE SCHEMA MCP;

-- Create a simple test user manually
INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS VALUES (
  OBJECT_CONSTRUCT(
    'event_id', UUID_STRING(),
    'action', 'system.user.created',
    'occurred_at', CURRENT_TIMESTAMP(),
    'actor_id', CURRENT_USER(),
    'source', 'system',
    'object', OBJECT_CONSTRUCT(
      'type', 'user',
      'id', 'test_auth_user'
    ),
    'attributes', OBJECT_CONSTRUCT(
      'email', 'test@example.com',
      'token_hash', SHA2('test_token_12345' || 'pepper', 256),
      'token_prefix', 'tk_test',
      'allowed_tools', ARRAY_CONSTRUCT('compose_query', 'list_sources'),
      'max_rows', 10000,
      'expires_at', DATEADD('day', 30, CURRENT_TIMESTAMP())
    )
  ),
  'ADMIN',
  CURRENT_TIMESTAMP()
);

-- Grant permissions
INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS VALUES (
  OBJECT_CONSTRUCT(
    'event_id', UUID_STRING(),
    'action', 'system.permission.granted',
    'occurred_at', CURRENT_TIMESTAMP(),
    'actor_id', CURRENT_USER(),
    'source', 'system',
    'object', OBJECT_CONSTRUCT(
      'type', 'user',
      'id', 'test_auth_user'
    ),
    'attributes', OBJECT_CONSTRUCT(
      'token_hash', SHA2('test_token_12345' || 'pepper', 256),
      'token_prefix', 'tk_test',
      'allowed_tools', ARRAY_CONSTRUCT('compose_query', 'list_sources'),
      'max_rows', 10000,
      'daily_runtime_seconds', 3600,
      'expires_at', DATEADD('day', 30, CURRENT_TIMESTAMP())
    )
  ),
  'ADMIN',
  CURRENT_TIMESTAMP()
);

-- Check the results
SELECT 
  object_id AS username,
  action,
  attributes:token_prefix::STRING AS token_prefix,
  attributes:allowed_tools AS tools,
  attributes:expires_at AS expires_at
FROM CLAUDE_BI.ACTIVITY.EVENTS
WHERE object_type = 'user'
  AND object_id = 'test_auth_user'
ORDER BY occurred_at DESC;