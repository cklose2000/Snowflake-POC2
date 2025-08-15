-- ============================================================================
-- 04_provision_users.sql
-- Provision test users: Sarah (human) and Claude Code (agent)
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE CLAUDE_BI;
USE SCHEMA ADMIN;

-- ============================================================================
-- PROVISION SARAH - Marketing Analyst (Human)
-- ============================================================================

CALL PROVISION_ACTOR(
  'sarah@company.com',           -- actor_email
  'HUMAN',                        -- actor_type  
  FALSE,                          -- can_write (read-only for analyst)
  'PASSWORD',                     -- auth_mode
  'TempPassword123!@#'            -- initial_secret (must change on first login)
);

-- The output will show:
-- username: SARAH_COMPANY_COM
-- role: R_ACTOR_HUM_<hash>
-- Next action: Change password on first login
-- Complete .env configuration

-- ============================================================================
-- PROVISION CLAUDE CODE - AI Agent
-- ============================================================================

CALL PROVISION_ACTOR(
  'claude.code@ai.assistant',    -- actor_email
  'AGENT',                        -- actor_type
  TRUE,                           -- can_write (needs to insert events)
  'KEYPAIR',                      -- auth_mode
  NULL                            -- initial_secret (not needed for keypair)
);

-- The output will show:
-- username: CLAUDE_CODE_AI_ASSISTANT
-- role: R_ACTOR_AGT_<hash>
-- Next action: Upload RSA public key within 10 minutes
-- Key deadline: <timestamp>

-- ============================================================================
-- GENERATE RSA KEY PAIR FOR CLAUDE CODE
-- Run these commands on your local machine:
-- ============================================================================

/*
-- 1. Generate private key (run in terminal):
openssl genrsa -out claude_code_rsa_key.pem 2048

-- 2. Convert to PKCS8 format (required by Snowflake Node SDK):
openssl pkcs8 -topk8 -inform PEM -outform PEM -nocrypt \
  -in claude_code_rsa_key.pem \
  -out claude_code_rsa_key.p8

-- 3. Extract public key:
openssl rsa -in claude_code_rsa_key.pem -pubout -out claude_code_rsa_key.pub

-- 4. Get the public key content (remove header/footer):
cat claude_code_rsa_key.pub | grep -v "BEGIN PUBLIC KEY" | grep -v "END PUBLIC KEY" | tr -d '\n'
*/

-- ============================================================================
-- UPLOAD PUBLIC KEY FOR CLAUDE CODE
-- Replace <PUBLIC_KEY> with the output from step 4 above
-- ============================================================================

-- Uncomment and run after generating the key:
/*
ALTER USER CLAUDE_CODE_AI_ASSISTANT 
SET RSA_PUBLIC_KEY = '<PUBLIC_KEY_CONTENT_HERE>';

-- Verify the key was set:
DESC USER CLAUDE_CODE_AI_ASSISTANT;
*/

-- ============================================================================
-- PROVISION ADDITIONAL TEST USER - Data Engineer (Human with write)
-- ============================================================================

CALL PROVISION_ACTOR(
  'alex@company.com',             -- actor_email
  'HUMAN',                        -- actor_type
  TRUE,                           -- can_write (data engineer needs write)
  'PASSWORD',                     -- auth_mode
  'EngineerPass456$%^'            -- initial_secret
);

-- ============================================================================
-- VIEW ALL PROVISIONED ACTORS
-- ============================================================================

SELECT 
  username,
  email,
  actor_type,
  auth_mode,
  role_name,
  CASE 
    WHEN key_deadline IS NOT NULL AND key_uploaded_at IS NULL 
    THEN 'Awaiting key upload by ' || key_deadline::STRING
    WHEN key_uploaded_at IS NOT NULL 
    THEN 'Key uploaded at ' || key_uploaded_at::STRING
    ELSE 'Password auth'
  END AS auth_status,
  is_active,
  provisioned_at,
  provisioned_by
FROM CLAUDE_BI.ADMIN.ACTOR_REGISTRY
ORDER BY provisioned_at DESC;

-- ============================================================================
-- TEST CONNECTION COMMANDS
-- ============================================================================

SELECT 'Test connections:' AS instructions;

-- For Sarah (password auth):
SELECT 'Sarah: snowsql -a ' || CURRENT_ACCOUNT() || ' -u SARAH_COMPANY_COM -r R_ACTOR_HUM_<hash> -w CLAUDE_WAREHOUSE' AS command;

-- For Claude Code (key-pair auth):
SELECT 'Claude: export SF_PK_PATH=/path/to/claude_code_rsa_key.p8' AS step1;
SELECT 'Claude: snowsql -a ' || CURRENT_ACCOUNT() || ' -u CLAUDE_CODE_AI_ASSISTANT --private-key-path $SF_PK_PATH -r R_ACTOR_AGT_<hash> -w CLAUDE_AGENT_WH' AS step2;

-- ============================================================================
-- CREATE .env FILES FOR EACH USER
-- ============================================================================

-- Sarah's .env (human, password):
SELECT '
# Sarah - Marketing Analyst (.env)
SNOWFLAKE_ACCOUNT=' || CURRENT_ACCOUNT() || '.' || CURRENT_REGION() || '
SNOWFLAKE_USERNAME=SARAH_COMPANY_COM
SNOWFLAKE_PASSWORD=<password_after_change>
SNOWFLAKE_ROLE=R_ACTOR_HUM_<hash>
SNOWFLAKE_WAREHOUSE=CLAUDE_WAREHOUSE
SNOWFLAKE_DATABASE=CLAUDE_BI
SNOWFLAKE_SCHEMA=MCP
' AS sarah_env;

-- Claude Code's .env (agent, keypair):
SELECT '
# Claude Code - AI Agent (.env)
SNOWFLAKE_ACCOUNT=' || CURRENT_ACCOUNT() || '.' || CURRENT_REGION() || '
SNOWFLAKE_USERNAME=CLAUDE_CODE_AI_ASSISTANT
SF_PK_PATH=/path/to/claude_code_rsa_key.p8
SNOWFLAKE_ROLE=R_ACTOR_AGT_<hash>
SNOWFLAKE_WAREHOUSE=CLAUDE_AGENT_WH
SNOWFLAKE_DATABASE=CLAUDE_BI
SNOWFLAKE_SCHEMA=MCP
' AS claude_env;

-- ============================================================================
-- QUICK PERMISSION TEST
-- ============================================================================

-- Test Sarah can read:
-- EXECUTE AS USER SARAH_COMPANY_COM;
-- CALL CLAUDE_BI.MCP.LIST_SOURCES();
-- CALL CLAUDE_BI.MCP.GET_USER_STATUS();

-- Test Claude Code can write:
-- EXECUTE AS USER CLAUDE_CODE_AI_ASSISTANT;
-- CALL CLAUDE_BI.MCP.SAFE_INSERT_EVENT(
--   OBJECT_CONSTRUCT(
--     'action', 'test.agent.connected',
--     'actor_id', 'claude_code',
--     'attributes', OBJECT_CONSTRUCT('test', TRUE)
--   ),
--   'TEST'
-- );

-- ============================================================================
-- Success message
-- ============================================================================
SELECT 'Users provisioned successfully!' AS status,
       'Next steps:' AS action,
       '1. Generate RSA key pair for Claude Code' AS step1,
       '2. Upload public key with ALTER USER' AS step2,
       '3. Update snowflake-mcp-client code' AS step3,
       '4. Test connections' AS step4;