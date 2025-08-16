-- ===================================================================
-- ADMIN COMPONENTS DEPLOYMENT (FIXED ORDER)
-- Run as ACCOUNTADMIN to create security components
-- ===================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE CLAUDE_BI;
USE SCHEMA MCP;

-- ===================================================================
-- STEP 1: CREATE SECRETS
-- ===================================================================

-- Create Claude API key secret
CREATE OR REPLACE SECRET MCP.CLAUDE_API_KEY
  TYPE = GENERIC_STRING
  COMMENT = 'Claude API key for natural language processing - UPDATE WITH ACTUAL KEY';

-- Create Slack webhook secret  
CREATE OR REPLACE SECRET MCP.SLACK_WEBHOOK_URL
  TYPE = GENERIC_STRING
  COMMENT = 'Slack webhook URL for notifications - UPDATE WITH ACTUAL URL OR LEAVE EMPTY';

-- ===================================================================
-- STEP 2: CREATE NETWORK RULE
-- ===================================================================

CREATE OR REPLACE NETWORK RULE MCP.CLAUDE_EGRESS
  TYPE = HOST_PORT
  MODE = EGRESS
  VALUE_LIST = ('api.anthropic.com:443', 'hooks.slack.com:443')
  COMMENT = 'Allow outbound connections to Claude API and Slack';

-- ===================================================================
-- STEP 3: CREATE EXTERNAL ACCESS INTEGRATION
-- ===================================================================

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION MCP.CLAUDE_EAI
  ALLOWED_NETWORK_RULES = (MCP.CLAUDE_EGRESS)
  ALLOWED_AUTHENTICATION_SECRETS = (MCP.CLAUDE_API_KEY, MCP.SLACK_WEBHOOK_URL)
  ENABLED = TRUE
  COMMENT = 'External access for Claude API and Slack webhooks';

-- ===================================================================
-- STEP 4: GRANT PERMISSIONS
-- ===================================================================

-- Grant integration usage
GRANT USAGE ON INTEGRATION MCP.CLAUDE_EAI TO USER CLAUDE_CODE_AI_AGENT;

-- Grant secret access
GRANT READ ON SECRET MCP.CLAUDE_API_KEY TO USER CLAUDE_CODE_AI_AGENT;
GRANT READ ON SECRET MCP.SLACK_WEBHOOK_URL TO USER CLAUDE_CODE_AI_AGENT;

-- ===================================================================
-- STEP 5: UPDATE SECRET VALUES (placeholder for now)
-- ===================================================================

-- Set placeholder values (update with real values later)
ALTER SECRET MCP.CLAUDE_API_KEY SET SECRET_STRING = 'sk-ant-api03-placeholder-update-with-real-key';
ALTER SECRET MCP.SLACK_WEBHOOK_URL SET SECRET_STRING = 'https://hooks.slack.com/services/placeholder';

-- ===================================================================
-- STEP 6: VERIFICATION
-- ===================================================================

SELECT 'SUCCESS: Admin components created!' as status,
       'Secrets created with placeholders' as secrets_status,
       'Network rule created' as network_status,
       'External Access Integration created' as eai_status,
       'Permissions granted' as grants_status;