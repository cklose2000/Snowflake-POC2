-- ===================================================================
-- ADMIN COMPONENTS DEPLOYMENT
-- Run as ACCOUNTADMIN to create security components
-- ===================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE CLAUDE_BI;
USE SCHEMA MCP;

-- ===================================================================
-- 1. CREATE SECRETS (without values - will set placeholder)
-- ===================================================================

-- Create Claude API key secret
CREATE OR REPLACE SECRET MCP.CLAUDE_API_KEY
  TYPE = GENERIC_STRING
  COMMENT = 'Claude API key for natural language processing - UPDATE WITH ACTUAL KEY';

-- Create Slack webhook secret (optional)
CREATE OR REPLACE SECRET MCP.SLACK_WEBHOOK_URL
  TYPE = GENERIC_STRING
  COMMENT = 'Slack webhook URL for notifications - UPDATE WITH ACTUAL URL OR LEAVE EMPTY';

-- ===================================================================
-- 2. CREATE NETWORK RULE
-- ===================================================================

CREATE OR REPLACE NETWORK RULE MCP.CLAUDE_EGRESS
  TYPE = HOST_PORT
  MODE = EGRESS
  VALUE_LIST = ('api.anthropic.com:443', 'hooks.slack.com:443')
  COMMENT = 'Allow outbound connections to Claude API and Slack';

-- ===================================================================
-- 3. CREATE EXTERNAL ACCESS INTEGRATION
-- ===================================================================

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION MCP.CLAUDE_EAI
  ALLOWED_NETWORK_RULES = (MCP.CLAUDE_EGRESS)
  ALLOWED_AUTHENTICATION_SECRETS = (MCP.CLAUDE_API_KEY, MCP.SLACK_WEBHOOK_URL)
  ENABLED = TRUE
  COMMENT = 'External access for Claude API and Slack webhooks';

-- ===================================================================
-- 4. GRANT USAGE ON INTEGRATION
-- ===================================================================

-- Grant to CLAUDE_CODE_AI_AGENT user
GRANT USAGE ON INTEGRATION MCP.CLAUDE_EAI TO USER CLAUDE_CODE_AI_AGENT;

-- ===================================================================
-- 5. GRANT SECRET ACCESS
-- ===================================================================

GRANT READ ON SECRET MCP.CLAUDE_API_KEY TO USER CLAUDE_CODE_AI_AGENT;
GRANT READ ON SECRET MCP.SLACK_WEBHOOK_URL TO USER CLAUDE_CODE_AI_AGENT;

-- ===================================================================
-- 6. VERIFICATION
-- ===================================================================

-- Show created components
SHOW SECRETS IN SCHEMA MCP;
SHOW NETWORK RULES IN SCHEMA MCP;
SHOW EXTERNAL ACCESS INTEGRATIONS;

-- Verify grants
SHOW GRANTS TO USER CLAUDE_CODE_AI_AGENT;

SELECT 'Admin components created successfully!' as status,
       'IMPORTANT: You must manually update the secret values' as next_step,
       'Run: ALTER SECRET MCP.CLAUDE_API_KEY SET SECRET_STRING = ''your-actual-api-key''' as command;