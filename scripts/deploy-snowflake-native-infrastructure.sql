-- ===================================================================
-- ALL-SNOWFLAKE NATIVE DASHBOARD INFRASTRUCTURE
-- Phase 1: Network Rules, Secrets, External Access, and Stages
-- ===================================================================

-- Use the MCP database and schema
USE DATABASE CLAUDE_BI;
USE SCHEMA MCP;

-- ===================================================================
-- 1. NETWORK RULES - External Access Controls
-- ===================================================================

-- Allow outbound HTTPS to Claude API and Slack webhooks only
CREATE OR REPLACE NETWORK RULE MCP.CLAUDE_EGRESS
  TYPE = HOST_PORT 
  MODE = EGRESS
  VALUE_LIST = ('api.anthropic.com:443', 'hooks.slack.com:443')
  COMMENT = 'Allowlist for Claude API and Slack webhook outbound access';

-- ===================================================================
-- 2. SECRETS - Secure credential storage
-- ===================================================================

-- Claude API key secret (set manually in production)
CREATE OR REPLACE SECRET MCP.CLAUDE_API_KEY 
  TYPE = GENERIC_STRING 
  VALUE = 'sk-placeholder-set-manually-in-prod'
  COMMENT = 'Claude API key for NL processing - SET MANUALLY IN PRODUCTION';

-- Slack webhook secret (optional for notifications)
CREATE OR REPLACE SECRET MCP.SLACK_WEBHOOK_URL 
  TYPE = GENERIC_STRING 
  VALUE = 'https://hooks.slack.com/placeholder'
  COMMENT = 'Slack webhook URL for dashboard notifications';

-- ===================================================================
-- 3. EXTERNAL ACCESS INTEGRATION - Outbound API access
-- ===================================================================

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION MCP.CLAUDE_EAI
  ALLOWED_NETWORK_RULES = (MCP.CLAUDE_EGRESS)
  ALLOWED_AUTHENTICATION_SECRETS = (MCP.CLAUDE_API_KEY, MCP.SLACK_WEBHOOK_URL)
  ENABLED = TRUE
  COMMENT = 'External access for Claude API and notification delivery';

-- ===================================================================
-- 4. NAMED STAGES - Pointer-based storage (Two-Table Law compliant)
-- ===================================================================

-- Dashboard specifications (JSON files)
CREATE STAGE IF NOT EXISTS MCP.DASH_SPECS
  ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
  FILE_FORMAT = (TYPE = JSON)
  COMMENT = 'Dashboard specification JSON files - pointer storage only';

-- Generated snapshots (PNG/PDF exports)
CREATE STAGE IF NOT EXISTS MCP.DASH_SNAPSHOTS
  ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
  FILE_FORMAT = (TYPE = PARQUET)  -- For binary files
  COMMENT = 'Dashboard snapshot exports - PNG/PDF files';

-- User cohort files (JSONL uploads)
CREATE STAGE IF NOT EXISTS MCP.DASH_COHORTS
  ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
  FILE_FORMAT = (TYPE = JSON)
  COMMENT = 'User cohort JSONL files for filtering';

-- Streamlit app deployments
CREATE STAGE IF NOT EXISTS MCP.DASH_APPS
  ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
  COMMENT = 'Streamlit application files and assets';

-- ===================================================================
-- 5. VALIDATION VIEWS - Monitor infrastructure health
-- ===================================================================

-- View external access integration status
CREATE OR REPLACE VIEW MCP.VW_EXTERNAL_ACCESS_STATUS AS
SELECT 
  name,
  type,
  enabled,
  comment,
  created,
  last_altered
FROM INFORMATION_SCHEMA.EXTERNAL_ACCESS_INTEGRATIONS
WHERE name = 'CLAUDE_EAI';

-- View stages and their usage
CREATE OR REPLACE VIEW MCP.VW_STAGE_INVENTORY AS
SELECT 
  stage_schema || '.' || stage_name as full_name,
  stage_type,
  stage_url,
  comment,
  created,
  last_altered
FROM INFORMATION_SCHEMA.STAGES
WHERE stage_schema = 'MCP' 
  AND stage_name LIKE 'DASH_%';

-- ===================================================================
-- 6. GRANTS - Secure access for Claude agent role
-- ===================================================================

-- Grant usage on external access integration
GRANT USAGE ON INTEGRATION MCP.CLAUDE_EAI TO ROLE R_CLAUDE_AGENT;

-- Grant stage access for dashboard operations
GRANT READ, WRITE ON STAGE MCP.DASH_SPECS TO ROLE R_CLAUDE_AGENT;
GRANT READ, WRITE ON STAGE MCP.DASH_SNAPSHOTS TO ROLE R_CLAUDE_AGENT;
GRANT READ, WRITE ON STAGE MCP.DASH_COHORTS TO ROLE R_CLAUDE_AGENT;
GRANT READ, WRITE ON STAGE MCP.DASH_APPS TO ROLE R_CLAUDE_AGENT;

-- Grant view access for monitoring
GRANT SELECT ON VIEW MCP.VW_EXTERNAL_ACCESS_STATUS TO ROLE R_CLAUDE_AGENT;
GRANT SELECT ON VIEW MCP.VW_STAGE_INVENTORY TO ROLE R_CLAUDE_AGENT;

-- ===================================================================
-- 7. WAREHOUSE OPTIMIZATION - Cost control for serverless tasks
-- ===================================================================

-- Optimize warehouse for small-scale operations
ALTER WAREHOUSE CLAUDE_AGENT_WH SET
  AUTO_SUSPEND = 60  -- 1 minute auto-suspend
  AUTO_RESUME = TRUE
  MIN_CLUSTER_COUNT = 1
  MAX_CLUSTER_COUNT = 1
  SCALING_POLICY = 'STANDARD'
  COMMENT = 'Optimized for 5-10 user dashboard operations with fast auto-suspend';

-- ===================================================================
-- 8. VALIDATION QUERIES - Test infrastructure
-- ===================================================================

-- Test external access integration
SELECT 'External Access Integration' as component, 
       CASE WHEN enabled THEN 'READY' ELSE 'NOT_ENABLED' END as status
FROM INFORMATION_SCHEMA.EXTERNAL_ACCESS_INTEGRATIONS 
WHERE name = 'CLAUDE_EAI';

-- Test stages
SELECT 'Stages' as component,
       COUNT(*) as stage_count,
       CASE WHEN COUNT(*) = 4 THEN 'READY' ELSE 'INCOMPLETE' END as status
FROM INFORMATION_SCHEMA.STAGES
WHERE stage_schema = 'MCP' AND stage_name LIKE 'DASH_%';

-- Test network rules
SELECT 'Network Rules' as component,
       COUNT(*) as rule_count,
       CASE WHEN COUNT(*) >= 1 THEN 'READY' ELSE 'MISSING' END as status
FROM INFORMATION_SCHEMA.NETWORK_RULES
WHERE name = 'CLAUDE_EGRESS';

-- ===================================================================
-- DEPLOYMENT NOTES
-- ===================================================================

/*
PRODUCTION SECURITY CHECKLIST:

1. SECRETS - Update manually in production:
   ALTER SECRET MCP.CLAUDE_API_KEY SET VALUE = 'sk-ant-api03-your-real-key';
   ALTER SECRET MCP.SLACK_WEBHOOK_URL SET VALUE = 'https://hooks.slack.com/your-webhook';

2. NETWORK RULES - Verify egress allowlist:
   - api.anthropic.com:443 (Claude API)
   - hooks.slack.com:443 (Slack notifications)
   - NO wildcards or overly broad access

3. RBAC - Ensure R_CLAUDE_AGENT has minimal required privileges:
   - USAGE on CLAUDE_EAI integration
   - READ/WRITE on DASH_* stages only
   - EXECUTE on dashboard procedures only
   - NO table access (Two-Table Law enforcement)

4. COST CONTROLS:
   - XSMALL warehouse with 60s auto-suspend
   - Clamp all user inputs (n≤50, limit≤5000)
   - Monitor task execution frequency

5. MONITORING:
   - Query VW_EXTERNAL_ACCESS_STATUS regularly
   - Monitor stage storage usage
   - Track procedure execution costs

Ready for Phase 2: Core Procedures Implementation
*/