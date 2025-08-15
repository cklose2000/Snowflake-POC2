-- ============================================================================
-- 01_security_foundation.sql
-- Create base roles, policies, and grants for native Snowflake auth
-- ============================================================================

USE ROLE SECURITYADMIN;

-- ============================================================================
-- SECTION 1: Core Application Roles
-- ============================================================================

-- Read-only role for querying and viewing data
CREATE ROLE IF NOT EXISTS R_APP_READ
  COMMENT = 'Base read role for Claude Code application';

-- Write role for data ingestion
CREATE ROLE IF NOT EXISTS R_APP_WRITE
  COMMENT = 'Base write role for event ingestion';

-- Admin role for provisioning and management
CREATE ROLE IF NOT EXISTS R_APP_ADMIN
  COMMENT = 'Admin role for user provisioning and system management';

-- Role hierarchy
GRANT ROLE R_APP_READ TO ROLE R_APP_WRITE;
GRANT ROLE R_APP_WRITE TO ROLE R_APP_ADMIN;
GRANT ROLE R_APP_ADMIN TO ROLE SECURITYADMIN;

-- ============================================================================
-- SECTION 2: Database and Schema Grants with Future Grants
-- ============================================================================

USE ROLE ACCOUNTADMIN;

-- Database access for readers
GRANT USAGE ON DATABASE CLAUDE_BI TO ROLE R_APP_READ;
GRANT USAGE ON ALL SCHEMAS IN DATABASE CLAUDE_BI TO ROLE R_APP_READ;
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE CLAUDE_BI TO ROLE R_APP_READ;

-- Schema-specific grants
GRANT USAGE ON SCHEMA CLAUDE_BI.MCP TO ROLE R_APP_READ;
GRANT USAGE ON SCHEMA CLAUDE_BI.ACTIVITY TO ROLE R_APP_READ;
GRANT USAGE ON SCHEMA CLAUDE_BI.LANDING TO ROLE R_APP_WRITE;

-- View access for readers (current and future)
GRANT SELECT ON ALL VIEWS IN SCHEMA CLAUDE_BI.MCP TO ROLE R_APP_READ;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA CLAUDE_BI.MCP TO ROLE R_APP_READ;

GRANT SELECT ON ALL VIEWS IN SCHEMA CLAUDE_BI.ACTIVITY TO ROLE R_APP_READ;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA CLAUDE_BI.ACTIVITY TO ROLE R_APP_READ;

-- Dynamic table read access
GRANT SELECT ON DYNAMIC TABLE CLAUDE_BI.ACTIVITY.EVENTS TO ROLE R_APP_READ;

-- Sequence access for writers (for generating IDs)
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA CLAUDE_BI.ACTIVITY TO ROLE R_APP_WRITE;
GRANT USAGE, SELECT ON FUTURE SEQUENCES IN SCHEMA CLAUDE_BI.ACTIVITY TO ROLE R_APP_WRITE;

-- ============================================================================
-- SECTION 3: Warehouse Grants
-- ============================================================================

-- Create dedicated warehouses if they don't exist
CREATE WAREHOUSE IF NOT EXISTS CLAUDE_WAREHOUSE
  WITH WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  COMMENT = 'Primary warehouse for Claude Code operations';

CREATE WAREHOUSE IF NOT EXISTS CLAUDE_AGENT_WH
  WITH WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 30
  AUTO_RESUME = TRUE
  COMMENT = 'Dedicated warehouse for AI agents with resource monitoring';

-- Grant warehouse access
GRANT USAGE ON WAREHOUSE CLAUDE_WAREHOUSE TO ROLE R_APP_READ;
GRANT USAGE ON WAREHOUSE CLAUDE_WAREHOUSE TO ROLE R_APP_WRITE;
GRANT USAGE ON WAREHOUSE CLAUDE_AGENT_WH TO ROLE R_APP_READ;
GRANT USAGE ON WAREHOUSE CLAUDE_AGENT_WH TO ROLE R_APP_WRITE;

-- ============================================================================
-- SECTION 4: Security Policies
-- ============================================================================

USE ROLE SECURITYADMIN;

-- Password policy for human users
CREATE OR REPLACE PASSWORD POLICY PP_HUMANS
  PASSWORD_MIN_LENGTH = 14
  PASSWORD_MAX_LENGTH = 256
  PASSWORD_MIN_UPPERCASE_CHARS = 1
  PASSWORD_MIN_LOWERCASE_CHARS = 1
  PASSWORD_MIN_NUMERIC_CHARS = 1
  PASSWORD_MIN_SPECIAL_CHARS = 1
  PASSWORD_MAX_AGE_DAYS = 90
  PASSWORD_MAX_RETRIES = 5
  PASSWORD_LOCKOUT_TIME_MINS = 30
  PASSWORD_HISTORY = 5
  COMMENT = 'Strong password policy for human users';

-- Session policy for security
CREATE OR REPLACE SESSION POLICY SP_STANDARD
  SESSION_IDLE_TIMEOUT_MINS = 240
  SESSION_UI_IDLE_TIMEOUT_MINS = 30
  COMMENT = 'Standard session timeouts for security';

-- Network policy (update with your actual IPs)
-- For now, allowing all IPs - UPDATE THIS IN PRODUCTION
CREATE OR REPLACE NETWORK POLICY NP_ALLOWED
  ALLOWED_IP_LIST = ('0.0.0.0/0')
  COMMENT = 'Network access control - UPDATE WITH ACTUAL IPs';

-- ============================================================================
-- SECTION 5: Resource Monitors for Cost Control
-- ============================================================================

USE ROLE ACCOUNTADMIN;

-- Resource monitor for agent warehouse
CREATE OR REPLACE RESOURCE MONITOR RM_AGENT_DAILY
  WITH CREDIT_QUOTA = 10  -- 10 credits per day max
  FREQUENCY = DAILY
  START_TIMESTAMP = IMMEDIATELY
  TRIGGERS
    ON 75 PERCENT DO NOTIFY
    ON 90 PERCENT DO NOTIFY
    ON 100 PERCENT DO SUSPEND;

-- Apply monitor to agent warehouse
ALTER WAREHOUSE CLAUDE_AGENT_WH SET RESOURCE_MONITOR = RM_AGENT_DAILY;

-- ============================================================================
-- SECTION 6: Admin Schema for Provisioning
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS CLAUDE_BI.ADMIN
  COMMENT = 'Administrative schema for user provisioning and management';

GRANT USAGE ON SCHEMA CLAUDE_BI.ADMIN TO ROLE R_APP_ADMIN;

-- Create admin tracking table for key-pair deadlines
CREATE OR REPLACE TABLE CLAUDE_BI.ADMIN.ACTOR_REGISTRY (
  username STRING NOT NULL PRIMARY KEY,
  email STRING NOT NULL,
  actor_type STRING NOT NULL,
  auth_mode STRING NOT NULL,
  role_name STRING NOT NULL,
  provisioned_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  provisioned_by STRING DEFAULT CURRENT_USER(),
  key_deadline TIMESTAMP_NTZ,
  key_uploaded_at TIMESTAMP_NTZ,
  last_rotation TIMESTAMP_NTZ,
  next_rotation_due TIMESTAMP_NTZ,
  is_active BOOLEAN DEFAULT TRUE,
  metadata VARIANT
) COMMENT = 'Registry of all provisioned actors (humans and agents)';

-- Grant admin role access to registry
GRANT SELECT, INSERT, UPDATE ON TABLE CLAUDE_BI.ADMIN.ACTOR_REGISTRY TO ROLE R_APP_ADMIN;

-- ============================================================================
-- SECTION 7: Audit Configuration
-- ============================================================================

-- Enable query tagging for all roles
ALTER ROLE R_APP_READ SET QUERY_TAG = '{"app":"claude_code","role":"reader"}';
ALTER ROLE R_APP_WRITE SET QUERY_TAG = '{"app":"claude_code","role":"writer"}';
ALTER ROLE R_APP_ADMIN SET QUERY_TAG = '{"app":"claude_code","role":"admin"}';

-- ============================================================================
-- SECTION 8: Grants Summary
-- ============================================================================

-- Display grants for verification
SHOW GRANTS TO ROLE R_APP_READ;
SHOW GRANTS TO ROLE R_APP_WRITE;
SHOW GRANTS TO ROLE R_APP_ADMIN;

-- Success message
SELECT 'Security foundation created successfully!' AS status,
       'Next step: Run 02_provision_actor.sql' AS next_action;