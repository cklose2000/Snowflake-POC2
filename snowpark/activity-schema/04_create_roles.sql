-- ============================================================================
-- 04_create_roles.sql
-- Create MCP security roles with minimal necessary permissions
-- ============================================================================

USE ROLE ACCOUNTADMIN;

-- Drop existing roles if they exist (for clean setup)
DROP ROLE IF EXISTS MCP_ADMIN_ROLE;
DROP ROLE IF EXISTS MCP_USER_ROLE;
DROP ROLE IF EXISTS MCP_SERVICE_ROLE;

-- Create MCP roles
CREATE ROLE MCP_ADMIN_ROLE COMMENT = 'Admin role for managing MCP users and permissions';
CREATE ROLE MCP_USER_ROLE COMMENT = 'Basic user role - can only execute MCP procedures';
CREATE ROLE MCP_SERVICE_ROLE COMMENT = 'Service role for procedures with EXECUTE AS OWNER';

-- ============================================================================
-- MCP_USER_ROLE - Minimal permissions (can ONLY execute MCP procedures)
-- ============================================================================
GRANT USAGE ON DATABASE CLAUDE_BI TO ROLE MCP_USER_ROLE;
GRANT USAGE ON SCHEMA CLAUDE_BI.MCP TO ROLE MCP_USER_ROLE;
GRANT USAGE ON WAREHOUSE MCP_XS_WH TO ROLE MCP_USER_ROLE;
-- Procedure grants will be added after procedures are created

-- ============================================================================
-- MCP_SERVICE_ROLE - Elevated permissions (used by EXECUTE AS OWNER procedures)
-- ============================================================================
GRANT USAGE ON DATABASE CLAUDE_BI TO ROLE MCP_SERVICE_ROLE;
GRANT USAGE ON ALL SCHEMAS IN DATABASE CLAUDE_BI TO ROLE MCP_SERVICE_ROLE;

-- Grant specific table permissions
GRANT SELECT ON TABLE CLAUDE_BI.ACTIVITY.EVENTS TO ROLE MCP_SERVICE_ROLE;
GRANT INSERT ON TABLE CLAUDE_BI.LANDING.RAW_EVENTS TO ROLE MCP_SERVICE_ROLE;

-- Grant future object permissions
GRANT SELECT ON FUTURE TABLES IN SCHEMA CLAUDE_BI.ACTIVITY TO ROLE MCP_SERVICE_ROLE;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA CLAUDE_BI.ACTIVITY TO ROLE MCP_SERVICE_ROLE;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA CLAUDE_BI.MCP TO ROLE MCP_SERVICE_ROLE;

-- Grant warehouse usage
GRANT USAGE ON WAREHOUSE MCP_XS_WH TO ROLE MCP_SERVICE_ROLE;
GRANT USAGE ON WAREHOUSE DT_XS_WH TO ROLE MCP_SERVICE_ROLE;

-- ============================================================================
-- MCP_ADMIN_ROLE - Can manage users and grant permissions
-- ============================================================================
GRANT ROLE MCP_SERVICE_ROLE TO ROLE MCP_ADMIN_ROLE;
GRANT CREATE USER ON ACCOUNT TO ROLE MCP_ADMIN_ROLE;
GRANT CREATE ROLE ON ACCOUNT TO ROLE MCP_ADMIN_ROLE;
-- Will have access to admin procedures

-- ============================================================================
-- Grant roles to ACCOUNTADMIN for management
-- ============================================================================
GRANT ROLE MCP_ADMIN_ROLE TO ROLE ACCOUNTADMIN;
GRANT ROLE MCP_SERVICE_ROLE TO ROLE ACCOUNTADMIN;
GRANT ROLE MCP_USER_ROLE TO ROLE ACCOUNTADMIN;

-- Verify roles
SHOW ROLES LIKE 'MCP%';