-- ============================================================================
-- 14_ddl_security.sql
-- Privilege-based enforcement: Agents can ONLY use SAFE_DDL
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE CLAUDE_BI;
USE SCHEMA MCP;

-- ============================================================================
-- Create DDL Owner Role (has actual DDL privileges)
-- ============================================================================
CREATE ROLE IF NOT EXISTS DDL_OWNER_ROLE
  COMMENT = 'Role with actual DDL privileges - used by SAFE_DDL procedure only';

-- Grant DDL privileges to owner role
GRANT CREATE PROCEDURE ON SCHEMA MCP TO ROLE DDL_OWNER_ROLE;
GRANT CREATE FUNCTION ON SCHEMA MCP TO ROLE DDL_OWNER_ROLE;
GRANT CREATE VIEW ON SCHEMA MCP TO ROLE DDL_OWNER_ROLE;
GRANT CREATE TABLE ON SCHEMA MCP TO ROLE DDL_OWNER_ROLE;
GRANT ALL PRIVILEGES ON SCHEMA MCP TO ROLE DDL_OWNER_ROLE;
GRANT USAGE ON WAREHOUSE CLAUDE_AGENT_WH TO ROLE DDL_OWNER_ROLE;

-- Grant owner role to ACCOUNTADMIN for management
GRANT ROLE DDL_OWNER_ROLE TO ROLE ACCOUNTADMIN;

-- ============================================================================
-- Create Agent Role (NO DDL privileges)
-- ============================================================================
CREATE ROLE IF NOT EXISTS MCP_AGENT_ROLE
  COMMENT = 'Role for AI agents - can only execute SAFE_DDL, no direct DDL';

-- Basic permissions for agents
GRANT USAGE ON DATABASE CLAUDE_BI TO ROLE MCP_AGENT_ROLE;
GRANT USAGE ON SCHEMA MCP TO ROLE MCP_AGENT_ROLE;
GRANT USAGE ON WAREHOUSE CLAUDE_AGENT_WH TO ROLE MCP_AGENT_ROLE;

-- Agents can SELECT from tables/views but NOT create/alter/drop
GRANT SELECT ON ALL TABLES IN SCHEMA MCP TO ROLE MCP_AGENT_ROLE;
GRANT SELECT ON ALL VIEWS IN SCHEMA MCP TO ROLE MCP_AGENT_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA MCP TO ROLE MCP_AGENT_ROLE;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA MCP TO ROLE MCP_AGENT_ROLE;

-- Agents can EXECUTE procedures but NOT create/alter/drop them
GRANT USAGE ON ALL PROCEDURES IN SCHEMA MCP TO ROLE MCP_AGENT_ROLE;
GRANT USAGE ON ALL FUNCTIONS IN SCHEMA MCP TO ROLE MCP_AGENT_ROLE;
GRANT USAGE ON FUTURE PROCEDURES IN SCHEMA MCP TO ROLE MCP_AGENT_ROLE;
GRANT USAGE ON FUTURE FUNCTIONS IN SCHEMA MCP TO ROLE MCP_AGENT_ROLE;

-- ============================================================================
-- CRITICAL: Revoke ALL DDL privileges from agent roles
-- ============================================================================
-- Explicitly revoke any DDL privileges that might have been granted
REVOKE CREATE PROCEDURE ON SCHEMA MCP FROM ROLE MCP_AGENT_ROLE;
REVOKE CREATE FUNCTION ON SCHEMA MCP FROM ROLE MCP_AGENT_ROLE;
REVOKE CREATE VIEW ON SCHEMA MCP FROM ROLE MCP_AGENT_ROLE;
REVOKE CREATE TABLE ON SCHEMA MCP FROM ROLE MCP_AGENT_ROLE;
REVOKE CREATE STAGE ON SCHEMA MCP FROM ROLE MCP_AGENT_ROLE;
REVOKE CREATE SEQUENCE ON SCHEMA MCP FROM ROLE MCP_AGENT_ROLE;
REVOKE CREATE PIPE ON SCHEMA MCP FROM ROLE MCP_AGENT_ROLE;
REVOKE CREATE STREAM ON SCHEMA MCP FROM ROLE MCP_AGENT_ROLE;
REVOKE CREATE TASK ON SCHEMA MCP FROM ROLE MCP_AGENT_ROLE;

-- Also revoke from MCP_USER_ROLE if it exists
REVOKE CREATE PROCEDURE ON SCHEMA MCP FROM ROLE MCP_USER_ROLE;
REVOKE CREATE FUNCTION ON SCHEMA MCP FROM ROLE MCP_USER_ROLE;
REVOKE CREATE VIEW ON SCHEMA MCP FROM ROLE MCP_USER_ROLE;
REVOKE CREATE TABLE ON SCHEMA MCP FROM ROLE MCP_USER_ROLE;
REVOKE CREATE STAGE ON SCHEMA MCP FROM ROLE MCP_USER_ROLE;
REVOKE CREATE SEQUENCE ON SCHEMA MCP FROM ROLE MCP_USER_ROLE;
REVOKE CREATE PIPE ON SCHEMA MCP FROM ROLE MCP_USER_ROLE;
REVOKE CREATE STREAM ON SCHEMA MCP FROM ROLE MCP_USER_ROLE;
REVOKE CREATE TASK ON SCHEMA MCP FROM ROLE MCP_USER_ROLE;

-- ============================================================================
-- Grant ONLY SAFE_DDL execution to agents
-- ============================================================================
-- This is the ONLY DDL-related permission agents have
GRANT USAGE ON PROCEDURE MCP.SAFE_DDL(STRING, STRING) TO ROLE MCP_AGENT_ROLE;
GRANT USAGE ON PROCEDURE MCP.SAFE_DDL(STRING, STRING) TO ROLE MCP_USER_ROLE;

-- Agents can view the DDL catalog but not modify it
GRANT SELECT ON VIEW MCP.VW_DDL_CATALOG TO ROLE MCP_AGENT_ROLE;
GRANT SELECT ON VIEW MCP.VW_DDL_HISTORY TO ROLE MCP_AGENT_ROLE;
GRANT SELECT ON VIEW MCP.VW_DDL_DRIFT TO ROLE MCP_AGENT_ROLE;
GRANT SELECT ON VIEW MCP.VW_DDL_TEST_COVERAGE TO ROLE MCP_AGENT_ROLE;

-- Grant agent role to users who need it
-- Example: GRANT ROLE MCP_AGENT_ROLE TO USER CLAUDE_CODE_AI_AGENT;

-- ============================================================================
-- Create Security Monitoring View
-- ============================================================================
CREATE OR REPLACE VIEW VW_DDL_SECURITY_STATUS AS
WITH role_privs AS (
  -- Check what DDL privileges each role has
  SELECT 
    GRANTEE_NAME as role_name,
    PRIVILEGE,
    TABLE_SCHEMA as schema_name,
    GRANTED_ON
  FROM INFORMATION_SCHEMA.OBJECT_PRIVILEGES
  WHERE TABLE_SCHEMA = 'MCP'
    AND PRIVILEGE IN ('CREATE', 'OWNERSHIP', 'ALTER', 'DROP')
    AND GRANTEE_NAME IN ('MCP_AGENT_ROLE', 'MCP_USER_ROLE', 'DDL_OWNER_ROLE')
),
safe_ddl_access AS (
  -- Check who can execute SAFE_DDL
  SELECT 
    GRANTEE_NAME as role_name,
    'EXECUTE SAFE_DDL' as privilege_type
  FROM INFORMATION_SCHEMA.OBJECT_PRIVILEGES
  WHERE TABLE_NAME = 'SAFE_DDL'
    AND TABLE_SCHEMA = 'MCP'
    AND PRIVILEGE = 'USAGE'
)
SELECT 
  role_name,
  CASE 
    WHEN role_name = 'DDL_OWNER_ROLE' THEN 'OWNER'
    WHEN role_name IN ('MCP_AGENT_ROLE', 'MCP_USER_ROLE') THEN 'AGENT'
    ELSE 'OTHER'
  END as role_type,
  COUNT(DISTINCT PRIVILEGE) as ddl_privilege_count,
  ARRAY_AGG(DISTINCT PRIVILEGE) as ddl_privileges,
  MAX(CASE WHEN privilege_type = 'EXECUTE SAFE_DDL' THEN 1 ELSE 0 END) as can_use_safe_ddl,
  CASE 
    WHEN role_name = 'DDL_OWNER_ROLE' AND COUNT(DISTINCT PRIVILEGE) > 0 THEN 'CORRECT'
    WHEN role_name IN ('MCP_AGENT_ROLE', 'MCP_USER_ROLE') AND COUNT(DISTINCT PRIVILEGE) = 0 THEN 'CORRECT'
    ELSE 'VIOLATION'
  END as security_status
FROM (
  SELECT role_name, PRIVILEGE, NULL as privilege_type FROM role_privs
  UNION ALL
  SELECT role_name, NULL as PRIVILEGE, privilege_type FROM safe_ddl_access
)
GROUP BY role_name
ORDER BY role_type, role_name;

-- Grant view access
GRANT SELECT ON VIEW VW_DDL_SECURITY_STATUS TO ROLE MCP_USER_ROLE;
GRANT SELECT ON VIEW VW_DDL_SECURITY_STATUS TO ROLE MCP_AGENT_ROLE;

-- ============================================================================
-- Security Validation Query
-- ============================================================================
-- Run this to verify security is properly configured:
/*
SELECT 
  role_name,
  role_type,
  ddl_privilege_count,
  can_use_safe_ddl,
  security_status,
  CASE 
    WHEN security_status = 'CORRECT' THEN '✓ Secure'
    ELSE '✗ SECURITY VIOLATION - ' || role_name || ' has DDL privileges!'
  END as status_message
FROM VW_DDL_SECURITY_STATUS
ORDER BY security_status DESC, role_name;

-- Expected output:
-- DDL_OWNER_ROLE: Has DDL privileges, status CORRECT
-- MCP_AGENT_ROLE: No DDL privileges, can use SAFE_DDL, status CORRECT  
-- MCP_USER_ROLE: No DDL privileges, can use SAFE_DDL, status CORRECT
*/

-- ============================================================================
-- Create Agent Users (if needed)
-- ============================================================================
-- Example of creating an agent user with proper restrictions:
/*
CREATE USER IF NOT EXISTS CLAUDE_AGENT_001
  PASSWORD = 'ComplexPassword123!'
  DEFAULT_ROLE = MCP_AGENT_ROLE
  DEFAULT_WAREHOUSE = CLAUDE_AGENT_WH
  COMMENT = 'AI Agent with DDL access only through SAFE_DDL';

GRANT ROLE MCP_AGENT_ROLE TO USER CLAUDE_AGENT_001;

-- Set query tag for attribution
ALTER USER CLAUDE_AGENT_001 SET DEFAULT_QUERY_TAG = 'agent:claude_001,type:ai_developer';
*/