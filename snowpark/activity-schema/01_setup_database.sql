-- ============================================================================
-- 01_setup_database.sql
-- Create database, schemas, and warehouses for Activity Schema 2.0
-- ============================================================================

USE ROLE ACCOUNTADMIN;

-- Create database
CREATE DATABASE IF NOT EXISTS CLAUDE_BI;

-- Create schemas (Snowflake doesn't nest schemas - keep them flat)
CREATE SCHEMA IF NOT EXISTS CLAUDE_BI.LANDING;
CREATE SCHEMA IF NOT EXISTS CLAUDE_BI.ACTIVITY;
CREATE SCHEMA IF NOT EXISTS CLAUDE_BI.MCP;

-- Create warehouse for Dynamic Table refresh (dedicated to avoid contention)
CREATE WAREHOUSE IF NOT EXISTS DT_XS_WH
  WAREHOUSE_SIZE = 'X-SMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  COMMENT = 'Dedicated warehouse for Dynamic Table refresh';

-- Create warehouse for MCP operations (longer auto-suspend for interactive use)
CREATE WAREHOUSE IF NOT EXISTS MCP_XS_WH
  WAREHOUSE_SIZE = 'X-SMALL'
  AUTO_SUSPEND = 600
  AUTO_RESUME = TRUE
  COMMENT = 'Warehouse for MCP query execution';

-- Confirm setup
SHOW SCHEMAS IN DATABASE CLAUDE_BI;
SHOW WAREHOUSES LIKE '%_XS_WH';