-- Snowflake Schema Bootstrap Script
-- Idempotent - safe to run multiple times
-- Creates all required database objects for SnowflakePOC2

-- Note: Replace ${SNOWFLAKE_DATABASE} with your actual database name
-- e.g., CLAUDE_BI for production

-- =============================================================================
-- STEP 1: Create Database (if not exists)
-- =============================================================================
CREATE DATABASE IF NOT EXISTS CLAUDE_BI;
USE DATABASE CLAUDE_BI;

-- =============================================================================
-- STEP 2: Create Schemas
-- =============================================================================

-- Activity Schema - for Activity Schema v2.0 events
CREATE SCHEMA IF NOT EXISTS ACTIVITY
  COMMENT = 'Activity Schema v2.0 - Event stream for all system activities';

-- Activity CCODE Schema - for Claude Code specific tables  
CREATE SCHEMA IF NOT EXISTS ACTIVITY_CCODE
  COMMENT = 'Claude Code specific activity tables - artifacts and audit results';

-- Analytics Schema - default schema for analytics objects
CREATE SCHEMA IF NOT EXISTS ANALYTICS
  COMMENT = 'Default schema for analytics views, tasks, and reports';

-- =============================================================================
-- STEP 3: Create Activity Schema v2.0 Table
-- =============================================================================

USE SCHEMA ACTIVITY;

CREATE TABLE IF NOT EXISTS EVENTS (
  -- Core Activity Schema v2.0 columns (required)
  activity_id VARCHAR(255) NOT NULL,
  ts TIMESTAMP_NTZ NOT NULL,
  customer VARCHAR(255) NOT NULL,
  activity VARCHAR(255) NOT NULL,
  feature_json VARIANT NOT NULL,
  
  -- Optional standard columns
  anonymous_customer_id VARCHAR(255),
  revenue_impact FLOAT,
  link VARCHAR(255),
  
  -- System extension columns (underscore prefixed per v2 spec)
  _source_system VARCHAR(255) DEFAULT 'claude_code',
  _source_version VARCHAR(255),
  _session_id VARCHAR(255),
  _query_tag VARCHAR(255),
  _activity_occurrence INTEGER,
  _activity_repeated_at TIMESTAMP_NTZ,
  
  -- Constraints
  PRIMARY KEY (activity_id),
  
  -- Indexes for common queries
  INDEX idx_customer_ts (customer, ts),
  INDEX idx_activity_ts (activity, ts),
  INDEX idx_ts (ts)
)
COMMENT = 'Activity Schema v2.0 compliant event stream'
CLUSTER BY (customer, DATE_TRUNC('day', ts));

-- =============================================================================
-- STEP 4: Create Activity CCODE Tables
-- =============================================================================

USE SCHEMA ACTIVITY_CCODE;

-- Artifacts table - stores generated artifacts and metadata
CREATE TABLE IF NOT EXISTS ARTIFACTS (
  artifact_id VARCHAR(255) NOT NULL,
  sample VARIANT,
  row_count INTEGER,
  schema_json VARIANT,
  s3_url VARCHAR(500),
  bytes BIGINT,
  created_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  customer VARCHAR(255),
  created_by_activity VARCHAR(255),
  
  PRIMARY KEY (artifact_id),
  FOREIGN KEY (created_by_activity) REFERENCES ACTIVITY.EVENTS(activity_id)
)
COMMENT = 'Storage for Claude Code generated artifacts and results';

-- Audit results table - stores audit outcomes
CREATE TABLE IF NOT EXISTS AUDIT_RESULTS (
  audit_id VARCHAR(255) NOT NULL,
  ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  passed BOOLEAN NOT NULL,
  details VARIANT,
  activity_id VARCHAR(255),
  customer VARCHAR(255),
  audit_type VARCHAR(100),
  
  PRIMARY KEY (audit_id),
  FOREIGN KEY (activity_id) REFERENCES ACTIVITY.EVENTS(activity_id)
)
COMMENT = 'Audit results for Claude Code operations';

-- =============================================================================
-- STEP 5: Create Analytics Schema Objects
-- =============================================================================

USE SCHEMA ANALYTICS;

-- Schema version tracking table
CREATE TABLE IF NOT EXISTS SCHEMA_VERSION (
  version INTEGER NOT NULL,
  applied_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  applied_by VARCHAR(255) DEFAULT CURRENT_USER(),
  description VARCHAR(1000),
  
  PRIMARY KEY (version)
)
COMMENT = 'Tracks schema version and migrations';

-- Insert initial version
INSERT INTO SCHEMA_VERSION (version, description)
SELECT 1, 'Initial Activity Schema v2.0 bootstrap'
WHERE NOT EXISTS (
  SELECT 1 FROM SCHEMA_VERSION WHERE version = 1
);

-- =============================================================================
-- STEP 6: Create Required Roles and Grants (if running as ACCOUNTADMIN)
-- =============================================================================

-- Note: Uncomment these if you have ACCOUNTADMIN privileges
-- and want to set up the complete security model

/*
-- Create role for Claude BI operations
CREATE ROLE IF NOT EXISTS CLAUDE_BI_ROLE
  COMMENT = 'Role for Claude Code BI operations';

-- Grant database privileges
GRANT USAGE ON DATABASE CLAUDE_BI TO ROLE CLAUDE_BI_ROLE;
GRANT USAGE ON ALL SCHEMAS IN DATABASE CLAUDE_BI TO ROLE CLAUDE_BI_ROLE;

-- Grant table privileges for Activity schema
GRANT SELECT, INSERT ON TABLE ACTIVITY.EVENTS TO ROLE CLAUDE_BI_ROLE;

-- Grant table privileges for Activity CCODE schema
GRANT SELECT, INSERT, UPDATE ON TABLE ACTIVITY_CCODE.ARTIFACTS TO ROLE CLAUDE_BI_ROLE;
GRANT SELECT, INSERT, UPDATE ON TABLE ACTIVITY_CCODE.AUDIT_RESULTS TO ROLE CLAUDE_BI_ROLE;

-- Grant create privileges for Analytics schema
GRANT CREATE VIEW ON SCHEMA ANALYTICS TO ROLE CLAUDE_BI_ROLE;
GRANT CREATE TABLE ON SCHEMA ANALYTICS TO ROLE CLAUDE_BI_ROLE;
GRANT CREATE TASK ON SCHEMA ANALYTICS TO ROLE CLAUDE_BI_ROLE;
GRANT CREATE STREAMLIT ON SCHEMA ANALYTICS TO ROLE CLAUDE_BI_ROLE;

-- Grant warehouse usage
GRANT USAGE ON WAREHOUSE CLAUDE_WAREHOUSE TO ROLE CLAUDE_BI_ROLE;

-- Create dedicated dashboard warehouse
CREATE WAREHOUSE IF NOT EXISTS DASHBOARD_WH
  WITH 
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 2
  COMMENT = 'Dedicated warehouse for dashboard operations';

GRANT USAGE ON WAREHOUSE DASHBOARD_WH TO ROLE CLAUDE_BI_ROLE;

-- Grant role to user
GRANT ROLE CLAUDE_BI_ROLE TO USER claude_bi_user;
*/

-- =============================================================================
-- STEP 7: Validation Queries
-- =============================================================================

-- Run these to verify the bootstrap was successful:

-- Check schemas exist
SHOW SCHEMAS IN DATABASE CLAUDE_BI;

-- Check tables exist
SHOW TABLES IN SCHEMA ACTIVITY;
SHOW TABLES IN SCHEMA ACTIVITY_CCODE;
SHOW TABLES IN SCHEMA ANALYTICS;

-- Verify Activity Schema v2 structure
DESCRIBE TABLE ACTIVITY.EVENTS;

-- Check current version
SELECT * FROM ANALYTICS.SCHEMA_VERSION ORDER BY version DESC LIMIT 1;

-- =============================================================================
-- SUCCESS MESSAGE
-- =============================================================================
SELECT 'Bootstrap complete! All required schemas and tables are ready.' as STATUS;