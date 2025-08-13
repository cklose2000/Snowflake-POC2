-- Activity Schema 2.0 Base Infrastructure
-- Run this once to set up the complete schema

CREATE DATABASE IF NOT EXISTS CLAUDE_BI;
USE DATABASE CLAUDE_BI;

CREATE SCHEMA IF NOT EXISTS ANALYTICS;
USE SCHEMA ANALYTICS;

-- Activity Schema 2.0 Base Stream (STRICT COMPLIANCE)
CREATE TABLE IF NOT EXISTS analytics.activity.events (
  activity_id VARCHAR(255) NOT NULL,
  ts TIMESTAMP_NTZ NOT NULL,
  customer VARCHAR(255) NOT NULL, 
  activity VARCHAR(255) NOT NULL,
  feature_json VARIANT NOT NULL,
  
  anonymous_customer_id VARCHAR(255),
  revenue_impact FLOAT,
  link VARCHAR(255),
  
  -- Extensions (implementation-specific with _ prefix)
  _source_system VARCHAR(255) DEFAULT 'claude_code',
  _source_version VARCHAR(255),
  _session_id VARCHAR(255),
  _query_tag VARCHAR(255),
  _activity_occurrence INTEGER,
  _activity_repeated_at TIMESTAMP_NTZ,
  
  CONSTRAINT pk_activity_events PRIMARY KEY (activity_id)
);

-- Claude Code Extension Tables
CREATE SCHEMA IF NOT EXISTS activity_ccode;

CREATE TABLE IF NOT EXISTS analytics.activity_ccode.artifacts (
  artifact_id VARCHAR(255) NOT NULL,
  sample VARIANT,                    -- ≤10 rows preview
  row_count INTEGER,
  schema_json VARIANT,               -- Column metadata
  storage_type VARCHAR(50),          -- 'inline', 'stage', 'table'
  storage_location VARCHAR(500),     -- Stage path or table name
  bytes BIGINT,
  compressed_bytes BIGINT,           -- Snowflake compression size
  created_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  customer VARCHAR(255),
  created_by_activity VARCHAR(255),  -- References activity.events.activity_id
  
  CONSTRAINT pk_artifacts PRIMARY KEY (artifact_id)
);

-- Large result storage table (for results > 10 rows)
CREATE TABLE IF NOT EXISTS analytics.activity_ccode.artifact_data (
  artifact_id VARCHAR(255) NOT NULL,
  row_number INTEGER NOT NULL,
  row_data VARIANT NOT NULL,         -- Full row as JSON
  
  CONSTRAINT pk_artifact_data PRIMARY KEY (artifact_id, row_number)
);

-- Create internal stage for very large files
CREATE OR REPLACE STAGE analytics.activity_ccode.artifact_stage
  COMMENT = 'Internal stage for large query results and files';

CREATE TABLE IF NOT EXISTS analytics.activity_ccode.audit_results (
  audit_id VARCHAR(255) NOT NULL,
  activity_id VARCHAR(255),          -- What was audited
  passed BOOLEAN,
  findings VARIANT,                  -- JSON array of issues found
  remediation TEXT,                  -- Suggested fixes
  audit_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  customer VARCHAR(255),
  
  CONSTRAINT pk_audit_results PRIMARY KEY (audit_id)
);

-- Performance optimization
ALTER TABLE analytics.activity.events 
  CLUSTER BY (customer, DATE_TRUNC('day', ts));

-- Resource Monitor
CREATE OR REPLACE RESOURCE MONITOR claude_bi_monitor 
  WITH credit_quota = 100
  TRIGGERS 
    ON 75 PERCENT DO NOTIFY
    ON 90 PERCENT DO SUSPEND_IMMEDIATE;

ALTER WAREHOUSE CLAUDE_WAREHOUSE SET RESOURCE_MONITOR = claude_bi_monitor;

-- Sample data for testing
INSERT INTO analytics.activity.events (
  activity_id, ts, customer, activity, feature_json
) VALUES (
  'test_001', 
  CURRENT_TIMESTAMP, 
  'system_user', 
  'ccode.system_initialized',
  '{"version": "1.0.0", "setup_complete": true}'
);