-- ============================================================================
-- 02_create_raw_events.sql
-- The ONLY data ingestion table - all data enters here as events
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE CLAUDE_BI;
USE SCHEMA LANDING;

-- Drop if exists for clean setup
DROP TABLE IF EXISTS CLAUDE_BI.LANDING.RAW_EVENTS;

-- Create the landing table (write target for ALL ingestion)
CREATE TABLE CLAUDE_BI.LANDING.RAW_EVENTS (
  payload VARIANT NOT NULL,
  _source_lane STRING DEFAULT 'DIRECT',
  _recv_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (TO_DATE(_recv_at))
COMMENT = 'Single landing zone for all events - business, permissions, audit';

-- Enable change tracking for Dynamic Table to consume
ALTER TABLE CLAUDE_BI.LANDING.RAW_EVENTS SET CHANGE_TRACKING = TRUE;

-- Verify table structure
DESCRIBE TABLE CLAUDE_BI.LANDING.RAW_EVENTS;