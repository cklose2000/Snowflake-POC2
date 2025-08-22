-- ============================================================================
-- HappyFox Enhanced Analytics Views - Corrected for ACTIVITY_STREAM Structure
-- Purpose: Core views for self-serve drill-down and export  
-- Maintains two-table doctrine: reads ONLY from ACTIVITY_STREAM
-- ============================================================================

USE DATABASE CLAUDE_BI;
USE WAREHOUSE CLAUDE_WAREHOUSE;

-- ============================================================================
-- A) CANONICAL CURRENT STATE (latest version per ticket)
-- This is THE authoritative view for current ticket state
-- ============================================================================

CREATE OR REPLACE VIEW MCP.VW_HF_TICKETS_LATEST AS
WITH ranked AS (
  SELECT
    -- Core identifiers
    FEATURE_JSON:attributes:ticket_data:id::NUMBER AS ticket_id,
    FEATURE_JSON:attributes:ticket_data:display_id::STRING AS display_id,
    
    -- Ticket details
    FEATURE_JSON:attributes:ticket_data:subject::STRING AS subject,
    FEATURE_JSON:attributes:ticket_data:first_message::STRING AS first_message,
    
    -- Status and classification
    FEATURE_JSON:attributes:ticket_data:status:name::STRING AS status,
    FEATURE_JSON:attributes:ticket_data:priority:name::STRING AS priority,
    FEATURE_JSON:attributes:ticket_data:category:name::STRING AS category,
    FEATURE_JSON:attributes:product_prefix::STRING AS product_prefix,
    
    -- Assignment
    FEATURE_JSON:attributes:ticket_data:assigned_to:name::STRING AS assignee_name,
    FEATURE_JSON:attributes:ticket_data:assigned_to:email::STRING AS assignee_email,
    FEATURE_JSON:attributes:ticket_data:assigned_to:id::NUMBER AS assignee_id,
    
    -- Timestamps
    TRY_TO_TIMESTAMP_NTZ(FEATURE_JSON:attributes:ticket_data:created_at::STRING) AS created_at,
    TRY_TO_TIMESTAMP_NTZ(FEATURE_JSON:attributes:ticket_data:last_updated_at::STRING) AS last_updated_at,
    TRY_TO_TIMESTAMP_NTZ(FEATURE_JSON:attributes:ticket_data:last_modified::STRING) AS last_modified,
    TRY_TO_TIMESTAMP_NTZ(FEATURE_JSON:attributes:ticket_data:last_user_reply_at::STRING) AS last_user_reply_at,
    TRY_TO_TIMESTAMP_NTZ(FEATURE_JSON:attributes:ticket_data:last_staff_reply_at::STRING) AS last_staff_reply_at,
    
    -- Metrics
    FEATURE_JSON:attributes:ticket_data:messages_count::NUMBER AS messages_count,
    FEATURE_JSON:attributes:ticket_data:attachments_count::NUMBER AS attachments_count,
    FEATURE_JSON:attributes:ticket_data:time_spent::NUMBER AS time_spent_minutes,
    
    -- Source and channel
    FEATURE_JSON:attributes:ticket_data:source::STRING AS source_channel,
    
    -- Full ticket data for downstream analysis
    FEATURE_JSON:attributes:ticket_data AS ticket_json,
    
    -- Event metadata
    TS AS last_event_time,
    _RECV_AT AS last_ingested_at,
    
    -- Ranking for latest version
    ROW_NUMBER() OVER (
      PARTITION BY FEATURE_JSON:attributes:ticket_data:id::NUMBER
      ORDER BY TS DESC
    ) AS rn
  FROM ACTIVITY.ACTIVITY_STREAM
  WHERE ACTIVITY = 'happyfox.ticket.upserted'
)
SELECT *
FROM ranked
WHERE rn = 1;