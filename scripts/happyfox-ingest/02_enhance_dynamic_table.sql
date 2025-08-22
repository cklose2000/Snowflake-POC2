-- ============================================================================
-- Dynamic Table Enhancement for HappyFox Events
-- Purpose: Ensure ACTIVITY.EVENTS properly handles HappyFox ticket events
-- Maintains two-table architecture with no new tables
-- ============================================================================

USE DATABASE CLAUDE_BI;
USE WAREHOUSE CLAUDE_WAREHOUSE;

-- ----------------------------------------------------------------------------
-- VERIFY EXISTING DYNAMIC TABLE
-- ----------------------------------------------------------------------------

-- First, check current Dynamic Table definition
SHOW DYNAMIC TABLES LIKE 'EVENTS' IN SCHEMA ACTIVITY;

-- ----------------------------------------------------------------------------
-- RECREATE/ENHANCE DYNAMIC TABLE TO INCLUDE HAPPYFOX EVENTS
-- This assumes the existing DT needs to be enhanced. If it already handles
-- all events properly, this can be skipped.
-- ----------------------------------------------------------------------------

CREATE OR REPLACE DYNAMIC TABLE ACTIVITY.EVENTS
    TARGET_LAG = '1 MINUTE'
    WAREHOUSE = CLAUDE_WAREHOUSE
    AS
    SELECT
        -- Core event fields
        DATA:event_id::STRING AS event_id,
        DATA:action::STRING AS action,
        DATA:actor_id::STRING AS actor_id,
        DATA:source::STRING AS source,
        
        -- Object references
        DATA:object_type::STRING AS object_type,
        DATA:object_id::STRING AS object_id,
        DATA:display_id::STRING AS display_id,
        
        -- Attributes - stores the complete data
        DATA:attributes AS attributes,
        
        -- Timestamps
        DATA:occurred_at::TIMESTAMP_NTZ AS occurred_at,
        OCCURRED_AT AS ingested_at,
        
        -- Derived fields for common queries
        CASE 
            WHEN DATA:action = 'happyfox.ticket.upserted' THEN DATA:attributes:product_prefix::STRING
            ELSE NULL
        END AS product_prefix,
        
        CASE 
            WHEN DATA:action = 'happyfox.ticket.upserted' THEN DATA:attributes:status::STRING
            ELSE NULL
        END AS ticket_status,
        
        -- Metadata
        SOURCE AS raw_source,
        DATA:idempotency_key::STRING AS idempotency_key
        
    FROM LANDING.RAW_EVENTS
    WHERE 1=1  -- All events flow through
    ;

-- ----------------------------------------------------------------------------
-- ADD SEARCH OPTIMIZATION FOR COMMON QUERIES
-- ----------------------------------------------------------------------------

-- Add search optimization for HappyFox ticket lookups
ALTER TABLE ACTIVITY.EVENTS ADD SEARCH OPTIMIZATION
    ON EQUALITY(action, object_id, display_id, actor_id, source)
    ON SUBSTRING(attributes:subject::STRING);

-- ----------------------------------------------------------------------------
-- VERIFICATION QUERIES
-- ----------------------------------------------------------------------------

-- Check if HappyFox events are flowing through
SELECT 
    action,
    COUNT(*) as event_count,
    MIN(occurred_at) as earliest,
    MAX(occurred_at) as latest
FROM ACTIVITY.EVENTS
WHERE action LIKE 'happyfox%'
GROUP BY action
ORDER BY event_count DESC;

-- Sample a few HappyFox events to verify structure
SELECT 
    event_id,
    action,
    object_id,
    display_id,
    product_prefix,
    ticket_status,
    occurred_at,
    attributes:subject::STRING as subject
FROM ACTIVITY.EVENTS
WHERE action = 'happyfox.ticket.upserted'
LIMIT 10;

-- Verify no additional tables were created
SELECT 
    TABLE_SCHEMA,
    TABLE_NAME,
    TABLE_TYPE
FROM CLAUDE_BI.INFORMATION_SCHEMA.TABLES
WHERE TABLE_CATALOG = 'CLAUDE_BI'
  AND TABLE_SCHEMA IN ('LANDING', 'ACTIVITY')
  AND TABLE_TYPE IN ('BASE TABLE', 'DYNAMIC TABLE')
ORDER BY TABLE_SCHEMA, TABLE_NAME;
-- Should show exactly 2 tables: LANDING.RAW_EVENTS and ACTIVITY.EVENTS