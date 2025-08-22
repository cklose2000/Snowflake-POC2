-- ============================================================================
-- HappyFox Analytics Views
-- Purpose: Create views for analyzing HappyFox data from ACTIVITY.EVENTS
-- All analytics via views - no new tables
-- ============================================================================

USE DATABASE CLAUDE_BI;
USE WAREHOUSE CLAUDE_WAREHOUSE;
USE SCHEMA MCP;

-- ----------------------------------------------------------------------------
-- VIEW 1: Current Ticket State (Latest version of each ticket)
-- ----------------------------------------------------------------------------

CREATE OR REPLACE VIEW MCP.VW_HF_TICKETS AS
WITH latest_tickets AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY object_id 
            ORDER BY occurred_at DESC
        ) as rn
    FROM ACTIVITY.EVENTS
    WHERE action = 'happyfox.ticket.upserted'
)
SELECT
    -- Core identifiers
    object_id::NUMBER AS ticket_id,
    display_id AS display_id,
    
    -- Ticket details from nested JSON
    attributes:ticket_data:subject::STRING AS subject,
    attributes:ticket_data:first_message::STRING AS first_message,
    
    -- Status and classification
    attributes:status::STRING AS status,
    attributes:priority::STRING AS priority,
    attributes:category::STRING AS category,
    attributes:product_prefix::STRING AS product_prefix,
    
    -- Assignment
    attributes:assignee::STRING AS assignee_name,
    attributes:ticket_data:assigned_to:email::STRING AS assignee_email,
    attributes:ticket_data:assigned_to:id::NUMBER AS assignee_id,
    
    -- Timestamps
    TRY_TO_TIMESTAMP_NTZ(attributes:ticket_data:created_at::STRING) AS created_at,
    TRY_TO_TIMESTAMP_NTZ(attributes:ticket_data:last_updated_at::STRING) AS last_updated_at,
    TRY_TO_TIMESTAMP_NTZ(attributes:ticket_data:last_modified::STRING) AS last_modified,
    TRY_TO_TIMESTAMP_NTZ(attributes:ticket_data:last_user_reply_at::STRING) AS last_user_reply_at,
    TRY_TO_TIMESTAMP_NTZ(attributes:ticket_data:last_staff_reply_at::STRING) AS last_staff_reply_at,
    
    -- Metrics
    attributes:ticket_data:messages_count::NUMBER AS messages_count,
    attributes:ticket_data:attachments_count::NUMBER AS attachments_count,
    attributes:ticket_data:time_spent::NUMBER AS time_spent_minutes,
    
    -- Source and channel
    attributes:ticket_data:source::STRING AS source_channel,
    
    -- Full ticket data for additional analysis
    attributes:ticket_data AS full_ticket_json,
    
    -- Metadata
    occurred_at AS last_event_time,
    ingested_at AS last_ingested_at
    
FROM latest_tickets
WHERE rn = 1;

-- ----------------------------------------------------------------------------
-- VIEW 2: Ticket Aging Analysis
-- ----------------------------------------------------------------------------

CREATE OR REPLACE VIEW MCP.VW_HF_TICKET_AGING AS
SELECT
    ticket_id,
    display_id,
    subject,
    status,
    priority,
    category,
    product_prefix,
    assignee_name,
    created_at,
    last_updated_at,
    
    -- Age calculations
    DATEDIFF('day', created_at, CURRENT_TIMESTAMP()) AS age_days,
    DATEDIFF('hour', created_at, CURRENT_TIMESTAMP()) AS age_hours,
    DATEDIFF('day', last_updated_at, CURRENT_TIMESTAMP()) AS days_since_update,
    
    -- Age buckets
    CASE 
        WHEN DATEDIFF('day', created_at, CURRENT_TIMESTAMP()) = 0 THEN '0-1 days'
        WHEN DATEDIFF('day', created_at, CURRENT_TIMESTAMP()) <= 3 THEN '2-3 days'
        WHEN DATEDIFF('day', created_at, CURRENT_TIMESTAMP()) <= 7 THEN '4-7 days'
        WHEN DATEDIFF('day', created_at, CURRENT_TIMESTAMP()) <= 14 THEN '8-14 days'
        WHEN DATEDIFF('day', created_at, CURRENT_TIMESTAMP()) <= 30 THEN '15-30 days'
        WHEN DATEDIFF('day', created_at, CURRENT_TIMESTAMP()) <= 60 THEN '31-60 days'
        WHEN DATEDIFF('day', created_at, CURRENT_TIMESTAMP()) <= 90 THEN '61-90 days'
        WHEN DATEDIFF('day', created_at, CURRENT_TIMESTAMP()) <= 180 THEN '91-180 days'
        ELSE '180+ days'
    END AS age_bucket,
    
    -- Lifecycle state
    CASE 
        WHEN status IN ('Closed', 'Resolved', 'Completed') THEN 'Closed'
        WHEN status IN ('New', 'Open', 'In Progress', 'Pending', 'On Hold') THEN 'Open'
        ELSE 'Unknown'
    END AS lifecycle_state,
    
    -- Response SLA
    CASE 
        WHEN last_staff_reply_at IS NULL THEN 'No Response'
        WHEN DATEDIFF('hour', created_at, last_staff_reply_at) <= 4 THEN 'Within 4 hours'
        WHEN DATEDIFF('hour', created_at, last_staff_reply_at) <= 24 THEN 'Within 24 hours'
        WHEN DATEDIFF('day', created_at, last_staff_reply_at) <= 3 THEN 'Within 3 days'
        ELSE 'Over 3 days'
    END AS first_response_sla
    
FROM MCP.VW_HF_TICKETS
WHERE created_at IS NOT NULL;

-- ----------------------------------------------------------------------------
-- VIEW 3: Custom Fields (EAV Pattern)
-- ----------------------------------------------------------------------------

CREATE OR REPLACE VIEW MCP.VW_HF_CUSTOM_FIELDS AS
WITH latest_tickets AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY object_id 
            ORDER BY occurred_at DESC
        ) as rn
    FROM ACTIVITY.EVENTS
    WHERE action = 'happyfox.ticket.upserted'
)
SELECT
    object_id::NUMBER AS ticket_id,
    display_id,
    f.value:id::NUMBER AS field_id,
    f.value:name::STRING AS field_name,
    f.value:type::STRING AS field_type,
    f.value:value::STRING AS field_value,
    f.value:value_id AS field_value_id,
    f.value:compulsory_on_complete::BOOLEAN AS is_required,
    f.value:visible_to_staff_only::BOOLEAN AS staff_only,
    occurred_at AS as_of_time
FROM latest_tickets,
LATERAL FLATTEN(INPUT => attributes:ticket_data:custom_fields) f
WHERE rn = 1
  AND f.value:value IS NOT NULL;

-- ----------------------------------------------------------------------------
-- VIEW 4: Ticket Tags (Many-to-Many)
-- ----------------------------------------------------------------------------

CREATE OR REPLACE VIEW MCP.VW_HF_TICKET_TAGS AS
WITH latest_tickets AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY object_id 
            ORDER BY occurred_at DESC
        ) as rn
    FROM ACTIVITY.EVENTS
    WHERE action = 'happyfox.ticket.upserted'
)
SELECT
    object_id::NUMBER AS ticket_id,
    display_id,
    t.value::STRING AS tag,
    occurred_at AS as_of_time
FROM latest_tickets,
LATERAL FLATTEN(INPUT => attributes:ticket_data:tags, OUTER => TRUE) t
WHERE rn = 1
  AND t.value IS NOT NULL;

-- ----------------------------------------------------------------------------
-- VIEW 5: Ticket History (All versions)
-- ----------------------------------------------------------------------------

CREATE OR REPLACE VIEW MCP.VW_HF_TICKET_HISTORY AS
SELECT
    object_id::NUMBER AS ticket_id,
    display_id,
    occurred_at AS version_time,
    LAG(occurred_at) OVER (PARTITION BY object_id ORDER BY occurred_at) AS previous_version_time,
    
    -- What changed
    attributes:status::STRING AS status,
    LAG(attributes:status::STRING) OVER (PARTITION BY object_id ORDER BY occurred_at) AS previous_status,
    
    attributes:assignee::STRING AS assignee,
    LAG(attributes:assignee::STRING) OVER (PARTITION BY object_id ORDER BY occurred_at) AS previous_assignee,
    
    attributes:priority::STRING AS priority,
    LAG(attributes:priority::STRING) OVER (PARTITION BY object_id ORDER BY occurred_at) AS previous_priority,
    
    -- Change detection
    CASE 
        WHEN LAG(attributes:status::STRING) OVER (PARTITION BY object_id ORDER BY occurred_at) IS NULL THEN 'Created'
        WHEN attributes:status::STRING != LAG(attributes:status::STRING) OVER (PARTITION BY object_id ORDER BY occurred_at) THEN 'Status Changed'
        WHEN attributes:assignee::STRING != LAG(attributes:assignee::STRING) OVER (PARTITION BY object_id ORDER BY occurred_at) THEN 'Reassigned'
        WHEN attributes:priority::STRING != LAG(attributes:priority::STRING) OVER (PARTITION BY object_id ORDER BY occurred_at) THEN 'Priority Changed'
        ELSE 'Updated'
    END AS change_type,
    
    -- Version metadata
    ROW_NUMBER() OVER (PARTITION BY object_id ORDER BY occurred_at) AS version_number,
    event_id,
    ingested_at
    
FROM ACTIVITY.EVENTS
WHERE action = 'happyfox.ticket.upserted'
ORDER BY ticket_id, version_time;

-- ----------------------------------------------------------------------------
-- VIEW 6: Product Analytics Summary
-- ----------------------------------------------------------------------------

CREATE OR REPLACE VIEW MCP.VW_HF_PRODUCT_SUMMARY AS
SELECT
    COALESCE(product_prefix, 'UNTAGGED') AS product,
    lifecycle_state,
    COUNT(*) AS ticket_count,
    
    -- Age distribution
    AVG(age_days) AS avg_age_days,
    MEDIAN(age_days) AS median_age_days,
    MAX(age_days) AS max_age_days,
    
    -- Status breakdown
    SUM(CASE WHEN status = 'New' THEN 1 ELSE 0 END) AS new_count,
    SUM(CASE WHEN status = 'In Progress' THEN 1 ELSE 0 END) AS in_progress_count,
    SUM(CASE WHEN status = 'On Hold' THEN 1 ELSE 0 END) AS on_hold_count,
    SUM(CASE WHEN status IN ('Closed', 'Resolved') THEN 1 ELSE 0 END) AS closed_count,
    
    -- Priority breakdown
    SUM(CASE WHEN priority = 'Urgent' THEN 1 ELSE 0 END) AS urgent_count,
    SUM(CASE WHEN priority = 'High' THEN 1 ELSE 0 END) AS high_count,
    SUM(CASE WHEN priority = 'Medium' THEN 1 ELSE 0 END) AS medium_count,
    SUM(CASE WHEN priority = 'Low' THEN 1 ELSE 0 END) AS low_count,
    
    -- Activity metrics
    AVG(messages_count) AS avg_messages,
    SUM(time_spent_minutes) AS total_time_spent_minutes
    
FROM MCP.VW_HF_TICKET_AGING
GROUP BY product, lifecycle_state
ORDER BY product, lifecycle_state;

-- ----------------------------------------------------------------------------
-- VIEW 7: SLA Breaches
-- ----------------------------------------------------------------------------

CREATE OR REPLACE VIEW MCP.VW_HF_SLA_BREACHES AS
WITH latest_tickets AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY object_id 
            ORDER BY occurred_at DESC
        ) as rn
    FROM ACTIVITY.EVENTS
    WHERE action = 'happyfox.ticket.upserted'
)
SELECT
    object_id::NUMBER AS ticket_id,
    display_id,
    attributes:ticket_data:subject::STRING AS subject,
    attributes:status::STRING AS status,
    attributes:priority::STRING AS priority,
    s.value:id::NUMBER AS sla_breach_id,
    s.value:name::STRING AS sla_name,
    s.value:breached::BOOLEAN AS is_breached,
    TRY_TO_TIMESTAMP_NTZ(s.value:breach_time::STRING) AS breach_time,
    s.value:first_response_time::NUMBER AS first_response_time_minutes,
    s.value:resolution_time::NUMBER AS resolution_time_minutes,
    occurred_at AS as_of_time
FROM latest_tickets,
LATERAL FLATTEN(INPUT => attributes:ticket_data:sla_breaches, OUTER => TRUE) s
WHERE rn = 1
  AND s.value:breached = TRUE;

-- ----------------------------------------------------------------------------
-- VERIFICATION
-- ----------------------------------------------------------------------------

-- Check all views are created
SELECT 
    TABLE_SCHEMA,
    TABLE_NAME,
    ROW_COUNT,
    COMMENT
FROM CLAUDE_BI.INFORMATION_SCHEMA.TABLES
WHERE TABLE_CATALOG = 'CLAUDE_BI'
  AND TABLE_SCHEMA = 'MCP'
  AND TABLE_NAME LIKE 'VW_HF_%'
  AND TABLE_TYPE = 'VIEW'
ORDER BY TABLE_NAME;

-- Sample data from main view
SELECT 
    product_prefix,
    lifecycle_state,
    COUNT(*) as ticket_count,
    AVG(age_days) as avg_age_days
FROM MCP.VW_HF_TICKET_AGING
GROUP BY product_prefix, lifecycle_state
ORDER BY ticket_count DESC
LIMIT 10;