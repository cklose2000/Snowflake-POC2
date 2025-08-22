-- ============================================================================
-- Monitoring Views for HappyFox Data Pipeline
-- Purpose: Track data quality, load history, and pipeline health
-- All monitoring via views - no new tables
-- ============================================================================

USE DATABASE CLAUDE_BI;
USE WAREHOUSE CLAUDE_WAREHOUSE;
USE SCHEMA MCP;

-- ----------------------------------------------------------------------------
-- VIEW 1: Load History
-- ----------------------------------------------------------------------------

CREATE OR REPLACE VIEW MCP.VW_HF_LOAD_HISTORY AS
SELECT
    DATA:occurred_at::TIMESTAMP_NTZ AS load_time,
    DATA:attributes:status::STRING AS status,
    DATA:attributes:loaded_count::NUMBER AS tickets_loaded,
    DATA:attributes:files_processed::NUMBER AS files_processed,
    DATA:attributes:start_count::NUMBER AS tickets_before,
    DATA:attributes:end_count::NUMBER AS tickets_after,
    DATA:attributes:timestamp::TIMESTAMP_NTZ AS completion_time,
    DATEDIFF('second', DATA:occurred_at::TIMESTAMP_NTZ, DATA:attributes:timestamp::TIMESTAMP_NTZ) AS duration_seconds
FROM LANDING.RAW_EVENTS
WHERE DATA:action = 'system.data.loaded'
  AND DATA:source = 'HAPPYFOX_LOADER'
ORDER BY load_time DESC;

-- ----------------------------------------------------------------------------
-- VIEW 2: Data Quality Metrics
-- ----------------------------------------------------------------------------

CREATE OR REPLACE VIEW MCP.VW_HF_DATA_QUALITY AS
WITH ticket_stats AS (
    SELECT
        COUNT(*) AS total_events,
        COUNT(DISTINCT object_id) AS unique_tickets,
        COUNT(DISTINCT product_prefix) AS product_count,
        
        -- Completeness checks
        SUM(CASE WHEN attributes:subject IS NULL THEN 1 ELSE 0 END) AS missing_subject,
        SUM(CASE WHEN attributes:status IS NULL THEN 1 ELSE 0 END) AS missing_status,
        SUM(CASE WHEN attributes:priority IS NULL THEN 1 ELSE 0 END) AS missing_priority,
        SUM(CASE WHEN attributes:product_prefix IS NULL THEN 1 ELSE 0 END) AS missing_product,
        SUM(CASE WHEN attributes:assignee IS NULL THEN 1 ELSE 0 END) AS unassigned_tickets,
        
        -- Data freshness
        MIN(occurred_at) AS earliest_ticket,
        MAX(occurred_at) AS latest_ticket,
        MAX(ingested_at) AS last_ingested
        
    FROM ACTIVITY.EVENTS
    WHERE action = 'happyfox.ticket.upserted'
)
SELECT
    total_events,
    unique_tickets,
    product_count,
    
    -- Completeness percentages
    ROUND(100.0 * (total_events - missing_subject) / NULLIF(total_events, 0), 2) AS subject_completeness_pct,
    ROUND(100.0 * (total_events - missing_status) / NULLIF(total_events, 0), 2) AS status_completeness_pct,
    ROUND(100.0 * (total_events - missing_priority) / NULLIF(total_events, 0), 2) AS priority_completeness_pct,
    ROUND(100.0 * (total_events - missing_product) / NULLIF(total_events, 0), 2) AS product_completeness_pct,
    ROUND(100.0 * (total_events - unassigned_tickets) / NULLIF(total_events, 0), 2) AS assignment_rate_pct,
    
    -- Data currency
    earliest_ticket,
    latest_ticket,
    last_ingested,
    DATEDIFF('hour', latest_ticket, CURRENT_TIMESTAMP()) AS hours_since_last_ticket,
    DATEDIFF('hour', last_ingested, CURRENT_TIMESTAMP()) AS hours_since_last_ingest
    
FROM ticket_stats;

-- ----------------------------------------------------------------------------
-- VIEW 3: Duplicate Detection
-- ----------------------------------------------------------------------------

CREATE OR REPLACE VIEW MCP.VW_HF_DUPLICATE_CHECK AS
WITH duplicate_candidates AS (
    SELECT
        object_id,
        COUNT(*) AS version_count,
        MIN(occurred_at) AS first_seen,
        MAX(occurred_at) AS last_seen,
        ARRAY_AGG(DISTINCT attributes:status::STRING) AS status_values,
        ARRAY_AGG(DISTINCT attributes:assignee::STRING) AS assignee_values
    FROM ACTIVITY.EVENTS
    WHERE action = 'happyfox.ticket.upserted'
    GROUP BY object_id
    HAVING COUNT(*) > 1
)
SELECT
    object_id AS ticket_id,
    version_count,
    first_seen,
    last_seen,
    DATEDIFF('hour', first_seen, last_seen) AS hours_between_versions,
    ARRAY_SIZE(status_values) AS unique_status_count,
    ARRAY_SIZE(assignee_values) AS unique_assignee_count,
    status_values,
    assignee_values
FROM duplicate_candidates
ORDER BY version_count DESC, last_seen DESC;

-- ----------------------------------------------------------------------------
-- VIEW 4: Pipeline Health Dashboard
-- ----------------------------------------------------------------------------

CREATE OR REPLACE VIEW MCP.VW_HF_PIPELINE_HEALTH AS
WITH recent_loads AS (
    SELECT
        MAX(DATA:occurred_at::TIMESTAMP_NTZ) AS last_load_time,
        MAX(DATA:attributes:loaded_count::NUMBER) AS last_load_count,
        AVG(DATA:attributes:loaded_count::NUMBER) AS avg_load_count,
        COUNT(*) AS load_count_7d
    FROM LANDING.RAW_EVENTS
    WHERE DATA:action = 'system.data.loaded'
      AND DATA:source = 'HAPPYFOX_LOADER'
      AND DATA:occurred_at::TIMESTAMP_NTZ >= DATEADD('day', -7, CURRENT_TIMESTAMP())
),
error_counts AS (
    SELECT
        COUNT(*) AS error_count_24h
    FROM LANDING.RAW_EVENTS
    WHERE DATA:action LIKE 'system.alert%'
      AND DATA:source = 'HAPPYFOX_MONITOR'
      AND DATA:occurred_at::TIMESTAMP_NTZ >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
),
data_stats AS (
    SELECT
        COUNT(DISTINCT object_id) AS total_tickets,
        MAX(occurred_at) AS latest_ticket_time
    FROM ACTIVITY.EVENTS
    WHERE action = 'happyfox.ticket.upserted'
)
SELECT
    -- Load metrics
    l.last_load_time,
    l.last_load_count,
    l.avg_load_count,
    l.load_count_7d,
    
    -- Error metrics
    e.error_count_24h,
    
    -- Data metrics
    d.total_tickets,
    d.latest_ticket_time,
    
    -- Health indicators
    CASE 
        WHEN DATEDIFF('hour', l.last_load_time, CURRENT_TIMESTAMP()) > 48 THEN 'STALE'
        WHEN e.error_count_24h > 0 THEN 'WARNING'
        ELSE 'HEALTHY'
    END AS pipeline_status,
    
    CASE
        WHEN DATEDIFF('hour', l.last_load_time, CURRENT_TIMESTAMP()) > 48 
        THEN 'No load in ' || DATEDIFF('hour', l.last_load_time, CURRENT_TIMESTAMP()) || ' hours'
        WHEN e.error_count_24h > 0 
        THEN e.error_count_24h || ' errors in last 24 hours'
        ELSE 'All systems operational'
    END AS status_message
    
FROM recent_loads l, error_counts e, data_stats d;

-- ----------------------------------------------------------------------------
-- VIEW 5: Ingestion Rate Tracking
-- ----------------------------------------------------------------------------

CREATE OR REPLACE VIEW MCP.VW_HF_INGESTION_RATE AS
WITH hourly_ingestion AS (
    SELECT
        DATE_TRUNC('hour', ingested_at) AS hour,
        COUNT(*) AS tickets_ingested,
        COUNT(DISTINCT object_id) AS unique_tickets
    FROM ACTIVITY.EVENTS
    WHERE action = 'happyfox.ticket.upserted'
      AND ingested_at >= DATEADD('day', -7, CURRENT_TIMESTAMP())
    GROUP BY DATE_TRUNC('hour', ingested_at)
),
daily_ingestion AS (
    SELECT
        DATE_TRUNC('day', ingested_at) AS day,
        COUNT(*) AS tickets_ingested,
        COUNT(DISTINCT object_id) AS unique_tickets
    FROM ACTIVITY.EVENTS
    WHERE action = 'happyfox.ticket.upserted'
      AND ingested_at >= DATEADD('day', -30, CURRENT_TIMESTAMP())
    GROUP BY DATE_TRUNC('day', ingested_at)
)
SELECT
    'Hourly' AS granularity,
    hour AS time_period,
    tickets_ingested,
    unique_tickets,
    ROUND(tickets_ingested / 60.0, 2) AS tickets_per_minute
FROM hourly_ingestion
UNION ALL
SELECT
    'Daily' AS granularity,
    day AS time_period,
    tickets_ingested,
    unique_tickets,
    ROUND(tickets_ingested / 1440.0, 2) AS tickets_per_minute
FROM daily_ingestion
ORDER BY granularity, time_period DESC;

-- ----------------------------------------------------------------------------
-- VIEW 6: Alert History
-- ----------------------------------------------------------------------------

CREATE OR REPLACE VIEW MCP.VW_HF_ALERT_HISTORY AS
SELECT
    DATA:occurred_at::TIMESTAMP_NTZ AS alert_time,
    DATA:action::STRING AS alert_action,
    DATA:attributes:alert_type::STRING AS alert_type,
    DATA:attributes:message::STRING AS message,
    DATA:attributes:load_result AS load_details,
    DATEDIFF('hour', DATA:occurred_at::TIMESTAMP_NTZ, CURRENT_TIMESTAMP()) AS hours_ago
FROM LANDING.RAW_EVENTS
WHERE DATA:source = 'HAPPYFOX_MONITOR'
  AND DATA:action LIKE 'system.alert%'
ORDER BY alert_time DESC;

-- ----------------------------------------------------------------------------
-- HELPER FUNCTIONS
-- ----------------------------------------------------------------------------

-- Function to get current pipeline status
CREATE OR REPLACE FUNCTION MCP.GET_HAPPYFOX_PIPELINE_STATUS()
RETURNS VARCHAR
AS
$$
    SELECT pipeline_status 
    FROM MCP.VW_HF_PIPELINE_HEALTH
$$;

-- Function to check if load is needed
CREATE OR REPLACE FUNCTION MCP.HAPPYFOX_LOAD_NEEDED()
RETURNS BOOLEAN
AS
$$
    SELECT DATEDIFF('hour', last_load_time, CURRENT_TIMESTAMP()) > 24
    FROM MCP.VW_HF_PIPELINE_HEALTH
$$;

-- ----------------------------------------------------------------------------
-- MONITORING DASHBOARD QUERY
-- ----------------------------------------------------------------------------

-- Sample dashboard query combining multiple monitoring views
CREATE OR REPLACE VIEW MCP.VW_HF_MONITORING_DASHBOARD AS
SELECT
    h.pipeline_status,
    h.status_message,
    h.total_tickets,
    h.last_load_time,
    h.last_load_count,
    q.subject_completeness_pct,
    q.assignment_rate_pct,
    q.hours_since_last_ingest,
    (SELECT COUNT(*) FROM MCP.VW_HF_ALERT_HISTORY WHERE hours_ago <= 24) AS alerts_24h,
    (SELECT COUNT(*) FROM MCP.VW_HF_DUPLICATE_CHECK) AS duplicate_ticket_count
FROM MCP.VW_HF_PIPELINE_HEALTH h, MCP.VW_HF_DATA_QUALITY q;

-- ----------------------------------------------------------------------------
-- VERIFICATION
-- ----------------------------------------------------------------------------

-- Check monitoring views
SELECT 
    'Pipeline Status' AS metric,
    pipeline_status AS value
FROM MCP.VW_HF_PIPELINE_HEALTH
UNION ALL
SELECT 
    'Total Tickets' AS metric,
    total_tickets::STRING AS value
FROM MCP.VW_HF_DATA_QUALITY
UNION ALL
SELECT 
    'Last Load' AS metric,
    load_time::STRING AS value
FROM MCP.VW_HF_LOAD_HISTORY
LIMIT 1;