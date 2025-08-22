-- ============================================================================
-- HappyFox Complete Enhanced Analytics Suite
-- All views for self-serve drill-down and export
-- Maintains two-table doctrine: reads ONLY from ACTIVITY_STREAM  
-- ============================================================================

USE DATABASE CLAUDE_BI;
USE WAREHOUSE CLAUDE_WAREHOUSE;

-- ============================================================================
-- B) TICKET HISTORY WITH DWELL TIMES
-- Shows how long tickets spent in each status
-- ============================================================================

CREATE OR REPLACE VIEW MCP.VW_HF_TICKET_HISTORY AS
WITH hist AS (
  SELECT
    FEATURE_JSON:attributes:ticket_data:id::NUMBER AS ticket_id,
    FEATURE_JSON:attributes:ticket_data:display_id::STRING AS display_id,
    TS AS event_time,
    FEATURE_JSON:attributes:ticket_data:status:name::STRING AS status,
    FEATURE_JSON:attributes:ticket_data:priority:name::STRING AS priority,
    FEATURE_JSON:attributes:ticket_data:assigned_to:email::STRING AS assignee_email,
    FEATURE_JSON:attributes:ticket_data:assigned_to:name::STRING AS assignee_name
  FROM ACTIVITY.ACTIVITY_STREAM
  WHERE ACTIVITY = 'happyfox.ticket.upserted'
),
segmented AS (
  SELECT
    ticket_id,
    display_id,
    status,
    assignee_name,
    event_time AS status_start_time,
    LEAD(event_time) OVER (PARTITION BY ticket_id ORDER BY event_time) AS next_event_time,
    LAG(status) OVER (PARTITION BY ticket_id ORDER BY event_time) AS previous_status,
    ROW_NUMBER() OVER (PARTITION BY ticket_id ORDER BY event_time) AS version_number
  FROM hist
  WHERE status IS NOT NULL
)
SELECT
  ticket_id,
  display_id,
  status,
  previous_status,
  assignee_name,
  status_start_time,
  COALESCE(next_event_time, CURRENT_TIMESTAMP()) AS status_end_time,
  DATEDIFF('minute', status_start_time, COALESCE(next_event_time, CURRENT_TIMESTAMP())) AS minutes_in_status,
  DATEDIFF('hour', status_start_time, COALESCE(next_event_time, CURRENT_TIMESTAMP())) AS hours_in_status,
  DATEDIFF('day', status_start_time, COALESCE(next_event_time, CURRENT_TIMESTAMP())) AS days_in_status,
  version_number,
  CASE 
    WHEN previous_status IS NULL THEN 'Created'
    WHEN status != previous_status THEN 'Status Changed'
    ELSE 'Updated'
  END AS change_type
FROM segmented
ORDER BY ticket_id, status_start_time;

-- ============================================================================
-- C) WIDE EXPORT-FRIENDLY VIEW
-- Flattened view with popular fields for easy CSV export
-- ============================================================================

CREATE OR REPLACE VIEW MCP.VW_HF_TICKETS_EXPORT AS
WITH base AS (
  SELECT * FROM MCP.VW_HF_TICKETS_LATEST
),
tags AS (
  -- Aggregate tags per ticket (handling both string and array formats)
  SELECT
    FEATURE_JSON:attributes:ticket_data:id::NUMBER AS ticket_id,
    CASE 
      WHEN IS_ARRAY(FEATURE_JSON:attributes:ticket_data:tags) THEN
        LISTAGG(t.value::STRING, ', ') WITHIN GROUP (ORDER BY t.value::STRING)
      ELSE
        FEATURE_JSON:attributes:ticket_data:tags::STRING
    END AS tags_csv
  FROM ACTIVITY.ACTIVITY_STREAM s
  LEFT JOIN LATERAL FLATTEN(
    INPUT => CASE 
      WHEN IS_ARRAY(FEATURE_JSON:attributes:ticket_data:tags) 
      THEN FEATURE_JSON:attributes:ticket_data:tags
      ELSE ARRAY_CONSTRUCT()
    END, 
    OUTER => TRUE
  ) t
  WHERE s.ACTIVITY = 'happyfox.ticket.upserted'
    AND FEATURE_JSON:attributes:ticket_data:id IS NOT NULL
  GROUP BY 1, FEATURE_JSON:attributes:ticket_data:tags
),
custom_fields AS (
  -- Extract common custom fields - latest version per ticket
  WITH latest_cf AS (
    SELECT
      FEATURE_JSON:attributes:ticket_data:id::NUMBER AS ticket_id,
      FEATURE_JSON:attributes:ticket_data:custom_fields AS cf_array,
      ROW_NUMBER() OVER (PARTITION BY FEATURE_JSON:attributes:ticket_data:id::NUMBER ORDER BY TS DESC) AS rn
    FROM ACTIVITY.ACTIVITY_STREAM
    WHERE ACTIVITY = 'happyfox.ticket.upserted'
      AND FEATURE_JSON:attributes:ticket_data:id IS NOT NULL
  )
  SELECT
    ticket_id,
    MAX(IFF(LOWER(f.value:name::STRING) LIKE '%component%', f.value:value::STRING, NULL)) AS component,
    MAX(IFF(LOWER(f.value:name::STRING) = 'root cause', f.value:value::STRING, NULL)) AS root_cause,
    MAX(IFF(LOWER(f.value:name::STRING) = 'severity', f.value:value::STRING, NULL)) AS severity,
    MAX(IFF(LOWER(f.value:name::STRING) = 'environment', f.value:value::STRING, NULL)) AS environment,
    MAX(IFF(LOWER(f.value:name::STRING) LIKE '%customer%', f.value:value::STRING, NULL)) AS customer_name
  FROM latest_cf,
       LATERAL FLATTEN(INPUT => cf_array, OUTER => TRUE) f
  WHERE rn = 1
  GROUP BY ticket_id
)
SELECT
  -- Core fields
  b.ticket_id,
  b.display_id,
  b.product_prefix,
  b.subject,
  
  -- Status and priority
  b.status,
  b.priority,
  b.category,
  
  -- Timestamps
  b.created_at,
  b.last_updated_at,
  b.last_modified,
  
  -- Age calculations
  DATEDIFF('day', b.created_at, CURRENT_TIMESTAMP()) AS age_days,
  DATEDIFF('hour', b.created_at, CURRENT_TIMESTAMP()) AS age_hours,
  DATEDIFF('day', b.last_updated_at, CURRENT_TIMESTAMP()) AS days_since_update,
  
  -- Assignment
  b.assignee_name,
  b.assignee_email,
  
  -- Interaction metrics
  b.messages_count,
  b.attachments_count,
  b.time_spent_minutes,
  b.source_channel,
  
  -- Response times
  b.last_user_reply_at,
  b.last_staff_reply_at,
  DATEDIFF('hour', b.created_at, b.last_staff_reply_at) AS first_response_hours,
  
  -- Custom fields
  cf.component,
  cf.root_cause,
  cf.severity,
  cf.environment,
  cf.customer_name,
  
  -- Tags
  t.tags_csv,
  
  -- Lifecycle state
  CASE 
    WHEN LOWER(b.status) IN ('closed', 'resolved', 'completed') THEN 'Closed'
    WHEN LOWER(b.status) IN ('new', 'open', 'in progress', 'pending', 'on hold') THEN 'Open'
    ELSE 'Unknown'
  END AS lifecycle_state,
  
  -- Age bucket for reporting
  CASE 
    WHEN DATEDIFF('day', b.created_at, CURRENT_TIMESTAMP()) = 0 THEN '0-1 days'
    WHEN DATEDIFF('day', b.created_at, CURRENT_TIMESTAMP()) <= 3 THEN '2-3 days'
    WHEN DATEDIFF('day', b.created_at, CURRENT_TIMESTAMP()) <= 7 THEN '4-7 days'
    WHEN DATEDIFF('day', b.created_at, CURRENT_TIMESTAMP()) <= 14 THEN '8-14 days'
    WHEN DATEDIFF('day', b.created_at, CURRENT_TIMESTAMP()) <= 30 THEN '15-30 days'
    WHEN DATEDIFF('day', b.created_at, CURRENT_TIMESTAMP()) <= 60 THEN '31-60 days'
    WHEN DATEDIFF('day', b.created_at, CURRENT_TIMESTAMP()) <= 90 THEN '61-90 days'
    WHEN DATEDIFF('day', b.created_at, CURRENT_TIMESTAMP()) <= 180 THEN '91-180 days'
    ELSE '180+ days'
  END AS age_bucket
  
FROM base b
LEFT JOIN custom_fields cf ON cf.ticket_id = b.ticket_id
LEFT JOIN tags t ON t.ticket_id = b.ticket_id;

-- ============================================================================
-- D) ONE-TICKET DETAIL VIEW (for drill-down)
-- Complete ticket information for single-ticket analysis
-- ============================================================================

CREATE OR REPLACE VIEW MCP.VW_HF_TICKET_DETAIL AS
SELECT
  -- All core fields
  ticket_id,
  display_id,
  subject,
  first_message,
  status,
  priority,
  category,
  product_prefix,
  
  -- Assignment
  assignee_name,
  assignee_email,
  assignee_id,
  
  -- All timestamps
  created_at,
  last_updated_at,
  last_modified,
  last_user_reply_at,
  last_staff_reply_at,
  
  -- Metrics
  messages_count,
  attachments_count,
  time_spent_minutes,
  source_channel,
  
  -- Age calculations
  DATEDIFF('day', created_at, CURRENT_TIMESTAMP()) AS age_days,
  DATEDIFF('hour', created_at, CURRENT_TIMESTAMP()) AS age_hours,
  
  -- Response metrics
  DATEDIFF('hour', created_at, last_staff_reply_at) AS first_response_hours,
  DATEDIFF('hour', last_user_reply_at, last_staff_reply_at) AS last_response_hours,
  
  -- Full JSON for complete access
  ticket_json AS full_ticket_data,
  GET_PATH(ticket_json, 'sla_breaches') AS sla_breaches,
  GET_PATH(ticket_json, 'custom_fields') AS custom_fields,
  GET_PATH(ticket_json, 'tags') AS tags,
  GET_PATH(ticket_json, 'cc_emails') AS cc_emails,
  GET_PATH(ticket_json, 'updates') AS updates,
  GET_PATH(ticket_json, 'user') AS user_info,
  
  -- Metadata
  last_event_time,
  last_ingested_at
  
FROM MCP.VW_HF_TICKETS_LATEST;

-- ============================================================================
-- E) PRODUCT VELOCITY VIEW (PoC for Enhanced Analytics)
-- Shows creation rate, resolution rate, and backlog trends
-- ============================================================================

CREATE OR REPLACE VIEW MCP.VW_HF_PRODUCT_VELOCITY AS
WITH latest AS (
  SELECT * FROM MCP.VW_HF_TICKETS_LATEST
),
closed_tickets AS (
  -- Find first time each ticket was closed/resolved
  SELECT
    FEATURE_JSON:attributes:ticket_data:id::NUMBER AS ticket_id,
    MIN(TS) AS first_closed_time
  FROM ACTIVITY.ACTIVITY_STREAM
  WHERE ACTIVITY = 'happyfox.ticket.upserted'
    AND LOWER(FEATURE_JSON:attributes:ticket_data:status:name::STRING) IN ('closed', 'resolved', 'completed')
  GROUP BY 1
),
daily_created AS (
  -- Tickets created per day by product
  SELECT
    product_prefix,
    DATE_TRUNC('day', created_at) AS day,
    COUNT(*) AS created_count
  FROM latest
  WHERE created_at IS NOT NULL
  GROUP BY 1, 2
),
daily_resolved AS (
  -- Tickets resolved per day by product
  SELECT
    l.product_prefix,
    DATE_TRUNC('day', c.first_closed_time) AS day,
    COUNT(*) AS resolved_count
  FROM latest l
  JOIN closed_tickets c ON c.ticket_id = l.ticket_id
  WHERE c.first_closed_time IS NOT NULL
  GROUP BY 1, 2
),
current_backlog AS (
  -- Current open tickets by product
  SELECT
    product_prefix,
    COUNT(*) AS open_tickets_now,
    AVG(DATEDIFF('day', created_at, CURRENT_TIMESTAMP())) AS avg_age_days,
    MEDIAN(DATEDIFF('day', created_at, CURRENT_TIMESTAMP())) AS median_age_days,
    MAX(DATEDIFF('day', created_at, CURRENT_TIMESTAMP())) AS max_age_days
  FROM latest
  WHERE LOWER(status) NOT IN ('closed', 'resolved', 'completed')
  GROUP BY 1
),
historical_backlog AS (
  -- Backlog 30 days ago
  SELECT
    l.product_prefix,
    COUNT(*) AS open_tickets_30d_ago
  FROM latest l
  LEFT JOIN closed_tickets c ON c.ticket_id = l.ticket_id
  WHERE l.created_at <= DATEADD('day', -30, CURRENT_DATE())
    AND (c.first_closed_time IS NULL OR c.first_closed_time > DATEADD('day', -30, CURRENT_DATE()))
  GROUP BY 1
)
SELECT
  COALESCE(cb.product_prefix, hb.product_prefix) AS product_prefix,
  
  -- 30-day metrics
  (SELECT SUM(created_count) FROM daily_created 
   WHERE product_prefix = COALESCE(cb.product_prefix, hb.product_prefix)
     AND day >= DATEADD('day', -30, CURRENT_DATE())) AS created_last_30d,
     
  (SELECT SUM(resolved_count) FROM daily_resolved 
   WHERE product_prefix = COALESCE(cb.product_prefix, hb.product_prefix)
     AND day >= DATEADD('day', -30, CURRENT_DATE())) AS resolved_last_30d,
  
  -- 7-day metrics for trend
  (SELECT SUM(created_count) FROM daily_created 
   WHERE product_prefix = COALESCE(cb.product_prefix, hb.product_prefix)
     AND day >= DATEADD('day', -7, CURRENT_DATE())) AS created_last_7d,
     
  (SELECT SUM(resolved_count) FROM daily_resolved 
   WHERE product_prefix = COALESCE(cb.product_prefix, hb.product_prefix)
     AND day >= DATEADD('day', -7, CURRENT_DATE())) AS resolved_last_7d,
  
  -- Current backlog
  cb.open_tickets_now,
  cb.avg_age_days,
  cb.median_age_days,
  cb.max_age_days,
  
  -- Backlog trend
  hb.open_tickets_30d_ago,
  (cb.open_tickets_now - COALESCE(hb.open_tickets_30d_ago, 0)) AS backlog_growth_30d,
  
  -- Velocity metrics
  CASE 
    WHEN COALESCE((SELECT SUM(resolved_count) FROM daily_resolved 
                   WHERE product_prefix = COALESCE(cb.product_prefix, hb.product_prefix)
                     AND day >= DATEADD('day', -30, CURRENT_DATE())), 0) = 0 THEN NULL
    ELSE ROUND((SELECT SUM(created_count) FROM daily_created 
                WHERE product_prefix = COALESCE(cb.product_prefix, hb.product_prefix)
                  AND day >= DATEADD('day', -30, CURRENT_DATE()))::FLOAT / 
               NULLIF((SELECT SUM(resolved_count) FROM daily_resolved 
                       WHERE product_prefix = COALESCE(cb.product_prefix, hb.product_prefix)
                         AND day >= DATEADD('day', -30, CURRENT_DATE())), 0), 2)
  END AS creation_to_resolution_ratio,
  
  -- Health score (simple version)
  CASE
    WHEN cb.median_age_days <= 7 AND 
         COALESCE(creation_to_resolution_ratio, 1) <= 1.1 THEN 'Healthy'
    WHEN cb.median_age_days <= 14 AND 
         COALESCE(creation_to_resolution_ratio, 1) <= 1.3 THEN 'Good'
    WHEN cb.median_age_days <= 30 AND 
         COALESCE(creation_to_resolution_ratio, 1) <= 1.5 THEN 'Fair'
    ELSE 'Needs Attention'
  END AS health_status
  
FROM current_backlog cb
FULL OUTER JOIN historical_backlog hb ON cb.product_prefix = hb.product_prefix
ORDER BY cb.open_tickets_now DESC NULLS LAST;

-- ============================================================================
-- F) AGENT PERFORMANCE VIEW
-- Shows agent productivity metrics
-- ============================================================================

CREATE OR REPLACE VIEW MCP.VW_HF_AGENT_PERFORMANCE AS
WITH agent_tickets AS (
  SELECT
    assignee_name,
    assignee_email,
    COUNT(*) AS total_assigned,
    SUM(CASE WHEN LOWER(status) IN ('closed', 'resolved', 'completed') THEN 1 ELSE 0 END) AS resolved_count,
    SUM(CASE WHEN LOWER(status) NOT IN ('closed', 'resolved', 'completed') THEN 1 ELSE 0 END) AS open_count,
    AVG(time_spent_minutes) AS avg_time_spent,
    SUM(time_spent_minutes) AS total_time_spent,
    AVG(messages_count) AS avg_messages,
    AVG(DATEDIFF('hour', created_at, last_staff_reply_at)) AS avg_first_response_hours,
    MEDIAN(DATEDIFF('hour', created_at, last_staff_reply_at)) AS median_first_response_hours
  FROM MCP.VW_HF_TICKETS_LATEST
  WHERE assignee_name IS NOT NULL
  GROUP BY 1, 2
)
SELECT
  assignee_name,
  assignee_email,
  total_assigned,
  resolved_count,
  open_count,
  ROUND(resolved_count * 100.0 / NULLIF(total_assigned, 0), 2) AS resolution_rate,
  avg_time_spent,
  total_time_spent,
  avg_messages,
  avg_first_response_hours,
  median_first_response_hours,
  CASE
    WHEN resolved_count >= 100 AND avg_first_response_hours <= 4 THEN 'Top Performer'
    WHEN resolved_count >= 50 AND avg_first_response_hours <= 8 THEN 'Strong Performer'
    WHEN resolved_count >= 25 THEN 'Standard Performer'
    ELSE 'Developing'
  END AS performance_tier
FROM agent_tickets
ORDER BY resolved_count DESC;

-- ============================================================================
-- VERIFICATION QUERIES
-- ============================================================================

-- Check all views were created
SELECT 
    TABLE_SCHEMA,
    TABLE_NAME
FROM CLAUDE_BI.INFORMATION_SCHEMA.TABLES
WHERE TABLE_CATALOG = 'CLAUDE_BI'
  AND TABLE_SCHEMA = 'MCP'
  AND TABLE_NAME IN (
    'VW_HF_TICKETS_LATEST',
    'VW_HF_TICKET_HISTORY',
    'VW_HF_TICKETS_EXPORT',
    'VW_HF_TICKET_DETAIL',
    'VW_HF_PRODUCT_VELOCITY',
    'VW_HF_AGENT_PERFORMANCE'
  )
  AND TABLE_TYPE = 'VIEW'
ORDER BY TABLE_NAME;

-- Sample the export view
SELECT 
    product_prefix,
    lifecycle_state,
    COUNT(*) as ticket_count
FROM MCP.VW_HF_TICKETS_EXPORT
GROUP BY 1, 2
ORDER BY 3 DESC
LIMIT 10;

-- Check product velocity
SELECT * FROM MCP.VW_HF_PRODUCT_VELOCITY
WHERE product_prefix IN ('GZ', 'CM', 'EZ', 'MN')
ORDER BY open_tickets_now DESC;