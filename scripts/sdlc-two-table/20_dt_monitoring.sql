-- ============================================================================
-- 20_dt_monitoring.sql
-- Monitoring views for Dynamic Table health and performance
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE CLAUDE_BI;
USE SCHEMA MCP;

-- ============================================================================
-- Overall Dynamic Table Health View
-- ============================================================================
CREATE OR REPLACE VIEW VW_DT_HEALTH AS
WITH table_stats AS (
  SELECT 
    'LANDING.RAW_EVENTS' as table_name,
    COUNT(*) as row_count,
    MIN(_RECV_AT) as earliest_event,
    MAX(_RECV_AT) as latest_event,
    DATEDIFF('day', MIN(_RECV_AT), MAX(_RECV_AT)) as data_span_days
  FROM CLAUDE_BI.LANDING.RAW_EVENTS
),
dt_stats AS (
  SELECT 
    'ACTIVITY.EVENTS' as table_name,
    COUNT(*) as row_count,
    MIN(ingested_at) as earliest_event,
    MAX(ingested_at) as latest_event,
    DATEDIFF('day', MIN(ingested_at), MAX(ingested_at)) as data_span_days
  FROM CLAUDE_BI.ACTIVITY.EVENTS
),
dt_info AS (
  SELECT 
    NAME,
    TARGET_LAG,
    REFRESH_MODE,
    SCHEDULING_STATE,
    DATA_TIMESTAMP,
    ROWS,
    BYTES
  FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_GRAPH_HISTORY())
  WHERE QUALIFIED_NAME = 'CLAUDE_BI.ACTIVITY.EVENTS'
  QUALIFY ROW_NUMBER() OVER (ORDER BY DATA_TIMESTAMP DESC) = 1
)
SELECT 
  t.row_count as raw_events_count,
  d.row_count as activity_events_count,
  t.row_count - d.row_count as pending_promotion_count,
  ROUND(100.0 * d.row_count / NULLIF(t.row_count, 0), 2) as promotion_percentage,
  i.TARGET_LAG,
  i.REFRESH_MODE,
  i.SCHEDULING_STATE,
  i.DATA_TIMESTAMP as last_refresh,
  DATEDIFF('second', i.DATA_TIMESTAMP, CURRENT_TIMESTAMP()) as seconds_since_refresh,
  CASE 
    WHEN t.row_count = d.row_count THEN 'SYNCHRONIZED'
    WHEN ABS(t.row_count - d.row_count) <= 10 THEN 'NEARLY_SYNCHRONIZED'
    WHEN ABS(t.row_count - d.row_count) <= 100 THEN 'MINOR_LAG'
    ELSE 'SIGNIFICANT_LAG'
  END as sync_status,
  CASE 
    WHEN i.SCHEDULING_STATE = 'ACTIVE' 
      AND DATEDIFF('second', i.DATA_TIMESTAMP, CURRENT_TIMESTAMP()) < 120 THEN 'HEALTHY'
    WHEN i.SCHEDULING_STATE = 'ACTIVE' 
      AND DATEDIFF('second', i.DATA_TIMESTAMP, CURRENT_TIMESTAMP()) < 300 THEN 'WARNING'
    WHEN i.SCHEDULING_STATE != 'ACTIVE' THEN 'SUSPENDED'
    ELSE 'UNHEALTHY'
  END as health_status
FROM table_stats t
CROSS JOIN dt_stats d
CROSS JOIN dt_info i;

-- ============================================================================
-- Row Count Consistency Monitor
-- ============================================================================
CREATE OR REPLACE VIEW VW_DT_CONSISTENCY AS
WITH hourly_counts AS (
  SELECT 
    DATE_TRUNC('hour', _RECV_AT) as hour,
    'RAW_EVENTS' as source,
    COUNT(*) as event_count
  FROM CLAUDE_BI.LANDING.RAW_EVENTS
  WHERE _RECV_AT >= DATEADD('day', -7, CURRENT_TIMESTAMP())
  GROUP BY 1
  UNION ALL
  SELECT 
    DATE_TRUNC('hour', ingested_at) as hour,
    'ACTIVITY_EVENTS' as source,
    COUNT(*) as event_count
  FROM CLAUDE_BI.ACTIVITY.EVENTS
  WHERE ingested_at >= DATEADD('day', -7, CURRENT_TIMESTAMP())
  GROUP BY 1
),
comparison AS (
  SELECT 
    hour,
    MAX(CASE WHEN source = 'RAW_EVENTS' THEN event_count ELSE 0 END) as raw_count,
    MAX(CASE WHEN source = 'ACTIVITY_EVENTS' THEN event_count ELSE 0 END) as activity_count
  FROM hourly_counts
  GROUP BY hour
)
SELECT 
  hour,
  raw_count,
  activity_count,
  raw_count - activity_count as difference,
  ROUND(100.0 * activity_count / NULLIF(raw_count, 0), 2) as promotion_rate,
  CASE 
    WHEN raw_count = activity_count THEN 'PERFECT'
    WHEN ABS(raw_count - activity_count) <= 1 THEN 'EXCELLENT'
    WHEN ABS(raw_count - activity_count) <= 5 THEN 'GOOD'
    ELSE 'CHECK_REQUIRED'
  END as consistency_status
FROM comparison
ORDER BY hour DESC;

-- ============================================================================
-- Lag Monitoring View
-- ============================================================================
CREATE OR REPLACE VIEW VW_DT_LAG_MONITOR AS
WITH recent_events AS (
  -- Events in RAW but not yet in ACTIVITY
  SELECT 
    PAYLOAD:event_id::string as event_id,
    PAYLOAD:action::string as action,
    _RECV_AT as raw_timestamp,
    NULL as activity_timestamp,
    DATEDIFF('second', _RECV_AT, CURRENT_TIMESTAMP()) as lag_seconds
  FROM CLAUDE_BI.LANDING.RAW_EVENTS r
  WHERE _RECV_AT >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
    AND NOT EXISTS (
      SELECT 1 
      FROM CLAUDE_BI.ACTIVITY.EVENTS a 
      WHERE a.event_id = r.PAYLOAD:event_id::string
    )
),
lag_summary AS (
  SELECT 
    COUNT(*) as pending_events,
    MIN(lag_seconds) as min_lag_seconds,
    AVG(lag_seconds) as avg_lag_seconds,
    MAX(lag_seconds) as max_lag_seconds,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY lag_seconds) as median_lag_seconds,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY lag_seconds) as p95_lag_seconds
  FROM recent_events
)
SELECT 
  pending_events,
  min_lag_seconds,
  ROUND(avg_lag_seconds, 2) as avg_lag_seconds,
  median_lag_seconds,
  p95_lag_seconds,
  max_lag_seconds,
  CASE 
    WHEN pending_events = 0 THEN 'NO_LAG'
    WHEN max_lag_seconds <= 60 THEN 'WITHIN_TARGET'
    WHEN max_lag_seconds <= 120 THEN 'ACCEPTABLE'
    WHEN max_lag_seconds <= 300 THEN 'WARNING'
    ELSE 'CRITICAL'
  END as lag_status,
  CURRENT_TIMESTAMP() as check_time
FROM lag_summary;

-- ============================================================================
-- Deduplication Effectiveness View
-- ============================================================================
CREATE OR REPLACE VIEW VW_DT_DEDUP_STATS AS
WITH dedup_analysis AS (
  SELECT 
    COUNT(*) as total_raw_events,
    COUNT(DISTINCT DEDUPE_KEY) as unique_events,
    COUNT(*) - COUNT(DISTINCT DEDUPE_KEY) as duplicates_removed,
    ROUND(100.0 * (COUNT(*) - COUNT(DISTINCT DEDUPE_KEY)) / NULLIF(COUNT(*), 0), 2) as dedup_percentage
  FROM CLAUDE_BI.LANDING.RAW_EVENTS
  WHERE _RECV_AT >= DATEADD('day', -1, CURRENT_TIMESTAMP())
    AND DEDUPE_KEY IS NOT NULL
),
promoted_analysis AS (
  SELECT COUNT(*) as promoted_count
  FROM CLAUDE_BI.ACTIVITY.EVENTS
  WHERE ingested_at >= DATEADD('day', -1, CURRENT_TIMESTAMP())
)
SELECT 
  d.total_raw_events,
  d.unique_events,
  d.duplicates_removed,
  d.dedup_percentage,
  p.promoted_count,
  CASE 
    WHEN d.unique_events = p.promoted_count THEN 'PERFECT_DEDUP'
    WHEN ABS(d.unique_events - p.promoted_count) <= 10 THEN 'GOOD_DEDUP'
    ELSE 'CHECK_DEDUP'
  END as dedup_status
FROM dedup_analysis d
CROSS JOIN promoted_analysis p;

-- ============================================================================
-- Performance Metrics View
-- ============================================================================
CREATE OR REPLACE VIEW VW_DT_PERFORMANCE AS
WITH hourly_metrics AS (
  SELECT 
    DATE_TRUNC('hour', _RECV_AT) as hour,
    COUNT(*) as events_ingested,
    MIN(_RECV_AT) as first_event,
    MAX(_RECV_AT) as last_event,
    DATEDIFF('second', MIN(_RECV_AT), MAX(_RECV_AT)) as span_seconds
  FROM CLAUDE_BI.LANDING.RAW_EVENTS
  WHERE _RECV_AT >= DATEADD('day', -1, CURRENT_TIMESTAMP())
  GROUP BY 1
)
SELECT 
  hour,
  events_ingested,
  span_seconds,
  CASE 
    WHEN span_seconds > 0 THEN ROUND(events_ingested::FLOAT / span_seconds, 2)
    ELSE 0
  END as events_per_second,
  CASE 
    WHEN events_ingested < 100 THEN 'LOW_VOLUME'
    WHEN events_ingested < 1000 THEN 'MEDIUM_VOLUME'
    WHEN events_ingested < 10000 THEN 'HIGH_VOLUME'
    ELSE 'VERY_HIGH_VOLUME'
  END as volume_category
FROM hourly_metrics
ORDER BY hour DESC;

-- ============================================================================
-- Error and Filter Statistics View
-- ============================================================================
CREATE OR REPLACE VIEW VW_DT_FILTER_STATS AS
WITH raw_stats AS (
  SELECT 
    COUNT(*) as total_raw,
    COUNT(CASE WHEN PAYLOAD:action IS NULL THEN 1 END) as null_action_count,
    COUNT(CASE WHEN PAYLOAD:occurred_at IS NULL THEN 1 END) as null_time_count,
    COUNT(CASE WHEN PAYLOAD:actor_id IS NULL AND PAYLOAD:actor IS NULL THEN 1 END) as null_actor_count,
    COUNT(CASE WHEN _RECV_AT < DATEADD('day', -30, CURRENT_TIMESTAMP()) THEN 1 END) as old_event_count
  FROM CLAUDE_BI.LANDING.RAW_EVENTS
),
promoted_stats AS (
  SELECT COUNT(*) as total_promoted
  FROM CLAUDE_BI.ACTIVITY.EVENTS
)
SELECT 
  r.total_raw,
  p.total_promoted,
  r.total_raw - p.total_promoted as filtered_count,
  r.null_action_count,
  r.null_time_count,
  r.null_actor_count,
  r.old_event_count,
  ROUND(100.0 * p.total_promoted / NULLIF(r.total_raw, 0), 2) as promotion_rate,
  ROUND(100.0 * (r.total_raw - p.total_promoted) / NULLIF(r.total_raw, 0), 2) as filter_rate
FROM raw_stats r
CROSS JOIN promoted_stats p;

-- ============================================================================
-- Dynamic Table Refresh History View
-- ============================================================================
CREATE OR REPLACE VIEW VW_DT_REFRESH_HISTORY AS
SELECT 
  NAME,
  SCHEMA_NAME,
  DATABASE_NAME,
  TARGET_LAG_SEC as target_lag_seconds,
  DATA_TIMESTAMP,
  DATEDIFF('second', DATA_TIMESTAMP, CURRENT_TIMESTAMP()) as age_seconds,
  SCHEDULING_STATE:state::string as state,
  INPUTS,
  CASE 
    WHEN DATEDIFF('second', DATA_TIMESTAMP, CURRENT_TIMESTAMP()) <= TARGET_LAG_SEC THEN 'ON_TIME'
    WHEN DATEDIFF('second', DATA_TIMESTAMP, CURRENT_TIMESTAMP()) <= TARGET_LAG_SEC * 2 THEN 'SLIGHT_DELAY'
    ELSE 'BEHIND_SCHEDULE'
  END as refresh_status
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_GRAPH_HISTORY())
WHERE QUALIFIED_NAME = 'CLAUDE_BI.ACTIVITY.EVENTS'
ORDER BY DATA_TIMESTAMP DESC
LIMIT 10;

-- ============================================================================
-- Alert Generation View
-- ============================================================================
CREATE OR REPLACE VIEW VW_DT_ALERTS AS
SELECT 
  'ROW_COUNT_DRIFT' as alert_type,
  'HIGH' as severity,
  'Raw events and Activity events differ by ' || ABS(raw_events_count - activity_events_count) || ' rows' as message,
  CURRENT_TIMESTAMP() as alert_time
FROM VW_DT_HEALTH
WHERE ABS(raw_events_count - activity_events_count) > 100
UNION ALL
SELECT 
  'REFRESH_LAG' as alert_type,
  'CRITICAL' as severity,
  'Dynamic Table has not refreshed in ' || seconds_since_refresh || ' seconds' as message,
  CURRENT_TIMESTAMP() as alert_time
FROM VW_DT_HEALTH
WHERE seconds_since_refresh > 300
UNION ALL
SELECT 
  'HIGH_LAG' as alert_type,
  'WARNING' as severity,
  'Maximum event lag is ' || max_lag_seconds || ' seconds' as message,
  CURRENT_TIMESTAMP() as alert_time
FROM VW_DT_LAG_MONITOR
WHERE max_lag_seconds > 120
UNION ALL
SELECT 
  'DEDUP_ISSUE' as alert_type,
  'MEDIUM' as severity,
  'Deduplication mismatch: ' || ABS(unique_events - promoted_count) || ' events difference' as message,
  CURRENT_TIMESTAMP() as alert_time
FROM VW_DT_DEDUP_STATS
WHERE ABS(unique_events - promoted_count) > 10;

-- ============================================================================
-- Summary Dashboard View
-- ============================================================================
CREATE OR REPLACE VIEW VW_DT_DASHBOARD AS
SELECT 
  h.health_status,
  h.sync_status,
  h.raw_events_count,
  h.activity_events_count,
  h.pending_promotion_count,
  h.promotion_percentage,
  h.seconds_since_refresh,
  l.pending_events as current_lag_events,
  l.max_lag_seconds,
  l.lag_status,
  d.duplicates_removed as duplicates_removed_today,
  d.dedup_percentage,
  f.filter_rate,
  (SELECT COUNT(*) FROM VW_DT_ALERTS) as active_alerts
FROM VW_DT_HEALTH h
CROSS JOIN VW_DT_LAG_MONITOR l
CROSS JOIN VW_DT_DEDUP_STATS d
CROSS JOIN VW_DT_FILTER_STATS f;

-- Grant permissions
GRANT SELECT ON VIEW VW_DT_HEALTH TO ROLE MCP_USER_ROLE;
GRANT SELECT ON VIEW VW_DT_CONSISTENCY TO ROLE MCP_USER_ROLE;
GRANT SELECT ON VIEW VW_DT_LAG_MONITOR TO ROLE MCP_USER_ROLE;
GRANT SELECT ON VIEW VW_DT_DEDUP_STATS TO ROLE MCP_USER_ROLE;
GRANT SELECT ON VIEW VW_DT_PERFORMANCE TO ROLE MCP_USER_ROLE;
GRANT SELECT ON VIEW VW_DT_FILTER_STATS TO ROLE MCP_USER_ROLE;
GRANT SELECT ON VIEW VW_DT_REFRESH_HISTORY TO ROLE MCP_USER_ROLE;
GRANT SELECT ON VIEW VW_DT_ALERTS TO ROLE MCP_USER_ROLE;
GRANT SELECT ON VIEW VW_DT_DASHBOARD TO ROLE MCP_USER_ROLE;