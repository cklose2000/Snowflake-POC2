-- ============================================================================
-- 20_dt_views_only.sql - Deploy monitoring views only (no grants)
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