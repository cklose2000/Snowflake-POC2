-- ============================================================================
-- Enhanced Monitoring Views for Activity Schema 2.0
-- ID stability, dedup metrics, performance tracking, and operational health
-- MAINTAINS 2-TABLE ARCHITECTURE - Quality events are just events
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE CLAUDE_BI;
USE SCHEMA MCP;

-- ============================================================================
-- Quality Events View - Just a filtered view of EVENTS table
-- ============================================================================
CREATE OR REPLACE VIEW CLAUDE_BI.ACTIVITY.QUALITY_EVENTS AS
SELECT 
  event_id,
  occurred_at AS detected_at,
  actor_id,
  action,
  object_type,
  object_id,
  source,
  -- Extract quality-specific attributes
  attributes:validation_status::STRING AS validation_status,
  attributes:error_message::STRING AS error_message,
  attributes:affected_event_id::STRING AS affected_event_id,
  attributes:raw_payload AS payload,
  attributes,
  _source_lane,
  _recv_at
FROM CLAUDE_BI.ACTIVITY.EVENTS
WHERE action LIKE 'quality.%'  -- Only quality namespace events
  -- Common quality actions:
  -- quality.validation.failed
  -- quality.schema.invalid  
  -- quality.size.exceeded
  -- quality.parse.error
  -- quality.namespace.violation
  -- quality.batch.validation.check
;

-- ============================================================================
-- ID Stability Monitoring - Track hash collision and uniqueness
-- ============================================================================
CREATE OR REPLACE VIEW CLAUDE_BI.MCP.ID_STABILITY_MONITOR AS
WITH id_stats AS (
  SELECT 
    COUNT(*) AS total_events,
    COUNT(DISTINCT event_id) AS unique_ids,
    COUNT(DISTINCT attributes:_meta.content_hash::STRING) AS unique_content_hashes,
    -- Perfect uniqueness = 1.0
    COUNT(DISTINCT event_id)::FLOAT / NULLIF(COUNT(*), 0) AS uniqueness_ratio,
    -- Track events with SHA2 IDs (64 char hex)
    SUM(CASE WHEN LENGTH(event_id) = 64 THEN 1 ELSE 0 END) AS sha2_ids,
    SUM(CASE WHEN LENGTH(event_id) != 64 THEN 1 ELSE 0 END) AS legacy_ids
  FROM CLAUDE_BI.ACTIVITY.EVENTS
  WHERE occurred_at >= DATEADD('day', -7, CURRENT_TIMESTAMP())
),
collision_check AS (
  SELECT 
    event_id,
    COUNT(*) AS occurrences
  FROM CLAUDE_BI.ACTIVITY.EVENTS
  WHERE occurred_at >= DATEADD('day', -7, CURRENT_TIMESTAMP())
  GROUP BY event_id
  HAVING COUNT(*) > 1
)
SELECT 
  i.total_events,
  i.unique_ids,
  i.unique_content_hashes,
  ROUND(i.uniqueness_ratio, 4) AS uniqueness_ratio,
  i.sha2_ids,
  i.legacy_ids,
  COUNT(c.event_id) AS potential_collisions,
  CASE 
    WHEN i.uniqueness_ratio = 1.0 THEN 'PERFECT'
    WHEN i.uniqueness_ratio >= 0.999 THEN 'EXCELLENT'
    WHEN i.uniqueness_ratio >= 0.99 THEN 'GOOD'
    ELSE 'INVESTIGATE'
  END AS stability_status
FROM id_stats i
LEFT JOIN collision_check c ON 1=1
GROUP BY ALL;

-- ============================================================================
-- Deduplication Metrics - Track duplicate detection and removal
-- ============================================================================
CREATE OR REPLACE VIEW CLAUDE_BI.MCP.DEDUP_METRICS AS
WITH hourly_dedup AS (
  SELECT 
    DATE_TRUNC('hour', _recv_at) AS hour,
    COUNT(*) AS raw_events,
    COUNT(DISTINCT attributes:_meta.content_hash::STRING) AS unique_content,
    MAX(attributes:_meta.dedupe_rank::NUMBER) AS max_dedupe_rank,
    -- Calculate dedup rate
    (COUNT(*) - COUNT(DISTINCT attributes:_meta.content_hash::STRING))::FLOAT / 
      NULLIF(COUNT(*), 0) * 100 AS dedup_percentage
  FROM CLAUDE_BI.ACTIVITY.EVENTS
  WHERE _recv_at >= DATEADD('day', -7, CURRENT_TIMESTAMP())
  GROUP BY 1
)
SELECT 
  hour,
  raw_events,
  unique_content,
  raw_events - unique_content AS duplicates_removed,
  ROUND(dedup_percentage, 2) AS dedup_percentage,
  max_dedupe_rank,
  CASE 
    WHEN dedup_percentage = 0 THEN 'NO_DUPLICATES'
    WHEN dedup_percentage < 1 THEN 'MINIMAL'
    WHEN dedup_percentage < 5 THEN 'NORMAL'
    WHEN dedup_percentage < 10 THEN 'ELEVATED'
    ELSE 'HIGH'
  END AS dedup_level
FROM hourly_dedup
ORDER BY hour DESC;

-- ============================================================================
-- Late Arrival Tracking - Monitor events arriving after their occurred_at
-- ============================================================================
CREATE OR REPLACE VIEW CLAUDE_BI.MCP.LATE_ARRIVAL_MONITOR AS
WITH arrival_stats AS (
  SELECT 
    DATE_TRUNC('hour', occurred_at) AS event_hour,
    AVG(attributes:_meta.arrival_lag_hours::NUMBER) AS avg_lag_hours,
    MAX(attributes:_meta.arrival_lag_hours::NUMBER) AS max_lag_hours,
    COUNT(*) AS event_count,
    SUM(CASE WHEN attributes:_meta.arrival_lag_hours::NUMBER > 0 THEN 1 ELSE 0 END) AS late_events,
    SUM(CASE WHEN attributes:_meta.arrival_lag_hours::NUMBER > 1 THEN 1 ELSE 0 END) AS very_late_events,
    SUM(CASE WHEN attributes:_meta.arrival_lag_hours::NUMBER > 24 THEN 1 ELSE 0 END) AS extremely_late_events
  FROM CLAUDE_BI.ACTIVITY.EVENTS
  WHERE occurred_at >= DATEADD('day', -7, CURRENT_TIMESTAMP())
  GROUP BY 1
)
SELECT 
  event_hour,
  event_count,
  late_events,
  very_late_events,
  extremely_late_events,
  ROUND(avg_lag_hours, 2) AS avg_lag_hours,
  max_lag_hours,
  ROUND(100.0 * late_events / NULLIF(event_count, 0), 2) AS late_percentage,
  CASE 
    WHEN max_lag_hours = 0 THEN 'REAL_TIME'
    WHEN max_lag_hours <= 1 THEN 'NEAR_REAL_TIME'
    WHEN max_lag_hours <= 24 THEN 'DELAYED'
    ELSE 'VERY_DELAYED'
  END AS timeliness_status
FROM arrival_stats
ORDER BY event_hour DESC;

-- ============================================================================
-- Dynamic Table Refresh Health - Monitor refresh lag and performance  
-- ============================================================================
CREATE OR REPLACE VIEW CLAUDE_BI.MCP.DYNAMIC_TABLE_HEALTH AS
SELECT 
  'EVENTS' AS table_name,
  '1 minute' AS target_lag,
  'DT_XS_WH' AS warehouse,
  COUNT(*) AS row_count,
  MIN(occurred_at) AS oldest_event,
  MAX(occurred_at) AS newest_event,
  DATEDIFF('second', MAX(occurred_at), CURRENT_TIMESTAMP()) AS seconds_behind_real_time,
  CASE 
    WHEN DATEDIFF('second', MAX(occurred_at), CURRENT_TIMESTAMP()) > 120 THEN 'WARNING'
    WHEN DATEDIFF('second', MAX(occurred_at), CURRENT_TIMESTAMP()) > 60 THEN 'ATTENTION'
    ELSE 'HEALTHY'
  END AS health_status
FROM CLAUDE_BI.ACTIVITY.EVENTS;

-- ============================================================================
-- Validation Failure Tracking - Monitor quality issues (from events)
-- ============================================================================
CREATE OR REPLACE VIEW CLAUDE_BI.MCP.VALIDATION_FAILURES AS
SELECT 
  DATE_TRUNC('hour', detected_at) AS hour,
  validation_status,
  COUNT(*) AS failure_count,
  MIN(detected_at) AS first_seen,
  MAX(detected_at) AS last_seen,
  ARRAY_AGG(DISTINCT _source_lane) WITHIN GROUP (ORDER BY _source_lane) AS affected_lanes,
  -- Sample error messages for debugging
  ANY_VALUE(error_message) AS sample_error,
  COUNT(DISTINCT affected_event_id) AS affected_events
FROM CLAUDE_BI.ACTIVITY.QUALITY_EVENTS
WHERE detected_at >= DATEADD('day', -7, CURRENT_TIMESTAMP())
  AND validation_status IS NOT NULL
GROUP BY 1, 2
ORDER BY hour DESC, failure_count DESC;

-- ============================================================================
-- Event Pipeline Metrics - End-to-end pipeline health
-- ============================================================================
CREATE OR REPLACE VIEW CLAUDE_BI.MCP.PIPELINE_METRICS AS
WITH raw_stats AS (
  SELECT 
    COUNT(*) AS raw_count,
    MIN(_recv_at) AS oldest_raw,
    MAX(_recv_at) AS newest_raw
  FROM CLAUDE_BI.LANDING.RAW_EVENTS
  WHERE _recv_at >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
),
event_stats AS (
  SELECT 
    COUNT(*) AS event_count,
    MIN(occurred_at) AS oldest_event,
    MAX(occurred_at) AS newest_event,
    COUNT(DISTINCT action) AS unique_actions,
    COUNT(DISTINCT actor_id) AS unique_actors
  FROM CLAUDE_BI.ACTIVITY.EVENTS
  WHERE occurred_at >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
    AND action NOT LIKE 'quality.%'  -- Exclude quality events from success count
),
quality_stats AS (
  SELECT 
    COUNT(*) AS quality_failures
  FROM CLAUDE_BI.ACTIVITY.EVENTS
  WHERE occurred_at >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
    AND action LIKE 'quality.%'
)
SELECT 
  r.raw_count,
  e.event_count,
  q.quality_failures,
  r.raw_count - e.event_count - q.quality_failures AS pending_or_filtered,
  ROUND(100.0 * e.event_count / NULLIF(r.raw_count, 0), 2) AS success_rate,
  ROUND(100.0 * q.quality_failures / NULLIF(r.raw_count, 0), 2) AS failure_rate,
  e.unique_actions,
  e.unique_actors,
  DATEDIFF('minute', e.newest_event, CURRENT_TIMESTAMP()) AS minutes_behind_real_time,
  r.newest_raw,
  e.newest_event,
  CASE 
    WHEN DATEDIFF('minute', e.newest_event, CURRENT_TIMESTAMP()) > 5 THEN 'DELAYED'
    WHEN q.quality_failures > r.raw_count * 0.01 THEN 'QUALITY_ISSUES'
    WHEN e.event_count < r.raw_count * 0.95 THEN 'FILTERING_HIGH'
    ELSE 'HEALTHY'
  END AS pipeline_status
FROM raw_stats r
CROSS JOIN event_stats e
CROSS JOIN quality_stats q;

-- ============================================================================
-- Namespace Compliance - Track reserved namespace violations
-- ============================================================================
CREATE OR REPLACE VIEW CLAUDE_BI.MCP.NAMESPACE_COMPLIANCE AS
WITH namespace_check AS (
  SELECT 
    action,
    source,
    SPLIT_PART(action, '.', 1) AS namespace,
    COUNT(*) AS event_count,
    MIN(occurred_at) AS first_seen,
    MAX(occurred_at) AS last_seen,
    CASE 
      WHEN action LIKE 'system.%' AND source != 'system' THEN 'VIOLATION'
      WHEN action LIKE 'mcp.%' AND source != 'mcp' THEN 'VIOLATION'
      WHEN action LIKE 'quality.%' AND source != 'quality' THEN 'VIOLATION'
      ELSE 'COMPLIANT'
    END AS compliance_status
  FROM CLAUDE_BI.ACTIVITY.EVENTS
  WHERE occurred_at >= DATEADD('day', -7, CURRENT_TIMESTAMP())
  GROUP BY 1, 2, 3
)
SELECT 
  namespace,
  COUNT(DISTINCT action) AS unique_actions,
  SUM(event_count) AS total_events,
  SUM(CASE WHEN compliance_status = 'VIOLATION' THEN event_count ELSE 0 END) AS violations,
  ROUND(100.0 * SUM(CASE WHEN compliance_status = 'COMPLIANT' THEN event_count ELSE 0 END) / 
    NULLIF(SUM(event_count), 0), 2) AS compliance_rate,
  LISTAGG(DISTINCT 
    CASE WHEN compliance_status = 'VIOLATION' 
    THEN action || ' (source: ' || source || ')' 
    END, '; '
  ) WITHIN GROUP (ORDER BY action) AS violation_details
FROM namespace_check
GROUP BY namespace
ORDER BY total_events DESC;

-- ============================================================================
-- Same-Millisecond Event Handling - Track micro-sequencing effectiveness
-- ============================================================================
CREATE OR REPLACE VIEW CLAUDE_BI.MCP.MICROSECOND_SEQUENCING AS
WITH same_ms_events AS (
  SELECT 
    occurred_at,
    COUNT(*) AS events_at_timestamp,
    COUNT(DISTINCT attributes:_meta.micro_sequence::NUMBER) AS unique_sequences,
    MIN(attributes:_meta.micro_sequence::NUMBER) AS min_sequence,
    MAX(attributes:_meta.micro_sequence::NUMBER) AS max_sequence,
    COUNT(DISTINCT event_id) AS unique_events
  FROM CLAUDE_BI.ACTIVITY.EVENTS
  WHERE occurred_at >= DATEADD('day', -1, CURRENT_TIMESTAMP())
  GROUP BY occurred_at
  HAVING COUNT(*) > 1
)
SELECT 
  DATE_TRUNC('hour', occurred_at) AS hour,
  COUNT(*) AS timestamps_with_collisions,
  SUM(events_at_timestamp) AS total_colliding_events,
  MAX(events_at_timestamp) AS max_events_per_timestamp,
  ROUND(AVG(events_at_timestamp), 2) AS avg_events_per_timestamp,
  SUM(CASE WHEN unique_sequences < events_at_timestamp THEN 1 ELSE 0 END) AS insufficient_sequencing,
  CASE 
    WHEN MAX(events_at_timestamp) <= 2 THEN 'LOW_CONCURRENCY'
    WHEN MAX(events_at_timestamp) <= 5 THEN 'MODERATE_CONCURRENCY'
    WHEN MAX(events_at_timestamp) <= 10 THEN 'HIGH_CONCURRENCY'
    ELSE 'VERY_HIGH_CONCURRENCY'
  END AS concurrency_level
FROM same_ms_events
GROUP BY 1
ORDER BY 1 DESC;

-- ============================================================================
-- Dependency Chain Health - Monitor events with dependencies
-- ============================================================================
CREATE OR REPLACE VIEW CLAUDE_BI.MCP.DEPENDENCY_CHAIN_HEALTH AS
WITH dependency_stats AS (
  SELECT 
    DATE_TRUNC('hour', occurred_at) AS hour,
    COUNT(*) AS total_events,
    SUM(CASE WHEN depends_on_event_id IS NOT NULL THEN 1 ELSE 0 END) AS events_with_deps,
    SUM(CASE 
      WHEN depends_on_event_id IS NOT NULL 
      AND NOT EXISTS (
        SELECT 1 FROM CLAUDE_BI.ACTIVITY.EVENTS parent 
        WHERE parent.event_id = e.depends_on_event_id
      ) THEN 1 ELSE 0 
    END) AS broken_dependencies
  FROM CLAUDE_BI.ACTIVITY.EVENTS e
  WHERE occurred_at >= DATEADD('day', -7, CURRENT_TIMESTAMP())
  GROUP BY 1
)
SELECT 
  hour,
  total_events,
  events_with_deps,
  broken_dependencies,
  ROUND(100.0 * events_with_deps / NULLIF(total_events, 0), 2) AS dependency_percentage,
  ROUND(100.0 * broken_dependencies / NULLIF(events_with_deps, 0), 2) AS broken_percentage,
  CASE 
    WHEN broken_dependencies > 0 THEN 'BROKEN_CHAINS'
    WHEN events_with_deps = 0 THEN 'NO_DEPENDENCIES'
    ELSE 'HEALTHY'
  END AS dependency_status
FROM dependency_stats
ORDER BY hour DESC;

-- ============================================================================
-- Grant permissions for monitoring views
-- ============================================================================
GRANT SELECT ON ALL VIEWS IN SCHEMA CLAUDE_BI.MCP TO ROLE MCP_ADMIN_ROLE;
GRANT SELECT ON VIEW CLAUDE_BI.MCP.PIPELINE_METRICS TO ROLE MCP_SERVICE_ROLE;
GRANT SELECT ON VIEW CLAUDE_BI.MCP.DYNAMIC_TABLE_HEALTH TO ROLE MCP_SERVICE_ROLE;
GRANT SELECT ON VIEW CLAUDE_BI.ACTIVITY.QUALITY_EVENTS TO ROLE MCP_SERVICE_ROLE;