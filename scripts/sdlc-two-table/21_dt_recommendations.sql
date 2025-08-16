-- ============================================================================
-- 21_dt_recommendations.sql
-- Dynamic Table Analysis and Recommendations
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE CLAUDE_BI;
USE SCHEMA MCP;

-- ============================================================================
-- Current State Analysis
-- ============================================================================
/*
FINDINGS FROM INITIAL ASSESSMENT:
1. Row counts match perfectly (249 in both tables) ✅
2. Target lag is 1 minute ✅
3. Refresh mode is FULL (could be optimized) ⚠️
4. Dynamic Table is ACTIVE ✅
5. Warehouse: CLAUDE_AGENT_WH (XS size)
*/

-- ============================================================================
-- Recommendation 1: Switch to INCREMENTAL Refresh Mode
-- ============================================================================
-- Current issue: Using FULL refresh mode due to CURRENT_TIMESTAMP in predicate
-- Solution: Modify the Dynamic Table to support incremental refresh

-- Recommended DDL change:
/*
CREATE OR REPLACE DYNAMIC TABLE CLAUDE_BI.ACTIVITY.EVENTS
  TARGET_LAG = '1 minute'
  REFRESH_MODE = 'INCREMENTAL'  -- Change from AUTO/FULL
  WAREHOUSE = CLAUDE_AGENT_WH
AS
WITH base AS (
  SELECT
    -- Same transformation logic...
  FROM CLAUDE_BI.LANDING.RAW_EVENTS
  WHERE _recv_at >= '2024-01-01'::TIMESTAMP  -- Fixed date instead of DATEADD
    -- OR use stream markers for true incremental
),
-- Rest of query...
*/

-- ============================================================================
-- Recommendation 2: Optimize Deduplication Strategy
-- ============================================================================
CREATE OR REPLACE PROCEDURE OPTIMIZE_DT_DEDUP()
RETURNS STRING
LANGUAGE SQL
COMMENT = 'Analyze and recommend deduplication optimizations'
AS
$$
BEGIN
  -- Check duplicate patterns
  LET dup_analysis VARIANT := (
    SELECT OBJECT_CONSTRUCT(
      'total_rows', COUNT(*),
      'unique_keys', COUNT(DISTINCT DEDUPE_KEY),
      'duplicate_rows', COUNT(*) - COUNT(DISTINCT DEDUPE_KEY),
      'duplicate_percentage', ROUND(100.0 * (COUNT(*) - COUNT(DISTINCT DEDUPE_KEY)) / COUNT(*), 2)
    )
    FROM CLAUDE_BI.LANDING.RAW_EVENTS
    WHERE _RECV_AT >= DATEADD('day', -7, CURRENT_TIMESTAMP())
  );
  
  IF (:dup_analysis:duplicate_percentage > 10) THEN
    RETURN 'HIGH DUPLICATION: Consider adding unique constraints at ingestion or using MERGE instead of INSERT';
  ELSEIF (:dup_analysis:duplicate_percentage > 5) THEN
    RETURN 'MODERATE DUPLICATION: Current dedup strategy is working but monitor closely';
  ELSE
    RETURN 'LOW DUPLICATION: Dedup overhead may not be necessary for all events';
  END IF;
END;
$$;

-- ============================================================================
-- Recommendation 3: Performance Tuning
-- ============================================================================
CREATE OR REPLACE PROCEDURE ANALYZE_DT_PERFORMANCE()
RETURNS VARIANT
LANGUAGE SQL
COMMENT = 'Analyze Dynamic Table performance and provide recommendations'
AS
$$
DECLARE
  avg_events_per_minute FLOAT;
  peak_events_per_minute FLOAT;
  current_lag_setting INTEGER DEFAULT 60;
  recommended_lag INTEGER;
BEGIN
  -- Calculate event rates
  SELECT 
    AVG(event_count) as avg_rate,
    MAX(event_count) as peak_rate
  INTO :avg_events_per_minute, :peak_events_per_minute
  FROM (
    SELECT 
      DATE_TRUNC('minute', _RECV_AT) as minute,
      COUNT(*) as event_count
    FROM CLAUDE_BI.LANDING.RAW_EVENTS
    WHERE _RECV_AT >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
    GROUP BY 1
  );
  
  -- Determine recommended lag based on volume
  IF :peak_events_per_minute < 100 THEN
    SET recommended_lag = 60;  -- 1 minute for low volume
  ELSEIF :peak_events_per_minute < 1000 THEN
    SET recommended_lag = 30;  -- 30 seconds for medium volume
  ELSE
    SET recommended_lag = 10;  -- 10 seconds for high volume
  END IF;
  
  RETURN OBJECT_CONSTRUCT(
    'avg_events_per_minute', ROUND(:avg_events_per_minute, 2),
    'peak_events_per_minute', ROUND(:peak_events_per_minute, 2),
    'current_lag_seconds', :current_lag_setting,
    'recommended_lag_seconds', :recommended_lag,
    'recommendation', CASE 
      WHEN :recommended_lag < :current_lag_setting THEN 'REDUCE LAG for better real-time performance'
      WHEN :recommended_lag > :current_lag_setting THEN 'INCREASE LAG to reduce compute costs'
      ELSE 'CURRENT LAG SETTING IS OPTIMAL'
    END
  );
END;
$$;

-- ============================================================================
-- Recommendation 4: Data Retention Optimization
-- ============================================================================
CREATE OR REPLACE VIEW VW_DT_RETENTION_ANALYSIS AS
WITH age_distribution AS (
  SELECT 
    CASE 
      WHEN DATEDIFF('day', _RECV_AT, CURRENT_TIMESTAMP()) <= 1 THEN '0-1 days'
      WHEN DATEDIFF('day', _RECV_AT, CURRENT_TIMESTAMP()) <= 7 THEN '2-7 days'
      WHEN DATEDIFF('day', _RECV_AT, CURRENT_TIMESTAMP()) <= 30 THEN '8-30 days'
      ELSE 'Over 30 days'
    END as age_bucket,
    COUNT(*) as event_count,
    ROUND(BYTES / 1024.0 / 1024.0, 2) as size_mb
  FROM CLAUDE_BI.LANDING.RAW_EVENTS,
       (SELECT SUM(BYTES) as BYTES FROM INFORMATION_SCHEMA.TABLE_STORAGE_METRICS 
        WHERE TABLE_NAME = 'RAW_EVENTS' AND TABLE_SCHEMA = 'LANDING')
  GROUP BY 1, BYTES
)
SELECT 
  age_bucket,
  event_count,
  ROUND(100.0 * event_count / SUM(event_count) OVER(), 2) as percentage,
  size_mb * (event_count::FLOAT / SUM(event_count) OVER()) as estimated_size_mb,
  CASE 
    WHEN age_bucket = 'Over 30 days' THEN 'ARCHIVE OR DELETE - Not processed by DT'
    WHEN age_bucket = '8-30 days' THEN 'CONSIDER ARCHIVAL - Low query frequency'
    ELSE 'KEEP - Active data'
  END as recommendation
FROM age_distribution
ORDER BY 
  CASE age_bucket
    WHEN '0-1 days' THEN 1
    WHEN '2-7 days' THEN 2
    WHEN '8-30 days' THEN 3
    ELSE 4
  END;

-- ============================================================================
-- Recommendation 5: Monitoring and Alerting Setup
-- ============================================================================
CREATE OR REPLACE PROCEDURE SETUP_DT_MONITORING()
RETURNS STRING
LANGUAGE SQL
COMMENT = 'Set up automated monitoring for Dynamic Table health'
AS
$$
BEGIN
  -- Create monitoring task
  CREATE OR REPLACE TASK TASK_DT_HEALTH_CHECK
    WAREHOUSE = CLAUDE_AGENT_WH
    SCHEDULE = 'USING CRON */5 * * * * UTC'  -- Every 5 minutes
    COMMENT = 'Monitor Dynamic Table health and alert on issues'
  AS
    INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS (PAYLOAD, _SOURCE_LANE, _RECV_AT)
    SELECT 
      OBJECT_CONSTRUCT(
        'event_id', UUID_STRING(),
        'action', 'dt.monitor.health_check',
        'occurred_at', CURRENT_TIMESTAMP(),
        'actor_id', 'DT_MONITOR',
        'source', 'monitoring',
        'object', OBJECT_CONSTRUCT(
          'type', 'HEALTH_CHECK',
          'target', 'ACTIVITY.EVENTS'
        ),
        'attributes', OBJECT_CONSTRUCT(
          'health_status', health_status,
          'sync_status', sync_status,
          'pending_count', pending_promotion_count,
          'lag_seconds', seconds_since_refresh,
          'alerts', (SELECT ARRAY_AGG(OBJECT_CONSTRUCT(
            'type', alert_type,
            'severity', severity,
            'message', message
          )) FROM VW_DT_ALERTS)
        )
      ),
      'DT_MONITOR',
      CURRENT_TIMESTAMP()
    FROM VW_DT_HEALTH
    WHERE health_status != 'HEALTHY' 
       OR sync_status NOT IN ('SYNCHRONIZED', 'NEARLY_SYNCHRONIZED');
  
  -- Note: Task is created suspended
  RETURN 'Monitoring task created. Run ALTER TASK TASK_DT_HEALTH_CHECK RESUME to activate.';
END;
$$;

-- ============================================================================
-- Master Recommendations Report
-- ============================================================================
CREATE OR REPLACE PROCEDURE GENERATE_DT_RECOMMENDATIONS()
RETURNS VARIANT
LANGUAGE SQL
COMMENT = 'Generate comprehensive Dynamic Table recommendations report'
AS
$$
DECLARE
  recommendations ARRAY DEFAULT ARRAY_CONSTRUCT();
  health_status STRING;
  sync_status STRING;
  refresh_mode STRING;
  perf_analysis VARIANT;
  dedup_recommendation STRING;
BEGIN
  -- Get current health
  SELECT health_status, sync_status INTO :health_status, :sync_status
  FROM VW_DT_HEALTH;
  
  -- Get refresh mode
  SELECT REFRESH_MODE INTO :refresh_mode
  FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_GRAPH_HISTORY())
  WHERE QUALIFIED_NAME = 'CLAUDE_BI.ACTIVITY.EVENTS'
  LIMIT 1;
  
  -- Get performance analysis
  CALL ANALYZE_DT_PERFORMANCE() INTO :perf_analysis;
  
  -- Get dedup recommendation
  CALL OPTIMIZE_DT_DEDUP() INTO :dedup_recommendation;
  
  -- Build recommendations array
  
  -- 1. Refresh mode
  IF :refresh_mode = 'FULL' THEN
    LET recommendations := ARRAY_APPEND(:recommendations, OBJECT_CONSTRUCT(
      'priority', 'HIGH',
      'category', 'PERFORMANCE',
      'recommendation', 'Switch to INCREMENTAL refresh mode',
      'impact', 'Reduce compute costs by 50-80%',
      'implementation', 'Modify WHERE clause to avoid CURRENT_TIMESTAMP or use fixed date'
    ));
  END IF;
  
  -- 2. Lag optimization
  IF :perf_analysis:recommendation != 'CURRENT LAG SETTING IS OPTIMAL' THEN
    LET recommendations := ARRAY_APPEND(:recommendations, OBJECT_CONSTRUCT(
      'priority', 'MEDIUM',
      'category', 'PERFORMANCE',
      'recommendation', :perf_analysis:recommendation,
      'impact', 'Better balance between latency and cost',
      'implementation', 'ALTER DYNAMIC TABLE CLAUDE_BI.ACTIVITY.EVENTS SET TARGET_LAG = ''' || 
                        :perf_analysis:recommended_lag_seconds || ' seconds'''
    ));
  END IF;
  
  -- 3. Deduplication
  LET recommendations := ARRAY_APPEND(:recommendations, OBJECT_CONSTRUCT(
    'priority', 'LOW',
    'category', 'DATA_QUALITY',
    'recommendation', :dedup_recommendation,
    'impact', 'Reduce storage and processing overhead',
    'implementation', 'Review ingestion pipeline for duplicate prevention'
  ));
  
  -- 4. Monitoring
  IF :health_status != 'HEALTHY' OR :sync_status != 'SYNCHRONIZED' THEN
    LET recommendations := ARRAY_APPEND(:recommendations, OBJECT_CONSTRUCT(
      'priority', 'HIGH',
      'category', 'OPERATIONS',
      'recommendation', 'Enable automated health monitoring',
      'impact', 'Proactive issue detection and alerting',
      'implementation', 'CALL SETUP_DT_MONITORING(); ALTER TASK TASK_DT_HEALTH_CHECK RESUME;'
    ));
  END IF;
  
  -- 5. Data retention
  LET recommendations := ARRAY_APPEND(:recommendations, OBJECT_CONSTRUCT(
    'priority', 'MEDIUM',
    'category', 'STORAGE',
    'recommendation', 'Implement data archival for events older than 30 days',
    'impact', 'Reduce storage costs, improve query performance',
    'implementation', 'DELETE FROM LANDING.RAW_EVENTS WHERE _RECV_AT < DATEADD(''day'', -30, CURRENT_TIMESTAMP())'
  ));
  
  -- Return comprehensive report
  RETURN OBJECT_CONSTRUCT(
    'report_time', CURRENT_TIMESTAMP(),
    'current_state', OBJECT_CONSTRUCT(
      'health_status', :health_status,
      'sync_status', :sync_status,
      'refresh_mode', :refresh_mode,
      'performance_metrics', :perf_analysis
    ),
    'recommendations', :recommendations,
    'summary', 'Found ' || ARRAY_SIZE(:recommendations) || ' recommendations for optimization'
  );
END;
$$;

-- ============================================================================
-- Quick Health Check
-- ============================================================================
CREATE OR REPLACE FUNCTION DT_HEALTH_SCORE()
RETURNS INTEGER
LANGUAGE SQL
COMMENT = 'Return a health score from 0-100 for the Dynamic Table'
AS
$$
  SELECT 
    GREATEST(0, 
      100 
      - CASE WHEN health_status != 'HEALTHY' THEN 30 ELSE 0 END
      - CASE WHEN sync_status = 'SIGNIFICANT_LAG' THEN 20 
             WHEN sync_status = 'MINOR_LAG' THEN 10 
             ELSE 0 END
      - CASE WHEN pending_promotion_count > 100 THEN 20
             WHEN pending_promotion_count > 10 THEN 10
             ELSE 0 END
      - CASE WHEN max_lag_seconds > 120 THEN 15
             WHEN max_lag_seconds > 60 THEN 5
             ELSE 0 END
      - CASE WHEN active_alerts > 0 THEN active_alerts * 5 ELSE 0 END
    )::INTEGER as health_score
  FROM VW_DT_DASHBOARD
$$;

-- Grant permissions
GRANT USAGE ON PROCEDURE OPTIMIZE_DT_DEDUP() TO ROLE MCP_USER_ROLE;
GRANT USAGE ON PROCEDURE ANALYZE_DT_PERFORMANCE() TO ROLE MCP_USER_ROLE;
GRANT USAGE ON PROCEDURE SETUP_DT_MONITORING() TO ROLE MCP_USER_ROLE;
GRANT USAGE ON PROCEDURE GENERATE_DT_RECOMMENDATIONS() TO ROLE MCP_USER_ROLE;
GRANT USAGE ON FUNCTION DT_HEALTH_SCORE() TO ROLE MCP_USER_ROLE;
GRANT SELECT ON VIEW VW_DT_RETENTION_ANALYSIS TO ROLE MCP_USER_ROLE;

-- ============================================================================
-- Executive Summary
-- ============================================================================
/*
KEY FINDINGS:
1. ✅ Core functionality working - row counts match
2. ⚠️ Using FULL refresh mode - should switch to INCREMENTAL
3. ✅ Meeting 1-minute target lag
4. ✅ Deduplication working correctly
5. ✅ Data filtering working (NULL handling)

TOP RECOMMENDATIONS:
1. Switch to INCREMENTAL refresh mode (HIGH PRIORITY)
2. Enable automated monitoring (MEDIUM PRIORITY)
3. Implement 30-day retention policy (MEDIUM PRIORITY)
4. Optimize lag setting based on volume (LOW PRIORITY)

NEXT STEPS:
1. Run: CALL GENERATE_DT_RECOMMENDATIONS();
2. Review recommendations and implement changes
3. Enable monitoring: CALL SETUP_DT_MONITORING();
4. Schedule regular health checks using DT_HEALTH_SCORE()
*/