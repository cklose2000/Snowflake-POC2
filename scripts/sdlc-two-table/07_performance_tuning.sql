-- ============================================================================
-- 07_performance_tuning.sql
-- SDLC Performance Optimizations - Two-Table Law Compliant
-- Search optimization, clustering, and performance monitoring
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE CLAUDE_BI;
USE SCHEMA MCP;

-- ============================================================================
-- Search Optimization Service (SOS) Configuration
-- Optimizes the existing EVENTS table for SDLC query patterns
-- ============================================================================

-- Enable Search Optimization on the EVENTS table for SDLC queries
-- This creates internal search indexes without creating new tables
ALTER TABLE CLAUDE_BI.ACTIVITY.EVENTS ADD SEARCH OPTIMIZATION;

-- Add specific search optimization for SDLC action patterns
-- These optimize WHERE clauses commonly used in SDLC views
ALTER TABLE CLAUDE_BI.ACTIVITY.EVENTS ADD SEARCH OPTIMIZATION 
ON (action) WHERE action LIKE 'sdlc.%';

-- Optimize for work_id lookups (most common SDLC query pattern)
ALTER TABLE CLAUDE_BI.ACTIVITY.EVENTS ADD SEARCH OPTIMIZATION 
ON (attributes:work_id);

-- Optimize for actor_id lookups (agent performance queries)
ALTER TABLE CLAUDE_BI.ACTIVITY.EVENTS ADD SEARCH OPTIMIZATION 
ON (actor_id) WHERE action LIKE 'sdlc.%';

-- Optimize for occurred_at range queries (time-based analysis)
ALTER TABLE CLAUDE_BI.ACTIVITY.EVENTS ADD SEARCH OPTIMIZATION 
ON (occurred_at) WHERE action LIKE 'sdlc.%';

-- ============================================================================
-- Clustering Keys for EVENTS Table
-- Improves query performance by co-locating related data
-- ============================================================================

-- Set clustering key on the EVENTS table to optimize SDLC queries
-- This groups related events together on disk for faster scans
ALTER TABLE CLAUDE_BI.ACTIVITY.EVENTS CLUSTER BY (
  DATE_TRUNC('day', occurred_at),  -- Group by day for time-based queries
  action,                          -- Group by action type
  SUBSTRING(action, 1, 10)         -- Group by action prefix (e.g., 'sdlc.work')
);

-- ============================================================================
-- Performance Monitoring Views
-- Track query performance and optimization effectiveness
-- ============================================================================

-- View to monitor Search Optimization Service effectiveness
CREATE OR REPLACE VIEW VW_SDLC_SEARCH_OPTIMIZATION_STATS AS
SELECT 
  table_name,
  search_optimization_progress,
  search_optimization_bytes,
  active_predicates,
  last_refreshed
FROM TABLE(INFORMATION_SCHEMA.SEARCH_OPTIMIZATION_HISTORY(
  DATE_RANGE_START => DATEADD('day', -7, CURRENT_DATE()),
  TABLE_NAME => 'CLAUDE_BI.ACTIVITY.EVENTS'
))
ORDER BY last_refreshed DESC;

-- View to monitor clustering effectiveness
CREATE OR REPLACE VIEW VW_SDLC_CLUSTERING_STATS AS
WITH clustering_info AS (
  SELECT 
    table_name,
    clustering_key,
    total_micro_partitions,
    total_constant_micro_partitions,
    average_overlaps,
    average_depth,
    partition_depth_histogram
  FROM TABLE(INFORMATION_SCHEMA.AUTOMATIC_CLUSTERING_HISTORY(
    DATE_RANGE_START => DATEADD('day', -7, CURRENT_DATE()),
    TABLE_NAME => 'CLAUDE_BI.ACTIVITY.EVENTS'
  ))
)
SELECT 
  *,
  -- Clustering health score (0-100, higher is better)
  CASE 
    WHEN average_overlaps <= 5 AND average_depth <= 10 THEN 95
    WHEN average_overlaps <= 10 AND average_depth <= 20 THEN 85
    WHEN average_overlaps <= 20 AND average_depth <= 40 THEN 70
    WHEN average_overlaps <= 50 AND average_depth <= 80 THEN 50
    ELSE 25
  END AS clustering_health_score
FROM clustering_info
ORDER BY clustering_health_score DESC;

-- View to track SDLC query performance
CREATE OR REPLACE VIEW VW_SDLC_QUERY_PERFORMANCE AS
WITH sdlc_queries AS (
  SELECT 
    qh.query_id,
    qh.query_text,
    qh.start_time,
    qh.end_time,
    qh.total_elapsed_time,
    qh.execution_status,
    qh.rows_produced,
    qh.bytes_scanned,
    qh.partitions_scanned,
    qh.partitions_total,
    qh.bytes_written_to_result,
    qh.compilation_time,
    qh.execution_time,
    qh.queued_provisioning_time,
    qh.queued_repair_time,
    qh.queued_overload_time,
    qh.transaction_blocked_time,
    -- Extract operation type from query
    CASE 
      WHEN CONTAINS(UPPER(qh.query_text), 'VW_WORK_ITEMS') THEN 'work_item_query'
      WHEN CONTAINS(UPPER(qh.query_text), 'VW_PRIORITY_QUEUE') THEN 'priority_queue'
      WHEN CONTAINS(UPPER(qh.query_text), 'VW_AGENT_PERFORMANCE') THEN 'agent_performance'
      WHEN CONTAINS(UPPER(qh.query_text), 'SDLC_CLAIM_NEXT') THEN 'claim_next'
      WHEN CONTAINS(UPPER(qh.query_text), 'SDLC_STATUS') THEN 'status_update'
      WHEN CONTAINS(UPPER(qh.query_text), 'VW_WORK_HISTORY') THEN 'work_history'
      ELSE 'other_sdlc'
    END AS operation_type
  FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY qh
  WHERE qh.start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
    AND (
      CONTAINS(UPPER(qh.query_text), 'SDLC') OR
      CONTAINS(UPPER(qh.query_text), 'VW_WORK') OR
      CONTAINS(UPPER(qh.query_text), 'VW_AGENT') OR
      CONTAINS(UPPER(qh.query_text), 'VW_PRIORITY')
    )
    AND qh.execution_status = 'SUCCESS'
)
SELECT 
  operation_type,
  COUNT(*) AS query_count,
  AVG(total_elapsed_time) AS avg_elapsed_ms,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_elapsed_time) AS median_elapsed_ms,
  PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY total_elapsed_time) AS p95_elapsed_ms,
  MAX(total_elapsed_time) AS max_elapsed_ms,
  AVG(bytes_scanned) AS avg_bytes_scanned,
  AVG(partitions_scanned::FLOAT / NULLIF(partitions_total, 0)) AS avg_partition_scan_ratio,
  COUNT(CASE WHEN total_elapsed_time > 10000 THEN 1 END) AS slow_queries_10s_plus,
  COUNT(CASE WHEN total_elapsed_time > 30000 THEN 1 END) AS slow_queries_30s_plus
FROM sdlc_queries
GROUP BY operation_type
ORDER BY avg_elapsed_ms DESC;

-- ============================================================================
-- Performance Optimization Recommendations
-- ============================================================================

-- View that provides optimization recommendations based on query patterns
CREATE OR REPLACE VIEW VW_SDLC_OPTIMIZATION_RECOMMENDATIONS AS
WITH performance_analysis AS (
  SELECT 
    'search_optimization' AS optimization_type,
    'Enable SOS on additional columns' AS recommendation,
    CASE 
      WHEN (SELECT COUNT(*) FROM VW_SDLC_SEARCH_OPTIMIZATION_STATS) = 0 THEN 'HIGH'
      WHEN (SELECT AVG(search_optimization_progress) FROM VW_SDLC_SEARCH_OPTIMIZATION_STATS) < 80 THEN 'MEDIUM'
      ELSE 'LOW'
    END AS priority,
    'Search Optimization Service not fully utilized' AS reason
    
  UNION ALL
  
  SELECT 
    'clustering',
    'Improve table clustering',
    CASE 
      WHEN (SELECT AVG(clustering_health_score) FROM VW_SDLC_CLUSTERING_STATS) < 70 THEN 'HIGH'
      WHEN (SELECT AVG(clustering_health_score) FROM VW_SDLC_CLUSTERING_STATS) < 85 THEN 'MEDIUM'
      ELSE 'LOW'
    END,
    'Clustering health score indicates room for improvement'
    
  UNION ALL
  
  SELECT 
    'query_optimization',
    'Optimize slow SDLC queries',
    CASE 
      WHEN (SELECT COUNT(*) FROM VW_SDLC_QUERY_PERFORMANCE WHERE avg_elapsed_ms > 5000) > 0 THEN 'HIGH'
      WHEN (SELECT COUNT(*) FROM VW_SDLC_QUERY_PERFORMANCE WHERE avg_elapsed_ms > 2000) > 0 THEN 'MEDIUM'
      ELSE 'LOW'
    END,
    'Some SDLC operations are running slowly'
    
  UNION ALL
  
  SELECT 
    'result_caching',
    'Enable result caching for dashboard queries',
    CASE 
      WHEN (SELECT COUNT(*) FROM VW_SDLC_QUERY_PERFORMANCE WHERE operation_type LIKE '%performance%' AND query_count > 100) > 0 THEN 'MEDIUM'
      ELSE 'LOW'
    END,
    'Frequently repeated queries could benefit from result caching'
)
SELECT 
  optimization_type,
  recommendation,
  priority,
  reason,
  -- Implementation SQL
  CASE optimization_type
    WHEN 'search_optimization' THEN 'ALTER TABLE CLAUDE_BI.ACTIVITY.EVENTS ADD SEARCH OPTIMIZATION ON (attributes:sprint_id);'
    WHEN 'clustering' THEN 'ALTER TABLE CLAUDE_BI.ACTIVITY.EVENTS RECLUSTER;'
    WHEN 'query_optimization' THEN 'Review and optimize views with window functions'
    WHEN 'result_caching' THEN 'ALTER SESSION SET USE_CACHED_RESULT = TRUE;'
    ELSE 'Custom optimization required'
  END AS implementation_sql
FROM performance_analysis
WHERE priority IN ('HIGH', 'MEDIUM')
ORDER BY 
  CASE priority WHEN 'HIGH' THEN 1 WHEN 'MEDIUM' THEN 2 ELSE 3 END,
  optimization_type;

-- ============================================================================
-- Materialized View Alternative (Using Events, Not Tables!)
-- Since we can't create tables, we use scheduled snapshot events instead
-- ============================================================================

-- Procedure to create performance-optimized snapshot events
CREATE OR REPLACE PROCEDURE SDLC_CREATE_PERFORMANCE_SNAPSHOTS()
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
const SF = snowflake;

try {
  const results = {
    snapshots_created: 0,
    snapshot_types: []
  };
  
  // 1. Work items snapshot (current state as event)
  const workItemsSQL = `
    SELECT 
      work_id,
      title,
      type,
      severity,
      status,
      assignee_id,
      points,
      sprint_id,
      created_at,
      last_updated_at,
      age_hours,
      priority_score
    FROM CLAUDE_BI.MCP.VW_WORK_ITEMS
  `;
  
  const workItemsStmt = SF.createStatement({ sqlText: workItemsSQL });
  const workItemsRS = workItemsStmt.execute();
  
  const workItemsData = [];
  while (workItemsRS.next()) {
    workItemsData.push({
      work_id: workItemsRS.getColumnValue('WORK_ID'),
      title: workItemsRS.getColumnValue('TITLE'),
      type: workItemsRS.getColumnValue('TYPE'),
      severity: workItemsRS.getColumnValue('SEVERITY'),
      status: workItemsRS.getColumnValue('STATUS'),
      assignee_id: workItemsRS.getColumnValue('ASSIGNEE_ID'),
      points: workItemsRS.getColumnValue('POINTS'),
      sprint_id: workItemsRS.getColumnValue('SPRINT_ID'),
      created_at: workItemsRS.getColumnValue('CREATED_AT'),
      last_updated_at: workItemsRS.getColumnValue('LAST_UPDATED_AT'),
      age_hours: workItemsRS.getColumnValue('AGE_HOURS'),
      priority_score: workItemsRS.getColumnValue('PRIORITY_SCORE')
    });
  }
  
  // Create snapshot event with all work items
  const snapshotPayload = {
    action: 'sdlc.snapshot.work_items',
    snapshot_timestamp: new Date().toISOString(),
    total_items: workItemsData.length,
    work_items: workItemsData,
    actor_id: 'performance_optimizer',
    idempotency_key: `work_items_snapshot_${Date.now()}`,
    schema_version: '1.0.0'
  };
  
  const snapshotSQL = `CALL CLAUDE_BI.MCP.SDLC_UPSERT_EVENT_IDEMPOTENT(?)`;
  SF.createStatement({
    sqlText: snapshotSQL,
    binds: [snapshotPayload]
  }).execute();
  
  results.snapshots_created++;
  results.snapshot_types.push('work_items');
  
  // 2. Agent performance snapshot
  const agentPerfSQL = `
    SELECT 
      agent_id,
      items_claimed,
      items_completed,
      completion_rate,
      avg_cycle_time_hours,
      claims_last_7d,
      completions_last_7d
    FROM CLAUDE_BI.MCP.VW_AGENT_PERFORMANCE
  `;
  
  const agentPerfStmt = SF.createStatement({ sqlText: agentPerfSQL });
  const agentPerfRS = agentPerfStmt.execute();
  
  const agentData = [];
  while (agentPerfRS.next()) {
    agentData.push({
      agent_id: agentPerfRS.getColumnValue('AGENT_ID'),
      items_claimed: agentPerfRS.getColumnValue('ITEMS_CLAIMED'),
      items_completed: agentPerfRS.getColumnValue('ITEMS_COMPLETED'),
      completion_rate: agentPerfRS.getColumnValue('COMPLETION_RATE'),
      avg_cycle_time_hours: agentPerfRS.getColumnValue('AVG_CYCLE_TIME_HOURS'),
      claims_last_7d: agentPerfRS.getColumnValue('CLAIMS_LAST_7D'),
      completions_last_7d: agentPerfRS.getColumnValue('COMPLETIONS_LAST_7D')
    });
  }
  
  const agentSnapshotPayload = {
    action: 'sdlc.snapshot.agent_performance',
    snapshot_timestamp: new Date().toISOString(),
    total_agents: agentData.length,
    agent_performance: agentData,
    actor_id: 'performance_optimizer',
    idempotency_key: `agent_perf_snapshot_${Date.now()}`,
    schema_version: '1.0.0'
  };
  
  SF.createStatement({
    sqlText: snapshotSQL,
    binds: [agentSnapshotPayload]
  }).execute();
  
  results.snapshots_created++;
  results.snapshot_types.push('agent_performance');
  
  return {
    result: 'ok',
    summary: results
  };
  
} catch (err) {
  return {
    result: 'error',
    error: err.toString()
  };
}
$$;

-- ============================================================================
-- Query Optimization Hints and Best Practices
-- ============================================================================

-- Create a view with optimization hints for common SDLC query patterns
CREATE OR REPLACE VIEW VW_SDLC_QUERY_OPTIMIZATION_GUIDE AS
SELECT 
  'work_item_lookup' AS query_pattern,
  'Fast lookup of specific work items' AS description,
  'Use work_id in WHERE clause with exact match' AS optimization_hint,
  'SELECT * FROM VW_WORK_ITEMS WHERE work_id = ?' AS example_query,
  'Leverages search optimization on attributes:work_id' AS why_fast
  
UNION ALL

SELECT 
  'priority_queue_scan',
  'Get next available work for agents',
  'Use LIMIT to restrict results, ORDER BY priority_score DESC',
  'SELECT * FROM VW_PRIORITY_QUEUE WHERE is_available = TRUE ORDER BY priority_score DESC LIMIT 10',
  'Clustering by action helps filter SDLC events efficiently'
  
UNION ALL

SELECT 
  'agent_performance_analysis',
  'Analyze agent metrics and trends',
  'Filter by date ranges to limit scan, use agent_id for specific agents',
  'SELECT * FROM VW_AGENT_PERFORMANCE WHERE agent_id = ? AND first_claim_at >= ?',
  'Search optimization on actor_id speeds up agent-specific queries'
  
UNION ALL

SELECT 
  'work_history_audit',
  'Get complete history for work item',
  'Always include work_id filter, use event sequence for ordering',
  'SELECT * FROM VW_WORK_HISTORY WHERE work_id = ? ORDER BY event_sequence',
  'Clustering groups related events together on disk'
  
UNION ALL

SELECT 
  'time_range_analytics',
  'Reports over time periods (velocity, burndown)',
  'Use DATE_TRUNC and explicit date ranges, avoid open-ended queries',
  'SELECT DATE_TRUNC(''week'', occurred_at), COUNT(*) FROM EVENTS WHERE occurred_at >= ? AND action = ?',
  'Clustering by date improves time-based scans'
  
UNION ALL

SELECT 
  'bulk_status_updates',
  'Update multiple work items efficiently',
  'Use procedures instead of individual SQL statements',
  'CALL SDLC_STATUS(work_id, new_status, expected_last_event_id, idempotency_key, actor_id)',
  'Procedures handle concurrency and validation efficiently';

-- ============================================================================
-- Performance Testing Utilities
-- ============================================================================

-- Procedure to run performance tests on SDLC operations
CREATE OR REPLACE PROCEDURE SDLC_PERFORMANCE_TEST(test_type STRING, iterations NUMBER DEFAULT 10)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
const SF = snowflake;

try {
  const testType = TEST_TYPE.toLowerCase();
  const iterations = ITERATIONS || 10;
  const results = {
    test_type: testType,
    iterations: iterations,
    timings: [],
    avg_ms: 0,
    min_ms: 0,
    max_ms: 0
  };
  
  for (let i = 0; i < iterations; i++) {
    const startTime = Date.now();
    
    switch (testType) {
      case 'work_item_lookup':
        // Test work item lookup performance
        SF.createStatement({
          sqlText: 'SELECT COUNT(*) FROM CLAUDE_BI.MCP.VW_WORK_ITEMS WHERE work_id LIKE \'WORK-%\''
        }).execute();
        break;
        
      case 'priority_queue':
        // Test priority queue performance
        SF.createStatement({
          sqlText: 'SELECT * FROM CLAUDE_BI.MCP.VW_PRIORITY_QUEUE WHERE is_available = TRUE ORDER BY priority_score DESC LIMIT 10'
        }).execute();
        break;
        
      case 'agent_performance':
        // Test agent performance view
        SF.createStatement({
          sqlText: 'SELECT * FROM CLAUDE_BI.MCP.VW_AGENT_PERFORMANCE ORDER BY completion_rate DESC LIMIT 10'
        }).execute();
        break;
        
      case 'work_history':
        // Test work history lookup (if any work exists)
        const workRS = SF.createStatement({
          sqlText: 'SELECT work_id FROM CLAUDE_BI.MCP.VW_WORK_ITEMS LIMIT 1'
        }).execute();
        
        if (workRS.next()) {
          const workId = workRS.getColumnValue('WORK_ID');
          SF.createStatement({
            sqlText: 'SELECT * FROM CLAUDE_BI.MCP.VW_WORK_HISTORY WHERE work_id = ?',
            binds: [workId]
          }).execute();
        }
        break;
        
      default:
        throw new Error(`Unknown test type: ${testType}`);
    }
    
    const endTime = Date.now();
    const duration = endTime - startTime;
    results.timings.push(duration);
  }
  
  // Calculate statistics
  results.avg_ms = results.timings.reduce((a, b) => a + b, 0) / results.timings.length;
  results.min_ms = Math.min(...results.timings);
  results.max_ms = Math.max(...results.timings);
  
  return {
    result: 'ok',
    performance_test: results
  };
  
} catch (err) {
  return {
    result: 'error',
    error: err.toString()
  };
}
$$;

-- ============================================================================
-- Grant permissions for performance monitoring
-- ============================================================================
GRANT SELECT ON VIEW VW_SDLC_SEARCH_OPTIMIZATION_STATS TO ROLE MCP_ADMIN_ROLE;
GRANT SELECT ON VIEW VW_SDLC_CLUSTERING_STATS TO ROLE MCP_ADMIN_ROLE;
GRANT SELECT ON VIEW VW_SDLC_QUERY_PERFORMANCE TO ROLE MCP_ADMIN_ROLE;
GRANT SELECT ON VIEW VW_SDLC_OPTIMIZATION_RECOMMENDATIONS TO ROLE MCP_ADMIN_ROLE;
GRANT SELECT ON VIEW VW_SDLC_QUERY_OPTIMIZATION_GUIDE TO ROLE MCP_USER_ROLE;

GRANT USAGE ON PROCEDURE SDLC_CREATE_PERFORMANCE_SNAPSHOTS() TO ROLE MCP_ADMIN_ROLE;
GRANT USAGE ON PROCEDURE SDLC_PERFORMANCE_TEST(STRING, NUMBER) TO ROLE MCP_ADMIN_ROLE;

-- ============================================================================
-- Performance Monitoring Commands (for reference)
-- ============================================================================

-- Check search optimization status
-- SELECT * FROM VW_SDLC_SEARCH_OPTIMIZATION_STATS;

-- Check clustering health
-- SELECT * FROM VW_SDLC_CLUSTERING_STATS;

-- Analyze query performance
-- SELECT * FROM VW_SDLC_QUERY_PERFORMANCE;

-- Get optimization recommendations
-- SELECT * FROM VW_SDLC_OPTIMIZATION_RECOMMENDATIONS;

-- Run performance tests
-- CALL SDLC_PERFORMANCE_TEST('work_item_lookup', 20);
-- CALL SDLC_PERFORMANCE_TEST('priority_queue', 20);

-- Force reclustering (if needed)
-- ALTER TABLE CLAUDE_BI.ACTIVITY.EVENTS RECLUSTER;

-- ============================================================================
-- END OF PERFORMANCE TUNING
-- 
-- Next: 08_test_scenarios.sql - Comprehensive testing scenarios
-- ============================================================================