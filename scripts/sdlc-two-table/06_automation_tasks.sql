-- ============================================================================
-- 06_automation_tasks.sql
-- SDLC Automation Tasks - Two-Table Law Compliant
-- SLA monitoring, snapshot generation, and health checks via scheduled tasks
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE CLAUDE_BI;
USE SCHEMA MCP;

-- ============================================================================
-- SLA Monitoring Procedure - Checks for breaches and auto-escalates
-- ============================================================================
CREATE OR REPLACE PROCEDURE SDLC_CHECK_SLA()
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
const SF = snowflake;

try {
  const results = {
    sla_breaches_found: 0,
    escalations_created: 0,
    warnings_issued: 0,
    checks_performed: []
  };
  
  // Define SLA rules (hours)
  const slaRules = {
    'p0': { status_limits: { 'new': 1, 'ready': 2, 'in_progress': 8, 'review': 4 }, total_limit: 24 },
    'p1': { status_limits: { 'new': 4, 'ready': 8, 'in_progress': 24, 'review': 8 }, total_limit: 72 },
    'p2': { status_limits: { 'new': 24, 'ready': 48, 'in_progress': 120, 'review': 24 }, total_limit: 336 },
    'p3': { status_limits: { 'new': 72, 'ready': 168, 'in_progress': 240, 'review': 48 }, total_limit: 720 }
  };
  
  // Check each severity level
  for (const [severity, rules] of Object.entries(slaRules)) {
    results.checks_performed.push(`Checking ${severity} items`);
    
    // Find items exceeding SLA
    const slaCheckSQL = `
      WITH status_duration AS (
        SELECT 
          w.work_id,
          w.title,
          w.status,
          w.severity,
          w.assignee_id,
          w.age_hours,
          -- Calculate time in current status
          DATEDIFF('hour', 
            COALESCE(
              (SELECT MAX(e.occurred_at) 
               FROM CLAUDE_BI.ACTIVITY.EVENTS e 
               WHERE e.action = 'sdlc.work.status' 
                 AND e.attributes:work_id::string = w.work_id
                 AND e.attributes:status::string = w.status),
              w.created_at
            ),
            CURRENT_TIMESTAMP()
          ) AS time_in_status_hours
        FROM CLAUDE_BI.MCP.VW_WORK_ITEMS w
        WHERE w.severity = ?
          AND w.status NOT IN ('done', 'cancelled')
      )
      SELECT 
        work_id,
        title,
        status,
        assignee_id,
        age_hours,
        time_in_status_hours,
        ? AS status_limit,
        ? AS total_limit
      FROM status_duration
      WHERE time_in_status_hours > ? OR age_hours > ?
    `;
    
    const statusLimit = rules.status_limits[Object.keys(rules.status_limits)[0]] || 24;  // Default
    const totalLimit = rules.total_limit;
    
    const slaStmt = SF.createStatement({
      sqlText: slaCheckSQL,
      binds: [severity, statusLimit, totalLimit, statusLimit, totalLimit]
    });
    const slaRS = slaStmt.execute();
    
    while (slaRS.next()) {
      const workId = slaRS.getColumnValue('WORK_ID');
      const title = slaRS.getColumnValue('TITLE');
      const status = slaRS.getColumnValue('STATUS');
      const assigneeId = slaRS.getColumnValue('ASSIGNEE_ID');
      const ageHours = slaRS.getColumnValue('AGE_HOURS');
      const timeInStatusHours = slaRS.getColumnValue('TIME_IN_STATUS_HOURS');
      
      results.sla_breaches_found++;
      
      // Determine breach type and severity
      let breachType, escalationLevel;
      if (ageHours > totalLimit) {
        breachType = 'total_age_exceeded';
        escalationLevel = 'critical';
      } else {
        breachType = 'status_time_exceeded';
        escalationLevel = severity === 'p0' ? 'critical' : 'warning';
      }
      
      // Create SLA breach event
      const breachPayload = {
        action: 'sdlc.sla.breach',
        work_id: workId,
        breach_type: breachType,
        escalation_level: escalationLevel,
        severity: severity,
        current_status: status,
        age_hours: ageHours,
        time_in_status_hours: timeInStatusHours,
        status_limit: statusLimit,
        total_limit: totalLimit,
        assignee_id: assigneeId,
        actor_id: 'sla_monitor',
        idempotency_key: `sla_breach_${workId}_${Date.now()}`,
        schema_version: '1.0.0'
      };
      
      const breachSQL = `CALL CLAUDE_BI.MCP.SDLC_UPSERT_EVENT_IDEMPOTENT(?)`;
      SF.createStatement({
        sqlText: breachSQL,
        binds: [breachPayload]
      }).execute();
      
      // Auto-escalate critical breaches
      if (escalationLevel === 'critical') {
        const escalationPayload = {
          action: 'sdlc.work.escalate',
          work_id: workId,
          escalation_reason: `SLA breach: ${breachType}`,
          original_assignee: assigneeId,
          escalation_level: escalationLevel,
          actor_id: 'sla_monitor',
          idempotency_key: `escalation_${workId}_${Date.now()}`,
          schema_version: '1.0.0'
        };
        
        const escalationSQL = `CALL CLAUDE_BI.MCP.SDLC_UPSERT_EVENT_IDEMPOTENT(?)`;
        SF.createStatement({
          sqlText: escalationSQL,
          binds: [escalationPayload]
        }).execute();
        
        results.escalations_created++;
      } else {
        results.warnings_issued++;
      }
    }
  }
  
  // Log SLA check completion
  const checkPayload = {
    action: 'sdlc.system.sla_check',
    breaches_found: results.sla_breaches_found,
    escalations_created: results.escalations_created,
    warnings_issued: results.warnings_issued,
    check_timestamp: new Date().toISOString(),
    actor_id: 'sla_monitor',
    idempotency_key: `sla_check_${Date.now()}`,
    schema_version: '1.0.0'
  };
  
  const logSQL = `CALL CLAUDE_BI.MCP.SDLC_UPSERT_EVENT_IDEMPOTENT(?)`;
  SF.createStatement({
    sqlText: logSQL,
    binds: [checkPayload]
  }).execute();
  
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
-- Daily Snapshot Generation - Creates summary events for reporting
-- ============================================================================
CREATE OR REPLACE PROCEDURE SDLC_GENERATE_DAILY_SNAPSHOT()
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
const SF = snowflake;

try {
  const today = new Date().toISOString().split('T')[0];
  const results = {
    snapshots_created: 0,
    snapshot_types: []
  };
  
  // 1. Overall SDLC metrics snapshot
  const metricsSQL = `
    SELECT 
      COUNT(CASE WHEN status = 'new' THEN 1 END) AS backlog_new,
      COUNT(CASE WHEN status = 'ready' THEN 1 END) AS ready_to_start,
      COUNT(CASE WHEN status = 'in_progress' THEN 1 END) AS work_in_progress,
      COUNT(CASE WHEN status = 'review' THEN 1 END) AS in_review,
      COUNT(CASE WHEN status = 'blocked' THEN 1 END) AS blocked,
      COUNT(CASE WHEN status = 'done' AND DATE(last_updated_at) = CURRENT_DATE()) AS completed_today,
      COUNT(*) AS total_work_items,
      AVG(CASE WHEN status NOT IN ('done', 'cancelled') THEN age_hours END) AS avg_age_hours_active,
      COUNT(CASE WHEN severity = 'p0' AND status NOT IN ('done', 'cancelled') THEN 1 END) AS critical_active
    FROM CLAUDE_BI.MCP.VW_WORK_ITEMS
  `;
  
  const metricsStmt = SF.createStatement({ sqlText: metricsSQL });
  const metricsRS = metricsStmt.execute();
  metricsRS.next();
  
  const metricsPayload = {
    action: 'sdlc.report.daily_metrics',
    snapshot_date: today,
    backlog_new: metricsRS.getColumnValue('BACKLOG_NEW'),
    ready_to_start: metricsRS.getColumnValue('READY_TO_START'),
    work_in_progress: metricsRS.getColumnValue('WORK_IN_PROGRESS'),
    in_review: metricsRS.getColumnValue('IN_REVIEW'),
    blocked: metricsRS.getColumnValue('BLOCKED'),
    completed_today: metricsRS.getColumnValue('COMPLETED_TODAY'),
    total_work_items: metricsRS.getColumnValue('TOTAL_WORK_ITEMS'),
    avg_age_hours_active: metricsRS.getColumnValue('AVG_AGE_HOURS_ACTIVE'),
    critical_active: metricsRS.getColumnValue('CRITICAL_ACTIVE'),
    actor_id: 'snapshot_generator',
    idempotency_key: `daily_metrics_${today}`,
    schema_version: '1.0.0'
  };
  
  const metricsWriteSQL = `CALL CLAUDE_BI.MCP.SDLC_UPSERT_EVENT_IDEMPOTENT(?)`;
  SF.createStatement({
    sqlText: metricsWriteSQL,
    binds: [metricsPayload]
  }).execute();
  
  results.snapshots_created++;
  results.snapshot_types.push('daily_metrics');
  
  // 2. Agent activity snapshot
  const agentSQL = `
    SELECT 
      COUNT(DISTINCT e.attributes:agent_id::string) AS active_agents,
      COUNT(CASE WHEN e.action = 'sdlc.agent.claim' THEN 1 END) AS claims_today,
      COUNT(CASE WHEN e.action = 'sdlc.work.done' AND e.actor_id LIKE '%agent%' THEN 1 END) AS completions_by_agents
    FROM CLAUDE_BI.ACTIVITY.EVENTS e
    WHERE DATE(e.occurred_at) = CURRENT_DATE()
      AND e.action LIKE 'sdlc.%'
  `;
  
  const agentStmt = SF.createStatement({ sqlText: agentSQL });
  const agentRS = agentStmt.execute();
  agentRS.next();
  
  const agentPayload = {
    action: 'sdlc.report.agent_activity',
    snapshot_date: today,
    active_agents: agentRS.getColumnValue('ACTIVE_AGENTS'),
    claims_today: agentRS.getColumnValue('CLAIMS_TODAY'),
    completions_by_agents: agentRS.getColumnValue('COMPLETIONS_BY_AGENTS'),
    actor_id: 'snapshot_generator',
    idempotency_key: `agent_activity_${today}`,
    schema_version: '1.0.0'
  };
  
  const agentWriteSQL = `CALL CLAUDE_BI.MCP.SDLC_UPSERT_EVENT_IDEMPOTENT(?)`;
  SF.createStatement({
    sqlText: agentWriteSQL,
    binds: [agentPayload]
  }).execute();
  
  results.snapshots_created++;
  results.snapshot_types.push('agent_activity');
  
  // 3. Quality metrics snapshot
  const qualitySQL = `
    SELECT 
      COUNT(*) AS items_analyzed,
      AVG(CASE WHEN rejection_count = 0 THEN 1 ELSE 0 END) AS first_time_success_rate,
      AVG(status_change_count) AS avg_status_changes
    FROM (
      SELECT 
        w.work_id,
        (SELECT COUNT(*) FROM CLAUDE_BI.ACTIVITY.EVENTS e
         WHERE e.action = 'sdlc.work.reject' 
           AND e.attributes:work_id::string = w.work_id) AS rejection_count,
        (SELECT COUNT(*) FROM CLAUDE_BI.ACTIVITY.EVENTS e
         WHERE e.action = 'sdlc.work.status' 
           AND e.attributes:work_id::string = w.work_id) AS status_change_count
      FROM CLAUDE_BI.MCP.VW_WORK_ITEMS w
      WHERE w.status = 'done'
        AND DATE(w.last_updated_at) >= DATEADD('day', -7, CURRENT_DATE())
    ) quality_data
  `;
  
  const qualityStmt = SF.createStatement({ sqlText: qualitySQL });
  const qualityRS = qualityStmt.execute();
  
  if (qualityRS.next() && qualityRS.getColumnValue('ITEMS_ANALYZED') > 0) {
    const qualityPayload = {
      action: 'sdlc.report.quality_metrics',
      snapshot_date: today,
      items_analyzed: qualityRS.getColumnValue('ITEMS_ANALYZED'),
      first_time_success_rate: qualityRS.getColumnValue('FIRST_TIME_SUCCESS_RATE'),
      avg_status_changes: qualityRS.getColumnValue('AVG_STATUS_CHANGES'),
      actor_id: 'snapshot_generator',
      idempotency_key: `quality_metrics_${today}`,
      schema_version: '1.0.0'
    };
    
    const qualityWriteSQL = `CALL CLAUDE_BI.MCP.SDLC_UPSERT_EVENT_IDEMPOTENT(?)`;
    SF.createStatement({
      sqlText: qualityWriteSQL,
      binds: [qualityPayload]
    }).execute();
    
    results.snapshots_created++;
    results.snapshot_types.push('quality_metrics');
  }
  
  return {
    result: 'ok',
    date: today,
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
-- Health Check Procedure - Validates system consistency
-- ============================================================================
CREATE OR REPLACE PROCEDURE SDLC_HEALTH_CHECK()
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
const SF = snowflake;

try {
  const results = {
    checks_performed: 0,
    issues_found: 0,
    warnings: [],
    critical_issues: []
  };
  
  // Check 1: Orphaned work items (no create event)
  results.checks_performed++;
  const orphanSQL = `
    SELECT w.work_id, w.title
    FROM CLAUDE_BI.MCP.VW_WORK_ITEMS w
    WHERE NOT EXISTS (
      SELECT 1 FROM CLAUDE_BI.ACTIVITY.EVENTS e
      WHERE e.action = 'sdlc.work.create'
        AND e.attributes:work_id::string = w.work_id
    )
    LIMIT 10
  `;
  
  const orphanStmt = SF.createStatement({ sqlText: orphanSQL });
  const orphanRS = orphanStmt.execute();
  
  while (orphanRS.next()) {
    results.issues_found++;
    results.critical_issues.push({
      type: 'orphaned_work_item',
      work_id: orphanRS.getColumnValue('WORK_ID'),
      message: 'Work item exists without create event'
    });
  }
  
  // Check 2: Stuck work items (no activity in 7+ days for active items)
  results.checks_performed++;
  const stuckSQL = `
    SELECT work_id, title, status, last_updated_at, age_hours
    FROM CLAUDE_BI.MCP.VW_WORK_ITEMS
    WHERE status NOT IN ('done', 'cancelled')
      AND last_updated_at <= DATEADD('day', -7, CURRENT_TIMESTAMP())
    ORDER BY age_hours DESC
    LIMIT 20
  `;
  
  const stuckStmt = SF.createStatement({ sqlText: stuckSQL });
  const stuckRS = stuckStmt.execute();
  
  while (stuckRS.next()) {
    results.issues_found++;
    const ageHours = stuckRS.getColumnValue('AGE_HOURS');
    const issueType = ageHours > 168 ? 'critical_issues' : 'warnings';  // 1 week threshold
    
    results[issueType].push({
      type: 'stuck_work_item',
      work_id: stuckRS.getColumnValue('WORK_ID'),
      status: stuckRS.getColumnValue('STATUS'),
      age_hours: ageHours,
      message: `No activity for ${Math.round(ageHours/24)} days`
    });
  }
  
  // Check 3: Circular dependencies
  results.checks_performed++;
  const circularSQL = `
    WITH RECURSIVE dep_chain AS (
      SELECT 
        e.attributes:work_id::string as work_id,
        e.attributes:depends_on_id::string as depends_on_id,
        1 as depth,
        e.attributes:work_id::string as original_work_id
      FROM CLAUDE_BI.ACTIVITY.EVENTS e
      WHERE e.action = 'sdlc.work.depends'
      
      UNION ALL
      
      SELECT 
        e.attributes:work_id::string,
        e.attributes:depends_on_id::string,
        dc.depth + 1,
        dc.original_work_id
      FROM dep_chain dc
      JOIN CLAUDE_BI.ACTIVITY.EVENTS e ON e.attributes:work_id::string = dc.depends_on_id
      WHERE e.action = 'sdlc.work.depends'
        AND dc.depth < 10
    )
    SELECT DISTINCT original_work_id as work_id
    FROM dep_chain
    WHERE work_id = depends_on_id
    LIMIT 5
  `;
  
  const circularStmt = SF.createStatement({ sqlText: circularSQL });
  const circularRS = circularStmt.execute();
  
  while (circularRS.next()) {
    results.issues_found++;
    results.critical_issues.push({
      type: 'circular_dependency',
      work_id: circularRS.getColumnValue('WORK_ID'),
      message: 'Circular dependency detected'
    });
  }
  
  // Check 4: Agent performance anomalies
  results.checks_performed++;
  const agentAnomaliesSQL = `
    SELECT agent_id, completion_rate, avg_cycle_time_hours
    FROM CLAUDE_BI.MCP.VW_AGENT_PERFORMANCE
    WHERE (completion_rate < 0.3 AND items_claimed >= 10)
       OR (avg_cycle_time_hours > 120 AND items_completed >= 5)
    LIMIT 10
  `;
  
  const agentStmt = SF.createStatement({ sqlText: agentAnomaliesSQL });
  const agentRS = agentStmt.execute();
  
  while (agentRS.next()) {
    results.issues_found++;
    const completionRate = agentRS.getColumnValue('COMPLETION_RATE');
    const cycleTime = agentRS.getColumnValue('AVG_CYCLE_TIME_HOURS');
    
    results.warnings.push({
      type: 'agent_performance_anomaly',
      agent_id: agentRS.getColumnValue('AGENT_ID'),
      completion_rate: completionRate,
      avg_cycle_time_hours: cycleTime,
      message: `Poor performance: ${Math.round(completionRate*100)}% completion, ${Math.round(cycleTime)}h avg cycle time`
    });
  }
  
  // Log health check results
  const healthPayload = {
    action: 'sdlc.system.health_check',
    checks_performed: results.checks_performed,
    issues_found: results.issues_found,
    critical_issues_count: results.critical_issues.length,
    warnings_count: results.warnings.length,
    check_timestamp: new Date().toISOString(),
    health_status: results.critical_issues.length > 0 ? 'CRITICAL' : 
                   results.warnings.length > 5 ? 'WARNING' : 'HEALTHY',
    actor_id: 'health_monitor',
    idempotency_key: `health_check_${Date.now()}`,
    schema_version: '1.0.0'
  };
  
  const healthSQL = `CALL CLAUDE_BI.MCP.SDLC_UPSERT_EVENT_IDEMPOTENT(?)`;
  SF.createStatement({
    sqlText: healthSQL,
    binds: [healthPayload]
  }).execute();
  
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
-- Create Scheduled Tasks (commented out - enable as needed)
-- ============================================================================

-- Task 1: SLA Monitoring (every 4 hours)
/*
CREATE TASK CLAUDE_BI.MCP.TASK_SLA_MONITORING
  WAREHOUSE = DT_XS_WH
  SCHEDULE = 'USING CRON 0 */4 * * * UTC'
  ALLOW_OVERLAPPING_EXECUTION = FALSE
  COMMENT = 'Monitor SLA breaches and auto-escalate critical items'
AS
  CALL CLAUDE_BI.MCP.SDLC_CHECK_SLA();
*/

-- Task 2: Daily Snapshots (every day at 6 AM UTC)
/*
CREATE TASK CLAUDE_BI.MCP.TASK_DAILY_SNAPSHOTS
  WAREHOUSE = DT_XS_WH
  SCHEDULE = 'USING CRON 0 6 * * * UTC'
  ALLOW_OVERLAPPING_EXECUTION = FALSE
  COMMENT = 'Generate daily metric snapshots for reporting'
AS
  CALL CLAUDE_BI.MCP.SDLC_GENERATE_DAILY_SNAPSHOT();
*/

-- Task 3: Health Checks (every 2 hours)
/*
CREATE TASK CLAUDE_BI.MCP.TASK_HEALTH_MONITORING
  WAREHOUSE = DT_XS_WH
  SCHEDULE = 'USING CRON 0 */2 * * * UTC'
  ALLOW_OVERLAPPING_EXECUTION = FALSE
  COMMENT = 'Monitor system health and data consistency'
AS
  CALL CLAUDE_BI.MCP.SDLC_HEALTH_CHECK();
*/

-- ============================================================================
-- Manual Task Management Commands (for operators)
-- ============================================================================

-- Enable all tasks
-- ALTER TASK CLAUDE_BI.MCP.TASK_SLA_MONITORING RESUME;
-- ALTER TASK CLAUDE_BI.MCP.TASK_DAILY_SNAPSHOTS RESUME;
-- ALTER TASK CLAUDE_BI.MCP.TASK_HEALTH_MONITORING RESUME;

-- Disable all tasks  
-- ALTER TASK CLAUDE_BI.MCP.TASK_SLA_MONITORING SUSPEND;
-- ALTER TASK CLAUDE_BI.MCP.TASK_DAILY_SNAPSHOTS SUSPEND;
-- ALTER TASK CLAUDE_BI.MCP.TASK_HEALTH_MONITORING SUSPEND;

-- Check task status
-- SHOW TASKS LIKE '%SDLC%' IN SCHEMA CLAUDE_BI.MCP;

-- View task history
-- SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY()) 
-- WHERE NAME LIKE '%SDLC%' 
-- ORDER BY SCHEDULED_TIME DESC LIMIT 10;

-- ============================================================================
-- Grant procedure permissions
-- ============================================================================
GRANT USAGE ON PROCEDURE SDLC_CHECK_SLA() TO ROLE MCP_ADMIN_ROLE;
GRANT USAGE ON PROCEDURE SDLC_GENERATE_DAILY_SNAPSHOT() TO ROLE MCP_ADMIN_ROLE;
GRANT USAGE ON PROCEDURE SDLC_HEALTH_CHECK() TO ROLE MCP_ADMIN_ROLE;

-- Allow users to check SLA status (read-only)
GRANT USAGE ON PROCEDURE SDLC_CHECK_SLA() TO ROLE MCP_USER_ROLE;

-- ============================================================================
-- END OF AUTOMATION TASKS
-- 
-- Next: 07_performance_tuning.sql - Search optimization and clustering
-- ============================================================================