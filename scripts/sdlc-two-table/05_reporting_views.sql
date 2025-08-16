-- ============================================================================
-- 05_reporting_views.sql
-- SDLC Dashboard and Executive Reporting Views - Two-Table Law Compliant
-- Advanced analytics and metrics over the event stream
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE CLAUDE_BI;
USE SCHEMA MCP;

-- ============================================================================
-- VW_SDLC_EXECUTIVE_DASHBOARD - High-level metrics for leadership
-- ============================================================================
CREATE OR REPLACE VIEW VW_SDLC_EXECUTIVE_DASHBOARD AS
WITH current_metrics AS (
  SELECT 
    COUNT(CASE WHEN status = 'new' THEN 1 END) AS backlog_new,
    COUNT(CASE WHEN status = 'backlog' THEN 1 END) AS backlog_ready,
    COUNT(CASE WHEN status = 'ready' THEN 1 END) AS ready_to_start,
    COUNT(CASE WHEN status = 'in_progress' THEN 1 END) AS work_in_progress,
    COUNT(CASE WHEN status = 'review' THEN 1 END) AS in_review,
    COUNT(CASE WHEN status = 'blocked' THEN 1 END) AS blocked,
    COUNT(CASE WHEN status = 'done' THEN 1 END) AS total_completed,
    COUNT(*) AS total_work_items,
    
    -- Age analysis
    AVG(CASE WHEN status NOT IN ('done', 'cancelled') THEN age_hours END) AS avg_age_hours_active,
    MAX(CASE WHEN status NOT IN ('done', 'cancelled') THEN age_hours END) AS oldest_active_hours,
    
    -- Priority distribution
    COUNT(CASE WHEN severity = 'p0' AND status NOT IN ('done', 'cancelled') THEN 1 END) AS critical_active,
    COUNT(CASE WHEN severity = 'p1' AND status NOT IN ('done', 'cancelled') THEN 1 END) AS high_active,
    
    -- Throughput (completed today)
    COUNT(CASE WHEN status = 'done' AND DATE(last_updated_at) = CURRENT_DATE() THEN 1 END) AS completed_today,
    
    -- Cycle time for completed items
    AVG(CASE WHEN status = 'done' THEN cycle_time_hours END) AS avg_cycle_time_hours
  FROM CLAUDE_BI.MCP.VW_WORK_ITEMS
),
recent_trends AS (
  SELECT 
    -- Completed in last 7 days by day
    DATE(e.occurred_at) AS completion_date,
    COUNT(*) AS items_completed,
    SUM(w.points) AS points_completed
  FROM CLAUDE_BI.ACTIVITY.EVENTS e
  JOIN CLAUDE_BI.MCP.VW_WORK_ITEMS w ON w.work_id = e.attributes:work_id::string
  WHERE e.action = 'sdlc.work.done'
    AND e.occurred_at >= DATEADD('day', -7, CURRENT_TIMESTAMP())
  GROUP BY DATE(e.occurred_at)
),
agent_activity AS (
  SELECT 
    COUNT(DISTINCT e.attributes:agent_id::string) AS active_agents_today,
    COUNT(*) AS agent_actions_today
  FROM CLAUDE_BI.ACTIVITY.EVENTS e
  WHERE e.action LIKE 'sdlc.agent.%'
    AND DATE(e.occurred_at) = CURRENT_DATE()
)
SELECT 
  -- Current state
  cm.backlog_new + cm.backlog_ready AS total_backlog,
  cm.ready_to_start,
  cm.work_in_progress,
  cm.in_review,
  cm.blocked,
  cm.total_completed,
  cm.total_work_items,
  
  -- Health metrics
  cm.avg_age_hours_active,
  cm.oldest_active_hours,
  cm.critical_active,
  cm.high_active,
  
  -- Velocity metrics
  cm.completed_today,
  cm.avg_cycle_time_hours,
  
  -- Trends (7-day average)
  (SELECT AVG(items_completed) FROM recent_trends) AS avg_daily_completion_7d,
  (SELECT AVG(points_completed) FROM recent_trends) AS avg_daily_points_7d,
  
  -- Agent activity
  aa.active_agents_today,
  aa.agent_actions_today,
  
  -- Health indicators
  CASE 
    WHEN cm.critical_active > 5 THEN 'CRITICAL'
    WHEN cm.blocked > cm.work_in_progress THEN 'BLOCKED'
    WHEN cm.avg_age_hours_active > 168 THEN 'AGING'  -- 1 week
    ELSE 'HEALTHY'
  END AS health_status,
  
  CURRENT_TIMESTAMP() AS dashboard_updated_at
FROM current_metrics cm
CROSS JOIN agent_activity aa;

-- ============================================================================
-- VW_VELOCITY_REPORT - Team velocity and throughput analysis  
-- ============================================================================
CREATE OR REPLACE VIEW VW_VELOCITY_REPORT AS
WITH weekly_completion AS (
  SELECT 
    DATE_TRUNC('week', e.occurred_at) AS week_start,
    COUNT(*) AS items_completed,
    SUM(COALESCE(w.points, 0)) AS points_completed,
    COUNT(DISTINCT e.attributes:work_id::string) AS unique_items,
    AVG(w.cycle_time_hours) AS avg_cycle_time_hours
  FROM CLAUDE_BI.ACTIVITY.EVENTS e
  JOIN CLAUDE_BI.MCP.VW_WORK_ITEMS w ON w.work_id = e.attributes:work_id::string
  WHERE e.action = 'sdlc.work.done'
    AND e.occurred_at >= DATEADD('week', -12, CURRENT_TIMESTAMP())  -- Last 12 weeks
  GROUP BY DATE_TRUNC('week', e.occurred_at)
),
velocity_stats AS (
  SELECT 
    AVG(points_completed) AS avg_weekly_points,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY points_completed) AS median_weekly_points,
    STDDEV(points_completed) AS stddev_weekly_points,
    AVG(items_completed) AS avg_weekly_items,
    AVG(avg_cycle_time_hours) AS overall_avg_cycle_time
  FROM weekly_completion
)
SELECT 
  wc.week_start,
  wc.items_completed,
  wc.points_completed,
  wc.avg_cycle_time_hours,
  
  -- Trend indicators
  LAG(wc.points_completed, 1) OVER (ORDER BY wc.week_start) AS previous_week_points,
  wc.points_completed - LAG(wc.points_completed, 1) OVER (ORDER BY wc.week_start) AS points_change,
  
  -- Compared to average
  vs.avg_weekly_points,
  wc.points_completed - vs.avg_weekly_points AS variance_from_avg,
  
  -- Performance rating
  CASE 
    WHEN wc.points_completed > vs.avg_weekly_points + vs.stddev_weekly_points THEN 'EXCELLENT'
    WHEN wc.points_completed > vs.avg_weekly_points THEN 'ABOVE_AVERAGE'
    WHEN wc.points_completed > vs.avg_weekly_points - vs.stddev_weekly_points THEN 'AVERAGE'
    ELSE 'BELOW_AVERAGE'
  END AS performance_rating
  
FROM weekly_completion wc
CROSS JOIN velocity_stats vs
ORDER BY wc.week_start DESC;

-- ============================================================================
-- VW_QUALITY_METRICS - Quality and rework analysis
-- ============================================================================
CREATE OR REPLACE VIEW VW_QUALITY_METRICS AS
WITH work_lifecycle AS (
  SELECT 
    w.work_id,
    w.type,
    w.severity,
    w.assignee_id,
    w.status,
    w.cycle_time_hours,
    
    -- Count status changes (indication of rework)
    (SELECT COUNT(*) FROM CLAUDE_BI.ACTIVITY.EVENTS e
     WHERE e.action = 'sdlc.work.status' 
       AND e.attributes:work_id::string = w.work_id) AS status_change_count,
       
    -- Count rejections
    (SELECT COUNT(*) FROM CLAUDE_BI.ACTIVITY.EVENTS e
     WHERE e.action = 'sdlc.work.reject'
       AND e.attributes:work_id::string = w.work_id) AS rejection_count,
       
    -- Count reassignments  
    (SELECT COUNT(*) FROM CLAUDE_BI.ACTIVITY.EVENTS e
     WHERE e.action = 'sdlc.work.assign'
       AND e.attributes:work_id::string = w.work_id) AS assignment_count,
       
    -- Time in each status
    (SELECT SUM(
       DATEDIFF('hour', 
         e1.occurred_at,
         COALESCE(
           LEAD(e1.occurred_at) OVER (ORDER BY e1.occurred_at),
           CURRENT_TIMESTAMP()
         )
       )
     )
     FROM CLAUDE_BI.ACTIVITY.EVENTS e1
     WHERE e1.action IN ('sdlc.work.status', 'sdlc.work.create')
       AND e1.attributes:work_id::string = w.work_id
       AND (e1.attributes:status::string = 'in_progress' OR e1.action = 'sdlc.work.create')
    ) AS time_in_progress_hours
    
  FROM CLAUDE_BI.MCP.VW_WORK_ITEMS w
  WHERE w.created_at >= DATEADD('day', -30, CURRENT_TIMESTAMP())  -- Last 30 days
),
quality_summary AS (
  SELECT 
    type,
    severity,
    COUNT(*) AS total_items,
    
    -- Rework indicators
    AVG(status_change_count) AS avg_status_changes,
    COUNT(CASE WHEN rejection_count > 0 THEN 1 END) AS items_with_rejections,
    COUNT(CASE WHEN assignment_count > 1 THEN 1 END) AS items_reassigned,
    
    -- Time metrics
    AVG(cycle_time_hours) AS avg_cycle_time,
    AVG(time_in_progress_hours) AS avg_time_in_progress,
    
    -- Quality score (lower rework = higher quality)
    AVG(
      CASE 
        WHEN rejection_count = 0 AND status_change_count <= 3 THEN 100
        WHEN rejection_count = 0 AND status_change_count <= 5 THEN 85
        WHEN rejection_count = 1 AND status_change_count <= 5 THEN 70
        WHEN rejection_count <= 2 AND status_change_count <= 7 THEN 50
        ELSE 25
      END
    ) AS quality_score
    
  FROM work_lifecycle
  GROUP BY type, severity
)
SELECT 
  qs.*,
  
  -- Quality ratings
  CASE 
    WHEN qs.quality_score >= 90 THEN 'EXCELLENT'
    WHEN qs.quality_score >= 80 THEN 'GOOD'
    WHEN qs.quality_score >= 70 THEN 'FAIR'
    WHEN qs.quality_score >= 60 THEN 'POOR'
    ELSE 'CRITICAL'
  END AS quality_rating,
  
  -- Rework percentage
  ROUND((qs.items_with_rejections::FLOAT / qs.total_items) * 100, 1) AS rework_percentage
  
FROM quality_summary qs
ORDER BY qs.quality_score ASC, qs.total_items DESC;

-- ============================================================================
-- VW_AGENT_LEADERBOARD - Agent performance rankings
-- ============================================================================
CREATE OR REPLACE VIEW VW_AGENT_LEADERBOARD AS
WITH agent_stats AS (
  SELECT 
    ap.agent_id,
    ap.items_claimed,
    ap.items_completed,
    ap.completion_rate,
    ap.avg_cycle_time_hours,
    ap.median_cycle_time_hours,
    ap.claims_last_7d,
    ap.completions_last_7d,
    
    -- Quality metrics
    (SELECT AVG(
       CASE 
         WHEN wh.event_type = 'sdlc.work.reject' THEN 0
         WHEN wh.event_type = 'sdlc.work.done' THEN 1
         ELSE NULL
       END
     ) FROM CLAUDE_BI.MCP.VW_WORK_HISTORY wh
     WHERE wh.actor_id = ap.agent_id
       AND wh.event_type IN ('sdlc.work.done', 'sdlc.work.reject')
       AND wh.occurred_at >= DATEADD('day', -30, CURRENT_TIMESTAMP())
    ) AS quality_score,
    
    -- Specialization analysis
    (SELECT LISTAGG(DISTINCT w.type, ', ') 
     FROM CLAUDE_BI.ACTIVITY.EVENTS e
     JOIN CLAUDE_BI.MCP.VW_WORK_ITEMS w ON w.work_id = e.attributes:work_id::string
     WHERE e.action = 'sdlc.work.done'
       AND e.actor_id = ap.agent_id
       AND e.occurred_at >= DATEADD('day', -30, CURRENT_TIMESTAMP())
    ) AS specializes_in,
    
    -- Productivity score (weighted combination of metrics)
    (ap.completion_rate * 50) + 
    (CASE WHEN ap.avg_cycle_time_hours <= 24 THEN 25 WHEN ap.avg_cycle_time_hours <= 48 THEN 15 ELSE 5 END) +
    (LEAST(ap.completions_last_7d * 5, 25)) AS productivity_score
    
  FROM CLAUDE_BI.MCP.VW_AGENT_PERFORMANCE ap
  WHERE ap.items_claimed >= 5  -- Only include agents with meaningful activity
),
rankings AS (
  SELECT 
    *,
    ROW_NUMBER() OVER (ORDER BY productivity_score DESC) AS overall_rank,
    ROW_NUMBER() OVER (ORDER BY completion_rate DESC) AS completion_rank,
    ROW_NUMBER() OVER (ORDER BY avg_cycle_time_hours ASC) AS speed_rank,
    ROW_NUMBER() OVER (ORDER BY completions_last_7d DESC) AS activity_rank
  FROM agent_stats
)
SELECT 
  agent_id,
  overall_rank,
  completion_rank,
  speed_rank,
  activity_rank,
  items_claimed,
  items_completed,
  ROUND(completion_rate * 100, 1) AS completion_rate_pct,
  ROUND(avg_cycle_time_hours, 1) AS avg_cycle_time_hours,
  claims_last_7d,
  completions_last_7d,
  COALESCE(ROUND(quality_score * 100, 1), 0) AS quality_score_pct,
  specializes_in,
  ROUND(productivity_score, 1) AS productivity_score,
  
  -- Performance badges
  CASE 
    WHEN overall_rank = 1 THEN '🏆 TOP_PERFORMER'
    WHEN completion_rate >= 0.9 THEN '✅ HIGH_RELIABILITY' 
    WHEN avg_cycle_time_hours <= 12 THEN '⚡ SPEED_DEMON'
    WHEN completions_last_7d >= 10 THEN '🔥 HIGH_VOLUME'
    ELSE '⭐ CONTRIBUTOR'
  END AS performance_badge
  
FROM rankings
ORDER BY overall_rank;

-- ============================================================================
-- VW_BOTTLENECK_ANALYSIS - Identify workflow bottlenecks
-- ============================================================================
CREATE OR REPLACE VIEW VW_BOTTLENECK_ANALYSIS AS
WITH status_transitions AS (
  SELECT 
    e.attributes:work_id::string AS work_id,
    e.attributes:status::string AS to_status,
    e.attributes:from_status::string AS from_status,
    e.occurred_at,
    LAG(e.occurred_at) OVER (
      PARTITION BY e.attributes:work_id::string 
      ORDER BY e.occurred_at
    ) AS previous_status_time
  FROM CLAUDE_BI.ACTIVITY.EVENTS e
  WHERE e.action = 'sdlc.work.status'
    AND e.occurred_at >= DATEADD('day', -30, CURRENT_TIMESTAMP())
),
status_durations AS (
  SELECT 
    from_status,
    to_status,
    work_id,
    DATEDIFF('hour', previous_status_time, occurred_at) AS duration_hours
  FROM status_transitions
  WHERE previous_status_time IS NOT NULL
    AND duration_hours > 0
    AND duration_hours < 8760  -- Less than 1 year (data quality filter)
),
bottleneck_metrics AS (
  SELECT 
    from_status,
    to_status,
    COUNT(*) AS transition_count,
    AVG(duration_hours) AS avg_duration_hours,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY duration_hours) AS median_duration_hours,
    PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY duration_hours) AS p90_duration_hours,
    MAX(duration_hours) AS max_duration_hours,
    
    -- Bottleneck score (high duration + high frequency = bigger bottleneck)
    (AVG(duration_hours) * COUNT(*)) AS bottleneck_score
  FROM status_durations
  GROUP BY from_status, to_status
  HAVING COUNT(*) >= 3  -- Only include transitions that happened at least 3 times
)
SELECT 
  from_status,
  to_status,
  transition_count,
  ROUND(avg_duration_hours, 1) AS avg_duration_hours,
  ROUND(median_duration_hours, 1) AS median_duration_hours,
  ROUND(p90_duration_hours, 1) AS p90_duration_hours,
  ROUND(max_duration_hours, 1) AS max_duration_hours,
  ROUND(bottleneck_score, 1) AS bottleneck_score,
  
  -- Bottleneck severity
  CASE 
    WHEN avg_duration_hours > 72 AND transition_count >= 10 THEN 'CRITICAL'
    WHEN avg_duration_hours > 48 AND transition_count >= 5 THEN 'HIGH'
    WHEN avg_duration_hours > 24 OR transition_count >= 15 THEN 'MEDIUM'
    ELSE 'LOW'
  END AS bottleneck_severity,
  
  -- Recommendations
  CASE 
    WHEN from_status = 'ready' AND avg_duration_hours > 48 THEN 'Need more development capacity'
    WHEN from_status = 'in_progress' AND avg_duration_hours > 72 THEN 'Work items too large or complex'
    WHEN from_status = 'review' AND avg_duration_hours > 24 THEN 'Review process needs acceleration'
    WHEN from_status = 'blocked' AND avg_duration_hours > 48 THEN 'Dependency management issues'
    ELSE 'Monitor for trends'
  END AS recommendation
  
FROM bottleneck_metrics
ORDER BY bottleneck_score DESC;

-- ============================================================================
-- VW_FORECAST_DASHBOARD - Predictive analytics for planning
-- ============================================================================
CREATE OR REPLACE VIEW VW_FORECAST_DASHBOARD AS
WITH historical_velocity AS (
  -- Last 8 weeks of velocity data
  SELECT 
    DATE_TRUNC('week', e.occurred_at) AS week_start,
    COUNT(*) AS items_completed,
    SUM(COALESCE(w.points, 3)) AS points_completed  -- Default 3 points for unestimated work
  FROM CLAUDE_BI.ACTIVITY.EVENTS e
  JOIN CLAUDE_BI.MCP.VW_WORK_ITEMS w ON w.work_id = e.attributes:work_id::string
  WHERE e.action = 'sdlc.work.done'
    AND e.occurred_at >= DATEADD('week', -8, CURRENT_TIMESTAMP())
  GROUP BY DATE_TRUNC('week', e.occurred_at)
),
velocity_stats AS (
  SELECT 
    AVG(points_completed) AS avg_weekly_velocity,
    STDDEV(points_completed) AS velocity_stddev,
    COUNT(*) AS weeks_of_data
  FROM historical_velocity
),
current_backlog AS (
  SELECT 
    COUNT(*) AS total_items,
    SUM(COALESCE(points, 3)) AS total_points,
    COUNT(CASE WHEN severity IN ('p0', 'p1') THEN 1 END) AS priority_items,
    SUM(CASE WHEN severity IN ('p0', 'p1') THEN COALESCE(points, 3) ELSE 0 END) AS priority_points
  FROM CLAUDE_BI.MCP.VW_WORK_ITEMS
  WHERE status IN ('new', 'backlog', 'ready')
)
SELECT 
  -- Current state
  cb.total_items AS backlog_items,
  cb.total_points AS backlog_points,
  cb.priority_items,
  cb.priority_points,
  
  -- Velocity metrics
  vs.avg_weekly_velocity,
  vs.velocity_stddev,
  vs.weeks_of_data,
  
  -- Forecasting (assuming current velocity continues)
  CASE 
    WHEN vs.avg_weekly_velocity > 0 
    THEN ROUND(cb.total_points / vs.avg_weekly_velocity, 1)
    ELSE NULL
  END AS weeks_to_clear_backlog,
  
  CASE 
    WHEN vs.avg_weekly_velocity > 0 
    THEN ROUND(cb.priority_points / vs.avg_weekly_velocity, 1)
    ELSE NULL  
  END AS weeks_to_clear_priority,
  
  -- Confidence intervals (based on velocity variance)
  CASE 
    WHEN vs.avg_weekly_velocity > 0 AND vs.velocity_stddev > 0
    THEN ROUND(cb.total_points / (vs.avg_weekly_velocity - vs.velocity_stddev), 1)
    ELSE NULL
  END AS pessimistic_weeks,
  
  CASE 
    WHEN vs.avg_weekly_velocity > 0 AND vs.velocity_stddev > 0
    THEN ROUND(cb.total_points / (vs.avg_weekly_velocity + vs.velocity_stddev), 1) 
    ELSE NULL
  END AS optimistic_weeks,
  
  -- Capacity planning
  ROUND(vs.avg_weekly_velocity * 4, 0) AS monthly_capacity_points,
  ROUND(vs.avg_weekly_velocity * 13, 0) AS quarterly_capacity_points,
  
  -- Health indicators
  CASE 
    WHEN cb.priority_points > (vs.avg_weekly_velocity * 2) THEN 'PRIORITY_OVERLOAD'
    WHEN cb.total_points > (vs.avg_weekly_velocity * 12) THEN 'BACKLOG_OVERFLOW'
    WHEN vs.velocity_stddev > (vs.avg_weekly_velocity * 0.5) THEN 'VELOCITY_UNSTABLE'
    ELSE 'HEALTHY'
  END AS forecast_health,
  
  CURRENT_TIMESTAMP() AS forecast_updated_at
  
FROM current_backlog cb
CROSS JOIN velocity_stats vs;

-- ============================================================================
-- Grant view permissions
-- ============================================================================
GRANT SELECT ON VIEW VW_SDLC_EXECUTIVE_DASHBOARD TO ROLE MCP_USER_ROLE;
GRANT SELECT ON VIEW VW_VELOCITY_REPORT TO ROLE MCP_USER_ROLE;
GRANT SELECT ON VIEW VW_QUALITY_METRICS TO ROLE MCP_USER_ROLE;
GRANT SELECT ON VIEW VW_AGENT_LEADERBOARD TO ROLE MCP_USER_ROLE;
GRANT SELECT ON VIEW VW_BOTTLENECK_ANALYSIS TO ROLE MCP_USER_ROLE;
GRANT SELECT ON VIEW VW_FORECAST_DASHBOARD TO ROLE MCP_USER_ROLE;

-- ============================================================================
-- END OF REPORTING VIEWS
-- 
-- Next: 06_automation_tasks.sql - SLA monitoring and automated snapshots
-- ============================================================================