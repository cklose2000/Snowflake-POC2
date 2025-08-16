-- ============================================================================
-- 02_core_views_v2.sql  
-- HARDENED Core SDLC Views - Two-Table Law Compliant
-- Includes display_id resolution, priority_score calculation, and event_sequence
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE CLAUDE_BI; 
USE SCHEMA MCP;

-- ============================================================================
-- VW_WORK_ITEMS - Current state of all work items with display IDs
-- ============================================================================
CREATE OR REPLACE VIEW VW_WORK_ITEMS AS
WITH sdlc_events AS (
  -- Get all SDLC-related events
  SELECT 
    event_id,
    occurred_at,
    actor_id,
    action AS event_type,
    object_id,
    attributes,
    -- Extract work_id from different locations for backwards compatibility
    COALESCE(
      attributes:work_id::string,
      object_id,
      attributes:object_id::string
    ) AS work_id,
    -- Add row number for latest event determination
    ROW_NUMBER() OVER (
      PARTITION BY COALESCE(attributes:work_id::string, object_id, attributes:object_id::string)
      ORDER BY occurred_at DESC, event_id DESC
    ) AS rn_overall
  FROM CLAUDE_BI.ACTIVITY.EVENTS
  WHERE (action LIKE 'sdlc.work.%' 
     OR action LIKE 'sdlc.agent.%' 
     OR action LIKE 'sdlc.sprint.%')
    AND COALESCE(attributes:work_id::string, object_id, attributes:object_id::string) IS NOT NULL
),
work_items AS (
  -- Get unique work items
  SELECT DISTINCT work_id
  FROM sdlc_events
),
latest_values AS (
  -- For each work item, get the latest value for each attribute type
  SELECT 
    w.work_id,
    
    -- Display ID (from number event or create event)
    COALESCE(
      (SELECT se.attributes:display_id::string
       FROM sdlc_events se 
       WHERE se.work_id = w.work_id 
         AND se.event_type = 'sdlc.work.number'
       ORDER BY se.occurred_at DESC, se.event_id DESC
       LIMIT 1),
      (SELECT se.attributes:display_id::string
       FROM sdlc_events se 
       WHERE se.work_id = w.work_id 
         AND se.event_type = 'sdlc.work.create'
       ORDER BY se.occurred_at DESC, se.event_id DESC
       LIMIT 1),
      w.work_id  -- Fallback to work_id if no display_id
    ) AS display_id,
    
    -- Title (from create event)
    (SELECT se.attributes:title::string
     FROM sdlc_events se 
     WHERE se.work_id = w.work_id 
       AND se.event_type = 'sdlc.work.create'
     ORDER BY se.occurred_at DESC, se.event_id DESC
     LIMIT 1) AS title,
     
    -- Type (from create event) 
    (SELECT se.attributes:type::string
     FROM sdlc_events se
     WHERE se.work_id = w.work_id
       AND se.event_type = 'sdlc.work.create'
     ORDER BY se.occurred_at DESC, se.event_id DESC  
     LIMIT 1) AS type,
     
    -- Severity (from create event)
    (SELECT se.attributes:severity::string
     FROM sdlc_events se
     WHERE se.work_id = w.work_id
       AND se.event_type = 'sdlc.work.create'
     ORDER BY se.occurred_at DESC, se.event_id DESC
     LIMIT 1) AS severity,
     
    -- Description (from create event)
    (SELECT se.attributes:description::string
     FROM sdlc_events se
     WHERE se.work_id = w.work_id
       AND se.event_type = 'sdlc.work.create' 
     ORDER BY se.occurred_at DESC, se.event_id DESC
     LIMIT 1) AS description,
     
    -- Current status (from latest status-changing event)
    COALESCE(
      -- Check for done event first
      (SELECT 'done'
       FROM sdlc_events se
       WHERE se.work_id = w.work_id
         AND se.event_type = 'sdlc.work.done'
       LIMIT 1),
      -- Then check latest status event
      (SELECT se.attributes:status::string
       FROM sdlc_events se
       WHERE se.work_id = w.work_id
         AND se.event_type IN ('sdlc.work.status', 'sdlc.work.reject')
       ORDER BY se.occurred_at DESC, se.event_id DESC
       LIMIT 1),
      -- Default from create
      (SELECT COALESCE(se.attributes:status::string, 'new')
       FROM sdlc_events se
       WHERE se.work_id = w.work_id
         AND se.event_type = 'sdlc.work.create'
       ORDER BY se.occurred_at DESC, se.event_id DESC
       LIMIT 1),
      'new'  -- Final default
    ) AS status,
     
    -- Current assignee (from latest assign event)
    (SELECT se.attributes:assignee_id::string
     FROM sdlc_events se
     WHERE se.work_id = w.work_id
       AND se.event_type = 'sdlc.work.assign'
     ORDER BY se.occurred_at DESC, se.event_id DESC
     LIMIT 1) AS assignee_id,
     
    -- Assignee type
    (SELECT se.attributes:assignee_type::string
     FROM sdlc_events se
     WHERE se.work_id = w.work_id
       AND se.event_type = 'sdlc.work.assign'
     ORDER BY se.occurred_at DESC, se.event_id DESC
     LIMIT 1) AS assignee_type,
     
    -- Current points estimate
    (SELECT se.attributes:points::number
     FROM sdlc_events se
     WHERE se.work_id = w.work_id
       AND se.event_type IN ('sdlc.work.estimate', 'sdlc.work.reestimate')
     ORDER BY se.occurred_at DESC, se.event_id DESC
     LIMIT 1) AS points,
     
    -- Current sprint
    (SELECT se.attributes:sprint_id::string
     FROM sdlc_events se
     WHERE se.work_id = w.work_id
       AND se.event_type = 'sdlc.sprint.assign'
     ORDER BY se.occurred_at DESC, se.event_id DESC
     LIMIT 1) AS sprint_id,
     
    -- Reporter (who created it)
    (SELECT se.actor_id
     FROM sdlc_events se
     WHERE se.work_id = w.work_id
       AND se.event_type = 'sdlc.work.create'
     ORDER BY se.occurred_at DESC, se.event_id DESC
     LIMIT 1) AS reporter_id,
     
    -- Created timestamp
    (SELECT se.occurred_at
     FROM sdlc_events se
     WHERE se.work_id = w.work_id
       AND se.event_type = 'sdlc.work.create'
     ORDER BY se.occurred_at DESC, se.event_id DESC
     LIMIT 1) AS created_at,
     
    -- Last updated timestamp  
    (SELECT se.occurred_at
     FROM sdlc_events se
     WHERE se.work_id = w.work_id
     ORDER BY se.occurred_at DESC, se.event_id DESC
     LIMIT 1) AS last_updated_at,
     
    -- Last event ID for optimistic concurrency
    (SELECT se.event_id
     FROM sdlc_events se
     WHERE se.work_id = w.work_id
     ORDER BY se.occurred_at DESC, se.event_id DESC
     LIMIT 1) AS last_event_id,
     
    -- Business value (from create)
    (SELECT se.attributes:business_value::number
     FROM sdlc_events se
     WHERE se.work_id = w.work_id
       AND se.event_type = 'sdlc.work.create'
     ORDER BY se.occurred_at DESC, se.event_id DESC
     LIMIT 1) AS business_value,
     
    -- Customer impact flag
    (SELECT se.attributes:customer_impact::boolean
     FROM sdlc_events se
     WHERE se.work_id = w.work_id
       AND se.event_type = 'sdlc.work.create'
     ORDER BY se.occurred_at DESC, se.event_id DESC
     LIMIT 1) AS customer_impact,
     
    -- Check if blocked
    (SELECT COUNT(*) > 0
     FROM sdlc_events dep
     WHERE dep.event_type = 'sdlc.work.depends'
       AND dep.attributes:work_id::string = w.work_id
       AND dep.attributes:depends_on_id::string NOT IN (
         SELECT w2.work_id 
         FROM work_items w2
         JOIN sdlc_events done ON done.work_id = w2.work_id
         WHERE done.event_type = 'sdlc.work.done'
       )
    ) AS is_blocked
     
  FROM work_items w
)
SELECT 
  work_id,
  display_id,
  title,
  type,
  severity,
  description,
  status,
  assignee_id,
  assignee_type,
  points,
  sprint_id,
  reporter_id,
  created_at,
  last_updated_at,
  last_event_id,
  business_value,
  customer_impact,
  is_blocked,
  -- Calculated fields
  DATEDIFF('hour', created_at, CURRENT_TIMESTAMP()) AS age_hours,
  CASE 
    WHEN status IN ('done', 'cancelled') THEN DATEDIFF('hour', created_at, last_updated_at)
    ELSE DATEDIFF('hour', created_at, CURRENT_TIMESTAMP())
  END AS cycle_time_hours,
  -- Priority score calculation (higher = more urgent)
  CASE severity
    WHEN 'p0' THEN 1000
    WHEN 'p1' THEN 100  
    WHEN 'p2' THEN 10
    ELSE 1
  END + 
  COALESCE(business_value, 0) +
  CASE WHEN customer_impact THEN 50 ELSE 0 END +
  CASE WHEN is_blocked THEN -100 ELSE 0 END AS priority_score
FROM latest_values
WHERE work_id IS NOT NULL;

-- ============================================================================
-- VW_PRIORITY_QUEUE - Available work ordered by priority (updated)
-- ============================================================================
CREATE OR REPLACE VIEW VW_PRIORITY_QUEUE AS
WITH available_work AS (
  SELECT 
    w.*,
    -- Check if has active agent claim (not superseded)
    CASE WHEN EXISTS (
      SELECT 1 FROM CLAUDE_BI.ACTIVITY.EVENTS claim
      WHERE claim.action = 'sdlc.agent.claim'
        AND claim.attributes:work_id::string = w.work_id
        AND claim.occurred_at > DATEADD('hour', -1, CURRENT_TIMESTAMP())  -- Claims expire after 1 hour
        AND NOT EXISTS (
          -- Check for supersession or status change
          SELECT 1 FROM CLAUDE_BI.ACTIVITY.EVENTS override
          WHERE override.attributes:work_id::string = w.work_id
            AND override.action IN ('sdlc.work.status', 'sdlc.work.done', 'sdlc.agent.error', 'sdlc.agent.superseded', 'sdlc.work.assign')
            AND override.occurred_at > claim.occurred_at
        )
    ) THEN TRUE ELSE FALSE END AS has_active_claim
    
  FROM VW_WORK_ITEMS w
  WHERE w.status IN ('new', 'ready', 'backlog')
    AND w.assignee_id IS NULL  -- Not specifically assigned
)
SELECT 
  work_id,
  display_id,
  title,
  type,
  severity, 
  status,
  points,
  sprint_id,
  priority_score,
  age_hours,
  business_value,
  customer_impact,
  last_event_id,
  is_blocked,
  has_active_claim,
  -- Final availability
  CASE 
    WHEN is_blocked THEN FALSE
    WHEN has_active_claim THEN FALSE
    ELSE TRUE
  END AS is_available
FROM available_work
WHERE NOT is_blocked 
  AND NOT has_active_claim
ORDER BY 
  priority_score DESC,
  age_hours DESC,  -- Older work gets higher priority
  work_id;  -- Stable sort

-- ============================================================================
-- VW_WORK_HISTORY - Complete audit trail with event sequence
-- ============================================================================
CREATE OR REPLACE VIEW VW_WORK_HISTORY AS
SELECT 
  e.event_id,
  e.occurred_at,
  e.actor_id,
  e.action AS event_type,
  COALESCE(
    e.attributes:work_id::string,
    e.object_id,
    e.attributes:object_id::string
  ) AS work_id,
  -- Get display_id for this work item
  (SELECT display_id FROM VW_WORK_ITEMS w WHERE w.work_id = COALESCE(
    e.attributes:work_id::string,
    e.object_id,
    e.attributes:object_id::string
  )) AS display_id,
  e.attributes,
  -- Human-readable event description
  CASE e.action
    WHEN 'sdlc.work.create' THEN 'Work item created: ' || e.attributes:title::string
    WHEN 'sdlc.work.number' THEN 'Assigned display number: ' || e.attributes:display_id::string
    WHEN 'sdlc.work.assign' THEN 'Assigned to ' || e.attributes:assignee_id::string
    WHEN 'sdlc.work.status' THEN 'Status changed to ' || e.attributes:status::string
    WHEN 'sdlc.work.done' THEN 'Marked as done'
    WHEN 'sdlc.work.reject' THEN 'Rejected: ' || e.attributes:reason::string
    WHEN 'sdlc.work.estimate' THEN 'Estimated at ' || e.attributes:points::string || ' points'
    WHEN 'sdlc.work.reestimate' THEN 'Re-estimated to ' || e.attributes:points::string || ' points'
    WHEN 'sdlc.work.depends' THEN 'Depends on ' || e.attributes:depends_on_id::string
    WHEN 'sdlc.sprint.assign' THEN 'Added to sprint ' || e.attributes:sprint_id::string
    WHEN 'sdlc.agent.claim' THEN 'Claimed by agent ' || e.attributes:agent_id::string
    WHEN 'sdlc.agent.error' THEN 'Agent error: ' || e.attributes:error_type::string
    WHEN 'sdlc.agent.superseded' THEN 'Agent claim superseded by ' || e.attributes:new_assignee::string
    ELSE e.action
  END AS event_description,
  -- Event sequence within this work item
  ROW_NUMBER() OVER (
    PARTITION BY COALESCE(e.attributes:work_id::string, e.object_id, e.attributes:object_id::string)
    ORDER BY e.occurred_at, e.event_id
  ) AS event_sequence
FROM CLAUDE_BI.ACTIVITY.EVENTS e
WHERE (e.action LIKE 'sdlc.work.%' 
   OR e.action LIKE 'sdlc.agent.%' 
   OR e.action LIKE 'sdlc.sprint.%')
  AND COALESCE(e.attributes:work_id::string, e.object_id, e.attributes:object_id::string) IS NOT NULL
ORDER BY 
  COALESCE(e.attributes:work_id::string, e.object_id, e.attributes:object_id::string),
  e.occurred_at,
  e.event_id;

-- ============================================================================
-- VW_BLOCKED_WORK - Work items blocked by dependencies (updated)
-- ============================================================================
CREATE OR REPLACE VIEW VW_BLOCKED_WORK AS
WITH dependencies AS (
  SELECT 
    e.attributes:work_id::string AS work_id,
    e.attributes:depends_on_id::string AS depends_on_id,
    e.attributes:dependency_type::string AS dependency_type,
    e.attributes:dependency_reason::string AS dependency_reason,
    e.occurred_at AS dependency_created_at
  FROM CLAUDE_BI.ACTIVITY.EVENTS e
  WHERE e.action = 'sdlc.work.depends'
),
blocking_status AS (
  SELECT 
    d.work_id,
    d.depends_on_id,
    d.dependency_type,
    d.dependency_reason,
    d.dependency_created_at,
    w_blocking.status AS blocking_item_status,
    w_blocking.title AS blocking_item_title,
    w_blocking.display_id AS blocking_item_display_id,
    CASE 
      WHEN w_blocking.status = 'done' THEN FALSE
      ELSE TRUE
    END AS is_currently_blocked
  FROM dependencies d
  LEFT JOIN VW_WORK_ITEMS w_blocking ON w_blocking.work_id = d.depends_on_id
)
SELECT 
  bs.work_id,
  w.display_id,
  bs.depends_on_id,
  bs.blocking_item_display_id,
  bs.dependency_type,
  bs.dependency_reason,
  bs.dependency_created_at,
  bs.blocking_item_status,
  bs.blocking_item_title,
  bs.is_currently_blocked,
  w.title,
  w.status,
  w.assignee_id,
  w.severity,
  w.priority_score,
  DATEDIFF('day', bs.dependency_created_at, CURRENT_TIMESTAMP()) AS days_blocked
FROM blocking_status bs
JOIN VW_WORK_ITEMS w ON w.work_id = bs.work_id
WHERE bs.is_currently_blocked = TRUE
ORDER BY w.priority_score DESC, days_blocked DESC, bs.work_id;

-- ============================================================================
-- VW_SPRINT_WORK - Work items in each sprint (updated with display_id)
-- ============================================================================
CREATE OR REPLACE VIEW VW_SPRINT_WORK AS
WITH sprint_assignments AS (
  SELECT 
    e.attributes:sprint_id::string AS sprint_id,
    e.attributes:work_id::string AS work_id,
    e.occurred_at AS assigned_at,
    e.actor_id AS assigned_by,
    ROW_NUMBER() OVER (
      PARTITION BY e.attributes:work_id::string 
      ORDER BY e.occurred_at DESC
    ) AS rn  -- Latest sprint assignment wins
  FROM CLAUDE_BI.ACTIVITY.EVENTS e
  WHERE e.action = 'sdlc.sprint.assign'
),
current_assignments AS (
  SELECT * FROM sprint_assignments WHERE rn = 1
)
SELECT 
  sa.sprint_id,
  sa.work_id,
  w.display_id,
  sa.assigned_at,
  sa.assigned_by,
  w.title,
  w.type,
  w.severity,
  w.status,
  w.assignee_id,
  w.points,
  w.priority_score,
  w.created_at AS work_created_at,
  w.last_updated_at,
  -- Sprint-specific metrics
  CASE 
    WHEN w.status = 'done' THEN w.points
    ELSE 0
  END AS completed_points,
  CASE
    WHEN w.status IN ('done', 'cancelled') THEN 0
    ELSE COALESCE(w.points, 0)
  END AS remaining_points
FROM current_assignments sa
JOIN VW_WORK_ITEMS w ON w.work_id = sa.work_id
ORDER BY sa.sprint_id, w.priority_score DESC;

-- ============================================================================
-- VW_AGENT_PERFORMANCE - Agent performance metrics (updated)
-- ============================================================================
CREATE OR REPLACE VIEW VW_AGENT_PERFORMANCE AS
WITH agent_claims AS (
  SELECT 
    e.attributes:agent_id::string AS agent_id,
    e.attributes:work_id::string AS work_id,
    e.occurred_at AS claimed_at
  FROM CLAUDE_BI.ACTIVITY.EVENTS e
  WHERE e.action = 'sdlc.agent.claim'
),
agent_completions AS (
  SELECT 
    e.attributes:work_id::string AS work_id,
    e.occurred_at AS completed_at,
    e.actor_id AS completing_actor
  FROM CLAUDE_BI.ACTIVITY.EVENTS e  
  WHERE e.action = 'sdlc.work.done'
),
agent_work AS (
  SELECT 
    ac.agent_id,
    ac.work_id,
    w.display_id,
    ac.claimed_at,
    acomp.completed_at,
    acomp.completing_actor,
    -- Was this work completed by the agent who claimed it?
    CASE WHEN acomp.completing_actor = ac.agent_id THEN TRUE ELSE FALSE END AS completed_by_claimer,
    -- Calculate cycle time
    CASE 
      WHEN acomp.completed_at IS NOT NULL AND acomp.completing_actor = ac.agent_id 
      THEN DATEDIFF('hour', ac.claimed_at, acomp.completed_at)
      ELSE NULL
    END AS cycle_time_hours
  FROM agent_claims ac
  LEFT JOIN agent_completions acomp ON acomp.work_id = ac.work_id
    AND acomp.completed_at >= ac.claimed_at  -- Only count completions after claim
  LEFT JOIN VW_WORK_ITEMS w ON w.work_id = ac.work_id
)
SELECT 
  agent_id,
  COUNT(*) AS items_claimed,
  COUNT(CASE WHEN completed_by_claimer THEN 1 END) AS items_completed,
  CASE 
    WHEN COUNT(*) > 0 
    THEN COUNT(CASE WHEN completed_by_claimer THEN 1 END) / COUNT(*)::FLOAT
    ELSE 0
  END AS completion_rate,
  AVG(cycle_time_hours) AS avg_cycle_time_hours,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY cycle_time_hours) AS median_cycle_time_hours,
  MIN(claimed_at) AS first_claim_at,
  MAX(claimed_at) AS last_claim_at,
  -- Recent activity (last 7 days)
  COUNT(CASE WHEN claimed_at >= DATEADD('day', -7, CURRENT_TIMESTAMP()) THEN 1 END) AS claims_last_7d,
  COUNT(CASE WHEN completed_at >= DATEADD('day', -7, CURRENT_TIMESTAMP()) AND completed_by_claimer THEN 1 END) AS completions_last_7d
FROM agent_work
GROUP BY agent_id
HAVING COUNT(*) > 0  -- Only agents who have claimed work
ORDER BY completion_rate DESC, items_completed DESC;

-- ============================================================================
-- Grant view permissions to MCP users
-- ============================================================================
GRANT SELECT ON VIEW VW_WORK_ITEMS TO ROLE MCP_USER_ROLE;
GRANT SELECT ON VIEW VW_PRIORITY_QUEUE TO ROLE MCP_USER_ROLE;
GRANT SELECT ON VIEW VW_BLOCKED_WORK TO ROLE MCP_USER_ROLE;
GRANT SELECT ON VIEW VW_WORK_HISTORY TO ROLE MCP_USER_ROLE;
GRANT SELECT ON VIEW VW_SPRINT_WORK TO ROLE MCP_USER_ROLE;
GRANT SELECT ON VIEW VW_AGENT_PERFORMANCE TO ROLE MCP_USER_ROLE;

-- ============================================================================
-- END OF HARDENED CORE VIEWS
-- 
-- Next: Fix SDLC_CLAIM_NEXT with version checking and retries
-- ============================================================================