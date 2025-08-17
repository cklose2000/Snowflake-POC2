-- ============================================================================
-- Namespace Management Views - Event-Based Lease System
-- No registry tables - pure event-driven namespace isolation
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE CLAUDE_BI;
USE SCHEMA MCP;

-- ============================================================================
-- View: Active namespace claims with TTL
-- ============================================================================
CREATE OR REPLACE VIEW MCP.VW_DEV_NAMESPACES
COMMENT = 'Active namespace claims with lease expiration'
AS
WITH namespace_events AS (
  SELECT 
    occurred_at,
    attributes:app_name::string AS app_name,
    attributes:namespace::string AS namespace,
    attributes:agent_id::string AS agent_id,
    attributes:lease_id::string AS lease_id,
    attributes:ttl_seconds::number AS ttl_seconds,
    action,
    event_id
  FROM CLAUDE_BI.ACTIVITY.EVENTS
  WHERE action IN ('dev.claim', 'dev.release')
    AND occurred_at >= DATEADD('hour', -24, CURRENT_TIMESTAMP())  -- 24hr window
),
lease_status AS (
  SELECT
    app_name,
    namespace,
    agent_id,
    lease_id,
    ttl_seconds,
    occurred_at AS claimed_at,
    DATEADD('second', COALESCE(ttl_seconds, 900), occurred_at) AS expires_at,
    action,
    -- Check if this lease was explicitly released
    LEAD(action) OVER (PARTITION BY lease_id ORDER BY occurred_at) AS next_action
  FROM namespace_events
)
SELECT 
  app_name,
  namespace,
  agent_id,
  lease_id,
  ttl_seconds,
  claimed_at,
  expires_at,
  TIMESTAMPDIFF('second', CURRENT_TIMESTAMP(), expires_at) AS expires_in_seconds,
  CASE 
    WHEN next_action = 'dev.release' THEN 'released'
    WHEN CURRENT_TIMESTAMP() > expires_at THEN 'expired'
    ELSE 'active'
  END AS lease_status
FROM lease_status
WHERE action = 'dev.claim'
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY app_name, namespace 
  ORDER BY claimed_at DESC
) = 1
  AND lease_status = 'active';  -- Only show active leases

-- ============================================================================
-- View: Development activity log
-- ============================================================================
CREATE OR REPLACE VIEW MCP.VW_DEV_ACTIVITY
COMMENT = 'Recent development activity from gateway events'
AS
SELECT 
  occurred_at,
  action,
  actor_id,
  attributes:app_name::string AS app_name,
  attributes:object_name::string AS object_name,
  attributes:object_type::string AS object_type,
  attributes:result:result::string AS result,
  attributes:result:error::string AS error,
  attributes:result:version::string AS version,
  attributes:stage_url::string AS stage_url,
  attributes:reason::string AS reason,
  event_id
FROM CLAUDE_BI.ACTIVITY.EVENTS
WHERE action LIKE 'dev.%' OR action LIKE 'ddl.%'
  AND occurred_at >= DATEADD('day', -14, CURRENT_TIMESTAMP())  -- 14-day window
ORDER BY occurred_at DESC;

-- ============================================================================
-- View: App development status summary
-- ============================================================================
CREATE OR REPLACE VIEW MCP.VW_APP_STATUS
COMMENT = 'Application development status by namespace'
AS
WITH app_activity AS (
  SELECT 
    attributes:app_name::string AS app_name,
    COUNT(DISTINCT attributes:object_name::string) AS objects_deployed,
    COUNT(CASE WHEN attributes:result:result::string = 'ok' THEN 1 END) AS successful_deploys,
    COUNT(CASE WHEN attributes:result:result::string = 'error' THEN 1 END) AS failed_deploys,
    MAX(occurred_at) AS last_activity
  FROM CLAUDE_BI.ACTIVITY.EVENTS
  WHERE action = 'dev.deployed'
    AND occurred_at >= DATEADD('day', -30, CURRENT_TIMESTAMP())
  GROUP BY 1
)
SELECT 
  n.app_name,
  n.namespace,
  n.agent_id,
  n.lease_id,
  n.expires_in_seconds,
  COALESCE(a.objects_deployed, 0) AS objects_deployed,
  COALESCE(a.successful_deploys, 0) AS successful_deploys,
  COALESCE(a.failed_deploys, 0) AS failed_deploys,
  a.last_activity,
  CURRENT_TIMESTAMP() AS check_time
FROM MCP.VW_DEV_NAMESPACES n
LEFT JOIN app_activity a ON n.app_name = a.app_name
ORDER BY n.app_name, n.namespace;

-- ============================================================================
-- View: Deployment conflicts and errors
-- ============================================================================
CREATE OR REPLACE VIEW MCP.VW_DEV_CONFLICTS
COMMENT = 'Version conflicts and deployment errors'
AS
SELECT 
  occurred_at,
  actor_id,
  attributes:object_name::string AS object_name,
  attributes:result:error::string AS error_type,
  attributes:result:current_version::string AS current_version,
  attributes:result:expected_version::string AS expected_version,
  attributes:result:error_class::string AS error_class,
  attributes:result:message::string AS error_message,
  event_id
FROM CLAUDE_BI.ACTIVITY.EVENTS
WHERE action IN ('dev.deployed', 'ddl.deploy.error')
  AND attributes:result:result::string = 'error'
  AND occurred_at >= DATEADD('day', -7, CURRENT_TIMESTAMP())
ORDER BY occurred_at DESC;

-- ============================================================================
-- View: Rate limiting events (leaky bucket)
-- ============================================================================
CREATE OR REPLACE VIEW MCP.VW_RATE_LIMITS
COMMENT = 'Rate limiting token consumption and refills'
AS
WITH token_events AS (
  SELECT 
    occurred_at,
    actor_id,
    action,
    attributes:tokens::number AS tokens,
    attributes:bucket_size::number AS bucket_size
  FROM CLAUDE_BI.ACTIVITY.EVENTS
  WHERE action IN ('dev.token.consume', 'dev.token.refill', 'dev.rate_limited')
    AND occurred_at >= DATEADD('hour', -1, CURRENT_TIMESTAMP())  -- 1hr window
),
agent_buckets AS (
  SELECT 
    actor_id,
    SUM(CASE 
      WHEN action = 'dev.token.refill' THEN tokens
      WHEN action = 'dev.token.consume' THEN -tokens
      ELSE 0
    END) AS current_tokens,
    MAX(bucket_size) AS bucket_size,
    MAX(occurred_at) AS last_activity,
    COUNT(CASE WHEN action = 'dev.rate_limited' THEN 1 END) AS rate_limit_hits
  FROM token_events
  GROUP BY actor_id
)
SELECT 
  actor_id,
  current_tokens,
  bucket_size,
  GREATEST(0, current_tokens) AS available_tokens,
  CASE 
    WHEN current_tokens <= 0 THEN 'exhausted'
    WHEN current_tokens < bucket_size * 0.2 THEN 'low'
    ELSE 'ok'
  END AS bucket_status,
  rate_limit_hits,
  last_activity
FROM agent_buckets
ORDER BY current_tokens ASC;

-- Grant select permissions
GRANT SELECT ON VIEW MCP.VW_DEV_NAMESPACES TO ROLE CLAUDE_AGENT_ROLE;
GRANT SELECT ON VIEW MCP.VW_DEV_ACTIVITY TO ROLE CLAUDE_AGENT_ROLE;
GRANT SELECT ON VIEW MCP.VW_APP_STATUS TO ROLE CLAUDE_AGENT_ROLE;
GRANT SELECT ON VIEW MCP.VW_DEV_CONFLICTS TO ROLE CLAUDE_AGENT_ROLE;
GRANT SELECT ON VIEW MCP.VW_RATE_LIMITS TO ROLE CLAUDE_AGENT_ROLE;

SELECT 'Namespace management views created' as status;