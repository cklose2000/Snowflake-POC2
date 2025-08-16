-- ============================================================================
-- 15_ddl_consistency.sql
-- Consistency view for immediate read-after-write on DDL events
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE CLAUDE_BI;
USE SCHEMA MCP;

-- ============================================================================
-- VW_DDL_CONSISTENCY: Union of promoted and unpromoted events
-- ============================================================================
CREATE OR REPLACE VIEW MCP.VW_DDL_CONSISTENCY AS
WITH promoted_events AS (
  -- Events already in ACTIVITY.EVENTS (promoted by Dynamic Table)
  SELECT 
    event_id,
    action,
    occurred_at,
    actor_id,
    source,
    object,
    attributes,
    attributes:idempotency_key::string as idempotency_key,
    'PROMOTED' as status
  FROM CLAUDE_BI.ACTIVITY.EVENTS
  WHERE action LIKE 'ddl.%'
),
unpromoted_events AS (
  -- Events still in RAW_EVENTS waiting for promotion
  SELECT 
    PAYLOAD:event_id::string as event_id,
    PAYLOAD:action::string as action,
    PAYLOAD:occurred_at::timestamp as occurred_at,
    PAYLOAD:actor_id::string as actor_id,
    PAYLOAD:source::string as source,
    PAYLOAD:object as object,
    PAYLOAD:attributes as attributes,
    PAYLOAD:attributes:idempotency_key::string as idempotency_key,
    'PENDING' as status
  FROM CLAUDE_BI.LANDING.RAW_EVENTS
  WHERE PAYLOAD:action::string LIKE 'ddl.%'
    AND _RECV_AT >= DATEADD('minute', -5, CURRENT_TIMESTAMP())  -- Only recent events
),
deduplicated AS (
  -- Combine both, preferring promoted over unpromoted
  SELECT * FROM promoted_events
  UNION ALL
  SELECT * FROM unpromoted_events
  WHERE idempotency_key NOT IN (
    SELECT idempotency_key FROM promoted_events WHERE idempotency_key IS NOT NULL
  )
    AND event_id NOT IN (
    SELECT event_id FROM promoted_events WHERE event_id IS NOT NULL
  )
)
SELECT 
  event_id,
  action,
  occurred_at,
  actor_id,
  source,
  object,
  attributes,
  idempotency_key,
  status,
  CASE 
    WHEN status = 'PROMOTED' THEN 'Committed to ACTIVITY.EVENTS'
    ELSE 'Pending promotion (visible immediately)'
  END as consistency_note
FROM deduplicated
ORDER BY occurred_at DESC;

-- ============================================================================
-- VW_DDL_CATALOG_CONSISTENT: Latest DDL versions with consistency
-- ============================================================================
CREATE OR REPLACE VIEW MCP.VW_DDL_CATALOG_CONSISTENT AS
WITH all_events AS (
  SELECT 
    attributes:object_identity::string as object_identity,
    attributes:object_type::string as object_type,
    attributes:version::string as version,
    attributes:canonical_hash::string as canonical_hash,
    attributes:author::string as author,
    attributes:reason::string as reason,
    occurred_at,
    status
  FROM VW_DDL_CONSISTENCY
  WHERE action IN ('ddl.object.create', 'ddl.object.alter')
),
latest_versions AS (
  SELECT 
    object_identity,
    object_type,
    version,
    canonical_hash,
    author,
    reason,
    occurred_at as last_modified,
    status,
    ROW_NUMBER() OVER (
      PARTITION BY object_identity 
      ORDER BY occurred_at DESC
    ) as rn
  FROM all_events
)
SELECT 
  object_identity,
  object_type,
  version,
  canonical_hash,
  author,
  reason,
  last_modified,
  status as consistency_status
FROM latest_versions 
WHERE rn = 1
ORDER BY object_type, object_identity;

-- ============================================================================
-- VW_DDL_PENDING_PROMOTION: Events waiting for Dynamic Table refresh
-- ============================================================================
CREATE OR REPLACE VIEW MCP.VW_DDL_PENDING_PROMOTION AS
SELECT 
  event_id,
  action,
  occurred_at,
  actor_id,
  attributes:object_identity::string as object_identity,
  attributes:version::string as version,
  DATEDIFF('second', occurred_at, CURRENT_TIMESTAMP()) as age_seconds,
  CASE 
    WHEN DATEDIFF('second', occurred_at, CURRENT_TIMESTAMP()) < 60 THEN 'Just created'
    WHEN DATEDIFF('second', occurred_at, CURRENT_TIMESTAMP()) < 180 THEN 'Promotion imminent'
    ELSE 'Check Dynamic Table status'
  END as promotion_status
FROM VW_DDL_CONSISTENCY
WHERE status = 'PENDING'
ORDER BY occurred_at DESC;

-- ============================================================================
-- Helper Function: Get Latest Version with Consistency
-- ============================================================================
CREATE OR REPLACE FUNCTION GET_DDL_VERSION_CONSISTENT(p_object_identity STRING)
RETURNS TABLE (
  object_identity STRING,
  version STRING,
  canonical_hash STRING,
  consistency_status STRING
)
LANGUAGE SQL
AS
$$
  SELECT 
    object_identity,
    version,
    canonical_hash,
    consistency_status
  FROM VW_DDL_CATALOG_CONSISTENT
  WHERE object_identity = p_object_identity
$$;

-- ============================================================================
-- Monitoring: Check Promotion Lag
-- ============================================================================
CREATE OR REPLACE VIEW VW_DDL_PROMOTION_LAG AS
WITH lag_stats AS (
  SELECT 
    COUNT(*) as pending_count,
    MIN(occurred_at) as oldest_pending,
    MAX(occurred_at) as newest_pending,
    AVG(DATEDIFF('second', occurred_at, CURRENT_TIMESTAMP())) as avg_lag_seconds,
    MAX(DATEDIFF('second', occurred_at, CURRENT_TIMESTAMP())) as max_lag_seconds
  FROM VW_DDL_CONSISTENCY
  WHERE status = 'PENDING'
)
SELECT 
  pending_count,
  oldest_pending,
  newest_pending,
  avg_lag_seconds,
  max_lag_seconds,
  CASE 
    WHEN pending_count = 0 THEN 'All events promoted'
    WHEN max_lag_seconds < 60 THEN 'Normal - promotion in progress'
    WHEN max_lag_seconds < 180 THEN 'Slight delay - monitor'
    ELSE 'WARNING - check Dynamic Table refresh'
  END as lag_status
FROM lag_stats;

-- ============================================================================
-- Grant Permissions
-- ============================================================================
GRANT SELECT ON VIEW MCP.VW_DDL_CONSISTENCY TO ROLE MCP_USER_ROLE;
GRANT SELECT ON VIEW MCP.VW_DDL_CONSISTENCY TO ROLE MCP_AGENT_ROLE;
GRANT SELECT ON VIEW MCP.VW_DDL_CATALOG_CONSISTENT TO ROLE MCP_USER_ROLE;
GRANT SELECT ON VIEW MCP.VW_DDL_CATALOG_CONSISTENT TO ROLE MCP_AGENT_ROLE;
GRANT SELECT ON VIEW MCP.VW_DDL_PENDING_PROMOTION TO ROLE MCP_USER_ROLE;
GRANT SELECT ON VIEW MCP.VW_DDL_PENDING_PROMOTION TO ROLE MCP_AGENT_ROLE;
GRANT SELECT ON VIEW MCP.VW_DDL_PROMOTION_LAG TO ROLE MCP_USER_ROLE;
GRANT SELECT ON VIEW MCP.VW_DDL_PROMOTION_LAG TO ROLE MCP_AGENT_ROLE;
GRANT USAGE ON FUNCTION MCP.GET_DDL_VERSION_CONSISTENT(STRING) TO ROLE MCP_USER_ROLE;
GRANT USAGE ON FUNCTION MCP.GET_DDL_VERSION_CONSISTENT(STRING) TO ROLE MCP_AGENT_ROLE;

-- ============================================================================
-- Usage Examples
-- ============================================================================
/*
-- Check current version with immediate consistency
SELECT * FROM VW_DDL_CATALOG_CONSISTENT 
WHERE object_identity = 'MCP.MY_PROC(STRING, NUMBER)';

-- See pending promotions
SELECT * FROM VW_DDL_PENDING_PROMOTION;

-- Monitor promotion lag
SELECT * FROM VW_DDL_PROMOTION_LAG;

-- Get specific object version
SELECT * FROM TABLE(GET_DDL_VERSION_CONSISTENT('MCP.MY_VIEW'));

-- See all DDL events (promoted and pending)
SELECT 
  action,
  occurred_at,
  attributes:object_identity::string as object_identity,
  attributes:version::string as version,
  status
FROM VW_DDL_CONSISTENCY
WHERE occurred_at >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
ORDER BY occurred_at DESC;
*/