-- ============================================================================
-- 10_ddl_views.sql
-- DDL Management Views - Two-Table Law Compliant
-- Views to manage and monitor DDL versioning
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE CLAUDE_BI;
USE SCHEMA MCP;

-- ============================================================================
-- Current Version Catalog
-- Shows the latest version of each object
-- ============================================================================
CREATE OR REPLACE VIEW VW_DDL_CATALOG AS
WITH latest_versions AS (
  SELECT 
    attributes:object_name::string as object_name,
    attributes:object_type::string as object_type,
    attributes:version::string as version,
    attributes:hash::string as hash,
    attributes:author::string as author,
    attributes:reason::string as reason,
    occurred_at as last_modified,
    ROW_NUMBER() OVER (
      PARTITION BY attributes:object_name::string 
      ORDER BY occurred_at DESC
    ) as rn
  FROM CLAUDE_BI.ACTIVITY.EVENTS
  WHERE action IN ('ddl.object.create', 'ddl.object.alter')
)
SELECT 
  object_name,
  object_type,
  version,
  hash,
  author,
  reason,
  last_modified
FROM latest_versions 
WHERE rn = 1
ORDER BY object_type, object_name;

-- ============================================================================
-- Version History
-- Complete history of all DDL changes
-- ============================================================================
CREATE OR REPLACE VIEW VW_DDL_HISTORY AS
SELECT 
  attributes:object_name::string as object_name,
  attributes:object_type::string as object_type,
  attributes:version::string as version,
  attributes:author::string as author,
  attributes:reason::string as reason,
  action as event_type,
  occurred_at,
  attributes:hash::string as hash,
  attributes:previous_hash::string as previous_hash
FROM CLAUDE_BI.ACTIVITY.EVENTS
WHERE action IN ('ddl.object.create', 'ddl.object.alter', 'ddl.object.rollback', 'ddl.object.deploy')
ORDER BY object_name, occurred_at DESC;

-- ============================================================================
-- Drift Detection View
-- Compares stored DDL with actual DDL
-- ============================================================================
CREATE OR REPLACE VIEW VW_DDL_DRIFT AS
WITH stored_ddl AS (
  -- Get latest stored version for each object
  SELECT 
    attributes:object_name::string as object_name,
    attributes:object_type::string as object_type,
    attributes:hash::string as stored_hash,
    attributes:version::string as version,
    attributes:ddl_text::string as stored_ddl
  FROM (
    SELECT *,
      ROW_NUMBER() OVER (
        PARTITION BY attributes:object_name::string 
        ORDER BY occurred_at DESC
      ) as rn
    FROM CLAUDE_BI.ACTIVITY.EVENTS
    WHERE action IN ('ddl.object.create', 'ddl.object.alter')
  )
  WHERE rn = 1
),
actual_procedures AS (
  -- Get actual DDL for procedures
  SELECT 
    'CLAUDE_BI.MCP.' || PROCEDURE_NAME as object_name,
    'PROCEDURE' as object_type,
    GET_DDL('PROCEDURE', 'CLAUDE_BI.MCP.' || PROCEDURE_NAME) as actual_ddl
  FROM INFORMATION_SCHEMA.PROCEDURES
  WHERE PROCEDURE_SCHEMA = 'MCP'
    AND PROCEDURE_CATALOG = 'CLAUDE_BI'
),
actual_views AS (
  -- Get actual DDL for views
  SELECT 
    'CLAUDE_BI.MCP.' || TABLE_NAME as object_name,
    'VIEW' as object_type,
    GET_DDL('VIEW', 'CLAUDE_BI.MCP.' || TABLE_NAME) as actual_ddl
  FROM INFORMATION_SCHEMA.VIEWS
  WHERE TABLE_SCHEMA = 'MCP'
    AND TABLE_CATALOG = 'CLAUDE_BI'
),
all_actual AS (
  SELECT * FROM actual_procedures
  UNION ALL
  SELECT * FROM actual_views
)
SELECT 
  COALESCE(s.object_name, a.object_name) as object_name,
  COALESCE(s.object_type, a.object_type) as object_type,
  s.version as stored_version,
  CASE 
    WHEN s.object_name IS NULL THEN 'NOT_TRACKED'
    WHEN a.object_name IS NULL THEN 'DELETED'
    WHEN s.stored_hash != SHA2(a.actual_ddl) THEN 'DRIFT_DETECTED'
    ELSE 'IN_SYNC'
  END as drift_status,
  s.stored_hash,
  SHA2(a.actual_ddl) as actual_hash
FROM stored_ddl s
FULL OUTER JOIN all_actual a ON s.object_name = a.object_name
WHERE s.stored_hash != SHA2(a.actual_ddl) 
   OR s.object_name IS NULL 
   OR a.object_name IS NULL;

-- ============================================================================
-- Deployment History
-- Track all deployments and their results
-- ============================================================================
CREATE OR REPLACE VIEW VW_DDL_DEPLOYMENTS AS
SELECT 
  attributes:object_name::string as object_name,
  attributes:version::string as version,
  attributes:deployed_by::string as deployed_by,
  attributes:environment::string as environment,
  occurred_at as deployed_at,
  action as deployment_type
FROM CLAUDE_BI.ACTIVITY.EVENTS
WHERE action IN ('ddl.object.deploy', 'ddl.deploy.failed')
ORDER BY occurred_at DESC;

-- ============================================================================
-- Test Coverage View
-- Shows which objects have tests
-- ============================================================================
CREATE OR REPLACE VIEW VW_DDL_TEST_COVERAGE AS
WITH object_tests AS (
  SELECT 
    attributes:object_name::string as object_name,
    COUNT(*) as test_count,
    MAX(occurred_at) as last_test_added
  FROM CLAUDE_BI.ACTIVITY.EVENTS
  WHERE action = 'ddl.object.test'
  GROUP BY 1
),
all_objects AS (
  SELECT DISTINCT
    attributes:object_name::string as object_name
  FROM CLAUDE_BI.ACTIVITY.EVENTS
  WHERE action IN ('ddl.object.create', 'ddl.object.alter')
)
SELECT 
  o.object_name,
  COALESCE(t.test_count, 0) as test_count,
  t.last_test_added,
  CASE 
    WHEN t.test_count > 0 THEN 'TESTED'
    ELSE 'NO_TESTS'
  END as test_status
FROM all_objects o
LEFT JOIN object_tests t ON o.object_name = t.object_name
ORDER BY test_count DESC, object_name;

-- ============================================================================
-- Unused Objects View
-- Find objects with no recent executions (placeholder - needs execution tracking)
-- ============================================================================
CREATE OR REPLACE VIEW VW_DDL_UNUSED AS
WITH recent_executions AS (
  -- This would track actual procedure executions if we logged them
  SELECT DISTINCT 
    attributes:object_name::string as object_name
  FROM CLAUDE_BI.ACTIVITY.EVENTS
  WHERE action = 'ddl.object.execute'  -- Would need to track executions
    AND occurred_at > DATEADD('day', -30, CURRENT_TIMESTAMP())
),
all_objects AS (
  SELECT 
    object_name,
    version,
    last_modified
  FROM VW_DDL_CATALOG
)
SELECT 
  o.object_name,
  o.version,
  o.last_modified,
  CASE 
    WHEN e.object_name IS NULL THEN 'UNUSED_30_DAYS'
    ELSE 'ACTIVE'
  END as usage_status
FROM all_objects o
LEFT JOIN recent_executions e ON o.object_name = e.object_name
WHERE e.object_name IS NULL
ORDER BY o.last_modified;

-- ============================================================================
-- Duplicate Detection View
-- Find similar named objects that might be duplicates
-- ============================================================================
CREATE OR REPLACE VIEW VW_DDL_DUPLICATES AS
WITH object_pairs AS (
  SELECT 
    a.object_name as object1,
    b.object_name as object2,
    a.object_type as type1,
    b.object_type as type2,
    EDITDISTANCE(
      SPLIT_PART(a.object_name, '.', -1), 
      SPLIT_PART(b.object_name, '.', -1)
    ) as name_distance
  FROM VW_DDL_CATALOG a
  JOIN VW_DDL_CATALOG b 
    ON a.object_name < b.object_name
    AND a.object_type = b.object_type
)
SELECT 
  object1,
  object2,
  type1 as object_type,
  name_distance
FROM object_pairs
WHERE name_distance <= 3  -- Similar names (edit distance of 3 or less)
ORDER BY name_distance, object1, object2;

-- ============================================================================
-- Rollback Candidates View
-- Shows available versions for rollback
-- ============================================================================
CREATE OR REPLACE VIEW VW_DDL_ROLLBACK_CANDIDATES AS
WITH version_history AS (
  SELECT 
    attributes:object_name::string as object_name,
    attributes:version::string as version,
    attributes:author::string as author,
    attributes:reason::string as reason,
    occurred_at,
    ROW_NUMBER() OVER (
      PARTITION BY attributes:object_name::string 
      ORDER BY occurred_at DESC
    ) as version_rank
  FROM CLAUDE_BI.ACTIVITY.EVENTS
  WHERE action IN ('ddl.object.create', 'ddl.object.alter')
)
SELECT 
  object_name,
  version,
  author,
  reason,
  occurred_at,
  CASE version_rank
    WHEN 1 THEN 'CURRENT'
    WHEN 2 THEN 'PREVIOUS'
    ELSE 'HISTORICAL'
  END as version_status
FROM version_history
WHERE version_rank <= 5  -- Show last 5 versions
ORDER BY object_name, version_rank;

-- Grant permissions
GRANT SELECT ON VIEW VW_DDL_CATALOG TO ROLE MCP_USER_ROLE;
GRANT SELECT ON VIEW VW_DDL_HISTORY TO ROLE MCP_USER_ROLE;
GRANT SELECT ON VIEW VW_DDL_DRIFT TO ROLE MCP_USER_ROLE;
GRANT SELECT ON VIEW VW_DDL_DEPLOYMENTS TO ROLE MCP_USER_ROLE;
GRANT SELECT ON VIEW VW_DDL_TEST_COVERAGE TO ROLE MCP_USER_ROLE;
GRANT SELECT ON VIEW VW_DDL_UNUSED TO ROLE MCP_USER_ROLE;
GRANT SELECT ON VIEW VW_DDL_DUPLICATES TO ROLE MCP_USER_ROLE;
GRANT SELECT ON VIEW VW_DDL_ROLLBACK_CANDIDATES TO ROLE MCP_USER_ROLE;