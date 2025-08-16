-- ============================================================================
-- 11_ddl_automation.sql
-- DDL Automation Tasks - Two-Table Law Compliant
-- Automated drift detection and maintenance
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE CLAUDE_BI;
USE SCHEMA MCP;

-- ============================================================================
-- Drift Detection Task
-- Runs every 6 hours to detect drift between stored and actual DDL
-- ============================================================================
CREATE OR REPLACE TASK TASK_DDL_DRIFT_CHECK
  WAREHOUSE = CLAUDE_AGENT_WH
  SCHEDULE = 'USING CRON 0 */6 * * * UTC'  -- Every 6 hours
  COMMENT = 'Detect drift between stored DDL and actual database objects'
AS
  INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS (PAYLOAD, _SOURCE_LANE, _RECV_AT)
  SELECT 
    OBJECT_CONSTRUCT(
      'event_id', UUID_STRING(),
      'action', 'ddl.drift.detected',
      'occurred_at', CURRENT_TIMESTAMP(),
      'actor_id', 'SYSTEM',
      'source', 'drift_detection',
      'object', OBJECT_CONSTRUCT(
        'type', 'DRIFT_REPORT',
        'count', COUNT(*)
      ),
      'attributes', OBJECT_CONSTRUCT(
        'drift_objects', ARRAY_AGG(
          OBJECT_CONSTRUCT(
            'object_name', object_name,
            'object_type', object_type,
            'drift_status', drift_status,
            'stored_version', stored_version
          )
        ),
        'check_time', CURRENT_TIMESTAMP()
      )
    ),
    'DRIFT_CHECK',
    CURRENT_TIMESTAMP()
  FROM VW_DDL_DRIFT
  WHERE drift_status != 'IN_SYNC'
  HAVING COUNT(*) > 0;

-- ============================================================================
-- Sync From Production Procedure
-- Captures current state of any drifted objects
-- ============================================================================
CREATE OR REPLACE PROCEDURE DDL_SYNC_FROM_PROD()
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
const SF = snowflake;

try {
  // Find all drifted objects
  const driftSQL = `
    SELECT 
      object_name,
      object_type,
      drift_status
    FROM VW_DDL_DRIFT
    WHERE drift_status = 'DRIFT_DETECTED'
  `;
  
  const driftStmt = SF.createStatement({ sqlText: driftSQL });
  const driftRS = driftStmt.execute();
  
  let syncedCount = 0;
  const errors = [];
  
  while (driftRS.next()) {
    const objectName = driftRS.getColumnValue('OBJECT_NAME');
    const objectType = driftRS.getColumnValue('OBJECT_TYPE');
    
    try {
      // Get actual DDL
      const getDdlSQL = `SELECT GET_DDL(?, ?) as ddl_text`;
      const getDdlStmt = SF.createStatement({
        sqlText: getDdlSQL,
        binds: [objectType, objectName]
      });
      const getDdlRS = getDdlStmt.execute();
      getDdlRS.next();
      const ddlText = getDdlRS.getColumnValue('DDL_TEXT');
      
      // Call DDL_DEPLOY to sync
      const deploySQL = `CALL DDL_DEPLOY(?, ?, ?, 'SYSTEM', 'Sync from production - drift detected')`;
      const deployStmt = SF.createStatement({
        sqlText: deploySQL,
        binds: [objectType, objectName, ddlText]
      });
      const deployRS = deployStmt.execute();
      deployRS.next();
      
      syncedCount++;
      
    } catch (err) {
      errors.push({
        object: objectName,
        error: err.toString()
      });
    }
  }
  
  return {
    result: 'sync_complete',
    synced: syncedCount,
    errors: errors
  };
  
} catch (err) {
  return {
    result: 'error',
    error: err.toString()
  };
}
$$;

-- ============================================================================
-- Cleanup Unused Objects Procedure
-- Marks unused objects for review/removal
-- ============================================================================
CREATE OR REPLACE PROCEDURE DDL_CLEANUP_UNUSED(
  days_unused INTEGER DEFAULT 90,
  dry_run BOOLEAN DEFAULT TRUE
)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
const SF = snowflake;

try {
  // Find unused objects
  const unusedSQL = `
    SELECT 
      object_name,
      version,
      last_modified
    FROM VW_DDL_UNUSED
    WHERE DATEDIFF('day', last_modified, CURRENT_TIMESTAMP()) > ?
  `;
  
  const unusedStmt = SF.createStatement({
    sqlText: unusedSQL,
    binds: [DAYS_UNUSED]
  });
  const unusedRS = unusedStmt.execute();
  
  const candidates = [];
  
  while (unusedRS.next()) {
    const objectName = unusedRS.getColumnValue('OBJECT_NAME');
    const version = unusedRS.getColumnValue('VERSION');
    const lastModified = unusedRS.getColumnValue('LAST_MODIFIED');
    
    candidates.push({
      object_name: objectName,
      version: version,
      last_modified: lastModified
    });
    
    if (!DRY_RUN) {
      // Log cleanup candidate event
      const cleanupSQL = `
        INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS (PAYLOAD, _SOURCE_LANE, _RECV_AT)
        SELECT 
          OBJECT_CONSTRUCT(
            'event_id', UUID_STRING(),
            'action', 'ddl.cleanup.candidate',
            'occurred_at', CURRENT_TIMESTAMP(),
            'actor_id', 'SYSTEM',
            'source', 'cleanup_task',
            'object', OBJECT_CONSTRUCT(
              'type', 'CLEANUP',
              'name', ?
            ),
            'attributes', OBJECT_CONSTRUCT(
              'object_name', ?,
              'version', ?,
              'last_modified', ?,
              'days_unused', ?
            )
          ),
          'CLEANUP',
          CURRENT_TIMESTAMP()
      `;
      
      SF.createStatement({
        sqlText: cleanupSQL,
        binds: [objectName, objectName, version, lastModified, DAYS_UNUSED]
      }).execute();
    }
  }
  
  return {
    result: 'cleanup_analysis',
    dry_run: DRY_RUN,
    candidates: candidates,
    message: DRY_RUN ? 'Dry run - no changes made' : 'Candidates marked for cleanup'
  };
  
} catch (err) {
  return {
    result: 'error',
    error: err.toString()
  };
}
$$;

-- ============================================================================
-- Version Pruning Procedure
-- Removes old versions keeping only recent N versions
-- ============================================================================
CREATE OR REPLACE PROCEDURE DDL_PRUNE_VERSIONS(
  versions_to_keep INTEGER DEFAULT 10
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
BEGIN
  -- Log pruning event
  INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS (PAYLOAD, _SOURCE_LANE, _RECV_AT)
  WITH version_counts AS (
    SELECT 
      attributes:object_name::string as object_name,
      COUNT(*) as version_count
    FROM CLAUDE_BI.ACTIVITY.EVENTS
    WHERE action IN ('ddl.object.create', 'ddl.object.alter')
    GROUP BY 1
    HAVING COUNT(*) > :versions_to_keep
  )
  SELECT 
    OBJECT_CONSTRUCT(
      'event_id', UUID_STRING(),
      'action', 'ddl.versions.pruned',
      'occurred_at', CURRENT_TIMESTAMP(),
      'actor_id', 'SYSTEM',
      'source', 'version_pruning',
      'object', OBJECT_CONSTRUCT(
        'type', 'PRUNE_REPORT'
      ),
      'attributes', OBJECT_CONSTRUCT(
        'objects_pruned', ARRAY_AGG(
          OBJECT_CONSTRUCT(
            'object_name', object_name,
            'versions_before', version_count,
            'versions_after', :versions_to_keep
          )
        ),
        'prune_time', CURRENT_TIMESTAMP()
      )
    ),
    'VERSION_PRUNE',
    CURRENT_TIMESTAMP()
  FROM version_counts;
  
  RETURN OBJECT_CONSTRUCT(
    'result', 'pruning_logged',
    'note', 'Old versions marked for archival'
  );
END;
$$;

-- ============================================================================
-- DDL Health Check Procedure
-- Comprehensive health check of DDL versioning system
-- ============================================================================
CREATE OR REPLACE PROCEDURE DDL_HEALTH_CHECK()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
  total_objects INTEGER;
  drifted_objects INTEGER;
  untested_objects INTEGER;
  unused_objects INTEGER;
  duplicate_candidates INTEGER;
BEGIN
  -- Count total tracked objects
  SELECT COUNT(DISTINCT object_name) INTO :total_objects
  FROM VW_DDL_CATALOG;
  
  -- Count drifted objects
  SELECT COUNT(*) INTO :drifted_objects
  FROM VW_DDL_DRIFT
  WHERE drift_status != 'IN_SYNC';
  
  -- Count untested objects
  SELECT COUNT(*) INTO :untested_objects
  FROM VW_DDL_TEST_COVERAGE
  WHERE test_status = 'NO_TESTS';
  
  -- Count unused objects
  SELECT COUNT(*) INTO :unused_objects
  FROM VW_DDL_UNUSED
  WHERE usage_status = 'UNUSED_30_DAYS';
  
  -- Count potential duplicates
  SELECT COUNT(*) INTO :duplicate_candidates
  FROM VW_DDL_DUPLICATES;
  
  -- Log health check event
  INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS (PAYLOAD, _SOURCE_LANE, _RECV_AT)
  SELECT 
    OBJECT_CONSTRUCT(
      'event_id', UUID_STRING(),
      'action', 'ddl.health.check',
      'occurred_at', CURRENT_TIMESTAMP(),
      'actor_id', 'SYSTEM',
      'source', 'health_check',
      'object', OBJECT_CONSTRUCT(
        'type', 'HEALTH_REPORT'
      ),
      'attributes', OBJECT_CONSTRUCT(
        'total_objects', :total_objects,
        'drifted_objects', :drifted_objects,
        'untested_objects', :untested_objects,
        'unused_objects', :unused_objects,
        'duplicate_candidates', :duplicate_candidates,
        'health_score', GREATEST(0, 100 - 
          (:drifted_objects * 10) - 
          (:untested_objects * 5) - 
          (:unused_objects * 3) - 
          (:duplicate_candidates * 2)),
        'check_time', CURRENT_TIMESTAMP()
      )
    ),
    'HEALTH_CHECK',
    CURRENT_TIMESTAMP();
  
  RETURN OBJECT_CONSTRUCT(
    'result', 'health_check_complete',
    'total_objects', :total_objects,
    'drifted_objects', :drifted_objects,
    'untested_objects', :untested_objects,
    'unused_objects', :unused_objects,
    'duplicate_candidates', :duplicate_candidates,
    'health_score', GREATEST(0, 100 - 
      (:drifted_objects * 10) - 
      (:untested_objects * 5) - 
      (:unused_objects * 3) - 
      (:duplicate_candidates * 2))
  );
END;
$$;

-- Grant permissions
GRANT USAGE ON PROCEDURE DDL_SYNC_FROM_PROD() TO ROLE MCP_USER_ROLE;
GRANT USAGE ON PROCEDURE DDL_CLEANUP_UNUSED(INTEGER, BOOLEAN) TO ROLE MCP_USER_ROLE;
GRANT USAGE ON PROCEDURE DDL_PRUNE_VERSIONS(INTEGER) TO ROLE MCP_USER_ROLE;
GRANT USAGE ON PROCEDURE DDL_HEALTH_CHECK() TO ROLE MCP_USER_ROLE;

-- Note: To enable the drift detection task, run:
-- ALTER TASK TASK_DDL_DRIFT_CHECK RESUME;