-- ============================================================================
-- 09_ddl_versioning.sql
-- DDL Versioning System Using Events - Two-Table Law Compliant
-- Every CREATE/ALTER/DROP becomes an event in LANDING.RAW_EVENTS
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE CLAUDE_BI;
USE SCHEMA MCP;

-- ============================================================================
-- DDL Event Taxonomy
-- ============================================================================
-- 'ddl.object.create'   - New object created
-- 'ddl.object.alter'    - Object modified
-- 'ddl.object.drop'     - Object removed
-- 'ddl.object.test'     - Test case for object
-- 'ddl.object.deploy'   - Deployment record
-- 'ddl.object.rollback' - Rollback to previous version
-- 'ddl.drift.detected'  - Drift between stored and actual

-- ============================================================================
-- 1. Capture Current State (Bootstrap existing objects)
-- ============================================================================
CREATE OR REPLACE PROCEDURE DDL_CAPTURE_CURRENT()
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
const SF = snowflake;

try {
  // Get all procedures in MCP schema
  const procSQL = `
    SELECT 
      PROCEDURE_NAME as name,
      PROCEDURE_CATALOG || '.' || PROCEDURE_SCHEMA || '.' || PROCEDURE_NAME as full_name
    FROM INFORMATION_SCHEMA.PROCEDURES 
    WHERE PROCEDURE_SCHEMA = 'MCP'
      AND PROCEDURE_CATALOG = 'CLAUDE_BI'
  `;
  
  const procStmt = SF.createStatement({ sqlText: procSQL });
  const procRS = procStmt.execute();
  
  let capturedCount = 0;
  const errors = [];
  
  while (procRS.next()) {
    const fullName = procRS.getColumnValue('FULL_NAME');
    const name = procRS.getColumnValue('NAME');
    
    try {
      // Get DDL for this procedure
      const getDdlSQL = `SELECT GET_DDL('PROCEDURE', ?) as ddl_text`;
      const getDdlStmt = SF.createStatement({
        sqlText: getDdlSQL,
        binds: [fullName]
      });
      const getDdlRS = getDdlStmt.execute();
      getDdlRS.next();
      const ddlText = getDdlRS.getColumnValue('DDL_TEXT');
      
      // Create hash for version tracking
      const hashSQL = `SELECT SHA2(?) as hash`;
      const hashStmt = SF.createStatement({
        sqlText: hashSQL,
        binds: [ddlText]
      });
      const hashRS = hashStmt.execute();
      hashRS.next();
      const ddlHash = hashRS.getColumnValue('HASH');
      
      // Insert DDL create event
      const insertSQL = `
        INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS (PAYLOAD, _SOURCE_LANE, _RECV_AT)
        SELECT 
          OBJECT_CONSTRUCT(
            'event_id', UUID_STRING(),
            'action', 'ddl.object.create',
            'occurred_at', CURRENT_TIMESTAMP(),
            'actor_id', 'SYSTEM',
            'source', 'ddl_capture',
            'object', OBJECT_CONSTRUCT(
              'type', 'PROCEDURE',
              'name', ?,
              'full_name', ?
            ),
            'attributes', OBJECT_CONSTRUCT(
              'object_type', 'PROCEDURE',
              'object_name', ?,
              'ddl_text', ?,
              'version', '1.0.0',
              'hash', ?,
              'author', 'SYSTEM',
              'reason', 'Initial capture of existing object'
            )
          ),
          'DDL_CAPTURE',
          CURRENT_TIMESTAMP()
      `;
      
      const insertStmt = SF.createStatement({
        sqlText: insertSQL,
        binds: [name, fullName, fullName, ddlText, ddlHash]
      });
      insertStmt.execute();
      
      capturedCount++;
      
    } catch (err) {
      errors.push({
        object: fullName,
        error: err.toString()
      });
    }
  }
  
  // Also capture views
  const viewSQL = `
    SELECT 
      TABLE_NAME as name,
      TABLE_CATALOG || '.' || TABLE_SCHEMA || '.' || TABLE_NAME as full_name
    FROM INFORMATION_SCHEMA.VIEWS 
    WHERE TABLE_SCHEMA = 'MCP'
      AND TABLE_CATALOG = 'CLAUDE_BI'
  `;
  
  const viewStmt = SF.createStatement({ sqlText: viewSQL });
  const viewRS = viewStmt.execute();
  
  while (viewRS.next()) {
    const fullName = viewRS.getColumnValue('FULL_NAME');
    const name = viewRS.getColumnValue('NAME');
    
    try {
      // Get DDL for this view
      const getDdlSQL = `SELECT GET_DDL('VIEW', ?) as ddl_text`;
      const getDdlStmt = SF.createStatement({
        sqlText: getDdlSQL,
        binds: [fullName]
      });
      const getDdlRS = getDdlStmt.execute();
      getDdlRS.next();
      const ddlText = getDdlRS.getColumnValue('DDL_TEXT');
      
      // Create hash
      const hashSQL = `SELECT SHA2(?) as hash`;
      const hashStmt = SF.createStatement({
        sqlText: hashSQL,
        binds: [ddlText]
      });
      const hashRS = hashStmt.execute();
      hashRS.next();
      const ddlHash = hashRS.getColumnValue('HASH');
      
      // Insert DDL create event
      const insertSQL = `
        INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS (PAYLOAD, _SOURCE_LANE, _RECV_AT)
        SELECT 
          OBJECT_CONSTRUCT(
            'event_id', UUID_STRING(),
            'action', 'ddl.object.create',
            'occurred_at', CURRENT_TIMESTAMP(),
            'actor_id', 'SYSTEM',
            'source', 'ddl_capture',
            'object', OBJECT_CONSTRUCT(
              'type', 'VIEW',
              'name', ?,
              'full_name', ?
            ),
            'attributes', OBJECT_CONSTRUCT(
              'object_type', 'VIEW',
              'object_name', ?,
              'ddl_text', ?,
              'version', '1.0.0',
              'hash', ?,
              'author', 'SYSTEM',
              'reason', 'Initial capture of existing object'
            )
          ),
          'DDL_CAPTURE',
          CURRENT_TIMESTAMP()
      `;
      
      const insertStmt = SF.createStatement({
        sqlText: insertSQL,
        binds: [name, fullName, fullName, ddlText, ddlHash]
      });
      insertStmt.execute();
      
      capturedCount++;
      
    } catch (err) {
      errors.push({
        object: fullName,
        error: err.toString()
      });
    }
  }
  
  return {
    result: 'ok',
    captured: capturedCount,
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
-- 2. Version-Aware Deploy Procedure
-- ============================================================================
CREATE OR REPLACE PROCEDURE DDL_DEPLOY(
  object_type STRING,
  object_name STRING, 
  ddl_text STRING,
  author STRING,
  reason STRING
)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
const SF = snowflake;

try {
  // Calculate hash of new DDL
  const hashSQL = `SELECT SHA2(?) as hash`;
  const hashStmt = SF.createStatement({
    sqlText: hashSQL,
    binds: [DDL_TEXT]
  });
  const hashRS = hashStmt.execute();
  hashRS.next();
  const newHash = hashRS.getColumnValue('HASH');
  
  // Get current version if exists
  const currentSQL = `
    SELECT 
      attributes:hash::string as hash,
      attributes:version::string as version
    FROM CLAUDE_BI.ACTIVITY.EVENTS
    WHERE action IN ('ddl.object.create', 'ddl.object.alter')
      AND attributes:object_name::string = ?
    ORDER BY occurred_at DESC
    LIMIT 1
  `;
  
  const currentStmt = SF.createStatement({
    sqlText: currentSQL,
    binds: [OBJECT_NAME]
  });
  const currentRS = currentStmt.execute();
  
  let currentHash = null;
  let currentVersion = '0.0.0';
  
  if (currentRS.next()) {
    currentHash = currentRS.getColumnValue('HASH');
    currentVersion = currentRS.getColumnValue('VERSION') || '1.0.0';
  }
  
  // Check if actually changed
  if (currentHash === newHash) {
    return {
      result: 'unchanged',
      reason: 'DDL identical to current version',
      version: currentVersion
    };
  }
  
  // Calculate new version (simple increment)
  const versionParts = currentVersion.split('.');
  const newVersion = versionParts[0] + '.' + versionParts[1] + '.' + 
                      (parseInt(versionParts[2]) + 1);
  
  // Log the change event FIRST (for audit)
  const action = currentHash ? 'ddl.object.alter' : 'ddl.object.create';
  
  const insertSQL = `
    INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS (PAYLOAD, _SOURCE_LANE, _RECV_AT)
    SELECT 
      OBJECT_CONSTRUCT(
        'event_id', UUID_STRING(),
        'action', ?,
        'occurred_at', CURRENT_TIMESTAMP(),
        'actor_id', ?,
        'source', 'ddl_deploy',
        'object', OBJECT_CONSTRUCT(
          'type', ?,
          'name', ?
        ),
        'attributes', OBJECT_CONSTRUCT(
          'object_type', ?,
          'object_name', ?,
          'ddl_text', ?,
          'version', ?,
          'hash', ?,
          'previous_hash', ?,
          'author', ?,
          'reason', ?
        )
      ),
      'DDL_DEPLOY',
      CURRENT_TIMESTAMP()
  `;
  
  const insertStmt = SF.createStatement({
    sqlText: insertSQL,
    binds: [
      action,
      AUTHOR,
      OBJECT_TYPE,
      OBJECT_NAME,
      OBJECT_TYPE,
      OBJECT_NAME,
      DDL_TEXT,
      newVersion,
      newHash,
      currentHash,
      AUTHOR,
      REASON
    ]
  });
  insertStmt.execute();
  
  // Actually execute the DDL
  try {
    const ddlStmt = SF.createStatement({ sqlText: DDL_TEXT });
    ddlStmt.execute();
  } catch (ddlErr) {
    // Log deployment failure
    const failSQL = `
      INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS (PAYLOAD, _SOURCE_LANE, _RECV_AT)
      SELECT 
        OBJECT_CONSTRUCT(
          'event_id', UUID_STRING(),
          'action', 'ddl.deploy.failed',
          'occurred_at', CURRENT_TIMESTAMP(),
          'actor_id', ?,
          'source', 'ddl_deploy',
          'object', OBJECT_CONSTRUCT(
            'type', ?,
            'name', ?
          ),
          'attributes', OBJECT_CONSTRUCT(
            'object_name', ?,
            'version', ?,
            'error', ?
          )
        ),
        'DDL_DEPLOY',
        CURRENT_TIMESTAMP()
    `;
    
    SF.createStatement({
      sqlText: failSQL,
      binds: [AUTHOR, OBJECT_TYPE, OBJECT_NAME, OBJECT_NAME, newVersion, ddlErr.toString()]
    }).execute();
    
    throw ddlErr;
  }
  
  // Log successful deployment
  const deploySQL = `
    INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS (PAYLOAD, _SOURCE_LANE, _RECV_AT)
    SELECT 
      OBJECT_CONSTRUCT(
        'event_id', UUID_STRING(),
        'action', 'ddl.object.deploy',
        'occurred_at', CURRENT_TIMESTAMP(),
        'actor_id', ?,
        'source', 'ddl_deploy',
        'object', OBJECT_CONSTRUCT(
          'type', ?,
          'name', ?
        ),
        'attributes', OBJECT_CONSTRUCT(
          'object_name', ?,
          'version', ?,
          'environment', 'PRODUCTION',
          'deployed_by', ?
        )
      ),
      'DDL_DEPLOY',
      CURRENT_TIMESTAMP()
  `;
  
  SF.createStatement({
    sqlText: deploySQL,
    binds: [AUTHOR, OBJECT_TYPE, OBJECT_NAME, OBJECT_NAME, newVersion, AUTHOR]
  }).execute();
  
  return {
    result: 'deployed',
    object_name: OBJECT_NAME,
    version: newVersion,
    previous_version: currentVersion
  };
  
} catch (err) {
  return {
    result: 'error',
    error: err.toString()
  };
}
$$;

-- ============================================================================
-- 3. Rollback to Previous Version
-- ============================================================================
CREATE OR REPLACE PROCEDURE DDL_ROLLBACK(
  object_name STRING, 
  target_version STRING DEFAULT NULL
)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
const SF = snowflake;

try {
  // Find target version DDL
  let versionSQL;
  let binds;
  
  if (TARGET_VERSION) {
    // Specific version requested
    versionSQL = `
      SELECT 
        attributes:ddl_text::string as ddl_text,
        attributes:version::string as version
      FROM CLAUDE_BI.ACTIVITY.EVENTS
      WHERE action IN ('ddl.object.create', 'ddl.object.alter')
        AND attributes:object_name::string = ?
        AND attributes:version::string = ?
      ORDER BY occurred_at DESC
      LIMIT 1
    `;
    binds = [OBJECT_NAME, TARGET_VERSION];
  } else {
    // Rollback to previous version
    versionSQL = `
      WITH versions AS (
        SELECT 
          attributes:ddl_text::string as ddl_text,
          attributes:version::string as version,
          occurred_at,
          ROW_NUMBER() OVER (ORDER BY occurred_at DESC) as rn
        FROM CLAUDE_BI.ACTIVITY.EVENTS
        WHERE action IN ('ddl.object.create', 'ddl.object.alter')
          AND attributes:object_name::string = ?
      )
      SELECT ddl_text, version
      FROM versions
      WHERE rn = 2  -- Get second newest (previous version)
    `;
    binds = [OBJECT_NAME];
  }
  
  const versionStmt = SF.createStatement({
    sqlText: versionSQL,
    binds: binds
  });
  const versionRS = versionStmt.execute();
  
  if (!versionRS.next()) {
    return {
      result: 'error',
      error: 'Target version not found'
    };
  }
  
  const ddlText = versionRS.getColumnValue('DDL_TEXT');
  const version = versionRS.getColumnValue('VERSION');
  
  // Get current version for logging
  const currentSQL = `
    SELECT attributes:version::string as version
    FROM CLAUDE_BI.ACTIVITY.EVENTS
    WHERE action IN ('ddl.object.create', 'ddl.object.alter')
      AND attributes:object_name::string = ?
    ORDER BY occurred_at DESC
    LIMIT 1
  `;
  
  const currentStmt = SF.createStatement({
    sqlText: currentSQL,
    binds: [OBJECT_NAME]
  });
  const currentRS = currentStmt.execute();
  currentRS.next();
  const fromVersion = currentRS.getColumnValue('VERSION');
  
  // Log rollback event
  const rollbackSQL = `
    INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS (PAYLOAD, _SOURCE_LANE, _RECV_AT)
    SELECT 
      OBJECT_CONSTRUCT(
        'event_id', UUID_STRING(),
        'action', 'ddl.object.rollback',
        'occurred_at', CURRENT_TIMESTAMP(),
        'actor_id', CURRENT_USER(),
        'source', 'ddl_rollback',
        'object', OBJECT_CONSTRUCT(
          'type', 'DATABASE_OBJECT',
          'name', ?
        ),
        'attributes', OBJECT_CONSTRUCT(
          'object_name', ?,
          'from_version', ?,
          'to_version', ?,
          'reason', 'Manual rollback'
        )
      ),
      'DDL_ROLLBACK',
      CURRENT_TIMESTAMP()
  `;
  
  SF.createStatement({
    sqlText: rollbackSQL,
    binds: [OBJECT_NAME, OBJECT_NAME, fromVersion, version]
  }).execute();
  
  // Execute the rollback DDL
  try {
    const ddlStmt = SF.createStatement({ sqlText: ddlText });
    ddlStmt.execute();
  } catch (ddlErr) {
    return {
      result: 'error',
      error: 'Rollback failed: ' + ddlErr.toString()
    };
  }
  
  return {
    result: 'rolled_back',
    object_name: OBJECT_NAME,
    from_version: fromVersion,
    to_version: version
  };
  
} catch (err) {
  return {
    result: 'error',
    error: err.toString()
  };
}
$$;

-- ============================================================================
-- 4. DDL Testing Framework
-- ============================================================================
CREATE OR REPLACE PROCEDURE DDL_ADD_TEST(
  object_name STRING,
  test_name STRING,
  test_sql STRING,
  expected_result VARIANT
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
BEGIN
  INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS (PAYLOAD, _SOURCE_LANE, _RECV_AT)
  SELECT 
    OBJECT_CONSTRUCT(
      'event_id', UUID_STRING(),
      'action', 'ddl.object.test',
      'occurred_at', CURRENT_TIMESTAMP(),
      'actor_id', CURRENT_USER(),
      'source', 'ddl_test',
      'object', OBJECT_CONSTRUCT(
        'type', 'TEST',
        'name', :test_name
      ),
      'attributes', OBJECT_CONSTRUCT(
        'object_name', :object_name,
        'test_name', :test_name,
        'test_sql', :test_sql,
        'expected_result', :expected_result
      )
    ),
    'DDL_TEST',
    CURRENT_TIMESTAMP();
    
  RETURN OBJECT_CONSTRUCT('result', 'test_added', 'test_name', :test_name);
END;
$$;

CREATE OR REPLACE PROCEDURE DDL_RUN_TESTS(object_name STRING)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
const SF = snowflake;

try {
  // Get all tests for this object
  const testsSQL = `
    SELECT 
      attributes:test_name::string as test_name,
      attributes:test_sql::string as test_sql,
      attributes:expected_result as expected_result
    FROM CLAUDE_BI.ACTIVITY.EVENTS
    WHERE action = 'ddl.object.test'
      AND attributes:object_name::string = ?
    ORDER BY occurred_at DESC
  `;
  
  const testsStmt = SF.createStatement({
    sqlText: testsSQL,
    binds: [OBJECT_NAME]
  });
  const testsRS = testsStmt.execute();
  
  const results = [];
  let passed = 0;
  let failed = 0;
  
  while (testsRS.next()) {
    const testName = testsRS.getColumnValue('TEST_NAME');
    const testSQL = testsRS.getColumnValue('TEST_SQL');
    const expected = testsRS.getColumnValue('EXPECTED_RESULT');
    
    try {
      // Run the test
      const testStmt = SF.createStatement({ sqlText: testSQL });
      const testRS = testStmt.execute();
      
      let actual;
      if (testRS.next()) {
        // Get first column of first row as result
        actual = testRS.getColumnValue(1);
      }
      
      // Compare results
      const testPassed = JSON.stringify(actual) === JSON.stringify(expected);
      
      if (testPassed) {
        passed++;
      } else {
        failed++;
      }
      
      results.push({
        test_name: testName,
        passed: testPassed,
        expected: expected,
        actual: actual
      });
      
    } catch (testErr) {
      failed++;
      results.push({
        test_name: testName,
        passed: false,
        error: testErr.toString()
      });
    }
  }
  
  return {
    result: 'tests_complete',
    object_name: OBJECT_NAME,
    passed: passed,
    failed: failed,
    results: results
  };
  
} catch (err) {
  return {
    result: 'error',
    error: err.toString()
  };
}
$$;

-- Grant permissions
GRANT USAGE ON PROCEDURE DDL_CAPTURE_CURRENT() TO ROLE MCP_USER_ROLE;
GRANT USAGE ON PROCEDURE DDL_DEPLOY(STRING, STRING, STRING, STRING, STRING) TO ROLE MCP_USER_ROLE;
GRANT USAGE ON PROCEDURE DDL_ROLLBACK(STRING, STRING) TO ROLE MCP_USER_ROLE;
GRANT USAGE ON PROCEDURE DDL_ADD_TEST(STRING, STRING, STRING, VARIANT) TO ROLE MCP_USER_ROLE;
GRANT USAGE ON PROCEDURE DDL_RUN_TESTS(STRING) TO ROLE MCP_USER_ROLE;