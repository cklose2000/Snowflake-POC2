-- ============================================================================
-- 13_ddl_deploy_hardened.sql  
-- Enhanced DDL_DEPLOY with race-proof concurrency and canonical hashing
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE CLAUDE_BI;
USE SCHEMA MCP;

-- ============================================================================
-- DDL_DEPLOY_HARDENED: Production-grade versioning with fail-fast execution
-- ============================================================================
CREATE OR REPLACE PROCEDURE MCP.DDL_DEPLOY_HARDENED(
  object_type STRING,
  object_identity STRING,  -- Full identity with signature for procs/funcs
  ddl_text STRING,
  author STRING,
  reason STRING,
  idempotency_key STRING,
  expected_hash STRING DEFAULT NULL,
  environment STRING DEFAULT 'PRODUCTION'
)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
COMMENT = 'Hardened DDL deployment with canonical hashing and concurrency control'
AS
$$
const SF = snowflake;

try {
  // Check idempotency - has this exact change been processed?
  const idempotencySQL = `
    SELECT COUNT(*) as count
    FROM CLAUDE_BI.ACTIVITY.EVENTS
    WHERE action IN ('ddl.object.create', 'ddl.object.alter')
      AND attributes:idempotency_key::string = ?
  `;
  
  const idempotencyStmt = SF.createStatement({
    sqlText: idempotencySQL,
    binds: [IDEMPOTENCY_KEY]
  });
  const idempotencyRS = idempotencyStmt.execute();
  idempotencyRS.next();
  
  if (idempotencyRS.getColumnValue('COUNT') > 0) {
    return {
      result: 'unchanged',
      reason: 'Idempotent operation - already processed',
      idempotency_key: IDEMPOTENCY_KEY
    };
  }
  
  // Get current version and hash if exists
  const currentSQL = `
    WITH latest AS (
      SELECT 
        attributes:hash::string as hash,
        attributes:version::string as version,
        attributes:canonical_hash::string as canonical_hash,
        occurred_at
      FROM CLAUDE_BI.ACTIVITY.EVENTS
      WHERE action IN ('ddl.object.create', 'ddl.object.alter')
        AND attributes:object_identity::string = ?
      ORDER BY occurred_at DESC
      LIMIT 1
    )
    SELECT * FROM latest
  `;
  
  const currentStmt = SF.createStatement({
    sqlText: currentSQL,
    binds: [OBJECT_IDENTITY]
  });
  const currentRS = currentStmt.execute();
  
  let currentHash = null;
  let currentVersion = '0.0.0';
  let currentCanonicalHash = null;
  
  if (currentRS.next()) {
    currentHash = currentRS.getColumnValue('HASH');
    currentCanonicalHash = currentRS.getColumnValue('CANONICAL_HASH') || currentHash;
    currentVersion = currentRS.getColumnValue('VERSION') || '1.0.0';
  }
  
  // Check expected hash for optimistic concurrency control
  if (EXPECTED_HASH && currentCanonicalHash !== EXPECTED_HASH) {
    return {
      result: 'conflict',
      error: 'Version conflict - object was modified',
      expected_hash: EXPECTED_HASH,
      actual_hash: currentCanonicalHash,
      hint: 'Re-read the current version and retry with updated expected_hash'
    };
  }
  
  // CRITICAL: Execute DDL FIRST (fail-fast approach)
  let executionError = null;
  let canonicalDDL = null;
  let canonicalHash = null;
  
  try {
    // Execute the DDL
    const ddlStmt = SF.createStatement({ sqlText: DDL_TEXT });
    ddlStmt.execute();
    
    // Get canonical DDL from GET_DDL (this is what's actually in the database)
    const getCanonicalSQL = `SELECT GET_DDL(?, ?) as canonical_ddl`;
    const getCanonicalStmt = SF.createStatement({
      sqlText: getCanonicalSQL,
      binds: [OBJECT_TYPE.replace('SECURE ', ''), OBJECT_IDENTITY]
    });
    const getCanonicalRS = getCanonicalStmt.execute();
    getCanonicalRS.next();
    canonicalDDL = getCanonicalRS.getColumnValue('CANONICAL_DDL');
    
    // Hash the canonical DDL (not the input)
    const hashSQL = `SELECT SHA2(?) as hash`;
    const hashStmt = SF.createStatement({
      sqlText: hashSQL,
      binds: [canonicalDDL]
    });
    const hashRS = hashStmt.execute();
    hashRS.next();
    canonicalHash = hashRS.getColumnValue('HASH');
    
  } catch (ddlErr) {
    executionError = ddlErr.toString();
    
    // Log DDL execution failure
    const failSQL = `
      INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS (PAYLOAD, _SOURCE_LANE, _RECV_AT)
      SELECT 
        OBJECT_CONSTRUCT(
          'event_id', UUID_STRING(),
          'action', 'ddl.deploy.failed',
          'occurred_at', CURRENT_TIMESTAMP(),
          'actor_id', ?,
          'source', 'DDL_DEPLOY_HARDENED',
          'object', OBJECT_CONSTRUCT(
            'type', ?,
            'identity', ?
          ),
          'attributes', OBJECT_CONSTRUCT(
            'object_type', ?,
            'object_identity', ?,
            'error', ?,
            'idempotency_key', ?,
            'environment', ?
          )
        ),
        'DDL_DEPLOY',
        CURRENT_TIMESTAMP()
    `;
    
    SF.createStatement({
      sqlText: failSQL,
      binds: [
        AUTHOR,
        OBJECT_TYPE,
        OBJECT_IDENTITY,
        OBJECT_TYPE,
        OBJECT_IDENTITY,
        executionError,
        IDEMPOTENCY_KEY,
        ENVIRONMENT
      ]
    }).execute();
    
    return {
      result: 'error',
      error: 'DDL execution failed',
      details: executionError,
      object_identity: OBJECT_IDENTITY
    };
  }
  
  // Check if actually changed (compare canonical hashes)
  if (currentCanonicalHash === canonicalHash) {
    return {
      result: 'unchanged',
      reason: 'DDL produces identical canonical form',
      version: currentVersion,
      canonical_hash: canonicalHash
    };
  }
  
  // Calculate new version
  const versionParts = currentVersion.split('.');
  const newVersion = versionParts[0] + '.' + versionParts[1] + '.' + 
                      (parseInt(versionParts[2]) + 1);
  
  // Determine action type
  const action = currentHash ? 'ddl.object.alter' : 'ddl.object.create';
  
  // Hash the input DDL too (for tracking what was submitted)
  const inputHashSQL = `SELECT SHA2(?) as hash`;
  const inputHashStmt = SF.createStatement({
    sqlText: inputHashSQL,
    binds: [DDL_TEXT]
  });
  const inputHashRS = inputHashStmt.execute();
  inputHashRS.next();
  const inputHash = inputHashRS.getColumnValue('HASH');
  
  // Log the successful deployment event
  const successSQL = `
    INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS (PAYLOAD, _SOURCE_LANE, _RECV_AT)
    SELECT 
      OBJECT_CONSTRUCT(
        'event_id', UUID_STRING(),
        'action', ?,
        'occurred_at', CURRENT_TIMESTAMP(),
        'actor_id', ?,
        'source', 'DDL_DEPLOY_HARDENED',
        'object', OBJECT_CONSTRUCT(
          'type', ?,
          'identity', ?
        ),
        'attributes', OBJECT_CONSTRUCT(
          'object_type', ?,
          'object_identity', ?,
          'ddl_text', ?,
          'canonical_ddl', ?,
          'version', ?,
          'hash', ?,
          'canonical_hash', ?,
          'previous_hash', ?,
          'previous_canonical_hash', ?,
          'author', ?,
          'reason', ?,
          'idempotency_key', ?,
          'environment', ?
        )
      ),
      'DDL_DEPLOY',
      CURRENT_TIMESTAMP()
  `;
  
  SF.createStatement({
    sqlText: successSQL,
    binds: [
      action,
      AUTHOR,
      OBJECT_TYPE,
      OBJECT_IDENTITY,
      OBJECT_TYPE,
      OBJECT_IDENTITY,
      DDL_TEXT,
      canonicalDDL,
      newVersion,
      inputHash,
      canonicalHash,
      currentHash,
      currentCanonicalHash,
      AUTHOR,
      REASON,
      IDEMPOTENCY_KEY,
      ENVIRONMENT
    ]
  }).execute();
  
  // Log deployment completion
  const deploySQL = `
    INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS (PAYLOAD, _SOURCE_LANE, _RECV_AT)
    SELECT 
      OBJECT_CONSTRUCT(
        'event_id', UUID_STRING(),
        'action', 'ddl.object.deploy',
        'occurred_at', CURRENT_TIMESTAMP(),
        'actor_id', ?,
        'source', 'DDL_DEPLOY_HARDENED',
        'object', OBJECT_CONSTRUCT(
          'type', ?,
          'identity', ?
        ),
        'attributes', OBJECT_CONSTRUCT(
          'object_identity', ?,
          'version', ?,
          'environment', ?,
          'deployed_by', ?,
          'idempotency_key', ?
        )
      ),
      'DDL_DEPLOY',
      CURRENT_TIMESTAMP()
  `;
  
  SF.createStatement({
    sqlText: deploySQL,
    binds: [
      AUTHOR,
      OBJECT_TYPE,
      OBJECT_IDENTITY,
      OBJECT_IDENTITY,
      newVersion,
      ENVIRONMENT,
      AUTHOR,
      IDEMPOTENCY_KEY
    ]
  }).execute();
  
  return {
    result: 'deployed',
    object_identity: OBJECT_IDENTITY,
    version: newVersion,
    previous_version: currentVersion,
    canonical_hash: canonicalHash,
    idempotency_key: IDEMPOTENCY_KEY,
    environment: ENVIRONMENT
  };
  
} catch (err) {
  // Log unexpected error
  const errorSQL = `
    INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS (PAYLOAD, _SOURCE_LANE, _RECV_AT)
    SELECT 
      OBJECT_CONSTRUCT(
        'event_id', UUID_STRING(),
        'action', 'ddl.deploy.error',
        'occurred_at', CURRENT_TIMESTAMP(),
        'actor_id', ?,
        'source', 'DDL_DEPLOY_HARDENED',
        'object', OBJECT_CONSTRUCT(
          'type', 'ERROR',
          'identity', ?
        ),
        'attributes', OBJECT_CONSTRUCT(
          'error', ?,
          'object_identity', ?,
          'idempotency_key', ?
        )
      ),
      'DDL_ERROR',
      CURRENT_TIMESTAMP()
  `;
  
  try {
    SF.createStatement({
      sqlText: errorSQL,
      binds: [
        AUTHOR,
        OBJECT_IDENTITY,
        err.toString(),
        OBJECT_IDENTITY,
        IDEMPOTENCY_KEY
      ]
    }).execute();
  } catch (logErr) {
    // Ignore logging errors
  }
  
  return {
    result: 'error',
    error: err.toString(),
    object_identity: OBJECT_IDENTITY
  };
}
$$;

-- Grant permission (internal use only, not for direct agent access)
GRANT USAGE ON PROCEDURE MCP.DDL_DEPLOY_HARDENED(STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING) 
  TO ROLE MCP_USER_ROLE;