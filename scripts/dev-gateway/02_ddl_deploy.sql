-- ============================================================================
-- DDL_DEPLOY - Core Execution Engine with Version Gating
-- Handles inline DDL with optimistic concurrency and safety checks
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE CLAUDE_BI;
USE SCHEMA MCP;

-- Drop existing if needed
DROP PROCEDURE IF EXISTS MCP.DDL_DEPLOY(STRING, STRING, STRING, STRING, STRING, STRING, STRING);

-- Create the core DDL deployment procedure with version gating
CREATE OR REPLACE PROCEDURE MCP.DDL_DEPLOY(
  object_type STRING,      -- VIEW, PROCEDURE, FUNCTION
  object_name STRING,      -- Fully qualified: DB.SCHEMA.NAME
  ddl TEXT,               -- The DDL to execute
  provenance STRING,      -- Who/what is deploying (agent_id)
  reason STRING,          -- Why this deployment
  expected_version STRING DEFAULT NULL,  -- For optimistic concurrency
  lease_id STRING DEFAULT NULL           -- For namespace validation
)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
COMMENT = 'Core DDL deployment with version gating and safety checks'
AS
$$
  var SF = snowflake;
  
  try {
    // Parse object name components
    var nameParts = OBJECT_NAME.split('.');
    if (nameParts.length !== 3) {
      throw new Error('Object name must be fully qualified: DB.SCHEMA.NAME');
    }
    
    var dbName = nameParts[0];
    var schemaName = nameParts[1];
    var objName = nameParts[2];
    
    // Set deterministic session parameters
    SF.execute({sqlText: "ALTER SESSION SET QUERY_TAG = 'deploy:" + PROVENANCE + "'"});
    SF.execute({sqlText: "ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 90"});
    SF.execute({sqlText: "ALTER SESSION SET QUOTED_IDENTIFIERS_IGNORE_CASE = TRUE"});
    SF.execute({sqlText: "ALTER SESSION SET TIMESTAMP_TYPE_MAPPING = TIMESTAMP_NTZ"});
    
    // Check version gate if provided
    if (EXPECTED_VERSION) {
      var versionCheckStmt = SF.createStatement({
        sqlText: "SELECT version FROM MCP.VW_LATEST_SCHEMA WHERE database_name = ? AND schema_name = ? AND object_name = ? AND object_type = ?",
        binds: [dbName, schemaName, objName, OBJECT_TYPE]
      });
      
      var versionRS = versionCheckStmt.execute();
      if (versionRS.next()) {
        var currentVersion = versionRS.getColumnValue('VERSION');
        if (currentVersion && currentVersion !== EXPECTED_VERSION) {
          return {
            result: 'error',
            error: 'version_conflict',
            current_version: currentVersion,
            expected_version: EXPECTED_VERSION,
            object: OBJECT_NAME
          };
        }
      }
    }
    
    // Validate namespace lease if provided
    if (LEASE_ID) {
      var leaseCheckStmt = SF.createStatement({
        sqlText: "SELECT COUNT(*) as valid_lease FROM MCP.VW_DEV_NAMESPACES WHERE lease_id = ? AND CURRENT_TIMESTAMP() < expires_at",
        binds: [LEASE_ID]
      });
      
      var leaseRS = leaseCheckStmt.execute();
      leaseRS.next();
      if (leaseRS.getColumnValue('VALID_LEASE') === 0) {
        return {
          result: 'error',
          error: 'invalid_lease',
          lease_id: LEASE_ID,
          message: 'Lease expired or not found'
        };
      }
    }
    
    // Safety checks on DDL
    var ddlUpper = DDL.toUpperCase();
    
    // Check for required patterns
    if (!ddlUpper.includes('CREATE OR REPLACE') && !ddlUpper.includes('CREATE IF NOT EXISTS')) {
      throw new Error('DDL must use CREATE OR REPLACE or CREATE IF NOT EXISTS pattern');
    }
    
    // Check for forbidden patterns
    var forbidden = ['TRUNCATE', 'ALTER ACCOUNT', 'DROP TABLE', 'DROP DATABASE', 'DROP SCHEMA'];
    for (var i = 0; i < forbidden.length; i++) {
      if (ddlUpper.includes(forbidden[i])) {
        return {
          result: 'error',
          error: 'forbidden_operation',
          pattern: forbidden[i],
          message: 'This operation is not allowed through the gateway'
        };
      }
    }
    
    // Validate single statement (count semicolons outside of dollar quotes)
    var inDollarQuote = false;
    var semicolonCount = 0;
    for (var j = 0; j < DDL.length; j++) {
      if (DDL[j] === '$' && DDL[j+1] === '$') {
        inDollarQuote = !inDollarQuote;
        j++; // Skip next char
      } else if (DDL[j] === ';' && !inDollarQuote) {
        semicolonCount++;
      }
    }
    
    if (semicolonCount > 1) {
      return {
        result: 'error',
        error: 'multiple_statements',
        count: semicolonCount,
        message: 'Only single DDL statements are allowed'
      };
    }
    
    // Shadow compile to candidate object first
    var candidateName = objName + '_CANDIDATE';
    var candidateDDL = DDL.replace(objName, candidateName);
    
    try {
      // Try to compile to candidate
      SF.execute({sqlText: candidateDDL});
      
      // If successful, drop the candidate
      var dropCandidateSQL = "DROP " + OBJECT_TYPE + " IF EXISTS " + 
                             dbName + "." + schemaName + "." + candidateName;
      SF.execute({sqlText: dropCandidateSQL});
      
    } catch (compileErr) {
      // Shadow compile failed - return error without affecting production
      return {
        result: 'error',
        error: 'compile_failed',
        compile_error: compileErr.toString(),
        object: OBJECT_NAME,
        message: 'DDL failed shadow compilation'
      };
    }
    
    // Execute the actual DDL
    SF.execute({sqlText: DDL});
    
    // Generate new version identifier
    var newVersion = new Date().toISOString();
    
    // Log the deployment as an event
    var deployEvent = {
      event_id: SF.createStatement({sqlText: "SELECT UUID_STRING()"}).execute().getColumnValue(1),
      action: 'ddl.object.deployed',
      occurred_at: new Date().toISOString(),
      actor_id: PROVENANCE,
      source: 'ddl_gateway',
      schema_version: '2.1.0',
      object: {
        type: 'ddl_object',
        id: OBJECT_NAME
      },
      attributes: {
        object_type: OBJECT_TYPE,
        object_name: OBJECT_NAME,
        database_name: dbName,
        schema_name: schemaName,
        version: newVersion,
        previous_version: EXPECTED_VERSION,
        provenance: PROVENANCE,
        reason: REASON,
        lease_id: LEASE_ID,
        ddl_length: DDL.length
      }
    };
    
    var eventStmt = SF.createStatement({
      sqlText: "INSERT INTO LANDING.RAW_EVENTS (PAYLOAD, _SOURCE_LANE, _RECV_AT) SELECT PARSE_JSON(?), 'DDL_DEPLOY', CURRENT_TIMESTAMP()",
      binds: [JSON.stringify(deployEvent)]
    });
    eventStmt.execute();
    
    return {
      result: 'ok',
      object: OBJECT_NAME,
      type: OBJECT_TYPE,
      version: newVersion,
      message: 'Deployment successful'
    };
    
  } catch (err) {
    // Log error event
    var errorEvent = {
      event_id: SF.createStatement({sqlText: "SELECT UUID_STRING()"}).execute().getColumnValue(1),
      action: 'ddl.deploy.error',
      occurred_at: new Date().toISOString(),
      actor_id: PROVENANCE,
      attributes: {
        object_name: OBJECT_NAME,
        error: err.toString(),
        error_code: err.code || null,
        sql_state: err.state || null
      }
    };
    
    SF.createStatement({
      sqlText: "INSERT INTO LANDING.RAW_EVENTS (PAYLOAD, _SOURCE_LANE, _RECV_AT) SELECT PARSE_JSON(?), 'DDL_ERROR', CURRENT_TIMESTAMP()",
      binds: [JSON.stringify(errorEvent)]
    }).execute();
    
    // Classify error type
    var errorClass = 'other';
    if (err.toString().includes('Syntax error')) {
      errorClass = 'syntax';
    } else if (err.toString().includes('does not exist')) {
      errorClass = 'dependency';
    } else if (err.toString().includes('Insufficient privileges')) {
      errorClass = 'privilege';
    } else if (err.toString().includes('timeout')) {
      errorClass = 'timeout';
    }
    
    return {
      result: 'error',
      error: err.toString(),
      error_class: errorClass,
      sql_code: err.code || null,
      sql_state: err.state || null,
      object: OBJECT_NAME
    };
  }
$$;

-- Grant execute permission
GRANT EXECUTE ON PROCEDURE MCP.DDL_DEPLOY(STRING, STRING, STRING, STRING, STRING, STRING, STRING) TO ROLE CLAUDE_AGENT_ROLE;

SELECT 'DDL_DEPLOY created with version gating and shadow compile' as status;