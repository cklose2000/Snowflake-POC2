-- ============================================================================
-- DDL_DEPLOY_FROM_STAGE - Handle Large DDL via Stage Files with Checksums
-- Validates MD5 before execution, prevents VARIANT size limits
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE CLAUDE_BI;
USE SCHEMA MCP;

-- Drop existing if needed
DROP PROCEDURE IF EXISTS MCP.DDL_DEPLOY_FROM_STAGE(STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING);

-- Create procedure for staged DDL deployment with checksum validation
CREATE OR REPLACE PROCEDURE MCP.DDL_DEPLOY_FROM_STAGE(
  object_type STRING,      -- VIEW, PROCEDURE, FUNCTION
  object_name STRING,      -- Fully qualified: DB.SCHEMA.NAME
  stage_url STRING,        -- @stage_name/path/to/file.sql
  expected_md5 STRING,     -- Expected MD5 checksum
  provenance STRING,       -- Who/what is deploying (agent_id)
  reason STRING,           -- Why this deployment
  expected_version STRING DEFAULT NULL,  -- For optimistic concurrency
  lease_id STRING DEFAULT NULL           -- For namespace validation
)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
COMMENT = 'Deploy DDL from stage file with checksum validation'
AS
$$
  var SF = snowflake;
  
  try {
    // Validate stage URL format
    if (!STAGE_URL.startsWith('@')) {
      throw new Error('Stage URL must start with @ symbol');
    }
    
    // Get file metadata including MD5
    var listStmt = SF.createStatement({
      sqlText: "LIST " + STAGE_URL + " PATTERN = '.*'"
    });
    
    var listRS = listStmt.execute();
    var fileMD5 = null;
    var fileSize = 0;
    
    if (listRS.next()) {
      fileMD5 = listRS.getColumnValue('MD5');
      fileSize = listRS.getColumnValue('SIZE');
    } else {
      return {
        result: 'error',
        error: 'file_not_found',
        stage_url: STAGE_URL,
        message: 'Stage file does not exist'
      };
    }
    
    // Validate checksum
    if (fileMD5 !== EXPECTED_MD5) {
      return {
        result: 'error',
        error: 'checksum_mismatch',
        expected_md5: EXPECTED_MD5,
        actual_md5: fileMD5,
        stage_url: STAGE_URL,
        message: 'File checksum does not match expected value'
      };
    }
    
    // Check file size (prevent huge files)
    var maxSizeBytes = 10 * 1024 * 1024; // 10MB limit
    if (fileSize > maxSizeBytes) {
      return {
        result: 'error',
        error: 'file_too_large',
        size_bytes: fileSize,
        max_bytes: maxSizeBytes,
        message: 'DDL file exceeds maximum size limit'
      };
    }
    
    // Read the DDL content from stage
    var getStmt = SF.createStatement({
      sqlText: "SELECT $1 as ddl_content FROM " + STAGE_URL + " (FILE_FORMAT => (TYPE = 'CSV' FIELD_DELIMITER = NONE RECORD_DELIMITER = NONE ESCAPE = NONE))"
    });
    
    var getRS = getStmt.execute();
    var ddlContent = '';
    
    if (getRS.next()) {
      ddlContent = getRS.getColumnValue('DDL_CONTENT');
    } else {
      return {
        result: 'error',
        error: 'file_read_failed',
        stage_url: STAGE_URL,
        message: 'Could not read DDL content from stage file'
      };
    }
    
    // Validate it's a single statement (enhanced check)
    var statements = [];
    var currentStmt = '';
    var inDollarQuote = false;
    var dollarQuoteDelim = '';
    
    for (var i = 0; i < ddlContent.length; i++) {
      var char = ddlContent[i];
      currentStmt += char;
      
      // Check for dollar quote start/end
      if (char === '$') {
        var delimMatch = ddlContent.substring(i).match(/^\$([A-Za-z0-9_]*)\$/);
        if (delimMatch) {
          if (!inDollarQuote) {
            inDollarQuote = true;
            dollarQuoteDelim = delimMatch[0];
            i += delimMatch[0].length - 1;
            currentStmt += delimMatch[0].substring(1);
          } else if (ddlContent.substring(i, i + dollarQuoteDelim.length) === dollarQuoteDelim) {
            inDollarQuote = false;
            i += dollarQuoteDelim.length - 1;
            currentStmt += dollarQuoteDelim.substring(1);
            dollarQuoteDelim = '';
          }
        }
      }
      
      // Check for statement terminator
      if (char === ';' && !inDollarQuote) {
        statements.push(currentStmt.trim());
        currentStmt = '';
      }
    }
    
    // Add any remaining statement
    if (currentStmt.trim()) {
      statements.push(currentStmt.trim());
    }
    
    // Ensure only one statement
    if (statements.length > 1) {
      return {
        result: 'error',
        error: 'multiple_statements',
        statement_count: statements.length,
        message: 'Stage file must contain exactly one DDL statement'
      };
    }
    
    // Now delegate to DDL_DEPLOY with the content
    var deployStmt = SF.createStatement({
      sqlText: "CALL MCP.DDL_DEPLOY(?, ?, ?, ?, ?, ?, ?)",
      binds: [
        OBJECT_TYPE,
        OBJECT_NAME,
        ddlContent,
        PROVENANCE,
        REASON + ' (from stage: ' + STAGE_URL + ')',
        EXPECTED_VERSION,
        LEASE_ID
      ]
    });
    
    var deployRS = deployStmt.execute();
    deployRS.next();
    var result = deployRS.getColumnValue(1);
    
    // Add stage metadata to result
    if (result.result === 'ok') {
      result.stage_url = STAGE_URL;
      result.file_size = fileSize;
      result.md5_validated = true;
    }
    
    // Log stage deployment event
    var stageEvent = {
      event_id: SF.createStatement({sqlText: "SELECT UUID_STRING()"}).execute().getColumnValue(1),
      action: 'ddl.stage.deployed',
      occurred_at: new Date().toISOString(),
      actor_id: PROVENANCE,
      attributes: {
        stage_url: STAGE_URL,
        md5: fileMD5,
        file_size: fileSize,
        object_name: OBJECT_NAME,
        deployment_result: result
      }
    };
    
    SF.createStatement({
      sqlText: "INSERT INTO LANDING.RAW_EVENTS (PAYLOAD, _SOURCE_LANE, _RECV_AT) SELECT PARSE_JSON(?), 'DDL_STAGE', CURRENT_TIMESTAMP()",
      binds: [JSON.stringify(stageEvent)]
    }).execute();
    
    return result;
    
  } catch (err) {
    return {
      result: 'error',
      error: err.toString(),
      stage_url: STAGE_URL,
      object: OBJECT_NAME
    };
  }
$$;

-- Grant execute permission
GRANT EXECUTE ON PROCEDURE MCP.DDL_DEPLOY_FROM_STAGE(STRING, STRING, STRING, STRING, STRING, STRING, STRING, STRING) TO ROLE CLAUDE_AGENT_ROLE;

SELECT 'DDL_DEPLOY_FROM_STAGE created with checksum validation' as status;