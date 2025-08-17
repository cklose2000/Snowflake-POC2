-- ============================================================================
-- MCP.DEV Router Procedure - Core Gateway for Development Operations
-- Event-Native, Two-Table Law Compliant, Zero String Concatenation
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE CLAUDE_BI;
USE SCHEMA MCP;

-- Drop existing if needed
DROP PROCEDURE IF EXISTS MCP.DEV(VARCHAR, VARIANT);

-- Create the thin orchestrator that routes all dev operations through events
CREATE OR REPLACE PROCEDURE MCP.DEV(action VARCHAR, params VARIANT)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
COMMENT = 'Event-native development gateway router - orchestrates all dev operations'
AS
$$
  var SF = snowflake;
  
  // Helper function to safely log events
  function logEvent(action, payload) {
    try {
      var eventId = SF.createStatement({
        sqlText: "SELECT UUID_STRING()"
      }).execute().getColumnValue(1);
      
      var eventPayload = {
        EVENT_ID: eventId,
        EVENT_TS: new Date().toISOString(),
        ACTION: action,
        PAYLOAD: payload
      };
      
      var insertStmt = SF.createStatement({
        sqlText: "INSERT INTO LANDING.RAW_EVENTS (PAYLOAD, _SOURCE_LANE, _RECV_AT) SELECT PARSE_JSON(?), 'DEV_GATEWAY', CURRENT_TIMESTAMP()",
        binds: [JSON.stringify(eventPayload)]
      });
      insertStmt.execute();
      
      return { logged: true, event_id: eventId };
    } catch (e) {
      return { logged: false, error: e.toString() };
    }
  }
  
  // Main router logic
  try {
    var result;
    
    switch (ACTION) {
      case 'claim':
        // Namespace claim with TTL
        if (!PARAMS.app_name || !PARAMS.namespace || !PARAMS.agent_id || !PARAMS.lease_id) {
          throw new Error('claim requires: app_name, namespace, agent_id, lease_id');
        }
        
        var claimPayload = {
          app_name: PARAMS.app_name,
          namespace: PARAMS.namespace,
          agent_id: PARAMS.agent_id,
          lease_id: PARAMS.lease_id,
          ttl_seconds: PARAMS.ttl_seconds || 900  // Default 15 minutes
        };
        
        logEvent('dev.claim', claimPayload);
        result = { 
          result: 'ok', 
          message: 'Namespace claimed',
          expires_in_seconds: claimPayload.ttl_seconds 
        };
        break;
        
      case 'deploy':
        // Route to appropriate deploy procedure
        if (!PARAMS.type || !PARAMS.name || !PARAMS.agent || !PARAMS.reason) {
          throw new Error('deploy requires: type, name, agent, reason, and either ddl or stage_url');
        }
        
        var hasStage = !!PARAMS.stage_url;
        var deployStmt;
        
        if (hasStage) {
          // Deploy from stage with checksum validation
          if (!PARAMS.expected_md5) {
            throw new Error('stage_url requires expected_md5 for validation');
          }
          
          deployStmt = SF.createStatement({
            sqlText: "CALL MCP.DDL_DEPLOY_FROM_STAGE(?, ?, ?, ?, ?, ?, ?, ?)",
            binds: [
              PARAMS.type,
              PARAMS.name,
              PARAMS.stage_url,
              PARAMS.expected_md5,
              PARAMS.agent,
              PARAMS.reason,
              PARAMS.expected_version || null,
              PARAMS.lease_id || null
            ]
          });
        } else {
          // Inline DDL deployment
          if (!PARAMS.ddl) {
            throw new Error('deploy requires either ddl or stage_url');
          }
          
          deployStmt = SF.createStatement({
            sqlText: "CALL MCP.DDL_DEPLOY(?, ?, ?, ?, ?, ?, ?)",
            binds: [
              PARAMS.type,
              PARAMS.name,
              PARAMS.ddl,
              PARAMS.agent,
              PARAMS.reason,
              PARAMS.expected_version || null,
              PARAMS.lease_id || null
            ]
          });
        }
        
        var deployRS = deployStmt.execute();
        deployRS.next();
        result = deployRS.getColumnValue(1);
        
        // Log deployment event
        logEvent('dev.deployed', {
          type: PARAMS.type,
          name: PARAMS.name,
          agent: PARAMS.agent,
          reason: PARAMS.reason,
          result: result,
          used_stage: hasStage
        });
        break;
        
      case 'discover':
        // Schema discovery
        logEvent('dev.discover.requested', PARAMS);
        
        var discoverSQL = "SELECT * FROM MCP.VW_LATEST_SCHEMA";
        if (PARAMS.filter) {
          // Safe filtering - only allow specific columns
          var allowedFilters = ['object_type', 'schema_name', 'status'];
          if (allowedFilters.indexOf(PARAMS.filter.column) >= 0) {
            discoverSQL = "SELECT * FROM MCP.VW_LATEST_SCHEMA WHERE " + 
                         PARAMS.filter.column + " = ?";
          }
        }
        
        var discoverStmt = SF.createStatement({
          sqlText: discoverSQL,
          binds: PARAMS.filter ? [PARAMS.filter.value] : []
        });
        
        var discoverRS = discoverStmt.execute();
        var rowCount = 0;
        while (discoverRS.next()) {
          rowCount++;
        }
        
        logEvent('dev.discover.completed', { 
          rows_found: rowCount,
          filter: PARAMS.filter || null 
        });
        
        result = { 
          result: 'ok', 
          rows_found: rowCount,
          message: 'Discovery complete - check VW_LATEST_SCHEMA' 
        };
        break;
        
      case 'validate':
        // Validate DDL without execution
        logEvent('dev.validate.requested', PARAMS);
        
        if (!PARAMS.ddl && !PARAMS.stage_url) {
          throw new Error('validate requires either ddl or stage_url');
        }
        
        // This will be implemented in DDL_VALIDATE procedure
        var validateStmt = SF.createStatement({
          sqlText: "CALL MCP.DDL_VALIDATE(?, ?)",
          binds: [
            PARAMS.ddl || null,
            PARAMS.stage_url || null
          ]
        });
        
        var validateRS = validateStmt.execute();
        validateRS.next();
        result = validateRS.getColumnValue(1);
        
        logEvent('dev.validate.completed', { 
          validation_result: result 
        });
        break;
        
      case 'release':
        // Release a namespace claim
        if (!PARAMS.lease_id) {
          throw new Error('release requires: lease_id');
        }
        
        logEvent('dev.release', {
          lease_id: PARAMS.lease_id,
          agent_id: PARAMS.agent_id || 'unknown'
        });
        
        result = { 
          result: 'ok', 
          message: 'Lease released' 
        };
        break;
        
      default:
        logEvent('dev.error', { 
          reason: 'unknown_action', 
          action: ACTION,
          params: PARAMS 
        });
        
        result = { 
          result: 'error', 
          error: 'Unknown action: ' + ACTION,
          valid_actions: ['claim', 'deploy', 'discover', 'validate', 'release']
        };
    }
    
    return result;
    
  } catch (err) {
    // Log error and return structured response
    logEvent('dev.error', {
      action: ACTION,
      error: err.toString(),
      stack: err.stack || null
    });
    
    return {
      result: 'error',
      error: err.toString(),
      action: ACTION
    };
  }
$$;

-- Grant execute permission to agent role
GRANT EXECUTE ON PROCEDURE MCP.DEV(VARCHAR, VARIANT) TO ROLE CLAUDE_AGENT_ROLE;

-- Show what we created
SELECT 'MCP.DEV router created successfully' as status;