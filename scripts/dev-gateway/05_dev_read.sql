-- ============================================================================
-- DEV_READ - Read-After-Write Consistency Layer
-- Handles event propagation lag between RAW_EVENTS and ACTIVITY.EVENTS
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE CLAUDE_BI;
USE SCHEMA MCP;

-- Drop existing if needed
DROP PROCEDURE IF EXISTS MCP.DEV_READ(VARCHAR, VARIANT);

-- Create the consistency-aware read procedure
CREATE OR REPLACE PROCEDURE MCP.DEV_READ(
  read_type VARCHAR,      -- 'schema', 'namespace', 'activity', 'status'
  params VARIANT         -- Parameters for the read operation
)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
COMMENT = 'Consistency-aware reads with RAW_EVENTS fallback and retry logic'
AS
$$
  var SF = snowflake;
  
  // Helper to execute query with retry
  function executeWithRetry(sqlText, binds, maxAttempts, backoffMs) {
    maxAttempts = maxAttempts || 3;
    backoffMs = backoffMs || 400;
    
    for (var attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        var stmt = SF.createStatement({
          sqlText: sqlText,
          binds: binds || []
        });
        return stmt.execute();
      } catch (err) {
        if (attempt === maxAttempts) {
          throw err;
        }
        // Exponential backoff with jitter
        var waitMs = backoffMs * Math.pow(2, attempt - 1) * (0.5 + Math.random());
        var waitStmt = SF.createStatement({
          sqlText: "CALL SYSTEM$WAIT(?)",
          binds: [Math.floor(waitMs / 1000)]
        });
        waitStmt.execute();
      }
    }
  }
  
  try {
    var result = {};
    var freshWindowMinutes = PARAMS.fresh_window_minutes || 2;  // Default 2 min fresh window
    
    switch (READ_TYPE) {
      case 'schema':
        // Read schema information with fresh check
        var objectName = PARAMS.object_name;
        var objectType = PARAMS.object_type || 'VIEW';
        
        // First check RAW_EVENTS for very recent deployments
        var freshSQL = `
          SELECT 
            PAYLOAD:attributes:object_name::string as object_name,
            PAYLOAD:attributes:version::string as version,
            PAYLOAD:occurred_at::timestamp as deployed_at,
            'RAW_EVENTS' as source
          FROM LANDING.RAW_EVENTS
          WHERE PAYLOAD:action::string = 'ddl.object.deployed'
            AND PAYLOAD:attributes:object_name::string = ?
            AND PAYLOAD:attributes:object_type::string = ?
            AND _RECV_AT >= DATEADD('minute', -?, CURRENT_TIMESTAMP())
          ORDER BY _RECV_AT DESC
          LIMIT 1
        `;
        
        var freshRS = executeWithRetry(freshSQL, [objectName, objectType, freshWindowMinutes], 3, 400);
        
        if (freshRS.next()) {
          result = {
            found: true,
            source: 'RAW_EVENTS',
            object_name: freshRS.getColumnValue('OBJECT_NAME'),
            version: freshRS.getColumnValue('VERSION'),
            deployed_at: freshRS.getColumnValue('DEPLOYED_AT')
          };
        } else {
          // Fall back to VW_LATEST_SCHEMA (based on ACTIVITY.EVENTS)
          var schemaSQL = `
            SELECT 
              object_name,
              object_type,
              version,
              last_updated,
              'VW_LATEST_SCHEMA' as source
            FROM MCP.VW_LATEST_SCHEMA
            WHERE object_name = ?
              AND object_type = ?
            LIMIT 1
          `;
          
          var schemaRS = executeWithRetry(schemaSQL, [objectName, objectType], 3, 400);
          
          if (schemaRS.next()) {
            result = {
              found: true,
              source: 'VW_LATEST_SCHEMA',
              object_name: schemaRS.getColumnValue('OBJECT_NAME'),
              object_type: schemaRS.getColumnValue('OBJECT_TYPE'),
              version: schemaRS.getColumnValue('VERSION'),
              last_updated: schemaRS.getColumnValue('LAST_UPDATED')
            };
          } else {
            result = {
              found: false,
              object_name: objectName,
              message: 'Object not found in schema'
            };
          }
        }
        break;
        
      case 'namespace':
        // Check namespace claims with fresh read
        var appName = PARAMS.app_name;
        var namespace = PARAMS.namespace;
        
        // Check both RAW and processed events
        var namespaceSQL = `
          WITH raw_claims AS (
            SELECT 
              PAYLOAD:attributes:app_name::string as app_name,
              PAYLOAD:attributes:namespace::string as namespace,
              PAYLOAD:attributes:agent_id::string as agent_id,
              PAYLOAD:attributes:lease_id::string as lease_id,
              PAYLOAD:occurred_at::timestamp as claimed_at,
              'RAW' as source
            FROM LANDING.RAW_EVENTS
            WHERE PAYLOAD:action::string = 'dev.claim'
              AND PAYLOAD:attributes:app_name::string = ?
              AND PAYLOAD:attributes:namespace::string = ?
              AND _RECV_AT >= DATEADD('minute', -?, CURRENT_TIMESTAMP())
            ORDER BY _RECV_AT DESC
            LIMIT 1
          ),
          view_claims AS (
            SELECT 
              app_name,
              namespace,
              agent_id,
              lease_id,
              claimed_at,
              'VIEW' as source
            FROM MCP.VW_DEV_NAMESPACES
            WHERE app_name = ?
              AND namespace = ?
          )
          SELECT * FROM raw_claims
          UNION ALL
          SELECT * FROM view_claims
          LIMIT 1
        `;
        
        var namespaceRS = executeWithRetry(
          namespaceSQL, 
          [appName, namespace, freshWindowMinutes, appName, namespace],
          3, 400
        );
        
        if (namespaceRS.next()) {
          result = {
            found: true,
            source: namespaceRS.getColumnValue('SOURCE'),
            app_name: namespaceRS.getColumnValue('APP_NAME'),
            namespace: namespaceRS.getColumnValue('NAMESPACE'),
            agent_id: namespaceRS.getColumnValue('AGENT_ID'),
            lease_id: namespaceRS.getColumnValue('LEASE_ID'),
            claimed_at: namespaceRS.getColumnValue('CLAIMED_AT')
          };
        } else {
          result = {
            found: false,
            app_name: appName,
            namespace: namespace,
            message: 'Namespace not claimed'
          };
        }
        break;
        
      case 'activity':
        // Read recent activity with fresh window
        var agentId = PARAMS.agent_id;
        var limitRows = PARAMS.limit || 10;
        
        var activitySQL = `
          WITH all_activity AS (
            -- Fresh from RAW_EVENTS
            SELECT 
              PAYLOAD:occurred_at::timestamp as occurred_at,
              PAYLOAD:action::string as action,
              PAYLOAD:attributes:result:result::string as result,
              PAYLOAD:attributes:object_name::string as object_name,
              'RAW' as source
            FROM LANDING.RAW_EVENTS
            WHERE PAYLOAD:actor_id::string = ?
              AND PAYLOAD:action::string LIKE 'dev.%'
              AND _RECV_AT >= DATEADD('minute', -?, CURRENT_TIMESTAMP())
            
            UNION ALL
            
            -- Older from view
            SELECT 
              occurred_at,
              action,
              result,
              object_name,
              'VIEW' as source
            FROM MCP.VW_DEV_ACTIVITY
            WHERE actor_id = ?
              AND occurred_at < DATEADD('minute', -?, CURRENT_TIMESTAMP())
          )
          SELECT * FROM all_activity
          ORDER BY occurred_at DESC
          LIMIT ?
        `;
        
        var activityRS = executeWithRetry(
          activitySQL,
          [agentId, freshWindowMinutes, agentId, freshWindowMinutes, limitRows],
          3, 400
        );
        
        var activities = [];
        while (activityRS.next()) {
          activities.push({
            occurred_at: activityRS.getColumnValue('OCCURRED_AT'),
            action: activityRS.getColumnValue('ACTION'),
            result: activityRS.getColumnValue('RESULT'),
            object_name: activityRS.getColumnValue('OBJECT_NAME'),
            source: activityRS.getColumnValue('SOURCE')
          });
        }
        
        result = {
          count: activities.length,
          agent_id: agentId,
          activities: activities
        };
        break;
        
      case 'status':
        // Get app/agent status with consistency
        var statusSQL = `
          SELECT 
            app_name,
            namespace,
            agent_id,
            expires_in_seconds,
            objects_deployed,
            successful_deploys,
            failed_deploys
          FROM MCP.VW_APP_STATUS
        `;
        
        if (PARAMS.app_name) {
          statusSQL += " WHERE app_name = ?";
        }
        
        var statusRS = executeWithRetry(
          statusSQL,
          PARAMS.app_name ? [PARAMS.app_name] : [],
          3, 400
        );
        
        var statuses = [];
        while (statusRS.next()) {
          statuses.push({
            app_name: statusRS.getColumnValue('APP_NAME'),
            namespace: statusRS.getColumnValue('NAMESPACE'),
            agent_id: statusRS.getColumnValue('AGENT_ID'),
            expires_in_seconds: statusRS.getColumnValue('EXPIRES_IN_SECONDS'),
            objects_deployed: statusRS.getColumnValue('OBJECTS_DEPLOYED'),
            successful_deploys: statusRS.getColumnValue('SUCCESSFUL_DEPLOYS'),
            failed_deploys: statusRS.getColumnValue('FAILED_DEPLOYS')
          });
        }
        
        result = {
          count: statuses.length,
          statuses: statuses
        };
        break;
        
      default:
        result = {
          error: 'unknown_read_type',
          valid_types: ['schema', 'namespace', 'activity', 'status']
        };
    }
    
    return result;
    
  } catch (err) {
    return {
      result: 'error',
      error: err.toString(),
      read_type: READ_TYPE
    };
  }
$$;

-- Grant execute permission
GRANT EXECUTE ON PROCEDURE MCP.DEV_READ(VARCHAR, VARIANT) TO ROLE CLAUDE_AGENT_ROLE;

SELECT 'DEV_READ consistency layer created' as status;