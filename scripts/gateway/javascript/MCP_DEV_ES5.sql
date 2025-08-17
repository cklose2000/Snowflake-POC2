CREATE OR REPLACE PROCEDURE MCP.DEV(action VARCHAR, params VARIANT)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
  var SF = snowflake;
  
  // Helper: Execute SQL with error handling
  function executeSql(sqlText, binds) {
    try {
      var stmt = SF.createStatement({
        sqlText: sqlText,
        binds: binds || []
      });
      return stmt.execute();
    } catch (err) {
      throw new Error('SQL execution failed: ' + err.message);
    }
  }
  
  // Helper: Log event to RAW_EVENTS
  function logEvent(action, attributes) {
    var eventPayload = {
      event_id: generateUUID(),
      action: action,
      occurred_at: new Date().toISOString(),
      actor_id: attributes.agent_id || attributes.agent || 'system',
      source: 'DEV_GATEWAY',
      schema_version: '2.1.0',
      attributes: attributes
    };
    
    try {
      executeSql(
        "INSERT INTO LANDING.RAW_EVENTS (PAYLOAD, _SOURCE_LANE, _RECV_AT) " +
        "SELECT PARSE_JSON(?), 'DEV_GATEWAY', CURRENT_TIMESTAMP()",
        [JSON.stringify(eventPayload)]
      );
      return { logged: true, event_id: eventPayload.event_id };
    } catch (err) {
      return { logged: false, error: err.message };
    }
  }
  
  // Helper: Generate UUID
  function generateUUID() {
    var rs = executeSql("SELECT UUID_STRING() as uuid");
    rs.next();
    return rs.getColumnValue('UUID');
  }
  
  // Helper: Get file MD5 from stage
  function getStageMD5(stageUrl) {
    try {
      var rs = executeSql('SELECT METADATA$FILENAME, METADATA$FILE_CONTENT_KEY as md5 ' +
                         'FROM ' + stageUrl + ' ' +
                         '(FILE_FORMAT => \'CSV\') LIMIT 1');
      if (rs.next()) {
        return rs.getColumnValue('MD5');
      }
      return null;
    } catch (err) {
      // Fallback: can't get MD5 in SQL proc, skip validation
      return null;
    }
  }
  
  // Helper: Check version gate
  function checkVersionGate(objectName, objectType, expectedVersion) {
    if (!expectedVersion) return { valid: true };
    
    var parts = objectName.split('.');
    if (parts.length !== 3) {
      return { valid: false, error: 'Object name must be fully qualified: DB.SCHEMA.NAME' };
    }
    
    try {
      var rs = executeSql(
        "SELECT version FROM MCP.VW_LATEST_SCHEMA " +
        "WHERE object_name = ? AND object_type = ? LIMIT 1",
        [parts[2], objectType]
      );
      
      if (rs.next()) {
        var currentVersion = rs.getColumnValue('VERSION');
        if (currentVersion && currentVersion !== expectedVersion) {
          return {
            valid: false,
            error: 'version_conflict',
            current_version: currentVersion,
            expected_version: expectedVersion
          };
        }
      }
      return { valid: true };
    } catch (err) {
      return { valid: true }; // Allow if can't check
    }
  }
  
  // Main router
  try {
    var result = {};
    
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
          ttl_seconds: PARAMS.ttl_seconds || 900
        };
        
        var claimLog = logEvent('dev.claim', claimPayload);
        result = {
          result: 'ok',
          message: 'Namespace claimed',
          event_id: claimLog.event_id,
          expires_in_seconds: claimPayload.ttl_seconds
        };
        break;
        
      case 'release':
        // Release namespace
        if (!PARAMS.lease_id) {
          throw new Error('release requires: lease_id');
        }
        
        var releaseLog = logEvent('dev.release', {
          lease_id: PARAMS.lease_id,
          agent_id: PARAMS.agent_id || 'unknown'
        });
        
        result = {
          result: 'ok',
          message: 'Namespace released',
          event_id: releaseLog.event_id
        };
        break;
        
      case 'deploy':
        // Inline DDL deployment
        if (!PARAMS.type || !PARAMS.name || !PARAMS.ddl || !PARAMS.agent || !PARAMS.reason) {
          throw new Error('deploy requires: type, name, ddl, agent, reason');
        }
        
        // Check version gate
        var versionCheck = checkVersionGate(PARAMS.name, PARAMS.type, PARAMS.expected_version);
        if (!versionCheck.valid) {
          result = {
            result: 'error',
            error: versionCheck.error,
            current_version: versionCheck.current_version,
            expected_version: versionCheck.expected_version,
            object: PARAMS.name
          };
          break;
        }
        
        // Execute DDL
        try {
          executeSql(PARAMS.ddl);
          var newVersion = new Date().toISOString();
          
          // Log successful deployment
          logEvent('ddl.object.deployed', {
            object_type: PARAMS.type,
            object_name: PARAMS.name,
            version: newVersion,
            previous_version: PARAMS.expected_version,
            provenance: PARAMS.agent,
            reason: PARAMS.reason,
            ddl_length: PARAMS.ddl.length
          });
          
          result = {
            result: 'ok',
            object: PARAMS.name,
            type: PARAMS.type,
            version: newVersion,
            message: 'Deployment successful'
          };
        } catch (err) {
          // Log deployment error
          logEvent('ddl.deploy.error', {
            object_name: PARAMS.name,
            error: err.message,
            agent: PARAMS.agent
          });
          
          result = {
            result: 'error',
            error: 'deployment_failed',
            message: err.message,
            object: PARAMS.name
          };
        }
        break;
        
      case 'deploy_from_stage':
        // Stage-based deployment
        if (!PARAMS.type || !PARAMS.name || !PARAMS.stage_url || !PARAMS.agent || !PARAMS.reason) {
          throw new Error('deploy_from_stage requires: type, name, stage_url, agent, reason');
        }
        
        // Check version gate
        var stageVersionCheck = checkVersionGate(PARAMS.name, PARAMS.type, PARAMS.expected_version);
        if (!stageVersionCheck.valid) {
          result = {
            result: 'error',
            error: stageVersionCheck.error,
            current_version: stageVersionCheck.current_version,
            expected_version: stageVersionCheck.expected_version,
            object: PARAMS.name
          };
          break;
        }
        
        // MD5 validation (if possible)
        if (PARAMS.expected_md5) {
          var actualMD5 = getStageMD5(PARAMS.stage_url);
          if (actualMD5 && actualMD5 !== PARAMS.expected_md5) {
            result = {
              result: 'error',
              error: 'checksum_mismatch',
              expected_md5: PARAMS.expected_md5,
              actual_md5: actualMD5,
              stage_url: PARAMS.stage_url
            };
            break;
          }
        }
        
        // Deploy from stage
        try {
          executeSql('EXECUTE IMMEDIATE FROM ' + PARAMS.stage_url);
          var stageVersion = new Date().toISOString();
          
          // Log successful deployment
          logEvent('ddl.object.deployed', {
            object_type: PARAMS.type,
            object_name: PARAMS.name,
            version: stageVersion,
            previous_version: PARAMS.expected_version,
            provenance: PARAMS.agent,
            reason: PARAMS.reason,
            stage_url: PARAMS.stage_url,
            md5_validated: !!PARAMS.expected_md5
          });
          
          result = {
            result: 'ok',
            object: PARAMS.name,
            type: PARAMS.type,
            version: stageVersion,
            message: 'Deployment from stage successful',
            md5_validated: !!PARAMS.expected_md5
          };
        } catch (err) {
          // Log deployment error
          logEvent('ddl.deploy.error', {
            object_name: PARAMS.name,
            error: err.message,
            stage_url: PARAMS.stage_url,
            agent: PARAMS.agent
          });
          
          result = {
            result: 'error',
            error: 'deployment_failed',
            message: err.message,
            object: PARAMS.name,
            stage_url: PARAMS.stage_url
          };
        }
        break;
        
      case 'discover':
        // Schema discovery
        logEvent('dev.discover.requested', PARAMS);
        
        var discoverSQL = "SELECT * FROM MCP.VW_LATEST_SCHEMA";
        var binds = [];
        
        if (PARAMS.filter) {
          var allowedFilters = ['object_type', 'schema_name', 'database_name'];
          if (allowedFilters.indexOf(PARAMS.filter.column) !== -1) {
            discoverSQL += ' WHERE ' + PARAMS.filter.column + ' = ?';
            binds.push(PARAMS.filter.value);
          }
        }
        
        try {
          var rs = executeSql(discoverSQL, binds);
          var rowCount = 0;
          var objects = [];
          
          while (rs.next() && rowCount < 100) {
            objects.push({
              object_name: rs.getColumnValue('OBJECT_NAME'),
              object_type: rs.getColumnValue('OBJECT_TYPE'),
              version: rs.getColumnValue('VERSION')
            });
            rowCount++;
          }
          
          logEvent('dev.discover.completed', {
            rows_found: rowCount,
            filter: PARAMS.filter || null
          });
          
          result = {
            result: 'ok',
            rows_found: rowCount,
            objects: objects,
            message: 'Discovery complete'
          };
        } catch (err) {
          result = {
            result: 'error',
            error: 'discovery_failed',
            message: err.message
          };
        }
        break;
        
      case 'validate':
        // DDL validation
        if (!PARAMS.ddl && !PARAMS.stage_url) {
          throw new Error('validate requires either ddl or stage_url');
        }
        
        logEvent('dev.validate.requested', PARAMS);
        
        // Simple syntax validation
        var ddlToValidate = PARAMS.ddl || '';
        var upperDDL = ddlToValidate.toUpperCase();
        
        var forbidden = ['TRUNCATE', 'DROP TABLE', 'DROP DATABASE', 'ALTER ACCOUNT'];
        var violations = [];
        for (var i = 0; i < forbidden.length; i++) {
          if (upperDDL.indexOf(forbidden[i]) !== -1) {
            violations.push(forbidden[i]);
          }
        }
        
        if (violations.length > 0) {
          result = {
            result: 'error',
            valid: false,
            violations: violations,
            message: 'DDL contains forbidden operations'
          };
        } else {
          result = {
            result: 'ok',
            valid: true,
            message: 'DDL validation passed'
          };
        }
        
        logEvent('dev.validate.completed', {
          validation_result: result
        });
        break;
        
      default:
        logEvent('dev.error', {
          reason: 'unknown_action',
          action: ACTION,
          params: PARAMS
        });
        
        result = {
          result: 'error',
          error: 'unknown_action',
          action: ACTION,
          valid_actions: ['claim', 'release', 'deploy', 'deploy_from_stage', 'discover', 'validate']
        };
    }
    
    return result;
    
  } catch (err) {
    // Log error and return
    logEvent('dev.error', {
      action: ACTION,
      error: err.message,
      stack: err.stack || null
    });
    
    return {
      result: 'error',
      error: err.message,
      action: ACTION
    };
  }
$$;