-- MCP.DEV: SQL-based router for development operations
CREATE OR REPLACE PROCEDURE MCP.DEV(
  action VARCHAR,
  params VARIANT
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
DECLARE
  res VARIANT;
  event_id STRING;
  deploy_result VARIANT;
BEGIN
  -- Generate event ID for logging
  event_id := UUID_STRING();
  
  -- Route based on action
  CASE :action
    WHEN 'claim' THEN
      -- Log namespace claim event
      INSERT INTO LANDING.RAW_EVENTS (PAYLOAD, _SOURCE_LANE, _RECV_AT)
      SELECT OBJECT_CONSTRUCT(
        'event_id', :event_id,
        'action', 'dev.claim',
        'occurred_at', CURRENT_TIMESTAMP()::STRING,
        'actor_id', GET(:params, 'agent_id')::STRING,
        'source', 'DEV_GATEWAY',
        'attributes', OBJECT_CONSTRUCT(
          'app_name', GET(:params, 'app_name')::STRING,
          'namespace', GET(:params, 'namespace')::STRING,
          'agent_id', GET(:params, 'agent_id')::STRING,
          'lease_id', GET(:params, 'lease_id')::STRING,
          'ttl_seconds', COALESCE(GET(:params, 'ttl_seconds')::NUMBER, 900)
        )
      ), 'DEV_GATEWAY', CURRENT_TIMESTAMP();
      
      res := OBJECT_CONSTRUCT(
        'result', 'ok',
        'message', 'Namespace claimed',
        'event_id', :event_id,
        'expires_in_seconds', COALESCE(GET(:params, 'ttl_seconds')::NUMBER, 900)
      );
      
    WHEN 'release' THEN
      -- Log namespace release event
      INSERT INTO LANDING.RAW_EVENTS (PAYLOAD, _SOURCE_LANE, _RECV_AT)
      SELECT OBJECT_CONSTRUCT(
        'event_id', :event_id,
        'action', 'dev.release',
        'occurred_at', CURRENT_TIMESTAMP()::STRING,
        'actor_id', GET(:params, 'agent_id')::STRING,
        'source', 'DEV_GATEWAY',
        'attributes', OBJECT_CONSTRUCT(
          'lease_id', GET(:params, 'lease_id')::STRING,
          'agent_id', GET(:params, 'agent_id')::STRING
        )
      ), 'DEV_GATEWAY', CURRENT_TIMESTAMP();
      
      res := OBJECT_CONSTRUCT(
        'result', 'ok',
        'message', 'Namespace released',
        'event_id', :event_id
      );
      
    WHEN 'deploy_from_stage' THEN
      -- Call DDL_DEPLOY_FROM_STAGE
      CALL MCP.DDL_DEPLOY_FROM_STAGE(
        GET(:params, 'type')::STRING,
        GET(:params, 'name')::STRING,
        GET(:params, 'stage_url')::STRING,
        GET(:params, 'agent')::STRING,
        GET(:params, 'reason')::STRING,
        GET(:params, 'expected_version')::STRING,
        GET(:params, 'expected_md5')::STRING
      ) INTO deploy_result;
      
      res := deploy_result;
      
    WHEN 'deploy' THEN
      -- Call DDL_DEPLOY for inline DDL
      CALL MCP.DDL_DEPLOY(
        GET(:params, 'type')::STRING,
        GET(:params, 'name')::STRING,
        GET(:params, 'ddl')::STRING,
        GET(:params, 'agent')::STRING,
        GET(:params, 'reason')::STRING,
        GET(:params, 'expected_version')::STRING
      ) INTO deploy_result;
      
      res := deploy_result;
      
    WHEN 'discover' THEN
      -- Log discovery request
      INSERT INTO LANDING.RAW_EVENTS (PAYLOAD, _SOURCE_LANE, _RECV_AT)
      SELECT OBJECT_CONSTRUCT(
        'event_id', :event_id,
        'action', 'dev.discover',
        'occurred_at', CURRENT_TIMESTAMP()::STRING,
        'actor_id', GET(:params, 'agent_id')::STRING,
        'source', 'DEV_GATEWAY',
        'attributes', :params
      ), 'DEV_GATEWAY', CURRENT_TIMESTAMP();
      
      res := OBJECT_CONSTRUCT(
        'result', 'ok',
        'message', 'Discovery logged - check VW_LATEST_SCHEMA',
        'event_id', :event_id
      );
      
    WHEN 'validate' THEN
      -- Simple validation response (can be enhanced later)
      res := OBJECT_CONSTRUCT(
        'result', 'ok',
        'valid', TRUE,
        'message', 'DDL validation not yet fully implemented',
        'event_id', :event_id
      );
      
    ELSE
      res := OBJECT_CONSTRUCT(
        'result', 'error',
        'error', 'unknown_action',
        'action', :action,
        'valid_actions', ARRAY_CONSTRUCT('claim', 'release', 'deploy', 'deploy_from_stage', 'discover', 'validate')
      );
  END CASE;
  
  RETURN res;
END;