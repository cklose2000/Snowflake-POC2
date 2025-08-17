-- DDL_DEPLOY: Deploy inline DDL with version gating
CREATE OR REPLACE PROCEDURE MCP.DDL_DEPLOY(
  object_type STRING,      -- VIEW, PROCEDURE, FUNCTION
  object_name STRING,      -- Fully qualified: DB.SCHEMA.NAME
  ddl TEXT,               -- The DDL to execute
  provenance STRING,      -- Who/what is deploying (agent_id)
  reason STRING,          -- Why this deployment
  expected_version STRING DEFAULT NULL  -- For optimistic concurrency
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
DECLARE
  cur_version STRING;
  db_name STRING;
  schema_name STRING;
  obj_name STRING;
  event_id STRING;
  new_version STRING;
  result_msg STRING;
BEGIN
  -- Parse object name
  db_name := SPLIT_PART(object_name, '.', 1);
  schema_name := SPLIT_PART(object_name, '.', 2);
  obj_name := SPLIT_PART(object_name, '.', 3);
  
  -- Check version gate if provided
  IF (expected_version IS NOT NULL) THEN
    SELECT version INTO :cur_version
    FROM MCP.VW_LATEST_SCHEMA
    WHERE object_name = :obj_name
      AND object_type = :object_type
    LIMIT 1;
    
    IF (cur_version IS NOT NULL AND cur_version != expected_version) THEN
      RETURN OBJECT_CONSTRUCT(
        'result', 'error',
        'error', 'version_conflict',
        'current_version', cur_version,
        'expected_version', expected_version,
        'object', object_name
      );
    END IF;
  END IF;
  
  -- Execute the DDL
  BEGIN
    EXECUTE IMMEDIATE :ddl;
    result_msg := 'Deployment successful';
  EXCEPTION
    WHEN OTHER THEN
      RETURN OBJECT_CONSTRUCT(
        'result', 'error',
        'error', 'deployment_failed',
        'message', SQLERRM,
        'sqlcode', SQLCODE,
        'object', object_name
      );
  END;
  
  -- Generate new version
  new_version := CURRENT_TIMESTAMP()::STRING;
  event_id := UUID_STRING();
  
  -- Log deployment event
  INSERT INTO LANDING.RAW_EVENTS (PAYLOAD, _SOURCE_LANE, _RECV_AT)
  SELECT OBJECT_CONSTRUCT(
    'event_id', :event_id,
    'action', 'ddl.object.deployed',
    'occurred_at', CURRENT_TIMESTAMP()::STRING,
    'actor_id', :provenance,
    'source', 'DDL_GATEWAY',
    'object', OBJECT_CONSTRUCT('type', 'ddl_object', 'id', :object_name),
    'attributes', OBJECT_CONSTRUCT(
      'object_type', :object_type,
      'object_name', :object_name,
      'database_name', :db_name,
      'schema_name', :schema_name,
      'version', :new_version,
      'previous_version', :expected_version,
      'provenance', :provenance,
      'reason', :reason,
      'ddl_length', LENGTH(:ddl)
    )
  ), 'DDL_DEPLOY', CURRENT_TIMESTAMP();
  
  RETURN OBJECT_CONSTRUCT(
    'result', 'ok',
    'object', object_name,
    'type', object_type,
    'version', new_version,
    'message', result_msg
  );
END;