-- ============================================================================
-- 12_safe_ddl_wrapper.sql
-- SAFE_DDL: The ONLY way agents can modify database objects
-- Unbypassable, race-proof, self-healing DDL management
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE CLAUDE_BI;
USE SCHEMA MCP;

-- ============================================================================
-- SAFE_DDL: Single Entry Point for All DDL Operations
-- ============================================================================
CREATE OR REPLACE PROCEDURE MCP.SAFE_DDL(
  ddl_text STRING,
  reason STRING DEFAULT NULL
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
COMMENT = 'The ONLY way to execute DDL. Enforces versioning, scope, and compliance.'
AS
$$
DECLARE
  -- Session context
  agent_id STRING;
  query_tag STRING;
  
  -- Parsed DDL components
  object_type STRING;
  object_identity STRING;
  schema_name STRING;
  
  -- Versioning
  idempotency_key STRING;
  
  -- Result
  deploy_result VARIANT;
  test_result VARIANT;
  
BEGIN
  -- Get agent/author from session
  SHOW PARAMETERS LIKE 'QUERY_TAG' IN SESSION;
  SELECT "value" INTO :query_tag FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) LIMIT 1;
  
  -- Extract agent ID from query tag or use current user
  SET agent_id = COALESCE(
    REGEXP_SUBSTR(:query_tag, 'agent:([^,]+)', 1, 1, 'i', 1),
    CURRENT_USER()
  );
  
  -- Parse DDL to extract type and identity
  -- Handle: CREATE OR REPLACE [SECURE] VIEW|PROCEDURE|FUNCTION
  SET object_type = UPPER(REGEXP_SUBSTR(
    :ddl_text,
    'CREATE\\s+(?:OR\\s+REPLACE\\s+)?(?:SECURE\\s+)?(VIEW|PROCEDURE|FUNCTION|TABLE FUNCTION)',
    1, 1, 'i', 1
  ));
  
  -- Extract full object identity (schema.name with signature for procs/funcs)
  -- First get the basic name
  SET object_identity = REGEXP_SUBSTR(
    :ddl_text,
    'CREATE\\s+(?:OR\\s+REPLACE\\s+)?(?:SECURE\\s+)?(?:VIEW|PROCEDURE|FUNCTION|TABLE FUNCTION)\\s+([^\\s\\(]+)',
    1, 1, 'i', 1
  );
  
  -- For procedures/functions, append the signature
  IF :object_type IN ('PROCEDURE', 'FUNCTION', 'TABLE FUNCTION') THEN
    -- Extract parameter signature
    LET signature STRING := REGEXP_SUBSTR(
      :ddl_text,
      'CREATE\\s+(?:OR\\s+REPLACE\\s+)?(?:PROCEDURE|FUNCTION|TABLE FUNCTION)\\s+[^\\(]+\\(([^\\)]*)\\)',
      1, 1, 'i', 1
    );
    
    -- Clean up the signature (extract just types)
    -- This is simplified - in production, parse more carefully
    SET object_identity = :object_identity || '(' || COALESCE(:signature, '') || ')';
  END IF;
  
  -- Validate DDL was parseable
  IF :object_type IS NULL OR :object_identity IS NULL THEN
    RETURN OBJECT_CONSTRUCT(
      'result', 'error',
      'error', 'Unable to parse DDL statement',
      'hint', 'Ensure DDL starts with CREATE OR REPLACE [SECURE] VIEW|PROCEDURE|FUNCTION'
    );
  END IF;
  
  -- Extract schema from object identity
  SET schema_name = SPLIT_PART(:object_identity, '.', -2);
  
  -- If no schema specified, assume MCP
  IF :schema_name = :object_identity OR :schema_name = '' THEN
    SET schema_name = 'MCP';
    SET object_identity = 'MCP.' || :object_identity;
  END IF;
  
  -- CRITICAL: Enforce schema scope - ONLY allow MCP schema
  IF :schema_name != 'MCP' THEN
    -- Log violation attempt
    INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS (PAYLOAD, _SOURCE_LANE, _RECV_AT)
    SELECT 
      OBJECT_CONSTRUCT(
        'event_id', UUID_STRING(),
        'action', 'ddl.violation.schema_scope',
        'occurred_at', CURRENT_TIMESTAMP(),
        'actor_id', :agent_id,
        'source', 'SAFE_DDL',
        'object', OBJECT_CONSTRUCT(
          'type', 'VIOLATION',
          'identity', :object_identity
        ),
        'attributes', OBJECT_CONSTRUCT(
          'attempted_schema', :schema_name,
          'allowed_schemas', ARRAY_CONSTRUCT('MCP'),
          'ddl_snippet', LEFT(:ddl_text, 200),
          'reason', :reason
        )
      ),
      'DDL_SECURITY',
      CURRENT_TIMESTAMP();
    
    RETURN OBJECT_CONSTRUCT(
      'result', 'error',
      'error', 'Schema not allowed',
      'attempted_schema', :schema_name,
      'allowed_schemas', ARRAY_CONSTRUCT('MCP'),
      'hint', 'Only MCP schema modifications are permitted through SAFE_DDL'
    );
  END IF;
  
  -- Generate idempotency key from DDL + reason
  SET idempotency_key = SHA2(:ddl_text || COALESCE(:reason, ''));
  
  -- Call the enhanced DDL_DEPLOY with all parameters
  CALL MCP.DDL_DEPLOY_HARDENED(
    :object_type,
    :object_identity,
    :ddl_text,
    :agent_id,
    :reason,
    :idempotency_key,
    NULL,  -- expected_hash (NULL for first deploy)
    'PRODUCTION'
  ) INTO :deploy_result;
  
  -- Check deployment result
  IF :deploy_result:result = 'error' THEN
    RETURN :deploy_result;
  END IF;
  
  -- If deployment successful and tests exist, run them
  IF :deploy_result:result IN ('deployed', 'unchanged') THEN
    -- Check if tests exist for this object
    LET test_count INTEGER := (
      SELECT COUNT(*)
      FROM CLAUDE_BI.ACTIVITY.EVENTS
      WHERE action = 'ddl.object.test'
        AND attributes:object_name::string = :object_identity
    );
    
    IF :test_count > 0 THEN
      -- Run tests
      CALL MCP.DDL_RUN_TESTS(:object_identity) INTO :test_result;
      
      -- If any test failed, rollback
      IF :test_result:failed > 0 THEN
        -- Log test failure
        INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS (PAYLOAD, _SOURCE_LANE, _RECV_AT)
        SELECT 
          OBJECT_CONSTRUCT(
            'event_id', UUID_STRING(),
            'action', 'ddl.test.failed',
            'occurred_at', CURRENT_TIMESTAMP(),
            'actor_id', :agent_id,
            'source', 'SAFE_DDL',
            'object', OBJECT_CONSTRUCT(
              'type', :object_type,
              'identity', :object_identity
            ),
            'attributes', OBJECT_CONSTRUCT(
              'version', :deploy_result:version,
              'tests_passed', :test_result:passed,
              'tests_failed', :test_result:failed,
              'test_results', :test_result:results
            )
          ),
          'DDL_TEST',
          CURRENT_TIMESTAMP();
        
        -- Rollback to previous version
        CALL MCP.DDL_ROLLBACK(:object_identity, NULL) INTO :test_result;
        
        RETURN OBJECT_CONSTRUCT(
          'result', 'error',
          'error', 'Tests failed - deployment rolled back',
          'object_identity', :object_identity,
          'version_attempted', :deploy_result:version,
          'tests_failed', :test_result:failed,
          'rollback_result', :test_result,
          'hint', 'Fix failing tests before redeploying'
        );
      END IF;
      
      -- Log test success
      INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS (PAYLOAD, _SOURCE_LANE, _RECV_AT)
      SELECT 
        OBJECT_CONSTRUCT(
          'event_id', UUID_STRING(),
          'action', 'ddl.test.passed',
          'occurred_at', CURRENT_TIMESTAMP(),
          'actor_id', :agent_id,
          'source', 'SAFE_DDL',
          'object', OBJECT_CONSTRUCT(
            'type', :object_type,
            'identity', :object_identity
          ),
          'attributes', OBJECT_CONSTRUCT(
            'version', :deploy_result:version,
            'tests_passed', :test_result:passed,
            'test_results', :test_result:results
          )
        ),
        'DDL_TEST',
        CURRENT_TIMESTAMP();
    END IF;
  END IF;
  
  -- Return successful result
  RETURN OBJECT_CONSTRUCT(
    'result', 'success',
    'object_type', :object_type,
    'object_identity', :object_identity,
    'version', :deploy_result:version,
    'idempotency_key', :idempotency_key,
    'tests_run', COALESCE(:test_count, 0),
    'tests_passed', COALESCE(:test_result:passed, 0),
    'author', :agent_id,
    'deploy_result', :deploy_result
  );
  
EXCEPTION
  WHEN OTHER THEN
    -- Log the error
    INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS (PAYLOAD, _SOURCE_LANE, _RECV_AT)
    SELECT 
      OBJECT_CONSTRUCT(
        'event_id', UUID_STRING(),
        'action', 'ddl.error',
        'occurred_at', CURRENT_TIMESTAMP(),
        'actor_id', COALESCE(:agent_id, CURRENT_USER()),
        'source', 'SAFE_DDL',
        'object', OBJECT_CONSTRUCT(
          'type', 'ERROR'
        ),
        'attributes', OBJECT_CONSTRUCT(
          'error_message', SQLERRM,
          'error_code', SQLCODE,
          'ddl_snippet', LEFT(:ddl_text, 200)
        )
      ),
      'DDL_ERROR',
      CURRENT_TIMESTAMP();
    
    RETURN OBJECT_CONSTRUCT(
      'result', 'error',
      'error', SQLERRM,
      'error_code', SQLCODE
    );
END;
$$;

-- ============================================================================
-- Grant Permissions
-- ============================================================================
-- Agents can ONLY call SAFE_DDL, nothing else
GRANT USAGE ON PROCEDURE MCP.SAFE_DDL(STRING, STRING) TO ROLE MCP_USER_ROLE;

-- ============================================================================
-- Example Usage for Agents
-- ============================================================================
/*
-- Simple view creation
CALL MCP.SAFE_DDL(
  'CREATE OR REPLACE VIEW MCP.VW_MY_VIEW AS SELECT * FROM ACTIVITY.EVENTS WHERE action = ''test''',
  'Adding test view for demo'
);

-- Procedure with parameters (signature included automatically)
CALL MCP.SAFE_DDL(
  'CREATE OR REPLACE PROCEDURE MCP.MY_PROC(p1 STRING, p2 NUMBER)
   RETURNS STRING
   LANGUAGE SQL
   AS
   $$
   BEGIN
     RETURN p1 || '':'' || p2;
   END;
   $$',
  'Simple concatenation procedure'
);

-- The system will:
-- 1. Parse and validate the DDL
-- 2. Ensure it's in MCP schema only
-- 3. Generate version and idempotency key
-- 4. Deploy with full versioning
-- 5. Run tests if they exist
-- 6. Auto-rollback if tests fail
*/