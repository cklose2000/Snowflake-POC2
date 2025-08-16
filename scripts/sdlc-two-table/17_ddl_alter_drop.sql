-- ============================================================================
-- 17_ddl_alter_drop.sql
-- SAFE_ALTER and SAFE_DROP wrappers for complete DDL control
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE CLAUDE_BI;
USE SCHEMA MCP;

-- ============================================================================
-- SAFE_ALTER: Wrapper for ALTER operations
-- ============================================================================
CREATE OR REPLACE PROCEDURE MCP.SAFE_ALTER(
  ddl_text STRING,
  reason STRING DEFAULT NULL
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
COMMENT = 'Safe wrapper for ALTER operations - enforces versioning'
AS
$$
DECLARE
  object_identity STRING;
  object_type STRING;
  current_ddl STRING;
  new_ddl STRING;
  result VARIANT;
BEGIN
  -- Parse ALTER statement
  SET object_type = UPPER(REGEXP_SUBSTR(
    :ddl_text,
    'ALTER\\s+(VIEW|PROCEDURE|FUNCTION|TABLE FUNCTION)',
    1, 1, 'i', 1
  ));
  
  SET object_identity = REGEXP_SUBSTR(
    :ddl_text,
    'ALTER\\s+(?:VIEW|PROCEDURE|FUNCTION|TABLE FUNCTION)\\s+([^\\s]+)',
    1, 1, 'i', 1
  );
  
  -- Validate parsing
  IF :object_type IS NULL OR :object_identity IS NULL THEN
    RETURN OBJECT_CONSTRUCT(
      'result', 'error',
      'error', 'Unable to parse ALTER statement'
    );
  END IF;
  
  -- Get current DDL
  LET get_ddl_result STRING;
  BEGIN
    SET get_ddl_result = (SELECT GET_DDL(:object_type, :object_identity));
  EXCEPTION
    WHEN OTHER THEN
      RETURN OBJECT_CONSTRUCT(
        'result', 'error',
        'error', 'Object not found: ' || :object_identity
      );
  END;
  
  -- Apply ALTER to get new DDL
  -- For now, we'll convert ALTER to CREATE OR REPLACE
  -- In production, parse the ALTER more carefully
  SET new_ddl = REGEXP_REPLACE(
    :ddl_text,
    'ALTER\\s+(' || :object_type || ')',
    'CREATE OR REPLACE \\1',
    1, 1, 'i'
  );
  
  -- Call SAFE_DDL with the CREATE OR REPLACE version
  CALL MCP.SAFE_DDL(:new_ddl, 'ALTER: ' || COALESCE(:reason, 'No reason provided')) INTO :result;
  
  RETURN :result;
END;
$$;

-- ============================================================================
-- SAFE_DROP: Wrapper for DROP operations with soft delete
-- ============================================================================
CREATE OR REPLACE PROCEDURE MCP.SAFE_DROP(
  object_type STRING,
  object_identity STRING,
  reason STRING DEFAULT NULL,
  hard_delete BOOLEAN DEFAULT FALSE
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
COMMENT = 'Safe wrapper for DROP operations - maintains version history'
AS
$$
DECLARE
  agent_id STRING;
  idempotency_key STRING;
  current_ddl STRING;
  backup_name STRING;
BEGIN
  -- Get agent ID
  SET agent_id = CURRENT_USER();
  
  -- Generate idempotency key
  SET idempotency_key = SHA2('DROP:' || :object_identity || COALESCE(:reason, ''));
  
  -- Check if object exists
  LET exists_check INTEGER := (
    SELECT COUNT(*)
    FROM CLAUDE_BI.ACTIVITY.EVENTS
    WHERE action IN ('ddl.object.create', 'ddl.object.alter')
      AND attributes:object_identity::string = :object_identity
      AND action != 'ddl.object.drop'
  );
  
  IF :exists_check = 0 THEN
    RETURN OBJECT_CONSTRUCT(
      'result', 'error',
      'error', 'Object not found or already dropped',
      'object_identity', :object_identity
    );
  END IF;
  
  -- Get current DDL for archival
  BEGIN
    SET current_ddl = (SELECT GET_DDL(:object_type, :object_identity));
  EXCEPTION
    WHEN OTHER THEN
      -- Object might already be dropped
      SET current_ddl = NULL;
  END;
  
  IF NOT :hard_delete AND :current_ddl IS NOT NULL THEN
    -- Soft delete: rename object with timestamp
    SET backup_name = :object_identity || '_DROPPED_' || 
                      TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MISS');
    
    -- Rename instead of drop
    LET rename_sql STRING := 'ALTER ' || :object_type || ' ' || 
                             :object_identity || ' RENAME TO ' || :backup_name;
    
    EXECUTE IMMEDIATE :rename_sql;
    
    -- Log soft delete
    INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS (PAYLOAD, _SOURCE_LANE, _RECV_AT)
    SELECT 
      OBJECT_CONSTRUCT(
        'event_id', UUID_STRING(),
        'action', 'ddl.object.soft_delete',
        'occurred_at', CURRENT_TIMESTAMP(),
        'actor_id', :agent_id,
        'source', 'SAFE_DROP',
        'object', OBJECT_CONSTRUCT(
          'type', :object_type,
          'identity', :object_identity
        ),
        'attributes', OBJECT_CONSTRUCT(
          'object_type', :object_type,
          'object_identity', :object_identity,
          'backup_name', :backup_name,
          'archived_ddl', :current_ddl,
          'reason', :reason,
          'idempotency_key', :idempotency_key,
          'recoverable', true
        )
      ),
      'DDL_DROP',
      CURRENT_TIMESTAMP();
      
  ELSE
    -- Hard delete: actually drop the object
    IF :current_ddl IS NOT NULL THEN
      EXECUTE IMMEDIATE 'DROP ' || :object_type || ' ' || :object_identity;
    END IF;
    
    -- Log hard delete
    INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS (PAYLOAD, _SOURCE_LANE, _RECV_AT)
    SELECT 
      OBJECT_CONSTRUCT(
        'event_id', UUID_STRING(),
        'action', 'ddl.object.drop',
        'occurred_at', CURRENT_TIMESTAMP(),
        'actor_id', :agent_id,
        'source', 'SAFE_DROP',
        'object', OBJECT_CONSTRUCT(
          'type', :object_type,
          'identity', :object_identity
        ),
        'attributes', OBJECT_CONSTRUCT(
          'object_type', :object_type,
          'object_identity', :object_identity,
          'final_ddl', :current_ddl,
          'reason', :reason,
          'idempotency_key', :idempotency_key,
          'recoverable', false
        )
      ),
      'DDL_DROP',
      CURRENT_TIMESTAMP();
  END IF;
  
  RETURN OBJECT_CONSTRUCT(
    'result', 'success',
    'operation', IFF(:hard_delete, 'hard_delete', 'soft_delete'),
    'object_identity', :object_identity,
    'backup_name', :backup_name,
    'recoverable', NOT :hard_delete
  );
  
EXCEPTION
  WHEN OTHER THEN
    -- Log error
    INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS (PAYLOAD, _SOURCE_LANE, _RECV_AT)
    SELECT 
      OBJECT_CONSTRUCT(
        'event_id', UUID_STRING(),
        'action', 'ddl.drop.error',
        'occurred_at', CURRENT_TIMESTAMP(),
        'actor_id', :agent_id,
        'source', 'SAFE_DROP',
        'object', OBJECT_CONSTRUCT(
          'type', 'ERROR'
        ),
        'attributes', OBJECT_CONSTRUCT(
          'error', SQLERRM,
          'object_identity', :object_identity
        )
      ),
      'DDL_ERROR',
      CURRENT_TIMESTAMP();
    
    RETURN OBJECT_CONSTRUCT(
      'result', 'error',
      'error', SQLERRM
    );
END;
$$;

-- ============================================================================
-- SAFE_RECOVER: Recover soft-deleted objects
-- ============================================================================
CREATE OR REPLACE PROCEDURE MCP.SAFE_RECOVER(
  backup_name STRING,
  new_name STRING DEFAULT NULL
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
COMMENT = 'Recover soft-deleted objects'
AS
$$
DECLARE
  object_type STRING;
  original_name STRING;
  recovery_name STRING;
  recovery_ddl STRING;
BEGIN
  -- Find the soft-deleted object
  LET recovery_info VARIANT := (
    SELECT 
      attributes:object_type::string as object_type,
      attributes:object_identity::string as original_name,
      attributes:archived_ddl::string as ddl_text
    FROM CLAUDE_BI.ACTIVITY.EVENTS
    WHERE action = 'ddl.object.soft_delete'
      AND attributes:backup_name::string = :backup_name
    ORDER BY occurred_at DESC
    LIMIT 1
  );
  
  IF :recovery_info IS NULL THEN
    RETURN OBJECT_CONSTRUCT(
      'result', 'error',
      'error', 'Backup not found',
      'backup_name', :backup_name
    );
  END IF;
  
  SET object_type = :recovery_info:object_type;
  SET original_name = :recovery_info:original_name;
  SET recovery_ddl = :recovery_info:ddl_text;
  SET recovery_name = COALESCE(:new_name, :original_name);
  
  -- Modify DDL to use recovery name
  SET recovery_ddl = REGEXP_REPLACE(
    :recovery_ddl,
    'CREATE\\s+(?:OR\\s+REPLACE\\s+)?(' || :object_type || ')\\s+[^\\s]+',
    'CREATE OR REPLACE \\1 ' || :recovery_name,
    1, 1, 'i'
  );
  
  -- Deploy recovered object
  CALL MCP.SAFE_DDL(:recovery_ddl, 'RECOVERY from ' || :backup_name);
  
  -- Drop the backup
  EXECUTE IMMEDIATE 'DROP ' || :object_type || ' ' || :backup_name;
  
  -- Log recovery
  INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS (PAYLOAD, _SOURCE_LANE, _RECV_AT)
  SELECT 
    OBJECT_CONSTRUCT(
      'event_id', UUID_STRING(),
      'action', 'ddl.object.recovered',
      'occurred_at', CURRENT_TIMESTAMP(),
      'actor_id', CURRENT_USER(),
      'source', 'SAFE_RECOVER',
      'object', OBJECT_CONSTRUCT(
        'type', :object_type,
        'identity', :recovery_name
      ),
      'attributes', OBJECT_CONSTRUCT(
        'backup_name', :backup_name,
        'original_name', :original_name,
        'recovery_name', :recovery_name
      )
    ),
    'DDL_RECOVERY',
    CURRENT_TIMESTAMP();
  
  RETURN OBJECT_CONSTRUCT(
    'result', 'success',
    'recovered_object', :recovery_name,
    'from_backup', :backup_name
  );
END;
$$;

-- ============================================================================
-- View: Dropped Objects Available for Recovery
-- ============================================================================
CREATE OR REPLACE VIEW MCP.VW_DDL_RECOVERABLE AS
SELECT 
  attributes:backup_name::string as backup_name,
  attributes:object_identity::string as original_name,
  attributes:object_type::string as object_type,
  occurred_at as dropped_at,
  actor_id as dropped_by,
  attributes:reason::string as drop_reason,
  DATEDIFF('day', occurred_at, CURRENT_TIMESTAMP()) as days_since_drop,
  CASE 
    WHEN DATEDIFF('day', occurred_at, CURRENT_TIMESTAMP()) < 7 THEN 'Recent'
    WHEN DATEDIFF('day', occurred_at, CURRENT_TIMESTAMP()) < 30 THEN 'Aging'
    ELSE 'Old - consider permanent deletion'
  END as recovery_status
FROM CLAUDE_BI.ACTIVITY.EVENTS
WHERE action = 'ddl.object.soft_delete'
  AND attributes:recoverable::boolean = true
ORDER BY occurred_at DESC;

-- ============================================================================
-- Grant Permissions
-- ============================================================================
GRANT USAGE ON PROCEDURE MCP.SAFE_ALTER(STRING, STRING) TO ROLE MCP_AGENT_ROLE;
GRANT USAGE ON PROCEDURE MCP.SAFE_ALTER(STRING, STRING) TO ROLE MCP_USER_ROLE;
GRANT USAGE ON PROCEDURE MCP.SAFE_DROP(STRING, STRING, STRING, BOOLEAN) TO ROLE MCP_AGENT_ROLE;
GRANT USAGE ON PROCEDURE MCP.SAFE_DROP(STRING, STRING, STRING, BOOLEAN) TO ROLE MCP_USER_ROLE;
GRANT USAGE ON PROCEDURE MCP.SAFE_RECOVER(STRING, STRING) TO ROLE MCP_USER_ROLE;
GRANT SELECT ON VIEW MCP.VW_DDL_RECOVERABLE TO ROLE MCP_USER_ROLE;
GRANT SELECT ON VIEW MCP.VW_DDL_RECOVERABLE TO ROLE MCP_AGENT_ROLE;

-- ============================================================================
-- Usage Examples
-- ============================================================================
/*
-- ALTER a procedure
CALL MCP.SAFE_ALTER(
  'ALTER PROCEDURE MCP.MY_PROC(STRING) SET COMMENT = ''Updated procedure''',
  'Adding documentation'
);

-- Soft delete (recoverable)
CALL MCP.SAFE_DROP('VIEW', 'MCP.OLD_VIEW', 'Obsolete view', FALSE);

-- Hard delete (permanent)
CALL MCP.SAFE_DROP('PROCEDURE', 'MCP.TEMP_PROC', 'Temporary procedure', TRUE);

-- View recoverable objects
SELECT * FROM MCP.VW_DDL_RECOVERABLE;

-- Recover a dropped object
CALL MCP.SAFE_RECOVER('MCP.OLD_VIEW_DROPPED_20240115_143022');

-- Recover with new name
CALL MCP.SAFE_RECOVER('MCP.OLD_VIEW_DROPPED_20240115_143022', 'MCP.RESTORED_VIEW');
*/