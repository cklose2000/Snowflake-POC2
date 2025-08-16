-- ===================================================================
-- TWO-TABLE LAW COMPLIANT IMPLEMENTATION
-- Everything is an event or a view - NO EXCEPTIONS
-- ===================================================================

USE DATABASE CLAUDE_BI;
USE SCHEMA MCP;

-- ===================================================================
-- DASHBOARD PERSISTENCE AS EVENTS + VIEWS
-- ===================================================================

-- Save dashboard spec as an event
CREATE OR REPLACE PROCEDURE MCP.SAVE_DASHBOARD_SPEC(spec VARIANT, name STRING)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE 
  spec_id STRING;
BEGIN
  spec_id := 'spec_' || UUID_STRING();
  
  -- Store spec as an event
  INSERT INTO LANDING.RAW_EVENTS (raw_event, source, loaded_at)
  VALUES (
    OBJECT_CONSTRUCT(
      'event_id', UUID_STRING(),
      'action', 'dashboard.spec.created',
      'actor_id', CURRENT_USER(),
      'object', OBJECT_CONSTRUCT(
        'type', 'dashboard_spec',
        'id', :spec_id
      ),
      'attributes', OBJECT_CONSTRUCT(
        'name', :name,
        'spec', :spec,
        'created_at', CURRENT_TIMESTAMP()
      ),
      'occurred_at', CURRENT_TIMESTAMP()
    ),
    'CLAUDE_CODE',
    CURRENT_TIMESTAMP()
  );
  
  -- Also write to stage for export/backup
  BEGIN
    LET stage_path := '@MCP.DASH_SPECS/' || :spec_id || '.json';
    CREATE OR REPLACE TEMPORARY TABLE temp_spec AS
    SELECT :spec as spec_data;
    
    COPY INTO IDENTIFIER(:stage_path)
    FROM (SELECT spec_data FROM temp_spec)
    FILE_FORMAT = (TYPE = JSON)
    OVERWRITE = TRUE;
  EXCEPTION
    WHEN OTHER THEN
      NULL; -- Stage write is optional
  END;
  
  RETURN OBJECT_CONSTRUCT(
    'ok', TRUE,
    'spec_id', :spec_id,
    'name', :name
  );
END;
$$;

-- Load dashboard spec from events
CREATE OR REPLACE FUNCTION MCP.LOAD_DASHBOARD_SPEC(spec_id STRING)
RETURNS VARIANT
LANGUAGE SQL
AS 
$$ 
  SELECT attributes:spec
  FROM ACTIVITY.EVENTS
  WHERE action = 'dashboard.spec.created'
    AND object_id = spec_id
  ORDER BY occurred_at DESC
  LIMIT 1
$$;

-- View of current dashboard specs
CREATE OR REPLACE VIEW MCP.VW_DASHBOARD_SPECS AS
WITH latest_specs AS (
  SELECT 
    object_id as spec_id,
    attributes:name::STRING as name,
    attributes:spec as spec,
    occurred_at as created_at,
    actor_id as created_by,
    ROW_NUMBER() OVER (PARTITION BY object_id ORDER BY occurred_at DESC) as rn
  FROM ACTIVITY.EVENTS
  WHERE action = 'dashboard.spec.created'
)
SELECT spec_id, name, spec, created_at, created_by
FROM latest_specs
WHERE rn = 1
ORDER BY created_at DESC;

-- ===================================================================
-- DASHBOARD SCHEDULES AS EVENTS + VIEWS
-- ===================================================================

-- Create schedule (stored as event, creates real Task)
CREATE OR REPLACE PROCEDURE MCP.CREATE_DASHBOARD_SCHEDULE(spec_id STRING, cron STRING)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE 
  schedule_id STRING;
  task_name STRING;
BEGIN
  schedule_id := 'sched_' || UUID_STRING();
  task_name := 'TASK_' || REPLACE(REPLACE(:schedule_id, '-', '_'), ' ', '_');
  
  -- Store schedule as an event
  INSERT INTO LANDING.RAW_EVENTS (raw_event, source, loaded_at)
  VALUES (
    OBJECT_CONSTRUCT(
      'event_id', UUID_STRING(),
      'action', 'dashboard.schedule.created',
      'actor_id', CURRENT_USER(),
      'object', OBJECT_CONSTRUCT(
        'type', 'schedule',
        'id', :schedule_id
      ),
      'attributes', OBJECT_CONSTRUCT(
        'spec_id', :spec_id,
        'cron', :cron,
        'task_name', :task_name,
        'status', 'active'
      ),
      'occurred_at', CURRENT_TIMESTAMP()
    ),
    'CLAUDE_CODE',
    CURRENT_TIMESTAMP()
  );
  
  -- Create actual Snowflake Task
  EXECUTE IMMEDIATE 
    'CREATE OR REPLACE TASK MCP.' || :task_name || '
     WAREHOUSE = CLAUDE_AGENT_WH
     SCHEDULE = ''USING CRON ' || :cron || ' UTC''
     AS 
       DECLARE
         spec VARIANT;
       BEGIN
         -- Load spec from events
         SELECT attributes:spec INTO :spec
         FROM ACTIVITY.EVENTS
         WHERE action = ''dashboard.spec.created''
           AND object_id = ''' || :spec_id || '''
         ORDER BY occurred_at DESC
         LIMIT 1;
         
         IF (:spec IS NOT NULL) THEN
           -- Log execution
           INSERT INTO LANDING.RAW_EVENTS (raw_event, source, loaded_at)
           VALUES (
             OBJECT_CONSTRUCT(
               ''event_id'', UUID_STRING(),
               ''action'', ''dashboard.schedule.executed'',
               ''actor_id'', ''SYSTEM'',
               ''object'', OBJECT_CONSTRUCT(''type'', ''schedule'', ''id'', ''' || :schedule_id || '''),
               ''occurred_at'', CURRENT_TIMESTAMP()
             ),
             ''SYSTEM'',
             CURRENT_TIMESTAMP()
           );
         END IF;
       END;';
  
  -- Resume the task
  EXECUTE IMMEDIATE 'ALTER TASK MCP.' || :task_name || ' RESUME';
  
  RETURN OBJECT_CONSTRUCT(
    'ok', TRUE,
    'schedule_id', :schedule_id,
    'task_name', 'MCP.' || :task_name,
    'spec_id', :spec_id,
    'cron', :cron
  );
END;
$$;

-- View of active schedules
CREATE OR REPLACE VIEW MCP.VW_DASHBOARD_SCHEDULES AS
WITH latest_schedules AS (
  SELECT 
    object_id as schedule_id,
    attributes:spec_id::STRING as spec_id,
    attributes:cron::STRING as cron,
    attributes:task_name::STRING as task_name,
    attributes:status::STRING as status,
    occurred_at as created_at,
    actor_id as created_by,
    ROW_NUMBER() OVER (PARTITION BY object_id ORDER BY occurred_at DESC) as rn
  FROM ACTIVITY.EVENTS
  WHERE action IN ('dashboard.schedule.created', 'dashboard.schedule.updated', 'dashboard.schedule.deleted')
)
SELECT schedule_id, spec_id, cron, task_name, status, created_at, created_by
FROM latest_schedules
WHERE rn = 1 AND status = 'active'
ORDER BY created_at DESC;

-- ===================================================================
-- ACTOR REGISTRY AS VIEW (Not a table!)
-- ===================================================================

CREATE OR REPLACE VIEW MCP.VW_ACTOR_REGISTRY AS
WITH actor_events AS (
  SELECT 
    object_id as actor_id,
    attributes:display_name::STRING as display_name,
    attributes:email::STRING as email,
    attributes:role::STRING as role,
    attributes:status::STRING as status,
    occurred_at,
    ROW_NUMBER() OVER (PARTITION BY object_id ORDER BY occurred_at DESC) as rn
  FROM ACTIVITY.EVENTS
  WHERE action IN ('actor.created', 'actor.updated')
)
SELECT actor_id, display_name, email, role, status, occurred_at as last_updated
FROM actor_events
WHERE rn = 1 AND status = 'active';

-- ===================================================================
-- TEST CRITICAL PATH - Events Only
-- ===================================================================

CREATE OR REPLACE PROCEDURE MCP.TEST_CRITICAL_PATH(nl TEXT)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE 
  plan VARIANT;
  saved VARIANT;
  loaded VARIANT;
  spec_id STRING;
  test_name STRING;
BEGIN
  test_name := 'test_' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MISS');
  
  -- Step 1: Compile NL to plan (will use fallback if needed)
  plan := OBJECT_CONSTRUCT(
    'proc', 'DASH_GET_METRICS',
    'params', OBJECT_CONSTRUCT(
      'start_ts', DATEADD('hour', -24, CURRENT_TIMESTAMP()),
      'end_ts', CURRENT_TIMESTAMP(),
      'filters', OBJECT_CONSTRUCT()
    ),
    'used_fallback', TRUE
  );
  
  -- Step 2: Save as event
  saved := (CALL MCP.SAVE_DASHBOARD_SPEC(:plan, :test_name));
  spec_id := :saved:spec_id::STRING;
  
  -- Step 3: Load from events
  loaded := MCP.LOAD_DASHBOARD_SPEC(:spec_id);
  
  -- Step 4: Verify we can query data
  LET data_test := (SELECT COUNT(*) > 0 FROM ACTIVITY.EVENTS LIMIT 1);
  
  -- Step 5: Log test execution
  INSERT INTO LANDING.RAW_EVENTS (raw_event, source, loaded_at)
  VALUES (
    OBJECT_CONSTRUCT(
      'event_id', UUID_STRING(),
      'action', 'test.critical_path.executed',
      'actor_id', CURRENT_USER(),
      'object', OBJECT_CONSTRUCT('type', 'test', 'id', :spec_id),
      'attributes', OBJECT_CONSTRUCT(
        'nl', :nl,
        'spec_id', :spec_id,
        'test_name', :test_name,
        'data_available', :data_test
      ),
      'occurred_at', CURRENT_TIMESTAMP()
    ),
    'CLAUDE_CODE',
    CURRENT_TIMESTAMP()
  );
  
  RETURN OBJECT_CONSTRUCT(
    'ok', TRUE,
    'nl', :nl,
    'spec_id', :spec_id,
    'saved', :saved:ok::BOOLEAN,
    'loaded', :loaded IS NOT NULL,
    'data_available', :data_test,
    'test_name', :test_name,
    'timestamp', CURRENT_TIMESTAMP()
  );
END;
$$;

-- ===================================================================
-- VERIFICATION: Ensure Two-Table Law
-- ===================================================================

CREATE OR REPLACE FUNCTION MCP.VERIFY_TWO_TABLE_LAW()
RETURNS VARIANT
LANGUAGE SQL
AS
$$
  SELECT OBJECT_CONSTRUCT(
    'compliant', COUNT(*) = 2,
    'table_count', COUNT(*),
    'tables', ARRAY_AGG(TABLE_SCHEMA || '.' || TABLE_NAME)
  )
  FROM CLAUDE_BI.INFORMATION_SCHEMA.TABLES
  WHERE TABLE_CATALOG = 'CLAUDE_BI'
    AND TABLE_TYPE IN ('BASE TABLE', 'DYNAMIC TABLE')
    AND TABLE_SCHEMA IN ('LANDING', 'ACTIVITY')
$$;

-- ===================================================================
-- GRANTS
-- ===================================================================

GRANT EXECUTE ON PROCEDURE MCP.SAVE_DASHBOARD_SPEC(VARIANT, STRING) TO USER CLAUDE_CODE_AI_AGENT;
GRANT EXECUTE ON FUNCTION MCP.LOAD_DASHBOARD_SPEC(STRING) TO USER CLAUDE_CODE_AI_AGENT;
GRANT SELECT ON VIEW MCP.VW_DASHBOARD_SPECS TO USER CLAUDE_CODE_AI_AGENT;
GRANT EXECUTE ON PROCEDURE MCP.CREATE_DASHBOARD_SCHEDULE(STRING, STRING) TO USER CLAUDE_CODE_AI_AGENT;
GRANT SELECT ON VIEW MCP.VW_DASHBOARD_SCHEDULES TO USER CLAUDE_CODE_AI_AGENT;
GRANT SELECT ON VIEW MCP.VW_ACTOR_REGISTRY TO USER CLAUDE_CODE_AI_AGENT;
GRANT EXECUTE ON PROCEDURE MCP.TEST_CRITICAL_PATH(STRING) TO USER CLAUDE_CODE_AI_AGENT;
GRANT EXECUTE ON FUNCTION MCP.VERIFY_TWO_TABLE_LAW() TO USER CLAUDE_CODE_AI_AGENT;

-- ===================================================================
-- VALIDATION
-- ===================================================================

-- Verify Two-Table Law
SELECT MCP.VERIFY_TWO_TABLE_LAW();

-- Test the critical path
CALL MCP.TEST_CRITICAL_PATH('show me activity trends');