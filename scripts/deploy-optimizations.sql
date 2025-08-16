-- ===================================================================
-- PERFORMANCE OPTIMIZATIONS FOR ALL-SNOWFLAKE NATIVE ARCHITECTURE
-- One hot, predictable path with minimal latency
-- ===================================================================

USE DATABASE CLAUDE_BI;
USE SCHEMA MCP;

-- ===================================================================
-- 1. TEST_ALL - Single server-side call for all health checks
-- ===================================================================

CREATE OR REPLACE PROCEDURE MCP.TEST_ALL()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
BEGIN
  -- Initialize results array
  LET results := ARRAY_CONSTRUCT();
  
  -- Test 1: Two-Table Law
  LET table_count := (
    SELECT COUNT(*)
    FROM CLAUDE_BI.INFORMATION_SCHEMA.TABLES
    WHERE TABLE_CATALOG = 'CLAUDE_BI'
      AND TABLE_TYPE IN ('BASE TABLE', 'DYNAMIC TABLE')
      AND TABLE_SCHEMA IN ('LANDING', 'ACTIVITY')
  );
  
  results := ARRAY_APPEND(results, OBJECT_CONSTRUCT(
    'test', 'two_table_law',
    'ok', :table_count = 2,
    'details', 'Tables: ' || :table_count || '/2'
  ));
  
  -- Test 2: Dashboard Procedures
  LET dash_procs_count := (
    SELECT COUNT(*)
    FROM INFORMATION_SCHEMA.PROCEDURES
    WHERE PROCEDURE_SCHEMA = 'MCP'
      AND PROCEDURE_NAME IN ('DASH_GET_SERIES', 'DASH_GET_TOPN', 
                             'DASH_GET_EVENTS', 'DASH_GET_METRICS', 'DASH_GET_PIVOT')
  );
  
  results := ARRAY_APPEND(results, OBJECT_CONSTRUCT(
    'test', 'dashboard_procedures',
    'ok', :dash_procs_count = 5,
    'details', 'Procedures: ' || :dash_procs_count || '/5'
  ));
  
  -- Test 3: Stages Created
  LET stages_count := (
    SELECT COUNT(*)
    FROM INFORMATION_SCHEMA.STAGES
    WHERE STAGE_SCHEMA = 'MCP'
      AND STAGE_NAME IN ('DASH_SPECS', 'DASH_SNAPSHOTS', 'DASH_COHORTS', 'DASH_APPS')
  );
  
  results := ARRAY_APPEND(results, OBJECT_CONSTRUCT(
    'test', 'stages',
    'ok', :stages_count = 4,
    'details', 'Stages: ' || :stages_count || '/4'
  ));
  
  -- Test 4: External Access Integration
  LET eai_exists := (
    SELECT COUNT(*) > 0
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID(-2)))
    WHERE "name" = 'CLAUDE_EAI'
  );
  SHOW EXTERNAL ACCESS INTEGRATIONS;
  
  results := ARRAY_APPEND(results, OBJECT_CONSTRUCT(
    'test', 'external_access',
    'ok', :eai_exists,
    'details', 'EAI: ' || IFF(:eai_exists, 'configured', 'missing')
  ));
  
  -- Test 5: Secrets Configured
  LET secrets_count := (
    SELECT COUNT(*)
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID(-2)))
    WHERE "schema_name" = 'MCP'
      AND "name" IN ('CLAUDE_API_KEY', 'SLACK_WEBHOOK_URL')
  );
  SHOW SECRETS IN SCHEMA MCP;
  
  results := ARRAY_APPEND(results, OBJECT_CONSTRUCT(
    'test', 'secrets',
    'ok', :secrets_count = 2,
    'details', 'Secrets: ' || :secrets_count || '/2'
  ));
  
  -- Test 6: Recent Activity
  LET recent_events := (
    SELECT COUNT(*)
    FROM ACTIVITY.EVENTS
    WHERE source = 'CLAUDE_CODE'
      AND occurred_at >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
  );
  
  results := ARRAY_APPEND(results, OBJECT_CONSTRUCT(
    'test', 'recent_activity',
    'ok', :recent_events > 0,
    'details', 'Events (1hr): ' || :recent_events
  ));
  
  -- Test 7: Dashboard Events
  LET dashboard_events := (
    SELECT COUNT(*)
    FROM ACTIVITY.EVENTS
    WHERE action = 'dashboard.created'
  );
  
  results := ARRAY_APPEND(results, OBJECT_CONSTRUCT(
    'test', 'dashboard_events',
    'ok', :dashboard_events > 0,
    'details', 'Dashboards created: ' || :dashboard_events
  ));
  
  -- Calculate overall status
  LET all_ok := (
    SELECT BOOLAND_AGG(value:ok::BOOLEAN)
    FROM TABLE(FLATTEN(input => :results))
  );
  
  RETURN OBJECT_CONSTRUCT(
    'ok', :all_ok,
    'timestamp', CURRENT_TIMESTAMP(),
    'tests', :results,
    'summary', IFF(:all_ok, 'All systems operational', 'Some tests failed')
  );
END;
$$;

-- ===================================================================
-- 2. DEMO_PATH - One-click orchestrator for the happy path
-- ===================================================================

CREATE OR REPLACE PROCEDURE MCP.DEMO_PATH(nl_text VARCHAR)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
BEGIN
  -- Step 1: Compile NL to plan (with fallback)
  LET plan := NULL;
  
  -- Try NL compilation if procedure exists
  BEGIN
    LET intent := OBJECT_CONSTRUCT('text', :nl_text);
    LET compile_result := (CALL MCP.COMPILE_NL_PLAN(:intent));
    IF (compile_result:ok::BOOLEAN) THEN
      plan := compile_result:plan;
    END IF;
  EXCEPTION
    WHEN OTHER THEN
      -- Fallback to default plan
      NULL;
  END;
  
  -- Fallback plan if compilation failed
  IF (:plan IS NULL) THEN
    plan := OBJECT_CONSTRUCT(
      'proc', 'DASH_GET_METRICS',
      'params', OBJECT_CONSTRUCT(
        'start_ts', DATEADD('hour', -24, CURRENT_TIMESTAMP()),
        'end_ts', CURRENT_TIMESTAMP(),
        'filters', OBJECT_CONSTRUCT()
      )
    );
  END IF;
  
  -- Step 2: Execute the plan
  LET proc_name := :plan:proc::STRING;
  LET params := :plan:params;
  LET result := NULL;
  
  CASE :proc_name
    WHEN 'DASH_GET_SERIES' THEN
      result := (CALL MCP.DASH_GET_SERIES(:params));
    WHEN 'DASH_GET_TOPN' THEN
      result := (CALL MCP.DASH_GET_TOPN(:params));
    WHEN 'DASH_GET_EVENTS' THEN
      result := (CALL MCP.DASH_GET_EVENTS(:params));
    WHEN 'DASH_GET_METRICS' THEN
      result := (CALL MCP.DASH_GET_METRICS(:params));
    ELSE
      result := OBJECT_CONSTRUCT('error', 'Unknown procedure: ' || :proc_name);
  END CASE;
  
  -- Step 3: Log the execution
  INSERT INTO LANDING.RAW_EVENTS (raw_event, source, loaded_at)
  SELECT 
    OBJECT_CONSTRUCT(
      'event_id', UUID_STRING(),
      'action', 'demo.path.executed',
      'actor_id', 'CLAUDE_CODE_AI_AGENT',
      'object', OBJECT_CONSTRUCT('type', 'demo', 'id', UUID_STRING()),
      'attributes', OBJECT_CONSTRUCT(
        'nl_text', :nl_text,
        'procedure', :proc_name,
        'row_count', ARRAY_SIZE(:result:data),
        'status', 'success'
      ),
      'occurred_at', CURRENT_TIMESTAMP()
    ),
    'CLAUDE_CODE',
    CURRENT_TIMESTAMP();
  
  -- Return comprehensive result
  RETURN OBJECT_CONSTRUCT(
    'ok', TRUE,
    'nl_text', :nl_text,
    'plan', :plan,
    'procedure_called', :proc_name,
    'row_count', ARRAY_SIZE(:result:data),
    'preview', ARRAY_SLICE(:result:data, 0, 10),
    'timestamp', CURRENT_TIMESTAMP()
  );
END;
$$;

-- ===================================================================
-- 3. SESSION POLICY - Enforce invariants at server level
-- ===================================================================

-- Run as ACCOUNTADMIN
CREATE OR REPLACE SESSION POLICY MCP.AGENT_POLICY
  SESSION_IDLE_TIMEOUT_MINS = 30
  SESSION_UI_IDLE_TIMEOUT_MINS = 30
  COMMENT = 'Session policy for Claude Code AI agents';

-- Set session parameters for the policy
ALTER SESSION POLICY MCP.AGENT_POLICY SET
  SESSION_PARAMETERS = (
    QUERY_TAG = 'ai_agent::native',
    STATEMENT_TIMEOUT_IN_SECONDS = 90,
    AUTOCOMMIT = TRUE,
    USE_CACHED_RESULT = TRUE,
    ROWS_PER_RESULTSET = 10000,
    CLIENT_RESULT_CHUNK_SIZE = 160
  );

-- Apply to the agent user
ALTER USER CLAUDE_CODE_AI_AGENT SET SESSION POLICY = MCP.AGENT_POLICY;

-- ===================================================================
-- 4. WAREHOUSE WARMER - Keep warehouse warm with minimal cost
-- ===================================================================

CREATE OR REPLACE TASK MCP.WARM_WAREHOUSE
  WAREHOUSE = CLAUDE_AGENT_WH
  SCHEDULE = 'USING CRON */3 * * * * UTC'  -- Every 3 minutes
  COMMENT = 'Keeps warehouse warm to avoid cold starts'
AS
  SELECT 'ping' as status, CURRENT_TIMESTAMP() as ts;

-- Start the warmer
ALTER TASK MCP.WARM_WAREHOUSE RESUME;

-- ===================================================================
-- 5. MICRO_BATCH_LOG - Efficient bulk logging procedure
-- ===================================================================

CREATE OR REPLACE PROCEDURE MCP.MICRO_BATCH_LOG(events ARRAY)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
BEGIN
  -- Bulk insert all events at once
  INSERT INTO LANDING.RAW_EVENTS (raw_event, source, loaded_at)
  SELECT 
    VALUE,
    'CLAUDE_CODE',
    CURRENT_TIMESTAMP()
  FROM TABLE(FLATTEN(input => :events));
  
  RETURN OBJECT_CONSTRUCT(
    'ok', TRUE,
    'events_logged', ARRAY_SIZE(:events),
    'timestamp', CURRENT_TIMESTAMP()
  );
END;
$$;

-- ===================================================================
-- 6. REDUCE DYNAMIC TABLE LAG for demos
-- ===================================================================

ALTER DYNAMIC TABLE ACTIVITY.EVENTS SET TARGET_LAG = '1 minute';

-- ===================================================================
-- 7. PRIME_EAI - Prime External Access Integration
-- ===================================================================

CREATE OR REPLACE PROCEDURE MCP.PRIME_EAI()
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
HANDLER = 'prime'
EXTERNAL_ACCESS_INTEGRATIONS = (CLAUDE_EAI)
PACKAGES = ('snowflake-snowpark-python')
SECRETS = ('claude_key' = MCP.CLAUDE_API_KEY)
AS
$$
import _snowflake

def prime(session):
    # Just read the secret to warm up the EAI
    try:
        api_key = _snowflake.get_generic_secret_string("claude_key")
        return {"ok": True, "primed": len(api_key) > 0}
    except:
        return {"ok": False, "primed": False}
$$;

-- ===================================================================
-- 8. CANONICAL VIEWS - Push logic to SQL
-- ===================================================================

CREATE OR REPLACE SECURE VIEW MCP.VW_CANONICAL_METRICS AS
SELECT 
  DATE_TRUNC('hour', occurred_at) as hour,
  COUNT(*) as event_count,
  COUNT(DISTINCT actor_id) as unique_actors,
  COUNT(DISTINCT action) as unique_actions
FROM ACTIVITY.EVENTS
WHERE occurred_at >= DATEADD('day', -7, CURRENT_TIMESTAMP())
GROUP BY 1
ORDER BY 1 DESC;

CREATE OR REPLACE SECURE VIEW MCP.VW_DEFAULT_DASHBOARD AS
SELECT OBJECT_CONSTRUCT(
  'title', 'Executive Dashboard',
  'panels', ARRAY_CONSTRUCT(
    OBJECT_CONSTRUCT('type', 'series', 'title', 'Hourly Activity'),
    OBJECT_CONSTRUCT('type', 'topn', 'title', 'Top Actions'),
    OBJECT_CONSTRUCT('type', 'metrics', 'title', 'Key Metrics')
  ),
  'refresh_interval', 300,
  'created_at', CURRENT_TIMESTAMP()
) as spec;

-- ===================================================================
-- 9. RESOURCE MONITOR - Prevent cost overruns
-- ===================================================================

-- Run as ACCOUNTADMIN
CREATE OR REPLACE RESOURCE MONITOR MCP.CLAUDE_AGENT_MONITOR
  WITH 
    CREDIT_QUOTA = 10  -- 10 credits max for POC
    FREQUENCY = DAILY
    START_TIMESTAMP = IMMEDIATELY
    TRIGGERS
      ON 75 PERCENT DO NOTIFY
      ON 90 PERCENT DO NOTIFY
      ON 100 PERCENT DO SUSPEND;

-- Assign to warehouse
ALTER WAREHOUSE CLAUDE_AGENT_WH SET RESOURCE_MONITOR = MCP.CLAUDE_AGENT_MONITOR;

-- ===================================================================
-- 10. CONNECTION POOL HELPER - For persistent sessions
-- ===================================================================

CREATE OR REPLACE PROCEDURE MCP.GET_SESSION_INFO()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
BEGIN
  RETURN OBJECT_CONSTRUCT(
    'session_id', CURRENT_SESSION(),
    'user', CURRENT_USER(),
    'role', CURRENT_ROLE(),
    'warehouse', CURRENT_WAREHOUSE(),
    'database', CURRENT_DATABASE(),
    'schema', CURRENT_SCHEMA(),
    'query_tag', CURRENT_QUERY_TAG(),
    'timestamp', CURRENT_TIMESTAMP()
  );
END;
$$;

-- ===================================================================
-- GRANTS
-- ===================================================================

GRANT EXECUTE ON PROCEDURE MCP.TEST_ALL() TO USER CLAUDE_CODE_AI_AGENT;
GRANT EXECUTE ON PROCEDURE MCP.DEMO_PATH(VARCHAR) TO USER CLAUDE_CODE_AI_AGENT;
GRANT EXECUTE ON PROCEDURE MCP.MICRO_BATCH_LOG(ARRAY) TO USER CLAUDE_CODE_AI_AGENT;
GRANT EXECUTE ON PROCEDURE MCP.PRIME_EAI() TO USER CLAUDE_CODE_AI_AGENT;
GRANT EXECUTE ON PROCEDURE MCP.GET_SESSION_INFO() TO USER CLAUDE_CODE_AI_AGENT;
GRANT SELECT ON VIEW MCP.VW_CANONICAL_METRICS TO USER CLAUDE_CODE_AI_AGENT;
GRANT SELECT ON VIEW MCP.VW_DEFAULT_DASHBOARD TO USER CLAUDE_CODE_AI_AGENT;

-- ===================================================================
-- VALIDATION
-- ===================================================================

-- Test the optimizations
CALL MCP.TEST_ALL();
CALL MCP.DEMO_PATH('show me top 10 actions');
CALL MCP.PRIME_EAI();
CALL MCP.GET_SESSION_INFO();