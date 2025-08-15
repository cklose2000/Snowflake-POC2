-- ============================================================================
-- 03_workload_procedures.sql  
-- Core workload procedures with proper CALLER/OWNER security model
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE CLAUDE_BI;
USE SCHEMA MCP;

-- ============================================================================
-- SAFE_INSERT_EVENT - OWNER mode with role guard
-- Encapsulates INSERT privilege so actors don't need direct table grants
-- ============================================================================
CREATE OR REPLACE PROCEDURE SAFE_INSERT_EVENT(
  event_payload VARIANT,
  source_lane STRING DEFAULT 'APPLICATION'
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER  -- Runs with elevated privileges
COMMENT = 'Safe event insertion with validation and role checking'
AS $$
BEGIN
  -- ============================================================================
  -- ROLE GUARD - Require write permission
  -- ============================================================================
  IF (NOT IS_ROLE_IN_SESSION('R_APP_WRITE')) THEN
    RETURN OBJECT_CONSTRUCT(
      'success', FALSE,
      'error', 'Forbidden: requires R_APP_WRITE role',
      'current_role', CURRENT_ROLE(),
      'required_role', 'R_APP_WRITE'
    );
  END IF;
  
  -- ============================================================================
  -- VALIDATION
  -- ============================================================================
  
  -- Check payload is not null
  IF (event_payload IS NULL) THEN
    RETURN OBJECT_CONSTRUCT(
      'success', FALSE,
      'error', 'Invalid payload: cannot be NULL'
    );
  END IF;
  
  -- Check payload is an object
  IF (NOT IS_OBJECT(event_payload)) THEN
    RETURN OBJECT_CONSTRUCT(
      'success', FALSE,
      'error', 'Invalid payload: must be a JSON object'
    );
  END IF;
  
  -- Ensure required fields exist (add event_id if missing)
  LET final_payload := CASE
    WHEN event_payload:event_id IS NULL 
    THEN OBJECT_INSERT(event_payload, 'event_id', UUID_STRING())
    ELSE event_payload
  END;
  
  -- Add occurred_at if missing
  final_payload := CASE
    WHEN final_payload:occurred_at IS NULL
    THEN OBJECT_INSERT(final_payload, 'occurred_at', CURRENT_TIMESTAMP()::STRING)
    ELSE final_payload
  END;
  
  -- ============================================================================
  -- AUDIT ENRICHMENT
  -- ============================================================================
  
  -- Add audit metadata
  final_payload := OBJECT_INSERT(
    final_payload,
    '_audit',
    OBJECT_CONSTRUCT(
      'inserted_by', CURRENT_USER(),
      'inserted_role', CURRENT_ROLE(),
      'inserted_at', CURRENT_TIMESTAMP(),
      'source_ip', CURRENT_CLIENT(),
      'source_lane', :source_lane,
      'query_id', LAST_QUERY_ID()
    ),
    TRUE  -- Update if exists
  );
  
  -- ============================================================================
  -- INSERT EVENT
  -- ============================================================================
  
  INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS (
    payload,
    source_lane,
    ingested_at
  )
  SELECT 
    :final_payload,
    :source_lane,
    CURRENT_TIMESTAMP();
  
  -- ============================================================================
  -- RETURN SUCCESS
  -- ============================================================================
  
  RETURN OBJECT_CONSTRUCT(
    'success', TRUE,
    'event_id', final_payload:event_id,
    'inserted_by', CURRENT_USER(),
    'inserted_at', CURRENT_TIMESTAMP()
  );
  
EXCEPTION
  WHEN OTHER THEN
    RETURN OBJECT_CONSTRUCT(
      'success', FALSE,
      'error', 'Insert failed',
      'details', SQLERRM,
      'code', SQLCODE
    );
END;
$$;

-- ============================================================================
-- COMPOSE_QUERY_PLAN - CALLER mode for read operations
-- Uses caller's privileges to respect row-level security
-- ============================================================================
CREATE OR REPLACE PROCEDURE COMPOSE_QUERY_PLAN(
  intent_text STRING,
  top_n NUMBER DEFAULT 100,
  time_window_hours NUMBER DEFAULT 24
)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER  -- Uses caller's permissions
COMMENT = 'Compose a query plan from natural language intent'
AS $$
// Set query tag for observability
var queryTag = {
  agent: 'claude_code',
  operation: 'compose_query_plan',
  user: snowflake.execute({sqlText: 'SELECT CURRENT_USER()'}).next() ? 
        snowflake.execute({sqlText: 'SELECT CURRENT_USER()'}).getColumnValue(1) : 'unknown',
  role: snowflake.execute({sqlText: 'SELECT CURRENT_ROLE()'}).next() ?
        snowflake.execute({sqlText: 'SELECT CURRENT_ROLE()'}).getColumnValue(1) : 'unknown',
  timestamp: new Date().toISOString()
};

snowflake.execute({
  sqlText: "ALTER SESSION SET QUERY_TAG = ?",
  binds: [JSON.stringify(queryTag)]
});

try {
  // Parse the intent (simplified - in production, use NLP)
  var intent = INTENT_TEXT.toLowerCase();
  var actions = [];
  
  // Determine query type based on keywords
  if (intent.includes('user') || intent.includes('signup') || intent.includes('registration')) {
    actions.push({
      type: 'filter',
      field: 'action',
      operator: 'like',
      value: 'user.%'
    });
  }
  
  if (intent.includes('order') || intent.includes('purchase') || intent.includes('sale')) {
    actions.push({
      type: 'filter', 
      field: 'action',
      operator: 'like',
      value: 'order.%'
    });
  }
  
  if (intent.includes('error') || intent.includes('fail')) {
    actions.push({
      type: 'filter',
      field: 'attributes:status',
      operator: '=',
      value: 'error'
    });
  }
  
  // Add time window
  if (TIME_WINDOW_HOURS > 0) {
    actions.push({
      type: 'filter',
      field: 'occurred_at',
      operator: '>=',
      value: 'DATEADD(hour, -' + TIME_WINDOW_HOURS + ', CURRENT_TIMESTAMP())'
    });
  }
  
  // Add limit
  actions.push({
    type: 'limit',
    value: TOP_N
  });
  
  // Build the query plan
  var plan = {
    intent: INTENT_TEXT,
    parsed_actions: actions,
    source_table: 'CLAUDE_BI.ACTIVITY.EVENTS',
    limit: TOP_N,
    time_window_hours: TIME_WINDOW_HOURS,
    composed_at: new Date().toISOString(),
    composed_by: queryTag.user,
    composed_role: queryTag.role
  };
  
  // Log the plan composition
  snowflake.execute({
    sqlText: `
      INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS
      SELECT OBJECT_CONSTRUCT(
        'event_id', UUID_STRING(),
        'action', 'query.plan.composed',
        'actor_id', CURRENT_USER(),
        'occurred_at', CURRENT_TIMESTAMP(),
        'attributes', PARSE_JSON(?)
      ), 'MCP', CURRENT_TIMESTAMP()
    `,
    binds: [JSON.stringify(plan)]
  });
  
  return {
    success: true,
    plan: plan
  };
  
} catch (error) {
  return {
    success: false,
    error: error.message,
    user: queryTag.user,
    role: queryTag.role
  };
}
$$;

-- ============================================================================
-- VALIDATE_QUERY_PLAN - CALLER mode to validate plans
-- ============================================================================
CREATE OR REPLACE PROCEDURE VALIDATE_QUERY_PLAN(
  query_plan VARIANT
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
COMMENT = 'Validate a query plan before execution'
AS $$
DECLARE
  is_valid BOOLEAN DEFAULT TRUE;
  errors ARRAY DEFAULT ARRAY_CONSTRUCT();
  warnings ARRAY DEFAULT ARRAY_CONSTRUCT();
BEGIN
  -- Check plan structure
  IF (query_plan IS NULL) THEN
    is_valid := FALSE;
    errors := ARRAY_APPEND(errors, 'Plan cannot be NULL');
  END IF;
  
  -- Check for required fields
  IF (NOT IS_OBJECT(query_plan)) THEN
    is_valid := FALSE;
    errors := ARRAY_APPEND(errors, 'Plan must be a JSON object');
  ELSE
    -- Check for actions array
    IF (query_plan:parsed_actions IS NULL) THEN
      is_valid := FALSE;
      errors := ARRAY_APPEND(errors, 'Plan must contain parsed_actions');
    END IF;
    
    -- Check limit
    IF (query_plan:limit IS NULL OR query_plan:limit > 10000) THEN
      warnings := ARRAY_APPEND(warnings, 'Limit not set or exceeds 10000, will be capped');
    END IF;
  END IF;
  
  -- Check user permissions (they need R_APP_READ at minimum)
  IF (NOT IS_ROLE_IN_SESSION('R_APP_READ')) THEN
    is_valid := FALSE;
    errors := ARRAY_APPEND(errors, 'User lacks R_APP_READ role');
  END IF;
  
  RETURN OBJECT_CONSTRUCT(
    'success', TRUE,
    'is_valid', is_valid,
    'errors', errors,
    'warnings', warnings,
    'validated_by', CURRENT_USER(),
    'validated_at', CURRENT_TIMESTAMP()
  );
END;
$$;

-- ============================================================================
-- EXECUTE_QUERY_PLAN - CALLER mode for query execution
-- Returns query_id for RESULT_SCAN pattern
-- ============================================================================
CREATE OR REPLACE PROCEDURE EXECUTE_QUERY_PLAN(
  query_plan VARIANT
)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
COMMENT = 'Execute a validated query plan and return query_id'
AS $$
const startTime = Date.now();

try {
  // Extract plan details
  var actions = QUERY_PLAN.parsed_actions || [];
  var limit = Math.min(QUERY_PLAN.limit || 100, 10000);  // Cap at 10k
  var source = QUERY_PLAN.source_table || 'CLAUDE_BI.ACTIVITY.EVENTS';
  
  // Build WHERE clause from actions
  var whereConditions = [];
  var binds = [];
  
  for (var i = 0; i < actions.length; i++) {
    var action = actions[i];
    if (action.type === 'filter') {
      if (action.operator === 'like') {
        whereConditions.push(action.field + " LIKE ?");
        binds.push(action.value.replace('*', '%'));
      } else if (action.operator === '>=') {
        whereConditions.push(action.field + " >= " + action.value);
      } else {
        whereConditions.push(action.field + " " + action.operator + " ?");
        binds.push(action.value);
      }
    }
  }
  
  // Build final query
  var sql = "SELECT * FROM " + source;
  if (whereConditions.length > 0) {
    sql += " WHERE " + whereConditions.join(" AND ");
  }
  sql += " ORDER BY occurred_at DESC";
  sql += " LIMIT " + limit;
  
  // Execute query
  var stmt = snowflake.createStatement({
    sqlText: sql,
    binds: binds
  });
  
  stmt.execute();
  var queryId = stmt.getQueryId();
  
  // Log execution
  snowflake.execute({
    sqlText: `
      INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS
      SELECT OBJECT_CONSTRUCT(
        'event_id', UUID_STRING(),
        'action', 'query.plan.executed',
        'actor_id', CURRENT_USER(),
        'occurred_at', CURRENT_TIMESTAMP(),
        'object', OBJECT_CONSTRUCT(
          'type', 'query',
          'id', ?
        ),
        'attributes', OBJECT_CONSTRUCT(
          'plan', PARSE_JSON(?),
          'execution_time_ms', ?,
          'row_limit', ?
        )
      ), 'MCP', CURRENT_TIMESTAMP()
    `,
    binds: [queryId, JSON.stringify(QUERY_PLAN), Date.now() - startTime, limit]
  });
  
  return {
    success: true,
    query_id: queryId,
    execution_time_ms: Date.now() - startTime,
    limit: limit,
    executed_by: snowflake.execute({sqlText: 'SELECT CURRENT_USER()'}).next() ?
                  snowflake.execute({sqlText: 'SELECT CURRENT_USER()'}).getColumnValue(1) : 'unknown'
  };
  
} catch (error) {
  return {
    success: false,
    error: error.message,
    plan: QUERY_PLAN
  };
}
$$;

-- ============================================================================
-- LIST_SOURCES - CALLER mode to list available data sources
-- ============================================================================
CREATE OR REPLACE PROCEDURE LIST_SOURCES()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
COMMENT = 'List available data sources for the current user'
AS $$
DECLARE
  sources ARRAY DEFAULT ARRAY_CONSTRUCT();
  source_count INTEGER DEFAULT 0;
BEGIN
  -- Check what the user can access
  IF (IS_ROLE_IN_SESSION('R_APP_READ')) THEN
    sources := ARRAY_APPEND(sources, OBJECT_CONSTRUCT(
      'name', 'ACTIVITY.EVENTS',
      'type', 'DYNAMIC_TABLE',
      'description', 'Processed activity events',
      'schema', 'CLAUDE_BI.ACTIVITY',
      'row_count_estimate', (SELECT COUNT(*) FROM CLAUDE_BI.ACTIVITY.EVENTS)
    ));
    source_count := source_count + 1;
  END IF;
  
  IF (IS_ROLE_IN_SESSION('R_APP_WRITE')) THEN
    sources := ARRAY_APPEND(sources, OBJECT_CONSTRUCT(
      'name', 'LANDING.RAW_EVENTS',
      'type', 'TABLE',
      'description', 'Raw event ingestion table',
      'schema', 'CLAUDE_BI.LANDING',
      'access', 'INSERT_ONLY'
    ));
    source_count := source_count + 1;
  END IF;
  
  -- Add views
  FOR record IN (
    SELECT TABLE_NAME, TABLE_COMMENT
    FROM CLAUDE_BI.INFORMATION_SCHEMA.VIEWS
    WHERE TABLE_SCHEMA = 'MCP'
  ) DO
    sources := ARRAY_APPEND(sources, OBJECT_CONSTRUCT(
      'name', record.TABLE_NAME,
      'type', 'VIEW',
      'description', record.TABLE_COMMENT,
      'schema', 'CLAUDE_BI.MCP'
    ));
    source_count := source_count + 1;
  END FOR;
  
  RETURN OBJECT_CONSTRUCT(
    'success', TRUE,
    'sources', sources,
    'count', source_count,
    'user', CURRENT_USER(),
    'role', CURRENT_ROLE()
  );
END;
$$;

-- ============================================================================
-- GET_USER_STATUS - Return current user's permissions and limits
-- ============================================================================
CREATE OR REPLACE PROCEDURE GET_USER_STATUS()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
COMMENT = 'Get current user status and permissions'
AS $$
BEGIN
  RETURN OBJECT_CONSTRUCT(
    'success', TRUE,
    'username', CURRENT_USER(),
    'role', CURRENT_ROLE(),
    'warehouse', CURRENT_WAREHOUSE(),
    'database', CURRENT_DATABASE(),
    'schema', CURRENT_SCHEMA(),
    'permissions', OBJECT_CONSTRUCT(
      'can_read', IS_ROLE_IN_SESSION('R_APP_READ'),
      'can_write', IS_ROLE_IN_SESSION('R_APP_WRITE'),
      'is_admin', IS_ROLE_IN_SESSION('R_APP_ADMIN')
    ),
    'session_info', OBJECT_CONSTRUCT(
      'client_ip', CURRENT_CLIENT(),
      'session_id', CURRENT_SESSION(),
      'query_tag', CURRENT_QUERY_TAG()
    ),
    'limits', OBJECT_CONSTRUCT(
      'max_query_rows', 10000,
      'warehouse_size', (SELECT WAREHOUSE_SIZE FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSES 
                         WHERE WAREHOUSE_NAME = CURRENT_WAREHOUSE() LIMIT 1)
    )
  );
END;
$$;

-- ============================================================================
-- Grant procedures to appropriate roles
-- ============================================================================

-- Write procedures (require R_APP_WRITE but granted to both)
GRANT USAGE ON PROCEDURE SAFE_INSERT_EVENT(VARIANT, STRING) TO ROLE R_APP_WRITE;
GRANT USAGE ON PROCEDURE SAFE_INSERT_EVENT(VARIANT, STRING) TO ROLE R_APP_READ;  -- They still need write role to use it

-- Read procedures
GRANT USAGE ON PROCEDURE COMPOSE_QUERY_PLAN(STRING, NUMBER, NUMBER) TO ROLE R_APP_READ;
GRANT USAGE ON PROCEDURE VALIDATE_QUERY_PLAN(VARIANT) TO ROLE R_APP_READ;
GRANT USAGE ON PROCEDURE EXECUTE_QUERY_PLAN(VARIANT) TO ROLE R_APP_READ;
GRANT USAGE ON PROCEDURE LIST_SOURCES() TO ROLE R_APP_READ;
GRANT USAGE ON PROCEDURE GET_USER_STATUS() TO ROLE R_APP_READ;

-- ============================================================================
-- Create helper views for common queries
-- ============================================================================

-- Recent events view
CREATE OR REPLACE SECURE VIEW V_RECENT_EVENTS
COMMENT = 'Last 24 hours of events'
AS
SELECT *
FROM CLAUDE_BI.ACTIVITY.EVENTS
WHERE occurred_at >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
ORDER BY occurred_at DESC
LIMIT 1000;

-- User activity summary
CREATE OR REPLACE SECURE VIEW V_USER_ACTIVITY_SUMMARY
COMMENT = 'Summary of user activities'
AS
SELECT 
  DATE_TRUNC('hour', occurred_at) AS hour,
  action,
  COUNT(*) AS event_count,
  COUNT(DISTINCT actor_id) AS unique_actors
FROM CLAUDE_BI.ACTIVITY.EVENTS
WHERE occurred_at >= DATEADD('day', -7, CURRENT_TIMESTAMP())
GROUP BY 1, 2
ORDER BY 1 DESC, 3 DESC;

-- Grant view access
GRANT SELECT ON VIEW V_RECENT_EVENTS TO ROLE R_APP_READ;
GRANT SELECT ON VIEW V_USER_ACTIVITY_SUMMARY TO ROLE R_APP_READ;

-- ============================================================================
-- Success message
-- ============================================================================
SELECT 'Workload procedures created successfully!' AS status,
       'Next step: Run 04_provision_users.sql' AS next_action;