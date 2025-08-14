-- ============================================================================
-- Snowpark Container Services MCP Setup
-- Creates roles, permissions, warehouses, and stored procedures for MCP
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE CLAUDE_BI;

-- ============================================================================
-- Step 1: Create MCP Roles
-- ============================================================================

-- Role for the container service itself
CREATE ROLE IF NOT EXISTS MCP_SERVICE_ROLE
  COMMENT = 'Role for MCP container service to manage resources';

-- Role for query execution with restricted permissions  
CREATE ROLE IF NOT EXISTS MCP_EXECUTOR_ROLE
  COMMENT = 'Role for MCP to execute queries with read-only access';

-- Role for end users to call MCP functions
CREATE ROLE IF NOT EXISTS MCP_USER_ROLE
  COMMENT = 'Role for users to invoke MCP tools';

-- ============================================================================
-- Step 2: Create MCP Warehouse
-- ============================================================================

-- Extra small warehouse for MCP queries
CREATE WAREHOUSE IF NOT EXISTS MCP_XS_WH
  WITH WAREHOUSE_SIZE = 'XSMALL'
  WAREHOUSE_TYPE = 'STANDARD'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  MIN_CLUSTER_COUNT = 1
  MAX_CLUSTER_COUNT = 1
  SCALING_POLICY = 'STANDARD'
  COMMENT = 'Extra small warehouse for MCP query execution';

-- ============================================================================
-- Step 3: Create Resource Monitor
-- ============================================================================

CREATE RESOURCE MONITOR IF NOT EXISTS MCP_DAILY_MONITOR
  WITH CREDIT_QUOTA = 10
  FREQUENCY = DAILY
  START_TIMESTAMP = IMMEDIATELY
  TRIGGERS 
    ON 75 PERCENT DO NOTIFY
    ON 90 PERCENT DO NOTIFY
    ON 100 PERCENT DO SUSPEND
  COMMENT = 'Daily credit limit for MCP operations';

-- Attach monitor to warehouse
ALTER WAREHOUSE MCP_XS_WH SET RESOURCE_MONITOR = MCP_DAILY_MONITOR;

-- ============================================================================
-- Step 4: Grant Permissions to MCP_EXECUTOR_ROLE
-- ============================================================================

-- Database and schema access
GRANT USAGE ON DATABASE CLAUDE_BI TO ROLE MCP_EXECUTOR_ROLE;
GRANT USAGE ON SCHEMA CLAUDE_BI.ACTIVITY TO ROLE MCP_EXECUTOR_ROLE;
GRANT USAGE ON SCHEMA CLAUDE_BI.ACTIVITY_CCODE TO ROLE MCP_EXECUTOR_ROLE;
GRANT USAGE ON SCHEMA CLAUDE_BI.ANALYTICS TO ROLE MCP_EXECUTOR_ROLE;

-- Warehouse access
GRANT USAGE ON WAREHOUSE MCP_XS_WH TO ROLE MCP_EXECUTOR_ROLE;

-- Read-only table access
GRANT SELECT ON ALL TABLES IN SCHEMA CLAUDE_BI.ACTIVITY TO ROLE MCP_EXECUTOR_ROLE;
GRANT SELECT ON ALL VIEWS IN SCHEMA CLAUDE_BI.ACTIVITY_CCODE TO ROLE MCP_EXECUTOR_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA CLAUDE_BI.ACTIVITY TO ROLE MCP_EXECUTOR_ROLE;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA CLAUDE_BI.ACTIVITY_CCODE TO ROLE MCP_EXECUTOR_ROLE;

-- Allow writing to activity events for logging
GRANT INSERT ON TABLE CLAUDE_BI.ACTIVITY.EVENTS TO ROLE MCP_EXECUTOR_ROLE;

-- ============================================================================
-- Step 5: Grant Permissions to MCP_SERVICE_ROLE
-- ============================================================================

-- Container service needs broader permissions
GRANT ROLE MCP_EXECUTOR_ROLE TO ROLE MCP_SERVICE_ROLE;
GRANT CREATE COMPUTE POOL ON ACCOUNT TO ROLE MCP_SERVICE_ROLE;
GRANT CREATE SERVICE ON SCHEMA CLAUDE_BI.PUBLIC TO ROLE MCP_SERVICE_ROLE;

-- ============================================================================
-- Step 6: Create Container Image Repository
-- ============================================================================

CREATE IMAGE REPOSITORY IF NOT EXISTS CLAUDE_BI.PUBLIC.MCP_REPO
  COMMENT = 'Repository for MCP container images';

GRANT READ, WRITE ON IMAGE REPOSITORY CLAUDE_BI.PUBLIC.MCP_REPO TO ROLE MCP_SERVICE_ROLE;

-- ============================================================================
-- Step 7: Create Compute Pool for Container Services
-- ============================================================================

CREATE COMPUTE POOL IF NOT EXISTS MCP_COMPUTE_POOL
  MIN_NODES = 1
  MAX_NODES = 2
  INSTANCE_FAMILY = CPU_X64_XS
  COMMENT = 'Compute pool for MCP container services';

GRANT USAGE ON COMPUTE POOL MCP_COMPUTE_POOL TO ROLE MCP_SERVICE_ROLE;

-- ============================================================================
-- Step 8: Create Stage for Configuration Files
-- ============================================================================

CREATE STAGE IF NOT EXISTS CLAUDE_BI.PUBLIC.MCP_STAGE
  COMMENT = 'Stage for MCP configuration files and contracts';

GRANT READ ON STAGE CLAUDE_BI.PUBLIC.MCP_STAGE TO ROLE MCP_SERVICE_ROLE;
GRANT READ ON STAGE CLAUDE_BI.PUBLIC.MCP_STAGE TO ROLE MCP_EXECUTOR_ROLE;

-- ============================================================================
-- Step 9: Create MCP Schema and Stored Procedures
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS CLAUDE_BI.MCP
  COMMENT = 'Schema for MCP stored procedures and functions';

USE SCHEMA CLAUDE_BI.MCP;

-- Stored procedure to validate query plans
CREATE OR REPLACE PROCEDURE VALIDATE_QUERY_PLAN(plan VARIANT)
  RETURNS VARIANT
  LANGUAGE SQL
  EXECUTE AS CALLER
  COMMENT = 'Validates a query plan against the schema contract'
AS
$$
DECLARE
  source STRING;
  errors ARRAY;
  max_rows INTEGER;
BEGIN
  errors := ARRAY_CONSTRUCT();
  source := plan:source::STRING;
  
  -- Check if source exists
  IF (source NOT IN (
    'ACTIVITY.EVENTS',
    'ACTIVITY_CCODE.VW_ACTIVITY_SUMMARY', 
    'ACTIVITY_CCODE.VW_ACTIVITY_COUNTS_24H',
    'ACTIVITY_CCODE.ARTIFACTS'
  )) THEN
    errors := ARRAY_APPEND(errors, 'Unknown source: ' || source);
  END IF;
  
  -- Check row limit
  max_rows := COALESCE(plan:top_n::INTEGER, 10000);
  IF (max_rows > 10000) THEN
    errors := ARRAY_APPEND(errors, 'Row limit exceeds maximum of 10000');
  END IF;
  
  -- Return validation result
  IF (ARRAY_SIZE(errors) > 0) THEN
    RETURN OBJECT_CONSTRUCT('valid', FALSE, 'errors', errors);
  ELSE
    RETURN OBJECT_CONSTRUCT('valid', TRUE, 'message', 'Plan is valid');
  END IF;
END;
$$;

-- Stored procedure to execute validated query plans
CREATE OR REPLACE PROCEDURE EXECUTE_QUERY_PLAN(plan VARIANT)
  RETURNS TABLE()
  LANGUAGE SQL
  EXECUTE AS CALLER
  COMMENT = 'Executes a validated query plan with security constraints'
AS
$$
DECLARE
  validation_result VARIANT;
  source STRING;
  sql_text STRING;
  top_n INTEGER;
BEGIN
  -- Validate the plan first
  CALL VALIDATE_QUERY_PLAN(:plan) INTO :validation_result;
  
  IF (NOT validation_result:valid::BOOLEAN) THEN
    RETURN TABLE(SELECT 'Error' AS STATUS, validation_result:errors AS ERRORS);
  END IF;
  
  -- Build safe SQL from plan (simplified version)
  source := 'CLAUDE_BI.' || plan:source::STRING;
  top_n := COALESCE(plan:top_n::INTEGER, 1000);
  
  -- Set session constraints
  ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 30;
  ALTER SESSION SET USE_CACHED_RESULT = TRUE;
  ALTER SESSION SET QUERY_TAG = OBJECT_CONSTRUCT(
    'mcp_procedure', 'EXECUTE_QUERY_PLAN',
    'mcp_user', CURRENT_USER(),
    'mcp_timestamp', CURRENT_TIMESTAMP()
  );
  
  -- Execute query (simplified - in production, build full SQL from plan)
  sql_text := 'SELECT * FROM ' || source || ' LIMIT ' || top_n::STRING;
  
  RETURN TABLE(EXECUTE IMMEDIATE :sql_text);
END;
$$;

-- Function to render SQL from query plan
CREATE OR REPLACE FUNCTION RENDER_SAFE_SQL(plan VARIANT)
  RETURNS STRING
  LANGUAGE SQL
  COMMENT = 'Renders safe SQL from a validated query plan'
AS
$$
  SELECT 
    'SELECT ' || 
    COALESCE(ARRAY_TO_STRING(plan:dimensions::ARRAY, ', '), '*') ||
    ' FROM CLAUDE_BI.' || plan:source::STRING ||
    CASE 
      WHEN plan:filters IS NOT NULL THEN ' WHERE 1=1'
      ELSE ''
    END ||
    CASE
      WHEN plan:top_n IS NOT NULL THEN ' LIMIT ' || plan:top_n::STRING
      ELSE ' LIMIT 10000'
    END
$$;

-- ============================================================================
-- Step 10: Grant Procedure Access to Users
-- ============================================================================

GRANT USAGE ON SCHEMA CLAUDE_BI.MCP TO ROLE MCP_USER_ROLE;
GRANT USAGE ON PROCEDURE CLAUDE_BI.MCP.VALIDATE_QUERY_PLAN(VARIANT) TO ROLE MCP_USER_ROLE;
GRANT USAGE ON PROCEDURE CLAUDE_BI.MCP.EXECUTE_QUERY_PLAN(VARIANT) TO ROLE MCP_USER_ROLE;
GRANT USAGE ON FUNCTION CLAUDE_BI.MCP.RENDER_SAFE_SQL(VARIANT) TO ROLE MCP_USER_ROLE;

-- Grant MCP_USER_ROLE to specific users
GRANT ROLE MCP_USER_ROLE TO ROLE CLAUDE_BI_ROLE;

-- ============================================================================
-- Step 11: Create External Access Integration (for hybrid approach)
-- ============================================================================

-- If you want MCP to call external validators
CREATE NETWORK RULE IF NOT EXISTS MCP_NETWORK_RULE
  TYPE = HOST_PORT
  VALUE_LIST = ('mcp-validator.workers.dev:443')
  MODE = EGRESS
  COMMENT = 'Allow MCP to call external validation service';

CREATE EXTERNAL ACCESS INTEGRATION IF NOT EXISTS MCP_EXTERNAL_ACCESS
  ALLOWED_NETWORK_RULES = (MCP_NETWORK_RULE)
  ENABLED = TRUE
  COMMENT = 'External access for MCP validation';

-- ============================================================================
-- Step 12: Create Activity Tracking Task
-- ============================================================================

CREATE TASK IF NOT EXISTS MONITOR_MCP_USAGE
  WAREHOUSE = MCP_XS_WH
  SCHEDULE = 'USING CRON 0 * * * * UTC'  -- Every hour
  COMMENT = 'Monitor MCP usage and costs'
AS
INSERT INTO CLAUDE_BI.ACTIVITY.EVENTS (
  activity_id,
  ts,
  customer,
  activity,
  feature_json,
  _source_system
)
SELECT
  'act_' || UUID_STRING() AS activity_id,
  CURRENT_TIMESTAMP() AS ts,
  'system' AS customer,
  'ccode.mcp.usage_tracked' AS activity,
  OBJECT_CONSTRUCT(
    'hour', DATE_TRUNC('hour', CURRENT_TIMESTAMP()),
    'credits_used', (
      SELECT SUM(credits_used) 
      FROM TABLE(INFORMATION_SCHEMA.WAREHOUSE_METERING_HISTORY(
        DATE_RANGE_START => DATEADD('hour', -1, CURRENT_TIMESTAMP()),
        WAREHOUSE_NAME => 'MCP_XS_WH'
      ))
    ),
    'query_count', (
      SELECT COUNT(*) 
      FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(
        DATE_RANGE_START => DATEADD('hour', -1, CURRENT_TIMESTAMP())
      ))
      WHERE QUERY_TAG:mcp_procedure IS NOT NULL
    ),
    'unique_users', (
      SELECT COUNT(DISTINCT USER_NAME)
      FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(
        DATE_RANGE_START => DATEADD('hour', -1, CURRENT_TIMESTAMP())
      ))
      WHERE QUERY_TAG:mcp_procedure IS NOT NULL
    )
  ) AS feature_json,
  'mcp_monitor' AS _source_system;

-- Start the monitoring task
ALTER TASK MONITOR_MCP_USAGE RESUME;

-- ============================================================================
-- Step 13: Deploy Container Service
-- ============================================================================

-- Upload container image (run from local machine with Docker):
-- docker build -t mcp-server:latest ./snowpark
-- docker tag mcp-server:latest <account>.registry.snowflakecomputing.com/CLAUDE_BI/PUBLIC/MCP_REPO/mcp-server:latest
-- docker push <account>.registry.snowflakecomputing.com/CLAUDE_BI/PUBLIC/MCP_REPO/mcp-server:latest

-- Create the service
CREATE SERVICE IF NOT EXISTS CLAUDE_BI.PUBLIC.MCP_SERVER
  IN COMPUTE POOL MCP_COMPUTE_POOL
  FROM @CLAUDE_BI.PUBLIC.MCP_STAGE/service.yaml
  COMMENT = 'MCP server running in Snowpark Container Services';

-- ============================================================================
-- Step 14: Test the Setup
-- ============================================================================

-- Test stored procedure
CALL CLAUDE_BI.MCP.EXECUTE_QUERY_PLAN(PARSE_JSON('{
  "source": "ACTIVITY_CCODE.VW_ACTIVITY_SUMMARY",
  "top_n": 10
}'));

-- Check service status
SHOW SERVICES IN SCHEMA CLAUDE_BI.PUBLIC;
SELECT SYSTEM$GET_SERVICE_STATUS('CLAUDE_BI.PUBLIC.MCP_SERVER');

-- Get service endpoint
SELECT SYSTEM$GET_SERVICE_ENDPOINT('CLAUDE_BI.PUBLIC.MCP_SERVER', 'api');

-- ============================================================================
-- Cleanup Commands (if needed)
-- ============================================================================
-- DROP SERVICE IF EXISTS CLAUDE_BI.PUBLIC.MCP_SERVER;
-- DROP COMPUTE POOL IF EXISTS MCP_COMPUTE_POOL;
-- DROP WAREHOUSE IF EXISTS MCP_XS_WH;
-- DROP ROLE IF EXISTS MCP_SERVICE_ROLE;
-- DROP ROLE IF EXISTS MCP_EXECUTOR_ROLE;
-- DROP ROLE IF EXISTS MCP_USER_ROLE;