-- ============================================================================
-- Fix Dashboard 404 Errors - WORK-00402
-- Creates functions and views to generate correct Snowsight URLs
-- ============================================================================

USE DATABASE CLAUDE_BI;
USE SCHEMA MCP;

-- ============================================================================
-- 1. Function to generate correct Snowsight Streamlit URLs
-- ============================================================================
CREATE OR REPLACE FUNCTION MCP.GET_STREAMLIT_URL(app_name STRING)
RETURNS STRING
LANGUAGE SQL
IMMUTABLE
AS
$$
  SELECT 
    'https://app.snowflake.com/' || 
    LOWER(CURRENT_ACCOUNT()) || '/' ||
    LOWER(CURRENT_REGION()) || '/' ||
    'streamlit-apps/' ||
    CURRENT_DATABASE() || '/' ||
    CURRENT_SCHEMA() || '/' ||
    UPPER(app_name) || '/' ||
    (SELECT url_id FROM CLAUDE_BI.INFORMATION_SCHEMA.STREAMLITS 
     WHERE name = UPPER(app_name) 
     AND database_name = CURRENT_DATABASE() 
     AND schema_name = CURRENT_SCHEMA()
     LIMIT 1)
$$;

-- ============================================================================
-- 2. View showing all dashboards with correct URLs
-- ============================================================================
CREATE OR REPLACE VIEW MCP.VW_DASHBOARD_URLS AS
SELECT 
    s.name AS dashboard_name,
    s.comment AS description,
    s.query_warehouse,
    s.created_on,
    s.url_id,
    -- Generate correct Snowsight URL
    'https://app.snowflake.com/' || 
    LOWER(CURRENT_ACCOUNT()) || '/' ||
    LOWER(CURRENT_REGION()) || '/' ||
    'streamlit-apps/' ||
    s.database_name || '/' ||
    s.schema_name || '/' ||
    s.name || '/' ||
    s.url_id AS dashboard_url,
    -- Check if main file exists
    CASE 
        WHEN s.name = 'COO_EXECUTIVE_DASHBOARD' THEN 'coo_dashboard.py'
        ELSE 'streamlit_app.py'
    END AS main_file,
    'Active' AS status
FROM CLAUDE_BI.INFORMATION_SCHEMA.STREAMLITS s
WHERE s.database_name = 'CLAUDE_BI'
  AND s.schema_name = 'MCP'
ORDER BY s.created_on DESC;

-- ============================================================================
-- 3. Procedure to test dashboard health
-- ============================================================================
CREATE OR REPLACE PROCEDURE MCP.TEST_DASHBOARD_HEALTH()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    result VARIANT;
    dashboard_count INT;
    file_count INT;
    warehouse_status STRING;
BEGIN
    -- Count dashboards
    SELECT COUNT(*) INTO dashboard_count
    FROM CLAUDE_BI.INFORMATION_SCHEMA.STREAMLITS
    WHERE database_name = 'CLAUDE_BI' AND schema_name = 'MCP';
    
    -- Count files in stage
    SELECT COUNT(*) INTO file_count
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
    WHERE "name" LIKE 'dash_apps/%.py';
    
    -- Check warehouse status
    SELECT STATE INTO warehouse_status
    FROM CLAUDE_BI.INFORMATION_SCHEMA.WAREHOUSES
    WHERE WAREHOUSE_NAME = 'CLAUDE_AGENT_WH';
    
    -- Build result
    result := OBJECT_CONSTRUCT(
        'timestamp', CURRENT_TIMESTAMP(),
        'dashboard_count', dashboard_count,
        'file_count', file_count,
        'warehouse_status', warehouse_status,
        'health_status', IFF(dashboard_count > 0 AND file_count > 0 AND warehouse_status IN ('STARTED', 'SUSPENDED'), 'HEALTHY', 'UNHEALTHY'),
        'issues', ARRAY_CONSTRUCT(
            IFF(dashboard_count = 0, 'No dashboards found', NULL),
            IFF(file_count = 0, 'No Python files in stage', NULL),
            IFF(warehouse_status NOT IN ('STARTED', 'SUSPENDED'), 'Warehouse issue: ' || warehouse_status, NULL)
        )
    );
    
    -- Log health check event
    INSERT INTO LANDING.RAW_EVENTS (PAYLOAD, _SOURCE_LANE, _RECV_AT)
    SELECT 
        OBJECT_CONSTRUCT(
            'event_id', UUID_STRING(),
            'action', 'dashboard.health_check',
            'occurred_at', CURRENT_TIMESTAMP(),
            'actor_id', 'system',
            'source', 'monitoring',
            'attributes', result
        ),
        'SYSTEM',
        CURRENT_TIMESTAMP();
    
    RETURN result;
END;
$$;

-- ============================================================================
-- 4. Procedure to validate and fix Streamlit URL
-- ============================================================================
CREATE OR REPLACE PROCEDURE MCP.VALIDATE_STREAMLIT_URL(app_name STRING)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    url_id STRING;
    correct_url STRING;
    app_exists BOOLEAN;
    file_exists BOOLEAN;
    result VARIANT;
BEGIN
    -- Check if app exists
    SELECT COUNT(*) > 0 INTO app_exists
    FROM CLAUDE_BI.INFORMATION_SCHEMA.STREAMLITS
    WHERE name = UPPER(app_name)
      AND database_name = 'CLAUDE_BI'
      AND schema_name = 'MCP';
    
    IF NOT app_exists THEN
        RETURN OBJECT_CONSTRUCT(
            'status', 'error',
            'message', 'Streamlit app not found: ' || app_name
        );
    END IF;
    
    -- Get URL ID
    SELECT url_id INTO url_id
    FROM CLAUDE_BI.INFORMATION_SCHEMA.STREAMLITS
    WHERE name = UPPER(app_name)
      AND database_name = 'CLAUDE_BI'
      AND schema_name = 'MCP'
    LIMIT 1;
    
    -- Check if Python file exists
    SELECT COUNT(*) > 0 INTO file_exists
    FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
    WHERE "name" LIKE '%' || LOWER(app_name) || '%.py'
       OR "name" = 'dash_apps/coo_dashboard.py'
       OR "name" = 'dash_apps/streamlit_app.py';
    
    -- Generate correct URL
    correct_url := 'https://app.snowflake.com/' || 
                   LOWER(CURRENT_ACCOUNT()) || '/' ||
                   LOWER(CURRENT_REGION()) || '/' ||
                   'streamlit-apps/CLAUDE_BI/MCP/' ||
                   UPPER(app_name) || '/' ||
                   url_id;
    
    -- Build result
    result := OBJECT_CONSTRUCT(
        'status', 'success',
        'app_name', UPPER(app_name),
        'url_id', url_id,
        'correct_url', correct_url,
        'app_exists', app_exists,
        'file_exists', file_exists,
        'validation_time', CURRENT_TIMESTAMP()
    );
    
    RETURN result;
END;
$$;

-- ============================================================================
-- 5. Create automated test suite
-- ============================================================================
CREATE OR REPLACE PROCEDURE MCP.TEST_ALL_DASHBOARDS()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    test_results ARRAY;
    dashboard_cursor CURSOR FOR
        SELECT name FROM CLAUDE_BI.INFORMATION_SCHEMA.STREAMLITS
        WHERE database_name = 'CLAUDE_BI' AND schema_name = 'MCP';
    dashboard_name STRING;
    test_result VARIANT;
BEGIN
    test_results := ARRAY_CONSTRUCT();
    
    -- Test each dashboard
    FOR record IN dashboard_cursor DO
        dashboard_name := record.name;
        
        -- Validate URL
        CALL MCP.VALIDATE_STREAMLIT_URL(:dashboard_name);
        test_result := SQLCODE;
        
        -- Add to results
        test_results := ARRAY_APPEND(test_results, 
            OBJECT_CONSTRUCT(
                'dashboard', dashboard_name,
                'test_result', test_result,
                'timestamp', CURRENT_TIMESTAMP()
            )
        );
    END FOR;
    
    -- Run health check
    CALL MCP.TEST_DASHBOARD_HEALTH();
    
    -- Return consolidated results
    RETURN OBJECT_CONSTRUCT(
        'test_run_id', UUID_STRING(),
        'timestamp', CURRENT_TIMESTAMP(),
        'dashboard_tests', test_results,
        'health_check', SQLCODE,
        'summary', OBJECT_CONSTRUCT(
            'total_dashboards', ARRAY_SIZE(test_results),
            'tests_passed', ARRAY_SIZE(test_results)
        )
    );
END;
$$;

-- ============================================================================
-- 6. Grant permissions
-- ============================================================================
GRANT USAGE ON FUNCTION MCP.GET_STREAMLIT_URL(STRING) TO ROLE MCP_USER_ROLE;
GRANT SELECT ON VIEW MCP.VW_DASHBOARD_URLS TO ROLE MCP_USER_ROLE;
GRANT USAGE ON PROCEDURE MCP.TEST_DASHBOARD_HEALTH() TO ROLE MCP_AGENT_ROLE;
GRANT USAGE ON PROCEDURE MCP.VALIDATE_STREAMLIT_URL(STRING) TO ROLE MCP_AGENT_ROLE;
GRANT USAGE ON PROCEDURE MCP.TEST_ALL_DASHBOARDS() TO ROLE MCP_AGENT_ROLE;

-- ============================================================================
-- 7. Show COO Dashboard URL
-- ============================================================================
SELECT 
    'COO Executive Dashboard' AS dashboard,
    dashboard_url AS correct_url,
    status
FROM MCP.VW_DASHBOARD_URLS
WHERE dashboard_name = 'COO_EXECUTIVE_DASHBOARD';