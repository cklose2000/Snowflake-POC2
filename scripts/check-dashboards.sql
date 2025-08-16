-- Check existing dashboards in the system

-- @statement
USE DATABASE CLAUDE_BI;

-- @statement
USE SCHEMA MCP;

-- @statement
-- Check dashboards in VW_DASHBOARDS
SELECT 
    dashboard_id,
    title,
    created_at,
    created_by,
    JSON_EXTRACT_PATH_TEXT(spec, 'panels') as panels_json,
    refresh_interval_sec
FROM MCP.VW_DASHBOARDS
ORDER BY created_at DESC
LIMIT 10;

-- @statement  
-- Check Streamlit apps
SHOW STREAMLITS IN SCHEMA MCP;

-- @statement
-- Get specific dashboard for testing
SELECT 
    dashboard_id,
    title,
    spec
FROM MCP.VW_DASHBOARDS
WHERE title LIKE '%Executive%' OR title LIKE '%Test%'
ORDER BY created_at DESC
LIMIT 1;