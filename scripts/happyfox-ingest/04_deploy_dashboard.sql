-- Deploy HappyFox Support Analytics Dashboard to Snowflake
USE ROLE SYSADMIN;
USE DATABASE CLAUDE_BI;
USE WAREHOUSE CLAUDE_WAREHOUSE;

-- Create schema for dashboards if not exists
CREATE SCHEMA IF NOT EXISTS DASHBOARDS;

-- Create the Streamlit app
CREATE OR REPLACE STREAMLIT DASHBOARDS.HAPPYFOX_SUPPORT_ANALYTICS
ROOT_LOCATION = '@CLAUDE_BI.DASHBOARDS.STREAMLIT_APPS/happyfox_support'
MAIN_FILE = 'app.py'
QUERY_WAREHOUSE = 'CLAUDE_WAREHOUSE'
TITLE = 'HappyFox Support Analytics'
COMMENT = 'Three-tier drill-down dashboard for support ticket analysis';

-- Grant access to the app
GRANT USAGE ON STREAMLIT DASHBOARDS.HAPPYFOX_SUPPORT_ANALYTICS TO ROLE PUBLIC;

-- Create stage for app files if not exists
CREATE STAGE IF NOT EXISTS DASHBOARDS.STREAMLIT_APPS
FILE_FORMAT = (TYPE = 'CSV')
ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');

-- Note: To upload the app file, run:
-- PUT file:///Users/chandler/claude7/GrowthZone/SnowflakePOC2/dashboards/happyfox_support/app.py @CLAUDE_BI.DASHBOARDS.STREAMLIT_APPS/happyfox_support/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;

-- Alternative: Create as a stored procedure that returns the app URL
CREATE OR REPLACE PROCEDURE DASHBOARDS.LAUNCH_HAPPYFOX_DASHBOARD()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    app_url STRING;
BEGIN
    -- Get the Streamlit app URL
    SELECT 'https://app.snowflake.com/uec18397/us-east-1/' || 
           CURRENT_ACCOUNT() || '/streamlit/apps/CLAUDE_BI.DASHBOARDS.HAPPYFOX_SUPPORT_ANALYTICS'
    INTO app_url;
    
    RETURN app_url;
END;
$$;

-- Create a view to track dashboard usage
CREATE OR REPLACE VIEW DASHBOARDS.VW_HAPPYFOX_DASHBOARD_METRICS AS
SELECT 
    'Executive Summary' as dashboard_level,
    COUNT(DISTINCT object_id) as total_tickets,
    COUNT(DISTINCT CASE WHEN attributes:status::STRING NOT IN ('Closed', 'Trash') THEN object_id END) as open_tickets,
    COUNT(DISTINCT CASE WHEN attributes:priority::STRING = 'Urgent' THEN object_id END) as urgent_tickets,
    AVG(TIMESTAMPDIFF(DAY, TRY_TO_TIMESTAMP_NTZ(attributes:created_at::STRING), CURRENT_TIMESTAMP())) as avg_age_days,
    CURRENT_TIMESTAMP() as last_refreshed
FROM CLAUDE_BI.ACTIVITY.EVENTS
WHERE action LIKE 'ticket.%'
  AND source = 'HAPPYFOX';

-- Log dashboard deployment as an event
INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS VALUES (
    OBJECT_CONSTRUCT(
        'event_id', UUID_STRING(),
        'action', 'dashboard.deployed',
        'actor_id', CURRENT_USER(),
        'object', OBJECT_CONSTRUCT(
            'type', 'streamlit_app',
            'id', 'HAPPYFOX_SUPPORT_ANALYTICS',
            'name', 'HappyFox Support Analytics Dashboard'
        ),
        'attributes', OBJECT_CONSTRUCT(
            'schema', 'DASHBOARDS',
            'features', ARRAY_CONSTRUCT(
                'executive_summary',
                'category_analysis',
                'ticket_details',
                'drill_down_navigation',
                'real_time_filters',
                'export_capability'
            ),
            'data_sources', ARRAY_CONSTRUCT(
                'ACTIVITY.EVENTS',
                'MCP.VW_SUPPORT_EXECUTIVE_SUMMARY',
                'MCP.VW_SUPPORT_AGING_ANALYSIS',
                'MCP.VW_SUPPORT_AGENT_WORKLOAD'
            )
        ),
        'occurred_at', CURRENT_TIMESTAMP()
    ),
    'SYSTEM',
    CURRENT_TIMESTAMP()
);

-- Verification
SELECT 'Dashboard deployment complete' as status,
       'Run CALL DASHBOARDS.LAUNCH_HAPPYFOX_DASHBOARD() to get the app URL' as next_step;