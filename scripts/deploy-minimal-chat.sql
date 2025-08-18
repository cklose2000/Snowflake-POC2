-- =============================================================================
-- Deploy Minimal Chat Dashboard to Snowflake
-- =============================================================================

USE DATABASE CLAUDE_BI;
USE SCHEMA MCP;

-- -----------------------------------------------------------------------------
-- 1. Create or replace the dashboard stage
-- -----------------------------------------------------------------------------
CREATE STAGE IF NOT EXISTS MCP.DASH_APPS
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Stage for Streamlit dashboard applications';

-- -----------------------------------------------------------------------------
-- 2. Upload the chat dashboard files
-- -----------------------------------------------------------------------------
-- Note: Run these PUT commands from SnowSQL or your local terminal:
-- PUT file:///Users/chandler/claude7/GrowthZone/SnowflakePOC2/dashboards/minimal_chat/app.py @MCP.DASH_APPS/minimal_chat/ OVERWRITE=TRUE AUTO_COMPRESS=FALSE;
-- PUT file:///Users/chandler/claude7/GrowthZone/SnowflakePOC2/dashboards/minimal_chat/app_simple.py @MCP.DASH_APPS/minimal_chat/ OVERWRITE=TRUE AUTO_COMPRESS=FALSE;

-- -----------------------------------------------------------------------------
-- 3. Create the Streamlit dashboard (simple version first)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE STREAMLIT CLAUDE_BI.MCP.MINIMAL_CHAT_DASHBOARD
    ROOT_LOCATION = '@CLAUDE_BI.MCP.DASH_APPS/minimal_chat'
    MAIN_FILE = 'app_simple.py'
    QUERY_WAREHOUSE = 'CLAUDE_WAREHOUSE'
    COMMENT = 'Minimal data-first chat interface for querying activity events';

-- -----------------------------------------------------------------------------
-- 4. Grant access to the dashboard
-- -----------------------------------------------------------------------------
GRANT USAGE ON STREAMLIT CLAUDE_BI.MCP.MINIMAL_CHAT_DASHBOARD TO ROLE CLAUDE_BI_READONLY;
GRANT USAGE ON STREAMLIT CLAUDE_BI.MCP.MINIMAL_CHAT_DASHBOARD TO ROLE ACCOUNTADMIN;

-- -----------------------------------------------------------------------------
-- 5. Create an advanced version with MCP procedures
-- -----------------------------------------------------------------------------
CREATE OR REPLACE STREAMLIT CLAUDE_BI.MCP.MINIMAL_CHAT_ADVANCED
    ROOT_LOCATION = '@CLAUDE_BI.MCP.DASH_APPS/minimal_chat'
    MAIN_FILE = 'app.py'
    QUERY_WAREHOUSE = 'CLAUDE_WAREHOUSE'
    COMMENT = 'Advanced chat with MCP procedure integration';

GRANT USAGE ON STREAMLIT CLAUDE_BI.MCP.MINIMAL_CHAT_ADVANCED TO ROLE CLAUDE_BI_READONLY;
GRANT USAGE ON STREAMLIT CLAUDE_BI.MCP.MINIMAL_CHAT_ADVANCED TO ROLE ACCOUNTADMIN;

-- -----------------------------------------------------------------------------
-- 6. Log the deployment
-- -----------------------------------------------------------------------------
INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS
SELECT 
    OBJECT_CONSTRUCT(
        'event_id', UUID_STRING(),
        'action', 'dashboard.minimal_chat.deployed',
        'actor_id', CURRENT_USER(),
        'attributes', OBJECT_CONSTRUCT(
            'dashboards', ARRAY_CONSTRUCT(
                'MINIMAL_CHAT_DASHBOARD',
                'MINIMAL_CHAT_ADVANCED'
            ),
            'main_files', ARRAY_CONSTRUCT(
                'app_simple.py',
                'app.py'
            ),
            'features', ARRAY_CONSTRUCT(
                'direct_sql_execution',
                'pattern_matching',
                'quick_actions',
                'data_export',
                'mcp_integration'
            )
        ),
        'occurred_at', CURRENT_TIMESTAMP()
    ),
    'SYSTEM',
    CURRENT_TIMESTAMP();

-- -----------------------------------------------------------------------------
-- 7. Create helper view for common queries
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW MCP.CHAT_COMMON_QUERIES AS
SELECT 
    'summary' as query_type,
    'Show activity summary' as description,
    $$ SELECT COUNT(*) as events, COUNT(DISTINCT ACTOR_ID) as actors FROM ACTIVITY.EVENTS WHERE OCCURRED_AT >= CURRENT_TIMESTAMP() - INTERVAL '7 days' $$ as query_template
UNION ALL
SELECT 
    'top_actors' as query_type,
    'Top active users' as description,
    $$ SELECT ACTOR_ID, COUNT(*) as count FROM ACTIVITY.EVENTS WHERE OCCURRED_AT >= CURRENT_TIMESTAMP() - INTERVAL '7 days' GROUP BY 1 ORDER BY 2 DESC LIMIT 10 $$ as query_template
UNION ALL
SELECT 
    'top_actions' as query_type,
    'Most common actions' as description,
    $$ SELECT ACTION, COUNT(*) as count FROM ACTIVITY.EVENTS WHERE OCCURRED_AT >= CURRENT_TIMESTAMP() - INTERVAL '7 days' GROUP BY 1 ORDER BY 2 DESC LIMIT 10 $$ as query_template
UNION ALL
SELECT 
    'recent_events' as query_type,
    'Recent activity' as description,
    $$ SELECT * FROM ACTIVITY.EVENTS ORDER BY OCCURRED_AT DESC LIMIT 20 $$ as query_template
UNION ALL
SELECT 
    'errors' as query_type,
    'Recent errors' as description,
    $$ SELECT * FROM ACTIVITY.EVENTS WHERE ACTION LIKE '%error%' OR ACTION LIKE '%fail%' ORDER BY OCCURRED_AT DESC LIMIT 20 $$ as query_template;

GRANT SELECT ON VIEW MCP.CHAT_COMMON_QUERIES TO ROLE CLAUDE_BI_READONLY;

-- -----------------------------------------------------------------------------
-- Show deployment results
-- -----------------------------------------------------------------------------
SELECT 'Minimal Chat Dashboard deployed successfully!' as status;

SELECT 
    'Access the simple version at:' as message,
    'https://app.snowflake.com/[account]/[org]/apps/streamlit/CLAUDE_BI.MCP.MINIMAL_CHAT_DASHBOARD' as url
UNION ALL
SELECT 
    'Access the advanced version at:' as message,
    'https://app.snowflake.com/[account]/[org]/apps/streamlit/CLAUDE_BI.MCP.MINIMAL_CHAT_ADVANCED' as url;

-- Show what needs to be uploaded
SELECT 
    'Remember to upload the Python files using these commands:' as reminder
UNION ALL
SELECT 
    'PUT file:///.../dashboards/minimal_chat/app.py @MCP.DASH_APPS/minimal_chat/ OVERWRITE=TRUE AUTO_COMPRESS=FALSE;' as command
UNION ALL
SELECT 
    'PUT file:///.../dashboards/minimal_chat/app_simple.py @MCP.DASH_APPS/minimal_chat/ OVERWRITE=TRUE AUTO_COMPRESS=FALSE;' as command;

COMMIT;