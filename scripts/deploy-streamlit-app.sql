-- Deploy Streamlit App to Snowflake Stage
-- This uploads the Python app that powers all dashboards

-- @statement
USE DATABASE CLAUDE_BI;

-- @statement
USE SCHEMA MCP;

-- =====================================================
-- 1. Create or replace the stage for Streamlit apps
-- =====================================================
-- @statement
CREATE STAGE IF NOT EXISTS MCP.DASH_APPS 
  COMMENT = 'Stage for Streamlit dashboard applications';

-- =====================================================
-- 2. Upload the Streamlit app file
-- =====================================================
-- @statement
PUT file:///Users/chandler/claude7/GrowthZone/SnowflakePOC2/stage/streamlit_app.py 
  @MCP.DASH_APPS 
  OVERWRITE = TRUE
  AUTO_COMPRESS = FALSE;

-- =====================================================
-- 3. Verify the file was uploaded
-- =====================================================
-- @statement
LIST @MCP.DASH_APPS;

-- =====================================================
-- 4. Grant permissions for Streamlit execution
-- =====================================================
-- @statement
GRANT READ ON STAGE MCP.DASH_APPS TO ROLE ACCOUNTADMIN;

-- @statement
GRANT WRITE ON STAGE MCP.DASH_APPS TO ROLE ACCOUNTADMIN;

-- =====================================================
-- 5. Create a test Streamlit app to verify setup
-- =====================================================
-- @statement
CREATE OR REPLACE STREAMLIT MCP.TEST_DASHBOARD_APP
  ROOT_LOCATION = '@MCP.DASH_APPS'
  MAIN_FILE = 'streamlit_app.py'
  QUERY_WAREHOUSE = 'CLAUDE_AGENT_WH'
  COMMENT = 'Test dashboard app to verify Streamlit setup';

-- =====================================================
-- 6. Get the URL for the test app
-- =====================================================
-- @statement
SELECT 
  'Test App URL' as info,
  SYSTEM$GET_STREAMLIT_URL('MCP.TEST_DASHBOARD_APP') AS url;

-- =====================================================
-- 7. Show success message
-- =====================================================
-- @statement
SELECT 
  'SUCCESS' as status,
  'Streamlit app deployed to stage' as message,
  'Use ?dashboard_id=<id> parameter to view specific dashboards' as usage;