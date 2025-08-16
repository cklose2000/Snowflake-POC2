-- Deploy Streamlit App to Snowflake
-- This script uploads the streamlit_app.py to a stage and creates the app

-- @statement
USE DATABASE CLAUDE_BI;

-- @statement
USE SCHEMA MCP;

-- @statement
-- Create stage for Streamlit apps if it doesn't exist
CREATE STAGE IF NOT EXISTS DASH_APPS 
  COMMENT = 'Stage for Streamlit dashboard applications';

-- @statement
-- Upload the streamlit_app.py file to the stage
-- Note: This requires the file to be uploaded via SnowSQL or UI
-- The PUT command must be run from a local client with the file:
-- PUT file:///path/to/streamlit_app.py @DASH_APPS AUTO_COMPRESS=FALSE OVERWRITE=TRUE;

-- For now, we'll create a placeholder that can be updated
-- @statement
CREATE OR REPLACE PROCEDURE UPLOAD_STREAMLIT_APP()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS OWNER
AS $$
BEGIN
  -- This is a placeholder for the upload process
  -- In production, use SnowSQL PUT command or Snowflake UI to upload
  RETURN 'Please upload streamlit_app.py to @MCP.DASH_APPS stage using SnowSQL or Snowflake UI';
END;
$$;

-- @statement
-- Create a sample Streamlit app for testing
-- This will fail if streamlit_app.py is not in the stage
CREATE OR REPLACE STREAMLIT DASH_SAMPLE_APP
  ROOT_LOCATION = '@MCP.DASH_APPS'
  MAIN_FILE = 'streamlit_app.py'
  QUERY_WAREHOUSE = 'CLAUDE_AGENT_WH'
  COMMENT = 'Sample dashboard app for testing';

-- @statement
-- Grant permissions to view and execute
GRANT USAGE ON STREAMLIT DASH_SAMPLE_APP TO ROLE R_APP_READ;

-- @statement
-- Create helper procedure to get Streamlit URL
CREATE OR REPLACE PROCEDURE GET_STREAMLIT_URL(app_name STRING)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS OWNER
AS $$
DECLARE
  url STRING;
BEGIN
  -- Get the Streamlit URL
  SELECT SYSTEM$GET_STREAMLIT_URL('MCP.' || app_name) INTO url;
  RETURN url;
EXCEPTION
  WHEN OTHER THEN
    -- Construct URL manually if system function fails
    RETURN 'https://uec18397.us-east-1.snowflakecomputing.com/dashboards/' || app_name;
END;
$$;

-- Instructions for deployment:
-- 1. Save streamlit_app.py locally
-- 2. Use SnowSQL to upload:
--    snowsql -a uec18397.us-east-1 -u CLAUDE_CODE_AI_AGENT --private-key-path /path/to/key.p8
--    USE DATABASE CLAUDE_BI;
--    USE SCHEMA MCP;
--    PUT file:///path/to/streamlit_app.py @DASH_APPS AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
-- 3. Run this script to create the stage and app structure
-- 4. Test with: CALL GET_STREAMLIT_URL('DASH_SAMPLE_APP');