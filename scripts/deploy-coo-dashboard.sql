-- Deploy COO-First Executive Dashboard
-- Zero-friction interface with preset cards and natural language refinement

-- @statement
USE DATABASE CLAUDE_BI;

-- @statement
USE SCHEMA MCP;

-- @statement
-- Create stage for dashboard specs if it doesn't exist
CREATE STAGE IF NOT EXISTS MCP.DASH_SPECS 
COMMENT = 'Dashboard specifications JSON files';

-- @statement
-- Create stage for dashboard snapshots
CREATE STAGE IF NOT EXISTS MCP.DASH_SNAPSHOTS
COMMENT = 'Dashboard snapshot artifacts (PNG/PDF/JSON)';

-- @statement
-- Upload the COO dashboard app to stage
PUT file:///Users/chandler/claude7/GrowthZone/SnowflakePOC2/stage/coo_dashboard.py 
  @MCP.DASH_APPS 
  OVERWRITE = TRUE
  AUTO_COMPRESS = FALSE;

-- @statement
-- Create the COO Executive Dashboard Streamlit app
CREATE OR REPLACE STREAMLIT MCP.COO_EXECUTIVE_DASHBOARD
  ROOT_LOCATION = '@MCP.DASH_APPS'
  MAIN_FILE = 'coo_dashboard.py'
  QUERY_WAREHOUSE = 'CLAUDE_AGENT_WH'
  COMMENT = 'COO-First Executive Dashboard with one-click analytics';

-- @statement
-- Create view for dashboard schedules
CREATE OR REPLACE VIEW MCP.VW_DASHBOARD_SCHEDULES AS
WITH schedule_events AS (
    SELECT 
        payload:attributes:schedule_id::STRING as schedule_id,
        payload:attributes:dashboard_id::STRING as dashboard_id,
        payload:attributes:frequency::STRING as frequency,
        payload:attributes:time::STRING as scheduled_time,
        payload:attributes:timezone::STRING as timezone,
        payload:attributes:display_tz::STRING as display_tz,
        payload:attributes:deliveries as deliveries,
        payload:attributes:next_run::TIMESTAMP_TZ as next_run,
        actor_id,
        occurred_at as created_at
    FROM ACTIVITY.EVENTS
    WHERE action = 'dashboard.schedule_created'
        AND occurred_at >= DATEADD('day', -90, CURRENT_TIMESTAMP())
),
latest_schedules AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY dashboard_id 
            ORDER BY created_at DESC
        ) as rn
    FROM schedule_events
)
SELECT 
    schedule_id,
    dashboard_id,
    frequency,
    scheduled_time,
    timezone,
    display_tz,
    deliveries,
    next_run,
    actor_id as created_by,
    created_at
FROM latest_schedules
WHERE rn = 1;

-- @statement
-- Create view for dashboard snapshots
CREATE OR REPLACE VIEW MCP.VW_DASHBOARD_SNAPSHOTS AS
SELECT 
    payload:attributes:snapshot_id::STRING as snapshot_id,
    payload:attributes:dashboard_id::STRING as dashboard_id,
    payload:attributes:schedule_id::STRING as schedule_id,
    payload:attributes:snapshot_path::STRING as snapshot_path,
    payload:attributes:format::STRING as format,
    payload:attributes:row_count::NUMBER as row_count,
    payload:attributes:generated_at::TIMESTAMP_TZ as generated_at,
    occurred_at
FROM ACTIVITY.EVENTS
WHERE action = 'dashboard.snapshot_generated'
    AND occurred_at >= DATEADD('day', -30, CURRENT_TIMESTAMP())
ORDER BY occurred_at DESC;

-- @statement
-- Test the COO dashboard by creating a sample event
CALL MCP.LOG_CLAUDE_EVENT(OBJECT_CONSTRUCT(
    'action', 'ui.dashboard_deployed',
    'actor_id', CURRENT_USER(),
    'object', OBJECT_CONSTRUCT(
        'type', 'streamlit_app',
        'id', 'COO_EXECUTIVE_DASHBOARD'
    ),
    'attributes', OBJECT_CONSTRUCT(
        'version', '1.0',
        'features', ARRAY_CONSTRUCT(
            'preset_cards',
            'natural_language',
            'result_canvas',
            'dashboard_builder',
            'schedule_modal'
        ),
        'deployed_at', CURRENT_TIMESTAMP()
    ),
    'occurred_at', CURRENT_TIMESTAMP()
), 'DEPLOYMENT');

-- @statement
-- Show the deployed Streamlit app info
SHOW STREAMLITS LIKE 'COO_EXECUTIVE_DASHBOARD' IN SCHEMA MCP;