-- Smoke Test for Dashboard Creation
-- This script creates a test dashboard event directly and verifies it appears

-- @statement
USE DATABASE CLAUDE_BI;

-- @statement
USE SCHEMA MCP;

-- =====================================================
-- 1. Insert a test dashboard event directly
-- =====================================================
-- @statement
INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS (
  action,
  actor,
  occurred_at,
  dedupe_key,
  payload,
  _source_lane,
  _recv_at
)
SELECT
  'dashboard.created',
  'SMOKE_TEST',
  CURRENT_TIMESTAMP(),
  CONCAT('smoke_test_', UUID_STRING()),
  OBJECT_CONSTRUCT(
    'event_id', CONCAT('smoke_test_', UUID_STRING()),
    'action', 'dashboard.created',
    'actor_id', 'SMOKE_TEST',
    'object', OBJECT_CONSTRUCT(
      'type', 'dashboard',
      'id', CONCAT('smoke_dash_', TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HHmmss'))
    ),
    'attributes', OBJECT_CONSTRUCT(
      'title', 'Smoke Test Dashboard',
      'spec', OBJECT_CONSTRUCT(
        'panels', ARRAY_CONSTRUCT(
          OBJECT_CONSTRUCT(
            'type', 'metrics',
            'title', 'Test Metrics'
          )
        )
      ),
      'refresh_interval_sec', 300,
      'is_active', TRUE,
      'streamlit_enabled', FALSE
    ),
    'occurred_at', CURRENT_TIMESTAMP()
  ),
  'SMOKE_TEST',
  CURRENT_TIMESTAMP();

-- =====================================================
-- 2. Check RAW_EVENTS immediately
-- =====================================================
-- @statement
SELECT 
  'RAW_EVENTS Check' as check_type,
  COUNT(*) as count,
  MAX(_recv_at) as latest_recv
FROM CLAUDE_BI.LANDING.RAW_EVENTS
WHERE action = 'dashboard.created' 
   OR payload:action::STRING = 'dashboard.created';

-- =====================================================
-- 3. Force Dynamic Table refresh
-- =====================================================
-- @statement
ALTER DYNAMIC TABLE CLAUDE_BI.ACTIVITY.EVENTS REFRESH;

-- =====================================================
-- 4. Wait for refresh (simulate with a simple query)
-- =====================================================
-- @statement
SELECT SYSTEM$WAIT(2);

-- =====================================================
-- 5. Check ACTIVITY.EVENTS
-- =====================================================
-- @statement
SELECT 
  'ACTIVITY.EVENTS Check' as check_type,
  COUNT(*) as count,
  MAX(occurred_at) as latest_event
FROM CLAUDE_BI.ACTIVITY.EVENTS
WHERE action = 'dashboard.created';

-- =====================================================
-- 6. Check Dynamic Table status
-- =====================================================
-- @statement
SELECT 
  'DT Status' as check_type,
  STATE,
  STATE_MESSAGE,
  LAST_SUCCESSFUL_REFRESH,
  REFRESH_VERSION,
  ROWS_INSERTED,
  ROWS_UPDATED,
  ROWS_DELETED
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(
  NAME => 'CLAUDE_BI.ACTIVITY.EVENTS'
))
ORDER BY REFRESH_START_TIME DESC
LIMIT 1;

-- =====================================================
-- 7. Debug: Check what's actually in RAW_EVENTS
-- =====================================================
-- @statement
SELECT 
  action,
  actor,
  occurred_at,
  dedupe_key,
  payload:action::STRING as payload_action,
  _source_lane,
  _recv_at
FROM CLAUDE_BI.LANDING.RAW_EVENTS
WHERE action = 'dashboard.created' 
   OR payload:action::STRING = 'dashboard.created'
   OR _source_lane IN ('DASHBOARD_SYSTEM', 'SMOKE_TEST')
ORDER BY _recv_at DESC
LIMIT 10;

-- =====================================================
-- 8. Check if VW_DASHBOARDS sees it
-- =====================================================
-- @statement
SELECT 
  'VW_DASHBOARDS Check' as check_type,
  COUNT(*) as dashboard_count
FROM MCP.VW_DASHBOARDS;

-- =====================================================
-- 9. Show all recent events to see what IS getting through
-- =====================================================
-- @statement
SELECT 
  action,
  COUNT(*) as event_count,
  MAX(occurred_at) as latest
FROM CLAUDE_BI.ACTIVITY.EVENTS
WHERE occurred_at >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
GROUP BY action
ORDER BY event_count DESC
LIMIT 20;