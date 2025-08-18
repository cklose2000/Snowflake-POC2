-- =============================================================================
-- Deploy Chat Support Views - Step 1: Create Views Only
-- =============================================================================

USE DATABASE CLAUDE_BI;
USE SCHEMA MCP;

-- View 1: Common queries
CREATE OR REPLACE VIEW MCP.CHAT_COMMON_QUERIES AS
SELECT 
    'summary' as query_type,
    'Show activity summary' as description
UNION ALL
SELECT 
    'top_actors' as query_type,
    'Top active users' as description
UNION ALL
SELECT 
    'top_actions' as query_type,
    'Most common actions' as description
UNION ALL
SELECT 
    'recent_events' as query_type,
    'Recent activity' as description
UNION ALL
SELECT 
    'errors' as query_type,
    'Recent errors' as description;

-- View 2: 7-day metrics
CREATE OR REPLACE VIEW MCP.CHAT_METRICS_7D AS
SELECT 
    COUNT(*) as total_events,
    COUNT(DISTINCT ACTOR_ID) as unique_actors,
    COUNT(DISTINCT ACTION) as unique_actions,
    COUNT(DISTINCT DATE(OCCURRED_AT)) as active_days,
    MAX(OCCURRED_AT) as latest_event,
    MIN(OCCURRED_AT) as earliest_event
FROM CLAUDE_BI.ACTIVITY.EVENTS
WHERE OCCURRED_AT >= CURRENT_TIMESTAMP() - INTERVAL '7 days';

-- View 3: 24-hour metrics
CREATE OR REPLACE VIEW MCP.CHAT_METRICS_24H AS
SELECT 
    COUNT(*) as total_events,
    COUNT(DISTINCT ACTOR_ID) as unique_actors,
    COUNT(DISTINCT ACTION) as unique_actions,
    COUNT(DISTINCT HOUR(OCCURRED_AT)) as active_hours,
    MAX(OCCURRED_AT) as latest_event,
    MIN(OCCURRED_AT) as earliest_event
FROM CLAUDE_BI.ACTIVITY.EVENTS
WHERE OCCURRED_AT >= CURRENT_TIMESTAMP() - INTERVAL '24 hours';

-- View 4: Top actors
CREATE OR REPLACE VIEW MCP.CHAT_TOP_ACTORS_7D AS
SELECT 
    ACTOR_ID,
    COUNT(*) as event_count,
    COUNT(DISTINCT ACTION) as unique_actions,
    MAX(OCCURRED_AT) as last_seen,
    MIN(OCCURRED_AT) as first_seen
FROM CLAUDE_BI.ACTIVITY.EVENTS
WHERE OCCURRED_AT >= CURRENT_TIMESTAMP() - INTERVAL '7 days'
GROUP BY ACTOR_ID
ORDER BY event_count DESC
LIMIT 20;

-- View 5: Top actions
CREATE OR REPLACE VIEW MCP.CHAT_TOP_ACTIONS_7D AS
SELECT 
    ACTION,
    COUNT(*) as occurrence_count,
    COUNT(DISTINCT ACTOR_ID) as unique_actors,
    MAX(OCCURRED_AT) as last_occurred,
    MIN(OCCURRED_AT) as first_occurred
FROM CLAUDE_BI.ACTIVITY.EVENTS
WHERE OCCURRED_AT >= CURRENT_TIMESTAMP() - INTERVAL '7 days'
GROUP BY ACTION
ORDER BY occurrence_count DESC
LIMIT 20;

-- View 6: Recent errors
CREATE OR REPLACE VIEW MCP.CHAT_RECENT_ERRORS AS
SELECT 
    OCCURRED_AT,
    ACTION,
    ACTOR_ID,
    OBJECT_TYPE,
    OBJECT_ID,
    SOURCE
FROM CLAUDE_BI.ACTIVITY.EVENTS
WHERE (ACTION LIKE '%error%' OR ACTION LIKE '%fail%' OR ACTION LIKE '%issue%')
    AND OCCURRED_AT >= CURRENT_TIMESTAMP() - INTERVAL '24 hours'
ORDER BY OCCURRED_AT DESC
LIMIT 100;

-- View 7: Activity timeline
CREATE OR REPLACE VIEW MCP.CHAT_ACTIVITY_TIMELINE AS
SELECT 
    DATE_TRUNC('hour', OCCURRED_AT) as hour_bucket,
    COUNT(*) as event_count,
    COUNT(DISTINCT ACTOR_ID) as unique_actors,
    COUNT(DISTINCT ACTION) as unique_actions
FROM CLAUDE_BI.ACTIVITY.EVENTS
WHERE OCCURRED_AT >= CURRENT_TIMESTAMP() - INTERVAL '24 hours'
GROUP BY hour_bucket
ORDER BY hour_bucket DESC;

-- Show results
SELECT 'Chat support views created successfully!' as status;

SELECT 
    TABLE_NAME as view_name,
    ROW_COUNT as rows_available
FROM CLAUDE_BI.INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'MCP'
    AND TABLE_NAME LIKE 'CHAT_%'
    AND TABLE_TYPE = 'VIEW'
ORDER BY TABLE_NAME;