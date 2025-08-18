-- =============================================================================
-- Deploy Chat Support Views (Simple approach without procedures)
-- =============================================================================

USE DATABASE CLAUDE_BI;
USE SCHEMA MCP;

-- -----------------------------------------------------------------------------
-- View for common chat queries
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW MCP.CHAT_COMMON_QUERIES AS
SELECT 
    'summary' as query_type,
    'Show activity summary' as description,
    'SELECT COUNT(*) as events, COUNT(DISTINCT ACTOR_ID) as actors FROM ACTIVITY.EVENTS WHERE OCCURRED_AT >= CURRENT_TIMESTAMP() - INTERVAL ''7 days''' as query_template
UNION ALL
SELECT 
    'top_actors' as query_type,
    'Top active users' as description,
    'SELECT ACTOR_ID, COUNT(*) as count FROM ACTIVITY.EVENTS WHERE OCCURRED_AT >= CURRENT_TIMESTAMP() - INTERVAL ''7 days'' GROUP BY 1 ORDER BY 2 DESC LIMIT 10' as query_template
UNION ALL
SELECT 
    'top_actions' as query_type,
    'Most common actions' as description,
    'SELECT ACTION, COUNT(*) as count FROM ACTIVITY.EVENTS WHERE OCCURRED_AT >= CURRENT_TIMESTAMP() - INTERVAL ''7 days'' GROUP BY 1 ORDER BY 2 DESC LIMIT 10' as query_template
UNION ALL
SELECT 
    'recent_events' as query_type,
    'Recent activity' as description,
    'SELECT * FROM ACTIVITY.EVENTS ORDER BY OCCURRED_AT DESC LIMIT 20' as query_template
UNION ALL
SELECT 
    'errors' as query_type,
    'Recent errors' as description,
    'SELECT * FROM ACTIVITY.EVENTS WHERE ACTION LIKE ''%error%'' OR ACTION LIKE ''%fail%'' ORDER BY OCCURRED_AT DESC LIMIT 20' as query_template;

-- -----------------------------------------------------------------------------
-- View for chat metrics (last 7 days)
-- -----------------------------------------------------------------------------
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

-- -----------------------------------------------------------------------------
-- View for chat metrics (last 24 hours)
-- -----------------------------------------------------------------------------
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

-- -----------------------------------------------------------------------------
-- View for top actors (last 7 days)
-- -----------------------------------------------------------------------------
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

-- -----------------------------------------------------------------------------
-- View for top actions (last 7 days)
-- -----------------------------------------------------------------------------
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

-- -----------------------------------------------------------------------------
-- View for recent errors
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW MCP.CHAT_RECENT_ERRORS AS
SELECT 
    OCCURRED_AT,
    ACTION,
    ACTOR_ID,
    OBJECT_TYPE,
    OBJECT_ID,
    ATTRIBUTES:error_message::STRING as error_message,
    ATTRIBUTES:error_code::STRING as error_code,
    SOURCE
FROM CLAUDE_BI.ACTIVITY.EVENTS
WHERE (ACTION LIKE '%error%' OR ACTION LIKE '%fail%' OR ACTION LIKE '%issue%')
    AND OCCURRED_AT >= CURRENT_TIMESTAMP() - INTERVAL '24 hours'
ORDER BY OCCURRED_AT DESC
LIMIT 100;

-- -----------------------------------------------------------------------------
-- View for activity timeline (hourly buckets)
-- -----------------------------------------------------------------------------
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

-- -----------------------------------------------------------------------------
-- Grant permissions (after views are created)
-- -----------------------------------------------------------------------------
GRANT SELECT ON VIEW MCP.CHAT_COMMON_QUERIES TO ROLE CLAUDE_BI_READONLY;
GRANT SELECT ON VIEW MCP.CHAT_METRICS_7D TO ROLE CLAUDE_BI_READONLY;
GRANT SELECT ON VIEW MCP.CHAT_METRICS_24H TO ROLE CLAUDE_BI_READONLY;
GRANT SELECT ON VIEW MCP.CHAT_TOP_ACTORS_7D TO ROLE CLAUDE_BI_READONLY;
GRANT SELECT ON VIEW MCP.CHAT_TOP_ACTIONS_7D TO ROLE CLAUDE_BI_READONLY;
GRANT SELECT ON VIEW MCP.CHAT_RECENT_ERRORS TO ROLE CLAUDE_BI_READONLY;
GRANT SELECT ON VIEW MCP.CHAT_ACTIVITY_TIMELINE TO ROLE CLAUDE_BI_READONLY;

-- -----------------------------------------------------------------------------
-- Log deployment
-- -----------------------------------------------------------------------------
INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS
SELECT 
    OBJECT_CONSTRUCT(
        'event_id', UUID_STRING(),
        'action', 'mcp.deployment.chat_views',
        'actor_id', CURRENT_USER(),
        'attributes', OBJECT_CONSTRUCT(
            'views', ARRAY_CONSTRUCT(
                'CHAT_COMMON_QUERIES',
                'CHAT_METRICS_7D',
                'CHAT_METRICS_24H',
                'CHAT_TOP_ACTORS_7D',
                'CHAT_TOP_ACTIONS_7D',
                'CHAT_RECENT_ERRORS',
                'CHAT_ACTIVITY_TIMELINE'
            ),
            'version', '1.0.0'
        ),
        'occurred_at', CURRENT_TIMESTAMP()
    ),
    'SYSTEM',
    CURRENT_TIMESTAMP();

-- Show what was created
SELECT 'Chat support views created successfully!' as status;

SELECT 
    TABLE_NAME as view_name,
    COMMENT as description
FROM CLAUDE_BI.INFORMATION_SCHEMA.VIEWS
WHERE TABLE_SCHEMA = 'MCP'
    AND TABLE_NAME LIKE 'CHAT_%'
ORDER BY TABLE_NAME;

COMMIT;