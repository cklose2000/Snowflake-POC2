-- =============================================================================
-- Deploy MCP Chat Support Procedures (Simplified)
-- =============================================================================

USE DATABASE CLAUDE_BI;
USE SCHEMA MCP;

-- -----------------------------------------------------------------------------
-- Simple helper view for common chat queries
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

-- Grant permissions
GRANT SELECT ON VIEW MCP.CHAT_COMMON_QUERIES TO ROLE CLAUDE_BI_READONLY;

-- -----------------------------------------------------------------------------
-- Simple procedure to get event metrics
-- -----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE MCP.GET_CHAT_METRICS(time_range VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
AS
BEGIN
    LET total_events NUMBER;
    LET unique_actors NUMBER;
    LET unique_actions NUMBER;
    
    -- Calculate metrics based on time range
    IF (time_range = 'last_hour') THEN
        SELECT COUNT(*), COUNT(DISTINCT ACTOR_ID), COUNT(DISTINCT ACTION)
        INTO :total_events, :unique_actors, :unique_actions
        FROM CLAUDE_BI.ACTIVITY.EVENTS
        WHERE OCCURRED_AT >= CURRENT_TIMESTAMP() - INTERVAL '1 hour';
    ELSEIF (time_range = 'last_24_hours') THEN
        SELECT COUNT(*), COUNT(DISTINCT ACTOR_ID), COUNT(DISTINCT ACTION)
        INTO :total_events, :unique_actors, :unique_actions
        FROM CLAUDE_BI.ACTIVITY.EVENTS
        WHERE OCCURRED_AT >= CURRENT_TIMESTAMP() - INTERVAL '24 hours';
    ELSEIF (time_range = 'last_30_days') THEN
        SELECT COUNT(*), COUNT(DISTINCT ACTOR_ID), COUNT(DISTINCT ACTION)
        INTO :total_events, :unique_actors, :unique_actions
        FROM CLAUDE_BI.ACTIVITY.EVENTS
        WHERE OCCURRED_AT >= CURRENT_TIMESTAMP() - INTERVAL '30 days';
    ELSE -- default to 7 days
        SELECT COUNT(*), COUNT(DISTINCT ACTOR_ID), COUNT(DISTINCT ACTION)
        INTO :total_events, :unique_actors, :unique_actions
        FROM CLAUDE_BI.ACTIVITY.EVENTS
        WHERE OCCURRED_AT >= CURRENT_TIMESTAMP() - INTERVAL '7 days';
    END IF;
    
    -- Return as JSON string
    RETURN OBJECT_CONSTRUCT(
        'total_events', total_events,
        'unique_actors', unique_actors,
        'unique_actions', unique_actions,
        'time_range', time_range
    )::VARCHAR;
END;

-- Grant permissions
GRANT USAGE ON PROCEDURE MCP.GET_CHAT_METRICS(VARCHAR) TO ROLE CLAUDE_BI_READONLY;

-- -----------------------------------------------------------------------------
-- Simple procedure to get top actors
-- -----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE MCP.GET_TOP_ACTORS(time_range VARCHAR, limit_count NUMBER)
RETURNS TABLE(actor_id VARCHAR, event_count NUMBER)
LANGUAGE SQL
AS
BEGIN
    IF (time_range = 'last_hour') THEN
        RETURN TABLE(
            SELECT ACTOR_ID, COUNT(*) as event_count
            FROM CLAUDE_BI.ACTIVITY.EVENTS
            WHERE OCCURRED_AT >= CURRENT_TIMESTAMP() - INTERVAL '1 hour'
            GROUP BY ACTOR_ID
            ORDER BY event_count DESC
            LIMIT limit_count
        );
    ELSEIF (time_range = 'last_24_hours') THEN
        RETURN TABLE(
            SELECT ACTOR_ID, COUNT(*) as event_count
            FROM CLAUDE_BI.ACTIVITY.EVENTS
            WHERE OCCURRED_AT >= CURRENT_TIMESTAMP() - INTERVAL '24 hours'
            GROUP BY ACTOR_ID
            ORDER BY event_count DESC
            LIMIT limit_count
        );
    ELSE -- default to 7 days
        RETURN TABLE(
            SELECT ACTOR_ID, COUNT(*) as event_count
            FROM CLAUDE_BI.ACTIVITY.EVENTS
            WHERE OCCURRED_AT >= CURRENT_TIMESTAMP() - INTERVAL '7 days'
            GROUP BY ACTOR_ID
            ORDER BY event_count DESC
            LIMIT limit_count
        );
    END IF;
END;

-- Grant permissions
GRANT USAGE ON PROCEDURE MCP.GET_TOP_ACTORS(VARCHAR, NUMBER) TO ROLE CLAUDE_BI_READONLY;

-- -----------------------------------------------------------------------------
-- Simple procedure to get top actions
-- -----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE MCP.GET_TOP_ACTIONS(time_range VARCHAR, limit_count NUMBER)
RETURNS TABLE(action VARCHAR, event_count NUMBER)
LANGUAGE SQL
AS
BEGIN
    IF (time_range = 'last_hour') THEN
        RETURN TABLE(
            SELECT ACTION, COUNT(*) as event_count
            FROM CLAUDE_BI.ACTIVITY.EVENTS
            WHERE OCCURRED_AT >= CURRENT_TIMESTAMP() - INTERVAL '1 hour'
            GROUP BY ACTION
            ORDER BY event_count DESC
            LIMIT limit_count
        );
    ELSEIF (time_range = 'last_24_hours') THEN
        RETURN TABLE(
            SELECT ACTION, COUNT(*) as event_count
            FROM CLAUDE_BI.ACTIVITY.EVENTS
            WHERE OCCURRED_AT >= CURRENT_TIMESTAMP() - INTERVAL '24 hours'
            GROUP BY ACTION
            ORDER BY event_count DESC
            LIMIT limit_count
        );
    ELSE -- default to 7 days
        RETURN TABLE(
            SELECT ACTION, COUNT(*) as event_count
            FROM CLAUDE_BI.ACTIVITY.EVENTS
            WHERE OCCURRED_AT >= CURRENT_TIMESTAMP() - INTERVAL '7 days'
            GROUP BY ACTION
            ORDER BY event_count DESC
            LIMIT limit_count
        );
    END IF;
END;

-- Grant permissions
GRANT USAGE ON PROCEDURE MCP.GET_TOP_ACTIONS(VARCHAR, NUMBER) TO ROLE CLAUDE_BI_READONLY;

-- -----------------------------------------------------------------------------
-- Log deployment
-- -----------------------------------------------------------------------------
INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS
SELECT 
    OBJECT_CONSTRUCT(
        'event_id', UUID_STRING(),
        'action', 'mcp.deployment.chat_procedures_simple',
        'actor_id', CURRENT_USER(),
        'attributes', OBJECT_CONSTRUCT(
            'procedures', ARRAY_CONSTRUCT(
                'GET_CHAT_METRICS',
                'GET_TOP_ACTORS',
                'GET_TOP_ACTIONS'
            ),
            'views', ARRAY_CONSTRUCT('CHAT_COMMON_QUERIES'),
            'version', '1.0.0'
        ),
        'occurred_at', CURRENT_TIMESTAMP()
    ),
    'SYSTEM',
    CURRENT_TIMESTAMP();

COMMIT;