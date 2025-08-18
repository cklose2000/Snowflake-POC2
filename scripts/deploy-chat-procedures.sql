-- =============================================================================
-- MCP Chat Support Procedures
-- Provides intent detection and guardrailed query execution for chat interface
-- =============================================================================

USE DATABASE CLAUDE_BI;
USE SCHEMA MCP;

-- -----------------------------------------------------------------------------
-- SUGGEST_INTENT: Understand what the user is asking for
-- -----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE MCP.SUGGEST_INTENT(params VARIANT)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
DECLARE
    user_input STRING;
    context STRING;
    time_filter STRING;
    result VARIANT;
BEGIN
    -- Extract parameters
    user_input := LOWER(params:user_input::STRING);
    context := COALESCE(params:context::STRING, 'chat_interface');
    time_filter := COALESCE(params:time_filter::STRING, 'last_7_days');
    
    -- Detect intent based on keywords
    LET intent := CASE
        WHEN user_input RLIKE '(count|how many|total)' THEN 'count'
        WHEN user_input RLIKE '(top|most|highest)' THEN 'top'
        WHEN user_input RLIKE '(recent|latest|last)' THEN 'recent'
        WHEN user_input RLIKE '(error|fail|issue|problem)' THEN 'errors'
        WHEN user_input RLIKE '(trend|over time|timeline)' THEN 'trend'
        WHEN user_input RLIKE '(compare|versus|vs)' THEN 'compare'
        ELSE 'explore'
    END;
    
    -- Detect entity type
    LET entity := CASE
        WHEN user_input RLIKE '(user|actor|who)' THEN 'actors'
        WHEN user_input RLIKE '(action|what|activity)' THEN 'actions'
        WHEN user_input RLIKE '(source|where from)' THEN 'sources'
        WHEN user_input RLIKE '(object|target)' THEN 'objects'
        ELSE 'events'
    END;
    
    -- Detect time range
    LET time_range := CASE
        WHEN user_input RLIKE '(today|24 hour|yesterday)' THEN 'last_24_hours'
        WHEN user_input RLIKE '(week|7 day)' THEN 'last_7_days'
        WHEN user_input RLIKE '(month|30 day)' THEN 'last_30_days'
        WHEN user_input RLIKE '(year|365)' THEN 'last_year'
        WHEN user_input RLIKE '(hour|60 min)' THEN 'last_hour'
        ELSE time_filter
    END;
    
    -- Detect aggregation type
    LET aggregation := CASE
        WHEN user_input RLIKE '(sum|total)' THEN 'sum'
        WHEN user_input RLIKE '(average|avg|mean)' THEN 'avg'
        WHEN user_input RLIKE '(max|maximum|highest)' THEN 'max'
        WHEN user_input RLIKE '(min|minimum|lowest)' THEN 'min'
        WHEN user_input RLIKE '(count|number)' THEN 'count'
        ELSE 'count'
    END;
    
    -- Build filters based on intent
    LET filters := OBJECT_CONSTRUCT();
    
    IF (intent = 'errors') THEN
        filters := OBJECT_INSERT(filters, 'action_filter', 
            'ACTION LIKE ''%error%'' OR ACTION LIKE ''%fail%''');
    END IF;
    
    -- Add time filter
    LET time_condition := CASE time_range
        WHEN 'last_hour' THEN 'OCCURRED_AT >= CURRENT_TIMESTAMP() - INTERVAL ''1 hour'''
        WHEN 'last_24_hours' THEN 'OCCURRED_AT >= CURRENT_TIMESTAMP() - INTERVAL ''24 hours'''
        WHEN 'last_7_days' THEN 'OCCURRED_AT >= CURRENT_TIMESTAMP() - INTERVAL ''7 days'''
        WHEN 'last_30_days' THEN 'OCCURRED_AT >= CURRENT_TIMESTAMP() - INTERVAL ''30 days'''
        WHEN 'last_year' THEN 'OCCURRED_AT >= CURRENT_TIMESTAMP() - INTERVAL ''365 days'''
        ELSE 'OCCURRED_AT >= CURRENT_TIMESTAMP() - INTERVAL ''7 days'''
    END;
    
    filters := OBJECT_INSERT(filters, 'time_filter', time_condition);
    
    -- Build suggested query structure
    LET query_type := CASE
        WHEN intent IN ('count', 'top') THEN 'aggregate'
        WHEN intent = 'recent' THEN 'select'
        WHEN intent = 'trend' THEN 'time_series'
        ELSE 'select'
    END;
    
    -- Return the intent analysis
    result := OBJECT_CONSTRUCT(
        'intent', intent,
        'entity', entity,
        'query_type', query_type,
        'time_range', time_range,
        'filters', filters,
        'aggregations', ARRAY_CONSTRUCT(aggregation),
        'confidence', CASE 
            WHEN intent != 'explore' THEN 0.9 
            ELSE 0.5 
        END,
        'suggested_query', CASE intent
            WHEN 'count' THEN 'SELECT COUNT(*) FROM EVENTS WHERE ' || time_condition
            WHEN 'top' THEN 'SELECT ' || entity || ', COUNT(*) FROM EVENTS WHERE ' || 
                           time_condition || ' GROUP BY 1 ORDER BY 2 DESC LIMIT 10'
            WHEN 'recent' THEN 'SELECT * FROM EVENTS WHERE ' || time_condition || 
                              ' ORDER BY OCCURRED_AT DESC LIMIT 10'
            ELSE 'SELECT * FROM EVENTS WHERE ' || time_condition || ' LIMIT 100'
        END
    );
    
    RETURN result;
END;

-- -----------------------------------------------------------------------------
-- READ: Execute queries with guardrails
-- -----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE MCP.READ(params VARIANT)
RETURNS TABLE()
LANGUAGE SQL
EXECUTE AS CALLER
AS
DECLARE
    query_text STRING;
    context STRING;
    max_rows INTEGER;
    result_cursor CURSOR FOR res;
BEGIN
    -- Extract parameters
    query_text := params:query::STRING;
    context := COALESCE(params:context::STRING, 'unknown');
    max_rows := COALESCE(params:max_rows::INTEGER, 1000);
    
    -- Validate query (basic guardrails)
    IF (query_text IS NULL OR LENGTH(query_text) = 0) THEN
        RETURN TABLE(SELECT 'Error: Empty query' as error);
    END IF;
    
    -- Check for dangerous operations
    IF (UPPER(query_text) RLIKE '.*(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE).*') THEN
        RETURN TABLE(SELECT 'Error: Only SELECT queries are allowed' as error);
    END IF;
    
    -- Ensure query is against ACTIVITY.EVENTS
    IF (NOT UPPER(query_text) RLIKE '.*ACTIVITY\.EVENTS.*' AND 
        NOT UPPER(query_text) RLIKE '.*CLAUDE_BI\.ACTIVITY\.EVENTS.*') THEN
        -- Add ACTIVITY.EVENTS if not present
        IF (NOT UPPER(query_text) RLIKE '.*FROM.*') THEN
            RETURN TABLE(SELECT 'Error: Query must specify a FROM clause' as error);
        END IF;
    END IF;
    
    -- Add row limit if not present
    IF (NOT UPPER(query_text) RLIKE '.*LIMIT.*') THEN
        query_text := query_text || ' LIMIT ' || max_rows::STRING;
    END IF;
    
    -- Log the query attempt
    INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS
    SELECT 
        OBJECT_CONSTRUCT(
            'event_id', UUID_STRING(),
            'action', 'mcp.read.query_executed',
            'actor_id', CURRENT_USER(),
            'attributes', OBJECT_CONSTRUCT(
                'query', query_text,
                'context', context,
                'max_rows', max_rows
            ),
            'occurred_at', CURRENT_TIMESTAMP()
        ),
        'MCP',
        CURRENT_TIMESTAMP();
    
    -- Execute the query
    BEGIN
        res := (EXECUTE IMMEDIATE :query_text);
        RETURN TABLE(res);
    EXCEPTION
        WHEN OTHER THEN
            -- Log error
            INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS
            SELECT 
                OBJECT_CONSTRUCT(
                    'event_id', UUID_STRING(),
                    'action', 'mcp.read.query_failed',
                    'actor_id', CURRENT_USER(),
                    'attributes', OBJECT_CONSTRUCT(
                        'query', query_text,
                        'error', SQLERRM,
                        'context', context
                    ),
                    'occurred_at', CURRENT_TIMESTAMP()
                ),
                'MCP',
                CURRENT_TIMESTAMP();
            
            RETURN TABLE(SELECT 'Error: ' || SQLERRM as error);
    END;
END;

-- -----------------------------------------------------------------------------
-- DASH_GET_EVENTS: Get events for dashboard (simplified)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE MCP.DASH_GET_EVENTS(time_range VARCHAR)
RETURNS TABLE(
    occurred_at TIMESTAMP_TZ,
    action VARCHAR,
    actor_id VARCHAR,
    object_type VARCHAR,
    object_id VARCHAR,
    source VARCHAR
)
LANGUAGE SQL
AS
DECLARE
    time_filter STRING;
BEGIN
    -- Build time filter
    time_filter := CASE time_range
        WHEN 'last_hour' THEN 'OCCURRED_AT >= CURRENT_TIMESTAMP() - INTERVAL ''1 hour'''
        WHEN 'last_24_hours' THEN 'OCCURRED_AT >= CURRENT_TIMESTAMP() - INTERVAL ''24 hours'''
        WHEN 'last_7_days' THEN 'OCCURRED_AT >= CURRENT_TIMESTAMP() - INTERVAL ''7 days'''
        WHEN 'last_30_days' THEN 'OCCURRED_AT >= CURRENT_TIMESTAMP() - INTERVAL ''30 days'''
        ELSE 'OCCURRED_AT >= CURRENT_TIMESTAMP() - INTERVAL ''7 days'''
    END;
    
    -- Return filtered events
    RETURN TABLE(
        SELECT 
            OCCURRED_AT,
            ACTION,
            ACTOR_ID,
            OBJECT_TYPE,
            OBJECT_ID,
            SOURCE
        FROM CLAUDE_BI.ACTIVITY.EVENTS
        WHERE OCCURRED_AT >= CURRENT_TIMESTAMP() - 
              CASE time_range
                  WHEN 'last_hour' THEN INTERVAL '1 hour'
                  WHEN 'last_24_hours' THEN INTERVAL '24 hours'
                  WHEN 'last_7_days' THEN INTERVAL '7 days'
                  WHEN 'last_30_days' THEN INTERVAL '30 days'
                  ELSE INTERVAL '7 days'
              END
        ORDER BY OCCURRED_AT DESC
        LIMIT 1000
    );
END;

-- -----------------------------------------------------------------------------
-- DASH_GET_METRICS: Get key metrics for dashboard
-- -----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE MCP.DASH_GET_METRICS(time_range VARCHAR)
RETURNS TABLE(
    total_events NUMBER,
    unique_actors NUMBER,
    unique_actions NUMBER,
    active_days NUMBER
)
LANGUAGE SQL
AS
BEGIN
    RETURN TABLE(
        SELECT 
            COUNT(*) as total_events,
            COUNT(DISTINCT ACTOR_ID) as unique_actors,
            COUNT(DISTINCT ACTION) as unique_actions,
            COUNT(DISTINCT DATE(OCCURRED_AT)) as active_days
        FROM CLAUDE_BI.ACTIVITY.EVENTS
        WHERE OCCURRED_AT >= CURRENT_TIMESTAMP() - 
              CASE time_range
                  WHEN 'last_hour' THEN INTERVAL '1 hour'
                  WHEN 'last_24_hours' THEN INTERVAL '24 hours'
                  WHEN 'last_7_days' THEN INTERVAL '7 days'
                  WHEN 'last_30_days' THEN INTERVAL '30 days'
                  ELSE INTERVAL '7 days'
              END
    );
END;

-- -----------------------------------------------------------------------------
-- DASH_GET_SERIES: Get time series data for charts
-- -----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE MCP.DASH_GET_SERIES(time_range VARCHAR, granularity VARCHAR)
RETURNS TABLE(
    time_bucket TIMESTAMP_TZ,
    event_count NUMBER,
    unique_actors NUMBER
)
LANGUAGE SQL
AS
BEGIN
    RETURN TABLE(
        SELECT 
            DATE_TRUNC(granularity, OCCURRED_AT) as time_bucket,
            COUNT(*) as event_count,
            COUNT(DISTINCT ACTOR_ID) as unique_actors
        FROM CLAUDE_BI.ACTIVITY.EVENTS
        WHERE OCCURRED_AT >= CURRENT_TIMESTAMP() - 
              CASE time_range
                  WHEN 'last_hour' THEN INTERVAL '1 hour'
                  WHEN 'last_24_hours' THEN INTERVAL '24 hours'
                  WHEN 'last_7_days' THEN INTERVAL '7 days'
                  WHEN 'last_30_days' THEN INTERVAL '30 days'
                  ELSE INTERVAL '7 days'
              END
        GROUP BY 1
        ORDER BY 1 DESC
        LIMIT 100
    );
END;

-- -----------------------------------------------------------------------------
-- Grant permissions
-- -----------------------------------------------------------------------------
GRANT USAGE ON PROCEDURE MCP.SUGGEST_INTENT(VARIANT) TO ROLE CLAUDE_BI_READONLY;
GRANT USAGE ON PROCEDURE MCP.READ(VARIANT) TO ROLE CLAUDE_BI_READONLY;
GRANT USAGE ON PROCEDURE MCP.DASH_GET_EVENTS(VARCHAR) TO ROLE CLAUDE_BI_READONLY;
GRANT USAGE ON PROCEDURE MCP.DASH_GET_METRICS(VARCHAR) TO ROLE CLAUDE_BI_READONLY;
GRANT USAGE ON PROCEDURE MCP.DASH_GET_SERIES(VARCHAR, VARCHAR) TO ROLE CLAUDE_BI_READONLY;

-- Log deployment
INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS
SELECT 
    OBJECT_CONSTRUCT(
        'event_id', UUID_STRING(),
        'action', 'mcp.deployment.chat_procedures',
        'actor_id', CURRENT_USER(),
        'attributes', OBJECT_CONSTRUCT(
            'procedures', ARRAY_CONSTRUCT(
                'SUGGEST_INTENT',
                'READ',
                'DASH_GET_EVENTS',
                'DASH_GET_METRICS',
                'DASH_GET_SERIES'
            ),
            'version', '1.0.0'
        ),
        'occurred_at', CURRENT_TIMESTAMP()
    ),
    'SYSTEM',
    CURRENT_TIMESTAMP();

COMMIT;