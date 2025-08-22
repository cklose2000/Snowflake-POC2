-- ============================================================================
-- HappyFox Ingestion Deployment Script
-- Purpose: Set up stages, formats, and initial load for HappyFox JSONL data
-- Maintains two-table architecture: RAW_EVENTS and EVENTS only
-- ============================================================================

USE DATABASE CLAUDE_BI;
USE WAREHOUSE CLAUDE_WAREHOUSE;

-- ----------------------------------------------------------------------------
-- STEP 1: Create File Format and Stages
-- ----------------------------------------------------------------------------

-- JSONL format for HappyFox data
CREATE OR REPLACE FILE FORMAT LANDING.FF_HAPPYFOX_JSONL 
    TYPE = JSON 
    STRIP_OUTER_ARRAY = FALSE
    COMPRESSION = AUTO;

-- Stage for one-time historical load
CREATE OR REPLACE STAGE LANDING.STG_HAPPYFOX_HISTORICAL 
    FILE_FORMAT = LANDING.FF_HAPPYFOX_JSONL
    COMMENT = 'Stage for initial HappyFox historical data load';

-- Stage for incremental loads
CREATE OR REPLACE STAGE LANDING.STG_HAPPYFOX_INBOX 
    FILE_FORMAT = LANDING.FF_HAPPYFOX_JSONL
    COMMENT = 'Stage for ongoing HappyFox incremental data files';

-- ----------------------------------------------------------------------------
-- STEP 2: Initial Historical Load Procedure
-- ----------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE LANDING.LOAD_HAPPYFOX_HISTORICAL()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    start_count INTEGER;
    end_count INTEGER;
    loaded_count INTEGER;
    duplicate_count INTEGER;
    result VARIANT;
BEGIN
    -- Get starting count
    SELECT COUNT(*) INTO start_count 
    FROM LANDING.RAW_EVENTS 
    WHERE DATA:source = 'HAPPYFOX';
    
    -- Insert with idempotency check
    -- Each ticket becomes an event with action='happyfox.ticket.upserted'
    INSERT INTO LANDING.RAW_EVENTS (DATA, SOURCE, OCCURRED_AT)
    SELECT DISTINCT
        OBJECT_CONSTRUCT(
            'event_id', UUID_STRING(),
            'action', 'happyfox.ticket.upserted',
            'actor_id', 'SYSTEM',
            'source', 'HAPPYFOX',
            'object_type', 'ticket',
            'object_id', $1:id::STRING,
            'display_id', $1:display_id::STRING,
            'attributes', OBJECT_CONSTRUCT(
                'ticket_data', $1,  -- Store complete ticket JSON
                'status', $1:status:name::STRING,
                'priority', $1:priority:name::STRING,
                'category', $1:category:name::STRING,
                'subject', $1:subject::STRING,
                'assignee', $1:assigned_to:name::STRING,
                'product_prefix', REGEXP_SUBSTR($1:display_id::STRING, '^#?([A-Za-z]+)', 1, 1, 'e', 1)
            ),
            'idempotency_key', SHA2(CONCAT(
                'happyfox|',
                $1:id::STRING, '|',
                COALESCE($1:last_modified::STRING,
                        $1:last_updated_at::STRING,
                        $1:created_at::STRING)
            ), 256),
            'occurred_at', COALESCE(
                TRY_TO_TIMESTAMP_NTZ($1:last_modified::STRING),
                TRY_TO_TIMESTAMP_NTZ($1:last_updated_at::STRING),
                TRY_TO_TIMESTAMP_NTZ($1:created_at::STRING),
                CURRENT_TIMESTAMP()
            )
        ) AS DATA,
        'HAPPYFOX' AS SOURCE,
        CURRENT_TIMESTAMP() AS OCCURRED_AT
    FROM @LANDING.STG_HAPPYFOX_HISTORICAL
    WHERE NOT EXISTS (
        SELECT 1 
        FROM LANDING.RAW_EVENTS r
        WHERE r.DATA:idempotency_key = SHA2(CONCAT(
            'happyfox|',
            $1:id::STRING, '|',
            COALESCE($1:last_modified::STRING,
                    $1:last_updated_at::STRING,
                    $1:created_at::STRING)
        ), 256)
    );
    
    -- Get ending count
    SELECT COUNT(*) INTO end_count 
    FROM LANDING.RAW_EVENTS 
    WHERE DATA:source = 'HAPPYFOX';
    
    loaded_count := end_count - start_count;
    
    -- Create result
    result := OBJECT_CONSTRUCT(
        'status', 'SUCCESS',
        'loaded_count', loaded_count,
        'start_count', start_count,
        'end_count', end_count,
        'timestamp', CURRENT_TIMESTAMP()::STRING
    );
    
    -- Log the load event
    INSERT INTO LANDING.RAW_EVENTS (DATA, SOURCE, OCCURRED_AT)
    VALUES (
        OBJECT_CONSTRUCT(
            'event_id', UUID_STRING(),
            'action', 'system.data.loaded',
            'actor_id', 'SYSTEM',
            'source', 'HAPPYFOX_LOADER',
            'attributes', result,
            'occurred_at', CURRENT_TIMESTAMP()
        ),
        'SYSTEM',
        CURRENT_TIMESTAMP()
    );
    
    RETURN result;
END;
$$;

-- ----------------------------------------------------------------------------
-- STEP 3: Incremental Load Procedure
-- ----------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE LANDING.LOAD_HAPPYFOX_INCREMENTAL()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    start_count INTEGER;
    end_count INTEGER;
    loaded_count INTEGER;
    file_count INTEGER;
    result VARIANT;
BEGIN
    -- Count files in inbox
    SELECT COUNT(*) INTO file_count
    FROM DIRECTORY(@LANDING.STG_HAPPYFOX_INBOX);
    
    IF (file_count = 0) THEN
        RETURN OBJECT_CONSTRUCT(
            'status', 'NO_FILES',
            'message', 'No files found in inbox stage',
            'timestamp', CURRENT_TIMESTAMP()::STRING
        );
    END IF;
    
    -- Get starting count
    SELECT COUNT(*) INTO start_count 
    FROM LANDING.RAW_EVENTS 
    WHERE DATA:source = 'HAPPYFOX';
    
    -- Insert new/updated tickets only (idempotent)
    INSERT INTO LANDING.RAW_EVENTS (DATA, SOURCE, OCCURRED_AT)
    SELECT DISTINCT
        OBJECT_CONSTRUCT(
            'event_id', UUID_STRING(),
            'action', 'happyfox.ticket.upserted',
            'actor_id', 'SYSTEM',
            'source', 'HAPPYFOX',
            'object_type', 'ticket',
            'object_id', $1:id::STRING,
            'display_id', $1:display_id::STRING,
            'attributes', OBJECT_CONSTRUCT(
                'ticket_data', $1,  -- Store complete ticket JSON
                'status', $1:status:name::STRING,
                'priority', $1:priority:name::STRING,
                'category', $1:category:name::STRING,
                'subject', $1:subject::STRING,
                'assignee', $1:assigned_to:name::STRING,
                'product_prefix', REGEXP_SUBSTR($1:display_id::STRING, '^#?([A-Za-z]+)', 1, 1, 'e', 1)
            ),
            'idempotency_key', SHA2(CONCAT(
                'happyfox|',
                $1:id::STRING, '|',
                COALESCE($1:last_modified::STRING,
                        $1:last_updated_at::STRING,
                        $1:created_at::STRING)
            ), 256),
            'occurred_at', COALESCE(
                TRY_TO_TIMESTAMP_NTZ($1:last_modified::STRING),
                TRY_TO_TIMESTAMP_NTZ($1:last_updated_at::STRING),
                TRY_TO_TIMESTAMP_NTZ($1:created_at::STRING),
                CURRENT_TIMESTAMP()
            )
        ) AS DATA,
        'HAPPYFOX' AS SOURCE,
        CURRENT_TIMESTAMP() AS OCCURRED_AT
    FROM @LANDING.STG_HAPPYFOX_INBOX
    WHERE NOT EXISTS (
        SELECT 1 
        FROM LANDING.RAW_EVENTS r
        WHERE r.DATA:idempotency_key = SHA2(CONCAT(
            'happyfox|',
            $1:id::STRING, '|',
            COALESCE($1:last_modified::STRING,
                    $1:last_updated_at::STRING,
                    $1:created_at::STRING)
        ), 256)
    );
    
    -- Get ending count
    SELECT COUNT(*) INTO end_count 
    FROM LANDING.RAW_EVENTS 
    WHERE DATA:source = 'HAPPYFOX';
    
    loaded_count := end_count - start_count;
    
    -- Archive processed files
    REMOVE @LANDING.STG_HAPPYFOX_INBOX;
    
    -- Create result
    result := OBJECT_CONSTRUCT(
        'status', 'SUCCESS',
        'files_processed', file_count,
        'loaded_count', loaded_count,
        'start_count', start_count,
        'end_count', end_count,
        'timestamp', CURRENT_TIMESTAMP()::STRING
    );
    
    -- Log the load event
    INSERT INTO LANDING.RAW_EVENTS (DATA, SOURCE, OCCURRED_AT)
    VALUES (
        OBJECT_CONSTRUCT(
            'event_id', UUID_STRING(),
            'action', 'system.data.loaded',
            'actor_id', 'SYSTEM',
            'source', 'HAPPYFOX_LOADER',
            'attributes', result,
            'occurred_at', CURRENT_TIMESTAMP()
        ),
        'SYSTEM',
        CURRENT_TIMESTAMP()
    );
    
    RETURN result;
END;
$$;

-- ----------------------------------------------------------------------------
-- STEP 4: Helper Functions
-- ----------------------------------------------------------------------------

-- Function to check load status
CREATE OR REPLACE FUNCTION LANDING.GET_HAPPYFOX_LOAD_STATUS()
RETURNS TABLE (
    total_tickets NUMBER,
    unique_tickets NUMBER,
    earliest_ticket TIMESTAMP_NTZ,
    latest_ticket TIMESTAMP_NTZ,
    last_load_time TIMESTAMP_NTZ,
    last_load_count NUMBER
)
AS
$$
    WITH ticket_stats AS (
        SELECT 
            COUNT(*) as total_tickets,
            COUNT(DISTINCT DATA:object_id) as unique_tickets,
            MIN(DATA:occurred_at::TIMESTAMP_NTZ) as earliest_ticket,
            MAX(DATA:occurred_at::TIMESTAMP_NTZ) as latest_ticket
        FROM LANDING.RAW_EVENTS
        WHERE DATA:action = 'happyfox.ticket.upserted'
    ),
    last_load AS (
        SELECT 
            DATA:occurred_at::TIMESTAMP_NTZ as last_load_time,
            DATA:attributes:loaded_count::NUMBER as last_load_count
        FROM LANDING.RAW_EVENTS
        WHERE DATA:action = 'system.data.loaded'
          AND DATA:source = 'HAPPYFOX_LOADER'
        ORDER BY OCCURRED_AT DESC
        LIMIT 1
    )
    SELECT 
        t.total_tickets,
        t.unique_tickets,
        t.earliest_ticket,
        t.latest_ticket,
        l.last_load_time,
        l.last_load_count
    FROM ticket_stats t, last_load l
$$;

-- ----------------------------------------------------------------------------
-- STEP 5: Grant Permissions
-- ----------------------------------------------------------------------------

-- Grant usage on procedures to relevant roles
GRANT USAGE ON PROCEDURE LANDING.LOAD_HAPPYFOX_HISTORICAL() TO ROLE SYSADMIN;
GRANT USAGE ON PROCEDURE LANDING.LOAD_HAPPYFOX_INCREMENTAL() TO ROLE SYSADMIN;
GRANT SELECT ON FUNCTION LANDING.GET_HAPPYFOX_LOAD_STATUS() TO ROLE SYSADMIN;

-- ----------------------------------------------------------------------------
-- VERIFICATION
-- ----------------------------------------------------------------------------

-- Show deployment status
SELECT 'Deployment complete. Ready to load HappyFox data.' as status,
       'Use: CALL LANDING.LOAD_HAPPYFOX_HISTORICAL() after uploading files to stage' as next_step;