-- ============================================================================
-- Incremental Load Task for HappyFox Data
-- Purpose: Automated daily loading of new/updated HappyFox tickets
-- Maintains idempotency and two-table architecture
-- ============================================================================

USE DATABASE CLAUDE_BI;
USE WAREHOUSE CLAUDE_WAREHOUSE;

-- ----------------------------------------------------------------------------
-- CREATE TASK FOR DAILY INCREMENTAL LOADS
-- ----------------------------------------------------------------------------

CREATE OR REPLACE TASK LANDING.TASK_HAPPYFOX_DAILY_LOAD
    WAREHOUSE = CLAUDE_WAREHOUSE
    SCHEDULE = 'USING CRON 0 2 * * * America/New_York'  -- 2 AM ET daily
    COMMENT = 'Daily incremental load of HappyFox tickets from inbox stage'
AS
    CALL LANDING.LOAD_HAPPYFOX_INCREMENTAL();

-- ----------------------------------------------------------------------------
-- CREATE MONITORING TASK (Optional - runs after main task)
-- ----------------------------------------------------------------------------

CREATE OR REPLACE TASK LANDING.TASK_HAPPYFOX_MONITOR
    WAREHOUSE = CLAUDE_WAREHOUSE
    AFTER LANDING.TASK_HAPPYFOX_DAILY_LOAD
    COMMENT = 'Monitor and alert on HappyFox load results'
AS
DECLARE
    load_result VARIANT;
    alert_message STRING;
BEGIN
    -- Get the latest load result
    SELECT DATA:attributes 
    INTO load_result
    FROM LANDING.RAW_EVENTS
    WHERE DATA:action = 'system.data.loaded'
      AND DATA:source = 'HAPPYFOX_LOADER'
    ORDER BY OCCURRED_AT DESC
    LIMIT 1;
    
    -- Check if we need to alert
    IF (load_result:status::STRING != 'SUCCESS') THEN
        -- Log an alert event
        INSERT INTO LANDING.RAW_EVENTS (DATA, SOURCE, OCCURRED_AT)
        VALUES (
            OBJECT_CONSTRUCT(
                'event_id', UUID_STRING(),
                'action', 'system.alert.raised',
                'actor_id', 'SYSTEM',
                'source', 'HAPPYFOX_MONITOR',
                'attributes', OBJECT_CONSTRUCT(
                    'alert_type', 'LOAD_FAILURE',
                    'load_result', load_result,
                    'message', 'HappyFox daily load did not complete successfully'
                ),
                'occurred_at', CURRENT_TIMESTAMP()
            ),
            'SYSTEM',
            CURRENT_TIMESTAMP()
        );
    ELSEIF (load_result:loaded_count::NUMBER = 0 AND load_result:files_processed::NUMBER > 0) THEN
        -- Alert if files were processed but nothing was loaded (all duplicates)
        INSERT INTO LANDING.RAW_EVENTS (DATA, SOURCE, OCCURRED_AT)
        VALUES (
            OBJECT_CONSTRUCT(
                'event_id', UUID_STRING(),
                'action', 'system.alert.info',
                'actor_id', 'SYSTEM',
                'source', 'HAPPYFOX_MONITOR',
                'attributes', OBJECT_CONSTRUCT(
                    'alert_type', 'NO_NEW_DATA',
                    'load_result', load_result,
                    'message', 'HappyFox files processed but no new tickets found'
                ),
                'occurred_at', CURRENT_TIMESTAMP()
            ),
            'SYSTEM',
            CURRENT_TIMESTAMP()
        );
    END IF;
    
    RETURN load_result;
END;

-- ----------------------------------------------------------------------------
-- CREATE CLEANUP TASK (Runs weekly)
-- ----------------------------------------------------------------------------

CREATE OR REPLACE TASK LANDING.TASK_HAPPYFOX_CLEANUP
    WAREHOUSE = CLAUDE_WAREHOUSE
    SCHEDULE = 'USING CRON 0 3 * * 0 America/New_York'  -- 3 AM ET on Sundays
    COMMENT = 'Weekly cleanup of processed HappyFox stage files'
AS
BEGIN
    -- Clean up any old files from historical stage (older than 7 days)
    REMOVE @LANDING.STG_HAPPYFOX_HISTORICAL PATTERN='.*' MODIFIED_BEFORE=DATEADD('day', -7, CURRENT_TIMESTAMP());
    
    -- Log cleanup event
    INSERT INTO LANDING.RAW_EVENTS (DATA, SOURCE, OCCURRED_AT)
    VALUES (
        OBJECT_CONSTRUCT(
            'event_id', UUID_STRING(),
            'action', 'system.maintenance.cleanup',
            'actor_id', 'SYSTEM',
            'source', 'HAPPYFOX_CLEANUP',
            'attributes', OBJECT_CONSTRUCT(
                'stage', 'STG_HAPPYFOX_HISTORICAL',
                'cleanup_type', 'old_files',
                'retention_days', 7
            ),
            'occurred_at', CURRENT_TIMESTAMP()
        ),
        'SYSTEM',
        CURRENT_TIMESTAMP()
    );
    
    RETURN 'Cleanup completed';
END;

-- ----------------------------------------------------------------------------
-- TASK MANAGEMENT PROCEDURES
-- ----------------------------------------------------------------------------

-- Procedure to enable all HappyFox tasks
CREATE OR REPLACE PROCEDURE LANDING.ENABLE_HAPPYFOX_TASKS()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
BEGIN
    ALTER TASK LANDING.TASK_HAPPYFOX_MONITOR RESUME;
    ALTER TASK LANDING.TASK_HAPPYFOX_DAILY_LOAD RESUME;
    ALTER TASK LANDING.TASK_HAPPYFOX_CLEANUP RESUME;
    
    RETURN 'All HappyFox tasks enabled';
END;
$$;

-- Procedure to disable all HappyFox tasks
CREATE OR REPLACE PROCEDURE LANDING.DISABLE_HAPPYFOX_TASKS()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
BEGIN
    ALTER TASK LANDING.TASK_HAPPYFOX_DAILY_LOAD SUSPEND;
    ALTER TASK LANDING.TASK_HAPPYFOX_MONITOR SUSPEND;
    ALTER TASK LANDING.TASK_HAPPYFOX_CLEANUP SUSPEND;
    
    RETURN 'All HappyFox tasks disabled';
END;
$$;

-- Procedure to manually trigger incremental load
CREATE OR REPLACE PROCEDURE LANDING.RUN_HAPPYFOX_LOAD_NOW()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    result VARIANT;
BEGIN
    -- Execute the incremental load
    CALL LANDING.LOAD_HAPPYFOX_INCREMENTAL() INTO :result;
    
    -- Also run the monitor
    EXECUTE IMMEDIATE 'EXECUTE TASK LANDING.TASK_HAPPYFOX_MONITOR';
    
    RETURN result;
END;
$$;

-- ----------------------------------------------------------------------------
-- TASK STATUS VIEW
-- ----------------------------------------------------------------------------

CREATE OR REPLACE VIEW LANDING.VW_HAPPYFOX_TASK_STATUS AS
SELECT
    NAME AS task_name,
    DATABASE_NAME,
    SCHEMA_NAME,
    STATE AS task_state,
    SCHEDULE,
    PREDECESSOR,
    LAST_SUCCESSFUL_SCHEDULED_TIME,
    NEXT_SCHEDULED_TIME,
    ERROR_MESSAGE AS last_error,
    COMMENT
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD('day', -7, CURRENT_TIMESTAMP()),
    TASK_NAME => 'TASK_HAPPYFOX%'
))
ORDER BY SCHEDULED_TIME DESC;

-- ----------------------------------------------------------------------------
-- GRANT PERMISSIONS
-- ----------------------------------------------------------------------------

GRANT USAGE ON PROCEDURE LANDING.ENABLE_HAPPYFOX_TASKS() TO ROLE SYSADMIN;
GRANT USAGE ON PROCEDURE LANDING.DISABLE_HAPPYFOX_TASKS() TO ROLE SYSADMIN;
GRANT USAGE ON PROCEDURE LANDING.RUN_HAPPYFOX_LOAD_NOW() TO ROLE SYSADMIN;
GRANT SELECT ON VIEW LANDING.VW_HAPPYFOX_TASK_STATUS TO ROLE SYSADMIN;

-- ----------------------------------------------------------------------------
-- INITIAL STATE
-- ----------------------------------------------------------------------------

-- Tasks are created in SUSPENDED state by default
-- Run this to enable them:
-- CALL LANDING.ENABLE_HAPPYFOX_TASKS();

SELECT 
    'Tasks created in SUSPENDED state' AS status,
    'Run: CALL LANDING.ENABLE_HAPPYFOX_TASKS(); to activate' AS next_step,
    'Run: CALL LANDING.RUN_HAPPYFOX_LOAD_NOW(); for manual execution' AS manual_option;