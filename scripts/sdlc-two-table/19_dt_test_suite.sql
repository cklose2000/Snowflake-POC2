-- ============================================================================
-- 19_dt_test_suite.sql
-- Comprehensive test suite for Dynamic Table functionality
-- Tests LANDING.RAW_EVENTS → ACTIVITY.EVENTS pipeline
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE CLAUDE_BI;
USE SCHEMA MCP;

-- ============================================================================
-- Test 1: Row Count Consistency
-- ============================================================================
CREATE OR REPLACE PROCEDURE TEST_DT_ROW_COUNTS()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
COMMENT = 'Test that row counts match between source and target tables'
AS
$$
DECLARE
  initial_raw_count INTEGER;
  initial_events_count INTEGER;
  test_event_count INTEGER DEFAULT 10;
  final_raw_count INTEGER;
  final_events_count INTEGER;
  wait_seconds INTEGER DEFAULT 90; -- Wait longer than 1-minute target lag
  i INTEGER;
BEGIN
  -- Get initial counts
  SELECT COUNT(*) INTO :initial_raw_count FROM CLAUDE_BI.LANDING.RAW_EVENTS;
  SELECT COUNT(*) INTO :initial_events_count FROM CLAUDE_BI.ACTIVITY.EVENTS;
  
  -- Insert test events
  FOR i IN 1 TO test_event_count DO
    INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS (PAYLOAD, _SOURCE_LANE, _RECV_AT)
    SELECT 
      OBJECT_CONSTRUCT(
        'event_id', 'DT_TEST_' || UUID_STRING(),
        'action', 'dt.test.row_count',
        'occurred_at', CURRENT_TIMESTAMP(),
        'actor_id', 'DT_TEST_SUITE',
        'source', 'test_suite',
        'object', OBJECT_CONSTRUCT(
          'type', 'TEST',
          'id', 'ROW_COUNT_' || :i
        ),
        'attributes', OBJECT_CONSTRUCT(
          'test_number', :i,
          'test_run', CURRENT_TIMESTAMP()
        )
      ),
      'DT_TEST',
      CURRENT_TIMESTAMP();
  END FOR;
  
  -- Wait for Dynamic Table refresh
  CALL SYSTEM$WAIT(:wait_seconds);
  
  -- Force a manual refresh if needed
  ALTER DYNAMIC TABLE CLAUDE_BI.ACTIVITY.EVENTS REFRESH;
  
  -- Get final counts
  SELECT COUNT(*) INTO :final_raw_count FROM CLAUDE_BI.LANDING.RAW_EVENTS;
  SELECT COUNT(*) INTO :final_events_count FROM CLAUDE_BI.ACTIVITY.EVENTS;
  
  -- Check if counts match
  IF (:final_raw_count - :initial_raw_count) = (:final_events_count - :initial_events_count) THEN
    RETURN OBJECT_CONSTRUCT(
      'test_name', 'ROW_COUNT_CONSISTENCY',
      'status', 'PASS',
      'events_inserted', :test_event_count,
      'raw_delta', :final_raw_count - :initial_raw_count,
      'events_delta', :final_events_count - :initial_events_count,
      'message', 'Row counts are consistent'
    );
  ELSE
    RETURN OBJECT_CONSTRUCT(
      'test_name', 'ROW_COUNT_CONSISTENCY',
      'status', 'FAIL',
      'events_inserted', :test_event_count,
      'raw_delta', :final_raw_count - :initial_raw_count,
      'events_delta', :final_events_count - :initial_events_count,
      'message', 'Row count mismatch detected'
    );
  END IF;
END;
$$;

-- ============================================================================
-- Test 2: Lag and Refresh Monitoring
-- ============================================================================
CREATE OR REPLACE PROCEDURE TEST_DT_LAG()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
COMMENT = 'Test that Dynamic Table meets target lag SLA'
AS
$$
DECLARE
  test_event_id STRING;
  insert_time TIMESTAMP_TZ;
  promotion_time TIMESTAMP_TZ;
  lag_seconds INTEGER;
  max_wait_seconds INTEGER DEFAULT 180;
  check_interval INTEGER DEFAULT 10;
  elapsed INTEGER DEFAULT 0;
BEGIN
  -- Generate unique test event
  SET test_event_id = 'LAG_TEST_' || UUID_STRING();
  SET insert_time = CURRENT_TIMESTAMP();
  
  -- Insert test event
  INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS (PAYLOAD, _SOURCE_LANE, _RECV_AT)
  SELECT 
    OBJECT_CONSTRUCT(
      'event_id', :test_event_id,
      'action', 'dt.test.lag_check',
      'occurred_at', :insert_time,
      'actor_id', 'DT_LAG_TEST',
      'source', 'test_suite',
      'object', OBJECT_CONSTRUCT(
        'type', 'LAG_TEST',
        'id', :test_event_id
      ),
      'attributes', OBJECT_CONSTRUCT(
        'insert_time', :insert_time,
        'target_lag_seconds', 60
      )
    ),
    'DT_LAG_TEST',
    :insert_time;
  
  -- Poll for event in ACTIVITY.EVENTS
  WHILE :elapsed < :max_wait_seconds DO
    SELECT MIN(ingested_at) INTO :promotion_time
    FROM CLAUDE_BI.ACTIVITY.EVENTS
    WHERE event_id = :test_event_id;
    
    IF :promotion_time IS NOT NULL THEN
      -- Event found, calculate lag
      SET lag_seconds = DATEDIFF('second', :insert_time, CURRENT_TIMESTAMP());
      
      IF :lag_seconds <= 90 THEN -- 60 second target + 30 second buffer
        RETURN OBJECT_CONSTRUCT(
          'test_name', 'LAG_MONITORING',
          'status', 'PASS',
          'lag_seconds', :lag_seconds,
          'target_lag', 60,
          'message', 'Event promoted within target lag'
        );
      ELSE
        RETURN OBJECT_CONSTRUCT(
          'test_name', 'LAG_MONITORING',
          'status', 'WARN',
          'lag_seconds', :lag_seconds,
          'target_lag', 60,
          'message', 'Event promoted but exceeded target lag'
        );
      END IF;
    END IF;
    
    CALL SYSTEM$WAIT(:check_interval);
    SET elapsed = :elapsed + :check_interval;
  END WHILE;
  
  RETURN OBJECT_CONSTRUCT(
    'test_name', 'LAG_MONITORING',
    'status', 'FAIL',
    'elapsed_seconds', :elapsed,
    'message', 'Event not promoted within timeout period'
  );
END;
$$;

-- ============================================================================
-- Test 3: Deduplication Test
-- ============================================================================
CREATE OR REPLACE PROCEDURE TEST_DT_DEDUP()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
COMMENT = 'Test that duplicate events are properly deduplicated'
AS
$$
DECLARE
  test_event_id STRING;
  duplicate_count INTEGER DEFAULT 5;
  i INTEGER;
  raw_count INTEGER;
  events_count INTEGER;
BEGIN
  -- Generate unique test event ID
  SET test_event_id = 'DEDUP_TEST_' || UUID_STRING();
  
  -- Insert the same event multiple times
  FOR i IN 1 TO duplicate_count DO
    INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS (PAYLOAD, _SOURCE_LANE, _RECV_AT, DEDUPE_KEY)
    SELECT 
      OBJECT_CONSTRUCT(
        'event_id', :test_event_id,
        'action', 'dt.test.dedup',
        'occurred_at', CURRENT_TIMESTAMP(),
        'actor_id', 'DT_DEDUP_TEST',
        'source', 'test_suite',
        'object', OBJECT_CONSTRUCT(
          'type', 'DEDUP_TEST',
          'id', :test_event_id
        ),
        'attributes', OBJECT_CONSTRUCT(
          'insertion_number', :i,
          'total_duplicates', :duplicate_count
        )
      ),
      'DT_DEDUP_TEST',
      DATEADD('second', :i, CURRENT_TIMESTAMP()),
      :test_event_id; -- Same dedupe key for all
  END FOR;
  
  -- Wait for Dynamic Table refresh
  CALL SYSTEM$WAIT(90);
  ALTER DYNAMIC TABLE CLAUDE_BI.ACTIVITY.EVENTS REFRESH;
  
  -- Count occurrences
  SELECT COUNT(*) INTO :raw_count 
  FROM CLAUDE_BI.LANDING.RAW_EVENTS 
  WHERE DEDUPE_KEY = :test_event_id;
  
  SELECT COUNT(*) INTO :events_count 
  FROM CLAUDE_BI.ACTIVITY.EVENTS 
  WHERE event_id = :test_event_id;
  
  -- Verify deduplication
  IF :events_count = 1 AND :raw_count = :duplicate_count THEN
    RETURN OBJECT_CONSTRUCT(
      'test_name', 'DEDUPLICATION',
      'status', 'PASS',
      'duplicates_inserted', :duplicate_count,
      'raw_count', :raw_count,
      'deduplicated_count', :events_count,
      'message', 'Deduplication working correctly'
    );
  ELSE
    RETURN OBJECT_CONSTRUCT(
      'test_name', 'DEDUPLICATION',
      'status', 'FAIL',
      'duplicates_inserted', :duplicate_count,
      'raw_count', :raw_count,
      'deduplicated_count', :events_count,
      'message', 'Deduplication not working as expected'
    );
  END IF;
END;
$$;

-- ============================================================================
-- Test 4: Data Integrity Test
-- ============================================================================
CREATE OR REPLACE PROCEDURE TEST_DT_INTEGRITY()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
COMMENT = 'Test data integrity and NULL handling'
AS
$$
DECLARE
  test_prefix STRING DEFAULT 'INTEGRITY_TEST_';
  valid_events INTEGER DEFAULT 0;
  invalid_events INTEGER DEFAULT 0;
  promoted_count INTEGER;
BEGIN
  -- Insert valid event
  INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS (PAYLOAD, _SOURCE_LANE, _RECV_AT)
  SELECT 
    OBJECT_CONSTRUCT(
      'event_id', :test_prefix || 'VALID',
      'action', 'dt.test.valid',
      'occurred_at', CURRENT_TIMESTAMP(),
      'actor_id', 'DT_INTEGRITY_TEST',
      'source', 'test_suite'
    ),
    'DT_INTEGRITY',
    CURRENT_TIMESTAMP();
  SET valid_events = :valid_events + 1;
  
  -- Insert event with NULL action (should be filtered)
  INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS (PAYLOAD, _SOURCE_LANE, _RECV_AT)
  SELECT 
    OBJECT_CONSTRUCT(
      'event_id', :test_prefix || 'NULL_ACTION',
      'occurred_at', CURRENT_TIMESTAMP(),
      'actor_id', 'DT_INTEGRITY_TEST'
    ),
    'DT_INTEGRITY',
    CURRENT_TIMESTAMP();
  SET invalid_events = :invalid_events + 1;
  
  -- Insert event with NULL occurred_at (should be filtered)
  INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS (PAYLOAD, _SOURCE_LANE, _RECV_AT)
  SELECT 
    OBJECT_CONSTRUCT(
      'event_id', :test_prefix || 'NULL_TIME',
      'action', 'dt.test.null_time',
      'actor_id', 'DT_INTEGRITY_TEST'
    ),
    'DT_INTEGRITY',
    CURRENT_TIMESTAMP();
  SET invalid_events = :invalid_events + 1;
  
  -- Insert event with NULL actor_id (should be filtered)
  INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS (PAYLOAD, _SOURCE_LANE, _RECV_AT)
  SELECT 
    OBJECT_CONSTRUCT(
      'event_id', :test_prefix || 'NULL_ACTOR',
      'action', 'dt.test.null_actor',
      'occurred_at', CURRENT_TIMESTAMP()
    ),
    'DT_INTEGRITY',
    CURRENT_TIMESTAMP();
  SET invalid_events = :invalid_events + 1;
  
  -- Wait for refresh
  CALL SYSTEM$WAIT(90);
  ALTER DYNAMIC TABLE CLAUDE_BI.ACTIVITY.EVENTS REFRESH;
  
  -- Count promoted events
  SELECT COUNT(*) INTO :promoted_count
  FROM CLAUDE_BI.ACTIVITY.EVENTS
  WHERE event_id LIKE :test_prefix || '%';
  
  -- Verify only valid events were promoted
  IF :promoted_count = :valid_events THEN
    RETURN OBJECT_CONSTRUCT(
      'test_name', 'DATA_INTEGRITY',
      'status', 'PASS',
      'valid_events', :valid_events,
      'invalid_events', :invalid_events,
      'promoted_count', :promoted_count,
      'message', 'Invalid events correctly filtered'
    );
  ELSE
    RETURN OBJECT_CONSTRUCT(
      'test_name', 'DATA_INTEGRITY',
      'status', 'FAIL',
      'valid_events', :valid_events,
      'invalid_events', :invalid_events,
      'promoted_count', :promoted_count,
      'message', 'Invalid events not properly filtered'
    );
  END IF;
END;
$$;

-- ============================================================================
-- Test 5: Performance and Scale Test
-- ============================================================================
CREATE OR REPLACE PROCEDURE TEST_DT_PERFORMANCE(batch_size INTEGER DEFAULT 1000)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
COMMENT = 'Test Dynamic Table performance with bulk inserts'
AS
$$
DECLARE
  start_time TIMESTAMP_TZ;
  insert_end_time TIMESTAMP_TZ;
  refresh_end_time TIMESTAMP_TZ;
  test_run_id STRING;
  initial_count INTEGER;
  final_count INTEGER;
  insert_duration_ms INTEGER;
  refresh_duration_ms INTEGER;
BEGIN
  SET test_run_id = 'PERF_TEST_' || UUID_STRING();
  SET start_time = CURRENT_TIMESTAMP();
  
  -- Get initial count
  SELECT COUNT(*) INTO :initial_count FROM CLAUDE_BI.ACTIVITY.EVENTS;
  
  -- Bulk insert events
  INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS (PAYLOAD, _SOURCE_LANE, _RECV_AT)
  SELECT 
    OBJECT_CONSTRUCT(
      'event_id', :test_run_id || '_' || SEQ4(),
      'action', 'dt.test.performance',
      'occurred_at', DATEADD('second', SEQ4(), CURRENT_TIMESTAMP()),
      'actor_id', 'PERF_TEST_' || MOD(SEQ4(), 10),
      'source', 'performance_test',
      'object', OBJECT_CONSTRUCT(
        'type', 'PERF_TEST',
        'id', SEQ4()
      ),
      'attributes', OBJECT_CONSTRUCT(
        'test_run', :test_run_id,
        'sequence', SEQ4(),
        'batch_size', :batch_size
      )
    ),
    'DT_PERFORMANCE',
    CURRENT_TIMESTAMP()
  FROM TABLE(GENERATOR(ROWCOUNT => :batch_size));
  
  SET insert_end_time = CURRENT_TIMESTAMP();
  SET insert_duration_ms = DATEDIFF('millisecond', :start_time, :insert_end_time);
  
  -- Force refresh and measure time
  ALTER DYNAMIC TABLE CLAUDE_BI.ACTIVITY.EVENTS REFRESH;
  
  SET refresh_end_time = CURRENT_TIMESTAMP();
  SET refresh_duration_ms = DATEDIFF('millisecond', :insert_end_time, :refresh_end_time);
  
  -- Get final count
  SELECT COUNT(*) INTO :final_count FROM CLAUDE_BI.ACTIVITY.EVENTS;
  
  -- Calculate metrics
  RETURN OBJECT_CONSTRUCT(
    'test_name', 'PERFORMANCE',
    'status', IFF(:final_count - :initial_count >= :batch_size * 0.95, 'PASS', 'FAIL'),
    'batch_size', :batch_size,
    'events_promoted', :final_count - :initial_count,
    'insert_duration_ms', :insert_duration_ms,
    'refresh_duration_ms', :refresh_duration_ms,
    'events_per_second', ROUND(:batch_size / (:insert_duration_ms / 1000.0), 2),
    'message', 'Performance test completed'
  );
END;
$$;

-- ============================================================================
-- Test 6: 30-Day Retention Window
-- ============================================================================
CREATE OR REPLACE PROCEDURE TEST_DT_RETENTION()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
COMMENT = 'Test that 30-day retention window is enforced'
AS
$$
DECLARE
  old_event_id STRING DEFAULT 'OLD_EVENT_' || UUID_STRING();
  recent_event_id STRING DEFAULT 'RECENT_EVENT_' || UUID_STRING();
  old_event_found BOOLEAN;
  recent_event_found BOOLEAN;
BEGIN
  -- Insert old event (35 days ago)
  INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS (PAYLOAD, _SOURCE_LANE, _RECV_AT)
  SELECT 
    OBJECT_CONSTRUCT(
      'event_id', :old_event_id,
      'action', 'dt.test.old_event',
      'occurred_at', DATEADD('day', -35, CURRENT_TIMESTAMP()),
      'actor_id', 'RETENTION_TEST',
      'source', 'test_suite'
    ),
    'DT_RETENTION',
    DATEADD('day', -35, CURRENT_TIMESTAMP());
  
  -- Insert recent event
  INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS (PAYLOAD, _SOURCE_LANE, _RECV_AT)
  SELECT 
    OBJECT_CONSTRUCT(
      'event_id', :recent_event_id,
      'action', 'dt.test.recent_event',
      'occurred_at', CURRENT_TIMESTAMP(),
      'actor_id', 'RETENTION_TEST',
      'source', 'test_suite'
    ),
    'DT_RETENTION',
    CURRENT_TIMESTAMP();
  
  -- Refresh
  ALTER DYNAMIC TABLE CLAUDE_BI.ACTIVITY.EVENTS REFRESH;
  CALL SYSTEM$WAIT(30);
  
  -- Check if events were promoted
  SELECT COUNT(*) > 0 INTO :old_event_found
  FROM CLAUDE_BI.ACTIVITY.EVENTS
  WHERE event_id = :old_event_id;
  
  SELECT COUNT(*) > 0 INTO :recent_event_found
  FROM CLAUDE_BI.ACTIVITY.EVENTS
  WHERE event_id = :recent_event_id;
  
  -- Old event should be filtered, recent should be included
  IF NOT :old_event_found AND :recent_event_found THEN
    RETURN OBJECT_CONSTRUCT(
      'test_name', 'RETENTION_WINDOW',
      'status', 'PASS',
      'old_event_filtered', NOT :old_event_found,
      'recent_event_included', :recent_event_found,
      'message', '30-day retention window working correctly'
    );
  ELSE
    RETURN OBJECT_CONSTRUCT(
      'test_name', 'RETENTION_WINDOW',
      'status', 'FAIL',
      'old_event_filtered', NOT :old_event_found,
      'recent_event_included', :recent_event_found,
      'message', 'Retention window not working as expected'
    );
  END IF;
END;
$$;

-- ============================================================================
-- Master Test Suite Runner
-- ============================================================================
CREATE OR REPLACE PROCEDURE RUN_DT_TEST_SUITE()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
COMMENT = 'Run all Dynamic Table tests and return results'
AS
$$
DECLARE
  test_results ARRAY DEFAULT ARRAY_CONSTRUCT();
  test_result VARIANT;
  overall_status STRING DEFAULT 'PASS';
BEGIN
  -- Run each test
  CALL TEST_DT_ROW_COUNTS() INTO :test_result;
  LET test_results := ARRAY_APPEND(:test_results, :test_result);
  IF :test_result:status != 'PASS' THEN
    SET overall_status = 'FAIL';
  END IF;
  
  CALL TEST_DT_LAG() INTO :test_result;
  LET test_results := ARRAY_APPEND(:test_results, :test_result);
  IF :test_result:status = 'FAIL' THEN
    SET overall_status = 'FAIL';
  END IF;
  
  CALL TEST_DT_DEDUP() INTO :test_result;
  LET test_results := ARRAY_APPEND(:test_results, :test_result);
  IF :test_result:status != 'PASS' THEN
    SET overall_status = 'FAIL';
  END IF;
  
  CALL TEST_DT_INTEGRITY() INTO :test_result;
  LET test_results := ARRAY_APPEND(:test_results, :test_result);
  IF :test_result:status != 'PASS' THEN
    SET overall_status = 'FAIL';
  END IF;
  
  CALL TEST_DT_PERFORMANCE(100) INTO :test_result; -- Smaller batch for testing
  LET test_results := ARRAY_APPEND(:test_results, :test_result);
  IF :test_result:status != 'PASS' THEN
    SET overall_status = 'FAIL';
  END IF;
  
  CALL TEST_DT_RETENTION() INTO :test_result;
  LET test_results := ARRAY_APPEND(:test_results, :test_result);
  IF :test_result:status != 'PASS' THEN
    SET overall_status = 'FAIL';
  END IF;
  
  -- Log test results as event
  INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS (PAYLOAD, _SOURCE_LANE, _RECV_AT)
  SELECT 
    OBJECT_CONSTRUCT(
      'event_id', UUID_STRING(),
      'action', 'dt.test.suite_complete',
      'occurred_at', CURRENT_TIMESTAMP(),
      'actor_id', 'DT_TEST_SUITE',
      'source', 'test_suite',
      'object', OBJECT_CONSTRUCT(
        'type', 'TEST_SUITE',
        'id', 'DYNAMIC_TABLE_TESTS'
      ),
      'attributes', OBJECT_CONSTRUCT(
        'overall_status', :overall_status,
        'test_results', :test_results,
        'test_count', ARRAY_SIZE(:test_results),
        'run_time', CURRENT_TIMESTAMP()
      )
    ),
    'DT_TEST_SUITE',
    CURRENT_TIMESTAMP();
  
  RETURN OBJECT_CONSTRUCT(
    'suite_name', 'DYNAMIC_TABLE_TEST_SUITE',
    'overall_status', :overall_status,
    'tests_run', ARRAY_SIZE(:test_results),
    'test_results', :test_results,
    'run_time', CURRENT_TIMESTAMP()
  );
END;
$$;

-- Grant permissions
GRANT USAGE ON PROCEDURE TEST_DT_ROW_COUNTS() TO ROLE MCP_USER_ROLE;
GRANT USAGE ON PROCEDURE TEST_DT_LAG() TO ROLE MCP_USER_ROLE;
GRANT USAGE ON PROCEDURE TEST_DT_DEDUP() TO ROLE MCP_USER_ROLE;
GRANT USAGE ON PROCEDURE TEST_DT_INTEGRITY() TO ROLE MCP_USER_ROLE;
GRANT USAGE ON PROCEDURE TEST_DT_PERFORMANCE(INTEGER) TO ROLE MCP_USER_ROLE;
GRANT USAGE ON PROCEDURE TEST_DT_RETENTION() TO ROLE MCP_USER_ROLE;
GRANT USAGE ON PROCEDURE RUN_DT_TEST_SUITE() TO ROLE MCP_USER_ROLE;