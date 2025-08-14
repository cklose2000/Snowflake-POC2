-- ============================================================================
-- Edge Case Test Suite for Activity Schema 2.0
-- Tests boundary conditions, failure modes, and recovery scenarios
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE CLAUDE_BI;

-- ============================================================================
-- TEST HARNESS SETUP
-- ============================================================================

CREATE OR REPLACE PROCEDURE CLAUDE_BI.MCP.RUN_EDGE_CASE_TESTS()
RETURNS TABLE(test_name STRING, status STRING, details VARIANT)
LANGUAGE SQL
AS
$$
DECLARE
  test_results ARRAY DEFAULT ARRAY_CONSTRUCT();
  test_result VARIANT;
BEGIN
  -- Test 1: Oversized payload handling
  BEGIN
    CALL CLAUDE_BI.MCP.SAFE_INSERT_EVENT(
      PARSE_JSON(REPEAT('{"data":"' || REPEAT('x', 10000) || '"}', 200)),  -- >1MB payload
      'TEST'
    );
    
    -- Check if dead letter event was created
    SELECT COUNT(*) INTO test_result
    FROM CLAUDE_BI.ACTIVITY.EVENTS
    WHERE action = 'quality.payload.oversized'
      AND occurred_at >= DATEADD('minute', -1, CURRENT_TIMESTAMP());
    
    test_results := ARRAY_APPEND(test_results, 
      OBJECT_CONSTRUCT(
        'test_name', 'Oversized Payload Rejection',
        'status', IFF(test_result > 0, 'PASSED', 'FAILED'),
        'details', OBJECT_CONSTRUCT('dead_letter_events', test_result)
      )
    );
  EXCEPTION
    WHEN OTHER THEN
      test_results := ARRAY_APPEND(test_results,
        OBJECT_CONSTRUCT(
          'test_name', 'Oversized Payload Rejection',
          'status', 'ERROR',
          'details', OBJECT_CONSTRUCT('error', SQLERRM)
        )
      );
  END;
  
  -- Test 2: Malformed JSON handling
  BEGIN
    INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS VALUES (
      'not valid json{]',  -- Malformed JSON
      'TEST',
      CURRENT_TIMESTAMP()
    );
    
    -- Dynamic Table should skip this
    CALL SYSTEM$WAIT(65);  -- Wait for DT refresh
    
    SELECT COUNT(*) INTO test_result
    FROM CLAUDE_BI.ACTIVITY.EVENTS
    WHERE _source_lane = 'TEST'
      AND _recv_at >= DATEADD('minute', -2, CURRENT_TIMESTAMP());
    
    test_results := ARRAY_APPEND(test_results,
      OBJECT_CONSTRUCT(
        'test_name', 'Malformed JSON Filtering',
        'status', IFF(test_result = 0, 'PASSED', 'FAILED'),
        'details', OBJECT_CONSTRUCT('malformed_events_in_dt', test_result)
      )
    );
  EXCEPTION
    WHEN OTHER THEN
      test_results := ARRAY_APPEND(test_results,
        OBJECT_CONSTRUCT(
          'test_name', 'Malformed JSON Filtering',
          'status', 'ERROR',
          'details', OBJECT_CONSTRUCT('error', SQLERRM)
        )
      );
  END;
  
  -- Test 3: Duplicate event deduplication
  BEGIN
    LET duplicate_payload := OBJECT_CONSTRUCT(
      'action', 'test.duplicate',
      'actor_id', 'test_user',
      'occurred_at', '2024-01-01T12:00:00Z',
      'source', 'test',
      'object', OBJECT_CONSTRUCT('type', 'test', 'id', 'dup_001')
    );
    
    -- Insert same event 3 times
    CALL CLAUDE_BI.MCP.SAFE_INSERT_EVENT(:duplicate_payload, 'TEST');
    CALL CLAUDE_BI.MCP.SAFE_INSERT_EVENT(:duplicate_payload, 'TEST');
    CALL CLAUDE_BI.MCP.SAFE_INSERT_EVENT(:duplicate_payload, 'TEST');
    
    CALL SYSTEM$WAIT(65);  -- Wait for DT refresh
    
    -- Should only have 1 event after dedup
    SELECT COUNT(*) INTO test_result
    FROM CLAUDE_BI.ACTIVITY.EVENTS
    WHERE action = 'test.duplicate'
      AND actor_id = 'test_user';
    
    test_results := ARRAY_APPEND(test_results,
      OBJECT_CONSTRUCT(
        'test_name', 'Duplicate Event Deduplication',
        'status', IFF(test_result = 1, 'PASSED', 'FAILED'),
        'details', OBJECT_CONSTRUCT('duplicate_count', test_result)
      )
    );
  EXCEPTION
    WHEN OTHER THEN
      test_results := ARRAY_APPEND(test_results,
        OBJECT_CONSTRUCT(
          'test_name', 'Duplicate Event Deduplication',
          'status', 'ERROR',
          'details', OBJECT_CONSTRUCT('error', SQLERRM)
        )
      );
  END;
  
  -- Test 4: Namespace enforcement
  BEGIN
    -- Try to insert system event from non-system source
    INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS VALUES (
      OBJECT_CONSTRUCT(
        'action', 'system.hack.attempt',
        'actor_id', 'hacker',
        'occurred_at', CURRENT_TIMESTAMP(),
        'source', 'malicious'  -- Wrong source for system.*
      ),
      'TEST',
      CURRENT_TIMESTAMP()
    );
    
    CALL SYSTEM$WAIT(65);  -- Wait for DT refresh
    
    -- Should be filtered out
    SELECT COUNT(*) INTO test_result
    FROM CLAUDE_BI.ACTIVITY.EVENTS
    WHERE action = 'system.hack.attempt';
    
    test_results := ARRAY_APPEND(test_results,
      OBJECT_CONSTRUCT(
        'test_name', 'Namespace Enforcement',
        'status', IFF(test_result = 0, 'PASSED', 'FAILED'),
        'details', OBJECT_CONSTRUCT('unauthorized_events', test_result)
      )
    );
  EXCEPTION
    WHEN OTHER THEN
      test_results := ARRAY_APPEND(test_results,
        OBJECT_CONSTRUCT(
          'test_name', 'Namespace Enforcement',
          'status', 'ERROR',
          'details', OBJECT_CONSTRUCT('error', SQLERRM)
        )
      );
  END;
  
  -- Test 5: Missing required fields
  BEGIN
    -- Insert event with no action
    INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS VALUES (
      OBJECT_CONSTRUCT(
        'actor_id', 'test_user',
        'occurred_at', CURRENT_TIMESTAMP()
        -- Missing 'action' field
      ),
      'TEST',
      CURRENT_TIMESTAMP()
    );
    
    CALL SYSTEM$WAIT(65);  -- Wait for DT refresh
    
    -- Should be filtered out
    SELECT COUNT(*) INTO test_result
    FROM CLAUDE_BI.ACTIVITY.EVENTS
    WHERE actor_id = 'test_user'
      AND action IS NULL
      AND _recv_at >= DATEADD('minute', -2, CURRENT_TIMESTAMP());
    
    test_results := ARRAY_APPEND(test_results,
      OBJECT_CONSTRUCT(
        'test_name', 'Missing Required Fields',
        'status', IFF(test_result = 0, 'PASSED', 'FAILED'),
        'details', OBJECT_CONSTRUCT('events_without_action', test_result)
      )
    );
  EXCEPTION
    WHEN OTHER THEN
      test_results := ARRAY_APPEND(test_results,
        OBJECT_CONSTRUCT(
          'test_name', 'Missing Required Fields',
          'status', 'ERROR',
          'details', OBJECT_CONSTRUCT('error', SQLERRM)
        )
      );
  END;
  
  -- Test 6: Permission precedence
  BEGIN
    -- Grant permission
    CALL CLAUDE_BI.MCP.SAFE_INSERT_EVENT(
      OBJECT_CONSTRUCT(
        'action', 'system.permission.granted',
        'actor_id', 'admin',
        'occurred_at', DATEADD('hour', -2, CURRENT_TIMESTAMP()),
        'source', 'system',
        'object', OBJECT_CONSTRUCT('type', 'user', 'id', 'test_user_perm'),
        'attributes', OBJECT_CONSTRUCT(
          'allowed_actions', ARRAY_CONSTRUCT('read', 'write'),
          'max_rows', 1000
        )
      ),
      'SYSTEM'
    );
    
    -- Then deny permission (should override)
    CALL CLAUDE_BI.MCP.SAFE_INSERT_EVENT(
      OBJECT_CONSTRUCT(
        'action', 'system.permission.denied',
        'actor_id', 'admin',
        'occurred_at', DATEADD('hour', -1, CURRENT_TIMESTAMP()),
        'source', 'system',
        'object', OBJECT_CONSTRUCT('type', 'user', 'id', 'test_user_perm'),
        'attributes', OBJECT_CONSTRUCT(
          'reason', 'Security violation'
        )
      ),
      'SYSTEM'
    );
    
    CALL SYSTEM$WAIT(65);  -- Wait for DT refresh
    
    -- Check current permission state
    SELECT is_active INTO test_result
    FROM CLAUDE_BI.MCP.CURRENT_PERMISSIONS
    WHERE user_id = 'test_user_perm';
    
    test_results := ARRAY_APPEND(test_results,
      OBJECT_CONSTRUCT(
        'test_name', 'Permission Precedence (DENY > GRANT)',
        'status', IFF(test_result = FALSE, 'PASSED', 'FAILED'),
        'details', OBJECT_CONSTRUCT('is_active', test_result)
      )
    );
  EXCEPTION
    WHEN OTHER THEN
      test_results := ARRAY_APPEND(test_results,
        OBJECT_CONSTRUCT(
          'test_name', 'Permission Precedence',
          'status', 'ERROR',
          'details', OBJECT_CONSTRUCT('error', SQLERRM)
        )
      );
  END;
  
  -- Test 7: Timestamp handling
  BEGIN
    -- Event with no occurred_at (should use _recv_at)
    INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS VALUES (
      OBJECT_CONSTRUCT(
        'action', 'test.no_timestamp',
        'actor_id', 'test_user',
        'source', 'test'
      ),
      'TEST',
      '2024-01-15 10:30:45.123456'::TIMESTAMP_TZ
    );
    
    CALL SYSTEM$WAIT(65);  -- Wait for DT refresh
    
    -- Check if occurred_at was set to _recv_at
    SELECT occurred_at = '2024-01-15 10:30:45.123456'::TIMESTAMP_TZ INTO test_result
    FROM CLAUDE_BI.ACTIVITY.EVENTS
    WHERE action = 'test.no_timestamp';
    
    test_results := ARRAY_APPEND(test_results,
      OBJECT_CONSTRUCT(
        'test_name', 'Missing Timestamp Fallback',
        'status', IFF(test_result = TRUE, 'PASSED', 'FAILED'),
        'details', OBJECT_CONSTRUCT('timestamp_matched', test_result)
      )
    );
  EXCEPTION
    WHEN OTHER THEN
      test_results := ARRAY_APPEND(test_results,
        OBJECT_CONSTRUCT(
          'test_name', 'Missing Timestamp Fallback',
          'status', 'ERROR',
          'details', OBJECT_CONSTRUCT('error', SQLERRM)
        )
      );
  END;
  
  -- Test 8: Event dependency checking
  BEGIN
    -- Insert dependent event without parent
    CALL CLAUDE_BI.MCP.SAFE_INSERT_EVENT(
      OBJECT_CONSTRUCT(
        'event_id', 'child_001',
        'action', 'test.child',
        'actor_id', 'test_user',
        'occurred_at', CURRENT_TIMESTAMP(),
        'source', 'test',
        'depends_on_event_id', 'parent_001'  -- Parent doesn't exist
      ),
      'TEST'
    );
    
    CALL SYSTEM$WAIT(65);  -- Wait for DT refresh
    
    -- Should not be in EVENTS (dependency not satisfied)
    SELECT COUNT(*) INTO test_result
    FROM CLAUDE_BI.ACTIVITY.EVENTS
    WHERE event_id = 'child_001';
    
    test_results := ARRAY_APPEND(test_results,
      OBJECT_CONSTRUCT(
        'test_name', 'Event Dependency Enforcement',
        'status', IFF(test_result = 0, 'PASSED', 'FAILED'),
        'details', OBJECT_CONSTRUCT('orphaned_events', test_result)
      )
    );
    
    -- Now insert parent
    CALL CLAUDE_BI.MCP.SAFE_INSERT_EVENT(
      OBJECT_CONSTRUCT(
        'event_id', 'parent_001',
        'action', 'test.parent',
        'actor_id', 'test_user',
        'occurred_at', DATEADD('hour', -1, CURRENT_TIMESTAMP()),
        'source', 'test'
      ),
      'TEST'
    );
    
    CALL SYSTEM$WAIT(65);  -- Wait for DT refresh
    
    -- Child should now appear
    SELECT COUNT(*) INTO test_result
    FROM CLAUDE_BI.ACTIVITY.EVENTS
    WHERE event_id = 'child_001';
    
    test_results := ARRAY_APPEND(test_results,
      OBJECT_CONSTRUCT(
        'test_name', 'Event Dependency Resolution',
        'status', IFF(test_result = 1, 'PASSED', 'FAILED'),
        'details', OBJECT_CONSTRUCT('resolved_events', test_result)
      )
    );
  EXCEPTION
    WHEN OTHER THEN
      test_results := ARRAY_APPEND(test_results,
        OBJECT_CONSTRUCT(
          'test_name', 'Event Dependencies',
          'status', 'ERROR',
          'details', OBJECT_CONSTRUCT('error', SQLERRM)
        )
      );
  END;
  
  -- Test 9: Concurrent inserts
  BEGIN
    -- Simulate concurrent inserts with same timestamp
    LET base_time := CURRENT_TIMESTAMP();
    
    FOR i IN 1 TO 10 DO
      INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS VALUES (
        OBJECT_CONSTRUCT(
          'action', 'test.concurrent',
          'actor_id', 'user_' || i::STRING,
          'occurred_at', base_time,  -- Same timestamp
          'source', 'test',
          'sequence_within_ms', i  -- Different sequences
        ),
        'TEST',
        CURRENT_TIMESTAMP()
      );
    END FOR;
    
    CALL SYSTEM$WAIT(65);  -- Wait for DT refresh
    
    -- All 10 should be present with correct ordering
    SELECT COUNT(*) INTO test_result
    FROM CLAUDE_BI.ACTIVITY.EVENTS
    WHERE action = 'test.concurrent'
      AND occurred_at = base_time;
    
    test_results := ARRAY_APPEND(test_results,
      OBJECT_CONSTRUCT(
        'test_name', 'Concurrent Insert Handling',
        'status', IFF(test_result = 10, 'PASSED', 'FAILED'),
        'details', OBJECT_CONSTRUCT('concurrent_events', test_result)
      )
    );
  EXCEPTION
    WHEN OTHER THEN
      test_results := ARRAY_APPEND(test_results,
        OBJECT_CONSTRUCT(
          'test_name', 'Concurrent Inserts',
          'status', 'ERROR',
          'details', OBJECT_CONSTRUCT('error', SQLERRM)
        )
      );
  END;
  
  -- Test 10: Backfill procedure
  BEGIN
    -- Create temp backup table
    CREATE OR REPLACE TEMPORARY TABLE test_backup AS
    SELECT * FROM CLAUDE_BI.ACTIVITY.EVENTS 
    WHERE action LIKE 'test.%'
    LIMIT 5;
    
    -- Run backfill
    CALL CLAUDE_BI.MCP.BACKFILL_FROM_BACKUP('test_backup', NULL, NULL);
    
    CALL SYSTEM$WAIT(65);  -- Wait for DT refresh
    
    -- Check for _RESTORE events
    SELECT COUNT(*) INTO test_result
    FROM CLAUDE_BI.ACTIVITY.EVENTS
    WHERE _source_lane = '_RESTORE'
      AND _recv_at >= DATEADD('minute', -2, CURRENT_TIMESTAMP());
    
    test_results := ARRAY_APPEND(test_results,
      OBJECT_CONSTRUCT(
        'test_name', 'Backfill Procedure',
        'status', IFF(test_result >= 5, 'PASSED', 'FAILED'),
        'details', OBJECT_CONSTRUCT('restored_events', test_result)
      )
    );
    
    DROP TABLE test_backup;
  EXCEPTION
    WHEN OTHER THEN
      test_results := ARRAY_APPEND(test_results,
        OBJECT_CONSTRUCT(
          'test_name', 'Backfill Procedure',
          'status', 'ERROR',
          'details', OBJECT_CONSTRUCT('error', SQLERRM)
        )
      );
  END;
  
  -- Return all test results
  RETURN TABLE(
    SELECT 
      VALUE:test_name::STRING as test_name,
      VALUE:status::STRING as status,
      VALUE:details as details
    FROM TABLE(FLATTEN(INPUT => test_results))
  );
END;
$$;

-- ============================================================================
-- STRESS TEST PROCEDURES
-- ============================================================================

CREATE OR REPLACE PROCEDURE CLAUDE_BI.MCP.STRESS_TEST_INSERTS(
  num_events INTEGER DEFAULT 1000,
  batch_size INTEGER DEFAULT 100
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
  start_time TIMESTAMP_TZ;
  end_time TIMESTAMP_TZ;
  success_count INTEGER DEFAULT 0;
  error_count INTEGER DEFAULT 0;
  result STRING;
BEGIN
  start_time := CURRENT_TIMESTAMP();
  
  FOR batch IN 0 TO CEIL(num_events / batch_size) - 1 DO
    FOR i IN 1 TO batch_size DO
      BEGIN
        CALL CLAUDE_BI.MCP.SAFE_INSERT_EVENT(
          OBJECT_CONSTRUCT(
            'action', 'stress.test.event',
            'actor_id', 'stress_user_' || ((batch * batch_size) + i)::STRING,
            'occurred_at', DATEADD('second', -i, CURRENT_TIMESTAMP()),
            'source', 'stress_test',
            'attributes', OBJECT_CONSTRUCT(
              'batch', batch,
              'index', i,
              'random_data', SHA2(RANDOM()::STRING, 256)
            )
          ),
          'STRESS_TEST'
        );
        success_count := success_count + 1;
      EXCEPTION
        WHEN OTHER THEN
          error_count := error_count + 1;
      END;
    END FOR;
    
    -- Small delay between batches
    CALL SYSTEM$WAIT(0.1);
  END FOR;
  
  end_time := CURRENT_TIMESTAMP();
  
  RETURN OBJECT_CONSTRUCT(
    'duration_seconds', DATEDIFF('second', start_time, end_time),
    'events_attempted', num_events,
    'events_succeeded', success_count,
    'events_failed', error_count,
    'events_per_second', success_count / GREATEST(DATEDIFF('second', start_time, end_time), 1)
  )::STRING;
END;
$$;

-- ============================================================================
-- VALIDATION QUERIES
-- ============================================================================

-- Check two-table compliance
CREATE OR REPLACE VIEW CLAUDE_BI.MCP.TABLE_COMPLIANCE_CHECK AS
SELECT 
  COUNT(*) as table_count,
  ARRAY_AGG(TABLE_SCHEMA || '.' || TABLE_NAME) as tables,
  CASE
    WHEN COUNT(*) = 2 AND 
         ARRAY_CONTAINS('LANDING.RAW_EVENTS'::VARIANT, tables) AND
         ARRAY_CONTAINS('ACTIVITY.EVENTS'::VARIANT, tables)
    THEN 'COMPLIANT'
    ELSE 'VIOLATION - ONLY 2 TABLES ALLOWED!'
  END as compliance_status
FROM CLAUDE_BI.INFORMATION_SCHEMA.TABLES
WHERE TABLE_CATALOG = 'CLAUDE_BI'
  AND TABLE_TYPE IN ('BASE TABLE', 'DYNAMIC TABLE')
  AND TABLE_SCHEMA IN ('LANDING', 'ACTIVITY');

-- Monitor Dynamic Table health
CREATE OR REPLACE VIEW CLAUDE_BI.MCP.DT_HEALTH_CHECK AS
SELECT 
  'EVENTS' as table_name,
  target_lag,
  refresh_mode,
  warehouse,
  DATEDIFF('seconds', 
    TRY_CAST(
      PARSE_JSON(
        (SELECT details FROM TABLE(
          INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(
            NAME => 'CLAUDE_BI.ACTIVITY.EVENTS'
          )
        ) WHERE phase = 'COMPLETED' 
        ORDER BY phase_end_time DESC 
        LIMIT 1)
      ):last_refresh_time::STRING AS TIMESTAMP_TZ
    ),
    CURRENT_TIMESTAMP()
  ) as seconds_since_refresh,
  CASE
    WHEN seconds_since_refresh > 300 THEN 'CRITICAL'
    WHEN seconds_since_refresh > 120 THEN 'WARNING'
    ELSE 'HEALTHY'
  END as health_status
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLES(
  DATABASE_NAME => 'CLAUDE_BI',
  SCHEMA_NAME => 'ACTIVITY'
))
WHERE name = 'EVENTS';

-- ============================================================================
-- RUN TESTS
-- ============================================================================

-- Execute edge case tests
CALL CLAUDE_BI.MCP.RUN_EDGE_CASE_TESTS();

-- Show results summary
SELECT 
  test_name,
  status,
  CASE 
    WHEN status = 'PASSED' THEN '✅'
    WHEN status = 'FAILED' THEN '❌'
    ELSE '⚠️'
  END as icon,
  details
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
ORDER BY 
  CASE status 
    WHEN 'FAILED' THEN 1
    WHEN 'ERROR' THEN 2
    WHEN 'PASSED' THEN 3
  END;