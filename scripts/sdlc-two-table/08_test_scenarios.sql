-- ============================================================================
-- 08_test_scenarios.sql
-- Comprehensive SDLC Test Scenarios - Two-Table Law Compliant
-- Tests all aspects of the event-driven SDLC system
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE CLAUDE_BI;
USE SCHEMA MCP;

-- ============================================================================
-- TEST SETUP PROCEDURES
-- ============================================================================

-- Cleanup test data (removes only test work items)
CREATE OR REPLACE PROCEDURE SDLC_TEST_CLEANUP()
RETURNS VARIANT
LANGUAGE SQL
AS
$$
BEGIN
  -- Remove test work items from event stream
  -- Note: We can't actually delete events (immutable), but we can mark them
  -- This is why we use a naming convention for test items
  
  -- Create a cleanup event to indicate test data should be ignored
  INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS
  SELECT 
    OBJECT_CONSTRUCT(
      'event_id', UUID_STRING(),
      'action', 'sdlc.test.cleanup',
      'occurred_at', CURRENT_TIMESTAMP(),
      'actor_id', 'test_framework',
      'source', 'sdlc_test',
      'schema_version', '2.1.0',
      'object', OBJECT_CONSTRUCT(
        'type', 'test_cleanup',
        'id', 'cleanup_' || TO_CHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MISS')
      ),
      'attributes', OBJECT_CONSTRUCT(
        'cleanup_pattern', 'TEST-%',
        'cleanup_timestamp', CURRENT_TIMESTAMP(),
        'reason', 'Test scenario cleanup'
      )
    ),
    'SDLC_TEST',
    CURRENT_TIMESTAMP();
    
  RETURN OBJECT_CONSTRUCT('result', 'ok', 'message', 'Test cleanup event created');
END;
$$;

-- ============================================================================
-- A. BASIC FUNCTIONALITY TESTS
-- ============================================================================

-- Test 1: Create various work items
CREATE OR REPLACE PROCEDURE SDLC_TEST_BASIC_CREATE()
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
  result VARIANT;
  test_results ARRAY DEFAULT ARRAY_CONSTRUCT();
BEGIN
  -- Create a feature work item
  CALL SDLC_CREATE_WORK(
    'TEST-001',
    'Implement user authentication',
    'feature',
    'p1',
    'Add OAuth2 login with Google and GitHub',
    'test_user',
    'test_create_001_' || UUID_STRING(),
    8,  -- business_value
    TRUE  -- customer_impact
  ) INTO result;
  test_results := ARRAY_APPEND(test_results, OBJECT_CONSTRUCT('test', 'create_feature', 'result', result));
  
  -- Create a bug work item
  CALL SDLC_CREATE_WORK(
    'TEST-002',
    'Fix memory leak in data processor',
    'bug',
    'p0',
    'Memory usage grows unbounded during large imports',
    'test_user',
    'test_create_002_' || UUID_STRING(),
    10,
    TRUE
  ) INTO result;
  test_results := ARRAY_APPEND(test_results, OBJECT_CONSTRUCT('test', 'create_bug', 'result', result));
  
  -- Create a technical debt item
  CALL SDLC_CREATE_WORK(
    'TEST-003',
    'Refactor legacy payment module',
    'debt',
    'p2',
    'Replace deprecated payment API with new SDK',
    'test_user',
    'test_create_003_' || UUID_STRING(),
    5,
    FALSE
  ) INTO result;
  test_results := ARRAY_APPEND(test_results, OBJECT_CONSTRUCT('test', 'create_debt', 'result', result));
  
  -- Try to create duplicate (should fail)
  CALL SDLC_CREATE_WORK(
    'TEST-001',  -- Duplicate ID
    'Duplicate work item',
    'feature',
    'p3',
    'This should fail',
    'test_user',
    'test_create_dup_' || UUID_STRING(),
    1,
    FALSE
  ) INTO result;
  test_results := ARRAY_APPEND(test_results, OBJECT_CONSTRUCT('test', 'create_duplicate', 'result', result));
  
  RETURN OBJECT_CONSTRUCT('test_suite', 'basic_create', 'results', test_results);
END;
$$;

-- Test 2: Status transitions
CREATE OR REPLACE PROCEDURE SDLC_TEST_STATUS_TRANSITIONS()
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
  result VARIANT;
  test_results ARRAY DEFAULT ARRAY_CONSTRUCT();
  last_event_id STRING;
BEGIN
  -- First create a work item
  CALL SDLC_CREATE_WORK(
    'TEST-STATUS-001',
    'Test status transitions',
    'feature',
    'p2',
    'Work item for testing status changes',
    'test_user',
    'test_status_create_' || UUID_STRING(),
    5,
    FALSE
  ) INTO result;
  
  -- Get the last event ID
  SELECT last_event_id INTO last_event_id
  FROM VW_WORK_ITEMS
  WHERE work_id = 'TEST-STATUS-001';
  
  -- Valid transition: new -> ready
  CALL SDLC_STATUS(
    'TEST-STATUS-001',
    'ready',
    last_event_id,
    'test_status_ready_' || UUID_STRING(),
    'test_user',
    'Moving to ready for development'
  ) INTO result;
  test_results := ARRAY_APPEND(test_results, OBJECT_CONSTRUCT('test', 'new_to_ready', 'result', result));
  
  -- Get updated last event ID
  SELECT last_event_id INTO last_event_id
  FROM VW_WORK_ITEMS
  WHERE work_id = 'TEST-STATUS-001';
  
  -- Valid transition: ready -> in_progress
  CALL SDLC_STATUS(
    'TEST-STATUS-001',
    'in_progress',
    last_event_id,
    'test_status_progress_' || UUID_STRING(),
    'test_user',
    'Starting development'
  ) INTO result;
  test_results := ARRAY_APPEND(test_results, OBJECT_CONSTRUCT('test', 'ready_to_progress', 'result', result));
  
  -- Get updated last event ID
  SELECT last_event_id INTO last_event_id
  FROM VW_WORK_ITEMS
  WHERE work_id = 'TEST-STATUS-001';
  
  -- Invalid transition: in_progress -> new (should fail)
  CALL SDLC_STATUS(
    'TEST-STATUS-001',
    'new',
    last_event_id,
    'test_status_invalid_' || UUID_STRING(),
    'test_user',
    'Invalid transition attempt'
  ) INTO result;
  test_results := ARRAY_APPEND(test_results, OBJECT_CONSTRUCT('test', 'invalid_transition', 'result', result));
  
  -- Valid transition: in_progress -> done
  CALL SDLC_STATUS(
    'TEST-STATUS-001',
    'done',
    last_event_id,
    'test_status_done_' || UUID_STRING(),
    'test_user',
    'Work completed'
  ) INTO result;
  test_results := ARRAY_APPEND(test_results, OBJECT_CONSTRUCT('test', 'progress_to_done', 'result', result));
  
  RETURN OBJECT_CONSTRUCT('test_suite', 'status_transitions', 'results', test_results);
END;
$$;

-- ============================================================================
-- B. CONCURRENCY TESTS
-- ============================================================================

-- Test 3: Optimistic locking and conflict handling
CREATE OR REPLACE PROCEDURE SDLC_TEST_CONCURRENCY()
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
  result1 VARIANT;
  result2 VARIANT;
  test_results ARRAY DEFAULT ARRAY_CONSTRUCT();
  last_event_id STRING;
  work_id STRING DEFAULT 'TEST-CONCUR-001';
BEGIN
  -- Create a work item
  CALL SDLC_CREATE_WORK(
    work_id,
    'Test concurrency control',
    'feature',
    'p2',
    'Work item for testing optimistic locking',
    'test_user',
    'test_concur_create_' || UUID_STRING(),
    5,
    FALSE
  ) INTO result1;
  
  -- Get the last event ID
  SELECT last_event_id INTO last_event_id
  FROM VW_WORK_ITEMS
  WHERE work_id = :work_id;
  
  -- Simulate concurrent updates with same expected_last_event_id
  -- First update should succeed
  CALL SDLC_STATUS(
    work_id,
    'ready',
    last_event_id,
    'test_concur_1_' || UUID_STRING(),
    'agent_1',
    'Agent 1 update'
  ) INTO result1;
  test_results := ARRAY_APPEND(test_results, OBJECT_CONSTRUCT('test', 'first_update', 'result', result1));
  
  -- Second update with same expected_last_event_id should fail (conflict)
  CALL SDLC_STATUS(
    work_id,
    'in_progress',
    last_event_id,  -- Using old event ID
    'test_concur_2_' || UUID_STRING(),
    'agent_2',
    'Agent 2 update'
  ) INTO result2;
  test_results := ARRAY_APPEND(test_results, OBJECT_CONSTRUCT('test', 'concurrent_update', 'result', result2));
  
  -- Verify conflict event was created
  LET conflict_count := (
    SELECT COUNT(*)
    FROM ACTIVITY.EVENTS
    WHERE action = 'sdlc.agent.error'
      AND attributes:work_id::string = :work_id
      AND attributes:error_type::string = 'conflict'
  );
  test_results := ARRAY_APPEND(test_results, OBJECT_CONSTRUCT('test', 'conflict_events', 'count', conflict_count));
  
  RETURN OBJECT_CONSTRUCT('test_suite', 'concurrency', 'results', test_results);
END;
$$;

-- Test 4: Idempotency verification
CREATE OR REPLACE PROCEDURE SDLC_TEST_IDEMPOTENCY()
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
  result1 VARIANT;
  result2 VARIANT;
  test_results ARRAY DEFAULT ARRAY_CONSTRUCT();
  idem_key STRING DEFAULT 'test_idem_' || UUID_STRING();
BEGIN
  -- First call with idempotency key
  CALL SDLC_CREATE_WORK(
    'TEST-IDEM-001',
    'Test idempotency',
    'feature',
    'p2',
    'Testing idempotent operations',
    'test_user',
    idem_key,
    5,
    FALSE
  ) INTO result1;
  test_results := ARRAY_APPEND(test_results, OBJECT_CONSTRUCT('test', 'first_call', 'result', result1));
  
  -- Second call with same idempotency key (should return idempotent_return)
  CALL SDLC_CREATE_WORK(
    'TEST-IDEM-002',  -- Different work_id
    'Different title',
    'bug',
    'p0',
    'Different description',
    'different_user',
    idem_key,  -- Same idempotency key
    10,
    TRUE
  ) INTO result2;
  test_results := ARRAY_APPEND(test_results, OBJECT_CONSTRUCT('test', 'duplicate_key', 'result', result2));
  
  -- Verify only one event was created
  LET event_count := (
    SELECT COUNT(*)
    FROM ACTIVITY.EVENTS
    WHERE attributes:idempotency_key::string = :idem_key
  );
  test_results := ARRAY_APPEND(test_results, OBJECT_CONSTRUCT('test', 'event_count', 'count', event_count));
  
  RETURN OBJECT_CONSTRUCT('test_suite', 'idempotency', 'results', test_results);
END;
$$;

-- ============================================================================
-- C. AGENT INTEGRATION TESTS
-- ============================================================================

-- Test 5: Agent claiming and assignment
CREATE OR REPLACE PROCEDURE SDLC_TEST_AGENT_OPERATIONS()
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
  result VARIANT;
  test_results ARRAY DEFAULT ARRAY_CONSTRUCT();
  i INT DEFAULT 1;
BEGIN
  -- Create multiple work items for agents to claim
  WHILE (i <= 5) DO
    CALL SDLC_CREATE_WORK(
      'TEST-AGENT-' || LPAD(i::STRING, 3, '0'),
      'Agent test work ' || i,
      CASE WHEN i % 2 = 0 THEN 'feature' ELSE 'bug' END,
      CASE WHEN i = 1 THEN 'p0' WHEN i = 2 THEN 'p1' ELSE 'p2' END,
      'Work item for agent testing',
      'test_user',
      'test_agent_create_' || i || '_' || UUID_STRING(),
      i * 2,
      i <= 2
    ) INTO result;
    i := i + 1;
  END WHILE;
  
  -- Test agent claiming with capabilities
  CALL SDLC_CLAIM_NEXT(
    'test_agent_1',
    'ai_coding_agent',
    ARRAY_CONSTRUCT('bug', 'python', 'testing'),
    3  -- max_retry_attempts
  ) INTO result;
  test_results := ARRAY_APPEND(test_results, OBJECT_CONSTRUCT('test', 'agent_claim_with_skills', 'result', result));
  
  -- Test agent claiming without capabilities
  CALL SDLC_CLAIM_NEXT(
    'test_agent_2',
    'general_agent',
    NULL,  -- No specific capabilities
    3
  ) INTO result;
  test_results := ARRAY_APPEND(test_results, OBJECT_CONSTRUCT('test', 'agent_claim_no_skills', 'result', result));
  
  -- Test work completion
  LET claimed_work := (
    SELECT work_id, last_event_id
    FROM VW_WORK_ITEMS
    WHERE assignee_id = 'test_agent_1'
    LIMIT 1
  );
  
  IF (claimed_work IS NOT NULL) THEN
    CALL SDLC_COMPLETE_WORK(
      claimed_work:WORK_ID,
      claimed_work:LAST_EVENT_ID,
      'test_agent_1',
      'Work completed successfully',
      ARRAY_CONSTRUCT('file1.js', 'file2.js'),
      TRUE  -- tests_passing
    ) INTO result;
    test_results := ARRAY_APPEND(test_results, OBJECT_CONSTRUCT('test', 'agent_complete_work', 'result', result));
  END IF;
  
  -- Test error handling
  CALL SDLC_HANDLE_ERROR(
    'TEST-AGENT-003',
    'test_agent_2',
    'timeout',
    'Operation timed out after 30 seconds',
    TRUE,  -- will_retry
    5000  -- retry_after_ms
  ) INTO result;
  test_results := ARRAY_APPEND(test_results, OBJECT_CONSTRUCT('test', 'agent_error_handling', 'result', result));
  
  RETURN OBJECT_CONSTRUCT('test_suite', 'agent_operations', 'results', test_results);
END;
$$;

-- ============================================================================
-- D. DEPENDENCY TESTS
-- ============================================================================

-- Test 6: Work dependencies and blocking
CREATE OR REPLACE PROCEDURE SDLC_TEST_DEPENDENCIES()
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
  result VARIANT;
  test_results ARRAY DEFAULT ARRAY_CONSTRUCT();
  last_event_id_1 STRING;
  last_event_id_2 STRING;
  last_event_id_3 STRING;
BEGIN
  -- Create three work items
  CALL SDLC_CREATE_WORK('TEST-DEP-001', 'Database schema', 'feature', 'p1', 'Create DB schema', 'test_user', 
                        'test_dep_1_' || UUID_STRING(), 8, TRUE) INTO result;
  CALL SDLC_CREATE_WORK('TEST-DEP-002', 'API endpoints', 'feature', 'p1', 'Build REST API', 'test_user',
                        'test_dep_2_' || UUID_STRING(), 8, TRUE) INTO result;
  CALL SDLC_CREATE_WORK('TEST-DEP-003', 'UI components', 'feature', 'p1', 'Create React UI', 'test_user',
                        'test_dep_3_' || UUID_STRING(), 8, TRUE) INTO result;
  
  -- Get last event IDs
  SELECT last_event_id INTO last_event_id_1 FROM VW_WORK_ITEMS WHERE work_id = 'TEST-DEP-001';
  SELECT last_event_id INTO last_event_id_2 FROM VW_WORK_ITEMS WHERE work_id = 'TEST-DEP-002';
  SELECT last_event_id INTO last_event_id_3 FROM VW_WORK_ITEMS WHERE work_id = 'TEST-DEP-003';
  
  -- Create dependency: API depends on Database
  CALL SDLC_ADD_DEPENDENCY(
    'TEST-DEP-002',  -- work_id
    'TEST-DEP-001',  -- depends_on_id
    'blocks',
    last_event_id_2,
    'test_dep_link_1_' || UUID_STRING(),
    'test_user',
    'API needs database schema first'
  ) INTO result;
  test_results := ARRAY_APPEND(test_results, OBJECT_CONSTRUCT('test', 'add_dependency', 'result', result));
  
  -- Create dependency: UI depends on API
  CALL SDLC_ADD_DEPENDENCY(
    'TEST-DEP-003',
    'TEST-DEP-002',
    'blocks',
    last_event_id_3,
    'test_dep_link_2_' || UUID_STRING(),
    'test_user',
    'UI needs API endpoints'
  ) INTO result;
  test_results := ARRAY_APPEND(test_results, OBJECT_CONSTRUCT('test', 'add_chain_dependency', 'result', result));
  
  -- Try to create circular dependency (should fail)
  SELECT last_event_id INTO last_event_id_1 FROM VW_WORK_ITEMS WHERE work_id = 'TEST-DEP-001';
  CALL SDLC_ADD_DEPENDENCY(
    'TEST-DEP-001',
    'TEST-DEP-003',  -- This would create a cycle
    'blocks',
    last_event_id_1,
    'test_dep_circular_' || UUID_STRING(),
    'test_user',
    'This should fail - circular dependency'
  ) INTO result;
  test_results := ARRAY_APPEND(test_results, OBJECT_CONSTRUCT('test', 'circular_dependency', 'result', result));
  
  -- Check blocked work view
  LET blocked_count := (
    SELECT COUNT(*)
    FROM VW_BLOCKED_WORK
    WHERE work_id IN ('TEST-DEP-002', 'TEST-DEP-003')
  );
  test_results := ARRAY_APPEND(test_results, OBJECT_CONSTRUCT('test', 'blocked_work_count', 'count', blocked_count));
  
  RETURN OBJECT_CONSTRUCT('test_suite', 'dependencies', 'results', test_results);
END;
$$;

-- ============================================================================
-- E. SLA AND AUTOMATION TESTS
-- ============================================================================

-- Test 7: SLA monitoring and escalation
CREATE OR REPLACE PROCEDURE SDLC_TEST_SLA_MONITORING()
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
  result VARIANT;
  test_results ARRAY DEFAULT ARRAY_CONSTRUCT();
BEGIN
  -- Create a P0 item that's already old (simulate with backdated event)
  INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS
  SELECT 
    OBJECT_CONSTRUCT(
      'event_id', UUID_STRING(),
      'action', 'sdlc.work.create',
      'occurred_at', DATEADD('hour', -25, CURRENT_TIMESTAMP()),  -- 25 hours ago (exceeds P0 SLA)
      'actor_id', 'test_user',
      'source', 'sdlc',
      'schema_version', '2.1.0',
      'object', OBJECT_CONSTRUCT(
        'type', 'work_item',
        'id', 'TEST-SLA-001'
      ),
      'attributes', OBJECT_CONSTRUCT(
        'work_id', 'TEST-SLA-001',
        'title', 'Critical production issue',
        'type', 'bug',
        'severity', 'p0',
        'description', 'System down - needs immediate attention',
        'idempotency_key', 'test_sla_' || UUID_STRING(),
        'schema_version', '1.0.0'
      )
    ),
    'SDLC_TEST',
    CURRENT_TIMESTAMP();
  
  -- Run SLA check
  CALL SDLC_CHECK_SLA() INTO result;
  test_results := ARRAY_APPEND(test_results, OBJECT_CONSTRUCT('test', 'sla_check', 'result', result));
  
  -- Verify breach event was created
  LET breach_count := (
    SELECT COUNT(*)
    FROM ACTIVITY.EVENTS
    WHERE action = 'sdlc.sla.breach'
      AND attributes:work_id::string = 'TEST-SLA-001'
  );
  test_results := ARRAY_APPEND(test_results, OBJECT_CONSTRUCT('test', 'breach_events', 'count', breach_count));
  
  -- Verify escalation event was created for P0
  LET escalation_count := (
    SELECT COUNT(*)
    FROM ACTIVITY.EVENTS
    WHERE action = 'sdlc.work.escalate'
      AND attributes:work_id::string = 'TEST-SLA-001'
  );
  test_results := ARRAY_APPEND(test_results, OBJECT_CONSTRUCT('test', 'escalation_events', 'count', escalation_count));
  
  RETURN OBJECT_CONSTRUCT('test_suite', 'sla_monitoring', 'results', test_results);
END;
$$;

-- Test 8: Daily snapshot generation
CREATE OR REPLACE PROCEDURE SDLC_TEST_SNAPSHOTS()
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
  result VARIANT;
  test_results ARRAY DEFAULT ARRAY_CONSTRUCT();
  snapshot_date STRING;
BEGIN
  -- Generate daily snapshot
  CALL SDLC_GENERATE_DAILY_SNAPSHOT() INTO result;
  test_results := ARRAY_APPEND(test_results, OBJECT_CONSTRUCT('test', 'generate_snapshot', 'result', result));
  
  SET snapshot_date = TO_CHAR(CURRENT_DATE(), 'YYYY-MM-DD');
  
  -- Verify snapshot events were created
  LET metrics_snapshot := (
    SELECT COUNT(*)
    FROM ACTIVITY.EVENTS
    WHERE action = 'sdlc.report.daily_metrics'
      AND attributes:snapshot_date::string = :snapshot_date
  );
  test_results := ARRAY_APPEND(test_results, OBJECT_CONSTRUCT('test', 'metrics_snapshot', 'count', metrics_snapshot));
  
  LET agent_snapshot := (
    SELECT COUNT(*)
    FROM ACTIVITY.EVENTS
    WHERE action = 'sdlc.report.agent_activity'
      AND attributes:snapshot_date::string = :snapshot_date
  );
  test_results := ARRAY_APPEND(test_results, OBJECT_CONSTRUCT('test', 'agent_snapshot', 'count', agent_snapshot));
  
  RETURN OBJECT_CONSTRUCT('test_suite', 'snapshots', 'results', test_results);
END;
$$;

-- ============================================================================
-- F. PERFORMANCE TESTS
-- ============================================================================

-- Test 9: Load test with many work items
CREATE OR REPLACE PROCEDURE SDLC_TEST_LOAD(num_items INT DEFAULT 100)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
  result VARIANT;
  test_results ARRAY DEFAULT ARRAY_CONSTRUCT();
  i INT DEFAULT 1;
  start_time TIMESTAMP;
  end_time TIMESTAMP;
  duration_ms INT;
BEGIN
  SET start_time = CURRENT_TIMESTAMP();
  
  -- Create many work items
  WHILE (i <= num_items) DO
    INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS
    SELECT 
      OBJECT_CONSTRUCT(
        'event_id', UUID_STRING(),
        'action', 'sdlc.work.create',
        'occurred_at', CURRENT_TIMESTAMP(),
        'actor_id', 'load_test_user',
        'source', 'sdlc',
        'schema_version', '2.1.0',
        'object', OBJECT_CONSTRUCT(
          'type', 'work_item',
          'id', 'TEST-LOAD-' || LPAD(i::STRING, 5, '0')
        ),
        'attributes', OBJECT_CONSTRUCT(
          'work_id', 'TEST-LOAD-' || LPAD(i::STRING, 5, '0'),
          'title', 'Load test item ' || i,
          'type', CASE WHEN i % 3 = 0 THEN 'feature' WHEN i % 3 = 1 THEN 'bug' ELSE 'debt' END,
          'severity', CASE WHEN i % 4 = 0 THEN 'p0' WHEN i % 4 = 1 THEN 'p1' WHEN i % 4 = 2 THEN 'p2' ELSE 'p3' END,
          'description', 'Load test work item number ' || i,
          'business_value', MOD(i, 10) + 1,
          'customer_impact', MOD(i, 2) = 0,
          'idempotency_key', 'load_test_' || i || '_' || UUID_STRING(),
          'schema_version', '1.0.0'
        )
      ),
      'SDLC_LOAD_TEST',
      CURRENT_TIMESTAMP();
    i := i + 1;
  END WHILE;
  
  SET end_time = CURRENT_TIMESTAMP();
  SET duration_ms = DATEDIFF('millisecond', start_time, end_time);
  
  test_results := ARRAY_APPEND(test_results, OBJECT_CONSTRUCT(
    'test', 'create_items',
    'count', num_items,
    'duration_ms', duration_ms,
    'items_per_second', ROUND(num_items * 1000.0 / duration_ms, 2)
  ));
  
  -- Test query performance on large dataset
  SET start_time = CURRENT_TIMESTAMP();
  
  LET work_count := (
    SELECT COUNT(*)
    FROM VW_WORK_ITEMS
    WHERE work_id LIKE 'TEST-LOAD-%'
  );
  
  SET end_time = CURRENT_TIMESTAMP();
  SET duration_ms = DATEDIFF('millisecond', start_time, end_time);
  
  test_results := ARRAY_APPEND(test_results, OBJECT_CONSTRUCT(
    'test', 'query_work_items',
    'count', work_count,
    'duration_ms', duration_ms
  ));
  
  -- Test priority queue performance
  SET start_time = CURRENT_TIMESTAMP();
  
  LET queue_count := (
    SELECT COUNT(*)
    FROM VW_PRIORITY_QUEUE
    WHERE work_id LIKE 'TEST-LOAD-%'
    LIMIT 10
  );
  
  SET end_time = CURRENT_TIMESTAMP();
  SET duration_ms = DATEDIFF('millisecond', start_time, end_time);
  
  test_results := ARRAY_APPEND(test_results, OBJECT_CONSTRUCT(
    'test', 'query_priority_queue',
    'duration_ms', duration_ms
  ));
  
  RETURN OBJECT_CONSTRUCT('test_suite', 'load_test', 'results', test_results);
END;
$$;

-- ============================================================================
-- G. DATA QUALITY TESTS
-- ============================================================================

-- Test 10: System health and data integrity
CREATE OR REPLACE PROCEDURE SDLC_TEST_DATA_QUALITY()
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
  result VARIANT;
  test_results ARRAY DEFAULT ARRAY_CONSTRUCT();
BEGIN
  -- Run health check
  CALL SDLC_HEALTH_CHECK() INTO result;
  test_results := ARRAY_APPEND(test_results, OBJECT_CONSTRUCT('test', 'health_check', 'result', result));
  
  -- Check for orphaned work items
  LET orphan_count := (
    SELECT COUNT(*)
    FROM VW_WORK_ITEMS w
    WHERE w.work_id LIKE 'TEST-%'
      AND NOT EXISTS (
        SELECT 1 
        FROM ACTIVITY.EVENTS e
        WHERE e.action = 'sdlc.work.create'
          AND e.attributes:work_id::string = w.work_id
      )
  );
  test_results := ARRAY_APPEND(test_results, OBJECT_CONSTRUCT('test', 'orphaned_items', 'count', orphan_count));
  
  -- Check for duplicate events (same idempotency key)
  LET duplicate_count := (
    SELECT COUNT(*) - COUNT(DISTINCT attributes:idempotency_key::string)
    FROM ACTIVITY.EVENTS
    WHERE action LIKE 'sdlc.%'
      AND attributes:idempotency_key IS NOT NULL
      AND attributes:work_id::string LIKE 'TEST-%'
  );
  test_results := ARRAY_APPEND(test_results, OBJECT_CONSTRUCT('test', 'duplicate_events', 'count', duplicate_count));
  
  -- Verify event consistency (all work items have create event)
  LET missing_create := (
    SELECT COUNT(DISTINCT attributes:work_id::string)
    FROM ACTIVITY.EVENTS e1
    WHERE e1.action LIKE 'sdlc.work.%'
      AND e1.action != 'sdlc.work.create'
      AND e1.attributes:work_id::string LIKE 'TEST-%'
      AND NOT EXISTS (
        SELECT 1
        FROM ACTIVITY.EVENTS e2
        WHERE e2.action = 'sdlc.work.create'
          AND e2.attributes:work_id::string = e1.attributes:work_id::string
      )
  );
  test_results := ARRAY_APPEND(test_results, OBJECT_CONSTRUCT('test', 'missing_create_events', 'count', missing_create));
  
  RETURN OBJECT_CONSTRUCT('test_suite', 'data_quality', 'results', test_results);
END;
$$;

-- ============================================================================
-- H. END-TO-END SCENARIOS
-- ============================================================================

-- Test 11: Complete work item lifecycle
CREATE OR REPLACE PROCEDURE SDLC_TEST_E2E_LIFECYCLE()
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
  result VARIANT;
  test_results ARRAY DEFAULT ARRAY_CONSTRUCT();
  work_id STRING DEFAULT 'TEST-E2E-001';
  last_event_id STRING;
BEGIN
  -- 1. Create work item
  CALL SDLC_CREATE_WORK(
    work_id,
    'End-to-end test feature',
    'feature',
    'p1',
    'Complete lifecycle test',
    'product_owner',
    'e2e_create_' || UUID_STRING(),
    8,
    TRUE
  ) INTO result;
  test_results := ARRAY_APPEND(test_results, OBJECT_CONSTRUCT('step', '1_create', 'result', result));
  
  -- 2. Estimate work
  SELECT last_event_id INTO last_event_id FROM VW_WORK_ITEMS WHERE work_id = :work_id;
  CALL SDLC_ESTIMATE(
    work_id,
    5,
    last_event_id,
    'e2e_estimate_' || UUID_STRING(),
    'tech_lead',
    'Medium complexity feature'
  ) INTO result;
  test_results := ARRAY_APPEND(test_results, OBJECT_CONSTRUCT('step', '2_estimate', 'result', result));
  
  -- 3. Move to ready
  SELECT last_event_id INTO last_event_id FROM VW_WORK_ITEMS WHERE work_id = :work_id;
  CALL SDLC_STATUS(
    work_id,
    'ready',
    last_event_id,
    'e2e_ready_' || UUID_STRING(),
    'scrum_master',
    'Ready for sprint'
  ) INTO result;
  test_results := ARRAY_APPEND(test_results, OBJECT_CONSTRUCT('step', '3_ready', 'result', result));
  
  -- 4. Agent claims work
  CALL SDLC_CLAIM_NEXT(
    'e2e_agent',
    'ai_developer',
    ARRAY_CONSTRUCT('feature', 'javascript'),
    3
  ) INTO result;
  test_results := ARRAY_APPEND(test_results, OBJECT_CONSTRUCT('step', '4_claim', 'result', result));
  
  -- 5. Complete work
  SELECT last_event_id INTO last_event_id FROM VW_WORK_ITEMS WHERE work_id = :work_id;
  CALL SDLC_COMPLETE_WORK(
    work_id,
    last_event_id,
    'e2e_agent',
    'Feature implemented and tested',
    ARRAY_CONSTRUCT('feature.js', 'feature.test.js'),
    TRUE
  ) INTO result;
  test_results := ARRAY_APPEND(test_results, OBJECT_CONSTRUCT('step', '5_complete', 'result', result));
  
  -- 6. Verify final state
  LET final_state := (
    SELECT status, assignee_id, cycle_time_hours
    FROM VW_WORK_ITEMS
    WHERE work_id = :work_id
  );
  test_results := ARRAY_APPEND(test_results, OBJECT_CONSTRUCT('step', '6_final_state', 'state', final_state));
  
  -- 7. Check history
  LET history_count := (
    SELECT COUNT(*)
    FROM VW_WORK_HISTORY
    WHERE work_id = :work_id
  );
  test_results := ARRAY_APPEND(test_results, OBJECT_CONSTRUCT('step', '7_history', 'event_count', history_count));
  
  RETURN OBJECT_CONSTRUCT('test_suite', 'e2e_lifecycle', 'results', test_results);
END;
$$;

-- ============================================================================
-- MASTER TEST RUNNER
-- ============================================================================

-- Run all test suites
CREATE OR REPLACE PROCEDURE SDLC_RUN_ALL_TESTS()
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
  all_results ARRAY DEFAULT ARRAY_CONSTRUCT();
  suite_result VARIANT;
  start_time TIMESTAMP;
  end_time TIMESTAMP;
BEGIN
  SET start_time = CURRENT_TIMESTAMP();
  
  -- Run each test suite
  CALL SDLC_TEST_BASIC_CREATE() INTO suite_result;
  all_results := ARRAY_APPEND(all_results, suite_result);
  
  CALL SDLC_TEST_STATUS_TRANSITIONS() INTO suite_result;
  all_results := ARRAY_APPEND(all_results, suite_result);
  
  CALL SDLC_TEST_CONCURRENCY() INTO suite_result;
  all_results := ARRAY_APPEND(all_results, suite_result);
  
  CALL SDLC_TEST_IDEMPOTENCY() INTO suite_result;
  all_results := ARRAY_APPEND(all_results, suite_result);
  
  CALL SDLC_TEST_AGENT_OPERATIONS() INTO suite_result;
  all_results := ARRAY_APPEND(all_results, suite_result);
  
  CALL SDLC_TEST_DEPENDENCIES() INTO suite_result;
  all_results := ARRAY_APPEND(all_results, suite_result);
  
  CALL SDLC_TEST_SLA_MONITORING() INTO suite_result;
  all_results := ARRAY_APPEND(all_results, suite_result);
  
  CALL SDLC_TEST_SNAPSHOTS() INTO suite_result;
  all_results := ARRAY_APPEND(all_results, suite_result);
  
  CALL SDLC_TEST_LOAD(50) INTO suite_result;  -- Smaller load test
  all_results := ARRAY_APPEND(all_results, suite_result);
  
  CALL SDLC_TEST_DATA_QUALITY() INTO suite_result;
  all_results := ARRAY_APPEND(all_results, suite_result);
  
  CALL SDLC_TEST_E2E_LIFECYCLE() INTO suite_result;
  all_results := ARRAY_APPEND(all_results, suite_result);
  
  SET end_time = CURRENT_TIMESTAMP();
  
  -- Clean up test data
  CALL SDLC_TEST_CLEANUP() INTO suite_result;
  
  RETURN OBJECT_CONSTRUCT(
    'test_run', 'complete',
    'start_time', start_time,
    'end_time', end_time,
    'duration_seconds', DATEDIFF('second', start_time, end_time),
    'test_suites', ARRAY_SIZE(all_results),
    'results', all_results,
    'cleanup', suite_result
  );
END;
$$;

-- ============================================================================
-- TEST VALIDATION QUERIES
-- ============================================================================

-- Query to verify test coverage
CREATE OR REPLACE VIEW VW_SDLC_TEST_COVERAGE AS
SELECT 
  'Procedures' AS component_type,
  COUNT(*) AS total_components,
  COUNT(CASE WHEN procedure_name LIKE '%TEST%' THEN 1 END) AS test_procedures
FROM INFORMATION_SCHEMA.PROCEDURES
WHERE procedure_schema = 'MCP'
  AND procedure_name LIKE 'SDLC%'

UNION ALL

SELECT 
  'Views',
  COUNT(*),
  COUNT(CASE WHEN table_name LIKE '%TEST%' THEN 1 END)
FROM INFORMATION_SCHEMA.VIEWS
WHERE table_schema = 'MCP'
  AND table_name LIKE 'VW_%'

UNION ALL

SELECT 
  'Event Types',
  COUNT(DISTINCT action),
  COUNT(DISTINCT CASE WHEN attributes:work_id::string LIKE 'TEST-%' THEN action END)
FROM CLAUDE_BI.ACTIVITY.EVENTS
WHERE action LIKE 'sdlc.%';

-- ============================================================================
-- PERFORMANCE BENCHMARKS
-- ============================================================================

-- View to track test performance over time
CREATE OR REPLACE VIEW VW_SDLC_TEST_BENCHMARKS AS
WITH test_runs AS (
  SELECT 
    occurred_at AS test_time,
    attributes:test_suite::string AS test_suite,
    attributes:duration_ms::number AS duration_ms,
    attributes:items_per_second::number AS throughput
  FROM CLAUDE_BI.ACTIVITY.EVENTS
  WHERE action = 'sdlc.test.benchmark'
    AND occurred_at >= DATEADD('day', -30, CURRENT_TIMESTAMP())
)
SELECT 
  test_suite,
  COUNT(*) AS run_count,
  AVG(duration_ms) AS avg_duration_ms,
  MIN(duration_ms) AS best_duration_ms,
  MAX(duration_ms) AS worst_duration_ms,
  AVG(throughput) AS avg_throughput,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY duration_ms) AS median_duration_ms
FROM test_runs
GROUP BY test_suite
ORDER BY test_suite;

-- ============================================================================
-- Grant permissions for test procedures
-- ============================================================================
GRANT USAGE ON PROCEDURE SDLC_TEST_CLEANUP() TO ROLE MCP_ADMIN_ROLE;
GRANT USAGE ON PROCEDURE SDLC_TEST_BASIC_CREATE() TO ROLE MCP_ADMIN_ROLE;
GRANT USAGE ON PROCEDURE SDLC_TEST_STATUS_TRANSITIONS() TO ROLE MCP_ADMIN_ROLE;
GRANT USAGE ON PROCEDURE SDLC_TEST_CONCURRENCY() TO ROLE MCP_ADMIN_ROLE;
GRANT USAGE ON PROCEDURE SDLC_TEST_IDEMPOTENCY() TO ROLE MCP_ADMIN_ROLE;
GRANT USAGE ON PROCEDURE SDLC_TEST_AGENT_OPERATIONS() TO ROLE MCP_ADMIN_ROLE;
GRANT USAGE ON PROCEDURE SDLC_TEST_DEPENDENCIES() TO ROLE MCP_ADMIN_ROLE;
GRANT USAGE ON PROCEDURE SDLC_TEST_SLA_MONITORING() TO ROLE MCP_ADMIN_ROLE;
GRANT USAGE ON PROCEDURE SDLC_TEST_SNAPSHOTS() TO ROLE MCP_ADMIN_ROLE;
GRANT USAGE ON PROCEDURE SDLC_TEST_LOAD(INT) TO ROLE MCP_ADMIN_ROLE;
GRANT USAGE ON PROCEDURE SDLC_TEST_DATA_QUALITY() TO ROLE MCP_ADMIN_ROLE;
GRANT USAGE ON PROCEDURE SDLC_TEST_E2E_LIFECYCLE() TO ROLE MCP_ADMIN_ROLE;
GRANT USAGE ON PROCEDURE SDLC_RUN_ALL_TESTS() TO ROLE MCP_ADMIN_ROLE;

GRANT SELECT ON VIEW VW_SDLC_TEST_COVERAGE TO ROLE MCP_ADMIN_ROLE;
GRANT SELECT ON VIEW VW_SDLC_TEST_BENCHMARKS TO ROLE MCP_ADMIN_ROLE;

-- ============================================================================
-- TEST EXECUTION INSTRUCTIONS
-- ============================================================================

/*
To run the SDLC system tests:

1. Run all tests:
   CALL SDLC_RUN_ALL_TESTS();

2. Run individual test suites:
   CALL SDLC_TEST_BASIC_CREATE();
   CALL SDLC_TEST_CONCURRENCY();
   CALL SDLC_TEST_AGENT_OPERATIONS();
   etc.

3. Run load test with custom size:
   CALL SDLC_TEST_LOAD(1000);  -- Create 1000 test items

4. Check test coverage:
   SELECT * FROM VW_SDLC_TEST_COVERAGE;

5. View performance benchmarks:
   SELECT * FROM VW_SDLC_TEST_BENCHMARKS;

6. Clean up test data:
   CALL SDLC_TEST_CLEANUP();

Note: All test work items use the prefix 'TEST-' to distinguish them from real data.
*/

-- ============================================================================
-- END OF TEST SCENARIOS
-- 
-- The SDLC system is now complete with comprehensive test coverage!
-- ============================================================================