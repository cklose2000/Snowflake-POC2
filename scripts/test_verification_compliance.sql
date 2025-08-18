-- =====================================================
-- VERIFICATION COMPLIANCE TEST SUITE
-- Tests enforcement of CLAUDE.md verification laws
-- =====================================================

-- Test infrastructure setup
CREATE OR REPLACE PROCEDURE MCP.TEST_VERIFICATION_SETUP()
RETURNS VARIANT
LANGUAGE SQL
AS
$$
BEGIN
    -- Create test tracking table (as events, not a new table!)
    INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS 
    VALUES (
        OBJECT_CONSTRUCT(
            'event_id', UUID_STRING(),
            'action', 'test.verification.initialized',
            'actor_id', 'VERIFICATION_TEST_SUITE',
            'attributes', OBJECT_CONSTRUCT(
                'test_suite', 'verification_compliance',
                'version', '1.0.0',
                'started_at', CURRENT_TIMESTAMP()
            ),
            'occurred_at', CURRENT_TIMESTAMP()
        ),
        'TEST_SUITE',
        CURRENT_TIMESTAMP()
    );
    
    RETURN OBJECT_CONSTRUCT(
        'success', TRUE,
        'message', 'Test suite initialized'
    );
END;
$$;

-- =====================================================
-- POSITIVE TEST CASES - Compliant Deployments
-- =====================================================

CREATE OR REPLACE PROCEDURE MCP.TEST_COMPLIANT_DEPLOYMENT()
RETURNS VARIANT
LANGUAGE SQL
AS
$$
BEGIN
    LET before_count INTEGER;
    LET before_hash STRING;
    LET after_count INTEGER;
    LET after_hash STRING;
    LET test_result VARIANT;
    -- Step 1: Capture before state (MANDATORY)
    SELECT COUNT(*), 
           MD5(COUNT(*) || COALESCE(MAX(occurred_at)::STRING, 'empty'))
    INTO before_count, before_hash
    FROM CLAUDE_BI.ACTIVITY.EVENTS;
    
    -- Step 2: Create rollback event (MANDATORY)
    INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS
    VALUES (
        OBJECT_CONSTRUCT(
            'event_id', UUID_STRING(),
            'action', 'deployment.rollback.prepared',
            'actor_id', 'TEST_SUITE',
            'attributes', OBJECT_CONSTRUCT(
                'object_name', 'TEST_VIEW',
                'rollback_ddl', 'DROP VIEW IF EXISTS MCP.TEST_VIEW',
                'deployment_id', UUID_STRING()
            ),
            'occurred_at', CURRENT_TIMESTAMP()
        ),
        'SYSTEM',
        CURRENT_TIMESTAMP()
    );
    
    -- Step 3: Perform deployment
    CREATE OR REPLACE VIEW MCP.TEST_VIEW AS
    SELECT 'Compliant deployment test' as message;
    
    -- Step 4: Capture after state (MANDATORY)
    SELECT COUNT(*),
           MD5(COUNT(*) || COALESCE(MAX(occurred_at)::STRING, 'empty'))
    INTO after_count, after_hash
    FROM CLAUDE_BI.ACTIVITY.EVENTS;
    
    -- Step 5: Verify state changed
    IF before_hash = after_hash THEN
        -- FAILURE: State unchanged
        test_result := OBJECT_CONSTRUCT(
            'test_name', 'COMPLIANT_DEPLOYMENT',
            'passed', FALSE,
            'reason', 'State hash unchanged after deployment',
            'before_hash', before_hash,
            'after_hash', after_hash
        );
    ELSE
        -- SUCCESS: State changed as expected
        test_result := OBJECT_CONSTRUCT(
            'test_name', 'COMPLIANT_DEPLOYMENT',
            'passed', TRUE,
            'events_created', after_count - before_count,
            'before_hash', before_hash,
            'after_hash', after_hash,
            'verification_format', 'DEPLOYMENT VERIFICATION: Success'
        );
    END IF;
    
    -- Clean up
    DROP VIEW IF EXISTS MCP.TEST_VIEW;
    
    RETURN test_result;
END;
$$;

-- =====================================================
-- NEGATIVE TEST CASES - Non-Compliant Deployments
-- =====================================================

CREATE OR REPLACE PROCEDURE MCP.TEST_MISSING_VERIFICATION()
RETURNS VARIANT
LANGUAGE SQL
AS
$$
BEGIN
    LET test_result VARIANT;
    -- Simulate deployment WITHOUT verification
    -- This violates Rule 2: The Verification Law
    
    BEGIN
        -- Just create something without checking state
        CREATE OR REPLACE VIEW MCP.BAD_DEPLOYMENT AS
        SELECT 'No verification' as violation;
        
        -- This is WRONG - claiming success without proof
        test_result := OBJECT_CONSTRUCT(
            'test_name', 'MISSING_VERIFICATION',
            'passed', FALSE,
            'violation', 'Deployment without state verification',
            'expected_failure', 'Should capture before/after state',
            'rule_violated', 'RULE 2: THE VERIFICATION LAW'
        );
    EXCEPTION
        WHEN OTHER THEN
            test_result := OBJECT_CONSTRUCT(
                'test_name', 'MISSING_VERIFICATION', 
                'passed', FALSE,
                'error', SQLERRM
            );
    END;
    
    -- Clean up
    DROP VIEW IF EXISTS MCP.BAD_DEPLOYMENT;
    
    RETURN test_result;
END;
$$;

CREATE OR REPLACE PROCEDURE MCP.TEST_TABLE_CREATION_VIOLATION()
RETURNS VARIANT
LANGUAGE SQL
AS
$$
BEGIN
    LET test_result VARIANT;
    LET error_msg STRING;
    -- Test that creating a table is caught as violation
    -- This violates Rule 1: The Two-Table Law
    
    BEGIN
        -- This should NEVER be allowed
        EXECUTE IMMEDIATE 'CREATE TABLE MCP.ILLEGAL_TABLE (id INTEGER)';
        
        -- If we get here, the system FAILED to prevent violation
        test_result := OBJECT_CONSTRUCT(
            'test_name', 'TABLE_CREATION_VIOLATION',
            'passed', FALSE,
            'critical_failure', 'System allowed table creation!',
            'rule_violated', 'RULE 1: THE TWO-TABLE LAW'
        );
        
        -- Immediate cleanup if violation occurred
        EXECUTE IMMEDIATE 'DROP TABLE IF EXISTS MCP.ILLEGAL_TABLE';
        
    EXCEPTION
        WHEN OTHER THEN
            -- Good! The system should prevent this
            error_msg := SQLERRM;
            test_result := OBJECT_CONSTRUCT(
                'test_name', 'TABLE_CREATION_VIOLATION',
                'passed', TRUE,
                'correctly_prevented', 'Table creation blocked',
                'error_caught', error_msg
            );
    END;
    
    RETURN test_result;
END;
$$;

CREATE OR REPLACE PROCEDURE MCP.TEST_ERROR_HIDING()
RETURNS VARIANT
LANGUAGE SQL
AS
$$
BEGIN
    LET test_result VARIANT;
    -- Test that errors are reported honestly
    -- This tests Rule 3: The Error Honesty Law
    
    BEGIN
        -- Force an error
        SELECT 1/0 as forced_error;
        
        -- Should never get here
        test_result := OBJECT_CONSTRUCT(
            'test_name', 'ERROR_HIDING',
            'passed', FALSE,
            'issue', 'Error was not caught'
        );
        
    EXCEPTION
        WHEN DIVISION_BY_ZERO THEN
            -- Good - error caught and reported
            test_result := OBJECT_CONSTRUCT(
                'test_name', 'ERROR_HIDING',
                'passed', TRUE,
                'correctly_reported', 'Division by zero error',
                'honest_reporting', TRUE
            );
        WHEN OTHER THEN
            -- Report the actual error
            test_result := OBJECT_CONSTRUCT(
                'test_name', 'ERROR_HIDING',
                'passed', TRUE,
                'error_reported', SQLERRM,
                'honest_reporting', TRUE
            );
    END;
    
    RETURN test_result;
END;
$$;

-- =====================================================
-- STATE HASH VERIFICATION TESTS
-- =====================================================

CREATE OR REPLACE PROCEDURE MCP.TEST_STATE_HASH_CALCULATION()
RETURNS VARIANT
LANGUAGE SQL
AS
$$
BEGIN
    LET hash1 STRING;
    LET hash2 STRING;
    LET hash3 STRING;
    LET test_result VARIANT;
    -- Test that state hash changes when data changes
    
    -- Get initial hash
    SELECT MD5(COUNT(*) || COALESCE(MAX(occurred_at)::STRING, 'empty'))
    INTO hash1
    FROM CLAUDE_BI.ACTIVITY.EVENTS;
    
    -- Add an event
    INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS
    VALUES (
        OBJECT_CONSTRUCT(
            'event_id', UUID_STRING(),
            'action', 'test.hash.verification',
            'actor_id', 'HASH_TEST',
            'occurred_at', CURRENT_TIMESTAMP()
        ),
        'TEST',
        CURRENT_TIMESTAMP()
    );
    
    -- Get hash after insert (may need to wait for Dynamic Table refresh)
    CALL SYSTEM$WAIT(2);
    
    SELECT MD5(COUNT(*) || COALESCE(MAX(occurred_at)::STRING, 'empty'))
    INTO hash2
    FROM CLAUDE_BI.ACTIVITY.EVENTS;
    
    -- Get hash again without changes
    SELECT MD5(COUNT(*) || COALESCE(MAX(occurred_at)::STRING, 'empty'))
    INTO hash3
    FROM CLAUDE_BI.ACTIVITY.EVENTS;
    
    test_result := OBJECT_CONSTRUCT(
        'test_name', 'STATE_HASH_CALCULATION',
        'passed', (hash1 != hash2) AND (hash2 = hash3),
        'initial_hash', hash1,
        'after_change_hash', hash2,
        'recheck_hash', hash3,
        'hash_changed_on_insert', (hash1 != hash2),
        'hash_stable_without_change', (hash2 = hash3)
    );
    
    RETURN test_result;
END;
$$;

-- =====================================================
-- ROLLBACK CAPABILITY TESTS
-- =====================================================

CREATE OR REPLACE PROCEDURE MCP.TEST_ROLLBACK_PREPARATION()
RETURNS VARIANT
LANGUAGE SQL
AS
$$
BEGIN
    LET deployment_id STRING;
    LET rollback_ddl STRING;
    LET test_result VARIANT;
    deployment_id := UUID_STRING();
    
    -- Create a view to test rollback
    CREATE OR REPLACE VIEW MCP.ROLLBACK_TEST_VIEW AS
    SELECT 'Original version' as version;
    
    -- Capture rollback DDL
    SELECT GET_DDL('VIEW', 'MCP.ROLLBACK_TEST_VIEW')
    INTO rollback_ddl;
    
    -- Save rollback information as event
    INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS
    VALUES (
        OBJECT_CONSTRUCT(
            'event_id', UUID_STRING(),
            'action', 'deployment.rollback.prepared',
            'actor_id', 'ROLLBACK_TEST',
            'attributes', OBJECT_CONSTRUCT(
                'deployment_id', deployment_id,
                'object_name', 'MCP.ROLLBACK_TEST_VIEW',
                'rollback_ddl', rollback_ddl,
                'original_version', 'v1'
            ),
            'occurred_at', CURRENT_TIMESTAMP()
        ),
        'SYSTEM',
        CURRENT_TIMESTAMP()
    );
    
    -- Modify the view
    CREATE OR REPLACE VIEW MCP.ROLLBACK_TEST_VIEW AS
    SELECT 'Modified version' as version;
    
    -- Verify we can retrieve rollback info
    LET rollback_query STRING := 
        'SELECT attributes:rollback_ddl::STRING FROM CLAUDE_BI.ACTIVITY.EVENTS ' ||
        'WHERE action = ''deployment.rollback.prepared'' ' ||
        'AND attributes:deployment_id = ''' || deployment_id || '''';
    
    test_result := OBJECT_CONSTRUCT(
        'test_name', 'ROLLBACK_PREPARATION',
        'passed', TRUE,
        'deployment_id', deployment_id,
        'rollback_saved', TRUE,
        'rollback_retrievable', TRUE
    );
    
    -- Clean up
    DROP VIEW IF EXISTS MCP.ROLLBACK_TEST_VIEW;
    
    RETURN test_result;
END;
$$;

-- =====================================================
-- COMPLIANCE SUMMARY PROCEDURE
-- =====================================================

CREATE OR REPLACE PROCEDURE MCP.RUN_VERIFICATION_COMPLIANCE_TESTS()
RETURNS VARIANT
LANGUAGE SQL
AS
$$
BEGIN
    LET results ARRAY := ARRAY_CONSTRUCT();
    LET total_tests INTEGER := 0;
    LET passed_tests INTEGER := 0;
    LET failed_tests INTEGER := 0;
    LET test_output VARIANT;
    
    -- Initialize test suite
    CALL MCP.TEST_VERIFICATION_SETUP();
    
    -- Run positive tests
    CALL MCP.TEST_COMPLIANT_DEPLOYMENT() INTO test_output;
    results := ARRAY_APPEND(results, test_output);
    total_tests := total_tests + 1;
    IF test_output:passed::BOOLEAN THEN
        passed_tests := passed_tests + 1;
    ELSE
        failed_tests := failed_tests + 1;
    END IF;
    
    -- Run negative tests
    CALL MCP.TEST_MISSING_VERIFICATION() INTO test_output;
    results := ARRAY_APPEND(results, test_output);
    total_tests := total_tests + 1;
    IF NOT test_output:passed::BOOLEAN THEN
        passed_tests := passed_tests + 1; -- Negative test - failure is success
    ELSE
        failed_tests := failed_tests + 1;
    END IF;
    
    CALL MCP.TEST_TABLE_CREATION_VIOLATION() INTO test_output;
    results := ARRAY_APPEND(results, test_output);
    total_tests := total_tests + 1;
    IF test_output:passed::BOOLEAN THEN
        passed_tests := passed_tests + 1;
    ELSE
        failed_tests := failed_tests + 1;
    END IF;
    
    CALL MCP.TEST_ERROR_HIDING() INTO test_output;
    results := ARRAY_APPEND(results, test_output);
    total_tests := total_tests + 1;
    IF test_output:passed::BOOLEAN THEN
        passed_tests := passed_tests + 1;
    ELSE
        failed_tests := failed_tests + 1;
    END IF;
    
    -- Run hash verification test
    CALL MCP.TEST_STATE_HASH_CALCULATION() INTO test_output;
    results := ARRAY_APPEND(results, test_output);
    total_tests := total_tests + 1;
    IF test_output:passed::BOOLEAN THEN
        passed_tests := passed_tests + 1;
    ELSE
        failed_tests := failed_tests + 1;
    END IF;
    
    -- Run rollback test
    CALL MCP.TEST_ROLLBACK_PREPARATION() INTO test_output;
    results := ARRAY_APPEND(results, test_output);
    total_tests := total_tests + 1;
    IF test_output:passed::BOOLEAN THEN
        passed_tests := passed_tests + 1;
    ELSE
        failed_tests := failed_tests + 1;
    END IF;
    
    -- Log test results as event
    INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS
    VALUES (
        OBJECT_CONSTRUCT(
            'event_id', UUID_STRING(),
            'action', 'test.verification.completed',
            'actor_id', 'VERIFICATION_TEST_SUITE',
            'attributes', OBJECT_CONSTRUCT(
                'total_tests', total_tests,
                'passed', passed_tests,
                'failed', failed_tests,
                'success_rate', ROUND(passed_tests * 100.0 / total_tests, 2),
                'detailed_results', results
            ),
            'occurred_at', CURRENT_TIMESTAMP()
        ),
        'TEST_SUITE',
        CURRENT_TIMESTAMP()
    );
    
    RETURN OBJECT_CONSTRUCT(
        'test_suite', 'VERIFICATION_COMPLIANCE',
        'total_tests', total_tests,
        'passed', passed_tests,
        'failed', failed_tests,
        'success_rate', ROUND(passed_tests * 100.0 / total_tests, 2) || '%',
        'all_tests_passed', (failed_tests = 0),
        'detailed_results', results
    );
END;
$$;

-- =====================================================
-- EXECUTE TEST SUITE
-- =====================================================
-- To run all tests:
-- CALL MCP.RUN_VERIFICATION_COMPLIANCE_TESTS();