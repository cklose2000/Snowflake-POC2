-- ============================================================================
-- Test Suite for HappyFox Ingestion Pipeline
-- Purpose: Validate two-table compliance, data integrity, and view accuracy
-- ============================================================================

USE DATABASE CLAUDE_BI;
USE WAREHOUSE CLAUDE_WAREHOUSE;

-- ----------------------------------------------------------------------------
-- TEST PROCEDURE
-- ----------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE MCP.TEST_HAPPYFOX_PIPELINE()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    test_results ARRAY DEFAULT ARRAY_CONSTRUCT();
    test_result VARIANT;
    table_count INTEGER;
    view_count INTEGER;
    event_count INTEGER;
    unique_tickets INTEGER;
    test_passed BOOLEAN;
    all_passed BOOLEAN DEFAULT TRUE;
BEGIN
    -- ========================================================================
    -- TEST 1: Two-Table Compliance
    -- ========================================================================
    SELECT COUNT(*) INTO table_count
    FROM CLAUDE_BI.INFORMATION_SCHEMA.TABLES
    WHERE TABLE_CATALOG = 'CLAUDE_BI'
      AND TABLE_SCHEMA IN ('LANDING', 'ACTIVITY')
      AND TABLE_TYPE IN ('BASE TABLE', 'DYNAMIC TABLE');
    
    test_passed := (table_count = 2);
    all_passed := all_passed AND test_passed;
    
    test_result := OBJECT_CONSTRUCT(
        'test_name', 'Two-Table Compliance',
        'description', 'Verify only RAW_EVENTS and EVENTS tables exist',
        'expected', 2,
        'actual', table_count,
        'passed', test_passed,
        'message', IFF(test_passed, 'PASS: Exactly 2 tables found', 'FAIL: Found ' || table_count || ' tables instead of 2')
    );
    test_results := ARRAY_APPEND(test_results, test_result);
    
    -- ========================================================================
    -- TEST 2: HappyFox Events Ingested
    -- ========================================================================
    SELECT COUNT(*) INTO event_count
    FROM LANDING.RAW_EVENTS
    WHERE DATA:action = 'happyfox.ticket.upserted';
    
    test_passed := (event_count > 0);
    all_passed := all_passed AND test_passed;
    
    test_result := OBJECT_CONSTRUCT(
        'test_name', 'HappyFox Events Present',
        'description', 'Verify HappyFox events exist in RAW_EVENTS',
        'expected', '>0',
        'actual', event_count,
        'passed', test_passed,
        'message', IFF(test_passed, 'PASS: ' || event_count || ' HappyFox events found', 'FAIL: No HappyFox events found')
    );
    test_results := ARRAY_APPEND(test_results, test_result);
    
    -- ========================================================================
    -- TEST 3: Dynamic Table Propagation
    -- ========================================================================
    SELECT COUNT(*) INTO event_count
    FROM ACTIVITY.EVENTS
    WHERE action = 'happyfox.ticket.upserted';
    
    test_passed := (event_count > 0);
    all_passed := all_passed AND test_passed;
    
    test_result := OBJECT_CONSTRUCT(
        'test_name', 'Dynamic Table Propagation',
        'description', 'Verify events flow to ACTIVITY.EVENTS',
        'expected', '>0',
        'actual', event_count,
        'passed', test_passed,
        'message', IFF(test_passed, 'PASS: ' || event_count || ' events in Dynamic Table', 'FAIL: Events not propagating to Dynamic Table')
    );
    test_results := ARRAY_APPEND(test_results, test_result);
    
    -- ========================================================================
    -- TEST 4: Idempotency Check
    -- ========================================================================
    DECLARE
        dup_count INTEGER;
    BEGIN
        -- Check for duplicate idempotency keys
        SELECT COUNT(*) INTO dup_count
        FROM (
            SELECT DATA:idempotency_key, COUNT(*) as cnt
            FROM LANDING.RAW_EVENTS
            WHERE DATA:action = 'happyfox.ticket.upserted'
            GROUP BY DATA:idempotency_key
            HAVING COUNT(*) > 1
        );
        
        test_passed := (dup_count = 0);
        all_passed := all_passed AND test_passed;
        
        test_result := OBJECT_CONSTRUCT(
            'test_name', 'Idempotency Validation',
            'description', 'Verify no duplicate tickets with same idempotency key',
            'expected', 0,
            'actual', dup_count,
            'passed', test_passed,
            'message', IFF(test_passed, 'PASS: No duplicates found', 'FAIL: ' || dup_count || ' duplicate idempotency keys found')
        );
        test_results := ARRAY_APPEND(test_results, test_result);
    END;
    
    -- ========================================================================
    -- TEST 5: View Creation
    -- ========================================================================
    SELECT COUNT(*) INTO view_count
    FROM CLAUDE_BI.INFORMATION_SCHEMA.TABLES
    WHERE TABLE_CATALOG = 'CLAUDE_BI'
      AND TABLE_SCHEMA = 'MCP'
      AND TABLE_NAME LIKE 'VW_HF_%'
      AND TABLE_TYPE = 'VIEW';
    
    test_passed := (view_count >= 7);  -- We created 7 main views
    all_passed := all_passed AND test_passed;
    
    test_result := OBJECT_CONSTRUCT(
        'test_name', 'View Creation',
        'description', 'Verify all HappyFox views are created',
        'expected', '>=7',
        'actual', view_count,
        'passed', test_passed,
        'message', IFF(test_passed, 'PASS: ' || view_count || ' views created', 'FAIL: Only ' || view_count || ' views found')
    );
    test_results := ARRAY_APPEND(test_results, test_result);
    
    -- ========================================================================
    -- TEST 6: View Data Accessibility
    -- ========================================================================
    BEGIN
        SELECT COUNT(DISTINCT ticket_id) INTO unique_tickets
        FROM MCP.VW_HF_TICKETS;
        
        test_passed := (unique_tickets > 0);
        all_passed := all_passed AND test_passed;
        
        test_result := OBJECT_CONSTRUCT(
            'test_name', 'View Data Accessibility',
            'description', 'Verify main ticket view returns data',
            'expected', '>0',
            'actual', unique_tickets,
            'passed', test_passed,
            'message', IFF(test_passed, 'PASS: ' || unique_tickets || ' unique tickets accessible', 'FAIL: No data in ticket view')
        );
        test_results := ARRAY_APPEND(test_results, test_result);
    EXCEPTION
        WHEN OTHER THEN
            test_passed := FALSE;
            all_passed := FALSE;
            test_result := OBJECT_CONSTRUCT(
                'test_name', 'View Data Accessibility',
                'description', 'Verify main ticket view returns data',
                'expected', '>0',
                'actual', 'ERROR',
                'passed', test_passed,
                'message', 'FAIL: Error accessing view - ' || SQLERRM
            );
            test_results := ARRAY_APPEND(test_results, test_result);
    END;
    
    -- ========================================================================
    -- TEST 7: Catalog Registration
    -- ========================================================================
    DECLARE
        catalog_count INTEGER;
    BEGIN
        SELECT COUNT(*) INTO catalog_count
        FROM MCP.CATALOG_VIEWS
        WHERE VIEW_NAME LIKE 'VW_HF_%';
        
        test_passed := (catalog_count >= 7);
        all_passed := all_passed AND test_passed;
        
        test_result := OBJECT_CONSTRUCT(
            'test_name', 'Catalog Registration',
            'description', 'Verify views are registered in catalog',
            'expected', '>=7',
            'actual', catalog_count,
            'passed', test_passed,
            'message', IFF(test_passed, 'PASS: ' || catalog_count || ' views cataloged', 'FAIL: Only ' || catalog_count || ' views in catalog')
        );
        test_results := ARRAY_APPEND(test_results, test_result);
    END;
    
    -- ========================================================================
    -- TEST 8: Aging Calculation
    -- ========================================================================
    DECLARE
        aging_count INTEGER;
    BEGIN
        SELECT COUNT(*) INTO aging_count
        FROM MCP.VW_HF_TICKET_AGING
        WHERE age_days >= 0
          AND age_bucket IS NOT NULL;
        
        test_passed := (aging_count > 0);
        all_passed := all_passed AND test_passed;
        
        test_result := OBJECT_CONSTRUCT(
            'test_name', 'Aging Calculation',
            'description', 'Verify aging metrics are calculated',
            'expected', '>0',
            'actual', aging_count,
            'passed', test_passed,
            'message', IFF(test_passed, 'PASS: Aging calculated for ' || aging_count || ' tickets', 'FAIL: Aging calculations not working')
        );
        test_results := ARRAY_APPEND(test_results, test_result);
    END;
    
    -- ========================================================================
    -- TEST 9: Product Prefix Extraction
    -- ========================================================================
    DECLARE
        prefix_count INTEGER;
    BEGIN
        SELECT COUNT(DISTINCT product_prefix) INTO prefix_count
        FROM MCP.VW_HF_TICKETS
        WHERE product_prefix IS NOT NULL;
        
        test_passed := (prefix_count > 0);
        all_passed := all_passed AND test_passed;
        
        test_result := OBJECT_CONSTRUCT(
            'test_name', 'Product Prefix Extraction',
            'description', 'Verify product prefixes are extracted',
            'expected', '>0',
            'actual', prefix_count,
            'passed', test_passed,
            'message', IFF(test_passed, 'PASS: ' || prefix_count || ' product prefixes found', 'FAIL: No product prefixes extracted')
        );
        test_results := ARRAY_APPEND(test_results, test_result);
    END;
    
    -- ========================================================================
    -- TEST 10: Monitoring Views
    -- ========================================================================
    DECLARE
        pipeline_status STRING;
    BEGIN
        SELECT pipeline_status INTO pipeline_status
        FROM MCP.VW_HF_PIPELINE_HEALTH;
        
        test_passed := (pipeline_status IS NOT NULL);
        all_passed := all_passed AND test_passed;
        
        test_result := OBJECT_CONSTRUCT(
            'test_name', 'Monitoring Views',
            'description', 'Verify monitoring views are functional',
            'expected', 'NOT NULL',
            'actual', pipeline_status,
            'passed', test_passed,
            'message', IFF(test_passed, 'PASS: Pipeline status is ' || pipeline_status, 'FAIL: Monitoring views not working')
        );
        test_results := ARRAY_APPEND(test_results, test_result);
    EXCEPTION
        WHEN OTHER THEN
            test_passed := FALSE;
            all_passed := FALSE;
            test_result := OBJECT_CONSTRUCT(
                'test_name', 'Monitoring Views',
                'description', 'Verify monitoring views are functional',
                'expected', 'NOT NULL',
                'actual', 'ERROR',
                'passed', test_passed,
                'message', 'FAIL: Error accessing monitoring views'
            );
            test_results := ARRAY_APPEND(test_results, test_result);
    END;
    
    -- ========================================================================
    -- RETURN TEST RESULTS
    -- ========================================================================
    RETURN OBJECT_CONSTRUCT(
        'test_suite', 'HappyFox Pipeline Tests',
        'executed_at', CURRENT_TIMESTAMP()::STRING,
        'total_tests', ARRAY_SIZE(test_results),
        'passed', all_passed,
        'summary', IFF(all_passed, 
                      'ALL TESTS PASSED', 
                      'SOME TESTS FAILED - Review results'),
        'test_results', test_results
    );
END;
$$;

-- ----------------------------------------------------------------------------
-- SIMPLE TEST RUNNER
-- ----------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE MCP.RUN_HAPPYFOX_TESTS()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    results VARIANT;
    output STRING;
BEGIN
    CALL MCP.TEST_HAPPYFOX_PIPELINE() INTO :results;
    
    -- Format output for display
    output := '
================================================================================
HAPPYFOX PIPELINE TEST RESULTS
================================================================================
Executed: ' || results:executed_at || '
Status: ' || results:summary || '
Tests Run: ' || results:total_tests || '

TEST DETAILS:
';
    
    -- Add each test result
    FOR i IN 0 TO ARRAY_SIZE(results:test_results) - 1 DO
        LET test := results:test_results[i];
        output := output || '
' || (i+1) || '. ' || test:test_name || '
   Result: ' || IFF(test:passed, '✓ PASS', '✗ FAIL') || '
   ' || test:message || '
';
    END FOR;
    
    output := output || '
================================================================================
';
    
    RETURN output;
END;
$$;

-- ----------------------------------------------------------------------------
-- GRANT PERMISSIONS
-- ----------------------------------------------------------------------------

GRANT USAGE ON PROCEDURE MCP.TEST_HAPPYFOX_PIPELINE() TO ROLE SYSADMIN;
GRANT USAGE ON PROCEDURE MCP.RUN_HAPPYFOX_TESTS() TO ROLE SYSADMIN;

-- ----------------------------------------------------------------------------
-- RUN TESTS
-- ----------------------------------------------------------------------------

-- Execute test suite and show results
CALL MCP.RUN_HAPPYFOX_TESTS();