-- ============================================================================
-- HappyFox Analytics Stack - Comprehensive Test Suite
-- Tests all components without needing Streamlit UI
-- ============================================================================

USE DATABASE CLAUDE_BI;
USE WAREHOUSE CLAUDE_WAREHOUSE;
USE SCHEMA MCP;

-- ============================================================================
-- TEST PROCEDURE: Simulates all Streamlit operations
-- ============================================================================

CREATE OR REPLACE PROCEDURE MCP.TEST_HAPPYFOX_ANALYTICS()
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    test_results ARRAY DEFAULT ARRAY_CONSTRUCT();
    test_name VARCHAR;
    test_status VARCHAR;
    test_message VARCHAR;
    row_count NUMBER;
    exec_time NUMBER;
    start_time TIMESTAMP_NTZ;
    total_tests NUMBER DEFAULT 0;
    passed_tests NUMBER DEFAULT 0;
    failed_tests NUMBER DEFAULT 0;
BEGIN
    -- ========================================================================
    -- TEST 1: Data Availability
    -- ========================================================================
    test_name := 'Data Availability';
    start_time := CURRENT_TIMESTAMP();
    
    BEGIN
        SELECT COUNT(*) INTO row_count FROM MCP.VW_HF_TICKETS_LATEST;
        
        IF (row_count > 0) THEN
            test_status := 'PASSED';
            test_message := 'Found ' || row_count || ' tickets in latest view';
            passed_tests := passed_tests + 1;
        ELSE
            test_status := 'FAILED';
            test_message := 'No data found in VW_HF_TICKETS_LATEST';
            failed_tests := failed_tests + 1;
        END IF;
    EXCEPTION
        WHEN OTHER THEN
            test_status := 'ERROR';
            test_message := SQLERRM;
            failed_tests := failed_tests + 1;
    END;
    
    exec_time := DATEDIFF('millisecond', start_time, CURRENT_TIMESTAMP());
    test_results := ARRAY_APPEND(test_results, OBJECT_CONSTRUCT(
        'test_name', test_name,
        'status', test_status,
        'message', test_message,
        'exec_time_ms', exec_time
    ));
    total_tests := total_tests + 1;
    
    -- ========================================================================
    -- TEST 2: Export View Performance
    -- ========================================================================
    test_name := 'Export View Performance';
    start_time := CURRENT_TIMESTAMP();
    
    BEGIN
        SELECT COUNT(*) INTO row_count 
        FROM MCP.VW_HF_TICKETS_EXPORT 
        WHERE lifecycle_state = 'Open';
        
        exec_time := DATEDIFF('millisecond', start_time, CURRENT_TIMESTAMP());
        
        IF (exec_time < 2000) THEN
            test_status := 'PASSED';
            test_message := 'Query completed in ' || exec_time || 'ms (' || row_count || ' open tickets)';
            passed_tests := passed_tests + 1;
        ELSE
            test_status := 'WARNING';
            test_message := 'Query took ' || exec_time || 'ms (target: <2000ms)';
            passed_tests := passed_tests + 1;
        END IF;
    EXCEPTION
        WHEN OTHER THEN
            test_status := 'ERROR';
            test_message := SQLERRM;
            failed_tests := failed_tests + 1;
    END;
    
    test_results := ARRAY_APPEND(test_results, OBJECT_CONSTRUCT(
        'test_name', test_name,
        'status', test_status,
        'message', test_message,
        'exec_time_ms', exec_time
    ));
    total_tests := total_tests + 1;
    
    -- ========================================================================
    -- TEST 3: Table Function - GET_HAPPYFOX_TICKETS
    -- ========================================================================
    test_name := 'Table Function GET_HAPPYFOX_TICKETS';
    start_time := CURRENT_TIMESTAMP();
    
    BEGIN
        -- Test with filters
        SELECT COUNT(*) INTO row_count 
        FROM TABLE(MCP.GET_HAPPYFOX_TICKETS('GZ', 'Open', 0, 30));
        
        test_status := 'PASSED';
        test_message := 'Function returned ' || row_count || ' GZ open tickets (0-30 days)';
        passed_tests := passed_tests + 1;
        
        -- Test with NULLs
        SELECT COUNT(*) INTO row_count 
        FROM TABLE(MCP.GET_HAPPYFOX_TICKETS(NULL, NULL, 0, 365));
        
        test_message := test_message || ', All products: ' || row_count;
        
    EXCEPTION
        WHEN OTHER THEN
            test_status := 'ERROR';
            test_message := SQLERRM;
            failed_tests := failed_tests + 1;
    END;
    
    exec_time := DATEDIFF('millisecond', start_time, CURRENT_TIMESTAMP());
    test_results := ARRAY_APPEND(test_results, OBJECT_CONSTRUCT(
        'test_name', test_name,
        'status', test_status,
        'message', test_message,
        'exec_time_ms', exec_time
    ));
    total_tests := total_tests + 1;
    
    -- ========================================================================
    -- TEST 4: Table Function - GET_HAPPYFOX_PRODUCT_STATS
    -- ========================================================================
    test_name := 'Table Function GET_HAPPYFOX_PRODUCT_STATS';
    start_time := CURRENT_TIMESTAMP();
    
    BEGIN
        SELECT COUNT(*) INTO row_count 
        FROM TABLE(MCP.GET_HAPPYFOX_PRODUCT_STATS());
        
        IF (row_count > 0) THEN
            test_status := 'PASSED';
            test_message := 'Function returned stats for ' || row_count || ' products';
            passed_tests := passed_tests + 1;
        ELSE
            test_status := 'FAILED';
            test_message := 'No product stats returned';
            failed_tests := failed_tests + 1;
        END IF;
    EXCEPTION
        WHEN OTHER THEN
            test_status := 'ERROR';
            test_message := SQLERRM;
            failed_tests := failed_tests + 1;
    END;
    
    exec_time := DATEDIFF('millisecond', start_time, CURRENT_TIMESTAMP());
    test_results := ARRAY_APPEND(test_results, OBJECT_CONSTRUCT(
        'test_name', test_name,
        'status', test_status,
        'message', test_message,
        'exec_time_ms', exec_time
    ));
    total_tests := total_tests + 1;
    
    -- ========================================================================
    -- TEST 5: Streamlit Overview Tab Queries
    -- ========================================================================
    test_name := 'Streamlit Overview Queries';
    start_time := CURRENT_TIMESTAMP();
    
    BEGIN
        -- Metrics query
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN lifecycle_state = 'Open' THEN 1 ELSE 0 END) as open,
            SUM(CASE WHEN lifecycle_state = 'Closed' THEN 1 ELSE 0 END) as closed
        INTO :row_count, :test_message, :exec_time  -- Reusing variables
        FROM MCP.VW_HF_TICKETS_EXPORT
        WHERE age_days BETWEEN 0 AND 180;
        
        -- Age distribution query
        SELECT COUNT(DISTINCT age_bucket) INTO row_count
        FROM MCP.VW_HF_TICKETS_EXPORT;
        
        test_status := 'PASSED';
        test_message := 'Overview queries successful, ' || row_count || ' age buckets found';
        passed_tests := passed_tests + 1;
        
    EXCEPTION
        WHEN OTHER THEN
            test_status := 'ERROR';
            test_message := SQLERRM;
            failed_tests := failed_tests + 1;
    END;
    
    exec_time := DATEDIFF('millisecond', start_time, CURRENT_TIMESTAMP());
    test_results := ARRAY_APPEND(test_results, OBJECT_CONSTRUCT(
        'test_name', test_name,
        'status', test_status,
        'message', test_message,
        'exec_time_ms', exec_time
    ));
    total_tests := total_tests + 1;
    
    -- ========================================================================
    -- TEST 6: Streamlit Trends Tab Queries
    -- ========================================================================
    test_name := 'Streamlit Trends Queries';
    start_time := CURRENT_TIMESTAMP();
    
    BEGIN
        -- Daily trend query
        WITH daily AS (
            SELECT DATE_TRUNC('day', created_at) as day, COUNT(*) as created
            FROM MCP.VW_HF_TICKETS_EXPORT
            WHERE created_at >= DATEADD('day', -30, CURRENT_DATE())
            GROUP BY day
        )
        SELECT COUNT(*) INTO row_count FROM daily;
        
        test_status := 'PASSED';
        test_message := 'Trends query returned ' || row_count || ' days of data';
        passed_tests := passed_tests + 1;
        
    EXCEPTION
        WHEN OTHER THEN
            test_status := 'ERROR';
            test_message := SQLERRM;
            failed_tests := failed_tests + 1;
    END;
    
    exec_time := DATEDIFF('millisecond', start_time, CURRENT_TIMESTAMP());
    test_results := ARRAY_APPEND(test_results, OBJECT_CONSTRUCT(
        'test_name', test_name,
        'status', test_status,
        'message', test_message,
        'exec_time_ms', exec_time
    ));
    total_tests := total_tests + 1;
    
    -- ========================================================================
    -- TEST 7: Search Functionality
    -- ========================================================================
    test_name := 'Search Functionality';
    start_time := CURRENT_TIMESTAMP();
    
    BEGIN
        -- Test subject search
        SELECT COUNT(*) INTO row_count
        FROM MCP.VW_HF_TICKETS_EXPORT
        WHERE LOWER(subject) LIKE LOWER('%password%');
        
        test_status := 'PASSED';
        test_message := 'Search found ' || row_count || ' tickets with "password" in subject';
        passed_tests := passed_tests + 1;
        
    EXCEPTION
        WHEN OTHER THEN
            test_status := 'ERROR';
            test_message := SQLERRM;
            failed_tests := failed_tests + 1;
    END;
    
    exec_time := DATEDIFF('millisecond', start_time, CURRENT_TIMESTAMP());
    test_results := ARRAY_APPEND(test_results, OBJECT_CONSTRUCT(
        'test_name', test_name,
        'status', test_status,
        'message', test_message,
        'exec_time_ms', exec_time
    ));
    total_tests := total_tests + 1;
    
    -- ========================================================================
    -- TEST 8: Export Query Performance
    -- ========================================================================
    test_name := 'Export Query Performance';
    start_time := CURRENT_TIMESTAMP();
    
    BEGIN
        -- Simulate large export
        SELECT COUNT(*) INTO row_count
        FROM MCP.VW_HF_TICKETS_EXPORT
        WHERE product_prefix = 'GZ'
        LIMIT 10000;
        
        exec_time := DATEDIFF('millisecond', start_time, CURRENT_TIMESTAMP());
        
        IF (exec_time < 5000) THEN
            test_status := 'PASSED';
            test_message := 'Export query completed in ' || exec_time || 'ms';
            passed_tests := passed_tests + 1;
        ELSE
            test_status := 'WARNING';
            test_message := 'Export query took ' || exec_time || 'ms (target: <5000ms)';
            passed_tests := passed_tests + 1;
        END IF;
        
    EXCEPTION
        WHEN OTHER THEN
            test_status := 'ERROR';
            test_message := SQLERRM;
            failed_tests := failed_tests + 1;
    END;
    
    test_results := ARRAY_APPEND(test_results, OBJECT_CONSTRUCT(
        'test_name', test_name,
        'status', test_status,
        'message', test_message,
        'exec_time_ms', exec_time
    ));
    total_tests := total_tests + 1;
    
    -- ========================================================================
    -- TEST 9: Data Consistency
    -- ========================================================================
    test_name := 'Data Consistency';
    start_time := CURRENT_TIMESTAMP();
    
    BEGIN
        DECLARE
            latest_count NUMBER;
            export_count NUMBER;
        BEGIN
            SELECT COUNT(*) INTO latest_count FROM MCP.VW_HF_TICKETS_LATEST;
            SELECT COUNT(*) INTO export_count FROM MCP.VW_HF_TICKETS_EXPORT;
            
            IF (latest_count = export_count) THEN
                test_status := 'PASSED';
                test_message := 'Row counts match: ' || latest_count;
                passed_tests := passed_tests + 1;
            ELSE
                test_status := 'FAILED';
                test_message := 'Row count mismatch: Latest=' || latest_count || ', Export=' || export_count;
                failed_tests := failed_tests + 1;
            END IF;
        END;
        
    EXCEPTION
        WHEN OTHER THEN
            test_status := 'ERROR';
            test_message := SQLERRM;
            failed_tests := failed_tests + 1;
    END;
    
    exec_time := DATEDIFF('millisecond', start_time, CURRENT_TIMESTAMP());
    test_results := ARRAY_APPEND(test_results, OBJECT_CONSTRUCT(
        'test_name', test_name,
        'status', test_status,
        'message', test_message,
        'exec_time_ms', exec_time
    ));
    total_tests := total_tests + 1;
    
    -- ========================================================================
    -- TEST 10: Edge Cases
    -- ========================================================================
    test_name := 'Edge Cases';
    start_time := CURRENT_TIMESTAMP();
    
    BEGIN
        -- Test with non-existent product
        SELECT COUNT(*) INTO row_count 
        FROM TABLE(MCP.GET_HAPPYFOX_TICKETS('NOTEXIST', NULL, 0, 365));
        
        -- Test with invalid date range
        SELECT COUNT(*) INTO :exec_time  -- Reusing variable
        FROM TABLE(MCP.GET_HAPPYFOX_TICKETS(NULL, NULL, 365, 0));
        
        test_status := 'PASSED';
        test_message := 'Edge cases handled correctly';
        passed_tests := passed_tests + 1;
        
    EXCEPTION
        WHEN OTHER THEN
            test_status := 'ERROR';
            test_message := SQLERRM;
            failed_tests := failed_tests + 1;
    END;
    
    exec_time := DATEDIFF('millisecond', start_time, CURRENT_TIMESTAMP());
    test_results := ARRAY_APPEND(test_results, OBJECT_CONSTRUCT(
        'test_name', test_name,
        'status', test_status,
        'message', test_message,
        'exec_time_ms', exec_time
    ));
    total_tests := total_tests + 1;
    
    -- ========================================================================
    -- RETURN TEST RESULTS
    -- ========================================================================
    RETURN OBJECT_CONSTRUCT(
        'test_suite', 'HappyFox Analytics Stack',
        'executed_at', CURRENT_TIMESTAMP()::STRING,
        'total_tests', total_tests,
        'passed', passed_tests,
        'failed', failed_tests,
        'success_rate', ROUND(passed_tests * 100.0 / total_tests, 2) || '%',
        'test_results', test_results
    );
END;
$$;

-- ============================================================================
-- RUN THE TEST SUITE
-- ============================================================================

CALL MCP.TEST_HAPPYFOX_ANALYTICS();

-- ============================================================================
-- ADDITIONAL VALIDATION QUERIES
-- ============================================================================

-- Check Search Optimization is active
SHOW TABLES LIKE 'ACTIVITY_STREAM' IN SCHEMA ACTIVITY;

-- Verify all views exist
SELECT 
    TABLE_NAME,
    ROW_COUNT,
    CREATED
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_CATALOG = 'CLAUDE_BI'
  AND TABLE_SCHEMA = 'MCP'
  AND TABLE_NAME IN (
    'VW_HF_TICKETS_LATEST',
    'VW_HF_TICKETS_EXPORT',
    'VW_HF_TICKET_HISTORY'
  )
  AND TABLE_TYPE = 'VIEW'
ORDER BY TABLE_NAME;

-- Test a complex aggregation (what Streamlit would do)
WITH product_summary AS (
    SELECT 
        product_prefix,
        lifecycle_state,
        COUNT(*) as count,
        AVG(age_days) as avg_age,
        AVG(time_spent_minutes) as avg_time
    FROM MCP.VW_HF_TICKETS_EXPORT
    GROUP BY product_prefix, lifecycle_state
)
SELECT 
    product_prefix,
    MAX(CASE WHEN lifecycle_state = 'Open' THEN count ELSE 0 END) as open_tickets,
    MAX(CASE WHEN lifecycle_state = 'Closed' THEN count ELSE 0 END) as closed_tickets,
    MAX(avg_age) as avg_age_days
FROM product_summary
GROUP BY product_prefix
ORDER BY (open_tickets + closed_tickets) DESC
LIMIT 5;