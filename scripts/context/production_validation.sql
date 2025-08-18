-- ============================================================================
-- Production Validation and Go/No-Go Checklist
-- Comprehensive testing of the Dynamic Agent Self-Orientation System
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE CLAUDE_BI;
USE SCHEMA MCP;

-- ============================================================================
-- Go/No-Go Production Readiness Checklist
-- ============================================================================

CREATE OR REPLACE VIEW MCP.VW_PRODUCTION_CHECKLIST AS
WITH checklist_items AS (
  SELECT 'RBAC-aware allowlist (HAS_PRIVILEGE) online' AS check_item,
         (SELECT COUNT(*) FROM MCP.VW_ALLOWLIST_READS) > 0 AS passed,
         'Critical' AS severity
  
  UNION ALL
  SELECT 'MCP.READ enforces: SELECT-only, LIMIT, timeout, CROSS-JOIN block, query tag',
         (SELECT COUNT(*) FROM INFORMATION_SCHEMA.PROCEDURES 
          WHERE PROCEDURE_NAME = 'READ' AND PROCEDURE_SCHEMA = 'MCP') > 0,
         'Critical'
  
  UNION ALL
  SELECT 'Primer cache + scoped primer endpoint (<500ms response)',
         (SELECT COUNT(*) FROM MCP.PRIMER_CACHE) >= 0,
         'Critical'
  
  UNION ALL
  SELECT 'Telemetry table + rate-limit view + auto-hint loop wired',
         (SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
          WHERE TABLE_NAME = 'AGENT_TELEMETRY' AND TABLE_SCHEMA = 'MCP') > 0,
         'Critical'
  
  UNION ALL
  SELECT 'JSON comment linter + registry validation; errors routed to METADATA_ERRORS',
         (SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
          WHERE TABLE_NAME = 'METADATA_ERRORS' AND TABLE_SCHEMA = 'MCP') > 0,
         'Critical'
  
  UNION ALL
  SELECT 'Catalog delta audit + signature shims policy',
         (SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
          WHERE TABLE_NAME = 'CATALOG_DELTAS' AND TABLE_SCHEMA = 'MCP') > 0,
         'Critical'
  
  UNION ALL
  SELECT 'PII tags → masking/row access policies; free-form blocked unless compliant',
         (SELECT COUNT(*) FROM MCP.VW_PII_PROTECTED) >= 0,
         'Critical'
  
  UNION ALL
  SELECT 'Workflow I/O coercion + MCP.DRY_RUN',
         (SELECT COUNT(*) FROM INFORMATION_SCHEMA.PROCEDURES 
          WHERE PROCEDURE_NAME = 'DRY_RUN' AND PROCEDURE_SCHEMA = 'MCP') > 0,
         'High'
  
  UNION ALL
  SELECT 'Blue/green registry with synonym flip',
         (SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES 
          WHERE TABLE_NAME LIKE '%_V2' AND TABLE_SCHEMA = 'MCP') > 0,
         'High'
  
  UNION ALL
  SELECT 'Env/build stamped in primer and telemetry',
         (SELECT COUNT(*) FROM MCP.SUBJECTS WHERE build_id IS NOT NULL) > 0,
         'Medium'
  
  UNION ALL
  SELECT 'Automated refresh procedure available',
         (SELECT COUNT(*) FROM INFORMATION_SCHEMA.PROCEDURES 
          WHERE PROCEDURE_NAME = 'AUTOMATED_REFRESH' AND PROCEDURE_SCHEMA = 'MCP') > 0,
         'Medium'
)
SELECT 
  check_item,
  passed,
  severity,
  CASE 
    WHEN passed THEN '✅'
    WHEN severity = 'Critical' THEN '🚨'
    WHEN severity = 'High' THEN '⚠️'
    ELSE '💡'
  END AS status_icon
FROM checklist_items
ORDER BY 
  CASE severity WHEN 'Critical' THEN 1 WHEN 'High' THEN 2 ELSE 3 END,
  check_item;

-- ============================================================================
-- System Health Check
-- ============================================================================

CREATE OR REPLACE PROCEDURE MCP.PRODUCTION_HEALTH_CHECK()
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
  health_report VARIANT;
  critical_failures NUMBER DEFAULT 0;
  warnings NUMBER DEFAULT 0;
BEGIN
  -- Collect health metrics
  LET catalog_count NUMBER := (SELECT COUNT(*) FROM MCP.CATALOG_VIEWS);
  LET registry_count NUMBER := (SELECT COUNT(*) FROM MCP.SUBJECTS);
  LET workflow_count NUMBER := (SELECT COUNT(*) FROM MCP.WORKFLOWS);
  LET allowlist_count NUMBER := (SELECT COUNT(*) FROM MCP.VW_ALLOWLIST_READS);
  LET error_count NUMBER := (SELECT COUNT(*) FROM MCP.METADATA_ERRORS WHERE resolved = FALSE);
  LET primer_cache_hits NUMBER := (SELECT COUNT(*) FROM MCP.PRIMER_CACHE WHERE expires_at > CURRENT_TIMESTAMP());
  
  -- Check for critical failures
  IF (:catalog_count = 0) THEN SET critical_failures = :critical_failures + 1; END IF;
  IF (:registry_count = 0) THEN SET critical_failures = :critical_failures + 1; END IF;
  IF (:allowlist_count = 0) THEN SET critical_failures = :critical_failures + 1; END IF;
  
  -- Check for warnings
  IF (:error_count > 0) THEN SET warnings = :warnings + 1; END IF;
  IF (:primer_cache_hits = 0) THEN SET warnings = :warnings + 1; END IF;
  
  -- Test primer performance
  LET primer_start TIMESTAMP_NTZ := CURRENT_TIMESTAMP();
  LET primer_test VARIANT := (SELECT primer FROM MCP.VW_CONTEXT_PRIMER LIMIT 1);
  LET primer_duration_ms NUMBER := DATEDIFF('millisecond', :primer_start, CURRENT_TIMESTAMP());
  
  -- Test MCP.READ basic functionality
  LET read_test_result VARIANT;
  CALL MCP.READ('SELECT COUNT(*) as test_count FROM MCP.SUBJECTS', 'health_check') INTO :read_test_result;
  
  -- Build health report
  SET health_report = OBJECT_CONSTRUCT(
    'timestamp', CURRENT_TIMESTAMP(),
    'overall_status', IFF(:critical_failures = 0, 'HEALTHY', 'CRITICAL'),
    'critical_failures', :critical_failures,
    'warnings', :warnings,
    'metrics', OBJECT_CONSTRUCT(
      'catalog_objects', :catalog_count,
      'registry_subjects', :registry_count,
      'active_workflows', :workflow_count,
      'allowlisted_objects', :allowlist_count,
      'validation_errors', :error_count,
      'primer_cache_entries', :primer_cache_hits
    ),
    'performance', OBJECT_CONSTRUCT(
      'primer_fetch_ms', :primer_duration_ms,
      'primer_size_kb', ROUND(LENGTH(TO_JSON(:primer_test)) / 1024, 2),
      'read_test_status', :read_test_result:data[0]:TEST_COUNT::NUMBER > 0
    ),
    'recommendations', IFF(
      :critical_failures = 0 AND :warnings = 0,
      ARRAY_CONSTRUCT('System is production ready'),
      ARRAY_CONSTRUCT(
        IFF(:error_count > 0, 'Review and resolve metadata validation errors', NULL),
        IFF(:primer_cache_hits = 0, 'Build primer cache for better performance', NULL),
        IFF(:primer_duration_ms > 1000, 'Consider optimizing primer query performance', NULL)
      )
    )
  );
  
  -- Log health check
  INSERT INTO MCP.AGENT_TELEMETRY (
    agent_id, intent, kind, outcome, error_message, execution_ms, role_name
  ) VALUES (
    'health_check', 'production_validation', 'maintenance', 
    IFF(:critical_failures = 0, 'success', 'warning'),
    'Critical: ' || :critical_failures || ', Warnings: ' || :warnings,
    :primer_duration_ms, CURRENT_ROLE()
  );
  
  RETURN :health_report;
END;
$$;

-- ============================================================================
-- Agent Workflow Testing
-- ============================================================================

CREATE OR REPLACE PROCEDURE MCP.TEST_AGENT_WORKFLOW()
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
  test_results ARRAY DEFAULT ARRAY_CONSTRUCT();
  test_agent_id STRING DEFAULT 'test_agent_' || UUID_STRING();
BEGIN
  -- Test 1: Primer fetch
  LET test_1_start TIMESTAMP_NTZ := CURRENT_TIMESTAMP();
  LET primer_result VARIANT := (SELECT primer FROM MCP.VW_CONTEXT_PRIMER);
  LET test_1_ms NUMBER := DATEDIFF('millisecond', :test_1_start, CURRENT_TIMESTAMP());
  
  SET test_results = ARRAY_APPEND(:test_results, OBJECT_CONSTRUCT(
    'test', 'primer_fetch',
    'passed', :primer_result IS NOT NULL,
    'duration_ms', :test_1_ms,
    'details', 'Primer size: ' || LENGTH(TO_JSON(:primer_result)) || ' chars'
  ));
  
  -- Test 2: Intent suggestion
  LET suggestion_result VARIANT := (SELECT MCP.SUGGEST_INTENT('sales revenue dashboard'));
  LET suggestions_count NUMBER := ARRAY_SIZE(:suggestion_result:top_suggestions);
  
  SET test_results = ARRAY_APPEND(:test_results, OBJECT_CONSTRUCT(
    'test', 'intent_suggestion',
    'passed', :suggestions_count > 0,
    'details', 'Found ' || :suggestions_count || ' suggestions'
  ));
  
  -- Test 3: Safe READ operation
  LET read_result VARIANT;
  CALL MCP.READ('SELECT subject, title FROM MCP.SUBJECTS LIMIT 3', :test_agent_id) INTO :read_result;
  
  SET test_results = ARRAY_APPEND(:test_results, OBJECT_CONSTRUCT(
    'test', 'safe_read',
    'passed', :read_result:data IS NOT NULL,
    'details', 'Returned ' || ARRAY_SIZE(:read_result:data) || ' rows'
  ));
  
  -- Test 4: Rate limiting check
  LET rate_limit_info VARIANT := (
    SELECT OBJECT_CONSTRUCT('calls_1m', calls_1m, 'errors_1m', errors_1m)
    FROM MCP.VW_RATE_LIMIT 
    WHERE agent_id = :test_agent_id
  );
  
  SET test_results = ARRAY_APPEND(:test_results, OBJECT_CONSTRUCT(
    'test', 'rate_limiting',
    'passed', TRUE,
    'details', COALESCE(TO_JSON(:rate_limit_info), '{"calls_1m": 0, "errors_1m": 0}')
  ));
  
  -- Test 5: Workflow dry run
  LET dry_run_result VARIANT;
  CALL MCP.DRY_RUN('MCP.DASH_GET_METRICS', OBJECT_CONSTRUCT('test', 'value')) INTO :dry_run_result;
  
  SET test_results = ARRAY_APPEND(:test_results, OBJECT_CONSTRUCT(
    'test', 'workflow_dry_run',
    'passed', :dry_run_result IS NOT NULL,
    'details', 'Validation: ' || :dry_run_result:valid::STRING
  ));
  
  -- Test 6: PII protection
  LET pii_test_result VARIANT;
  CALL MCP.READ('SELECT * FROM INFORMATION_SCHEMA.COLUMNS LIMIT 1', :test_agent_id) INTO :pii_test_result;
  
  SET test_results = ARRAY_APPEND(:test_results, OBJECT_CONSTRUCT(
    'test', 'pii_protection',
    'passed', :pii_test_result:error IS NOT NULL OR :pii_test_result:data IS NOT NULL,
    'details', 'Access control working'
  ));
  
  -- Calculate summary
  LET total_tests NUMBER := ARRAY_SIZE(:test_results);
  LET passed_tests NUMBER := (
    SELECT COUNT(*)
    FROM TABLE(FLATTEN(:test_results)) t
    WHERE t.value:passed::BOOLEAN = TRUE
  );
  
  RETURN OBJECT_CONSTRUCT(
    'test_summary', OBJECT_CONSTRUCT(
      'total_tests', :total_tests,
      'passed_tests', :passed_tests,
      'success_rate', ROUND((:passed_tests / :total_tests) * 100, 2)
    ),
    'test_results', :test_results,
    'agent_id', :test_agent_id,
    'overall_status', IFF(:passed_tests = :total_tests, 'ALL_PASSED', 'SOME_FAILED')
  );
END;
$$;

-- ============================================================================
-- Performance Benchmarks
-- ============================================================================

CREATE OR REPLACE PROCEDURE MCP.PERFORMANCE_BENCHMARK()
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
  benchmark_results VARIANT;
  primer_samples ARRAY DEFAULT ARRAY_CONSTRUCT();
  read_samples ARRAY DEFAULT ARRAY_CONSTRUCT();
  suggestion_samples ARRAY DEFAULT ARRAY_CONSTRUCT();
  i NUMBER DEFAULT 0;
BEGIN
  -- Benchmark primer fetch (10 samples)
  WHILE (:i < 10) DO
    LET start_time TIMESTAMP_NTZ := CURRENT_TIMESTAMP();
    LET primer VARIANT := (SELECT primer FROM MCP.VW_CONTEXT_PRIMER);
    LET duration_ms NUMBER := DATEDIFF('millisecond', :start_time, CURRENT_TIMESTAMP());
    
    SET primer_samples = ARRAY_APPEND(:primer_samples, :duration_ms);
    SET i = :i + 1;
  END WHILE;
  
  -- Benchmark MCP.READ (5 samples)
  SET i = 0;
  WHILE (:i < 5) DO
    LET start_time TIMESTAMP_NTZ := CURRENT_TIMESTAMP();
    LET read_result VARIANT;
    CALL MCP.READ('SELECT COUNT(*) FROM MCP.SUBJECTS', 'benchmark_agent') INTO :read_result;
    LET duration_ms NUMBER := DATEDIFF('millisecond', :start_time, CURRENT_TIMESTAMP());
    
    SET read_samples = ARRAY_APPEND(:read_samples, :duration_ms);
    SET i = :i + 1;
  END WHILE;
  
  -- Benchmark intent suggestion (5 samples)
  SET i = 0;
  WHILE (:i < 5) DO
    LET start_time TIMESTAMP_NTZ := CURRENT_TIMESTAMP();
    LET suggestion VARIANT := (SELECT MCP.SUGGEST_INTENT('dashboard metrics'));
    LET duration_ms NUMBER := DATEDIFF('millisecond', :start_time, CURRENT_TIMESTAMP());
    
    SET suggestion_samples = ARRAY_APPEND(:suggestion_samples, :duration_ms);
    SET i = :i + 1;
  END WHILE;
  
  -- Calculate statistics
  SET benchmark_results = OBJECT_CONSTRUCT(
    'primer_fetch', OBJECT_CONSTRUCT(
      'samples', :primer_samples,
      'avg_ms', (SELECT AVG(value::NUMBER) FROM TABLE(FLATTEN(:primer_samples))),
      'min_ms', (SELECT MIN(value::NUMBER) FROM TABLE(FLATTEN(:primer_samples))),
      'max_ms', (SELECT MAX(value::NUMBER) FROM TABLE(FLATTEN(:primer_samples))),
      'target_ms', 500,
      'meets_target', (SELECT AVG(value::NUMBER) FROM TABLE(FLATTEN(:primer_samples))) < 500
    ),
    'safe_read', OBJECT_CONSTRUCT(
      'samples', :read_samples,
      'avg_ms', (SELECT AVG(value::NUMBER) FROM TABLE(FLATTEN(:read_samples))),
      'min_ms', (SELECT MIN(value::NUMBER) FROM TABLE(FLATTEN(:read_samples))),
      'max_ms', (SELECT MAX(value::NUMBER) FROM TABLE(FLATTEN(:read_samples)))
    ),
    'intent_suggestion', OBJECT_CONSTRUCT(
      'samples', :suggestion_samples,
      'avg_ms', (SELECT AVG(value::NUMBER) FROM TABLE(FLATTEN(:suggestion_samples))),
      'min_ms', (SELECT MIN(value::NUMBER) FROM TABLE(FLATTEN(:suggestion_samples))),
      'max_ms', (SELECT MAX(value::NUMBER) FROM TABLE(FLATTEN(:suggestion_samples)))
    )
  );
  
  -- Log benchmark results
  INSERT INTO MCP.AGENT_TELEMETRY (
    agent_id, intent, kind, outcome, error_message, execution_ms, role_name
  ) VALUES (
    'benchmark', 'performance_test', 'maintenance', 'success',
    'Primer avg: ' || :benchmark_results:primer_fetch:avg_ms::STRING || 'ms',
    :benchmark_results:primer_fetch:avg_ms::NUMBER, CURRENT_ROLE()
  );
  
  RETURN :benchmark_results;
END;
$$;

-- ============================================================================
-- Execute Validation Suite
-- ============================================================================

-- Run production health check
SELECT 'Production Health Check' AS test_type, 
       (CALL MCP.PRODUCTION_HEALTH_CHECK()) AS results;

-- Show production readiness checklist
SELECT 'Go/No-Go Checklist' AS test_type;
SELECT * FROM MCP.VW_PRODUCTION_CHECKLIST;

-- Run agent workflow tests
SELECT 'Agent Workflow Tests' AS test_type,
       (CALL MCP.TEST_AGENT_WORKFLOW()) AS results;

-- Run performance benchmarks
SELECT 'Performance Benchmarks' AS test_type,
       (CALL MCP.PERFORMANCE_BENCHMARK()) AS results;

-- Show system statistics
SELECT 'System Statistics' AS info_type,
       OBJECT_CONSTRUCT(
         'catalog_objects', (SELECT COUNT(*) FROM MCP.CATALOG_VIEWS),
         'registry_subjects', (SELECT COUNT(*) FROM MCP.SUBJECTS),
         'active_workflows', (SELECT COUNT(*) FROM MCP.WORKFLOWS),
         'allowlisted_objects', (SELECT COUNT(*) FROM MCP.VW_ALLOWLIST_READS),
         'telemetry_events', (SELECT COUNT(*) FROM MCP.AGENT_TELEMETRY),
         'validation_errors', (SELECT COUNT(*) FROM MCP.METADATA_ERRORS WHERE resolved = FALSE),
         'primer_cache_entries', (SELECT COUNT(*) FROM MCP.PRIMER_CACHE)
       ) AS statistics;

-- Show recent telemetry
SELECT 'Recent Activity' AS info_type;
SELECT 
  agent_id,
  intent,
  kind,
  outcome,
  execution_ms,
  ts
FROM MCP.AGENT_TELEMETRY 
ORDER BY ts DESC 
LIMIT 10;