-- ============================================================================
-- CI/CD Integration for Dynamic Agent Self-Orientation System
-- Pre-deployment validation and change detection
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE CLAUDE_BI;
USE SCHEMA MCP;

-- ============================================================================
-- JSON Comment Validation for CI/CD
-- ============================================================================

CREATE OR REPLACE PROCEDURE MCP.VALIDATE_JSON_COMMENTS(file_pattern STRING DEFAULT '%.sql')
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
  validation_errors ARRAY DEFAULT ARRAY_CONSTRUCT();
  view_errors NUMBER DEFAULT 0;
  proc_errors NUMBER DEFAULT 0;
BEGIN
  -- Clear previous validation errors for this run
  DELETE FROM MCP.METADATA_ERRORS WHERE captured_at < DATEADD('hour', -1, CURRENT_TIMESTAMP());
  
  -- Validate view comments
  INSERT INTO MCP.METADATA_ERRORS (object_name, object_type, reason, invalid_json)
  SELECT 
    TABLE_SCHEMA || '.' || TABLE_NAME,
    'VIEW',
    'Invalid JSON in COMMENT',
    COMMENT
  FROM INFORMATION_SCHEMA.VIEWS 
  WHERE TABLE_SCHEMA IN ('APP', 'MCP', 'ACTIVITY', 'SECURITY')
    AND COMMENT IS NOT NULL 
    AND TRY_PARSE_JSON(COMMENT) IS NULL;
  
  GET DIAGNOSTICS view_errors = ROW_COUNT;
  
  -- Validate procedure comments
  INSERT INTO MCP.METADATA_ERRORS (object_name, object_type, reason, invalid_json)
  SELECT 
    PROCEDURE_SCHEMA || '.' || PROCEDURE_NAME,
    'PROCEDURE',
    'Invalid JSON in COMMENT',
    COMMENT
  FROM INFORMATION_SCHEMA.PROCEDURES 
  WHERE PROCEDURE_SCHEMA = 'MCP'
    AND COMMENT IS NOT NULL 
    AND TRY_PARSE_JSON(COMMENT) IS NULL;
  
  GET DIAGNOSTICS proc_errors = ROW_COUNT;
  
  -- Check for required JSON fields in views
  LET missing_subject_views ARRAY := (
    SELECT ARRAY_AGG(TABLE_SCHEMA || '.' || TABLE_NAME)
    FROM INFORMATION_SCHEMA.VIEWS 
    WHERE TABLE_SCHEMA IN ('APP', 'MCP', 'ACTIVITY', 'SECURITY')
      AND (TRY_PARSE_JSON(COMMENT):subject IS NULL OR TRY_PARSE_JSON(COMMENT):subject = '')
      AND COMMENT IS NOT NULL
  );
  
  -- Check for required JSON fields in procedures
  LET missing_intent_procs ARRAY := (
    SELECT ARRAY_AGG(PROCEDURE_SCHEMA || '.' || PROCEDURE_NAME)
    FROM INFORMATION_SCHEMA.PROCEDURES 
    WHERE PROCEDURE_SCHEMA = 'MCP'
      AND (TRY_PARSE_JSON(COMMENT):intent IS NULL OR TRY_PARSE_JSON(COMMENT):intent = '')
      AND COMMENT IS NOT NULL
  );
  
  RETURN OBJECT_CONSTRUCT(
    'validation_status', IFF(:view_errors + :proc_errors = 0, 'PASSED', 'FAILED'),
    'total_errors', :view_errors + :proc_errors,
    'view_json_errors', :view_errors,
    'procedure_json_errors', :proc_errors,
    'missing_subject_views', COALESCE(:missing_subject_views, ARRAY_CONSTRUCT()),
    'missing_intent_procedures', COALESCE(:missing_intent_procs, ARRAY_CONSTRUCT()),
    'recommendations', IFF(
      :view_errors + :proc_errors = 0,
      ARRAY_CONSTRUCT('All JSON comments are valid'),
      ARRAY_CONSTRUCT(
        'Fix JSON syntax errors before deployment',
        'Ensure all views have "subject" field',
        'Ensure all procedures have "intent" field'
      )
    )
  );
END;
$$;

-- ============================================================================
-- Registry Diff Detection for CI/CD
-- ============================================================================

CREATE OR REPLACE PROCEDURE MCP.GENERATE_REGISTRY_DIFF()
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
  diff_report VARIANT;
  new_subjects ARRAY;
  removed_subjects ARRAY;
  modified_subjects ARRAY;
  new_workflows ARRAY;
  removed_workflows ARRAY;
  modified_workflows ARRAY;
BEGIN
  -- Refresh catalog to get latest state
  CALL MCP.REFRESH_CATALOG();
  
  -- Build V2 registry for comparison
  CALL MCP.REBUILD_REGISTRY('V2');
  
  -- Find new subjects
  SELECT ARRAY_AGG(subject) INTO :new_subjects
  FROM MCP.SUBJECTS_V2 
  WHERE subject NOT IN (SELECT subject FROM MCP.SUBJECTS WHERE active = TRUE);
  
  -- Find removed subjects
  SELECT ARRAY_AGG(subject) INTO :removed_subjects
  FROM MCP.SUBJECTS 
  WHERE active = TRUE 
    AND subject NOT IN (SELECT subject FROM MCP.SUBJECTS_V2);
  
  -- Find modified subjects (different metadata)
  SELECT ARRAY_AGG(s2.subject) INTO :modified_subjects
  FROM MCP.SUBJECTS s1
  JOIN MCP.SUBJECTS_V2 s2 ON s1.subject = s2.subject
  WHERE s1.active = TRUE
    AND (s1.title != s2.title OR s1.default_view != s2.default_view OR s1.tags != s2.tags);
  
  -- Find new workflows
  SELECT ARRAY_AGG(intent) INTO :new_workflows
  FROM MCP.WORKFLOWS_V2 
  WHERE intent NOT IN (SELECT intent FROM MCP.WORKFLOWS WHERE active = TRUE);
  
  -- Find removed workflows
  SELECT ARRAY_AGG(intent) INTO :removed_workflows
  FROM MCP.WORKFLOWS 
  WHERE active = TRUE 
    AND intent NOT IN (SELECT intent FROM MCP.WORKFLOWS_V2);
  
  -- Find modified workflows
  SELECT ARRAY_AGG(w2.intent) INTO :modified_workflows
  FROM MCP.WORKFLOWS w1
  JOIN MCP.WORKFLOWS_V2 w2 ON w1.intent = w2.intent
  WHERE w1.active = TRUE
    AND (w1.title != w2.title OR w1.inputs != w2.inputs OR w1.outputs != w2.outputs);
  
  -- Create diff report
  SET diff_report = OBJECT_CONSTRUCT(
    'timestamp', CURRENT_TIMESTAMP(),
    'has_changes', (
      ARRAY_SIZE(COALESCE(:new_subjects, ARRAY_CONSTRUCT())) +
      ARRAY_SIZE(COALESCE(:removed_subjects, ARRAY_CONSTRUCT())) +
      ARRAY_SIZE(COALESCE(:modified_subjects, ARRAY_CONSTRUCT())) +
      ARRAY_SIZE(COALESCE(:new_workflows, ARRAY_CONSTRUCT())) +
      ARRAY_SIZE(COALESCE(:removed_workflows, ARRAY_CONSTRUCT())) +
      ARRAY_SIZE(COALESCE(:modified_workflows, ARRAY_CONSTRUCT()))
    ) > 0,
    'subjects', OBJECT_CONSTRUCT(
      'new', COALESCE(:new_subjects, ARRAY_CONSTRUCT()),
      'removed', COALESCE(:removed_subjects, ARRAY_CONSTRUCT()),
      'modified', COALESCE(:modified_subjects, ARRAY_CONSTRUCT())
    ),
    'workflows', OBJECT_CONSTRUCT(
      'new', COALESCE(:new_workflows, ARRAY_CONSTRUCT()),
      'removed', COALESCE(:removed_workflows, ARRAY_CONSTRUCT()),
      'modified', COALESCE(:modified_workflows, ARRAY_CONSTRUCT())
    ),
    'change_summary', OBJECT_CONSTRUCT(
      'total_changes', (
        ARRAY_SIZE(COALESCE(:new_subjects, ARRAY_CONSTRUCT())) +
        ARRAY_SIZE(COALESCE(:removed_subjects, ARRAY_CONSTRUCT())) +
        ARRAY_SIZE(COALESCE(:modified_subjects, ARRAY_CONSTRUCT())) +
        ARRAY_SIZE(COALESCE(:new_workflows, ARRAY_CONSTRUCT())) +
        ARRAY_SIZE(COALESCE(:removed_workflows, ARRAY_CONSTRUCT())) +
        ARRAY_SIZE(COALESCE(:modified_workflows, ARRAY_CONSTRUCT()))
      ),
      'breaking_changes', ARRAY_SIZE(COALESCE(:removed_subjects, ARRAY_CONSTRUCT())) + 
                          ARRAY_SIZE(COALESCE(:removed_workflows, ARRAY_CONSTRUCT())),
      'requires_approval', (
        ARRAY_SIZE(COALESCE(:removed_subjects, ARRAY_CONSTRUCT())) > 0 OR
        ARRAY_SIZE(COALESCE(:removed_workflows, ARRAY_CONSTRUCT())) > 0
      )
    )
  );
  
  -- Log the diff for audit
  INSERT INTO MCP.CATALOG_DELTAS (kind, object_name, change_type, delta)
  VALUES ('REGISTRY', 'FULL_DIFF', 'COMPARISON', :diff_report);
  
  RETURN :diff_report;
END;
$$;

-- ============================================================================
-- Pre-Deployment Safety Checks
-- ============================================================================

CREATE OR REPLACE PROCEDURE MCP.PRE_DEPLOYMENT_CHECKS()
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
  safety_report VARIANT;
  json_validation VARIANT;
  registry_diff VARIANT;
  critical_issues NUMBER DEFAULT 0;
  warnings NUMBER DEFAULT 0;
BEGIN
  -- Run JSON validation
  CALL MCP.VALIDATE_JSON_COMMENTS() INTO :json_validation;
  
  -- Generate registry diff
  CALL MCP.GENERATE_REGISTRY_DIFF() INTO :registry_diff;
  
  -- Count critical issues
  IF (:json_validation:total_errors::NUMBER > 0) THEN 
    SET critical_issues = :critical_issues + 1; 
  END IF;
  
  IF (:registry_diff:change_summary:breaking_changes::NUMBER > 0) THEN 
    SET critical_issues = :critical_issues + 1; 
  END IF;
  
  -- Count warnings
  IF (:registry_diff:change_summary:total_changes::NUMBER > 10) THEN 
    SET warnings = :warnings + 1; 
  END IF;
  
  -- Build safety report
  SET safety_report = OBJECT_CONSTRUCT(
    'deployment_status', IFF(:critical_issues = 0, 'APPROVED', 'BLOCKED'),
    'critical_issues', :critical_issues,
    'warnings', :warnings,
    'checks', OBJECT_CONSTRUCT(
      'json_validation', :json_validation,
      'registry_diff', :registry_diff
    ),
    'next_steps', IFF(
      :critical_issues = 0,
      ARRAY_CONSTRUCT(
        'Deployment approved',
        'Run: CALL MCP.FLIP_REGISTRY() to activate changes'
      ),
      ARRAY_CONSTRUCT(
        'Fix JSON validation errors',
        'Review breaking changes',
        'Get approval for removed objects'
      )
    )
  );
  
  -- Log pre-deployment check
  INSERT INTO MCP.AGENT_TELEMETRY (
    agent_id, intent, kind, outcome, error_message, execution_ms, role_name
  ) VALUES (
    'ci_cd', 'pre_deployment_check', 'validation',
    IFF(:critical_issues = 0, 'approved', 'blocked'),
    'Critical: ' || :critical_issues || ', Warnings: ' || :warnings,
    0, CURRENT_ROLE()
  );
  
  RETURN :safety_report;
END;
$$;

-- ============================================================================
-- Post-Deployment Verification
-- ============================================================================

CREATE OR REPLACE PROCEDURE MCP.POST_DEPLOYMENT_VERIFICATION()
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
  verification_report VARIANT;
  health_check VARIANT;
  test_results VARIANT;
  deployment_success BOOLEAN DEFAULT TRUE;
BEGIN
  -- Run health check
  CALL MCP.PRODUCTION_HEALTH_CHECK() INTO :health_check;
  
  -- Run agent workflow tests
  CALL MCP.TEST_AGENT_WORKFLOW() INTO :test_results;
  
  -- Check if deployment was successful
  IF (:health_check:overall_status::STRING != 'HEALTHY') THEN
    SET deployment_success = FALSE;
  END IF;
  
  IF (:test_results:overall_status::STRING != 'ALL_PASSED') THEN
    SET deployment_success = FALSE;
  END IF;
  
  -- Build verification report
  SET verification_report = OBJECT_CONSTRUCT(
    'deployment_verified', :deployment_success,
    'timestamp', CURRENT_TIMESTAMP(),
    'health_check', :health_check,
    'test_results', :test_results,
    'actions_required', IFF(
      :deployment_success,
      ARRAY_CONSTRUCT('Deployment successful - system ready'),
      ARRAY_CONSTRUCT(
        'Investigate health check failures',
        'Review failed agent tests',
        'Consider rollback if critical issues found'
      )
    )
  );
  
  -- Log verification results
  INSERT INTO MCP.AGENT_TELEMETRY (
    agent_id, intent, kind, outcome, error_message, execution_ms, role_name
  ) VALUES (
    'ci_cd', 'post_deployment_verification', 'validation',
    IFF(:deployment_success, 'success', 'failure'),
    'Health: ' || :health_check:overall_status::STRING || 
    ', Tests: ' || :test_results:overall_status::STRING,
    0, CURRENT_ROLE()
  );
  
  RETURN :verification_report;
END;
$$;

-- ============================================================================
-- Rollback Procedure
-- ============================================================================

CREATE OR REPLACE PROCEDURE MCP.EMERGENCY_ROLLBACK(reason STRING DEFAULT 'Emergency rollback')
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
  rollback_result VARIANT;
  backup_available BOOLEAN DEFAULT FALSE;
BEGIN
  -- Check if backup tables exist
  SELECT COUNT(*) > 0 INTO :backup_available
  FROM INFORMATION_SCHEMA.TABLES 
  WHERE TABLE_SCHEMA = 'MCP' AND TABLE_NAME = 'SUBJECTS_BACKUP';
  
  IF (NOT :backup_available) THEN
    RETURN OBJECT_CONSTRUCT(
      'rollback_status', 'FAILED',
      'error', 'No backup tables available for rollback',
      'action_required', 'Manual intervention needed'
    );
  END IF;
  
  -- Perform rollback
  BEGIN
    -- Restore from backup
    DELETE FROM MCP.SUBJECTS;
    DELETE FROM MCP.SUBJECT_VIEWS;
    DELETE FROM MCP.WORKFLOWS;
    
    INSERT INTO MCP.SUBJECTS SELECT * FROM MCP.SUBJECTS_BACKUP;
    INSERT INTO MCP.SUBJECT_VIEWS SELECT * FROM MCP.SUBJECT_VIEWS_BACKUP;
    INSERT INTO MCP.WORKFLOWS SELECT * FROM MCP.WORKFLOWS_BACKUP;
    
    -- Clear primer cache to force refresh with old data
    DELETE FROM MCP.PRIMER_CACHE;
    
    -- Log rollback
    INSERT INTO MCP.AGENT_TELEMETRY (
      agent_id, intent, kind, outcome, error_message, execution_ms, role_name
    ) VALUES (
      'emergency_rollback', 'system_rollback', 'maintenance', 'success',
      :reason, 0, CURRENT_ROLE()
    );
    
    SET rollback_result = OBJECT_CONSTRUCT(
      'rollback_status', 'SUCCESS',
      'reason', :reason,
      'timestamp', CURRENT_TIMESTAMP(),
      'restored_objects', OBJECT_CONSTRUCT(
        'subjects', (SELECT COUNT(*) FROM MCP.SUBJECTS),
        'subject_views', (SELECT COUNT(*) FROM MCP.SUBJECT_VIEWS),
        'workflows', (SELECT COUNT(*) FROM MCP.WORKFLOWS)
      )
    );
    
  EXCEPTION
    WHEN OTHER THEN
      SET rollback_result = OBJECT_CONSTRUCT(
        'rollback_status', 'FAILED',
        'error', SQLERRM,
        'sqlcode', SQLCODE,
        'action_required', 'Manual database restore needed'
      );
  END;
  
  RETURN :rollback_result;
END;
$$;

-- ============================================================================
-- CI/CD Webhook Integration Points
-- ============================================================================

-- View for CI/CD systems to query deployment status
CREATE OR REPLACE VIEW MCP.VW_CI_CD_STATUS AS
SELECT 
  'last_deployment' AS status_type,
  (SELECT MAX(ts) FROM MCP.AGENT_TELEMETRY WHERE intent = 'pre_deployment_check') AS last_check,
  (SELECT outcome FROM MCP.AGENT_TELEMETRY WHERE intent = 'pre_deployment_check' ORDER BY ts DESC LIMIT 1) AS last_result,
  (SELECT COUNT(*) FROM MCP.METADATA_ERRORS WHERE resolved = FALSE) AS pending_errors
UNION ALL
SELECT 
  'registry_health',
  CURRENT_TIMESTAMP(),
  IFF((SELECT COUNT(*) FROM MCP.SUBJECTS) > 0, 'healthy', 'empty'),
  (SELECT COUNT(*) FROM MCP.SUBJECTS)
UNION ALL
SELECT 
  'system_health',
  CURRENT_TIMESTAMP(),
  IFF((SELECT COUNT(*) FROM MCP.VW_ALLOWLIST_READS) > 0, 'operational', 'down'),
  (SELECT COUNT(*) FROM MCP.VW_ALLOWLIST_READS);

-- ============================================================================
-- Example CI/CD Pipeline Usage
-- ============================================================================

/*
-- Example GitHub Actions workflow steps:

1. Pre-deployment validation:
   CALL MCP.PRE_DEPLOYMENT_CHECKS();
   -- If result.deployment_status != 'APPROVED', fail the build

2. Deploy changes:
   -- Apply SQL changes
   CALL MCP.REFRESH_CATALOG();
   CALL MCP.REBUILD_REGISTRY('V2');

3. Activate deployment:
   CALL MCP.FLIP_REGISTRY();

4. Post-deployment verification:
   CALL MCP.POST_DEPLOYMENT_VERIFICATION();
   -- If result.deployment_verified != TRUE, trigger rollback

5. Emergency rollback (if needed):
   CALL MCP.EMERGENCY_ROLLBACK('Failed post-deployment verification');
*/

-- Create a simple deployment orchestrator
CREATE OR REPLACE PROCEDURE MCP.DEPLOY_WITH_VALIDATION()
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
  pre_check VARIANT;
  deploy_result VARIANT;
  post_check VARIANT;
  overall_success BOOLEAN DEFAULT FALSE;
BEGIN
  -- Pre-deployment checks
  CALL MCP.PRE_DEPLOYMENT_CHECKS() INTO :pre_check;
  
  IF (:pre_check:deployment_status::STRING != 'APPROVED') THEN
    RETURN OBJECT_CONSTRUCT(
      'deployment_status', 'BLOCKED',
      'stage', 'pre_deployment',
      'details', :pre_check
    );
  END IF;
  
  -- Deploy (flip registry)
  CALL MCP.FLIP_REGISTRY() INTO :deploy_result;
  
  -- Post-deployment verification
  CALL MCP.POST_DEPLOYMENT_VERIFICATION() INTO :post_check;
  
  SET overall_success = :post_check:deployment_verified::BOOLEAN;
  
  -- Auto-rollback on failure
  IF (NOT :overall_success) THEN
    LET rollback_result VARIANT;
    CALL MCP.EMERGENCY_ROLLBACK('Auto-rollback due to verification failure') INTO :rollback_result;
    
    RETURN OBJECT_CONSTRUCT(
      'deployment_status', 'FAILED_AND_ROLLED_BACK',
      'stage', 'post_deployment',
      'rollback_result', :rollback_result,
      'verification_details', :post_check
    );
  END IF;
  
  RETURN OBJECT_CONSTRUCT(
    'deployment_status', 'SUCCESS',
    'stage', 'completed',
    'pre_check', :pre_check,
    'deploy_result', :deploy_result,
    'post_check', :post_check
  );
END;
$$;