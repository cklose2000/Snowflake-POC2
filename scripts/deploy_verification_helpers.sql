-- =====================================================
-- DEPLOYMENT VERIFICATION HELPER PROCEDURES
-- Simplifies compliance with CLAUDE.md verification laws
-- =====================================================

-- =====================================================
-- CORE STATE MANAGEMENT HELPERS
-- =====================================================

CREATE OR REPLACE PROCEDURE MCP.CAPTURE_STATE()
RETURNS VARIANT
LANGUAGE SQL
AS
$$
BEGIN
    LET event_count INTEGER;
    LET last_event_time TIMESTAMP;
    LET state_hash STRING;
    -- Capture current state following Rule 2: The Verification Law
    SELECT 
        COUNT(*),
        MAX(occurred_at),
        MD5(COUNT(*) || COALESCE(MAX(occurred_at)::STRING, 'empty'))
    INTO event_count, last_event_time, state_hash
    FROM CLAUDE_BI.ACTIVITY.EVENTS;
    
    -- Store state capture as event for audit trail
    INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS
    SELECT 
        OBJECT_CONSTRUCT(
            'event_id', UUID_STRING(),
            'action', 'verification.state.captured',
            'actor_id', CURRENT_USER(),
            'attributes', OBJECT_CONSTRUCT(
                'event_count', event_count,
                'last_event_time', last_event_time,
                'state_hash', state_hash,
                'captured_at', CURRENT_TIMESTAMP()
            ),
            'occurred_at', CURRENT_TIMESTAMP()
        ),
        'VERIFICATION',
        CURRENT_TIMESTAMP();
    
    RETURN OBJECT_CONSTRUCT(
        'event_count', event_count,
        'last_event_time', last_event_time,
        'state_hash', state_hash,
        'captured_at', CURRENT_TIMESTAMP()
    );
END;
$$;

CREATE OR REPLACE PROCEDURE MCP.VERIFY_DEPLOYMENT(
    before_state VARIANT,
    deployment_id STRING DEFAULT NULL
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
BEGIN
    LET after_count INTEGER;
    LET after_time TIMESTAMP;
    LET after_hash STRING;
    LET state_changed BOOLEAN;
    LET events_created INTEGER;
    LET verification_passed BOOLEAN;
    LET failure_reason STRING := NULL;
    deployment_id := COALESCE(deployment_id, UUID_STRING());
    
    -- Capture after state
    SELECT 
        COUNT(*),
        MAX(occurred_at),
        MD5(COUNT(*) || COALESCE(MAX(occurred_at)::STRING, 'empty'))
    INTO after_count, after_time, after_hash
    FROM CLAUDE_BI.ACTIVITY.EVENTS;
    
    -- Calculate changes
    events_created := after_count - before_state:event_count::INTEGER;
    state_changed := (after_hash != before_state:state_hash::STRING);
    
    -- Determine verification status
    IF NOT state_changed AND events_created = 0 THEN
        verification_passed := TRUE; -- No changes expected
    ELSIF state_changed AND events_created > 0 THEN
        verification_passed := TRUE; -- Changes detected as expected
    ELSIF NOT state_changed AND events_created > 0 THEN
        verification_passed := FALSE;
        failure_reason := 'Events created but state hash unchanged';
    ELSE
        verification_passed := FALSE;
        failure_reason := 'Unexpected state change pattern';
    END IF;
    
    -- Log verification result
    CALL MCP.LOG_VERIFICATION_RESULT(
        deployment_id,
        CURRENT_USER(),
        before_state:state_hash::STRING,
        after_hash,
        events_created,
        verification_passed,
        failure_reason
    );
    
    -- Show proof events if changes occurred
    LET proof_query STRING := '';
    IF events_created > 0 THEN
        proof_query := 'SELECT * FROM CLAUDE_BI.ACTIVITY.EVENTS WHERE occurred_at > ''' || 
                      before_state:last_event_time::STRING || ''' ORDER BY occurred_at DESC LIMIT 10';
    END IF;
    
    RETURN OBJECT_CONSTRUCT(
        'deployment_id', deployment_id,
        'verification_status', IFF(verification_passed, 'PASSED', 'FAILED'),
        'before_state', OBJECT_CONSTRUCT(
            'count', before_state:event_count,
            'hash', before_state:state_hash
        ),
        'after_state', OBJECT_CONSTRUCT(
            'count', after_count,
            'hash', after_hash
        ),
        'changes', OBJECT_CONSTRUCT(
            'state_changed', state_changed,
            'events_created', events_created
        ),
        'proof_query', proof_query,
        'failure_reason', failure_reason,
        'verified_at', CURRENT_TIMESTAMP()
    );
END;
$$;

-- =====================================================
-- ROLLBACK PREPARATION HELPERS
-- =====================================================

CREATE OR REPLACE PROCEDURE MCP.CREATE_ROLLBACK_EVENT(
    object_type STRING,
    object_name STRING,
    deployment_id STRING DEFAULT NULL
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
BEGIN
    LET rollback_ddl STRING;
    LET rollback_event_id STRING;
    deployment_id := COALESCE(deployment_id, UUID_STRING());
    rollback_event_id := UUID_STRING();
    
    -- Get current DDL for rollback (Rule 5: The Rollback Readiness Law)
    BEGIN
        SELECT GET_DDL(object_type, object_name) INTO rollback_ddl;
    EXCEPTION
        WHEN OTHER THEN
            -- Object doesn't exist yet (new creation)
            rollback_ddl := 'DROP ' || object_type || ' IF EXISTS ' || object_name;
    END;
    
    -- Store rollback information as event
    INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS
    SELECT 
        OBJECT_CONSTRUCT(
            'event_id', rollback_event_id,
            'action', 'deployment.rollback.prepared',
            'actor_id', CURRENT_USER(),
            'attributes', OBJECT_CONSTRUCT(
                'deployment_id', deployment_id,
                'object_type', object_type,
                'object_name', object_name,
                'rollback_ddl', rollback_ddl,
                'prepared_at', CURRENT_TIMESTAMP()
            ),
            'occurred_at', CURRENT_TIMESTAMP()
        ),
        'SYSTEM',
        CURRENT_TIMESTAMP();
    
    -- Log telemetry
    CALL MCP.LOG_ROLLBACK_PREPARATION(
        deployment_id,
        CURRENT_USER(),
        object_name,
        rollback_ddl
    );
    
    RETURN OBJECT_CONSTRUCT(
        'success', TRUE,
        'deployment_id', deployment_id,
        'rollback_event_id', rollback_event_id,
        'object_name', object_name,
        'rollback_prepared', TRUE
    );
END;
$$;

CREATE OR REPLACE PROCEDURE MCP.EXECUTE_ROLLBACK(
    deployment_id STRING
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
BEGIN
    LET rollback_ddl STRING;
    LET rollback_count INTEGER := 0;
    LET rollback_errors ARRAY := ARRAY_CONSTRUCT();
    LET rollback_cursor CURSOR FOR
        SELECT 
            event_data:attributes:rollback_ddl::STRING as ddl,
            event_data:attributes:object_name::STRING as obj_name
        FROM CLAUDE_BI.ACTIVITY.EVENTS
        WHERE event_data:action::STRING = 'deployment.rollback.prepared'
          AND event_data:attributes:deployment_id::STRING = deployment_id
        ORDER BY event_data:occurred_at::TIMESTAMP DESC;
    FOR record IN rollback_cursor DO
        BEGIN
            EXECUTE IMMEDIATE record.ddl;
            rollback_count := rollback_count + 1;
        EXCEPTION
            WHEN OTHER THEN
                rollback_errors := ARRAY_APPEND(rollback_errors, 
                    OBJECT_CONSTRUCT('object', record.obj_name, 'error', SQLERRM));
        END;
    END FOR;
    
    -- Log rollback execution
    INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS
    SELECT 
        OBJECT_CONSTRUCT(
            'event_id', UUID_STRING(),
            'action', 'deployment.rollback.executed',
            'actor_id', CURRENT_USER(),
            'attributes', OBJECT_CONSTRUCT(
                'deployment_id', deployment_id,
                'objects_rolled_back', rollback_count,
                'errors', rollback_errors,
                'executed_at', CURRENT_TIMESTAMP()
            ),
            'occurred_at', CURRENT_TIMESTAMP()
        ),
        'SYSTEM',
        CURRENT_TIMESTAMP();
    
    RETURN OBJECT_CONSTRUCT(
        'success', ARRAY_SIZE(rollback_errors) = 0,
        'deployment_id', deployment_id,
        'objects_rolled_back', rollback_count,
        'errors', rollback_errors
    );
END;
$$;

-- =====================================================
-- COMPLIANCE CHECKING HELPERS
-- =====================================================

CREATE OR REPLACE PROCEDURE MCP.CHECK_COMPLIANCE(
    deployment_plan VARIANT
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
BEGIN
    LET compliance_checks ARRAY := ARRAY_CONSTRUCT();
    LET violations ARRAY := ARRAY_CONSTRUCT();
    LET is_compliant BOOLEAN := TRUE;
    -- Check Rule 1: Two-Table Law
    IF deployment_plan:creates_table::BOOLEAN = TRUE THEN
        violations := ARRAY_APPEND(violations, 'RULE_1_VIOLATION: Attempting to create new table');
        is_compliant := FALSE;
    END IF;
    compliance_checks := ARRAY_APPEND(compliance_checks, 
        OBJECT_CONSTRUCT('rule', 'TWO_TABLE_LAW', 'passed', NOT deployment_plan:creates_table::BOOLEAN));
    
    -- Check Rule 2: Verification Law
    IF deployment_plan:has_verification::BOOLEAN != TRUE THEN
        violations := ARRAY_APPEND(violations, 'RULE_2_VIOLATION: Missing verification steps');
        is_compliant := FALSE;
    END IF;
    compliance_checks := ARRAY_APPEND(compliance_checks,
        OBJECT_CONSTRUCT('rule', 'VERIFICATION_LAW', 'passed', deployment_plan:has_verification::BOOLEAN));
    
    -- Check Rule 3: Error Honesty Law
    IF deployment_plan:handles_errors::BOOLEAN != TRUE THEN
        violations := ARRAY_APPEND(violations, 'RULE_3_VIOLATION: No error handling specified');
        is_compliant := FALSE;
    END IF;
    compliance_checks := ARRAY_APPEND(compliance_checks,
        OBJECT_CONSTRUCT('rule', 'ERROR_HONESTY_LAW', 'passed', deployment_plan:handles_errors::BOOLEAN));
    
    -- Check Rule 5: Rollback Readiness Law
    IF deployment_plan:has_rollback::BOOLEAN != TRUE THEN
        violations := ARRAY_APPEND(violations, 'RULE_5_VIOLATION: No rollback capability');
        is_compliant := FALSE;
    END IF;
    compliance_checks := ARRAY_APPEND(compliance_checks,
        OBJECT_CONSTRUCT('rule', 'ROLLBACK_READINESS_LAW', 'passed', deployment_plan:has_rollback::BOOLEAN));
    
    -- Log compliance check
    INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS
    SELECT 
        OBJECT_CONSTRUCT(
            'event_id', UUID_STRING(),
            'action', 'verification.compliance.checked',
            'actor_id', CURRENT_USER(),
            'attributes', OBJECT_CONSTRUCT(
                'deployment_plan', deployment_plan,
                'compliance_checks', compliance_checks,
                'violations', violations,
                'is_compliant', is_compliant,
                'checked_at', CURRENT_TIMESTAMP()
            ),
            'occurred_at', CURRENT_TIMESTAMP()
        ),
        'VERIFICATION',
        CURRENT_TIMESTAMP();
    
    RETURN OBJECT_CONSTRUCT(
        'is_compliant', is_compliant,
        'compliance_checks', compliance_checks,
        'violations', violations,
        'recommendation', IFF(is_compliant, 'Safe to proceed', 'Fix violations before deployment')
    );
END;
$$;

-- =====================================================
-- SIMPLIFIED DEPLOYMENT WRAPPER
-- =====================================================

CREATE OR REPLACE PROCEDURE MCP.SAFE_DEPLOY(
    deployment_statements ARRAY,
    deployment_description STRING DEFAULT 'Deployment'
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
BEGIN
    LET deployment_id STRING;
    LET before_state VARIANT;
    LET deployment_results ARRAY := ARRAY_CONSTRUCT();
    LET deployment_success BOOLEAN := TRUE;
    LET error_msg STRING;
    LET stmt_index INTEGER := 0;
    deployment_id := UUID_STRING();
    
    -- Step 1: Log deployment start
    CALL MCP.LOG_DEPLOYMENT_ATTEMPT(
        deployment_id,
        CURRENT_USER(),
        'BATCH_DEPLOYMENT',
        deployment_description
    );
    
    -- Step 2: Capture before state
    CALL MCP.CAPTURE_STATE() INTO before_state;
    
    -- Step 3: Execute deployment statements
    FOR stmt_index IN 0 TO ARRAY_SIZE(deployment_statements) - 1 DO
        BEGIN
            EXECUTE IMMEDIATE deployment_statements[stmt_index];
            deployment_results := ARRAY_APPEND(deployment_results,
                OBJECT_CONSTRUCT('statement', stmt_index, 'success', TRUE));
        EXCEPTION
            WHEN OTHER THEN
                error_msg := SQLERRM;
                deployment_success := FALSE;
                deployment_results := ARRAY_APPEND(deployment_results,
                    OBJECT_CONSTRUCT('statement', stmt_index, 'success', FALSE, 'error', error_msg));
                
                -- Log error (Rule 3: Error Honesty Law)
                CALL MCP.LOG_ERROR_OCCURRENCE(
                    deployment_id,
                    CURRENT_USER(),
                    error_msg,
                    TRUE,
                    FALSE -- We stop on error
                );
                
                -- Stop on first error
                BREAK;
        END;
    END FOR;
    
    -- Step 4: Verify deployment
    LET verification_result VARIANT := CALL MCP.VERIFY_DEPLOYMENT(before_state, deployment_id);
    
    -- Step 5: Return comprehensive result
    RETURN OBJECT_CONSTRUCT(
        'deployment_id', deployment_id,
        'success', deployment_success AND verification_result:verification_status::STRING = 'PASSED',
        'description', deployment_description,
        'statements_executed', ARRAY_SIZE(deployment_results),
        'deployment_results', deployment_results,
        'verification', verification_result,
        'recommendation', IFF(deployment_success, 
            'Deployment completed - check verification',
            'Deployment failed - consider rollback'
        )
    );
END;
$$;

-- =====================================================
-- DEPLOYMENT TEMPLATE GENERATOR
-- =====================================================

CREATE OR REPLACE PROCEDURE MCP.GENERATE_COMPLIANT_TEMPLATE(
    object_type STRING,
    object_name STRING
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
BEGIN
    LET template_sql STRING;
    -- Generate a template that follows all verification laws
    template_sql := '-- COMPLIANT DEPLOYMENT TEMPLATE\n' ||
                   '-- Generated: ' || CURRENT_TIMESTAMP()::STRING || '\n\n' ||
                   '-- Step 1: Declare variables\n' ||
                   'DECLARE\n' ||
                   '    deployment_id STRING DEFAULT UUID_STRING();\n' ||
                   '    before_state VARIANT;\n' ||
                   '    verification_result VARIANT;\n' ||
                   'BEGIN\n' ||
                   '    -- Step 2: Capture before state (MANDATORY - Rule 2)\n' ||
                   '    CALL MCP.CAPTURE_STATE() INTO before_state;\n\n' ||
                   '    -- Step 3: Create rollback event (MANDATORY - Rule 5)\n' ||
                   '    CALL MCP.CREATE_ROLLBACK_EVENT(''' || object_type || ''', ''' || object_name || ''', deployment_id);\n\n' ||
                   '    -- Step 4: Perform deployment\n' ||
                   '    BEGIN\n' ||
                   '        -- YOUR DDL HERE\n' ||
                   '        CREATE OR REPLACE ' || object_type || ' ' || object_name || ' AS\n' ||
                   '        -- Implementation goes here\n' ||
                   '        ;\n' ||
                   '    EXCEPTION\n' ||
                   '        WHEN OTHER THEN\n' ||
                   '            -- Step 5: Handle errors honestly (MANDATORY - Rule 3)\n' ||
                   '            CALL MCP.LOG_ERROR_OCCURRENCE(deployment_id, CURRENT_USER(), SQLERRM, TRUE, FALSE);\n' ||
                   '            RETURN OBJECT_CONSTRUCT(''success'', FALSE, ''error'', SQLERRM);\n' ||
                   '    END;\n\n' ||
                   '    -- Step 6: Verify deployment (MANDATORY - Rule 2)\n' ||
                   '    CALL MCP.VERIFY_DEPLOYMENT(before_state, deployment_id) INTO verification_result;\n\n' ||
                   '    -- Step 7: Return result with proof\n' ||
                   '    RETURN verification_result;\n' ||
                   'END;';
    
    RETURN OBJECT_CONSTRUCT(
        'template_type', 'COMPLIANT_DEPLOYMENT',
        'object_type', object_type,
        'object_name', object_name,
        'template_sql', template_sql,
        'instructions', ARRAY_CONSTRUCT(
            'Replace YOUR DDL HERE with actual implementation',
            'This template ensures compliance with all verification laws',
            'State verification is mandatory and automatic',
            'Rollback capability is built in',
            'Errors are handled and reported honestly'
        )
    );
END;
$$;

-- =====================================================
-- QUICK VERIFICATION OUTPUT FORMATTER
-- =====================================================

CREATE OR REPLACE PROCEDURE MCP.FORMAT_VERIFICATION_OUTPUT(
    before_state VARIANT,
    after_state VARIANT,
    events_created INTEGER DEFAULT 0
)
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- Format output according to CLAUDE.md requirements
    RETURN 'DEPLOYMENT VERIFICATION:\n' ||
           '- Before State Hash: ' || before_state:state_hash::STRING || '\n' ||
           '- After State Hash: ' || after_state:state_hash::STRING || '\n' ||
           '- Events Created: ' || events_created::STRING || '\n' ||
           '- Proof Events Shown: ' || IFF(events_created > 0, 'yes', 'no') || '\n' ||
           '- Success: ' || IFF(before_state:state_hash != after_state:state_hash, 'true', 'false');
END;
$$;

-- =====================================================
-- Initialize helper system
-- =====================================================
INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS
SELECT 
    OBJECT_CONSTRUCT(
        'event_id', UUID_STRING(),
        'action', 'verification.helpers.initialized',
        'actor_id', 'SYSTEM',
        'attributes', OBJECT_CONSTRUCT(
            'helper_procedures', ARRAY_CONSTRUCT(
                'MCP.CAPTURE_STATE',
                'MCP.VERIFY_DEPLOYMENT',
                'MCP.CREATE_ROLLBACK_EVENT',
                'MCP.EXECUTE_ROLLBACK',
                'MCP.CHECK_COMPLIANCE',
                'MCP.SAFE_DEPLOY',
                'MCP.GENERATE_COMPLIANT_TEMPLATE',
                'MCP.FORMAT_VERIFICATION_OUTPUT'
            ),
            'version', '1.0.0',
            'initialized_at', CURRENT_TIMESTAMP()
        ),
        'occurred_at', CURRENT_TIMESTAMP()
    ),
    'SYSTEM',
    CURRENT_TIMESTAMP();