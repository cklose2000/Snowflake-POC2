-- =====================================================
-- VERIFICATION TELEMETRY INFRASTRUCTURE
-- Tracks compliance with CLAUDE.md verification laws
-- =====================================================

-- =====================================================
-- TELEMETRY TABLE (FOLLOWING TWO-TABLE LAW)
-- All telemetry stored as events, not new tables!
-- =====================================================

-- Create view for agent telemetry (over events, not a new table!)
CREATE OR REPLACE VIEW MCP.AGENT_TELEMETRY AS
SELECT 
    event_data:event_id::STRING as telemetry_id,
    event_data:action::STRING as action,
    event_data:actor_id::STRING as agent_id,
    event_data:attributes:deployment_id::STRING as deployment_id,
    event_data:attributes:verification_status::STRING as verification_status,
    event_data:attributes:before_hash::STRING as before_hash,
    event_data:attributes:after_hash::STRING as after_hash,
    event_data:attributes:state_changed::BOOLEAN as state_changed,
    event_data:attributes:events_created::INTEGER as events_created,
    event_data:attributes:rollback_prepared::BOOLEAN as rollback_prepared,
    event_data:attributes:error_reported::STRING as error_reported,
    event_data:attributes:rule_violations::ARRAY as rule_violations,
    event_data:attributes:compliance_score::NUMBER as compliance_score,
    event_data:occurred_at::TIMESTAMP as occurred_at,
    source,
    ingested_at
FROM CLAUDE_BI.ACTIVITY.EVENTS
WHERE event_data:action::STRING LIKE 'telemetry.%'
   OR event_data:action::STRING LIKE 'deployment.%'
   OR event_data:action::STRING LIKE 'verification.%';

COMMENT ON VIEW MCP.AGENT_TELEMETRY IS 
$${"type": "telemetry_view", "purpose": "Track verification compliance", "version": "1.0.0"}$$;

-- =====================================================
-- TELEMETRY CAPTURE PROCEDURES
-- =====================================================

CREATE OR REPLACE PROCEDURE MCP.LOG_DEPLOYMENT_ATTEMPT(
    deployment_id STRING,
    agent_id STRING,
    operation_type STRING,
    target_object STRING
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
BEGIN
    -- Log deployment attempt as event
    INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS
    SELECT 
        OBJECT_CONSTRUCT(
            'event_id', UUID_STRING(),
            'action', 'telemetry.deployment.started',
            'actor_id', agent_id,
            'attributes', OBJECT_CONSTRUCT(
                'deployment_id', deployment_id,
                'operation_type', operation_type,
                'target_object', target_object,
                'timestamp', CURRENT_TIMESTAMP()
            ),
            'occurred_at', CURRENT_TIMESTAMP()
        ),
        'TELEMETRY',
        CURRENT_TIMESTAMP();
    
    RETURN OBJECT_CONSTRUCT(
        'success', TRUE,
        'deployment_id', deployment_id,
        'logged_at', CURRENT_TIMESTAMP()
    );
END;
$$;

CREATE OR REPLACE PROCEDURE MCP.LOG_VERIFICATION_RESULT(
    deployment_id STRING,
    agent_id STRING,
    before_hash STRING,
    after_hash STRING,
    events_created INTEGER,
    verification_passed BOOLEAN,
    failure_reason STRING DEFAULT NULL
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
BEGIN
    LET compliance_score NUMBER;
    LET rule_violations ARRAY;
    rule_violations := ARRAY_CONSTRUCT();
    compliance_score := 0;
    
    -- Check verification compliance
    IF before_hash IS NULL THEN
        rule_violations := ARRAY_APPEND(rule_violations, 'MISSING_BEFORE_STATE');
    ELSE
        compliance_score := compliance_score + 25;
    END IF;
    
    IF after_hash IS NULL THEN
        rule_violations := ARRAY_APPEND(rule_violations, 'MISSING_AFTER_STATE');
    ELSE
        compliance_score := compliance_score + 25;
    END IF;
    
    IF before_hash = after_hash AND events_created > 0 THEN
        rule_violations := ARRAY_APPEND(rule_violations, 'STATE_HASH_UNCHANGED');
    ELSIF before_hash != after_hash THEN
        compliance_score := compliance_score + 25;
    END IF;
    
    IF verification_passed THEN
        compliance_score := compliance_score + 25;
    END IF;
    
    -- Log verification result as event
    INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS
    SELECT 
        OBJECT_CONSTRUCT(
            'event_id', UUID_STRING(),
            'action', 'telemetry.verification.completed',
            'actor_id', agent_id,
            'attributes', OBJECT_CONSTRUCT(
                'deployment_id', deployment_id,
                'verification_status', IFF(verification_passed, 'PASSED', 'FAILED'),
                'before_hash', before_hash,
                'after_hash', after_hash,
                'state_changed', (before_hash != after_hash),
                'events_created', events_created,
                'compliance_score', compliance_score,
                'rule_violations', rule_violations,
                'failure_reason', failure_reason,
                'timestamp', CURRENT_TIMESTAMP()
            ),
            'occurred_at', CURRENT_TIMESTAMP()
        ),
        'TELEMETRY',
        CURRENT_TIMESTAMP();
    
    RETURN OBJECT_CONSTRUCT(
        'success', TRUE,
        'deployment_id', deployment_id,
        'verification_passed', verification_passed,
        'compliance_score', compliance_score,
        'violations', rule_violations
    );
END;
$$;

CREATE OR REPLACE PROCEDURE MCP.LOG_ROLLBACK_PREPARATION(
    deployment_id STRING,
    agent_id STRING,
    object_name STRING,
    rollback_ddl STRING
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
BEGIN
    -- Log rollback preparation as event
    INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS
    SELECT 
        OBJECT_CONSTRUCT(
            'event_id', UUID_STRING(),
            'action', 'telemetry.rollback.prepared',
            'actor_id', agent_id,
            'attributes', OBJECT_CONSTRUCT(
                'deployment_id', deployment_id,
                'object_name', object_name,
                'rollback_ddl_length', LENGTH(rollback_ddl),
                'rollback_prepared', TRUE,
                'timestamp', CURRENT_TIMESTAMP()
            ),
            'occurred_at', CURRENT_TIMESTAMP()
        ),
        'TELEMETRY',
        CURRENT_TIMESTAMP();
    
    RETURN OBJECT_CONSTRUCT(
        'success', TRUE,
        'deployment_id', deployment_id,
        'rollback_prepared', TRUE
    );
END;
$$;

CREATE OR REPLACE PROCEDURE MCP.LOG_ERROR_OCCURRENCE(
    deployment_id STRING,
    agent_id STRING,
    error_message STRING,
    error_handled BOOLEAN,
    continued_after_error BOOLEAN
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
BEGIN
    -- Log error occurrence as event
    INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS
    SELECT 
        OBJECT_CONSTRUCT(
            'event_id', UUID_STRING(),
            'action', 'telemetry.error.occurred',
            'actor_id', agent_id,
            'attributes', OBJECT_CONSTRUCT(
                'deployment_id', deployment_id,
                'error_message', error_message,
                'error_handled', error_handled,
                'continued_after_error', continued_after_error,
                'compliance_violation', continued_after_error, -- Continuing after error violates Rule 3
                'timestamp', CURRENT_TIMESTAMP()
            ),
            'occurred_at', CURRENT_TIMESTAMP()
        ),
        'TELEMETRY',
        CURRENT_TIMESTAMP();
    
    RETURN OBJECT_CONSTRUCT(
        'success', TRUE,
        'deployment_id', deployment_id,
        'error_logged', TRUE,
        'violation_detected', continued_after_error
    );
END;
$$;

-- =====================================================
-- COMPLIANCE MONITORING VIEWS
-- =====================================================

CREATE OR REPLACE VIEW MCP.V_VERIFICATION_COMPLIANCE_SUMMARY AS
WITH deployment_stats AS (
    SELECT 
        DATE_TRUNC('hour', occurred_at) as hour,
        COUNT(*) as total_deployments,
        SUM(IFF(verification_status = 'PASSED', 1, 0)) as passed_verifications,
        SUM(IFF(verification_status = 'FAILED', 1, 0)) as failed_verifications,
        AVG(compliance_score) as avg_compliance_score,
        COUNT(DISTINCT agent_id) as unique_agents
    FROM MCP.AGENT_TELEMETRY
    WHERE action = 'telemetry.verification.completed'
    GROUP BY hour
)
SELECT 
    hour,
    total_deployments,
    passed_verifications,
    failed_verifications,
    ROUND(passed_verifications * 100.0 / NULLIF(total_deployments, 0), 2) as success_rate,
    ROUND(avg_compliance_score, 2) as avg_compliance_score,
    unique_agents
FROM deployment_stats
ORDER BY hour DESC;

COMMENT ON VIEW MCP.V_VERIFICATION_COMPLIANCE_SUMMARY IS 
$${"type": "monitoring", "purpose": "Track verification compliance over time"}$$;

CREATE OR REPLACE VIEW MCP.V_RULE_VIOLATIONS AS
SELECT 
    agent_id,
    deployment_id,
    occurred_at,
    ARRAY_TO_STRING(rule_violations, ', ') as violations,
    compliance_score,
    verification_status
FROM MCP.AGENT_TELEMETRY
WHERE ARRAY_SIZE(rule_violations) > 0
ORDER BY occurred_at DESC;

COMMENT ON VIEW MCP.V_RULE_VIOLATIONS IS 
$${"type": "monitoring", "purpose": "Track verification rule violations"}$$;

CREATE OR REPLACE VIEW MCP.V_AGENT_COMPLIANCE_SCORES AS
SELECT 
    agent_id,
    COUNT(*) as total_deployments,
    AVG(compliance_score) as avg_compliance_score,
    MIN(compliance_score) as min_compliance_score,
    MAX(compliance_score) as max_compliance_score,
    SUM(IFF(compliance_score = 100, 1, 0)) as perfect_deployments,
    SUM(IFF(compliance_score < 50, 1, 0)) as poor_deployments,
    MAX(occurred_at) as last_deployment
FROM MCP.AGENT_TELEMETRY
WHERE action = 'telemetry.verification.completed'
GROUP BY agent_id
ORDER BY avg_compliance_score DESC;

COMMENT ON VIEW MCP.V_AGENT_COMPLIANCE_SCORES IS 
$${"type": "monitoring", "purpose": "Agent compliance scorecard"}$$;

-- =====================================================
-- AUTOMATED COMPLIANCE ALERTS
-- =====================================================

CREATE OR REPLACE PROCEDURE MCP.CHECK_COMPLIANCE_VIOLATIONS()
RETURNS VARIANT
LANGUAGE SQL
AS
$$
BEGIN
    LET violations_last_hour INTEGER;
    LET low_compliance_agents INTEGER;
    LET critical_violations ARRAY;
    LET alert_needed BOOLEAN := FALSE;
    -- Check violations in last hour
    SELECT COUNT(*)
    INTO violations_last_hour
    FROM MCP.V_RULE_VIOLATIONS
    WHERE occurred_at > DATEADD('hour', -1, CURRENT_TIMESTAMP());
    
    -- Check agents with low compliance
    SELECT COUNT(*)
    INTO low_compliance_agents
    FROM MCP.V_AGENT_COMPLIANCE_SCORES
    WHERE avg_compliance_score < 75
      AND total_deployments > 5;
    
    -- Check for critical violations (table creation attempts)
    SELECT ARRAY_AGG(deployment_id)
    INTO critical_violations
    FROM MCP.AGENT_TELEMETRY
    WHERE ARRAY_CONTAINS('TABLE_CREATION_ATTEMPTED'::VARIANT, rule_violations)
      AND occurred_at > DATEADD('hour', -1, CURRENT_TIMESTAMP());
    
    IF violations_last_hour > 10 OR low_compliance_agents > 0 OR ARRAY_SIZE(critical_violations) > 0 THEN
        alert_needed := TRUE;
        
        -- Log alert as event
        INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS
        SELECT 
            OBJECT_CONSTRUCT(
                'event_id', UUID_STRING(),
                'action', 'telemetry.compliance.alert',
                'actor_id', 'COMPLIANCE_MONITOR',
                'attributes', OBJECT_CONSTRUCT(
                    'violations_last_hour', violations_last_hour,
                    'low_compliance_agents', low_compliance_agents,
                    'critical_violations', critical_violations,
                    'alert_severity', IFF(ARRAY_SIZE(critical_violations) > 0, 'CRITICAL', 'WARNING'),
                    'timestamp', CURRENT_TIMESTAMP()
                ),
                'occurred_at', CURRENT_TIMESTAMP()
            ),
            'TELEMETRY',
            CURRENT_TIMESTAMP();
    END IF;
    
    RETURN OBJECT_CONSTRUCT(
        'check_completed', TRUE,
        'alert_triggered', alert_needed,
        'violations_last_hour', violations_last_hour,
        'low_compliance_agents', low_compliance_agents,
        'critical_violations', critical_violations
    );
END;
$$;

-- =====================================================
-- TELEMETRY REPORTING PROCEDURES
-- =====================================================

CREATE OR REPLACE PROCEDURE MCP.GENERATE_COMPLIANCE_REPORT(
    start_date TIMESTAMP DEFAULT NULL,
    end_date TIMESTAMP DEFAULT NULL
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
BEGIN
    LET report_start TIMESTAMP;
    LET report_end TIMESTAMP;
    LET total_deployments INTEGER;
    LET compliant_deployments INTEGER;
    LET compliance_rate NUMBER;
    LET top_violations ARRAY;
    LET worst_agents ARRAY;
    report_start := COALESCE(start_date, DATEADD('day', -7, CURRENT_TIMESTAMP()));
    report_end := COALESCE(end_date, CURRENT_TIMESTAMP());
    
    -- Calculate overall stats
    SELECT 
        COUNT(*),
        SUM(IFF(compliance_score = 100, 1, 0))
    INTO total_deployments, compliant_deployments
    FROM MCP.AGENT_TELEMETRY
    WHERE action = 'telemetry.verification.completed'
      AND occurred_at BETWEEN report_start AND report_end;
    
    compliance_rate := ROUND(compliant_deployments * 100.0 / NULLIF(total_deployments, 0), 2);
    
    -- Get top violations
    WITH violation_counts AS (
        SELECT 
            v.value::STRING as violation_type,
            COUNT(*) as count
        FROM MCP.AGENT_TELEMETRY t,
        LATERAL FLATTEN(input => t.rule_violations) v
        WHERE t.occurred_at BETWEEN report_start AND report_end
        GROUP BY violation_type
        ORDER BY count DESC
        LIMIT 5
    )
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT('violation', violation_type, 'count', count))
    INTO top_violations
    FROM violation_counts;
    
    -- Get worst performing agents
    WITH agent_scores AS (
        SELECT 
            agent_id,
            AVG(compliance_score) as avg_score,
            COUNT(*) as deployment_count
        FROM MCP.AGENT_TELEMETRY
        WHERE action = 'telemetry.verification.completed'
          AND occurred_at BETWEEN report_start AND report_end
        GROUP BY agent_id
        HAVING deployment_count > 3
        ORDER BY avg_score ASC
        LIMIT 5
    )
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT('agent', agent_id, 'avg_score', avg_score, 'deployments', deployment_count))
    INTO worst_agents
    FROM agent_scores;
    
    -- Store report as event
    INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS
    SELECT 
        OBJECT_CONSTRUCT(
            'event_id', UUID_STRING(),
            'action', 'telemetry.compliance.report',
            'actor_id', 'COMPLIANCE_REPORTER',
            'attributes', OBJECT_CONSTRUCT(
                'report_period', OBJECT_CONSTRUCT('start', report_start, 'end', report_end),
                'total_deployments', total_deployments,
                'compliant_deployments', compliant_deployments,
                'compliance_rate', compliance_rate,
                'top_violations', top_violations,
                'worst_performing_agents', worst_agents,
                'generated_at', CURRENT_TIMESTAMP()
            ),
            'occurred_at', CURRENT_TIMESTAMP()
        ),
        'TELEMETRY',
        CURRENT_TIMESTAMP();
    
    RETURN OBJECT_CONSTRUCT(
        'report_type', 'COMPLIANCE_REPORT',
        'period', OBJECT_CONSTRUCT('start', report_start, 'end', report_end),
        'total_deployments', total_deployments,
        'compliance_rate', compliance_rate || '%',
        'top_violations', top_violations,
        'worst_performing_agents', worst_agents,
        'report_generated', CURRENT_TIMESTAMP()
    );
END;
$$;

-- =====================================================
-- SCHEDULED COMPLIANCE CHECK (Can be scheduled via Snowflake Tasks)
-- =====================================================

CREATE OR REPLACE PROCEDURE MCP.SCHEDULE_COMPLIANCE_MONITORING()
RETURNS VARIANT
LANGUAGE SQL
AS
$$
BEGIN
    -- This would normally create a Snowflake Task, but following the rules,
    -- we'll just document how to monitor
    
    -- Log scheduling event
    INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS
    SELECT 
        OBJECT_CONSTRUCT(
            'event_id', UUID_STRING(),
            'action', 'telemetry.monitoring.configured',
            'actor_id', 'SYSTEM',
            'attributes', OBJECT_CONSTRUCT(
                'monitoring_views', ARRAY_CONSTRUCT(
                    'MCP.V_VERIFICATION_COMPLIANCE_SUMMARY',
                    'MCP.V_RULE_VIOLATIONS',
                    'MCP.V_AGENT_COMPLIANCE_SCORES'
                ),
                'check_procedures', ARRAY_CONSTRUCT(
                    'MCP.CHECK_COMPLIANCE_VIOLATIONS',
                    'MCP.GENERATE_COMPLIANCE_REPORT'
                ),
                'configured_at', CURRENT_TIMESTAMP()
            ),
            'occurred_at', CURRENT_TIMESTAMP()
        ),
        'TELEMETRY',
        CURRENT_TIMESTAMP();
    
    RETURN OBJECT_CONSTRUCT(
        'success', TRUE,
        'message', 'Compliance monitoring configured',
        'next_steps', 'Schedule MCP.CHECK_COMPLIANCE_VIOLATIONS() to run hourly'
    );
END;
$$;

-- =====================================================
-- Initialize telemetry system
-- =====================================================
CALL MCP.SCHEDULE_COMPLIANCE_MONITORING();