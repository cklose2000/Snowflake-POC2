-- ============================================================================
-- 01_event_taxonomy.sql
-- SDLC Event Taxonomy - Two-Table Law Compliant
-- Defines all SDLC event types and their JSON payload schemas
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE CLAUDE_BI;
USE SCHEMA MCP;

-- ============================================================================
-- SDLC EVENT TAXONOMY
-- 
-- ALL events are stored in CLAUDE_BI.ACTIVITY.EVENTS with these event_type values.
-- NO new tables are created - this file is purely documentation and examples.
-- ============================================================================

/* 
Event Envelope Standard (all SDLC events include these fields in payload):
{
  "work_id": "WORK-123",              -- Work item identifier  
  "idempotency_key": "uuid",          -- Prevents duplicate processing
  "expected_last_event_id": "evt-id", -- Optimistic concurrency control
  "tenant_id": "tenant-1",            -- Optional: multi-tenancy
  "trace_id": "trace-123",            -- Optional: debugging/correlation
  "actor_id": "user-or-agent-id",     -- Who performed the action
  "occurred_at": "2024-01-01T00:00:00Z", -- When it happened
  "schema_version": "1.0.0"           -- Payload schema version
}
*/

-- ============================================================================
-- 1. WORK ITEM LIFECYCLE EVENTS
-- ============================================================================

-- Create a new work item
-- Event Type: 'sdlc.work.create'
-- Example payload:
/*
{
  "work_id": "WORK-001",
  "title": "Implement user authentication",
  "type": "feature",           -- feature|bug|debt|spike|epic
  "severity": "p2",            -- p0|p1|p2|p3 (p0 = critical)
  "description": "Add OAuth2 login flow with Google",
  "reporter_id": "user-123",
  "labels": ["auth", "security"],
  "business_value": 8,         -- 1-10 scale
  "customer_impact": true,
  "idempotency_key": "create-001-abc123",
  "tenant_id": "growthzone",
  "schema_version": "1.0.0"
}
*/

-- Assign work to someone  
-- Event Type: 'sdlc.work.assign'
-- Example payload:
/*
{
  "work_id": "WORK-001",
  "assignee_id": "agent-claude",
  "assignee_type": "ai_agent",  -- human|ai_agent|team
  "assigned_by": "user-123",
  "reason": "Agent has auth experience",
  "expected_last_event_id": "evt-create-001",
  "idempotency_key": "assign-001-def456",
  "schema_version": "1.0.0"
}
*/

-- Change work status
-- Event Type: 'sdlc.work.status'  
-- Example payload:
/*
{
  "work_id": "WORK-001",
  "status": "in_progress",      -- new|backlog|ready|in_progress|review|done|blocked|cancelled
  "from_status": "ready",
  "status_reason": "Starting implementation",
  "expected_last_event_id": "evt-assign-001",
  "idempotency_key": "status-001-ghi789",
  "schema_version": "1.0.0"
}
*/

-- Mark work as done
-- Event Type: 'sdlc.work.done'
-- Example payload:
/*
{
  "work_id": "WORK-001", 
  "completion_time_ms": 7200000,  -- 2 hours
  "completion_notes": "OAuth flow implemented and tested",
  "deliverables": ["auth-service.js", "login-component.tsx"],
  "tests_passing": true,
  "code_reviewed": true,
  "expected_last_event_id": "evt-status-001",
  "idempotency_key": "done-001-jkl012",
  "schema_version": "1.0.0"
}
*/

-- Reject/return work
-- Event Type: 'sdlc.work.reject'
-- Example payload:
/*
{
  "work_id": "WORK-001",
  "reason": "Tests failing",
  "rejection_details": "OAuth redirect URL not working in production",
  "rejected_by": "user-reviewer",
  "return_to_status": "in_progress",
  "expected_last_event_id": "evt-done-001",
  "idempotency_key": "reject-001-mno345",
  "schema_version": "1.0.0"
}
*/

-- ============================================================================
-- 2. DEPENDENCY & BLOCKING EVENTS
-- ============================================================================

-- Create dependency relationship
-- Event Type: 'sdlc.work.depends'
-- Example payload:
/*
{
  "work_id": "WORK-002",
  "depends_on_id": "WORK-001", 
  "dependency_type": "blocks",  -- blocks|relates_to|part_of|duplicates
  "dependency_reason": "Needs auth system before user profiles",
  "created_by": "user-123",
  "expected_last_event_id": "evt-create-002",
  "idempotency_key": "depend-002-pqr678",
  "schema_version": "1.0.0"
}
*/

-- ============================================================================
-- 3. ESTIMATION & TRACKING EVENTS
-- ============================================================================

-- Initial estimate
-- Event Type: 'sdlc.work.estimate' 
-- Example payload:
/*
{
  "work_id": "WORK-001",
  "points": 5,                 -- Story points (fibonacci: 1,2,3,5,8,13,21)
  "estimator": "team-backend",
  "estimation_method": "planning_poker",
  "confidence": "medium",       -- low|medium|high
  "estimation_notes": "Well-defined requirements, some OAuth complexity",
  "expected_last_event_id": "evt-create-001",
  "idempotency_key": "estimate-001-stu901",
  "schema_version": "1.0.0"
}
*/

-- Re-estimate (when scope changes)
-- Event Type: 'sdlc.work.reestimate'
-- Example payload:
/*
{
  "work_id": "WORK-001",
  "points": 8,
  "previous_points": 5,
  "estimator": "agent-claude",
  "reestimate_reason": "Additional security requirements discovered",
  "expected_last_event_id": "evt-estimate-001",
  "idempotency_key": "reestimate-001-vwx234",
  "schema_version": "1.0.0"
}
*/

-- ============================================================================
-- 4. SPRINT/ITERATION MANAGEMENT EVENTS
-- ============================================================================

-- Create new sprint
-- Event Type: 'sdlc.sprint.create'
-- Example payload:
/*
{
  "sprint_id": "SPRINT-2024-01",
  "name": "Authentication Sprint",
  "start_date": "2024-01-15T00:00:00Z",
  "end_date": "2024-01-29T23:59:59Z",
  "team_id": "team-backend",
  "capacity_points": 40,
  "sprint_goal": "Complete user authentication system",
  "created_by": "scrum-master",
  "idempotency_key": "sprint-create-567890",
  "schema_version": "1.0.0"
}
*/

-- Assign work to sprint
-- Event Type: 'sdlc.sprint.assign'
-- Example payload:
/*
{
  "work_id": "WORK-001",
  "sprint_id": "SPRINT-2024-01",
  "assigned_by": "scrum-master",
  "commitment_level": "committed", -- committed|stretch|nice_to_have
  "expected_last_event_id": "evt-estimate-001",
  "idempotency_key": "sprint-assign-001-abc123",
  "schema_version": "1.0.0"
}
*/

-- Close sprint
-- Event Type: 'sdlc.sprint.close'
-- Example payload:
/*
{
  "sprint_id": "SPRINT-2024-01",
  "completed_points": 35,
  "planned_points": 40,
  "carryover_work_ids": ["WORK-003", "WORK-004"],
  "velocity": 35,
  "burndown_data": [...],      -- Array of daily progress snapshots
  "retrospective_notes": "Good sprint, need better estimation",
  "closed_by": "scrum-master",
  "idempotency_key": "sprint-close-567890",
  "schema_version": "1.0.0"
}
*/

-- ============================================================================
-- 5. AGENT OPERATIONS EVENTS
-- ============================================================================

-- Agent claims work
-- Event Type: 'sdlc.agent.claim'
-- Example payload:
/*
{
  "work_id": "WORK-001",
  "agent_id": "agent-claude", 
  "agent_type": "ai_coding_agent",
  "agent_capabilities": ["javascript", "react", "oauth"],
  "claim_reason": "Matching skills for auth work",
  "claimed_at": "2024-01-15T10:00:00Z",
  "expected_last_event_id": "evt-assign-001",
  "idempotency_key": "claim-001-def456",
  "schema_version": "1.0.0"
}
*/

-- Agent encounters error/conflict
-- Event Type: 'sdlc.agent.error'
-- Example payload:
/*
{
  "work_id": "WORK-001",
  "agent_id": "agent-claude",
  "error_type": "conflict",     -- conflict|timeout|invalid_state|system_error
  "error_message": "Work item was modified by another agent",
  "expected_last_event_id": "evt-assign-001",
  "actual_last_event_id": "evt-claim-002",
  "retry_count": 1,
  "will_retry": true,
  "retry_after_ms": 5000,
  "idempotency_key": "error-001-ghi789",
  "schema_version": "1.0.0"
}
*/

-- ============================================================================
-- 6. REPORTING & SNAPSHOT EVENTS (instead of creating tables!)
-- ============================================================================

-- Sprint burndown snapshot (generated by scheduled task)
-- Event Type: 'sdlc.report.burndown'
-- Example payload:
/*
{
  "sprint_id": "SPRINT-2024-01",
  "snapshot_date": "2024-01-20",
  "remaining_points": 25,
  "completed_points": 15,
  "total_points": 40,
  "work_items_done": 3,
  "work_items_remaining": 5,
  "days_remaining": 5,
  "on_track": true,
  "generated_by": "scheduled_task",
  "idempotency_key": "burndown-sprint-01-20240120",
  "schema_version": "1.0.0"
}
*/

-- Team velocity snapshot
-- Event Type: 'sdlc.report.velocity'
-- Example payload:
/*
{
  "team_id": "team-backend",
  "week_ending": "2024-01-21",
  "completed_points": 35,
  "committed_points": 40,
  "completion_rate": 0.875,
  "velocity_trend": "stable",   -- increasing|stable|decreasing
  "avg_cycle_time_hours": 18,
  "work_items_completed": 8,
  "generated_by": "scheduled_task", 
  "idempotency_key": "velocity-backend-20240121",
  "schema_version": "1.0.0"
}
*/

-- ============================================================================
-- 7. QUALITY & TESTING EVENTS
-- ============================================================================

-- Test results
-- Event Type: 'sdlc.work.test'
-- Example payload:
/*
{
  "work_id": "WORK-001",
  "test_type": "unit",          -- unit|integration|e2e|manual
  "test_result": "passed",      -- passed|failed|skipped
  "tests_run": 25,
  "tests_passed": 23,
  "tests_failed": 2,
  "test_duration_ms": 5000,
  "test_report_url": "https://ci.example.com/report/123",
  "tested_by": "ci-system",
  "idempotency_key": "test-001-jkl012",
  "schema_version": "1.0.0"
}
*/

-- Code review
-- Event Type: 'sdlc.work.review'
-- Example payload:
/*
{
  "work_id": "WORK-001",
  "reviewer_id": "user-senior-dev",
  "review_result": "approved",   -- approved|needs_changes|rejected
  "review_comments": "Good implementation, minor style issues",
  "review_score": 8,            -- 1-10 quality score
  "code_quality_issues": 2,
  "security_issues": 0,
  "performance_issues": 1,
  "idempotency_key": "review-001-mno345",
  "schema_version": "1.0.0"
}
*/

-- ============================================================================
-- SAMPLE EVENT INSERTION (for testing)
-- ============================================================================

-- Example: Insert a work creation event
INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS 
SELECT 
  OBJECT_CONSTRUCT(
    'event_id', UUID_STRING(),
    'action', 'sdlc.work.create',
    'occurred_at', CURRENT_TIMESTAMP(),
    'actor_id', 'user-demo',
    'source', 'sdlc',
    'schema_version', '2.1.0',
    'object', OBJECT_CONSTRUCT(
      'type', 'work_item',
      'id', 'WORK-DEMO-001'
    ),
    'attributes', OBJECT_CONSTRUCT(
      'work_id', 'WORK-DEMO-001',
      'title', 'Build SDLC ticketing system',
      'type', 'epic',
      'severity', 'p1',
      'description', 'Implement event-driven SDLC system in Snowflake',
      'business_value', 10,
      'customer_impact', true,
      'idempotency_key', 'demo-create-' || UUID_STRING(),
      'tenant_id', 'claude-demo',
      'schema_version', '1.0.0'
    )
  ),
  'SDLC_DEMO',
  CURRENT_TIMESTAMP();

-- Example: Insert a work assignment event  
INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS
SELECT
  OBJECT_CONSTRUCT(
    'event_id', UUID_STRING(),
    'action', 'sdlc.work.assign',
    'occurred_at', CURRENT_TIMESTAMP(),
    'actor_id', 'scrum-master',
    'source', 'sdlc',
    'schema_version', '2.1.0',
    'object', OBJECT_CONSTRUCT(
      'type', 'work_item',
      'id', 'WORK-DEMO-001'
    ),
    'attributes', OBJECT_CONSTRUCT(
      'work_id', 'WORK-DEMO-001',
      'assignee_id', 'agent-claude',
      'assignee_type', 'ai_agent',
      'assigned_by', 'scrum-master',
      'reason', 'Agent specializes in event-driven architectures',
      'idempotency_key', 'demo-assign-' || UUID_STRING(),
      'schema_version', '1.0.0'
    )
  ),
  'SDLC_DEMO',
  CURRENT_TIMESTAMP();

-- ============================================================================
-- END OF EVENT TAXONOMY
-- 
-- Next: 02_core_views.sql - Views to read current state from these events
-- ============================================================================