-- Test creating SDLC ticket
-- First ensure we have the helper procedure

USE DATABASE CLAUDE_BI;
USE SCHEMA MCP;

-- Simple test - just try to insert an event directly
INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS
SELECT 
  OBJECT_CONSTRUCT(
    'event_id', UUID_STRING(),
    'action', 'sdlc.work.create',
    'occurred_at', CURRENT_TIMESTAMP(),
    'actor_id', 'chandler',
    'source', 'sdlc',
    'schema_version', '2.1.0',
    'object', OBJECT_CONSTRUCT(
      'type', 'work_item',
      'id', 'WORK_TEST_001'
    ),
    'attributes', OBJECT_CONSTRUCT(
      'work_id', 'WORK_TEST_001',
      'display_id', 'WORK-00001',
      'title', 'Implement Sequential Ticket Numbering System',
      'type', 'enhancement',
      'severity', 'p1',
      'description', 'Fix the ticket numbering system to use Snowflake sequences for guaranteed sequential numbering instead of COUNT-based approach which has race conditions.',
      'reporter_id', 'chandler',
      'business_value', 8,
      'customer_impact', TRUE,
      'status', 'new',
      'idempotency_key', 'manual_ticket_001'
    )
  ),
  'MANUAL',
  CURRENT_TIMESTAMP();

-- Check if it worked
SELECT * FROM CLAUDE_BI.MCP.VW_WORK_ITEMS WHERE work_id = 'WORK_TEST_001';