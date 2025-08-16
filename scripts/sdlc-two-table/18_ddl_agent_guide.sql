-- ============================================================================
-- 18_ddl_agent_guide.sql
-- Complete Agent Guide for Hardened DDL Versioning System
-- ============================================================================

-- ============================================================================
-- AGENT DDL GUIDE: THE ONLY RULE
-- ============================================================================
/*
┌─────────────────────────────────────────────────────────────────────────┐
│                                                                         │
│  THERE IS ONLY ONE RULE FOR DDL:                                      │
│                                                                         │
│  ALL DDL MUST GO THROUGH SAFE_DDL                                     │
│                                                                         │
│  You have NO privileges to CREATE, ALTER, or DROP directly.           │
│  Any attempt to bypass SAFE_DDL will be detected and logged.          │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
*/

-- ============================================================================
-- QUICK REFERENCE FOR AGENTS
-- ============================================================================

-- CREATE OR REPLACE a view
CALL MCP.SAFE_DDL(
  'CREATE OR REPLACE VIEW MCP.VW_AGENT_TEST AS 
   SELECT * FROM ACTIVITY.EVENTS WHERE action = ''test''',
  'Creating test view for demo'
);

-- CREATE OR REPLACE a procedure (signature auto-detected)
CALL MCP.SAFE_DDL(
  'CREATE OR REPLACE PROCEDURE MCP.AGENT_PROC(p1 STRING, p2 NUMBER)
   RETURNS STRING
   LANGUAGE SQL
   AS
   $$
   BEGIN
     RETURN p1 || '':'' || p2;
   END;
   $$',
  'Concatenation utility procedure'
);

-- CREATE OR REPLACE a function
CALL MCP.SAFE_DDL(
  'CREATE OR REPLACE FUNCTION MCP.AGENT_FUNC(x NUMBER)
   RETURNS NUMBER
   LANGUAGE SQL
   AS
   $$
     x * 2
   $$',
  'Double the input value'
);

-- ============================================================================
-- HANDLING CONFLICTS AND RETRIES
-- ============================================================================

-- Step 1: Check current version before modifying
SELECT 
  object_identity,
  version,
  canonical_hash,
  author,
  last_modified
FROM MCP.VW_DDL_CATALOG_CONSISTENT
WHERE object_identity = 'MCP.MY_PROC(STRING, NUMBER)';

-- Step 2: If you get a conflict error, re-read and retry
-- The error will include expected_hash and actual_hash
-- Use the actual_hash as your new expected_hash in retry

-- Step 3: Generate idempotency key for your change
-- idempotency_key = SHA2(ddl_text || reason)
-- This prevents duplicate processing of the same change

-- ============================================================================
-- WORKING WITH OBJECT SIGNATURES
-- ============================================================================

-- For procedures and functions, ALWAYS include the full signature
-- CORRECT: 'MCP.MY_PROC(STRING, NUMBER)'
-- WRONG:   'MCP.MY_PROC'

-- The system automatically extracts signatures from CREATE statements:
-- CREATE PROCEDURE MY_PROC(p1 STRING, p2 NUMBER)
-- Becomes: MCP.MY_PROC(STRING, NUMBER)

-- For overloaded procedures, signatures distinguish them:
-- MCP.PROCESS_DATA(STRING)           -- Version 1
-- MCP.PROCESS_DATA(STRING, NUMBER)   -- Version 2 (different procedure)

-- ============================================================================
-- TESTING YOUR DDL
-- ============================================================================

-- Add tests to your objects
CALL MCP.DDL_ADD_TEST(
  'MCP.MY_FUNC(NUMBER)',           -- Object identity with signature
  'test_doubles_correctly',        -- Test name
  'SELECT MCP.MY_FUNC(5)',        -- Test SQL
  10                               -- Expected result
);

-- Run tests manually before deployment
CALL MCP.DDL_RUN_TESTS('MCP.MY_FUNC(NUMBER)');

-- Tests run automatically after SAFE_DDL deployment
-- Failed tests trigger automatic rollback

-- ============================================================================
-- VIEWING DDL HISTORY AND VERSIONS
-- ============================================================================

-- Current catalog with immediate consistency
SELECT * FROM MCP.VW_DDL_CATALOG_CONSISTENT;

-- Full history of changes
SELECT * FROM MCP.VW_DDL_HISTORY 
WHERE object_identity = 'MCP.MY_VIEW'
ORDER BY occurred_at DESC;

-- Check for drift between stored and actual DDL
SELECT * FROM MCP.VW_DDL_DRIFT
WHERE drift_status != 'IN_SYNC';

-- See your recent DDL operations
SELECT 
  occurred_at,
  attributes:object_identity::string as object,
  attributes:version::string as version,
  attributes:author::string as author,
  attributes:reason::string as reason
FROM MCP.VW_DDL_CONSISTENCY
WHERE action IN ('ddl.object.create', 'ddl.object.alter')
  AND actor_id = CURRENT_USER()
ORDER BY occurred_at DESC
LIMIT 10;

-- ============================================================================
-- ALTER AND DROP OPERATIONS
-- ============================================================================

-- ALTER operations (converted to CREATE OR REPLACE internally)
CALL MCP.SAFE_ALTER(
  'ALTER PROCEDURE MCP.MY_PROC(STRING) SET COMMENT = ''Updated docs''',
  'Adding documentation'
);

-- Soft DELETE (recoverable - renames object)
CALL MCP.SAFE_DROP('VIEW', 'MCP.OLD_VIEW', 'No longer needed', FALSE);

-- Hard DELETE (permanent - cannot recover)
CALL MCP.SAFE_DROP('PROCEDURE', 'MCP.TEMP_PROC', 'Temporary test', TRUE);

-- View recoverable objects
SELECT * FROM MCP.VW_DDL_RECOVERABLE;

-- Recover a soft-deleted object
CALL MCP.SAFE_RECOVER('MCP.OLD_VIEW_DROPPED_20240115_143022');

-- ============================================================================
-- MONITORING AND COMPLIANCE
-- ============================================================================

-- Check if your operations are compliant
SELECT * FROM MCP.VW_DDL_COMPLIANCE_MONITOR
WHERE USER_NAME = CURRENT_USER();

-- View any compliance violations
SELECT * FROM MCP.VW_DDL_COMPLIANCE_ALERTS;

-- Check security status (you should have NO DDL privileges)
SELECT * FROM MCP.VW_DDL_SECURITY_STATUS
WHERE role_name IN ('MCP_AGENT_ROLE', 'MCP_USER_ROLE');

-- ============================================================================
-- ERROR HANDLING PATTERNS
-- ============================================================================

-- Pattern 1: Handle version conflicts
DO
$$
DECLARE
  result VARIANT;
  retry_count INTEGER DEFAULT 0;
  max_retries INTEGER DEFAULT 3;
BEGIN
  WHILE retry_count < max_retries DO
    CALL MCP.SAFE_DDL(
      'CREATE OR REPLACE VIEW MCP.MY_VIEW AS SELECT 1 as col',
      'Update view'
    ) INTO result;
    
    IF result:result = 'success' THEN
      -- Success, exit loop
      BREAK;
    ELSEIF result:result = 'conflict' THEN
      -- Conflict, retry with updated hash
      retry_count := retry_count + 1;
      -- In real code, extract actual_hash and use it
      CONTINUE;
    ELSE
      -- Other error, don't retry
      RAISE EXCEPTION 'DDL failed: %', result:error;
    END IF;
  END WHILE;
END;
$$;

-- Pattern 2: Check before modify
DO
$$
DECLARE
  current_version STRING;
BEGIN
  -- Get current version
  SELECT version INTO current_version
  FROM MCP.VW_DDL_CATALOG_CONSISTENT
  WHERE object_identity = 'MCP.MY_PROC(STRING)';
  
  -- Only update if not already on target version
  IF current_version != '2.0.0' THEN
    CALL MCP.SAFE_DDL(
      'CREATE OR REPLACE PROCEDURE MCP.MY_PROC(p STRING) ...',
      'Upgrade to v2.0.0'
    );
  END IF;
END;
$$;

-- ============================================================================
-- ATTRIBUTION AND TRACING
-- ============================================================================

-- Your operations are automatically attributed via:
-- 1. CURRENT_USER() - your username
-- 2. Query tags - set by your session
-- 3. Trace IDs - for distributed tracing

-- Set a query tag for better attribution
ALTER SESSION SET QUERY_TAG = 'agent:claude_001,task:feature_xyz,trace:abc123';

-- Your tag will be captured in all DDL events

-- ============================================================================
-- BEST PRACTICES FOR AGENTS
-- ============================================================================

/*
1. ALWAYS use SAFE_DDL - no exceptions
2. Include meaningful reasons for changes
3. Use full object signatures for procedures/functions
4. Add tests to critical objects
5. Check current version before modifying
6. Handle conflicts with retry logic
7. Use soft delete for recoverable drops
8. Set query tags for attribution
9. Monitor compliance status regularly
10. Never attempt direct DDL - it will fail and be logged
*/

-- ============================================================================
-- TROUBLESHOOTING
-- ============================================================================

-- Q: I get "Schema not allowed" error
-- A: You can only modify objects in MCP schema

-- Q: I get "Version conflict" error
-- A: Another agent modified the object. Re-read and retry.

-- Q: My DDL executes but isn't in the catalog
-- A: Check VW_DDL_PENDING_PROMOTION - may be waiting for Dynamic Table refresh

-- Q: I accidentally dropped an object
-- A: Check VW_DDL_RECOVERABLE and use SAFE_RECOVER if it was soft-deleted

-- Q: I want to see what changed
-- A: Query VW_DDL_HISTORY for complete change history

-- Q: Tests are failing after deployment
-- A: The system auto-rolls back. Fix tests and redeploy.

-- ============================================================================
-- COMPLETE EXAMPLE WORKFLOW
-- ============================================================================

-- 1. Create a new function with tests
CALL MCP.SAFE_DDL(
  'CREATE OR REPLACE FUNCTION MCP.CALCULATE_TAX(amount NUMBER, rate NUMBER)
   RETURNS NUMBER
   LANGUAGE SQL
   COMMENT = ''Calculate tax amount''
   AS
   $$
     ROUND(amount * rate, 2)
   $$',
  'New tax calculation function'
);

-- 2. Add tests
CALL MCP.DDL_ADD_TEST(
  'MCP.CALCULATE_TAX(NUMBER, NUMBER)',
  'test_basic_calculation',
  'SELECT MCP.CALCULATE_TAX(100, 0.08)',
  8.00
);

CALL MCP.DDL_ADD_TEST(
  'MCP.CALCULATE_TAX(NUMBER, NUMBER)',
  'test_zero_amount',
  'SELECT MCP.CALCULATE_TAX(0, 0.08)',
  0.00
);

-- 3. Verify version
SELECT version, canonical_hash 
FROM MCP.VW_DDL_CATALOG_CONSISTENT
WHERE object_identity = 'MCP.CALCULATE_TAX(NUMBER, NUMBER)';

-- 4. Update the function
CALL MCP.SAFE_DDL(
  'CREATE OR REPLACE FUNCTION MCP.CALCULATE_TAX(amount NUMBER, rate NUMBER)
   RETURNS NUMBER
   LANGUAGE SQL
   COMMENT = ''Calculate tax with validation''
   AS
   $$
     CASE 
       WHEN amount < 0 OR rate < 0 THEN NULL
       ELSE ROUND(amount * rate, 2)
     END
   $$',
  'Add input validation'
);

-- 5. View history
SELECT version, reason, occurred_at
FROM MCP.VW_DDL_HISTORY
WHERE object_identity = 'MCP.CALCULATE_TAX(NUMBER, NUMBER)'
ORDER BY occurred_at DESC;

-- ============================================================================
-- END OF AGENT GUIDE
-- ============================================================================