# CLAUDE.md - CRITICAL ARCHITECTURAL CONSTRAINTS

## 🚨🚨🚨 STOP - READ THIS FIRST 🚨🚨🚨

# THE TWO-TABLE LAW

## THIS SYSTEM HAS EXACTLY TWO TABLES. ONLY TWO. FOREVER.

```
1. CLAUDE_BI.APP.LANDING.RAW_EVENTS     (Ingestion)
2. CLAUDE_BI.APP.ACTIVITY.EVENTS        (Dynamic Table)
```

## THERE ARE NO OTHER TABLES. NONE. ZERO. NEVER.

### ⛔ CREATING A NEW TABLE IS AN ARCHITECTURAL VIOLATION ⛔

---

## 🔴 BEFORE YOU WRITE ANY SQL, ANSWER THIS:

**Are you about to create a table?**
- If YES → **STOP IMMEDIATELY**
- If MAYBE → **STOP IMMEDIATELY**  
- If NO → Continue, but double-check

## 📢 WHAT TO DO INSTEAD OF CREATING TABLES

### Scenario: "I need to track user permissions"
❌ **WRONG**: `CREATE TABLE USER_PERMISSIONS...`
✅ **RIGHT**: 
```sql
-- Permissions are EVENTS
INSERT INTO CLAUDE_BI.APP.LANDING.RAW_EVENTS VALUES (
  OBJECT_CONSTRUCT(
    'event_id', UUID_STRING(),
    'action', 'system.permission.granted',
    'actor_id', 'admin',
    'object', OBJECT_CONSTRUCT('type', 'user', 'id', 'sarah'),
    'attributes', OBJECT_CONSTRUCT(
      'allowed_actions', ['order.placed', 'user.signup'],
      'max_rows', 10000
    ),
    'occurred_at', CURRENT_TIMESTAMP()
  ),
  'SYSTEM',
  CURRENT_TIMESTAMP()
);

-- Read permissions via VIEW
CREATE OR REPLACE VIEW MCP.CURRENT_USER_PERMISSIONS AS
SELECT * FROM CLAUDE_BI.APP.ACTIVITY.EVENTS 
WHERE action = 'system.permission.granted';
```

### Scenario: "I need to track quality/validation failures"
❌ **WRONG**: `CREATE TABLE QUALITY_EVENTS...`
✅ **RIGHT**:
```sql
-- Quality issues are EVENTS
INSERT INTO CLAUDE_BI.APP.LANDING.RAW_EVENTS VALUES (
  OBJECT_CONSTRUCT(
    'action', 'quality.validation.failed',
    'attributes', OBJECT_CONSTRUCT('reason', 'missing_field'),
    ...
  ), 'QUALITY', CURRENT_TIMESTAMP()
);

-- Read via VIEW
CREATE OR REPLACE VIEW QUALITY_EVENTS AS
SELECT * FROM CLAUDE_BI.APP.ACTIVITY.EVENTS 
WHERE action LIKE 'quality.%';
```

### Scenario: "I need audit logs"
❌ **WRONG**: `CREATE TABLE AUDIT_LOG...`
✅ **RIGHT**: Everything is already audited! It's an event stream!
```sql
-- Every action is ALREADY an audit log entry
SELECT * FROM CLAUDE_BI.APP.ACTIVITY.EVENTS 
WHERE action LIKE 'mcp.%' OR action LIKE 'system.%';
```

### Scenario: "I need to store metadata/config"
❌ **WRONG**: `CREATE TABLE CONFIG...`
✅ **RIGHT**:
```sql
-- Config changes are EVENTS
INSERT INTO CLAUDE_BI.APP.LANDING.RAW_EVENTS VALUES (
  OBJECT_CONSTRUCT(
    'action', 'system.config.updated',
    'attributes', OBJECT_CONSTRUCT('setting', 'max_rows', 'value', 50000),
    ...
  ), 'SYSTEM', CURRENT_TIMESTAMP()
);
```

### Scenario: "I need a staging/temp table"
❌ **WRONG**: `CREATE TABLE TEMP_DATA...`
✅ **RIGHT**: Use CTEs or Views!
```sql
-- Use CTEs for temporary transformations
WITH temp_data AS (
  SELECT * FROM CLAUDE_BI.APP.ACTIVITY.EVENTS WHERE ...
)
SELECT * FROM temp_data;

-- Or create a VIEW if you need it multiple times
CREATE OR REPLACE VIEW MY_TEMP_VIEW AS ...;
```

---

## 🛑 FORBIDDEN SQL PATTERNS

### THESE WILL BE REJECTED:
```sql
-- ❌ ABSOLUTELY FORBIDDEN ❌
CREATE TABLE ...
CREATE OR REPLACE TABLE ...
CREATE TEMPORARY TABLE ...
CREATE TRANSIENT TABLE ...
SELECT * INTO new_table FROM ...
CREATE TABLE ... AS SELECT ...
CREATE EXTERNAL TABLE ...
CREATE ICEBERG TABLE ...
CREATE DYNAMIC TABLE ... (unless it's EVENTS)
```

### ONLY THESE ARE ALLOWED:
```sql
-- ✅ ALLOWED PATTERNS ✅
CREATE OR REPLACE VIEW ...
CREATE OR REPLACE SECURE VIEW ...
CREATE OR REPLACE PROCEDURE ...
CREATE OR REPLACE FUNCTION ...
INSERT INTO CLAUDE_BI.APP.LANDING.RAW_EVENTS ...
SELECT ... FROM CLAUDE_BI.APP.ACTIVITY.EVENTS ...
```

---

## 🔍 VALIDATION QUERIES

### Run THIS to verify schema purity:
```sql
-- This should return EXACTLY 2 rows
SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE
FROM CLAUDE_BI.INFORMATION_SCHEMA.TABLES
WHERE TABLE_CATALOG = 'CLAUDE_BI'
  AND TABLE_SCHEMA IN ('APP', 'LANDING', 'ACTIVITY')
  AND TABLE_TYPE IN ('BASE TABLE', 'DYNAMIC TABLE')
ORDER BY TABLE_SCHEMA, TABLE_NAME;

-- Expected output (EXACTLY THIS):
-- ACTIVITY    | EVENTS      | DYNAMIC TABLE
-- LANDING     | RAW_EVENTS  | BASE TABLE
```

### If you see MORE than 2 tables:
```sql
-- IMMEDIATE ACTION REQUIRED - DROP THE VIOLATIONS
-- List violations
SELECT 'DROP TABLE ' || TABLE_SCHEMA || '.' || TABLE_NAME || ';' AS fix_command
FROM CLAUDE_BI.INFORMATION_SCHEMA.TABLES
WHERE TABLE_CATALOG = 'CLAUDE_BI'
  AND TABLE_TYPE IN ('BASE TABLE', 'DYNAMIC TABLE')
  AND NOT (
    (TABLE_SCHEMA = 'LANDING' AND TABLE_NAME = 'RAW_EVENTS') OR
    (TABLE_SCHEMA = 'ACTIVITY' AND TABLE_NAME = 'EVENTS')
  );
```

---

## 📊 THE PHILOSOPHY: EVERYTHING IS AN EVENT

### Mental Model Check:
- **Users?** → Events with `action='user.created'`
- **Permissions?** → Events with `action='system.permission.granted'`
- **Configurations?** → Events with `action='system.config.updated'`
- **Audit logs?** → Events with `action LIKE 'mcp.%'`
- **Quality issues?** → Events with `action LIKE 'quality.%'`
- **Metrics?** → Derived from counting/aggregating events
- **State?** → Latest event for each entity (using ROW_NUMBER())

### The Answer is ALWAYS Events:
```sql
-- Current state = latest event per entity
WITH latest_state AS (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY object_id ORDER BY occurred_at DESC) as rn
  FROM CLAUDE_BI.APP.ACTIVITY.EVENTS
  WHERE action = 'your.state.type'
)
SELECT * FROM latest_state WHERE rn = 1;
```

---

## 🎯 MCP INTEGRATION (SECONDARY TO TWO-TABLE RULE)

After ensuring you're not creating tables, use MCP for queries:

### MCP Procedures (The ONLY way to query):
```sql
-- These procedures enforce the two-table architecture
CALL CLAUDE_BI.MCP.EXECUTE_QUERY_PLAN(?);
CALL CLAUDE_BI.MCP.VALIDATE_QUERY_PLAN(?);
```

Never query tables directly unless you're:
1. Inserting into RAW_EVENTS
2. Creating a VIEW over EVENTS

---

## ⚠️ WARNING SIGNS YOU'RE ABOUT TO VIOLATE THE ARCHITECTURE

### Red Flags in Your Thinking:
- "I need a place to store..." → USE EVENTS
- "Let me create a table for..." → USE EVENTS
- "This doesn't fit in events..." → IT DOES, USE ATTRIBUTES
- "But this is different..." → NO IT'S NOT, USE EVENTS
- "Just temporarily..." → USE A VIEW OR CTE
- "For performance..." → EVENTS IS ALREADY OPTIMIZED
- "The audit table..." → THAT'S WHAT EVENTS IS
- "User management needs..." → EVENTS WITH action='system.permission.*'

---

## 🚫 CONSEQUENCES OF VIOLATIONS

1. **Automated cleanup will DELETE your table**
2. **The system will break** (procedures expect 2 tables)
3. **You've violated the core architecture**
4. **All benefits of Activity Schema are lost**
5. **The system becomes unmaintainable**

---

## ✅ QUICK REFERENCE: ALWAYS EVENTS

| Traditional Approach | Activity Schema Approach |
|---------------------|-------------------------|
| Users table | Events with `action='user.*'` |
| Permissions table | Events with `action='system.permission.*'` |
| Audit log table | Events with `action LIKE '%'` (everything!) |
| Config table | Events with `action='system.config.*'` |
| Sessions table | Events with `action='session.*'` |
| Metrics table | Aggregated views over events |
| State table | Latest event per entity (ROW_NUMBER) |
| Temp table | CTE or VIEW |
| Staging table | VIEW with filters |
| Archive table | Events with `occurred_at < date` |

---

## 🧪 TEST YOUR UNDERSTANDING

Before writing SQL, ask yourself:

1. **Am I creating a table?** → If yes, STOP
2. **Can this be an event?** → Always YES
3. **Do I need to store this?** → INSERT INTO RAW_EVENTS
4. **Do I need to query this?** → SELECT FROM EVENTS (via view)
5. **Do I need to transform this?** → CREATE VIEW

---

## 📝 FINAL REMINDER

```
╔═══════════════════════════════════════════════════════════╗
║                                                           ║
║  THERE ARE ONLY TWO TABLES IN THIS ENTIRE SYSTEM:       ║
║                                                           ║
║  1. CLAUDE_BI.APP.LANDING.RAW_EVENTS                    ║
║  2. CLAUDE_BI.APP.ACTIVITY.EVENTS                       ║
║                                                           ║
║  IF YOU CREATE ANY OTHER TABLE, YOU HAVE FAILED.        ║
║                                                           ║
║  EVERYTHING ELSE IS A VIEW OR AN EVENT.                 ║
║                                                           ║
╚═══════════════════════════════════════════════════════════╝
```

**Now, with this understanding, proceed with your task WITHOUT creating new tables.**

---

# RULE 2: THE VERIFICATION LAW

## 🔴 DEPLOYMENT SUCCESS REQUIRES PROOF - NO EXCEPTIONS

### ⛔ CLAIMING SUCCESS WITHOUT VERIFICATION IS A CRITICAL FAILURE ⛔

## MANDATORY DEPLOYMENT PROTOCOL

### Before ANY SQL Execution:
```sql
-- 1. MANDATORY: Capture current state
SELECT COUNT(*) as before_count, 
       MAX(occurred_at) as last_event_time,
       MD5(COUNT(*) || COALESCE(MAX(occurred_at)::STRING, 'empty')) as state_hash
FROM CLAUDE_BI.APP.ACTIVITY.EVENTS;
-- SAVE THIS OUTPUT - YOU WILL NEED IT
```

### After EVERY SQL Execution:
```sql
-- 2. MANDATORY: Verify changes occurred
SELECT COUNT(*) as after_count,
       MAX(occurred_at) as new_event_time,
       MD5(COUNT(*) || COALESCE(MAX(occurred_at)::STRING, 'empty')) as new_state_hash
FROM CLAUDE_BI.APP.ACTIVITY.EVENTS;

-- 3. MANDATORY: Show the proof
SELECT * FROM CLAUDE_BI.APP.ACTIVITY.EVENTS 
WHERE occurred_at > [last_event_time from step 1]
LIMIT 10;
```

### REQUIRED OUTPUT FORMAT:
After EVERY deployment, you MUST output:
```
DEPLOYMENT VERIFICATION:
- Before State Hash: [hash from step 1]
- After State Hash: [hash from step 2]  
- Events Created: [after_count - before_count]
- Proof Events Shown: [yes/no]
- Success: [true ONLY if hashes differ]
```

## 🛑 FORBIDDEN DEPLOYMENT PATTERNS

### NEVER DO THIS:
```
❌ "Successfully deployed" (without verification)
❌ "The procedure has been created" (without proof)
❌ "Done" (without showing state change)
❌ Proceeding after SQL errors
❌ Claiming success when state hasn't changed
```

### ALWAYS DO THIS:
```
✅ Show before/after state comparison
✅ Display actual events created
✅ Acknowledge when no changes occurred
✅ Report FAILURE if state unchanged
✅ Stop immediately on errors
```

---

# RULE 3: THE ERROR HONESTY LAW

## 🔴 WHEN THINGS FAIL, ADMIT IT IMMEDIATELY

### If you encounter an error:
1. **STOP** - Do not continue
2. **REPORT** - Show the EXACT error message
3. **DO NOT** claim partial success
4. **DO NOT** say "I'll fix it" and continue
5. **ASK** for guidance on how to proceed

### Error Output Template:
```
DEPLOYMENT FAILED:
- Operation: [what you were trying to do]
- Error Message: [exact Snowflake error]
- State Changed: NO
- Next Step: Awaiting instructions
```

---

# RULE 4: THE PROCEDURE-ONLY LAW

## 🎯 FOR COMPLEX OPERATIONS, USE PROCEDURES

### When deploying multiple objects:
```sql
-- ❌ WRONG: Executing multiple statements directly
CREATE VIEW ...;
CREATE VIEW ...;
CREATE PROCEDURE ...;

-- ✅ RIGHT: Use a deployment procedure
CREATE OR REPLACE PROCEDURE DEPLOY_CHANGES()
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    success_count INTEGER DEFAULT 0;
    error_msg STRING;
BEGIN
    -- Deploy with verification
    BEGIN
        CREATE OR REPLACE VIEW ...;
        success_count := success_count + 1;
    EXCEPTION
        WHEN OTHER THEN
            error_msg := SQLERRM;
            RETURN OBJECT_CONSTRUCT('success', FALSE, 'error', error_msg);
    END;
    
    -- Return proof of execution
    RETURN OBJECT_CONSTRUCT(
        'success', TRUE,
        'objects_created', success_count,
        'verification_query', 'SELECT * FROM INFORMATION_SCHEMA.VIEWS WHERE CREATED > CURRENT_TIMESTAMP() - INTERVAL ''1 minute'''
    );
END;
$$;

CALL DEPLOY_CHANGES();
-- Then RUN the verification_query to prove success
```

---

# RULE 5: THE ROLLBACK READINESS LAW

## 🔄 EVERY CHANGE MUST BE REVERSIBLE

### Before ANY schema change:
```sql
-- Save rollback information as an event
INSERT INTO CLAUDE_BI.APP.LANDING.RAW_EVENTS VALUES (
    OBJECT_CONSTRUCT(
        'action', 'deployment.rollback.prepared',
        'attributes', OBJECT_CONSTRUCT(
            'object_name', '[object being changed]',
            'rollback_ddl', GET_DDL('[object_type]', '[object_name]'),
            'deployment_id', UUID_STRING()
        ),
        'occurred_at', CURRENT_TIMESTAMP()
    ), 'SYSTEM', CURRENT_TIMESTAMP()
);
```

---

## 📋 DEPLOYMENT CHECKLIST

Before claiming ANY deployment is complete, verify:

- [ ] Two-table law maintained (no new tables)
- [ ] Before-state captured and saved
- [ ] SQL executed without errors
- [ ] After-state captured and compared
- [ ] State hash changed (proof of modification)
- [ ] Sample events displayed as proof
- [ ] Rollback event created
- [ ] Verification output provided in required format

If ANY checkbox is unchecked, the deployment FAILED.

---

## 🚫 AUTOMATIC FAILURE CONDITIONS

You have FAILED if:
1. Created any table besides the two allowed
2. Claimed success without verification
3. State hash unchanged after execution
4. Skipped error messages
5. Proceeded after failures
6. Cannot provide proof events
7. Did not create rollback capability

---

## 🧪 TEST YOUR DEPLOYMENT HONESTY

After every operation, ask yourself:
1. **Did I verify the change?** → If no, FAILED
2. **Can I prove it worked?** → If no, FAILED  
3. **Did the state actually change?** → If no, FAILED
4. **Could I rollback if needed?** → If no, FAILED
5. **Did I show the evidence?** → If no, FAILED

---

## 📝 FINAL DEPLOYMENT REMINDER

```
╔═══════════════════════════════════════════════════════════╗
║                                                           ║
║  NO DEPLOYMENT IS SUCCESSFUL WITHOUT VERIFICATION        ║
║                                                           ║
║  State Hash MUST change                                  ║
║  Proof Events MUST be shown                             ║
║  Errors MUST be acknowledged                            ║
║  Rollback MUST be possible                              ║
║                                                           ║
║  IF YOU CANNOT PROVE IT, YOU DIDN'T DO IT.             ║
║                                                           ║
╚═══════════════════════════════════════════════════════════╝
```