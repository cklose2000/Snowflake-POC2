# CLAUDE.md - Mandatory Claude Code Behavior Guidance

**READ THIS FIRST**: This file defines strict behavioral requirements for Claude Code when working on this project. Violation of these guidelines wastes time and introduces risk.

## 🎯 MCP INTEGRATION - PRIMARY DATA ACCESS METHOD

**CRITICAL**: This project now uses MCP (Model Context Protocol) for all data operations. 

### ✅ USE MCP TOOLS FOR ALL QUERIES
- **compose_query_plan** - Execute queries and get results
- **list_sources** - List available data sources
- **validate_plan** - Validate query plans
- **create_dashboard** - Generate dashboards

### ❌ NEVER ACCESS SNOWFLAKE DIRECTLY
- Do NOT write raw SQL
- Do NOT use snowflake-sdk
- Do NOT access credentials

See `claude-config/CLAUDE_MCP.md` for detailed MCP usage guidelines.

## 🔒 SNOWFLAKE CONNECTION - ABSOLUTE REQUIREMENTS

### ✅ ALWAYS DO THIS

**1. Use Environment Variables ONLY**
```bash
# These variables are ALWAYS available in .env - never prompt for credentials
SNOWFLAKE_ACCOUNT=your-account.snowflakecomputing.com
SNOWFLAKE_USERNAME=your-username  
SNOWFLAKE_PASSWORD=your-password
SNOWFLAKE_DATABASE=CLAUDE_BI
SNOWFLAKE_SCHEMA=ANALYTICS
SNOWFLAKE_WAREHOUSE=CLAUDE_WAREHOUSE
SNOWFLAKE_ROLE=CLAUDE_BI_ROLE
```

**2. Use This Exact Connection Pattern**
```python
# Python snowflake-connector-python
import snowflake.connector
import os

conn = snowflake.connector.connect(
    account=os.getenv('SNOWFLAKE_ACCOUNT'),
    user=os.getenv('SNOWFLAKE_USERNAME'),
    password=os.getenv('SNOWFLAKE_PASSWORD'),
    database=os.getenv('SNOWFLAKE_DATABASE'),
    schema=os.getenv('SNOWFLAKE_SCHEMA'),
    warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
    role=os.getenv('SNOWFLAKE_ROLE')
)
```

```javascript
// Node.js snowflake-sdk
const snowflake = require('snowflake-sdk');

const connection = snowflake.createConnection({
  account: process.env.SNOWFLAKE_ACCOUNT,
  username: process.env.SNOWFLAKE_USERNAME,
  password: process.env.SNOWFLAKE_PASSWORD,
  database: process.env.SNOWFLAKE_DATABASE,
  schema: process.env.SNOWFLAKE_SCHEMA,
  warehouse: process.env.SNOWFLAKE_WAREHOUSE,
  role: process.env.SNOWFLAKE_ROLE
});
```

**3. Always Set Context Immediately After Connection**
```sql
-- These commands are ALWAYS required after connection
USE DATABASE CLAUDE_BI;
USE SCHEMA ANALYTICS;
USE WAREHOUSE CLAUDE_WAREHOUSE;
```

### ❌ NEVER DO THIS

- ❌ Prompt for credentials interactively
- ❌ Use different connection methods in different files
- ❌ Forget to set database/schema context
- ❌ Use default snowflake connection without explicit credentials
- ❌ Try alternative connection libraries without justification

---

## 🗺️ SCHEMA AWARENESS - KNOW WHAT EXISTS

### ACTUAL Snowflake Structure (THIS IS REAL - NOT NESTED!)

```sql
-- Snowflake has FLAT schemas, not nested. This is the REAL structure:
CLAUDE_BI (database)
├── ACTIVITY (schema)
│   └── EVENTS (table)          -- Full path: CLAUDE_BI.ACTIVITY.EVENTS
├── ACTIVITY_CCODE (schema)  
│   ├── ARTIFACTS (table)       -- Full path: CLAUDE_BI.ACTIVITY_CCODE.ARTIFACTS
│   └── AUDIT_RESULTS (table)   -- Full path: CLAUDE_BI.ACTIVITY_CCODE.AUDIT_RESULTS
├── ANALYTICS (schema)           -- Default schema after USE SCHEMA ANALYTICS
│   └── SCHEMA_VERSION (table)  -- Full path: CLAUDE_BI.ANALYTICS.SCHEMA_VERSION
└── PUBLIC (schema)              -- Ignore this
```

### Guaranteed Table Structures

**CLAUDE_BI.ACTIVITY.EVENTS** (Activity Schema 2.0 compliant)
```sql
-- ALWAYS EXISTS - never check if table exists
CREATE TABLE CLAUDE_BI.ACTIVITY.EVENTS (
  activity_id VARCHAR(255) NOT NULL,      -- PK
  ts TIMESTAMP_NTZ NOT NULL,              -- Event time (UTC)
  customer VARCHAR(255) NOT NULL,         -- Entity identifier  
  activity VARCHAR(255) NOT NULL,         -- Namespaced action
  feature_json VARIANT NOT NULL,          -- Activity metadata
  
  anonymous_customer_id VARCHAR(255),     -- Pre-identification tracking
  revenue_impact FLOAT,                   -- Money in/out
  link VARCHAR(255),                      -- Reference URL
  
  -- System extensions (always present)
  _source_system VARCHAR(255),            -- Always 'claude_code'
  _source_version VARCHAR(255),           -- Bridge version
  _session_id VARCHAR(255),               -- UI session tracking
  _query_tag VARCHAR(255),                -- Snowflake query correlation
  _activity_occurrence INTEGER,           -- 1st, 2nd, 3rd for customer
  _activity_repeated_at TIMESTAMP_NTZ     -- Performance optimization
);
```

**CLAUDE_BI.ACTIVITY_CCODE.ARTIFACTS**
```sql
-- ALWAYS EXISTS - artifact storage + metadata
CREATE TABLE CLAUDE_BI.ACTIVITY_CCODE.ARTIFACTS (
  artifact_id VARCHAR(255) NOT NULL,      -- PK, links to activity.events.link
  sample VARIANT,                         -- Preview (≤10 rows)
  row_count INTEGER,                      -- Full result size
  schema_json VARIANT,                    -- Column metadata
  s3_url VARCHAR(500),                    -- Full data location
  bytes BIGINT,                           -- Size metrics
  created_ts TIMESTAMP_NTZ,               -- When created
  customer VARCHAR(255),                  -- Who created it
  created_by_activity VARCHAR(255)        -- References activity.events.activity_id
);
```

### ✅ PROPER SCHEMA OPERATIONS

```javascript
// ALWAYS use the schema module - NEVER hardcode paths
const schema = require('../snowflake-schema');

// Get fully qualified name
const eventsTable = schema.getFQN('ACTIVITY', 'EVENTS');
// Returns: CLAUDE_BI.ACTIVITY.EVENTS

// Or use two-part names after setting context
const twoPartName = schema.getTwoPartName('ACTIVITY', 'EVENTS');
// Returns: ACTIVITY.EVENTS
```

```sql
-- With context set (USE DATABASE CLAUDE_BI; USE SCHEMA ANALYTICS)
SELECT * FROM ACTIVITY.EVENTS WHERE activity = 'ccode.sql_executed';

-- Always use parameterized queries
SELECT * FROM ACTIVITY.EVENTS WHERE customer = ? AND ts > ?;

-- Always limit large result sets  
SELECT * FROM ACTIVITY.EVENTS ORDER BY ts DESC LIMIT 1000;
```

### ❌ BANNED SCHEMA OPERATIONS

- ❌ Hardcoding table paths like `'analytics.activity.events'` - USE THE SCHEMA MODULE
- ❌ `SHOW TABLES` or `DESCRIBE` commands (you know what exists)
- ❌ `SELECT * FROM information_schema` exploration
- ❌ Checking if tables exist with `IF EXISTS` 
- ❌ Using unqualified table names like `SELECT * FROM events`
- ❌ Running discovery queries like `SHOW SCHEMAS`
- ❌ Writing `analytics.activity.events` anywhere - it's `CLAUDE_BI.ACTIVITY.EVENTS` or use schema module!

---

## 🛡️ SAFESQL ENFORCEMENT

### Only These Templates Are Allowed (v1)

```javascript
// packages/safesql/templates.js - these are the ONLY allowed SQL patterns
const ALLOWED_TEMPLATES = [
  'describe_table',    // Table structure
  'sample_top',        // SELECT * FROM table LIMIT n (ONLY exception for SELECT *)
  'top_n',            // Ranked results by metric
  'time_series',      // Time-based aggregation
  'breakdown',        // Group by analysis  
  'comparison'        // Before/after analysis
];
```

### ✅ ALLOWED SQL PATTERNS
```sql
-- Template: sample_top (ONLY place SELECT * is allowed)
SELECT * FROM analytics.activity.events LIMIT 10;

-- Template: top_n  
SELECT customer, COUNT(*) as query_count
FROM analytics.activity.events 
WHERE activity = 'ccode.sql_executed'
GROUP BY customer 
ORDER BY query_count DESC 
LIMIT 10;

-- Template: time_series
SELECT DATE_TRUNC('hour', ts) as hour, COUNT(*) as events
FROM analytics.activity.events
WHERE ts > CURRENT_TIMESTAMP - INTERVAL '24 hours'
GROUP BY hour
ORDER BY hour;
```

### ❌ BANNED SQL PATTERNS
- ❌ Raw SQL execution (wait for v2)
- ❌ `SELECT *` except in `sample_top` template
- ❌ Unparameterized WHERE clauses
- ❌ DDL operations (CREATE, DROP, ALTER)
- ❌ DML operations (INSERT, UPDATE, DELETE) except through bridge
- ❌ Subqueries without templates

---

## 🎯 ACTIVITY SCHEMA COMPLIANCE

### Required Activity Namespacing
```sql
-- All activities MUST use 'ccode.' prefix
INSERT INTO analytics.activity.events (
  activity_id, ts, customer, activity, feature_json
) VALUES (
  'act_' || UUID(),
  CURRENT_TIMESTAMP,
  'user_123',
  'ccode.sql_executed',  -- ✅ Properly namespaced
  '{"template": "top_n", "rows_returned": 150}'
);
```

### Standard Activity Types
```sql
'ccode.user_asked'        -- User submitted a question
'ccode.sql_executed'      -- Snowflake query run
'ccode.artifact_created'  -- Result artifact generated  
'ccode.audit_passed'      -- Audit verification succeeded
'ccode.audit_failed'      -- Audit verification failed
'ccode.bridge_started'    -- Bridge process launched
'ccode.agent_invoked'     -- Subagent called
```

### Self-Join Query Pattern (Activity Schema 2.0)
```sql
-- ✅ ALWAYS join activity stream to itself using customer + timestamp
SELECT 
  sql_exec.customer,
  sql_exec.feature_json:template as template_used,
  audit.feature_json:passed as audit_result
FROM analytics.activity.events sql_exec
LEFT JOIN analytics.activity.events audit
  ON (sql_exec.customer = audit.customer 
      AND audit.ts BETWEEN sql_exec.ts AND sql_exec.ts + INTERVAL '30 seconds')
WHERE sql_exec.activity = 'ccode.sql_executed'
  AND audit.activity LIKE 'ccode.audit_%';
```

---

## 🚨 ERROR HANDLING REQUIREMENTS

### ✅ PROPER ERROR HANDLING
```python
try:
    conn = snowflake.connector.connect(**connection_params)
    conn.cursor().execute("USE DATABASE CLAUDE_BI")
except snowflake.connector.errors.DatabaseError as e:
    print(f"Database error: {e}")
    # Log error but DO NOT re-prompt for credentials
    raise
except Exception as e:
    print(f"Connection error: {e}")
    # Check .env file exists, then raise
    raise
```

### ❌ BANNED ERROR HANDLING
- ❌ Catching connection errors and prompting for new credentials
- ❌ Switching to different connection methods on failure
- ❌ Ignoring errors and continuing without proper connection
- ❌ Using try/except to "discover" if tables exist

---

## 📋 PRE-FLIGHT CHECKLIST

**Before ANY Snowflake operation, verify:**

1. ✅ Environment variables loaded from `.env`
2. ✅ Connection uses exact pattern above  
3. ✅ Database/schema context set immediately
4. ✅ Query uses SafeSQL template or falls within allowed patterns
5. ✅ Table names fully qualified (`analytics.activity.events`)
6. ✅ Activity names properly namespaced (`ccode.*`)
7. ✅ Error handling does not re-prompt for credentials

---

## 🔍 DEBUGGING PROTOCOL

**If Snowflake operations fail:**

1. **Check .env file** - verify all variables are set
2. **Check network** - can you reach the account URL?
3. **Check permissions** - is the role/warehouse accessible?  
4. **Check query syntax** - is it using allowed templates?
5. **Log the error** - but do not explore/discover
6. **Fail fast** - do not continue with partial connection

**DO NOT:**
- ❌ Run exploratory queries to "figure out" the schema
- ❌ Try alternative connection methods
- ❌ Prompt user for different credentials
- ❌ Continue operating with degraded functionality

---

## 💡 WHY THESE RULES EXIST

1. **Consistency**: Same connection method every time eliminates variables
2. **Security**: Environment variables prevent credential leakage  
3. **Performance**: No discovery queries means faster operations
4. **Reliability**: Known schema structure prevents exploration failures
5. **Compliance**: Activity Schema 2.0 enables portable queries
6. **Auditability**: All operations are logged as activities

**Remember**: This system observes itself. Every action you take becomes data for improvement. Make those actions predictable and consistent.