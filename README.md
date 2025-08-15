# Snowflake POC2 - Complete Dashboard & Logging System with Claude Code

**Production-ready dashboard system with enforced single-path access, complete logging, and executive-friendly interfaces.**

## 🎯 Latest Updates (2025-08-15)

### ✅ Complete Claude Code Integration
- **Single enforced path**: All Snowflake access through `sf` command with RSA authentication
- **Complete lifecycle logging**: SQL, git, npm, code edits - everything logged to ACTIVITY.EVENTS
- **Session tracking**: Unique session IDs for all operations
- **Performance optimized**: Session reuse, result caching, query tagging

### ✅ Dashboard System Deployed
- **4 Core Procedures**: DASH_GET_SERIES, DASH_GET_TOPN, DASH_GET_EVENTS, DASH_GET_METRICS
- **Executive Presets**: One-click buttons for common queries
- **Auto-refresh**: 5-minute updates for mobile viewing
- **Natural Language**: Convert text to dashboard queries

### ✅ Enhanced SQL Processing
- **Robust statement splitter**: Handles procedures, dollar quotes, comments
- **Statement markers**: Zero-heuristic splitting with `-- @statement`
- **Session optimization**: AUTOCOMMIT, USE_CACHED_RESULT, stable query tags

## 🚀 Quick Start

### 1. Claude Code Access (Enforced Single Path)

```bash
# The ONLY way to access Snowflake from Claude Code
sf status                                    # Check connection
sf sql "SELECT COUNT(*) FROM ACTIVITY.EVENTS"  # Run SQL
sf exec-file scripts/dashboard-procs.sql    # Execute SQL file
sf log --action "custom.event"              # Log arbitrary events
```

### 2. Dashboard Procedures

```sql
-- Time series data
CALL MCP.DASH_GET_SERIES(
  DATEADD('hour', -24, CURRENT_TIMESTAMP()),
  CURRENT_TIMESTAMP(),
  'hour',      -- interval: minute, hour, day, week
  NULL,        -- filters
  NULL         -- group_by
);

-- Top-N ranking
CALL MCP.DASH_GET_TOPN(
  DATEADD('day', -7, CURRENT_TIMESTAMP()),
  CURRENT_TIMESTAMP(),
  'action',    -- dimension to rank
  NULL,        -- filters
  10           -- top N items
);

-- Recent events stream
CALL MCP.DASH_GET_EVENTS(
  DATEADD('minute', -5, CURRENT_TIMESTAMP()),  -- cursor
  50                                            -- limit
);

-- Summary metrics
CALL MCP.DASH_GET_METRICS(
  DATEADD('hour', -24, CURRENT_TIMESTAMP()),
  CURRENT_TIMESTAMP(),
  NULL         -- filters
);
```

## 🏛️ Architecture: The Two-Table Law

### THIS SYSTEM HAS EXACTLY TWO TABLES. ONLY TWO. FOREVER.

```sql
1. CLAUDE_BI.LANDING.RAW_EVENTS     -- All ingestion
2. CLAUDE_BI.ACTIVITY.EVENTS        -- Dynamic Table (auto-refresh)
```

**Everything else is a VIEW, PROCEDURE, or EVENT. No exceptions.**

## 🔐 Authentication & Access Control

### Claude Code Agent (RSA Key-Pair)
```bash
# Environment (hardcoded in ~/bin/sf wrapper)
SNOWFLAKE_ACCOUNT=uec18397.us-east-1
SNOWFLAKE_USERNAME=CLAUDE_CODE_AI_AGENT
SF_PK_PATH=/path/to/claude_code_rsa_key.p8
SNOWFLAKE_WAREHOUSE=CLAUDE_AGENT_WH
SNOWFLAKE_DATABASE=CLAUDE_BI
SNOWFLAKE_SCHEMA=MCP
```

### Enforcement Mechanisms
1. **sf wrapper** at `~/bin/sf` - Only allowed access path
2. **snowsql blocker** - Prevents direct SnowSQL usage
3. **Git hooks** - Auto-log commits and pushes
4. **npm script wrapping** - Log all build/test operations

## 📊 Complete Logging System

### What Gets Logged

| Category | Events | Example |
|----------|--------|---------|
| **SQL Operations** | All queries via sf | `ccode.sql.executed` |
| **Git Activity** | Commits, pushes | `git.commit`, `git.push` |
| **Build/Test** | npm scripts | `npm.test.begin`, `npm.test.end` |
| **File Operations** | Code edits | `code.edit`, `code.create` |
| **Dashboard** | Generation, viewing | `dashboard.created` |
| **Sessions** | Start/end | `ccode.session.started` |

### Monitoring Views

```sql
-- Claude Code operations
SELECT * FROM MCP.VW_CLAUDE_CODE_OPERATIONS;

-- Session summary
SELECT * FROM MCP.VW_CLAUDE_CODE_SESSIONS;

-- Daily statistics
SELECT * FROM MCP.VW_CLAUDE_CODE_DAILY_STATS;

-- Git activity
SELECT * FROM MCP.VW_GIT_COMMITS;
SELECT * FROM MCP.VW_GIT_PUSHES;

-- Build/test results
SELECT * FROM MCP.VW_BUILD_TEST_RESULTS;

-- Complete timeline
SELECT * FROM MCP.VW_ACTIVITY_TIMELINE;

-- Recent errors
SELECT * FROM MCP.VW_RECENT_ERRORS;
```

## 🎨 Dashboard System

### Executive Dashboard Features
- **One-click presets**: Today, Last Hour, This Week
- **Real-time charts**: Time series, rankings, metrics
- **Auto-refresh**: 5-minute intervals
- **Mobile responsive**: Optimized for exec viewing
- **Natural language**: Text to dashboard queries

### Dashboard HTML Interface
```bash
# Start the dashboard server
node src/server.js

# Open dashboard
open http://localhost:3000/dashboard.html
```

### Preset Configurations
- **Time Series**: Today, Last 6h (15-min), Last Hour (5-min), This Week
- **Rankings**: Top Actions, Top Users, Top Errors
- **Metrics**: Today's Summary, Last Hour Summary
- **Live Stream**: Real-time event feed

## 📁 Project Structure

```
/
├── snowflake-mcp-client/
│   ├── src/
│   │   ├── simple-cli.ts        # CLI with robust SQL splitter
│   │   └── simple-client.ts     # Optimized Snowflake client
│   └── dist/                     # Compiled JavaScript
│
├── scripts/
│   ├── dashboard-procs.sql      # Dashboard stored procedures
│   ├── dashboard-procs-simple.sql # Simplified versions
│   └── monitoring-views.sql     # Monitoring views
│
├── docs/
│   ├── snowflake-playbook.md    # Canonical patterns & best practices
│   └── MCP_ADMIN_GUIDE.md      # Admin documentation
│
├── examples/
│   └── procs/                    # Example procedures
│       ├── time_series_aggregation.sql
│       ├── dynamic_pivot.sql
│       └── ranked_results.sql
│
├── ui/
│   ├── dashboard.html           # Executive dashboard
│   └── js/
│       └── dashboard.js         # Dashboard logic
│
├── .githooks/                   # Git hooks for logging
│   ├── post-commit
│   └── pre-push
│
├── ~/bin/
│   ├── sf                       # Enforced access wrapper
│   └── snowsql                  # Blocker script
│
├── CLAUDE.md                    # Two-table law documentation
├── events.json                  # Event taxonomy
└── package.json                 # Wrapped npm scripts
```

## 🚀 Performance Optimizations

### Connection & Session
```javascript
// Optimized connection settings
{
  clientSessionKeepAlive: true,
  statementTimeout: 120
}

// Session optimizations
ALTER SESSION SET 
  AUTOCOMMIT = TRUE,
  USE_CACHED_RESULT = TRUE,
  STATEMENT_TIMEOUT_IN_SECONDS = 120,
  QUERY_TAG = 'cc-cli|session:xyz';
```

### SQL File Processing
- Statement markers for reliable splitting
- Dollar quote handling for procedures
- Comment preservation
- Batch execution on single connection

## 🔧 Development Workflow

### 1. SQL Development
```bash
# Edit SQL with statement markers
vim scripts/my-procs.sql

# Add markers before each statement
-- @statement
CREATE OR REPLACE PROCEDURE ...

# Deploy
sf exec-file scripts/my-procs.sql
```

### 2. Testing Procedures
```bash
# Test dashboard procedure
sf sql "CALL MCP.DASH_GET_METRICS(
  DATEADD('hour', -24, CURRENT_TIMESTAMP()),
  CURRENT_TIMESTAMP(),
  NULL
)"
```

### 3. Monitoring Activity
```bash
# Check recent Claude Code operations
sf sql "SELECT * FROM MCP.VW_CLAUDE_CODE_OPERATIONS LIMIT 10"

# View session summary
sf sql "SELECT * FROM MCP.VW_CLAUDE_CODE_SESSIONS"
```

## 📚 Documentation

### Core Guides
- [CLAUDE.md](./CLAUDE.md) - Two-table architecture rules
- [docs/snowflake-playbook.md](./docs/snowflake-playbook.md) - Patterns & best practices
- [NATIVE_AUTH.md](./NATIVE_AUTH.md) - Authentication guide
- [events.json](./events.json) - Event taxonomy

### Example Procedures
- [time_series_aggregation.sql](./examples/procs/time_series_aggregation.sql)
- [dynamic_pivot.sql](./examples/procs/dynamic_pivot.sql)
- [ranked_results.sql](./examples/procs/ranked_results.sql)

## 🛡️ Security Features

- **RSA key-pair authentication** for Claude Code
- **Single enforced access path** through sf wrapper
- **Complete audit trail** in ACTIVITY.EVENTS
- **Session tracking** with unique IDs
- **Query tagging** for observability
- **EXECUTE AS OWNER** procedures with controlled access

## 🎯 Key Achievements

1. **Complete Logging**: Every Claude Code operation logged
2. **Enforced Path**: Single access method, no bypasses
3. **Dashboard Ready**: 4 procedures + UI for executives
4. **Performance Optimized**: Session reuse, result caching
5. **Production Ready**: Error handling, monitoring, documentation

## ✨ Ready for Production

The system is fully operational with:
- ✅ Claude Code agent with enforced RSA authentication
- ✅ Dashboard procedures deployed and tested
- ✅ Complete logging of all operations
- ✅ Monitoring views for observability
- ✅ Git hooks for automatic logging
- ✅ Robust SQL processing with statement markers
- ✅ Performance optimizations applied
- ✅ Comprehensive documentation and examples

**All operations are logged, monitored, and auditable!**