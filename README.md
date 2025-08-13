# SnowflakePOC2

Claude Desktop-like UI powered by Claude Code with Activity Schema 2.0 compliance, Dashboard Factory v1 complete with bulletproof hardening.

## 🎉 Dashboard Factory v1 HARDENED & PRODUCTION-READY

### Critical Improvements Applied (2025-08-13)
- ✅ **SQL Injection Prevention**: All queries use parameterized binds
- ✅ **Schema Compliance**: Guaranteed valid schemas (no 'manual' mode)
- ✅ **FQN Resolution**: Dynamic schema mapping without hardcoding
- ✅ **Graceful Degradation**: Continues without CREATE TASK privileges
- ✅ **Session Management**: Proper warehouse/database/schema context
- ✅ **Comprehensive Testing**: Full regression test suite
- ✅ **Startup Validation**: Schema verification on server start
- ✅ **Enhanced Monitoring**: Detailed health endpoints

### Activity Schema IS the Product
- ✅ **Activity-Native Views**: 6 specialized views analyzing telemetry data
- ✅ **Zero Fake Tables**: Activity Schema itself is the dataset
- ✅ **Dashboard Generation Working**: Creates views, tasks, and Streamlit code
- ✅ **Full Observability**: Every action logged to Activity stream
- ✅ **Idempotent Operations**: Consistent naming with spec hashes

### What's Working Now
- ✅ **Create dashboards from chat**: "Show activity dashboard" → working dashboard
- ✅ **Activity telemetry views**: VW_ACTIVITY_COUNTS_24H, VW_LLM_TELEMETRY, etc.
- ✅ **Automatic refresh tasks**: Scheduled with CRON expressions
- ✅ **Streamlit generation**: Full dashboard code with charts and metrics
- ✅ **Schema compliance**: Frozen spec v1 validation passing
- ✅ **Production hardening**: All 10 critical improvements implemented

## Features

- 🎯 **Looks like Claude Desktop. Runs on Claude Code.**
- 🔍 **Every claim verified by an Audit Agent.**
- 📊 **Strict Activity Schema v2 logging. No drift.**
- 🛡️ **SafeSQL templates only. No raw SQL in v1.**
- 📈 **Full SQL result tables + interactive dashboards**
- 🏭 **Dashboard Factory**: Convert conversations to dashboards in <5 minutes
- 💾 **Pure Snowflake storage. No external dependencies.**
- 🔒 **Schema Awareness**: Bulletproof schema validation and management

## Quick Start

```bash
git clone <this-repo>
cd SnowflakePOC2
npm install

# Setup environment
cp .env.example .env
# Edit .env with your Snowflake credentials

# Bootstrap Activity views (one-time setup)
node run-bootstrap.js  # Creates 6 Activity-native views

# Start the integrated server
node integrated-server.js

# Open browser to http://localhost:3000
# Try: "Show me an activity dashboard"
```

### Test Dashboard Creation
```bash
# Quick test of dashboard factory
node test-minimal-dashboard.js

# Verify created objects
node verify-dashboard.js
```

## Schema Management

The project now includes production-ready schema awareness:

```bash
# Bootstrap all required Snowflake objects (idempotent)
npm run bootstrap-schema

# Validate schema matches expectations
npm run validate-schema

# Check for hardcoded schema paths (lint)
npm run lint:schemas
```

## Architecture

### Core Components
- **UI Shell**: Web-based chat interface with real-time WebSocket communication
- **Integrated Server**: Express + WebSocket server with Claude Code CLI integration
- **Bridge**: Claude Code CLI wrapper with Activity Schema v2 logging
- **Dashboard Factory**: Converts conversations to Snowflake dashboards
- **Schema Module**: Centralized Snowflake schema configuration and validation

### Snowflake Structure
```
CLAUDE_BI (database)
├── ACTIVITY (schema)
│   └── EVENTS                        # Activity Schema v2.0 event stream
├── ACTIVITY_CCODE (schema)
│   ├── ARTIFACTS                     # Generated artifacts storage
│   ├── AUDIT_RESULTS                 # Audit verification outcomes
│   ├── VW_ACTIVITY_COUNTS_24H        # Activity breakdown (24h window)
│   ├── VW_LLM_TELEMETRY              # LLM performance metrics
│   ├── VW_SQL_EXECUTIONS             # SQL query analysis
│   ├── VW_DASHBOARD_OPERATIONS       # Dashboard lifecycle events
│   ├── VW_SAFESQL_TEMPLATES          # Template usage patterns
│   └── VW_ACTIVITY_SUMMARY           # High-level metrics
└── ANALYTICS (schema)
    └── activity_dashboard__*         # Generated dashboard views
```

### Key Features
- **SafeSQL Templates**: No raw SQL execution, only validated templates
- **Activity Schema v2.0**: Strict 14-column compliance with namespaced activities
- **Schema Validation**: Startup checks for existence, privileges, and structure
- **Environment-Aware**: All configuration from environment variables
- **Dashboard Factory**: Real-time dashboard generation with progress tracking

## Development Status

- ✅ **Phase 1: Dashboard Factory v1** (COMPLETE 2025-08-13)
  - Activity-native views created
  - Dashboard generation from chat working
  - Full Activity Schema observability
  - Streamlit code generation
  - **HARDENED**: All 10 critical improvements applied
    - SQL injection prevention via parameter binds
    - Always-valid schema generation
    - FQN resolution without hardcoding
    - Graceful degradation for missing privileges
    - Session context management
    - Comprehensive safety tests
- ✅ **Schema Awareness Implementation** (COMPLETE)
- 🚧 **Phase 2: Meta Dashboard & Analytics** (NEXT)
- 🚧 **Phase 3: Production Deployment** (READY)

## Contributing

See CLAUDE.md for mandatory coding guidelines and schema requirements.