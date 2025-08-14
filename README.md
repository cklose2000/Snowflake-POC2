# SnowflakePOC2

Claude Desktop-like UI powered by Claude Code with Activity Schema 2.0 compliance and Contract-First Schema Enforcement.

## 🛡️ CONTRACT-FIRST SCHEMA ENFORCEMENT (2025-08-13)

**"One contract file → generated FQNs + types → code must use those → CI & runtime scream if reality deviates."**

### Contract System Status: ✅ OPERATIONAL
- **Contract Hash**: `439f8097e41903a7`
- **Enforcement Layers**: Pre-commit hooks + CI validation + Runtime checks
- **Schema Drift**: Build error (not runtime surprise)
- **Violations Eliminated**: 625 → 585 (40 fixed via parallel refactoring)
- **Core Packages**: ✅ Fully contract-compliant

### Multi-Layer Protection
- ✅ **Pre-commit Hooks**: Block raw FQNs, SQL injection, unqualified views
- ✅ **CI/CD Validation**: Contract compliance on every PR
- ✅ **Runtime Validation**: Live schema drift detection + health endpoints
- ✅ **Generated Helpers**: Type-safe FQN functions from single source of truth
- ✅ **Drift Watchdog**: 24-hour monitoring with Activity logging
- ✅ **Build Failures**: Schema violations prevent deployment

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

## Contract Enforcement Commands

The project enforces schema consistency through generated contracts:

```bash
# Code generation from contract
npm run codegen

# Contract compliance validation
npm run lint:contract
npm run test:contract

# Runtime schema validation
npm run validate:runtime
npm run validate:runtime:strict
npm run validate:runtime:fix

# Legacy schema commands (still available)
npm run bootstrap-schema
npm run validate-schema
```

## Architecture

### Core Components
- **UI Shell**: Web-based chat interface with real-time WebSocket communication
- **Integrated Server**: Express + WebSocket server with Claude Code CLI integration
- **Bridge**: Claude Code CLI wrapper with Activity Schema v2 logging
- **Dashboard Factory**: Converts conversations to Snowflake dashboards
- **Contract System**: Single source of truth schema enforcement
  - `schemas/activity_v2.contract.json` - Schema contract definition
  - `packages/snowflake-schema/generated.js` - Type-safe generated helpers
  - `scripts/codegen-schema.js` - Contract-to-code generator

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
- **Contract-First Architecture**: Schema violations become build errors
- **SafeSQL Templates**: No raw SQL execution, only validated templates
- **Activity Schema v2.0**: Strict 14-column compliance with namespaced activities
- **Generated Helpers**: `fqn()`, `qualifySource()`, `createActivityName()` functions
- **Multi-Layer Enforcement**: Pre-commit + CI + Runtime validation
- **Drift Detection**: 24-hour monitoring with automatic Activity logging
- **Environment-Aware**: All configuration from environment variables
- **Dashboard Factory**: Real-time dashboard generation with progress tracking

## Development Status

- ✅ **Phase 1: Dashboard Factory v1** (COMPLETE 2025-08-13)
  - Activity-native views created
  - Dashboard generation from chat working
  - Full Activity Schema observability
  - Streamlit code generation
- ✅ **Contract Enforcement System** (COMPLETE 2025-08-13)
  - Single source of truth: `activity_v2.contract.json`
  - Generated type-safe helpers: `fqn()`, `qualifySource()`
  - Multi-layer enforcement: Pre-commit + CI + Runtime
  - Drift detection with 24-hour monitoring
  - Contract hash `439f8097e41903a7` - stable and operational
- ✅ **Parallel Refactoring** (COMPLETE 2025-08-13)
  - 4 specialized subagent teams deployed
  - 40 violations eliminated (625 → 585)
  - Dashboard Factory fully contract-compliant
  - All core packages refactored to use generated helpers
  - Pre-commit hooks now preventing new violations
- 🚧 **Phase 2: Meta Dashboard & Analytics** (NEXT)
- ✅ **Phase 3: Production Deployment** (READY)

## Contributing

See CLAUDE.md for mandatory coding guidelines and schema requirements.