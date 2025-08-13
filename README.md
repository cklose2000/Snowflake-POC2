# SnowflakePOC2

Claude Desktop-like UI powered by Claude Code with Activity Schema 2.0 compliance, Dashboard Factory, and production-ready schema awareness.

## 🚀 Recent Updates

### Dashboard Factory v1 Integration (Phase 1 Complete)
- ✅ WebSocket-based dashboard creation from conversation context
- ✅ Real-time progress updates and status tracking
- ✅ Activity Schema v2.0 strict compliance
- ✅ Comprehensive preflight checks with fail-fast validation
- ✅ BI-First Smart Routing for dashboard patterns

### Production-Ready Schema Awareness
- ✅ **Centralized Schema Module**: Single source of truth for all Snowflake objects
- ✅ **Startup Validation**: Validates schema structure, privileges, and Activity Schema v2
- ✅ **Environment-Aware**: No hardcoded database names, pulls from environment
- ✅ **Clear Error Messages**: Shows exactly what's wrong and how to fix it
- ✅ **Bootstrap Script**: Idempotent setup for consistent environments

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

# Setup environment and bootstrap Snowflake schema
cp .env.example .env
# Edit .env with your Snowflake credentials
npm run bootstrap-schema  # Creates required schemas and tables
npm run validate-schema   # Verify everything is set up correctly

# Start the integrated server
node integrated-server.js

# Or run individual components
npm run dev
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
│   └── EVENTS              # Activity Schema v2.0 event stream
├── ACTIVITY_CCODE (schema)
│   ├── ARTIFACTS          # Generated artifacts storage
│   └── AUDIT_RESULTS      # Audit verification outcomes
└── ANALYTICS (schema)
    └── SCHEMA_VERSION     # Schema migration tracking
```

### Key Features
- **SafeSQL Templates**: No raw SQL execution, only validated templates
- **Activity Schema v2.0**: Strict 14-column compliance with namespaced activities
- **Schema Validation**: Startup checks for existence, privileges, and structure
- **Environment-Aware**: All configuration from environment variables
- **Dashboard Factory**: Real-time dashboard generation with progress tracking

## Development Status

- ✅ Phase 1: Dashboard Factory Integration (COMPLETE)
- ✅ Schema Awareness Implementation (COMPLETE)
- 🚧 Phase 2: Meta Dashboard & Analytics (PENDING)
- 🚧 Phase 3: Production Deployment (PENDING)

## Contributing

See CLAUDE.md for mandatory coding guidelines and schema requirements.