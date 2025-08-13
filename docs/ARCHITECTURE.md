# SnowflakePOC2 Architecture

## Overview

SnowflakePOC2 is a Business Intelligence wrapper around Claude Code CLI that provides a Claude Desktop-like UI with strict Activity Schema 2.0 compliance and built-in audit capabilities.

## System Components

### 1. UI Shell (Tauri + React)
- **Purpose**: Claude Desktop clone interface
- **Technologies**: Tauri, React, TypeScript
- **Components**:
  - Chat interface for natural language queries
  - Results table with pagination and export
  - Audit panel showing verification results
  - Real-time WebSocket communication

### 2. Claude Code Bridge
- **Purpose**: Orchestrates Claude Code CLI and routes requests
- **Responsibilities**:
  - Spawns and manages Claude Code CLI processes
  - Routes Snowflake queries to SafeSQL agent
  - Triggers automatic auditing of success claims
  - Logs all activities to Activity Schema 2.0
- **WebSocket Server**: Port 8080 for UI communication

### 3. Specialized Agents

#### Snowflake Agent
- **Purpose**: Exclusive handler for all Snowflake operations
- **Constraints**: SafeSQL templates only (no raw SQL in v1)
- **Storage Strategy**:
  - Inline: ≤10 rows stored in artifacts.sample
  - Table: 10-1000 rows in artifact_data table
  - Stage: >1000 rows in internal Snowflake stage

#### Audit Agent
- **Purpose**: Automatically verifies success claims
- **Triggers**: ✅, "complete", "successfully", percentages
- **Validation Types**:
  - Percentage claims with tolerance checking
  - Completion verification against pending tasks
  - Performance metrics validation
  - Data quality assessments

### 4. Activity Schema 2.0
- **Base Stream**: analytics.activity.events
- **Required Fields**:
  - activity_id: Unique identifier
  - ts: Timestamp (UTC)
  - customer: Entity identifier
  - activity: Namespaced action (ccode.*)
  - feature_json: Activity metadata
- **Extensions**: Prefixed with underscore (_source_system, _session_id)

### 5. Pure Snowflake Storage
- **No External Dependencies**: All data stored in Snowflake
- **Storage Tiers**:
  - Small results: Direct in artifacts table
  - Medium results: artifact_data table
  - Large results: Internal stage with compression
- **Metadata Tracking**: Schema, row counts, storage location

## Data Flow

1. **User Query** → UI Shell → WebSocket → Bridge
2. **Bridge Analysis** → Detect Snowflake intent → Route to Snowflake Agent
3. **Snowflake Agent** → Validate with SafeSQL → Execute template → Store artifact
4. **Activity Logging** → Every action logged to Activity Schema
5. **Audit Trigger** → Success claim detected → Audit Agent verification
6. **Results Return** → Bridge → WebSocket → UI Shell → User

## Security Model

### Snowflake Connection
- Environment variables only (no prompting)
- Immediate context setting (USE DATABASE/SCHEMA)
- Row-level security via customer isolation

### SQL Execution
- SafeSQL templates only in v1
- Banned patterns (DROP, DELETE, etc.)
- SELECT * restricted to sample_top template
- Parameterized queries required

### Audit Requirements
- All success claims auto-verified
- Evidence-based validation
- Remediation suggestions for failures
- Victory Audit score before production

## Monitoring & Observability

### Activity Tracking
- Every user interaction logged
- SQL execution metrics captured
- Artifact creation tracked
- Audit results stored

### Performance Metrics
- First token: < 300ms target
- Card ready p95: < 8s target
- Ingestion lag p95: < 5s target
- Audit pass rate: ≥ 95% required

### Resource Management
- Snowflake Resource Monitor (100 credit quota)
- Automatic suspension at 90%
- Clustering by customer and date

## Deployment Considerations

### Prerequisites
- Node.js 16+
- Snowflake account with CLAUDE_BI database
- Claude Code CLI installed
- Environment variables configured

### Scaling Strategy
- Horizontal: Multiple Bridge instances
- Vertical: Larger Snowflake warehouse
- Caching: Internal stage for repeated queries
- Batching: Activity logger batch inserts

### Disaster Recovery
- All data in Snowflake (automatic backups)
- Activity stream as audit trail
- Artifact metadata for reconstruction
- Stage cleanup policies

## Future Enhancements (v2)

1. **Raw SQL Support** (with enhanced validation)
2. **Multi-tenant isolation** improvements
3. **Real-time dashboards** with streaming
4. **Advanced caching** strategies
5. **Query optimization** recommendations
6. **Automated performance tuning**