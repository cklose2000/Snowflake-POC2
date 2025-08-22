# HappyFox to Snowflake Activity Schema Ingestion

## Overview

This implementation provides a **two-table compliant** ingestion pipeline for HappyFox support ticket data into Snowflake's Activity Schema. It maintains architectural purity with only `LANDING.RAW_EVENTS` and `ACTIVITY.EVENTS` (Dynamic Table) as physical tables, with all analytics provided through views.

## Architecture

```
HappyFox API → JSONL File → Snowflake Stage → RAW_EVENTS → EVENTS (Dynamic) → Analytics Views
```

### Key Principles
- **Two-Table Law**: Only RAW_EVENTS and EVENTS tables exist
- **Event-Driven**: Each ticket is an event with `action='happyfox.ticket.upserted'`
- **Idempotent**: Duplicate prevention via hash of ticket_id + last_modified
- **Insert-Only**: No updates, each ticket version is a new event
- **View-Based Analytics**: All queries through views, no physical marts

## File Structure

```
scripts/happyfox-ingest/
├── 01_deploy_happyfox_ingest.sql    # Core ingestion setup
├── 02_enhance_dynamic_table.sql      # Dynamic table configuration
├── 03_happyfox_views.sql             # Analytics views
├── 04_catalog_registration.sql       # View discovery catalog
├── 06_incremental_task.sql           # Automated daily loads
├── 07_monitoring.sql                  # Pipeline monitoring
├── 08_test_suite.sql                 # Validation tests
├── upload_data.sh                     # Data upload helper
└── README.md                          # This file
```

## Quick Start

### 1. Deploy Infrastructure

```bash
# Deploy all components
SF_PK_PATH=./claude_code_rsa_key.p8 ~/bin/sf exec-file scripts/happyfox-ingest/01_deploy_happyfox_ingest.sql
SF_PK_PATH=./claude_code_rsa_key.p8 ~/bin/sf exec-file scripts/happyfox-ingest/02_enhance_dynamic_table.sql
SF_PK_PATH=./claude_code_rsa_key.p8 ~/bin/sf exec-file scripts/happyfox-ingest/03_happyfox_views.sql
SF_PK_PATH=./claude_code_rsa_key.p8 ~/bin/sf exec-file scripts/happyfox-ingest/04_catalog_registration.sql
SF_PK_PATH=./claude_code_rsa_key.p8 ~/bin/sf exec-file scripts/happyfox-ingest/06_incremental_task.sql
SF_PK_PATH=./claude_code_rsa_key.p8 ~/bin/sf exec-file scripts/happyfox-ingest/07_monitoring.sql
```

### 2. Upload Data

```bash
# Upload JSONL file to Snowflake stage
./scripts/happyfox-ingest/upload_data.sh
```

### 3. Load Data

```bash
# Run initial historical load
SF_PK_PATH=./claude_code_rsa_key.p8 ~/bin/sf sql "CALL CLAUDE_BI.LANDING.LOAD_HAPPYFOX_HISTORICAL();"

# Check load status
SF_PK_PATH=./claude_code_rsa_key.p8 ~/bin/sf sql "SELECT * FROM TABLE(CLAUDE_BI.LANDING.GET_HAPPYFOX_LOAD_STATUS());"
```

### 4. Verify

```bash
# Run test suite
SF_PK_PATH=./claude_code_rsa_key.p8 ~/bin/sf sql "CALL CLAUDE_BI.MCP.RUN_HAPPYFOX_TESTS();"

# Check two-table compliance
SF_PK_PATH=./claude_code_rsa_key.p8 ~/bin/sf sql "
SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE
FROM CLAUDE_BI.INFORMATION_SCHEMA.TABLES
WHERE TABLE_CATALOG = 'CLAUDE_BI'
  AND TABLE_SCHEMA IN ('LANDING', 'ACTIVITY')
  AND TABLE_TYPE IN ('BASE TABLE', 'DYNAMIC TABLE');"
```

## Available Views

All analytics are accessed through views in the `MCP` schema:

| View Name | Description | Key Metrics |
|-----------|-------------|-------------|
| `VW_HF_TICKETS` | Current state of all tickets | ticket_id, status, assignee, created_at |
| `VW_HF_TICKET_AGING` | Aging analysis with buckets | age_days, age_bucket, lifecycle_state |
| `VW_HF_CUSTOM_FIELDS` | EAV pattern for custom fields | field_name, field_value, is_required |
| `VW_HF_TICKET_TAGS` | Many-to-many tags | ticket_id, tag |
| `VW_HF_TICKET_HISTORY` | Complete change history | version_time, change_type |
| `VW_HF_PRODUCT_SUMMARY` | Aggregated by product | ticket_count, avg_age_days |
| `VW_HF_SLA_BREACHES` | SLA violations | breach_time, sla_name |

### Discovery

```sql
-- Find available views
SELECT * FROM MCP.VW_HAPPYFOX_CATALOG;

-- Search for specific views
SELECT * FROM TABLE(MCP.DISCOVER_HAPPYFOX_VIEWS('aging'));
```

## Common Queries

### Open Ticket Aging
```sql
SELECT 
    age_bucket,
    COUNT(*) as ticket_count,
    AVG(age_days) as avg_age
FROM MCP.VW_HF_TICKET_AGING
WHERE lifecycle_state = 'Open'
GROUP BY age_bucket
ORDER BY MIN(age_days);
```

### Product Analysis
```sql
SELECT * 
FROM MCP.VW_HF_PRODUCT_SUMMARY
WHERE lifecycle_state = 'Open'
ORDER BY ticket_count DESC;
```

### Recent Changes
```sql
SELECT *
FROM MCP.VW_HF_TICKET_HISTORY
WHERE change_type != 'Updated'
  AND version_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
ORDER BY version_time DESC;
```

## Monitoring

### Pipeline Health
```sql
SELECT * FROM MCP.VW_HF_PIPELINE_HEALTH;
```

### Load History
```sql
SELECT * FROM MCP.VW_HF_LOAD_HISTORY
ORDER BY load_time DESC
LIMIT 10;
```

### Data Quality
```sql
SELECT * FROM MCP.VW_HF_DATA_QUALITY;
```

## Incremental Loads

### Enable Daily Task
```sql
-- Enable automated daily loads
CALL CLAUDE_BI.LANDING.ENABLE_HAPPYFOX_TASKS();

-- Check task status
SELECT * FROM CLAUDE_BI.LANDING.VW_HAPPYFOX_TASK_STATUS;
```

### Manual Load
```sql
-- Trigger manual incremental load
CALL CLAUDE_BI.LANDING.RUN_HAPPYFOX_LOAD_NOW();
```

## Data Flow

1. **Extract**: HappyFox API → JSONL file (complete ticket JSON)
2. **Stage**: Upload JSONL to Snowflake internal stage
3. **Transform**: Each ticket becomes event with `action='happyfox.ticket.upserted'`
4. **Load**: Insert into RAW_EVENTS with idempotency key
5. **Propagate**: Dynamic Table projects to ACTIVITY.EVENTS
6. **Analyze**: Views provide analytics without creating tables

## Event Structure

Each HappyFox ticket becomes an event:

```json
{
  "event_id": "uuid",
  "action": "happyfox.ticket.upserted",
  "actor_id": "SYSTEM",
  "source": "HAPPYFOX",
  "object_type": "ticket",
  "object_id": "502843",
  "display_id": "#GZ00502843",
  "attributes": {
    "ticket_data": {/* complete ticket JSON */},
    "status": "New",
    "priority": "Medium",
    "category": "ChamberMaster/MemberZone",
    "subject": "Ticket subject",
    "assignee": "Agent Name",
    "product_prefix": "GZ"
  },
  "idempotency_key": "sha256_hash",
  "occurred_at": "2025-08-22T10:31:46Z"
}
```

## Troubleshooting

### No Data After Load
```sql
-- Check if events are in RAW_EVENTS
SELECT COUNT(*) FROM LANDING.RAW_EVENTS 
WHERE DATA:action = 'happyfox.ticket.upserted';

-- Check Dynamic Table lag
SHOW DYNAMIC TABLES LIKE 'EVENTS' IN SCHEMA ACTIVITY;
```

### Duplicate Prevention
```sql
-- Find any duplicates (should return 0)
SELECT DATA:idempotency_key, COUNT(*)
FROM LANDING.RAW_EVENTS
WHERE DATA:action = 'happyfox.ticket.upserted'
GROUP BY DATA:idempotency_key
HAVING COUNT(*) > 1;
```

### View Performance
```sql
-- Add search optimization if needed
ALTER TABLE ACTIVITY.EVENTS ADD SEARCH OPTIMIZATION
ON EQUALITY(action, object_id, display_id);
```

## Best Practices

1. **Always verify two-table compliance** after any changes
2. **Use views for all analytics** - never create physical tables
3. **Monitor pipeline health** regularly via monitoring views
4. **Run test suite** after deployments
5. **Use idempotency keys** to prevent duplicates
6. **Catalog all new views** for discoverability

## Support

For issues or questions:
1. Check monitoring views for pipeline status
2. Run test suite to validate configuration
3. Review load history for error messages
4. Verify two-table compliance

## License

Internal use only - GrowthZone SnowflakePOC2