# API Reference - Dashboard Procedures

**Complete reference for all dashboard analytics procedures**

## 🎯 Overview

The dashboard system provides 5 core procedures that handle all common analytics needs:

| Procedure | Purpose | Best For |
|-----------|---------|----------|
| `DASH_GET_SERIES` | Time series data | Trends, charts, historical analysis |
| `DASH_GET_TOPN` | Rankings and top lists | Leaderboards, most active items |
| `DASH_GET_EVENTS` | Event streaming | Real-time feeds, recent activity |
| `DASH_GET_METRICS` | Summary statistics | KPIs, counts, aggregates |
| `DASH_GET_PIVOT` | Cross-tabulation | Correlation analysis, breakdowns |

All procedures work with the unified event data model and return JSON results ready for visualization.

## 📊 DASH_GET_SERIES

**Purpose**: Generate time series data with automatic interval grouping

### Signature
```sql
CALL MCP.DASH_GET_SERIES(
  start_time TIMESTAMP,
  end_time TIMESTAMP, 
  interval VARCHAR,      -- 'minute', 'hour', 'day', 'week'
  filters VARIANT,       -- Optional: WHERE conditions
  group_by VARCHAR       -- Optional: Additional grouping
)
```

### Examples

#### Basic Time Series
```sql
-- Activity over the last 24 hours, grouped by hour
CALL MCP.DASH_GET_SERIES(
  DATEADD('hour', -24, CURRENT_TIMESTAMP()),
  CURRENT_TIMESTAMP(),
  'hour',
  NULL,
  NULL
);
```

#### Filtered Time Series
```sql
-- Error events over the last week, grouped by day
CALL MCP.DASH_GET_SERIES(
  DATEADD('week', -1, CURRENT_TIMESTAMP()),
  CURRENT_TIMESTAMP(),
  'day',
  PARSE_JSON('{"action": "error.*"}'),
  NULL
);
```

#### Grouped Time Series
```sql
-- Activity by action type over the last 6 hours
CALL MCP.DASH_GET_SERIES(
  DATEADD('hour', -6, CURRENT_TIMESTAMP()),
  CURRENT_TIMESTAMP(),
  'minute',
  NULL,
  'action'
);
```

### Response Format
```json
[
  {
    "time_bucket": "2025-08-16 12:00:00",
    "count": 145,
    "group_by_value": "user.login" // if group_by specified
  },
  ...
]
```

## 🏆 DASH_GET_TOPN

**Purpose**: Generate rankings and top-N lists

### Signature
```sql
CALL MCP.DASH_GET_TOPN(
  start_time TIMESTAMP,
  end_time TIMESTAMP,
  dimension VARCHAR,     -- Column to rank by
  filters VARIANT,       -- Optional: WHERE conditions  
  limit_n INTEGER        -- Top N items to return
)
```

### Examples

#### Top Actions
```sql
-- Top 10 most frequent actions today
CALL MCP.DASH_GET_TOPN(
  DATEADD('day', -1, CURRENT_TIMESTAMP()),
  CURRENT_TIMESTAMP(),
  'action',
  NULL,
  10
);
```

#### Top Users (with filters)
```sql
-- Top 5 most active users for specific actions
CALL MCP.DASH_GET_TOPN(
  DATEADD('hour', -6, CURRENT_TIMESTAMP()),
  CURRENT_TIMESTAMP(),
  'actor_id',
  PARSE_JSON('{"action": "user.*"}'),
  5
);
```

#### Top Error Sources
```sql
-- Top sources of errors this week
CALL MCP.DASH_GET_TOPN(
  DATEADD('week', -1, CURRENT_TIMESTAMP()),
  CURRENT_TIMESTAMP(),
  'source',
  PARSE_JSON('{"action": "error.*"}'),
  10
);
```

### Response Format
```json
[
  {
    "dimension_value": "user.login",
    "count": 1234,
    "rank": 1,
    "percentage": 23.4
  },
  ...
]
```

## 📡 DASH_GET_EVENTS

**Purpose**: Stream recent events for real-time monitoring

### Signature
```sql
CALL MCP.DASH_GET_EVENTS(
  cursor_time TIMESTAMP,  -- Show events after this time
  limit_rows INTEGER      -- Maximum rows to return
)
```

### Examples

#### Recent Activity Stream
```sql
-- Last 50 events
CALL MCP.DASH_GET_EVENTS(
  DATEADD('minute', -5, CURRENT_TIMESTAMP()),
  50
);
```

#### Continuous Polling
```sql
-- Get events since last poll (for real-time updates)
CALL MCP.DASH_GET_EVENTS(
  '2025-08-16 12:30:00',  -- Last seen timestamp
  100
);
```

### Response Format
```json
[
  {
    "event_id": "uuid-here",
    "action": "user.login",
    "actor_id": "alice",
    "source": "WEB_APP",
    "occurred_at": "2025-08-16 12:30:15",
    "attributes": {"ip": "192.168.1.1"}
  },
  ...
]
```

## 📈 DASH_GET_METRICS

**Purpose**: Calculate summary metrics and KPIs

### Signature
```sql
CALL MCP.DASH_GET_METRICS(
  start_time TIMESTAMP,
  end_time TIMESTAMP,
  filters VARIANT        -- Optional: WHERE conditions
)
```

### Examples

#### Basic Metrics
```sql
-- Overall activity metrics for today
CALL MCP.DASH_GET_METRICS(
  DATEADD('day', -1, CURRENT_TIMESTAMP()),
  CURRENT_TIMESTAMP(),
  NULL
);
```

#### Error Rate Metrics
```sql
-- Error-specific metrics
CALL MCP.DASH_GET_METRICS(
  DATEADD('hour', -1, CURRENT_TIMESTAMP()),
  CURRENT_TIMESTAMP(),
  PARSE_JSON('{"action": "error.*"}')
);
```

### Response Format
```json
{
  "total_events": 12543,
  "unique_users": 234,
  "unique_actions": 15,
  "events_per_minute": 8.7,
  "time_range_hours": 24,
  "most_common_action": "user.page_view",
  "most_active_hour": "14:00"
}
```

## 🔄 DASH_GET_PIVOT

**Purpose**: Cross-tabulation and correlation analysis

### Signature
```sql
CALL MCP.DASH_GET_PIVOT(
  start_time TIMESTAMP,
  end_time TIMESTAMP,
  row_dimension VARCHAR,    -- Rows in pivot table
  col_dimension VARCHAR,    -- Columns in pivot table
  filters VARIANT          -- Optional: WHERE conditions
)
```

### Examples

#### Action by Source Matrix
```sql
-- Cross-tab of actions vs sources
CALL MCP.DASH_GET_PIVOT(
  DATEADD('day', -1, CURRENT_TIMESTAMP()),
  CURRENT_TIMESTAMP(),
  'action',
  'source',
  NULL
);
```

#### User Activity Patterns
```sql
-- Users vs time-of-day patterns
CALL MCP.DASH_GET_PIVOT(
  DATEADD('week', -1, CURRENT_TIMESTAMP()),
  CURRENT_TIMESTAMP(),
  'EXTRACT(hour FROM occurred_at)',
  'EXTRACT(dow FROM occurred_at)',
  PARSE_JSON('{"action": "user.*"}')
);
```

### Response Format
```json
[
  {
    "row_value": "user.login",
    "col_value": "WEB_APP", 
    "count": 456,
    "row_total": 1200,
    "col_total": 800,
    "percentage_of_total": 12.3
  },
  ...
]
```

## 🔍 Advanced Filtering

All procedures accept a `filters` parameter with JSON query conditions:

### Filter Examples
```json
// Single condition
{"action": "user.login"}

// Pattern matching
{"action": "user.*"}

// Multiple conditions (AND)
{"action": "user.login", "source": "WEB_APP"}

// Time-based filtering (handled automatically)
{"occurred_at": ">=2025-08-16 12:00:00"}

// Attribute filtering
{"attributes.ip": "192.168.*"}
```

### Complex Filters
```sql
-- Multiple patterns
PARSE_JSON('{"action": ["user.login", "user.logout"], "source": "WEB_APP"}')

-- Exclude patterns
PARSE_JSON('{"action": "!error.*"}')

-- Numeric ranges
PARSE_JSON('{"attributes.duration": ">1000"}')
```

## 🚀 Performance Tips

### Optimal Time Ranges
- **Real-time**: Last 5-15 minutes
- **Hourly analysis**: Last 6-24 hours  
- **Daily trends**: Last 7-30 days
- **Weekly patterns**: Last 8-12 weeks

### Efficient Filtering
```sql
-- Good: Specific time ranges
DATEADD('hour', -6, CURRENT_TIMESTAMP())

-- Good: Action pattern filtering
PARSE_JSON('{"action": "user.*"}')

-- Avoid: Very large time ranges without filters
-- Avoid: Complex nested attribute filtering
```

### Caching Behavior
- Results are cached for 60 seconds
- Time series data cached longer for stable intervals
- Use consistent time boundaries for better cache hit rates

## 🔐 Security & Access

### Authentication Required
All procedures require authenticated access through the Claude Code sf wrapper:
```bash
sf sql "CALL MCP.DASH_GET_SERIES(...)"
```

### Data Access
- Procedures run as `EXECUTE AS OWNER`
- Only access `ACTIVITY.EVENTS` table
- No direct table access allowed
- Complete audit trail of all calls

### Rate Limiting
- No explicit rate limits
- Snowflake warehouse auto-scaling handles load
- Large queries may timeout after 120 seconds

## 📞 Support

### Debugging Failed Calls
```sql
-- Check recent errors
SELECT * FROM MCP.VW_RECENT_ERRORS WHERE action LIKE 'mcp.dash%';

-- Verify procedure exists  
SHOW PROCEDURES IN SCHEMA MCP;

-- Test with minimal parameters
CALL MCP.DASH_GET_EVENTS(CURRENT_TIMESTAMP(), 1);
```

### Common Issues
- **Empty results**: Check time range and filters
- **Timeout errors**: Reduce time range or add filters
- **Permission denied**: Verify Claude Code authentication
- **Invalid JSON**: Check filter syntax

---

## 🎯 Quick Reference

```sql
-- Time series (last 24h by hour)
CALL MCP.DASH_GET_SERIES(DATEADD('day',-1,CURRENT_TIMESTAMP()), CURRENT_TIMESTAMP(), 'hour', NULL, NULL);

-- Top 10 actions (today)  
CALL MCP.DASH_GET_TOPN(DATEADD('day',-1,CURRENT_TIMESTAMP()), CURRENT_TIMESTAMP(), 'action', NULL, 10);

-- Recent events (last 50)
CALL MCP.DASH_GET_EVENTS(DATEADD('minute',-5,CURRENT_TIMESTAMP()), 50);

-- Summary metrics (last hour)
CALL MCP.DASH_GET_METRICS(DATEADD('hour',-1,CURRENT_TIMESTAMP()), CURRENT_TIMESTAMP(), NULL);

-- Cross-tab actions vs sources (today)
CALL MCP.DASH_GET_PIVOT(DATEADD('day',-1,CURRENT_TIMESTAMP()), CURRENT_TIMESTAMP(), 'action', 'source', NULL);
```

**All procedures return JSON results optimized for dashboard visualization and business intelligence.**