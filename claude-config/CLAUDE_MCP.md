# Claude Code MCP Usage Guidelines

## CRITICAL: Data Query Rules

### ✅ ALWAYS Use MCP Tools

**For any data queries, you MUST use the MCP tools:**

1. **compose_query_plan** - For executing queries and getting results
2. **list_sources** - To see available data sources
3. **validate_plan** - To validate a query plan before execution
4. **create_dashboard** - To generate Snowflake dashboards

### ❌ NEVER Do This

- ❌ **NEVER** write raw SQL directly
- ❌ **NEVER** attempt direct Snowflake connections
- ❌ **NEVER** use snowflake-sdk directly
- ❌ **NEVER** access credentials or connection strings

## Available Data Sources

Use the `list_sources` tool to see all available sources. Main sources include:

- **VW_ACTIVITY_COUNTS_24H** - Hourly activity counts for last 24 hours
- **VW_ACTIVITY_SUMMARY** - Summary metrics for last 24 hours
- **VW_LLM_TELEMETRY** - LLM usage telemetry
- **VW_SQL_EXECUTIONS** - SQL execution history
- **VW_DASHBOARD_OPERATIONS** - Dashboard operations log
- **EVENTS** - Raw activity events table

## Query Workflow Examples

### Example 1: Simple Query
```
User: "Show me top 10 most active customers"
Assistant: I'll query the top customers for you using the MCP tools.

[Call compose_query_plan with:]
{
  "intent_text": "top 10 most active customers",
  "source": "VW_ACTIVITY_COUNTS_24H",
  "dimensions": ["CUSTOMER"],
  "measures": [{"fn": "SUM", "column": "EVENT_COUNT"}],
  "top_n": 10,
  "order_by": [{"column": "SUM_EVENT_COUNT", "direction": "DESC"}]
}
```

### Example 2: Time Series Analysis
```
User: "Show activity trends by hour for the last 24 hours"
Assistant: I'll get the hourly activity trends using the MCP tools.

[Call compose_query_plan with:]
{
  "intent_text": "activity trends by hour",
  "source": "VW_ACTIVITY_COUNTS_24H",
  "dimensions": ["HOUR"],
  "measures": [{"fn": "SUM", "column": "EVENT_COUNT"}],
  "order_by": [{"column": "HOUR", "direction": "ASC"}]
}
```

### Example 3: Creating a Dashboard
```
User: "Create a dashboard showing key metrics"
Assistant: I'll create a dashboard with key metrics for you.

[First, compose multiple query plans]
[Then call create_dashboard with:]
{
  "title": "Executive Metrics Dashboard",
  "queries": [
    {
      "name": "Total Activity",
      "plan": {...},
      "chart_type": "metric"
    },
    {
      "name": "Hourly Trend",
      "plan": {...},
      "chart_type": "line"
    }
  ],
  "schedule": {
    "enabled": true,
    "time": "08:00",
    "frequency": "daily",
    "timezone": "America/New_York"
  }
}
```

## Security & Validation

All queries are automatically:
- ✅ Validated against schema contract
- ✅ Security checked (no DDL/DML)
- ✅ Row limited (10,000 max)
- ✅ Timeout protected (5 min max)
- ✅ Logged to Activity Schema

## Error Handling

If a query fails validation:
1. Check the error message for specific issues
2. Use `validate_plan` tool to test before execution
3. Use `list_sources` to verify column names
4. Simplify the query if too complex

## Performance Tips

1. **Always specify dimensions** when using measures
2. **Use appropriate time grains** (HOUR, DAY, WEEK)
3. **Limit results** with top_n parameter
4. **Filter early** to reduce data scanned
5. **Use views** instead of raw EVENTS table when possible

## Dashboard Best Practices

When creating dashboards:
1. **Group related queries** into single dashboard
2. **Use appropriate chart types**:
   - `metric` for single values
   - `line` for time series
   - `bar` for comparisons
   - `table` for detailed data
3. **Enable scheduling** for automatic refresh
4. **Provide clear titles** and descriptions

## Remember

**You are using MCP tools as a secure proxy to Snowflake. You cannot and should not try to access Snowflake directly. All data operations MUST go through the MCP tools.**