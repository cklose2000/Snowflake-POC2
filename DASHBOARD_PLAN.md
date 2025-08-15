# Dashboard Generator Revival Plan

## Current State
We have a sophisticated dashboard generation system that:
- Takes natural language input
- Generates dashboard specifications
- Creates Snowflake views
- Produces Streamlit apps

## Integration with New Logging System

### Phase 1: Connect Dashboard to Live Data
- [ ] Update dashboard.js to use Claude Code's logged events
- [ ] Connect to ACTIVITY.EVENTS via simple-client
- [ ] Remove old token-based authentication from frontend

### Phase 2: Natural Language Dashboard Generation
- [ ] Create "Generate Dashboard" command that:
  1. Takes user's dashboard request
  2. Analyzes intent using DashboardFactory
  3. Generates appropriate panels/views
  4. Creates live dashboard

### Phase 3: Dashboard Types to Support

#### 1. Activity Monitoring Dashboard
```javascript
"Show me user activity over the last 24 hours"
→ Time series chart
→ Top users ranking
→ Event type breakdown
```

#### 2. Performance Dashboard
```javascript
"Track system performance metrics"
→ Query execution times
→ Error rates
→ Resource usage
```

#### 3. Business Metrics Dashboard
```javascript
"Show order trends and revenue"
→ Order volume over time
→ Revenue by product
→ Customer segments
```

## Implementation Steps

### Step 1: Test Dashboard Factory
```javascript
// Test with logged events
const factory = new DashboardFactory(snowflakeConn, logger);
const spec = await factory.generateSpec({
  message: "Show me all Claude Code activity today"
});
const dashboard = await factory.create(spec);
```

### Step 2: Create Dashboard CLI
```javascript
// New command: generate-dashboard.js
const client = new SnowflakeSimpleClient(config);
const factory = new DashboardFactory(client);

// Parse user intent
const intent = process.argv[2];
const dashboard = await factory.generateFromIntent(intent);
```

### Step 3: Update Frontend
- Modify dashboard.html to pull from ACTIVITY.EVENTS
- Use Claude Code client for data fetching
- Remove WebSocket, use polling or server-sent events

### Step 4: Streamlit Integration
```python
# Generated Streamlit app connects via:
- Snowflake connector
- Claude Code credentials
- Direct queries to ACTIVITY.EVENTS
```

## Key Components to Update

1. **dashboard.js**: 
   - Remove token auth
   - Add Claude Code client integration
   - Query ACTIVITY.EVENTS directly

2. **dashboard-factory.js**:
   - Update to generate queries against ACTIVITY.EVENTS
   - Add more panel types
   - Improve intent analysis

3. **New: generate-dashboard.js**:
   - CLI tool for dashboard generation
   - Natural language input
   - Outputs HTML or Streamlit

## Example Dashboard Generation Flow

```bash
# User request
node generate-dashboard.js "Show me all errors in the last hour"

# System:
1. Analyzes intent → "error monitoring, 1 hour window"
2. Generates spec with panels:
   - Error count metric
   - Error timeline chart
   - Error details table
3. Creates Snowflake views:
   - V_RECENT_ERRORS
   - V_ERROR_TIMELINE
4. Generates dashboard HTML
5. Opens in browser
```

## Benefits of Integration

1. **All operations logged** - Dashboard generation itself becomes an event
2. **No authentication complexity** - Uses Claude Code's RSA key
3. **Real-time data** - Pulls from live ACTIVITY.EVENTS
4. **Self-documenting** - Dashboard specs stored as events
5. **Version control** - Each dashboard has a hash/version

## Next Actions

1. Test existing DashboardFactory with new data structure
2. Create simple CLI for dashboard generation
3. Update frontend to use new auth system
4. Generate first dashboard from natural language
5. Document the complete flow