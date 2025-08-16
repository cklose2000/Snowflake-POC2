# Dashboard Factory Implementation

## 🎯 Overview

Complete end-to-end dashboard system with:
- ✅ One-click presets via dashboard-server.js
- ✅ Natural language queries via Claude Code
- ✅ Snowflake-native Streamlit apps
- ✅ 5-minute auto-refresh for mobile
- ✅ Two-table architecture maintained (NO new tables!)

## 🚀 Quick Start

### 1. Deploy SQL Changes

```bash
# Apply the refactored dashboard procedures (uses EVENTS, not DASHBOARDS table)
sf exec-file scripts/dashboard-procs.sql

# Create the stage and helper procedures
sf exec-file scripts/deploy-streamlit.sql
```

### 2. Install Dependencies

```bash
npm install
```

### 3. Start Dashboard Server

```bash
# Terminal 1: Main server (port 3000)
npm start

# Terminal 2: Dashboard API server (port 3001)
npm run dashboard-server
```

### 4. Upload Streamlit Template

```bash
# Using SnowSQL (with RSA auth)
snowsql -a uec18397.us-east-1 -u CLAUDE_CODE_AI_AGENT --private-key-path /path/to/key.p8

USE DATABASE CLAUDE_BI;
USE SCHEMA MCP;
PUT file://stage/streamlit_app.py @DASH_APPS AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
```

### 5. Open Dashboard UI

```bash
open http://localhost:3000/dashboard.html
```

## 📊 Using the Dashboard

### One-Click Presets

Click any preset button to instantly execute:
- **📅 Today** - Last 24 hours by hour
- **⏰ Last 6h** - 15-minute buckets
- **⚡ Last Hour** - 5-minute buckets  
- **📊 This Week** - Daily buckets
- **🎯 Top Actions** - Most frequent actions
- **👥 Top Users** - Most active users
- **📈 Today's Summary** - Key metrics

### Natural Language Queries

Type queries like:
- "Show events by 15 minutes over last 6 hours"
- "Top 10 users today"
- "Error rate this week"

### Generate Streamlit Dashboard

1. Click **🚀 Generate** button
2. System creates a Snowflake-native Streamlit app
3. Opens dashboard URL in new tab
4. Dashboard auto-refreshes every 5 minutes

## 🏗️ Architecture

### Two-Table Compliance

```sql
-- ONLY these tables exist:
1. CLAUDE_BI.LANDING.RAW_EVENTS     -- Ingestion
2. CLAUDE_BI.ACTIVITY.EVENTS        -- Dynamic table

-- Dashboard specs stored as events:
INSERT INTO LANDING.RAW_EVENTS VALUES (
  OBJECT_CONSTRUCT(
    'action', 'dashboard.created',
    'object', OBJECT_CONSTRUCT('type', 'dashboard', 'id', 'dash_123'),
    'attributes', OBJECT_CONSTRUCT('spec', {...}, 'title', '...')
  ), 'DASHBOARD_SYSTEM', CURRENT_TIMESTAMP()
);

-- Read via view:
CREATE VIEW MCP.VW_DASHBOARDS AS
SELECT * FROM ACTIVITY.EVENTS 
WHERE action = 'dashboard.created';
```

### API Endpoints

```javascript
// Dashboard Server (port 3001)
GET  /api/test              // Test connection
POST /api/execute-proc      // Run dashboard procedures
POST /api/execute-preset    // Execute preset configurations  
POST /api/create-streamlit  // Generate Streamlit dashboard
```

### Data Flow

```
UI (dashboard.html)
    ↓
Dashboard Server (RSA auth)
    ↓
DASH_GET_* Procedures
    ↓
ACTIVITY.EVENTS table
    ↓
Streamlit App (auto-refresh)
```

## 🔐 Security

- All access through RSA-authenticated `CLAUDE_CODE_AI_AGENT`
- Single warehouse: `CLAUDE_AGENT_WH`
- Query tagging: `dash-api|proc:DASH_GET_SERIES`
- Complete audit trail in EVENTS

## 📱 Mobile Features

- Responsive design
- 5-minute auto-refresh
- Touch-friendly buttons
- Collapsible panels
- PWA-ready

## 🛠️ Troubleshooting

### Dashboard Server Won't Start

```bash
# Check RSA key path
export SF_PK_PATH=/path/to/claude_code_rsa_key.p8

# Verify connection
node -e "const {SnowflakeSimpleClient} = require('./snowflake-mcp-client/dist/simple-client.js'); 
const sf = new SnowflakeSimpleClient({privateKeyPath: process.env.SF_PK_PATH}); 
sf.connect().then(() => console.log('Connected!')).catch(console.error);"
```

### Streamlit App Not Found

```sql
-- Check if stage exists
SHOW STAGES LIKE 'DASH_APPS' IN SCHEMA MCP;

-- List files in stage
LIST @MCP.DASH_APPS;

-- Re-upload if missing
PUT file://stage/streamlit_app.py @MCP.DASH_APPS;
```

### Dashboard Not Refreshing

```python
# Check Streamlit logs in Snowflake
SELECT * FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE QUERY_TAG LIKE 'streamlit%'
ORDER BY START_TIME DESC;
```

## 📝 Adding New Presets

Edit `src/presets.js`:

```javascript
{
  id: 'my_preset',
  label: 'My Preset',
  icon: '🎨',
  description: 'Custom preset description',
  proc: 'DASH_GET_SERIES',
  params: {
    start_ts: "DATEADD('day', -7, CURRENT_TIMESTAMP())",
    end_ts: 'CURRENT_TIMESTAMP()',
    interval_str: 'day'
  }
}
```

## 🎯 Success Metrics

- ✅ COO gets one-click answers
- ✅ Natural language queries work
- ✅ Mobile URLs with auto-refresh
- ✅ Two-table architecture maintained
- ✅ Complete audit trail
- ✅ RSA authentication enforced

## 📚 Related Documentation

- [CLAUDE.md](./CLAUDE.md) - Two-table architecture rules
- [README.md](./README.md) - Main project documentation
- [scripts/dashboard-procs.sql](./scripts/dashboard-procs.sql) - Dashboard procedures
- [stage/streamlit_app.py](./stage/streamlit_app.py) - Streamlit template

## 🚦 Next Steps

1. **Production Deployment**
   - Deploy to production Snowflake account
   - Configure production warehouse sizing
   - Set up monitoring alerts

2. **Enhanced Features**
   - Add more visualization types
   - Custom color schemes
   - Export to PDF/Excel
   - Scheduled email reports

3. **Performance Optimization**
   - Result caching strategy
   - Query optimization hints
   - Materialized view consideration

## 💡 Tips

- Use `QUERY_TAG` to track dashboard usage
- Monitor `ACTIVITY.EVENTS` for dashboard analytics
- Create role-based dashboard access
- Use Snowflake Tasks for scheduled refreshes

---

**Ready to go!** The dashboard factory is fully operational with enforced two-table architecture, RSA authentication, and mobile-friendly Streamlit apps.