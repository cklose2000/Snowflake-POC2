# HappyFox Analytics - Ultra-Minimal Snowflake Deployment

## 🎯 Goal: 100% Snowflake Native, Zero External Dependencies

This deployment creates a complete analytics suite using ONLY Snowflake native features:
- Views for analytics (no new tables)
- Table functions for programmatic access
- Streamlit in Snowflake for UI
- Native export capabilities

## 📦 What You Get

1. **SQL Table Functions** - Query tickets programmatically
2. **Streamlit App** - Interactive dashboard in Snowflake
3. **Export Functions** - One-click CSV exports
4. **Native Alerts** - Automated monitoring (optional)

## 🚀 Quick Deployment (5 minutes)

### Step 1: Deploy SQL Components

```bash
# Deploy table functions and permissions
SF_PK_PATH=./claude_code_rsa_key.p8 ~/bin/sf exec-file scripts/happyfox-ingest/08_sis_app.sql
```

This creates:
- `MCP.GET_HAPPYFOX_TICKETS()` - Flexible ticket queries
- `MCP.GET_HAPPYFOX_PRODUCT_STATS()` - Product summaries
- Permissions for PUBLIC role

### Step 2: Create Streamlit App in Snowsight

1. **Open Snowsight** → Navigate to Streamlit
2. **Click** "+ Streamlit App"
3. **Configure**:
   - App Name: `HAPPYFOX_ANALYTICS`
   - Warehouse: `CLAUDE_WAREHOUSE`
   - Database: `CLAUDE_BI`
   - Schema: `MCP`
4. **Copy** the entire content of `09_sis_app_code.py`
5. **Paste** into the Streamlit editor
6. **Run** the app

### Step 3: Access Your Analytics

#### Option A: Streamlit App (Recommended)
```
https://app.snowflake.com/<your-account>/CLAUDE_BI/MCP/streamlits/HAPPYFOX_ANALYTICS
```

#### Option B: SQL Queries
```sql
-- Get all GZ open tickets from last 30 days
SELECT * FROM TABLE(MCP.GET_HAPPYFOX_TICKETS('GZ', 'Open', 0, 30));

-- Get product summary stats
SELECT * FROM TABLE(MCP.GET_HAPPYFOX_PRODUCT_STATS());

-- Export to CSV in Snowsight
SELECT * FROM MCP.VW_HF_TICKETS_EXPORT WHERE product_prefix = 'GZ';
-- Then click "Download Results" in Snowsight
```

## 📊 Using the Streamlit App

### Overview Tab
- Key metrics (total, open, closed tickets)
- Product breakdown with charts
- Age distribution histogram

### Trends Tab
- Daily created vs closed tickets
- Backlog growth over time
- Adjustable date range (7-90 days)

### Details Tab
- Search tickets by subject, ID, or assignee
- View up to 500 tickets at once
- Sort and filter interactively

### Export Tab
- Export filtered data to CSV
- One-click download
- Includes all key fields

## 🔧 Advanced Usage

### Programmatic Access
```sql
-- Use table functions in your procedures
CREATE OR REPLACE PROCEDURE MY_ANALYSIS()
AS
$$
DECLARE
  open_count NUMBER;
BEGIN
  SELECT COUNT(*) INTO open_count 
  FROM TABLE(MCP.GET_HAPPYFOX_TICKETS(NULL, 'Open', 0, 30));
  
  RETURN open_count;
END;
$$;
```

### Scheduled Exports
```sql
-- Create a task to export weekly
CREATE TASK WEEKLY_EXPORT
  WAREHOUSE = CLAUDE_WAREHOUSE
  SCHEDULE = 'USING CRON 0 9 * * MON America/New_York'
AS
  COPY INTO @~/exports/happyfox_weekly.csv
  FROM (SELECT * FROM MCP.VW_HF_TICKETS_EXPORT WHERE lifecycle_state = 'Open')
  FILE_FORMAT = (TYPE = CSV HEADER = TRUE)
  SINGLE = TRUE
  OVERWRITE = TRUE;
```

### Native Monitoring
```sql
-- Enable the high backlog alert
ALTER ALERT MCP.HAPPYFOX_HIGH_BACKLOG_ALERT RESUME;
```

## 🎯 The Narrow Stack Achievement

What we're using:
- ✅ Snowflake Tables: 2 only (RAW_EVENTS, ACTIVITY_STREAM)
- ✅ Snowflake Views: For all analytics
- ✅ Snowflake Functions: For programmatic access
- ✅ Streamlit in Snowflake: Native UI
- ✅ Snowflake RBAC: Native security

What we're NOT using:
- ❌ External Python environments
- ❌ Docker/Kubernetes
- ❌ External APIs
- ❌ Authentication layers
- ❌ CI/CD pipelines
- ❌ Package managers
- ❌ External monitoring

## 🔍 Verification

```sql
-- Verify views exist
SELECT TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS 
WHERE TABLE_SCHEMA = 'MCP' AND TABLE_NAME LIKE 'VW_HF%';

-- Test table functions
SELECT COUNT(*) FROM TABLE(MCP.GET_HAPPYFOX_TICKETS('GZ', 'Open', 0, 30));

-- Check data freshness
SELECT MAX(last_event_time) FROM MCP.VW_HF_TICKETS_LATEST;
```

## 📝 Notes

- All data flows through ACTIVITY_STREAM (two-table compliant)
- Views automatically use SEARCH OPTIMIZATION for performance
- Streamlit app runs entirely in Snowflake (no external hosting)
- Export via native Snowflake features (no external tools)
- Zero external dependencies = maximum simplicity

## 🆘 Troubleshooting

**App won't load?**
- Check warehouse is running
- Verify you have SELECT permission on MCP schema

**No data showing?**
- Verify views have data: `SELECT COUNT(*) FROM MCP.VW_HF_TICKETS_LATEST;`
- Check filters aren't too restrictive

**Export failing?**
- Ensure warehouse size is adequate for data volume
- Check timeout settings if exporting large datasets

## 🎉 Success!

You now have a complete HappyFox analytics suite running 100% inside Snowflake with zero external dependencies. The narrowest possible stack achieved!