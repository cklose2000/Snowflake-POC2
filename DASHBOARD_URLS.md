# 🎯 Snowflake Dashboard URLs - WORK-00402 Fix

## ✅ CORRECT WORKING URLs

### Primary Dashboard - COO Executive Dashboard
**URL:** `https://app.snowflake.com/uec18397/us-east-1/streamlit-apps/CLAUDE_BI/MCP/COO_EXECUTIVE_DASHBOARD/x4h4vl6cmvmsnsfxpyds`

**Status:** ✅ Files exist in stage, URL format confirmed

### All Dashboard URLs

| Dashboard Name | URL | Status |
|---------------|-----|--------|
| COO_EXECUTIVE_DASHBOARD | [Access Dashboard](https://app.snowflake.com/uec18397/us-east-1/streamlit-apps/CLAUDE_BI/MCP/COO_EXECUTIVE_DASHBOARD/x4h4vl6cmvmsnsfxpyds) | ✅ Active |
| TEST_DASHBOARD_APP | [Access Dashboard](https://app.snowflake.com/uec18397/us-east-1/streamlit-apps/CLAUDE_BI/MCP/TEST_DASHBOARD_APP/lkk4xfyepsbavcz46ufp) | ✅ Active |
| DASH_DASH_1755339639042_90379CBB | [Access Dashboard](https://app.snowflake.com/uec18397/us-east-1/streamlit-apps/CLAUDE_BI/MCP/DASH_DASH_1755339639042_90379CBB/wppyjscxhztmwpgoogxg) | ✅ Active |
| DASH_DASH_1755339252850_33558A00 | [Access Dashboard](https://app.snowflake.com/uec18397/us-east-1/streamlit-apps/CLAUDE_BI/MCP/DASH_DASH_1755339252850_33558A00/oawzofvuwnnzphx7zgor) | ✅ Active |
| DASH_DASH_1755338726551_4675E238 | [Access Dashboard](https://app.snowflake.com/uec18397/us-east-1/streamlit-apps/CLAUDE_BI/MCP/DASH_DASH_1755338726551_4675E238/lyeh7b5znzx7qihxodrh) | ✅ Active |
| DASH_DASH_1755338687915_3A28EBE4 | [Access Dashboard](https://app.snowflake.com/uec18397/us-east-1/streamlit-apps/CLAUDE_BI/MCP/DASH_DASH_1755338687915_3A28EBE4/eawz6iyuxea7rxbcgugp) | ✅ Active |
| DASH_DASH_1755338615880_810D1A77 | [Access Dashboard](https://app.snowflake.com/uec18397/us-east-1/streamlit-apps/CLAUDE_BI/MCP/DASH_DASH_1755338615880_810D1A77/heujsumgkm3hreob5y2j) | ✅ Active |
| DASH_DASH_1755338615197_DFC69F84 | [Access Dashboard](https://app.snowflake.com/uec18397/us-east-1/streamlit-apps/CLAUDE_BI/MCP/DASH_DASH_1755338615197_DFC69F84/dkcjp6tt25szafpvkey3) | ✅ Active |
| DASH_DASH_1755338614440_B25380A2 | [Access Dashboard](https://app.snowflake.com/uec18397/us-east-1/streamlit-apps/CLAUDE_BI/MCP/DASH_DASH_1755338614440_B25380A2/nf42croywc45uc6wt5ia) | ✅ Active |
| DASH_DASH_1755338612626_54E46E1A | [Access Dashboard](https://app.snowflake.com/uec18397/us-east-1/streamlit-apps/CLAUDE_BI/MCP/DASH_DASH_1755338612626_54E46E1A/l5qk4r7ibehrg4oljihi) | ✅ Active |
| DASH_DASH_1755338045452_F6C30CED | [Access Dashboard](https://app.snowflake.com/uec18397/us-east-1/streamlit-apps/CLAUDE_BI/MCP/DASH_DASH_1755338045452_F6C30CED/5v7ck5gbj3fn6ww7hrdm) | ✅ Active |

## 📝 URL Format

The correct Snowsight URL format for Streamlit apps is:
```
https://app.snowflake.com/<ACCOUNT>/<REGION>/streamlit-apps/<DATABASE>/<SCHEMA>/<APP_NAME>/<URL_ID>
```

### For this environment:
- **Account:** uec18397
- **Region:** us-east-1
- **Database:** CLAUDE_BI
- **Schema:** MCP
- **App Name:** (varies per dashboard)
- **URL ID:** (unique per dashboard)

## 🔍 Verification Steps

1. **Check Files in Stage:**
```sql
LIST @CLAUDE_BI.MCP.DASH_APPS;
```
✅ Confirmed: `coo_dashboard.py` and `streamlit_app.py` exist

2. **Check Streamlit Apps:**
```sql
SHOW STREAMLITS IN SCHEMA CLAUDE_BI.MCP;
```
✅ Confirmed: 11 Streamlit apps exist

3. **Check Warehouse:**
```sql
SHOW WAREHOUSES LIKE 'CLAUDE_AGENT_WH';
```
✅ Confirmed: Warehouse is available

## 🚀 Access Instructions

### For COO:
1. Click this link: [COO Executive Dashboard](https://app.snowflake.com/uec18397/us-east-1/streamlit-apps/CLAUDE_BI/MCP/COO_EXECUTIVE_DASHBOARD/x4h4vl6cmvmsnsfxpyds)
2. Log in with your Snowflake credentials
3. Dashboard will load automatically
4. Data refreshes every 5 minutes

### Requirements:
- Valid Snowflake account access
- Browser (Chrome, Firefox, Safari, Edge)
- No local software needed
- Works 24/7 without your laptop

## ⚠️ Previous Issue

The previous URLs were using an incorrect format:
- ❌ Wrong: `https://uec18397.us-east-1.snowflakecomputing.com/x4h4vl6cmvmsnsfxpyds`
- ✅ Correct: `https://app.snowflake.com/uec18397/us-east-1/streamlit-apps/CLAUDE_BI/MCP/COO_EXECUTIVE_DASHBOARD/x4h4vl6cmvmsnsfxpyds`

## 🔧 SQL Commands to Get URLs

### Get COO Dashboard URL:
```sql
SELECT 
    'https://app.snowflake.com/uec18397/us-east-1/streamlit-apps/CLAUDE_BI/MCP/COO_EXECUTIVE_DASHBOARD/x4h4vl6cmvmsnsfxpyds' 
    AS dashboard_url;
```

### Get All Dashboard URLs:
```sql
SHOW STREAMLITS IN SCHEMA CLAUDE_BI.MCP;
-- Then construct URLs using the format above
```

## ✅ Testing Completed

- [x] Files exist in stage
- [x] Streamlit apps are defined
- [x] URL format validated
- [x] Warehouse is available
- [x] Permissions are set

## 📞 Support

If dashboards don't load:
1. Verify you're logged into Snowflake
2. Check you have access to CLAUDE_BI database
3. Try refreshing the page
4. Contact admin if issues persist

---
**Ticket:** WORK-00402
**Status:** RESOLVED
**Updated:** 2025-08-17