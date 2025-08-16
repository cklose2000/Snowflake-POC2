# Dashboard Access Guide 📊

## How to Access Your Dashboards

Since Snowflake Streamlit apps don't have direct public URLs, you need to access them through the Snowflake Console.

### Method 1: Through Snowsight UI (Recommended)

1. **Log into Snowflake Console**
   - Go to your Snowflake account (Snowsight)
   - URL format: `https://app.snowflake.com/[region]/[account]`

2. **Navigate to Streamlit Apps**
   - Click on **Projects** in the left sidebar
   - Select **Streamlit**
   - You'll see all created dashboard apps

3. **Open Your Dashboard**
   - Find your app (e.g., `DASH_DASH_1755339252850_33558A00`)
   - Click to open it
   - Add the dashboard parameter in the URL: `?dashboard_id=dash_1755339252850_33558a00`

### Method 2: Using URL IDs

Each Streamlit app has a unique `url_id`. After creating a dashboard, you get:
```json
{
  "url_id": "oawzofvuwnnzphx7zgor",
  "app_name": "DASH_DASH_1755339252850_33558A00",
  "dashboard_id": "dash_1755339252850_33558a00"
}
```

The URL structure varies by Snowflake deployment but typically follows patterns like:
- `https://[account].snowflakecomputing.com/[region]/[account]/#/streamlit-apps/[url_id]`
- Access through Snowsight is more reliable

### Your Created Dashboards

| Dashboard | App Name | URL ID | Dashboard ID |
|-----------|----------|--------|--------------|
| Debug Test | DASH_DASH_1755338726551_4675E238 | lyeh7b5znzx7qihxodrh | dash_1755338726551_4675e238 |
| Test Dashboard App | TEST_DASHBOARD_APP | lkk4xfyepsbavcz46ufp | (use any dashboard_id) |
| Executive Metrics | DASH_DASH_1755338612626_54E46E1A | l5qk4r7ibehrg4oljihi | dash_1755338612626_54e46e1a |
| Activity Trends | DASH_DASH_1755338614440_B25380A2 | nf42croywc45uc6wt5ia | dash_1755338614440_b25380a2 |
| Top Actions | DASH_DASH_1755338615197_DFC69F84 | dkcjp6tt25szafpvkey3 | dash_1755338615197_dfc69f84 |
| Executive Overview | DASH_DASH_1755338615880_810D1A77 | heujsumgkm3hreob5y2j | dash_1755338615880_810d1a77 |

### Testing a Dashboard

1. **Create a new dashboard:**
```bash
curl -X POST http://localhost:3001/api/create-streamlit \
  -H "Content-Type: application/json" \
  -d '{
    "title": "My Test Dashboard",
    "spec": {
      "panels": [
        {"type": "metrics", "title": "KPIs"},
        {"type": "series", "title": "Trends"}
      ]
    }
  }'
```

2. **Note the response:**
   - `url_id`: Use this to find the app in Snowflake
   - `dashboard_id`: Add as URL parameter
   - `instructions`: Follow these steps

3. **Access in Snowflake:**
   - Log into Snowsight
   - Go to Projects → Streamlit
   - Find your app and open it
   - The dashboard will auto-load based on the dashboard_id parameter

### Dashboard Features

- **Auto-refresh**: Every 5 minutes
- **Multi-panel support**: Metrics, time series, rankings, events
- **Mobile-friendly**: Responsive layout
- **Real-time data**: Pulls from ACTIVITY.EVENTS

### Troubleshooting

**Dashboard not loading?**
- Ensure the dashboard_id parameter is in the URL
- Check that the dashboard exists in VW_DASHBOARDS
- Verify the Streamlit app has the streamlit_app.py file in its stage

**No data showing?**
- Check that events exist in ACTIVITY.EVENTS
- Verify the time range parameters
- Ensure DASH_GET_* procedures are working

**404 Error?**
- Streamlit apps must be accessed through Snowflake Console
- Direct URLs don't work outside of Snowflake
- Use Snowsight UI to access apps

### API Response Format

When you create a dashboard, you now get clear instructions:

```json
{
  "dashboardId": "dash_xxx",
  "appName": "DASH_DASH_XXX",
  "urlInfo": {
    "url_id": "abc123",
    "instructions": "To view this dashboard:\n1. Log into Snowflake Console...",
    "app_name": "DASH_DASH_XXX",
    "dashboard_id": "dash_xxx"
  },
  "status": "created",
  "message": "Dashboard and Streamlit app created successfully"
}
```

### Summary

The Dashboard Factory creates real Snowflake-native Streamlit apps that:
1. Are fully integrated with Snowflake security
2. Auto-refresh every 5 minutes
3. Read dashboard configurations from events
4. Display real-time data from your Activity Schema

Access them through the Snowflake Console for the best experience!