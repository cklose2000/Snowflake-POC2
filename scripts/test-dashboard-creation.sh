#!/bin/bash
# Test Dashboard Creation End-to-End

echo "🚀 Testing Dashboard Factory End-to-End"
echo "======================================="

# Base URL
BASE_URL="http://localhost:3001"

# Test 1: Create a metrics dashboard
echo ""
echo "1️⃣ Creating Metrics Dashboard..."
RESPONSE=$(curl -s -X POST $BASE_URL/api/create-streamlit \
  -H "Content-Type: application/json" \
  -d '{
    "title": "Executive Metrics Dashboard",
    "spec": {
      "panels": [
        {
          "type": "metrics",
          "title": "Key Performance Indicators",
          "params": {
            "start_ts": "DATEADD('"'day'"', -7, CURRENT_TIMESTAMP())",
            "end_ts": "CURRENT_TIMESTAMP()"
          }
        }
      ]
    }
  }')

echo "$RESPONSE" | jq
DASHBOARD_ID=$(echo "$RESPONSE" | jq -r '.data.dashboardId')
echo "✅ Created dashboard: $DASHBOARD_ID"

# Test 2: Create a time series dashboard
echo ""
echo "2️⃣ Creating Time Series Dashboard..."
RESPONSE=$(curl -s -X POST $BASE_URL/api/create-streamlit \
  -H "Content-Type: application/json" \
  -d '{
    "title": "Activity Trends",
    "spec": {
      "panels": [
        {
          "type": "series",
          "title": "Hourly Activity",
          "params": {
            "start_ts": "DATEADD('"'hour'"', -24, CURRENT_TIMESTAMP())",
            "end_ts": "CURRENT_TIMESTAMP()",
            "interval_str": "hour"
          }
        }
      ]
    }
  }')

echo "$RESPONSE" | jq
DASHBOARD_ID2=$(echo "$RESPONSE" | jq -r '.data.dashboardId')
echo "✅ Created dashboard: $DASHBOARD_ID2"

# Test 3: Create a top-N dashboard
echo ""
echo "3️⃣ Creating Top Actions Dashboard..."
RESPONSE=$(curl -s -X POST $BASE_URL/api/create-streamlit \
  -H "Content-Type: application/json" \
  -d '{
    "title": "Top Actions This Week",
    "spec": {
      "panels": [
        {
          "type": "topn",
          "title": "Most Frequent Actions",
          "params": {
            "start_ts": "DATEADD('"'day'"', -7, CURRENT_TIMESTAMP())",
            "end_ts": "CURRENT_TIMESTAMP()",
            "dimension": "action",
            "n": 10
          }
        }
      ]
    }
  }')

echo "$RESPONSE" | jq
DASHBOARD_ID3=$(echo "$RESPONSE" | jq -r '.data.dashboardId')
echo "✅ Created dashboard: $DASHBOARD_ID3"

# Test 4: Create a multi-panel dashboard
echo ""
echo "4️⃣ Creating Multi-Panel Executive Dashboard..."
RESPONSE=$(curl -s -X POST $BASE_URL/api/create-streamlit \
  -H "Content-Type: application/json" \
  -d '{
    "title": "Executive Overview",
    "spec": {
      "panels": [
        {
          "type": "metrics",
          "title": "KPIs",
          "params": {
            "start_ts": "DATEADD('"'day'"', -1, CURRENT_TIMESTAMP())",
            "end_ts": "CURRENT_TIMESTAMP()"
          }
        },
        {
          "type": "series",
          "title": "24-Hour Trend",
          "params": {
            "start_ts": "DATEADD('"'hour'"', -24, CURRENT_TIMESTAMP())",
            "end_ts": "CURRENT_TIMESTAMP()",
            "interval_str": "hour"
          }
        },
        {
          "type": "topn",
          "title": "Top Activities",
          "params": {
            "start_ts": "DATEADD('"'day'"', -1, CURRENT_TIMESTAMP())",
            "end_ts": "CURRENT_TIMESTAMP()",
            "dimension": "action",
            "n": 5
          }
        },
        {
          "type": "events",
          "title": "Recent Events",
          "params": {
            "cursor_ts": "DATEADD('"'minute'"', -30, CURRENT_TIMESTAMP())",
            "limit_rows": 20
          }
        }
      ]
    }
  }')

echo "$RESPONSE" | jq
DASHBOARD_ID4=$(echo "$RESPONSE" | jq -r '.data.dashboardId')
echo "✅ Created dashboard: $DASHBOARD_ID4"

# Test 5: Test natural language query
echo ""
echo "5️⃣ Testing Natural Language Query..."
RESPONSE=$(curl -s -X POST $BASE_URL/api/nl-query \
  -H "Content-Type: application/json" \
  -d '{
    "prompt": "show me the top 5 actions in the last hour"
  }')

echo "$RESPONSE" | jq

# Summary
echo ""
echo "📊 Dashboard Summary"
echo "===================="
echo "Created 4 dashboards:"
echo "1. Metrics: $DASHBOARD_ID"
echo "2. Time Series: $DASHBOARD_ID2"
echo "3. Top-N: $DASHBOARD_ID3"
echo "4. Multi-Panel: $DASHBOARD_ID4"
echo ""
echo "🔗 View dashboards at:"
echo "https://uec18397.us-east-1.snowflakecomputing.com/lkk4xfyepsbavcz46ufp?dashboard_id=$DASHBOARD_ID4"
echo ""
echo "✅ Dashboard Factory test complete!"