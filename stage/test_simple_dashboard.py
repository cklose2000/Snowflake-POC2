"""
Simple Test Dashboard - Verifying Streamlit Works
"""

import streamlit as st
from snowflake.snowpark.context import get_active_session
from datetime import datetime

# Get session
session = get_active_session()

# Configure page
st.set_page_config(
    page_title="Test Dashboard", 
    layout="wide"
)

st.title("🎯 Snowflake Streamlit Test Dashboard")
st.markdown("---")

# Show connection info
col1, col2, col3 = st.columns(3)

with col1:
    st.metric("Status", "✅ Connected")
    
with col2:
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    st.metric("Current Time", current_time)
    
with col3:
    st.metric("Database", "CLAUDE_BI")

st.markdown("---")

# Test query
st.subheader("📊 Test Query - Recent Events")

try:
    # Simple query to test connection
    query = """
    SELECT 
        COUNT(*) as event_count,
        MAX(occurred_at) as latest_event,
        MIN(occurred_at) as earliest_event
    FROM CLAUDE_BI.ACTIVITY.EVENTS
    WHERE occurred_at >= DATEADD('day', -7, CURRENT_TIMESTAMP())
    """
    
    result = session.sql(query).collect()
    
    if result:
        row = result[0]
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Total Events (7 days)", f"{row['EVENT_COUNT']:,}")
        with col2:
            st.metric("Latest Event", str(row['LATEST_EVENT'])[:19])
        with col3:
            st.metric("Earliest Event", str(row['EARLIEST_EVENT'])[:19])
    
    st.success("✅ Database connection successful!")
    
except Exception as e:
    st.error(f"❌ Error: {str(e)}")

st.markdown("---")

# Show access instructions
st.subheader("📝 Access Instructions")

st.markdown("""
### If you can see this page, Streamlit is working! 

**To access dashboards:**
1. Login to Snowsight: https://app.snowflake.com
2. Navigate to Projects → Streamlit
3. Click on the dashboard name
4. Or use the direct URL provided

**Dashboard URLs follow this pattern:**
```
https://app.snowflake.com/<account>/<region>/streamlit-apps/<database>/<schema>/<app_name>/<url_id>
```

**For this environment:**
- Account: uec18397
- Region: us-east-1
- Database: CLAUDE_BI
- Schema: MCP
""")

# Footer
st.markdown("---")
st.caption(f"Test Dashboard | Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")