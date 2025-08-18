"""
COO Executive Dashboard for Snowflake Activity Streams (Fixed Version)
Compatible with Snowflake Streamlit environment and correct table schema
"""

import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session
from datetime import datetime, timedelta

# Get Snowflake session
session = get_active_session()

# Configure page
st.set_page_config(
    page_title="COO Executive Dashboard", 
    page_icon="📊",
    layout="wide"
)

# Title and header
st.title("📊 COO Executive Dashboard")
st.markdown("**Real-time insights into business operations from Activity Streams**")
st.markdown("---")

# Date range selector
col1, col2, col3 = st.columns([2, 2, 8])
with col1:
    days_back = st.selectbox(
        "Time Range",
        options=[1, 7, 14, 30, 90],
        index=1,
        format_func=lambda x: f"Last {x} days"
    )
with col2:
    refresh_button = st.button("🔄 Refresh Data")

# Calculate date range
end_date = datetime.now()
start_date = end_date - timedelta(days=days_back)

# Key Metrics Row
st.markdown("## 📈 Key Metrics")
metrics_cols = st.columns(4)

try:
    # Query for key metrics - using correct column names
    metrics_query = f"""
    WITH date_range AS (
        SELECT 
            '{start_date.strftime('%Y-%m-%d')}'::TIMESTAMP as start_date,
            CURRENT_TIMESTAMP() as end_date
    ),
    recent_events AS (
        SELECT *
        FROM CLAUDE_BI.ACTIVITY.EVENTS
        WHERE OCCURRED_AT >= (SELECT start_date FROM date_range)
    ),
    metrics AS (
        SELECT
            COUNT(*) as total_events,
            COUNT(DISTINCT ACTOR_ID) as unique_actors,
            COUNT(DISTINCT ACTION) as unique_actions,
            COUNT(DISTINCT DATE(OCCURRED_AT)) as active_days
        FROM recent_events
    )
    SELECT * FROM metrics
    """

    metrics_df = session.sql(metrics_query).to_pandas()

    with metrics_cols[0]:
        st.metric(
            "Total Events", 
            f"{int(metrics_df['TOTAL_EVENTS'].iloc[0]):,}",
            delta=f"Last {days_back} days"
        )

    with metrics_cols[1]:
        st.metric(
            "Unique Actors", 
            f"{int(metrics_df['UNIQUE_ACTORS'].iloc[0]):,}"
        )

    with metrics_cols[2]:
        st.metric(
            "Event Types", 
            f"{int(metrics_df['UNIQUE_ACTIONS'].iloc[0]):,}"
        )

    with metrics_cols[3]:
        st.metric(
            "Active Days", 
            f"{int(metrics_df['ACTIVE_DAYS'].iloc[0]):,}"
        )

except Exception as e:
    st.error(f"Error loading metrics: {str(e)}")
    # Fallback metrics
    with metrics_cols[0]:
        st.metric("Total Events", "Loading...")
    with metrics_cols[1]:
        st.metric("Unique Actors", "Loading...")
    with metrics_cols[2]:
        st.metric("Event Types", "Loading...")
    with metrics_cols[3]:
        st.metric("Active Days", "Loading...")

st.markdown("---")

# Activity Timeline
st.markdown("## 📅 Activity Timeline")

try:
    timeline_query = f"""
    WITH hourly_events AS (
        SELECT 
            DATE_TRUNC('hour', OCCURRED_AT) as hour,
            COUNT(*) as event_count,
            COUNT(DISTINCT ACTOR_ID) as unique_actors
        FROM CLAUDE_BI.ACTIVITY.EVENTS
        WHERE OCCURRED_AT >= '{start_date.strftime('%Y-%m-%d')}'
        GROUP BY 1
        ORDER BY 1 DESC
        LIMIT 24
    )
    SELECT 
        TO_CHAR(hour, 'YYYY-MM-DD HH24:MI') as time_period,
        event_count,
        unique_actors
    FROM hourly_events
    ORDER BY hour DESC
    """

    timeline_df = session.sql(timeline_query).to_pandas()

    if not timeline_df.empty:
        # Simple dataframe without column_config
        st.dataframe(timeline_df, use_container_width=True)
        
        # Simple bar chart using Streamlit native charts
        st.markdown("### Event Count Trend")
        chart_data = timeline_df.set_index('TIME_PERIOD')['EVENT_COUNT']
        st.bar_chart(chart_data)
    else:
        st.info("No timeline data available for the selected period")

except Exception as e:
    st.error(f"Error loading timeline: {str(e)}")
    st.write("Debug - Timeline query error details:")
    st.code(str(e))

# Two column layout for additional data
col1, col2 = st.columns(2)

with col1:
    st.markdown("### 🎯 Top Actions")
    
    try:
        actions_query = f"""
        SELECT 
            ACTION,
            COUNT(*) as count
        FROM CLAUDE_BI.ACTIVITY.EVENTS
        WHERE OCCURRED_AT >= '{start_date.strftime('%Y-%m-%d')}'
        GROUP BY ACTION
        ORDER BY count DESC
        LIMIT 10
        """
        
        actions_df = session.sql(actions_query).to_pandas()
        
        if not actions_df.empty:
            # Simple dataframe display
            st.dataframe(actions_df, use_container_width=True)
            
            # Simple bar chart
            chart_data = actions_df.set_index('ACTION')['COUNT']
            st.bar_chart(chart_data)
        else:
            st.info("No action data available")
            
    except Exception as e:
        st.error(f"Error loading actions: {str(e)}")
        st.write("Debug - Actions query error details:")
        st.code(str(e))

with col2:
    st.markdown("### 👥 Top Actors")
    
    try:
        actors_query = f"""
        SELECT 
            ACTOR_ID,
            COUNT(*) as event_count
        FROM CLAUDE_BI.ACTIVITY.EVENTS
        WHERE OCCURRED_AT >= '{start_date.strftime('%Y-%m-%d')}'
        GROUP BY ACTOR_ID
        ORDER BY event_count DESC
        LIMIT 10
        """
        
        actors_df = session.sql(actors_query).to_pandas()
        
        if not actors_df.empty:
            # Simple dataframe display
            st.dataframe(actors_df, use_container_width=True)
            
            # Simple bar chart
            chart_data = actors_df.set_index('ACTOR_ID')['EVENT_COUNT']
            st.bar_chart(chart_data)
        else:
            st.info("No actor data available")
            
    except Exception as e:
        st.error(f"Error loading actors: {str(e)}")
        st.write("Debug - Actors query error details:")
        st.code(str(e))

st.markdown("---")

# Event Source Distribution
st.markdown("## 📊 Event Sources")

try:
    # Updated to use correct column name
    sources_query = f"""
    SELECT 
        SOURCE,
        COUNT(*) as count,
        ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as percentage
    FROM CLAUDE_BI.ACTIVITY.EVENTS
    WHERE OCCURRED_AT >= '{start_date.strftime('%Y-%m-%d')}'
        AND SOURCE IS NOT NULL
    GROUP BY SOURCE
    ORDER BY count DESC
    """

    sources_df = session.sql(sources_query).to_pandas()

    if not sources_df.empty:
        # Simple dataframe display
        st.dataframe(sources_df, use_container_width=True)
        
        # Simple bar chart for source distribution
        st.markdown("### Source Distribution")
        chart_data = sources_df.set_index('SOURCE')['COUNT']
        st.bar_chart(chart_data)
    else:
        st.info("No source data available")
        
except Exception as e:
    st.error(f"Error loading sources: {str(e)}")
    st.write("Debug - Sources query error details:")
    st.code(str(e))

st.markdown("---")

# Recent Events Table
st.markdown("## 📋 Recent Events")

try:
    # Updated to use correct column names
    recent_events_query = f"""
    SELECT 
        OCCURRED_AT,
        ACTION,
        ACTOR_ID,
        OBJECT_TYPE,
        OBJECT_ID,
        SOURCE
    FROM CLAUDE_BI.ACTIVITY.EVENTS
    WHERE OCCURRED_AT >= '{start_date.strftime('%Y-%m-%d')}'
    ORDER BY OCCURRED_AT DESC
    LIMIT 50
    """

    recent_df = session.sql(recent_events_query).to_pandas()

    if not recent_df.empty:
        # Simple dataframe display
        st.dataframe(recent_df, use_container_width=True)
    else:
        st.info("No recent events found")
        
except Exception as e:
    st.error(f"Error loading recent events: {str(e)}")
    st.write("Debug - Recent events query error details:")
    st.code(str(e))

# Footer
st.markdown("---")
st.caption(f"Dashboard refreshed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Data range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

# Debug info
with st.expander("🔧 Debug Information"):
    st.write("**Dashboard Version:** Fixed (Compatible with Snowflake Streamlit)")
    st.write("**Session Info:**")
    st.write(f"- Current Time: {datetime.now()}")
    st.write(f"- Date Range: {start_date} to {end_date}")
    st.write(f"- Days Back: {days_back}")
    
    try:
        # Test basic connectivity
        test_query = "SELECT CURRENT_TIMESTAMP() as current_time, CURRENT_USER() as current_user"
        test_result = session.sql(test_query).to_pandas()
        st.write("**Connection Test:**")
        st.dataframe(test_result)
        
        # Test table structure
        structure_query = "SELECT COUNT(*) as total_records FROM CLAUDE_BI.ACTIVITY.EVENTS"
        structure_result = session.sql(structure_query).to_pandas()
        st.write("**Table Test:**")
        st.dataframe(structure_result)
        
        # Show sample data
        sample_query = "SELECT * FROM CLAUDE_BI.ACTIVITY.EVENTS ORDER BY OCCURRED_AT DESC LIMIT 3"
        sample_result = session.sql(sample_query).to_pandas()
        st.write("**Sample Data:**")
        st.dataframe(sample_result)
        
    except Exception as e:
        st.error(f"Debug test failed: {str(e)}")
        
    # Show table schema
    st.write("**Expected Table Schema:**")
    st.code("""
    CLAUDE_BI.ACTIVITY.EVENTS columns:
    - EVENT_ID (VARCHAR)
    - OCCURRED_AT (TIMESTAMP_TZ)
    - ACTION (VARCHAR) 
    - ACTOR_ID (VARCHAR)
    - OBJECT_TYPE (VARCHAR)
    - OBJECT_ID (VARCHAR)
    - ATTRIBUTES (VARIANT)
    - SOURCE (VARCHAR)
    - INGESTED_AT (TIMESTAMP_TZ)
    """)