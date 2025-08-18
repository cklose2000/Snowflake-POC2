"""
COO Executive Dashboard for Snowflake Activity Streams (Simple Version)
Provides real-time insights into business operations without plotly dependency
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
    # Query for key metrics
    metrics_query = f"""
    WITH date_range AS (
        SELECT 
            '{start_date.strftime('%Y-%m-%d')}'::TIMESTAMP as start_date,
            CURRENT_TIMESTAMP() as end_date
    ),
    recent_events AS (
        SELECT *
        FROM CLAUDE_BI.ACTIVITY.EVENTS
        WHERE occurred_at >= (SELECT start_date FROM date_range)
    ),
    metrics AS (
        SELECT
            COUNT(*) as total_events,
            COUNT(DISTINCT actor_id) as unique_actors,
            COUNT(DISTINCT action) as unique_actions,
            COUNT(DISTINCT DATE(occurred_at)) as active_days
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

# Activity Timeline (Simple table instead of chart)
st.markdown("## 📅 Activity Timeline")

try:
    timeline_query = f"""
    WITH hourly_events AS (
        SELECT 
            DATE_TRUNC('hour', occurred_at) as hour,
            COUNT(*) as event_count,
            COUNT(DISTINCT actor_id) as unique_actors
        FROM CLAUDE_BI.ACTIVITY.EVENTS
        WHERE occurred_at >= '{start_date.strftime('%Y-%m-%d')}'
        GROUP BY 1
        ORDER BY 1 DESC
        LIMIT 24
    )
    SELECT 
        TO_CHAR(hour, 'YYYY-MM-DD HH24:MI') as time_period,
        event_count,
        unique_actors
    FROM hourly_events
    """

    timeline_df = session.sql(timeline_query).to_pandas()

    if not timeline_df.empty:
        st.dataframe(
            timeline_df,
            hide_index=True,
            column_config={
                "time_period": "Time Period",
                "event_count": st.column_config.NumberColumn("Events", format="%d"),
                "unique_actors": st.column_config.NumberColumn("Unique Actors", format="%d")
            }
        )
        
        # Simple bar chart using Streamlit native charts
        st.markdown("### Event Count Trend")
        chart_data = timeline_df.set_index('time_period')['event_count']
        st.bar_chart(chart_data)
    else:
        st.info("No timeline data available for the selected period")

except Exception as e:
    st.error(f"Error loading timeline: {str(e)}")

# Two column layout for additional data
col1, col2 = st.columns(2)

with col1:
    st.markdown("### 🎯 Top Actions")
    
    try:
        actions_query = f"""
        SELECT 
            action,
            COUNT(*) as count
        FROM CLAUDE_BI.ACTIVITY.EVENTS
        WHERE occurred_at >= '{start_date.strftime('%Y-%m-%d')}'
        GROUP BY action
        ORDER BY count DESC
        LIMIT 10
        """
        
        actions_df = session.sql(actions_query).to_pandas()
        
        if not actions_df.empty:
            st.dataframe(
                actions_df,
                hide_index=True,
                column_config={
                    "action": "Action",
                    "count": st.column_config.NumberColumn("Count", format="%d")
                }
            )
            
            # Simple bar chart
            chart_data = actions_df.set_index('ACTION')['COUNT']
            st.bar_chart(chart_data)
        else:
            st.info("No action data available")
            
    except Exception as e:
        st.error(f"Error loading actions: {str(e)}")

with col2:
    st.markdown("### 👥 Top Actors")
    
    try:
        actors_query = f"""
        SELECT 
            actor_id,
            COUNT(*) as event_count
        FROM CLAUDE_BI.ACTIVITY.EVENTS
        WHERE occurred_at >= '{start_date.strftime('%Y-%m-%d')}'
        GROUP BY actor_id
        ORDER BY event_count DESC
        LIMIT 10
        """
        
        actors_df = session.sql(actors_query).to_pandas()
        
        if not actors_df.empty:
            st.dataframe(
                actors_df,
                hide_index=True,
                column_config={
                    "actor_id": "Actor",
                    "event_count": st.column_config.NumberColumn("Events", format="%d")
                }
            )
            
            # Simple bar chart
            chart_data = actors_df.set_index('ACTOR_ID')['EVENT_COUNT']
            st.bar_chart(chart_data)
        else:
            st.info("No actor data available")
            
    except Exception as e:
        st.error(f"Error loading actors: {str(e)}")

st.markdown("---")

# Event Source Distribution
st.markdown("## 📊 Event Sources")

try:
    sources_query = f"""
    SELECT 
        _source_lane as source,
        COUNT(*) as count,
        ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as percentage
    FROM CLAUDE_BI.ACTIVITY.EVENTS
    WHERE occurred_at >= '{start_date.strftime('%Y-%m-%d')}'
    GROUP BY _source_lane
    ORDER BY count DESC
    """

    sources_df = session.sql(sources_query).to_pandas()

    if not sources_df.empty:
        st.dataframe(
            sources_df,
            hide_index=True,
            column_config={
                "SOURCE": "Source",
                "COUNT": st.column_config.NumberColumn("Events", format="%d"),
                "PERCENTAGE": st.column_config.NumberColumn("Percent", format="%.1f%%")
            }
        )
        
        # Simple pie chart alternative - just show the data
        st.markdown("### Source Distribution")
        chart_data = sources_df.set_index('SOURCE')['COUNT']
        st.bar_chart(chart_data)
    else:
        st.info("No source data available")
        
except Exception as e:
    st.error(f"Error loading sources: {str(e)}")

st.markdown("---")

# Recent Events Table
st.markdown("## 📋 Recent Events")

try:
    recent_events_query = f"""
    SELECT 
        occurred_at,
        action,
        actor_id,
        object:type::STRING as object_type,
        object:id::STRING as object_id,
        _source_lane as source
    FROM CLAUDE_BI.ACTIVITY.EVENTS
    WHERE occurred_at >= '{start_date.strftime('%Y-%m-%d')}'
    ORDER BY occurred_at DESC
    LIMIT 50
    """

    recent_df = session.sql(recent_events_query).to_pandas()

    if not recent_df.empty:
        st.dataframe(
            recent_df,
            hide_index=True,
            column_config={
                "OCCURRED_AT": st.column_config.DatetimeColumn("Time", format="DD/MM/YY HH:mm"),
                "ACTION": "Action",
                "ACTOR_ID": "Actor", 
                "OBJECT_TYPE": "Object Type",
                "OBJECT_ID": "Object ID",
                "SOURCE": "Source"
            }
        )
    else:
        st.info("No recent events found")
        
except Exception as e:
    st.error(f"Error loading recent events: {str(e)}")

# Footer
st.markdown("---")
st.caption(f"Dashboard refreshed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Data range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

# Debug info
with st.expander("🔧 Debug Information"):
    st.write("**Dashboard Version:** Simple (No Plotly)")
    st.write("**Session Info:**")
    st.write(f"- Current Time: {datetime.now()}")
    st.write(f"- Date Range: {start_date} to {end_date}")
    st.write(f"- Days Back: {days_back}")
    
    try:
        # Test basic connectivity
        test_query = "SELECT CURRENT_TIMESTAMP() as current_time, CURRENT_USER() as current_user"
        test_result = session.sql(test_query).to_pandas()
        st.write("**Connection Test:**")
        st.dataframe(test_result, hide_index=True)
    except Exception as e:
        st.error(f"Connection test failed: {str(e)}")