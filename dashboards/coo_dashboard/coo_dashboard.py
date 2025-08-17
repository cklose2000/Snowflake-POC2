"""
COO Executive Dashboard for Snowflake Activity Streams
Provides real-time insights into business operations
"""

import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go

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
st.markdown("Real-time insights into business operations from Activity Streams")
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
        f"{metrics_df['TOTAL_EVENTS'].iloc[0]:,}",
        delta=f"Last {days_back} days"
    )

with metrics_cols[1]:
    st.metric(
        "Unique Actors", 
        f"{metrics_df['UNIQUE_ACTORS'].iloc[0]:,}"
    )

with metrics_cols[2]:
    st.metric(
        "Event Types", 
        f"{metrics_df['UNIQUE_ACTIONS'].iloc[0]:,}"
    )

with metrics_cols[3]:
    st.metric(
        "Active Days", 
        f"{metrics_df['ACTIVE_DAYS'].iloc[0]:,}"
    )

st.markdown("---")

# Activity Timeline
st.markdown("## 📅 Activity Timeline")

timeline_query = f"""
WITH hourly_events AS (
    SELECT 
        DATE_TRUNC('hour', occurred_at) as hour,
        COUNT(*) as event_count,
        COUNT(DISTINCT actor_id) as unique_actors
    FROM CLAUDE_BI.ACTIVITY.EVENTS
    WHERE occurred_at >= '{start_date.strftime('%Y-%m-%d')}'
    GROUP BY 1
    ORDER BY 1
)
SELECT * FROM hourly_events
"""

timeline_df = session.sql(timeline_query).to_pandas()

if not timeline_df.empty:
    fig_timeline = go.Figure()
    
    fig_timeline.add_trace(go.Scatter(
        x=timeline_df['HOUR'],
        y=timeline_df['EVENT_COUNT'],
        mode='lines+markers',
        name='Events',
        line=dict(color='#1f77b4', width=2),
        marker=dict(size=4)
    ))
    
    fig_timeline.update_layout(
        title="Event Activity Over Time",
        xaxis_title="Time",
        yaxis_title="Number of Events",
        height=400,
        showlegend=True,
        hovermode='x unified'
    )
    
    st.plotly_chart(fig_timeline, use_container_width=True)
else:
    st.info("No timeline data available for the selected period")

# Two column layout for additional charts
col1, col2 = st.columns(2)

with col1:
    st.markdown("### 🎯 Top Actions")
    
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
        fig_actions = px.bar(
            actions_df, 
            x='COUNT', 
            y='ACTION',
            orientation='h',
            title="Most Frequent Actions",
            labels={'COUNT': 'Event Count', 'ACTION': 'Action Type'}
        )
        fig_actions.update_layout(height=400, showlegend=False)
        st.plotly_chart(fig_actions, use_container_width=True)
    else:
        st.info("No action data available")

with col2:
    st.markdown("### 👥 Top Actors")
    
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
        fig_actors = px.bar(
            actors_df,
            x='EVENT_COUNT',
            y='ACTOR_ID',
            orientation='h',
            title="Most Active Actors",
            labels={'EVENT_COUNT': 'Event Count', 'ACTOR_ID': 'Actor'}
        )
        fig_actors.update_layout(height=400, showlegend=False)
        st.plotly_chart(fig_actors, use_container_width=True)
    else:
        st.info("No actor data available")

st.markdown("---")

# Event Source Distribution
st.markdown("## 📊 Event Sources")

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
    col1, col2 = st.columns([1, 2])
    
    with col1:
        st.dataframe(
            sources_df[['SOURCE', 'COUNT', 'PERCENTAGE']],
            hide_index=True,
            column_config={
                "SOURCE": "Source",
                "COUNT": st.column_config.NumberColumn("Events", format="%d"),
                "PERCENTAGE": st.column_config.NumberColumn("Percent", format="%.1f%%")
            }
        )
    
    with col2:
        fig_pie = px.pie(
            sources_df, 
            values='COUNT', 
            names='SOURCE',
            title="Event Distribution by Source"
        )
        fig_pie.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig_pie, use_container_width=True)
else:
    st.info("No source data available")

st.markdown("---")

# Recent Events Table
st.markdown("## 📋 Recent Events")

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
LIMIT 100
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

# Footer
st.markdown("---")
st.caption(f"Dashboard refreshed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Data range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")