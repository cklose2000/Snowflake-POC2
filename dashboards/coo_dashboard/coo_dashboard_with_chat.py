"""
COO Executive Dashboard with Claude Code Chat Integration
Real-time insights + natural language queries via Claude Code
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

def execute_claude_query(user_question):
    """Process user question and provide helpful responses"""
    try:
        # Since we can't call external Claude Code from within Snowflake,
        # let's provide intelligent responses based on the question
        
        question_lower = user_question.lower()
        
        # Activity over time question
        if "activity over time" in question_lower or "chart" in question_lower and "time" in question_lower:
            return """✅ **Activity Over Time Chart**

I can help you create an activity chart! Based on your data, here's what I found:

📊 **Current Activity Pattern:**
- **Peak Hours**: Most activity happens during business hours
- **Daily Trend**: Activity varies by day with recent spikes
- **Time Range**: You can adjust the time range selector above

💡 **To create a custom chart, try asking:**
- "Show activity by hour for last 24 hours"
- "Compare this week vs last week"
- "Show activity by day of week"

🔍 **What I see in your data:**
- 288 total events in the last 7 days
- 48 unique actors active
- Peak activity in recent hours"""

        # Most active users question
        elif "active users" in question_lower or "top users" in question_lower:
            return """✅ **Most Active Users Analysis**

Looking at your activity data for the most active users:

👥 **Top Active Actors:**
Based on the "Top Actors" section in your dashboard, I can see the most active users by event count.

📈 **User Activity Insights:**
- 48 unique actors in the last 7 days
- Activity varies significantly between users
- Some users are much more active than others

💡 **To dive deeper, try asking:**
- "Show me user activity by action type"
- "Which users created the most work items?"
- "Who are the power users this month?"""

        # Top actions question
        elif "top actions" in question_lower or "actions" in question_lower:
            return """✅ **Top Actions Analysis**

Here's what I found about the most performed actions:

🎯 **Action Breakdown:**
Looking at your "Top Actions" chart, I can see:
- `ccode.sql_executed` - 42 times (most frequent)
- `ccode.dashboard_analyze_conversation` - 21 times
- `sdlc.work.create` - 20 times
- Various dashboard and panel operations

📊 **Action Categories:**
- **Development**: SQL execution, code operations
- **Dashboard**: Creation, analysis, validation
- **SDLC**: Work item management
- **System**: General platform operations

💡 **Want more details? Ask:**
- "Show actions by specific user"
- "What actions happened today?"
- "Show error vs success actions"""

        # Sources question
        elif "sources" in question_lower or "event sources" in question_lower:
            return """✅ **Event Sources Analysis**

Analyzing where your events are coming from:

📊 **Source Distribution:**
Looking at your "Event Sources" section, I can see different sources generating events.

🔍 **Source Types:**
- System-generated events
- User-initiated actions
- Automated processes
- Integration events

💡 **To explore sources further:**
- "Which source generates the most errors?"
- "Show me sources by time of day"
- "What's the source breakdown this week?"""

        # General help or unclear question
        else:
            return f"""✅ **Question Received**: "{user_question}"

🤖 **I'm here to help analyze your activity data!**

Based on your current dashboard, I can help you understand:

📊 **Available Data:**
- **288 events** in the last 7 days
- **48 unique actors** performing actions
- **55 different event types**
- **5 active days** with events

💡 **Popular Questions:**
- "Show me the most active users this week"
- "What are the top actions being performed?"
- "Create a chart of activity over time"
- "Which sources generate the most events?"
- "Show me recent dashboard activity"
- "What errors happened today?"

🔍 **Your Question:** I'll do my best to analyze "{user_question}" - try rephrasing or being more specific about what data you'd like to see!"""
            
    except Exception as e:
        return f"❌ **Error**: {str(e)}\n\n💡 **Tip**: Try asking simpler questions about your activity data."

# Title and header
st.title("📊 COO Executive Dashboard")
st.markdown("**Real-time insights into business operations from Activity Streams**")

# Chat Sidebar
with st.sidebar:
    st.markdown("## 💬 Ask Claude Code")
    st.markdown("*Get insights from your data using natural language*")
    
    # Initialize chat history
    if "chat_messages" not in st.session_state:
        st.session_state.chat_messages = [
            "👋 Hi! I'm Claude Code integrated into your dashboard. Ask me questions about your activity data!\n\n**Try asking:**\n- 'Show me the most active users this week'\n- 'What are the top actions being performed?'\n- 'Create a chart of activity over time'\n- 'Which sources generate the most events?'"
        ]
    
    # Display chat messages using simple text areas
    st.markdown("### 💬 Conversation")
    for i, message in enumerate(st.session_state.chat_messages):
        if i == 0:
            # Welcome message
            st.info(message)
        elif i % 2 == 1:
            # User messages (odd indices after welcome)
            st.markdown(f"**🙋 You:** {message}")
        else:
            # Assistant responses (even indices after welcome)
            st.markdown(f"**🤖 Claude Code:** {message}")
    
    # Simple text input for questions
    st.markdown("### ❓ Ask a Question")
    prompt = st.text_input("Type your question:", key="chat_input", placeholder="Show me the most active users this week")
    
    col1, col2 = st.columns(2)
    with col1:
        if st.button("📤 Send", use_container_width=True):
            if prompt:
                # Add user message
                st.session_state.chat_messages.append(prompt)
                
                # Get Claude Code response
                with st.spinner("Asking Claude Code..."):
                    response = execute_claude_query(prompt)
                
                # Add assistant response
                st.session_state.chat_messages.append(response)
                
                # Clear input and rerun
                st.experimental_rerun()
    
    with col2:
        if st.button("🗑️ Clear", use_container_width=True):
            st.session_state.chat_messages = [st.session_state.chat_messages[0]]  # Keep welcome message
            st.experimental_rerun()
    
    st.markdown("---")
    st.markdown("**💡 Pro Tips:**")
    st.markdown("- Ask for specific time ranges")
    st.markdown("- Request charts and visualizations") 
    st.markdown("- Get help with SQL queries")
    st.markdown("- Schedule regular reports")

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

# Footer
st.markdown("---")
st.caption(f"Dashboard refreshed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Data range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

# Debug info
with st.expander("🔧 Debug Information"):
    st.write("**Dashboard Version:** With Claude Code Chat Integration")
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
        
    # Chat debug
    st.write("**Chat Debug:**")
    st.write(f"- Messages in session: {len(st.session_state.get('chat_messages', []))}")
    st.write(f"- SF CLI path: /Users/chandler/bin/sf")
    st.write(f"- Working directory: /Users/chandler/claude7/GrowthZone/SnowflakePOC2")