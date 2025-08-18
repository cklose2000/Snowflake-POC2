"""
Minimal Data-First Chat Interface for Snowflake (Simple Version)
Direct SQL execution without MCP procedures
"""

import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session
from datetime import datetime, timedelta

# Get Snowflake session
session = get_active_session()

# Configure page - minimal, single column
st.set_page_config(
    page_title="Data Chat", 
    page_icon="💬",
    layout="centered"  # Single column, not wide
)

# Minimal title
st.title("💬 Data Chat")
st.caption("Ask questions about your activity data")

# Initialize session state
if "messages" not in st.session_state:
    st.session_state.messages = []
if "last_query" not in st.session_state:
    st.session_state.last_query = None
if "last_result" not in st.session_state:
    st.session_state.last_result = None

def parse_user_input(user_input: str) -> dict:
    """
    Simple pattern matching to understand user intent
    """
    input_lower = user_input.lower()
    
    # Detect time range
    if "hour" in input_lower or "today" in input_lower:
        time_range = "24 hours"
        time_filter = "OCCURRED_AT >= CURRENT_TIMESTAMP() - INTERVAL '24 hours'"
    elif "week" in input_lower:
        time_range = "7 days"
        time_filter = "OCCURRED_AT >= CURRENT_TIMESTAMP() - INTERVAL '7 days'"
    elif "month" in input_lower:
        time_range = "30 days"
        time_filter = "OCCURRED_AT >= CURRENT_TIMESTAMP() - INTERVAL '30 days'"
    else:
        time_range = "7 days"
        time_filter = "OCCURRED_AT >= CURRENT_TIMESTAMP() - INTERVAL '7 days'"
    
    # Detect query type
    query_type = "summary"  # default
    
    if any(word in input_lower for word in ["count", "how many", "total"]):
        query_type = "count"
    elif any(word in input_lower for word in ["top", "most", "highest"]):
        if "user" in input_lower or "actor" in input_lower:
            query_type = "top_actors"
        elif "action" in input_lower:
            query_type = "top_actions"
        elif "source" in input_lower:
            query_type = "top_sources"
        else:
            query_type = "top_actions"  # default to actions
    elif any(word in input_lower for word in ["recent", "latest", "last"]):
        query_type = "recent"
    elif any(word in input_lower for word in ["error", "fail", "problem"]):
        query_type = "errors"
    elif "change" in input_lower:
        query_type = "changes"
    
    return {
        "query_type": query_type,
        "time_filter": time_filter,
        "time_range": time_range
    }

def build_query(parsed_input: dict) -> str:
    """
    Build SQL query based on parsed input
    """
    query_type = parsed_input["query_type"]
    time_filter = parsed_input["time_filter"]
    
    if query_type == "count":
        return f"""
        SELECT 
            COUNT(*) as total_events,
            COUNT(DISTINCT ACTOR_ID) as unique_actors,
            COUNT(DISTINCT ACTION) as unique_actions
        FROM CLAUDE_BI.ACTIVITY.EVENTS
        WHERE {time_filter}
        """
    
    elif query_type == "top_actors":
        return f"""
        SELECT 
            ACTOR_ID,
            COUNT(*) as event_count,
            COUNT(DISTINCT ACTION) as unique_actions
        FROM CLAUDE_BI.ACTIVITY.EVENTS
        WHERE {time_filter}
        GROUP BY ACTOR_ID
        ORDER BY event_count DESC
        LIMIT 10
        """
    
    elif query_type == "top_actions":
        return f"""
        SELECT 
            ACTION,
            COUNT(*) as occurrence_count,
            COUNT(DISTINCT ACTOR_ID) as unique_actors
        FROM CLAUDE_BI.ACTIVITY.EVENTS
        WHERE {time_filter}
        GROUP BY ACTION
        ORDER BY occurrence_count DESC
        LIMIT 10
        """
    
    elif query_type == "top_sources":
        return f"""
        SELECT 
            SOURCE,
            COUNT(*) as event_count,
            ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as percentage
        FROM CLAUDE_BI.ACTIVITY.EVENTS
        WHERE {time_filter}
            AND SOURCE IS NOT NULL
        GROUP BY SOURCE
        ORDER BY event_count DESC
        LIMIT 10
        """
    
    elif query_type == "recent":
        return f"""
        SELECT 
            OCCURRED_AT,
            ACTION,
            ACTOR_ID,
            OBJECT_TYPE,
            OBJECT_ID,
            SOURCE
        FROM CLAUDE_BI.ACTIVITY.EVENTS
        WHERE {time_filter}
        ORDER BY OCCURRED_AT DESC
        LIMIT 20
        """
    
    elif query_type == "errors":
        return f"""
        SELECT 
            OCCURRED_AT,
            ACTION,
            ACTOR_ID,
            ATTRIBUTES:error_message::STRING as error_message,
            ATTRIBUTES:error_code::STRING as error_code
        FROM CLAUDE_BI.ACTIVITY.EVENTS
        WHERE {time_filter}
            AND (ACTION LIKE '%error%' OR ACTION LIKE '%fail%' OR ACTION LIKE '%issue%')
        ORDER BY OCCURRED_AT DESC
        LIMIT 20
        """
    
    elif query_type == "changes":
        return f"""
        WITH recent_stats AS (
            SELECT 
                DATE_TRUNC('hour', OCCURRED_AT) as hour_bucket,
                COUNT(*) as events_per_hour
            FROM CLAUDE_BI.ACTIVITY.EVENTS
            WHERE {time_filter}
            GROUP BY 1
        )
        SELECT 
            hour_bucket,
            events_per_hour,
            LAG(events_per_hour) OVER (ORDER BY hour_bucket) as previous_hour,
            events_per_hour - LAG(events_per_hour) OVER (ORDER BY hour_bucket) as change
        FROM recent_stats
        ORDER BY hour_bucket DESC
        LIMIT 24
        """
    
    else:  # summary
        return f"""
        WITH summary AS (
            SELECT 
                COUNT(*) as total_events,
                COUNT(DISTINCT ACTOR_ID) as unique_actors,
                COUNT(DISTINCT ACTION) as unique_actions,
                COUNT(DISTINCT DATE(OCCURRED_AT)) as active_days,
                MAX(OCCURRED_AT) as latest_event,
                MIN(OCCURRED_AT) as earliest_event
            FROM CLAUDE_BI.ACTIVITY.EVENTS
            WHERE {time_filter}
        ),
        top_action AS (
            SELECT ACTION, COUNT(*) as count
            FROM CLAUDE_BI.ACTIVITY.EVENTS
            WHERE {time_filter}
            GROUP BY ACTION
            ORDER BY count DESC
            LIMIT 1
        )
        SELECT 
            s.*,
            ta.ACTION as most_common_action,
            ta.count as most_common_count
        FROM summary s
        CROSS JOIN top_action ta
        """

def execute_query(query: str) -> pd.DataFrame:
    """
    Execute query and return results
    """
    try:
        result = session.sql(query).to_pandas()
        st.session_state.last_query = query
        st.session_state.last_result = result
        return result
    except Exception as e:
        return pd.DataFrame({"error": [str(e)]})

def format_results(df: pd.DataFrame, query_type: str, time_range: str) -> str:
    """
    Format query results into readable response
    """
    if df.empty:
        return "📭 No data found for your query."
    
    if "error" in df.columns:
        return f"❌ Error: {df['error'].iloc[0]}"
    
    if query_type == "count":
        response = f"📊 **Activity Summary (Last {time_range}):**\n\n"
        response += f"• **Total Events**: {df['TOTAL_EVENTS'].iloc[0]:,}\n"
        response += f"• **Unique Actors**: {df['UNIQUE_ACTORS'].iloc[0]:,}\n"
        response += f"• **Unique Actions**: {df['UNIQUE_ACTIONS'].iloc[0]:,}"
        return response
    
    elif query_type == "top_actors":
        response = f"👥 **Top Active Users (Last {time_range}):**\n\n"
        for i, row in df.head(5).iterrows():
            response += f"{i+1}. **{row['ACTOR_ID']}**: {row['EVENT_COUNT']:,} events "
            response += f"({row['UNIQUE_ACTIONS']} unique actions)\n"
        return response
    
    elif query_type == "top_actions":
        response = f"🎯 **Top Actions (Last {time_range}):**\n\n"
        for i, row in df.head(5).iterrows():
            response += f"{i+1}. **{row['ACTION']}**: {row['OCCURRENCE_COUNT']:,} times "
            response += f"({row['UNIQUE_ACTORS']} unique actors)\n"
        return response
    
    elif query_type == "top_sources":
        response = f"📍 **Event Sources (Last {time_range}):**\n\n"
        for i, row in df.head(5).iterrows():
            response += f"{i+1}. **{row['SOURCE']}**: {row['EVENT_COUNT']:,} events "
            response += f"({row['PERCENTAGE']:.1f}%)\n"
        return response
    
    elif query_type == "recent":
        response = f"📋 **Recent Events (Last {time_range}):**\n\n"
        for i, row in df.head(5).iterrows():
            time_str = row['OCCURRED_AT'].strftime('%H:%M:%S') if pd.notnull(row['OCCURRED_AT']) else 'Unknown'
            response += f"• **{time_str}**: {row['ACTOR_ID']} → {row['ACTION']}\n"
        if len(df) > 5:
            response += f"\n*Showing 5 of {len(df)} events*"
        return response
    
    elif query_type == "errors":
        if df.empty:
            return f"✅ **No errors found in the last {time_range}!**"
        response = f"⚠️ **Recent Errors (Last {time_range}):**\n\n"
        for i, row in df.head(5).iterrows():
            time_str = row['OCCURRED_AT'].strftime('%H:%M:%S') if pd.notnull(row['OCCURRED_AT']) else 'Unknown'
            response += f"• **{time_str}**: {row['ACTION']}\n"
            if pd.notnull(row.get('ERROR_MESSAGE')):
                response += f"  *{row['ERROR_MESSAGE']}*\n"
        return response
    
    elif query_type == "changes":
        response = f"📈 **Activity Changes (Last {time_range}):**\n\n"
        recent_changes = df[df['CHANGE'].notna()].head(5)
        for _, row in recent_changes.iterrows():
            hour_str = row['HOUR_BUCKET'].strftime('%m/%d %H:00') if pd.notnull(row['HOUR_BUCKET']) else 'Unknown'
            change = row['CHANGE']
            trend = "📈" if change > 0 else "📉" if change < 0 else "➡️"
            response += f"• **{hour_str}**: {row['EVENTS_PER_HOUR']:,} events {trend} "
            if change != 0:
                response += f"({change:+,.0f} from previous hour)\n"
            else:
                response += "(no change)\n"
        return response
    
    else:  # summary
        response = f"📊 **Data Summary (Last {time_range}):**\n\n"
        response += f"• **Total Events**: {df['TOTAL_EVENTS'].iloc[0]:,}\n"
        response += f"• **Unique Actors**: {df['UNIQUE_ACTORS'].iloc[0]:,}\n"
        response += f"• **Unique Actions**: {df['UNIQUE_ACTIONS'].iloc[0]:,}\n"
        response += f"• **Active Days**: {df['ACTIVE_DAYS'].iloc[0]}\n"
        if 'MOST_COMMON_ACTION' in df.columns:
            response += f"• **Most Common Action**: {df['MOST_COMMON_ACTION'].iloc[0]} "
            response += f"({df['MOST_COMMON_COUNT'].iloc[0]:,} times)\n"
        if 'LATEST_EVENT' in df.columns:
            latest = df['LATEST_EVENT'].iloc[0]
            if pd.notnull(latest):
                response += f"• **Latest Event**: {latest.strftime('%Y-%m-%d %H:%M:%S')}"
        return response

# Quick action buttons
col1, col2, col3, col4 = st.columns(4)

with col1:
    if st.button("📊 Summary", use_container_width=True, help="Get overview of recent activity"):
        query = "Show me a summary of activity in the last 7 days"
        st.session_state.messages.append({"role": "user", "content": query})
        
        parsed = parse_user_input(query)
        sql = build_query(parsed)
        result = execute_query(sql)
        response = format_results(result, parsed["query_type"], parsed["time_range"])
        
        st.session_state.messages.append({"role": "assistant", "content": response})
        st.rerun()

with col2:
    if st.button("👥 Top Users", use_container_width=True, help="See most active users"):
        query = "Who are the top users this week?"
        st.session_state.messages.append({"role": "user", "content": query})
        
        parsed = parse_user_input(query)
        sql = build_query(parsed)
        result = execute_query(sql)
        response = format_results(result, parsed["query_type"], parsed["time_range"])
        
        st.session_state.messages.append({"role": "assistant", "content": response})
        st.rerun()

with col3:
    if st.button("⚠️ Errors", use_container_width=True, help="Check for recent errors"):
        query = "Show me recent errors"
        st.session_state.messages.append({"role": "user", "content": query})
        
        parsed = parse_user_input(query)
        sql = build_query(parsed)
        result = execute_query(sql)
        response = format_results(result, parsed["query_type"], parsed["time_range"])
        
        st.session_state.messages.append({"role": "assistant", "content": response})
        st.rerun()

with col4:
    if st.button("🗑️ Clear", use_container_width=True, help="Clear chat history"):
        st.session_state.messages = []
        st.session_state.last_query = None
        st.session_state.last_result = None
        st.rerun()

st.markdown("---")

# Display chat messages
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# Chat input
if prompt := st.chat_input("Ask about your data..."):
    # Add user message
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)
    
    # Generate and execute query
    with st.chat_message("assistant"):
        with st.spinner("Analyzing your question..."):
            # Parse input
            parsed = parse_user_input(prompt)
            
            # Build SQL
            sql = build_query(parsed)
            
            # Execute
            result = execute_query(sql)
            
            # Format response
            response = format_results(result, parsed["query_type"], parsed["time_range"])
            
            st.markdown(response)
            
            # Show data table if results exist
            if not result.empty and "error" not in result.columns:
                with st.expander("📊 View Raw Data"):
                    st.dataframe(result, use_container_width=True)
                    
                    # Download button
                    csv = result.to_csv(index=False)
                    st.download_button(
                        label="📥 Download CSV",
                        data=csv,
                        file_name=f"query_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv"
                    )
            
            # Save response
            st.session_state.messages.append({"role": "assistant", "content": response})

# Example questions
with st.expander("💡 Example Questions"):
    st.markdown("""
    Try asking questions like:
    
    **📊 Summaries:**
    - "Show me a summary of activity"
    - "What happened today?"
    - "How many events this week?"
    
    **👥 Users:**
    - "Who are the top users?"
    - "Show me the most active actors"
    - "Which users generated the most events?"
    
    **🎯 Actions:**
    - "What are the top actions?"
    - "Show me the most common activities"
    - "What actions happened today?"
    
    **📋 Recent Activity:**
    - "Show me recent events"
    - "What are the latest actions?"
    - "Display recent activity"
    
    **⚠️ Errors:**
    - "Show me recent errors"
    - "Any failures today?"
    - "Check for problems"
    
    **📈 Changes:**
    - "What changed recently?"
    - "Show activity changes"
    - "How is activity trending?"
    """)

# Debug info
with st.expander("🔍 Debug Info"):
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**Session Stats:**")
        st.write(f"• Messages: {len(st.session_state.messages)}")
        st.write(f"• Time: {datetime.now().strftime('%H:%M:%S')}")
    
    with col2:
        st.markdown("**Database:**")
        st.write("• Database: CLAUDE_BI")
        st.write("• Table: ACTIVITY.EVENTS")
    
    if st.session_state.last_query:
        st.markdown("**Last Query:**")
        st.code(st.session_state.last_query, language="sql")
    
    if st.session_state.last_result is not None and not st.session_state.last_result.empty:
        st.markdown("**Result Shape:**")
        st.write(f"• Rows: {len(st.session_state.last_result)}")
        st.write(f"• Columns: {len(st.session_state.last_result.columns)}")
        st.write(f"• Columns: {', '.join(st.session_state.last_result.columns)}")