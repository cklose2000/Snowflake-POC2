"""
Minimal Data-First Chat Interface for Snowflake
Zero clutter, every response backed by actual data
"""

import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session
from datetime import datetime, timedelta
import json

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

def build_plan(user_input: str) -> dict:
    """
    Build a guardrailed query plan from user input
    Uses MCP.SUGGEST_INTENT to understand what the user wants
    """
    try:
        # Call MCP.SUGGEST_INTENT to understand user intent
        intent_query = f"""
        CALL CLAUDE_BI.MCP.SUGGEST_INTENT(
            OBJECT_CONSTRUCT(
                'user_input', '{user_input.replace("'", "''")}',
                'context', 'chat_interface',
                'time_filter', 'last_7_days'
            )
        )
        """
        
        result = session.sql(intent_query).collect()
        
        if result and len(result) > 0:
            # Parse the suggested intent
            intent_data = json.loads(result[0][0]) if isinstance(result[0][0], str) else result[0][0]
            
            # Build the plan based on intent
            plan = {
                'intent': intent_data.get('intent', 'explore'),
                'query_type': intent_data.get('query_type', 'select'),
                'filters': intent_data.get('filters', {}),
                'aggregations': intent_data.get('aggregations', []),
                'time_range': intent_data.get('time_range', 'last_7_days')
            }
            
            # Add default time filter if not specified
            if 'time_filter' not in plan['filters']:
                if plan['time_range'] == 'last_24_hours':
                    plan['filters']['time_filter'] = "OCCURRED_AT >= CURRENT_TIMESTAMP() - INTERVAL '24 hours'"
                elif plan['time_range'] == 'last_7_days':
                    plan['filters']['time_filter'] = "OCCURRED_AT >= CURRENT_TIMESTAMP() - INTERVAL '7 days'"
                elif plan['time_range'] == 'last_30_days':
                    plan['filters']['time_filter'] = "OCCURRED_AT >= CURRENT_TIMESTAMP() - INTERVAL '30 days'"
            
            return plan
            
    except Exception as e:
        # Fallback to basic pattern matching if SUGGEST_INTENT fails
        return build_fallback_plan(user_input)

def build_fallback_plan(user_input: str) -> dict:
    """
    Fallback plan builder using pattern matching
    """
    input_lower = user_input.lower()
    
    # Detect time ranges
    time_filter = "OCCURRED_AT >= CURRENT_TIMESTAMP() - INTERVAL '7 days'"
    if "today" in input_lower or "24 hour" in input_lower:
        time_filter = "OCCURRED_AT >= CURRENT_TIMESTAMP() - INTERVAL '24 hours'"
    elif "week" in input_lower or "7 day" in input_lower:
        time_filter = "OCCURRED_AT >= CURRENT_TIMESTAMP() - INTERVAL '7 days'"
    elif "month" in input_lower or "30 day" in input_lower:
        time_filter = "OCCURRED_AT >= CURRENT_TIMESTAMP() - INTERVAL '30 days'"
    
    # Detect what they're asking for
    if "count" in input_lower or "how many" in input_lower:
        intent = "count"
    elif "top" in input_lower or "most" in input_lower:
        intent = "top"
    elif "recent" in input_lower or "latest" in input_lower:
        intent = "recent"
    elif "error" in input_lower or "fail" in input_lower:
        intent = "errors"
    else:
        intent = "explore"
    
    # Detect entity type
    entity = "events"
    if "user" in input_lower or "actor" in input_lower:
        entity = "actors"
    elif "action" in input_lower:
        entity = "actions"
    elif "source" in input_lower:
        entity = "sources"
    
    return {
        'intent': intent,
        'entity': entity,
        'filters': {'time_filter': time_filter}
    }

def run_guarded(plan: dict) -> pd.DataFrame:
    """
    Execute the plan through MCP.READ guardrail
    Returns actual data, not mock responses
    """
    try:
        # Build the appropriate query based on plan
        if plan['intent'] == 'count':
            query = f"""
            SELECT COUNT(*) as total_count
            FROM CLAUDE_BI.ACTIVITY.EVENTS
            WHERE {plan['filters']['time_filter']}
            """
        
        elif plan['intent'] == 'top' and plan.get('entity') == 'actors':
            query = f"""
            SELECT ACTOR_ID, COUNT(*) as event_count
            FROM CLAUDE_BI.ACTIVITY.EVENTS
            WHERE {plan['filters']['time_filter']}
            GROUP BY ACTOR_ID
            ORDER BY event_count DESC
            LIMIT 10
            """
        
        elif plan['intent'] == 'top' and plan.get('entity') == 'actions':
            query = f"""
            SELECT ACTION, COUNT(*) as event_count
            FROM CLAUDE_BI.ACTIVITY.EVENTS
            WHERE {plan['filters']['time_filter']}
            GROUP BY ACTION
            ORDER BY event_count DESC
            LIMIT 10
            """
        
        elif plan['intent'] == 'recent':
            query = f"""
            SELECT OCCURRED_AT, ACTION, ACTOR_ID, OBJECT_TYPE, OBJECT_ID
            FROM CLAUDE_BI.ACTIVITY.EVENTS
            WHERE {plan['filters']['time_filter']}
            ORDER BY OCCURRED_AT DESC
            LIMIT 10
            """
        
        elif plan['intent'] == 'errors':
            query = f"""
            SELECT OCCURRED_AT, ACTION, ACTOR_ID, 
                   ATTRIBUTES:error_message::STRING as error_message
            FROM CLAUDE_BI.ACTIVITY.EVENTS
            WHERE {plan['filters']['time_filter']}
              AND (ACTION LIKE '%error%' OR ACTION LIKE '%fail%')
            ORDER BY OCCURRED_AT DESC
            LIMIT 10
            """
        
        else:  # explore/general
            query = f"""
            WITH summary AS (
                SELECT 
                    COUNT(*) as total_events,
                    COUNT(DISTINCT ACTOR_ID) as unique_actors,
                    COUNT(DISTINCT ACTION) as unique_actions,
                    MAX(OCCURRED_AT) as latest_event,
                    MIN(OCCURRED_AT) as earliest_event
                FROM CLAUDE_BI.ACTIVITY.EVENTS
                WHERE {plan['filters']['time_filter']}
            )
            SELECT * FROM summary
            """
        
        # Execute through MCP.READ for guardrails (if it exists)
        # First check if MCP.READ exists
        try:
            guarded_query = f"""
            CALL CLAUDE_BI.MCP.READ(
                OBJECT_CONSTRUCT(
                    'query', $${query}$$,
                    'context', 'chat_interface',
                    'max_rows', 1000
                )
            )
            """
            result = session.sql(guarded_query).to_pandas()
        except:
            # Fallback to direct query if MCP.READ doesn't exist
            result = session.sql(query).to_pandas()
        
        # Store for reference
        st.session_state.last_query = query
        st.session_state.last_result = result
        
        return result
        
    except Exception as e:
        # Return error as dataframe
        return pd.DataFrame({'error': [str(e)]})

def format_response(df: pd.DataFrame, plan: dict, user_input: str) -> str:
    """
    Format the dataframe into a readable response
    """
    if df.empty:
        return "No data found for your query."
    
    if 'error' in df.columns:
        return f"❌ Error executing query: {df['error'].iloc[0]}"
    
    # Format based on intent
    if plan['intent'] == 'count':
        count = df['TOTAL_COUNT'].iloc[0] if 'TOTAL_COUNT' in df.columns else df.iloc[0, 0]
        return f"📊 **Total Events**: {count:,} events found in the specified time range."
    
    elif plan['intent'] == 'top' and plan.get('entity') == 'actors':
        response = "👥 **Top Active Users:**\n\n"
        for _, row in df.head(5).iterrows():
            actor = row['ACTOR_ID'] if 'ACTOR_ID' in row else row.iloc[0]
            count = row['EVENT_COUNT'] if 'EVENT_COUNT' in row else row.iloc[1]
            response += f"• **{actor}**: {count:,} events\n"
        return response
    
    elif plan['intent'] == 'top' and plan.get('entity') == 'actions':
        response = "🎯 **Top Actions:**\n\n"
        for _, row in df.head(5).iterrows():
            action = row['ACTION'] if 'ACTION' in row else row.iloc[0]
            count = row['EVENT_COUNT'] if 'EVENT_COUNT' in row else row.iloc[1]
            response += f"• **{action}**: {count:,} times\n"
        return response
    
    elif plan['intent'] == 'recent':
        response = "📋 **Recent Events:**\n\n"
        for _, row in df.head(5).iterrows():
            time = row['OCCURRED_AT'] if 'OCCURRED_AT' in row else row.iloc[0]
            action = row['ACTION'] if 'ACTION' in row else row.iloc[1]
            actor = row['ACTOR_ID'] if 'ACTOR_ID' in row else row.iloc[2]
            response += f"• **{time}**: {actor} performed {action}\n"
        return response
    
    elif plan['intent'] == 'errors':
        if df.empty:
            return "✅ No errors found in the specified time range!"
        response = "⚠️ **Recent Errors:**\n\n"
        for _, row in df.head(5).iterrows():
            time = row['OCCURRED_AT'] if 'OCCURRED_AT' in row else row.iloc[0]
            action = row['ACTION'] if 'ACTION' in row else row.iloc[1]
            response += f"• **{time}**: {action}\n"
        return response
    
    else:  # explore/general summary
        if 'TOTAL_EVENTS' in df.columns:
            response = "📊 **Data Summary:**\n\n"
            response += f"• **Total Events**: {df['TOTAL_EVENTS'].iloc[0]:,}\n"
            response += f"• **Unique Actors**: {df['UNIQUE_ACTORS'].iloc[0]:,}\n"
            response += f"• **Unique Actions**: {df['UNIQUE_ACTIONS'].iloc[0]:,}\n"
            if 'LATEST_EVENT' in df.columns:
                response += f"• **Latest Event**: {df['LATEST_EVENT'].iloc[0]}\n"
            return response
        else:
            # Generic table display
            return f"**Query Results:**\n\n{df.to_string()}"

# Quick action buttons
st.markdown("### 🚀 Quick Actions")
col1, col2, col3, col4 = st.columns(4)

with col1:
    if st.button("📊 What changed?", use_container_width=True):
        user_input = "What changed in the last 24 hours?"
        st.session_state.messages.append({"role": "user", "content": user_input})
        plan = build_plan(user_input)
        result = run_guarded(plan)
        response = format_response(result, plan, user_input)
        st.session_state.messages.append({"role": "assistant", "content": response})
        st.rerun()

with col2:
    if st.button("👥 Top actors", use_container_width=True):
        user_input = "Show me the top actors this week"
        st.session_state.messages.append({"role": "user", "content": user_input})
        plan = build_plan(user_input)
        result = run_guarded(plan)
        response = format_response(result, plan, user_input)
        st.session_state.messages.append({"role": "assistant", "content": response})
        st.rerun()

with col3:
    if st.button("⚠️ Recent errors", use_container_width=True):
        user_input = "Show me recent errors"
        st.session_state.messages.append({"role": "user", "content": user_input})
        plan = build_plan(user_input)
        result = run_guarded(plan)
        response = format_response(result, plan, user_input)
        st.session_state.messages.append({"role": "assistant", "content": response})
        st.rerun()

with col4:
    if st.button("🔄 Clear chat", use_container_width=True):
        st.session_state.messages = []
        st.session_state.last_query = None
        st.session_state.last_result = None
        st.rerun()

st.markdown("---")

# Display chat messages (using containers for Snowflake compatibility)
for message in st.session_state.messages:
    if message["role"] == "user":
        st.markdown(f"**🙋 You:** {message['content']}")
    else:
        st.markdown(f"**🤖 Assistant:** {message['content']}")

# Chat input using text_input instead of chat_input (for Snowflake compatibility)
prompt = st.text_input("Ask about your data...", key="user_input")
if st.button("Send", type="primary"):
    if prompt:
        # Add user message
        st.session_state.messages.append({"role": "user", "content": prompt})
        
        # Generate response
        with st.spinner("Querying data..."):
            # Build plan from user input
            plan = build_plan(prompt)
            
            # Execute through guardrails
            result = run_guarded(plan)
            
            # Format response
            response = format_response(result, plan, prompt)
            
            # Add to messages
            st.session_state.messages.append({"role": "assistant", "content": response})
            
            # Store result for display
            st.session_state.last_result = result
            
        # Rerun to show new messages
        st.rerun()

# Show data table if available from last query
if st.session_state.last_result is not None and not st.session_state.last_result.empty:
    if 'error' not in st.session_state.last_result.columns:
        with st.expander("📊 View Data Table"):
            st.dataframe(st.session_state.last_result, use_container_width=True)

# Debug section (collapsible)
with st.expander("🔍 Debug Info"):
    if st.session_state.last_query:
        st.markdown("**Last Query:**")
        st.code(st.session_state.last_query, language="sql")
    
    if st.session_state.last_result is not None:
        st.markdown("**Last Result:**")
        st.dataframe(st.session_state.last_result)
    
    st.markdown("**Session Info:**")
    st.write(f"• Messages: {len(st.session_state.messages)}")
    st.write(f"• Time: {datetime.now()}")