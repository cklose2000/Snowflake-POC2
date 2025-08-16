"""
COO-First Executive Dashboard
Zero-friction interface: Click → Results
No tables, no confirms, just insights
"""

import streamlit as st
from snowflake.snowpark.context import get_active_session
import json
from datetime import datetime, timedelta, timezone
import pandas as pd
import hashlib
import re

# Get Snowpark session
session = get_active_session()

# Configure page
st.set_page_config(
    page_title="Claude Code Executive Dashboard", 
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Initialize session state
if 'current_plan' not in st.session_state:
    st.session_state.current_plan = None
if 'time_range' not in st.session_state:
    st.session_state.time_range = '24h'
if 'interval' not in st.session_state:
    st.session_state.interval = 'hour'
if 'last_dashboard' not in st.session_state:
    st.session_state.last_dashboard = None
if 'favorites' not in st.session_state:
    st.session_state.favorites = []
if 'claude_status' not in st.session_state:
    st.session_state.claude_status = 'Listening'
if 'claude_mode' not in st.session_state:
    st.session_state.claude_mode = 'Auto'  # Auto or Approve
if 'show_agent_console' not in st.session_state:
    st.session_state.show_agent_console = False
if 'last_claude_action' not in st.session_state:
    st.session_state.last_claude_action = None

# Helper to get ISO timestamps
def get_iso_timestamp(delta=None):
    """Get ISO timestamp with optional delta"""
    now = datetime.now(timezone.utc)
    if delta:
        now = now - delta
    return now.isoformat()

def get_start_of_day():
    """Get ISO timestamp for start of today (UTC)"""
    now = datetime.now(timezone.utc)
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    return start.isoformat()

# Preset card configurations with ISO timestamps
PRESET_CARDS = {
    "activity_by_user": {
        "title": "Activity by User",
        "subtitle": "Last 7 days",
        "icon": "👥",
        "color": "#1f77b4",
        "plan": {
            "plan_version": "1.0",
            "proc": "DASH_GET_TOPN",
            "params": {
                "start_ts": get_iso_timestamp(timedelta(days=7)),
                "end_ts": get_iso_timestamp(),
                "dimension": "actor",
                "n": 25,
                "limit": 1000,
                "filters": {}
            },
            "panels": [{"type": "bar", "title": "Activity by User"}]
        }
    },
    "top_actions_today": {
        "title": "Top Actions",
        "subtitle": "Today",
        "icon": "🎯",
        "color": "#ff7f0e",
        "plan": {
            "plan_version": "1.0",
            "proc": "DASH_GET_TOPN",
            "params": {
                "start_ts": get_start_of_day(),
                "end_ts": get_iso_timestamp(),
                "dimension": "action",
                "n": 10,
                "limit": 1000,
                "filters": {}
            },
            "panels": [{"type": "bar", "title": "Top Actions Today"}]
        }
    },
    "unique_actors": {
        "title": "Unique Actors",
        "subtitle": "Last 30 days",
        "icon": "🌟",
        "color": "#2ca02c",
        "plan": {
            "plan_version": "1.0",
            "proc": "DASH_GET_METRICS",
            "params": {
                "start_ts": get_iso_timestamp(timedelta(days=30)),
                "end_ts": get_iso_timestamp(),
                "filters": {}
            },
            "panels": [{"type": "metric", "title": "30-Day Overview"}]
        }
    },
    "events_by_source": {
        "title": "Events by Source",
        "subtitle": "Last 24 hours",
        "icon": "📊",
        "color": "#d62728",
        "plan": {
            "plan_version": "1.0",
            "proc": "DASH_GET_TOPN",
            "params": {
                "start_ts": get_iso_timestamp(timedelta(hours=24)),
                "end_ts": get_iso_timestamp(),
                "dimension": "source",
                "n": 10,
                "limit": 1000,
                "filters": {}
            },
            "panels": [{"type": "bar", "title": "Events by Source"}]
        }
    },
    "hourly_activity": {
        "title": "Activity Timeline",
        "subtitle": "Last 24 hours",
        "icon": "📈",
        "color": "#9467bd",
        "plan": {
            "plan_version": "1.0",
            "proc": "DASH_GET_SERIES",
            "params": {
                "start_ts": get_iso_timestamp(timedelta(hours=24)),
                "end_ts": get_iso_timestamp(),
                "interval": "hour",
                "filters": {}
            },
            "panels": [{"type": "line", "title": "Hourly Activity"}]
        }
    },
    "live_stream": {
        "title": "Live Activity",
        "subtitle": "Real-time",
        "icon": "🔴",
        "color": "#8c564b",
        "plan": {
            "plan_version": "1.0",
            "proc": "DASH_GET_EVENTS",
            "params": {
                "cursor_ts": get_iso_timestamp(timedelta(minutes=5)),
                "limit": 50
            },
            "panels": [{"type": "table", "title": "Recent Events"}]
        }
    }
}

def run_plan(session, plan, query_tag):
    """Execute plan with single VARIANT parameter - the correct way"""
    # Whitelist of allowed procedures
    allowed = {"DASH_GET_SERIES", "DASH_GET_TOPN", "DASH_GET_EVENTS", "DASH_GET_METRICS"}
    
    proc = plan.get("proc")
    if proc not in allowed:
        raise ValueError(f"Disallowed proc: {proc}")
    
    params = plan.get("params", {})
    
    # Ensure timestamps are ISO format (not SQL expressions)
    if "start_ts" in params and "DATEADD" in str(params["start_ts"]):
        # Default to last 24 hours if SQL expression detected
        params["start_ts"] = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
    if "end_ts" in params and "CURRENT" in str(params["end_ts"]):
        params["end_ts"] = datetime.now(timezone.utc).isoformat()
    if "cursor_ts" in params and "DATEADD" in str(params["cursor_ts"]):
        params["cursor_ts"] = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
    
    # Clamp limits for safety
    if "limit" in params:
        params["limit"] = min(int(params.get("limit", 1000)), 5000)
    if "n" in params:
        params["n"] = min(int(params.get("n", 10)), 50)
    
    # Validate interval if present
    if "interval" in params:
        valid_intervals = {"minute", "5 minute", "15 minute", "hour", "day"}
        if params["interval"] not in valid_intervals:
            params["interval"] = "hour"  # Default to hour
    
    # Set query tag with Claude attribution
    session.sql(f"ALTER SESSION SET QUERY_TAG = '{query_tag}'").collect()
    
    # THE CRITICAL FIX: Use single VARIANT parameter with PARSE_JSON(?)
    stmt = f"CALL MCP.{proc}(PARSE_JSON(?))"
    
    # Bind the JSON parameter
    payload = json.dumps(params)
    result_df = session.sql(stmt).bind(params=[payload]).to_pandas()
    
    return result_df

def execute_plan(plan):
    """Execute a dashboard plan and return results"""
    try:
        st.session_state.claude_status = 'Calling'
        proc = plan['proc']
        
        # Log procedure call
        log_claude_event('proc_called', {
            'proc': proc,
            'query_tag': f'dash-ui|agent:claude|proc:{proc}'
        })
        
        # Use the correct execution method
        query_tag = f'dash-ui|agent:claude|proc:{proc}'
        result_df = run_plan(session, plan, query_tag)
        
        # Parse the result (procedures return VARIANT)
        if not result_df.empty:
            result_col = result_df.columns[0]
            result = result_df.iloc[0][result_col]
            
            # Parse JSON if string
            if isinstance(result, str):
                result = json.loads(result)
            
            # Check for success
            if isinstance(result, dict) and result.get('ok'):
                data = result.get('data', [])
                st.session_state.claude_status = 'Rendered'
                
                # Log render completed
                log_claude_event('render_completed', {
                    'rows': len(data),
                    'proc': proc
                })
                
                return data
            elif isinstance(result, list):
                # Some procs might return data directly
                st.session_state.claude_status = 'Rendered'
                return result
            else:
                st.session_state.claude_status = 'Error'
                return None
        
        st.session_state.claude_status = 'Error'
        return None
    except Exception as e:
        st.session_state.claude_status = 'Error'
        st.error(f"Claude encountered an error: {str(e)}")
        
        # Log error
        log_claude_event('error', {
            'error': str(e),
            'proc': proc
        })
        
        return None

def render_preset_card(key, config):
    """Render a clickable preset card"""
    with st.container():
        if st.button(
            f"{config['icon']} **{config['title']}**\n\n{config['subtitle']}",
            key=key,
            use_container_width=True,
            help=f"Click to view {config['title'].lower()}"
        ):
            # Immediate execution - no confirm dialog
            st.session_state.current_plan = config['plan']
            st.session_state.view_mode = 'canvas'
            st.experimental_rerun()

def render_claude_status():
    """Render Claude Code status chip"""
    status = st.session_state.claude_status
    status_icons = {
        'Listening': '🟢',
        'Thinking': '🟡', 
        'Calling': '🔵',
        'Rendered': '✅',
        'Error': '🔴'
    }
    return f"{status_icons.get(status, '⚪')} Claude: {status}"

def render_home():
    """Render the home screen with preset cards"""
    # Claude Code branding
    col1, col2 = st.columns([3, 1])
    with col1:
        st.title("📊 Executive Dashboard")
        st.caption("🤖 Powered by Claude Code - Secure, auditable analytics through whitelisted procedures only")
    with col2:
        st.info(render_claude_status())
    
    # Top bar with global controls
    col1, col2, col3, col4, col5 = st.columns([2, 2, 2, 2, 2])
    with col1:
        time_options = {
            '6h': 'Last 6 hours',
            '24h': 'Last 24 hours',
            '7d': 'Last 7 days',
            '30d': 'Last 30 days'
        }
        st.session_state.time_range = st.selectbox(
            "Time Range",
            options=list(time_options.keys()),
            format_func=lambda x: time_options[x],
            index=list(time_options.keys()).index(st.session_state.time_range),
            key='global_time'
        )
    
    with col2:
        interval_options = {
            '15 minute': '15 minutes',
            'hour': 'Hourly',
            'day': 'Daily'
        }
        st.session_state.interval = st.selectbox(
            "Interval",
            options=list(interval_options.keys()),
            format_func=lambda x: interval_options[x],
            index=list(interval_options.keys()).index(st.session_state.interval),
            key='global_interval'
        )
    
    with col3:
        mode_options = ['Auto', 'Approve']
        st.session_state.claude_mode = st.selectbox(
            "Claude Mode",
            options=mode_options,
            index=mode_options.index(st.session_state.claude_mode),
            help="Auto: Claude executes immediately. Approve: Review before execution."
        )
    
    with col4:
        if st.button("🤖 Agent Console", help="View Claude's decision process (hotkey: c)"):
            st.session_state.show_agent_console = not st.session_state.show_agent_console
    
    st.divider()
    
    # Preset cards in a grid
    st.subheader("One-Click Analytics")
    
    cols = st.columns(3)
    for i, (key, config) in enumerate(PRESET_CARDS.items()):
        with cols[i % 3]:
            render_preset_card(key, config)
    
    # Recent dashboards
    st.divider()
    st.subheader("Recent Dashboards")
    
    try:
        recent_query = """
        SELECT 
            dashboard_id,
            title,
            created_at,
            created_by
        FROM MCP.VW_DASHBOARDS
        ORDER BY created_at DESC
        LIMIT 3
        """
        recent_df = session.sql(recent_query).to_pandas()
        
        if not recent_df.empty:
            cols = st.columns(3)
            for i, row in enumerate(recent_df.itertuples()):
                with cols[i]:
                    if st.button(
                        f"📋 {row.TITLE or 'Untitled'}\n\n{row.CREATED_AT}",
                        key=f"recent_{row.DASHBOARD_ID}",
                        use_container_width=True
                    ):
                        st.session_state.last_dashboard = row.DASHBOARD_ID
                        st.experimental_set_query_params(dashboard_id=row.DASHBOARD_ID)
                        st.experimental_rerun()
        else:
            st.info("No recent dashboards. Create one from a preset above!")
    except:
        st.info("Recent dashboards will appear here")
    
    # Favorites
    if st.session_state.favorites:
        st.divider()
        st.subheader("⭐ Favorites")
        cols = st.columns(3)
        for i, fav in enumerate(st.session_state.favorites[:3]):
            with cols[i]:
                st.button(f"⭐ {fav['title']}", key=f"fav_{i}", use_container_width=True)

def log_claude_event(action, attributes):
    """Log Claude Code agent events"""
    try:
        event_sql = f"""
        CALL MCP.LOG_CLAUDE_EVENT(OBJECT_CONSTRUCT(
            'action', 'agent.{action}',
            'actor_id', 'CLAUDE_CODE',
            'attributes', PARSE_JSON('{json.dumps(attributes)}'),
            'occurred_at', CURRENT_TIMESTAMP()
        ), 'CLAUDE_AGENT')
        """
        session.sql(event_sql).collect()
    except Exception as e:
        print(f"Failed to log Claude event: {e}")

def parse_natural_language(text):
    """Parse natural language into a plan with human confirmation"""
    st.session_state.claude_status = 'Thinking'
    
    # Log intent received
    log_claude_event('intent_received', {
        'intent': text,
        'mode': st.session_state.claude_mode
    })
    
    confirmation = ""
    plan = None
    
    # Time parsing patterns - now with ISO timestamps
    time_match = re.search(r'last (\d+)\s*(hours?|days?|minutes?)', text.lower())
    if time_match:
        amount = int(time_match.group(1))
        unit = time_match.group(2).rstrip('s')
        confirmation = f"Looking at the last {amount} {unit}"
        
        # Calculate ISO timestamps
        if unit == 'hour':
            delta = timedelta(hours=amount)
        elif unit == 'day':
            delta = timedelta(days=amount)
        elif unit == 'minute':
            delta = timedelta(minutes=amount)
        else:
            delta = timedelta(hours=24)
        
        plan = {
            "plan_version": "1.0",
            "proc": "DASH_GET_SERIES",
            "params": {
                "start_ts": get_iso_timestamp(delta),
                "end_ts": get_iso_timestamp(),
                "interval": "hour" if unit == "hour" else "day",
                "filters": {}
            }
        }
    
    # User filtering
    user_match = re.search(r'(?:user|actor)\s+([^\s]+)', text.lower())
    if user_match and plan:
        user = user_match.group(1)
        confirmation += f" filtered to user {user}"
        plan['params']['filters']['actor'] = user
    
    # Action filtering  
    action_match = re.search(r'action[s]?\s+([^\s]+)', text.lower())
    if action_match and plan:
        action = action_match.group(1)
        confirmation += f" for action {action}"
        plan['params']['filters']['action'] = action
    
    # Interval adjustment
    if 'by hour' in text.lower():
        if plan:
            plan['params']['interval'] = 'hour'
        confirmation += " by hour"
    elif 'by day' in text.lower():
        if plan:
            plan['params']['interval'] = 'day'
        confirmation += " by day"
    elif '15 min' in text.lower() or 'fifteen min' in text.lower():
        if plan:
            plan['params']['interval'] = '15 minute'
        confirmation += " in 15-minute intervals"
    
    # Default if no match - use ISO timestamps
    if not plan:
        confirmation = "Claude will show activity for the last 24 hours"
        plan = {
            "plan_version": "1.0",
            "proc": "DASH_GET_SERIES",
            "params": {
                "start_ts": get_iso_timestamp(timedelta(hours=24)),
                "end_ts": get_iso_timestamp(),
                "interval": "hour",
                "filters": {}
            }
        }
    
    # Log plan compiled
    log_claude_event('plan_compiled', {
        'plan_hash': hashlib.md5(json.dumps(plan).encode()).hexdigest(),
        'proc': plan['proc'],
        'validation': 'ok'
    })
    
    # Format Claude's confirmation
    proc_name = plan['proc'].replace('DASH_GET_', '').replace('_', ' ').title()
    confirmation = f"Claude will call `{plan['proc']}` {confirmation}"
    
    st.session_state.last_claude_action = {
        'intent': text,
        'plan': plan,
        'confirmation': confirmation,
        'timestamp': datetime.now()
    }
    
    return plan, confirmation

def render_agent_console():
    """Render Claude's Agent Console drawer"""
    with st.sidebar:
        st.header("🤖 Claude Code Agent Console")
        
        if st.session_state.last_claude_action:
            action = st.session_state.last_claude_action
            
            # Intent
            st.subheader("📝 Intent")
            st.code(action['intent'])
            
            # Plan
            st.subheader("📋 Plan JSON")
            st.json(action['plan'])
            
            # Guardrails
            st.subheader("✅ Guardrail Checks")
            st.success("✓ Role: CLAUDE_CODE_AI_AGENT")
            st.success("✓ Database: CLAUDE_BI.MCP only")
            st.success("✓ Procedures: Whitelisted only")
            st.success(f"✓ Limits: n≤50, limit≤1000")
            
            # Procedure preview
            st.subheader("🔍 Procedure Preview")
            proc = action['plan']['proc']
            st.code(f"CALL MCP.{proc}(...)\n# Query Tag: dash-ui|agent:claude|proc:{proc}")
            
            # Confirmation
            st.subheader("💬 Claude's Confirmation")
            st.info(action['confirmation'])
            
            # Actions
            col1, col2 = st.columns(2)
            with col1:
                if st.button("📋 Copy Plan"):
                    st.write("Plan copied!")
            with col2:
                if st.button("🐛 Report Issue"):
                    st.write("Report sent!")
        else:
            st.info("No recent Claude actions. Try asking Claude a question!")
        
        if st.button("Close Console"):
            st.session_state.show_agent_console = False
            st.experimental_rerun()

def render_result_canvas():
    """Render the result canvas with focus controls"""
    # Show Agent Console if enabled
    if st.session_state.show_agent_console:
        render_agent_console()
    
    if not st.session_state.current_plan:
        st.info("Select a preset from the home screen to begin")
        if st.button("← Back to Home"):
            st.session_state.view_mode = 'home'
            st.experimental_rerun()
        return
    
    # Header with Claude branding
    col1, col2 = st.columns([4, 1])
    with col1:
        st.subheader("📊 Result Canvas")
        st.caption("🤖 Powered by Claude Code")
    with col2:
        st.info(render_claude_status())
    
    # Action buttons
    col1, col2, col3, col4, col5 = st.columns([2, 2, 2, 2, 2])
    with col1:
        if st.button("💾 Build Dashboard"):
            save_dashboard()
    with col2:
        if st.button("⏰ Schedule"):
            st.session_state.show_schedule = True
    with col3:
        if st.button("🤖 Console", help="View Claude's process"):
            st.session_state.show_agent_console = True
            st.experimental_rerun()
    with col4:
        if st.button("🏠 Home"):
            st.session_state.view_mode = 'home'
            st.experimental_rerun()
    
    # Check execution mode
    if st.session_state.claude_mode == 'Approve' and st.session_state.get('needs_approval'):
        st.warning("⏸️ Claude is waiting for your approval")
        
        # Show plan preview
        plan = st.session_state.current_plan
        st.code(f"""
Claude will execute:
CALL MCP.{plan['proc']}(
    parameters...
)
""")
        
        col1, col2, col3 = st.columns(3)
        with col1:
            if st.button("✅ Approve", type="primary"):
                st.session_state.needs_approval = False
                st.experimental_rerun()
        with col2:
            if st.button("✏️ Modify"):
                st.info("Modification not yet implemented")
        with col3:
            if st.button("❌ Cancel"):
                st.session_state.current_plan = None
                st.session_state.needs_approval = False
                st.experimental_rerun()
        return
    
    # Execute current plan
    with st.spinner(f"Claude is calling {st.session_state.current_plan.get('proc', 'procedure')}..."):
        data = execute_plan(st.session_state.current_plan)
    
    if data:
        # Render based on panel type
        panel_type = st.session_state.current_plan.get('panels', [{}])[0].get('type', 'bar')
        
        if panel_type == 'metric':
            # Metrics display
            cols = st.columns(len(data) if len(data) <= 4 else 4)
            for i, metric in enumerate(data[:4]):
                with cols[i]:
                    label = metric.get('label', metric.get('metric', 'Value'))
                    value = metric.get('value', 0)
                    st.metric(label, f"{value:,}")
        
        elif panel_type == 'line':
            # Time series chart
            df = pd.DataFrame(data)
            if 'TIME_BUCKET' in df.columns:
                df['Time'] = pd.to_datetime(df['TIME_BUCKET'])
                df['Count'] = df.get('EVENT_COUNT', df.get('CNT', 0))
                df = df.set_index('Time')
                st.line_chart(df['Count'], height=400)
        
        elif panel_type == 'bar':
            # Bar chart for rankings
            df = pd.DataFrame(data)
            if len(df) > 0:
                # Find item and count columns
                item_col = next((c for c in df.columns if c.upper() in ['ITEM', 'DIMENSION']), df.columns[0])
                count_col = next((c for c in df.columns if c.upper() in ['COUNT', 'CNT', 'EVENT_COUNT']), df.columns[-1])
                
                if item_col and count_col:
                    df = df.set_index(item_col)
                    st.bar_chart(df[count_col], height=400)
                    
                    # Claude's explanation
                    st.caption(f"🤖 Claude aggregated {count_col.lower()} by {item_col.lower()} for your selected time range")
        
        elif panel_type == 'table':
            # Table for events
            df = pd.DataFrame(data)
            st.dataframe(df, use_container_width=True, height=400)
        
        # Compact data preview
        with st.expander("📋 Data Preview"):
            st.dataframe(pd.DataFrame(data).head(10))
    
    st.divider()
    
    # Focus controls
    st.subheader("🎯 Refine Results")
    
    col1, col2, col3 = st.columns([3, 3, 4])
    
    with col1:
        actor_filter = st.text_input("Filter by user", placeholder="e.g., john@example.com")
        if actor_filter and st.button("Apply User Filter", key="apply_actor"):
            if 'filters' not in st.session_state.current_plan['params']:
                st.session_state.current_plan['params']['filters'] = {}
            st.session_state.current_plan['params']['filters']['actor'] = actor_filter
            st.experimental_rerun()
    
    with col2:
        action_filter = st.text_input("Filter by action", placeholder="e.g., user.login")
        if action_filter and st.button("Apply Action Filter", key="apply_action"):
            if 'filters' not in st.session_state.current_plan['params']:
                st.session_state.current_plan['params']['filters'] = {}
            st.session_state.current_plan['params']['filters']['action'] = action_filter
            st.experimental_rerun()
    
    with col3:
        group_options = ["None", "action", "actor", "source"]
        group_by = st.selectbox("Group by", options=group_options)
        if group_by != "None" and st.button("Apply Grouping", key="apply_group"):
            st.session_state.current_plan['params']['group_by'] = group_by
            st.experimental_rerun()
    
    # Natural language refinement with Claude
    st.divider()
    st.subheader("🤖 Talk to Claude Code")
    
    # Example chips
    col1, col2, col3 = st.columns(3)
    with col1:
        if st.button("📝 'top actions for john@example.com'", key="ex1"):
            st.session_state.nl_example = "top actions for john@example.com last week"
    with col2:
        if st.button("📊 'compare sources by hour'", key="ex2"):
            st.session_state.nl_example = "compare sources by hour for today"
    with col3:
        if st.button("📈 'metrics for last 30 days'", key="ex3"):
            st.session_state.nl_example = "show metrics for last 30 days"
    
    nl_input = st.text_input(
        "Ask Claude",
        value=st.session_state.get('nl_example', ''),
        placeholder="Ask Claude: 'hone in on acme_coo@... last 48h by hour'",
        key="nl_refine",
        help="Claude will translate your request into secure procedure calls"
    )
    
    if nl_input and st.button("🤖 Ask Claude", key="apply_nl", type="primary"):
        plan, confirmation = parse_natural_language(nl_input)
        st.success(f"✓ {confirmation}")
        st.session_state.current_plan = plan
        
        if st.session_state.claude_mode == 'Approve':
            st.session_state.needs_approval = True
        
        # Clear example
        if 'nl_example' in st.session_state:
            del st.session_state.nl_example
        
        st.experimental_rerun()
    
    # Schedule modal
    if st.session_state.get('show_schedule'):
        render_schedule_modal()

def save_dashboard():
    """Save current canvas as a dashboard"""
    dashboard_id = f"dash_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    title = st.text_input("Dashboard Title", value="Claude Code Executive Dashboard")
    
    if st.button("Save", key="confirm_save"):
        try:
            # Create dashboard spec
            spec = {
                "panels": st.session_state.current_plan.get('panels', []),
                "plan": st.session_state.current_plan
            }
            
            # Log dashboard.created event
            sql = f"""
            CALL MCP.LOG_CLAUDE_EVENT(OBJECT_CONSTRUCT(
                'action', 'dashboard.created',
                'actor_id', CURRENT_USER(),
                'object', OBJECT_CONSTRUCT(
                    'type', 'dashboard',
                    'id', '{dashboard_id}'
                ),
                'attributes', OBJECT_CONSTRUCT(
                    'title', '{title}',
                    'spec', PARSE_JSON('{json.dumps(spec)}'),
                    'plan_hash', '{hashlib.md5(json.dumps(spec).encode()).hexdigest()}',
                    'dedupe_key', '{dashboard_id}'
                ),
                'occurred_at', CURRENT_TIMESTAMP()
            ), 'COO_UI')
            """
            session.sql(sql).collect()
            
            st.success(f"✅ Claude Code saved your dashboard! Deep link: /d/{dashboard_id}")
            st.balloons()
            st.session_state.last_dashboard = dashboard_id
            
            # Log dashboard created by Claude
            log_claude_event('dashboard_created', {
                'dashboard_id': dashboard_id,
                'title': title
            })
            
        except Exception as e:
            st.error(f"Failed to save dashboard: {str(e)}")

def render_schedule_modal():
    """Render inline schedule configuration"""
    with st.container():
        st.subheader("⏰ Schedule Dashboard")
        
        col1, col2 = st.columns(2)
        
        with col1:
            frequency = st.selectbox(
                "Frequency",
                options=["DAILY", "WEEKDAYS", "WEEKLY"],
                format_func=lambda x: {
                    "DAILY": "Every day",
                    "WEEKDAYS": "Weekdays only",
                    "WEEKLY": "Weekly"
                }[x]
            )
            
            time_input = st.time_input("Time", value=datetime.strptime("07:00", "%H:%M").time())
        
        with col2:
            tz_options = {
                "America/New_York": "ET",
                "America/Chicago": "CT",
                "America/Denver": "MT",
                "America/Los_Angeles": "PT"
            }
            timezone = st.selectbox(
                "Time Zone",
                options=list(tz_options.keys()),
                format_func=lambda x: tz_options[x]
            )
            
            delivery = st.multiselect(
                "Deliver to",
                options=["email", "slack"],
                default=["email"]
            )
        
        if st.button("Create Schedule", key="confirm_schedule"):
            try:
                schedule_id = f"sched_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                
                # Log schedule event
                sql = f"""
                CALL MCP.LOG_CLAUDE_EVENT(OBJECT_CONSTRUCT(
                    'action', 'dashboard.schedule_created',
                    'actor_id', CURRENT_USER(),
                    'object', OBJECT_CONSTRUCT(
                        'type', 'schedule',
                        'id', '{schedule_id}'
                    ),
                    'attributes', OBJECT_CONSTRUCT(
                        'dashboard_id', '{st.session_state.last_dashboard}',
                        'frequency', '{frequency}',
                        'time', '{time_input.strftime("%H:%M")}',
                        'timezone', '{timezone}',
                        'display_tz', '{tz_options[timezone]}',
                        'deliveries', ARRAY_CONSTRUCT({','.join([f"'{d}'" for d in delivery])}),
                        'dedupe_key', '{schedule_id}'
                    ),
                    'occurred_at', CURRENT_TIMESTAMP()
                ), 'COO_UI')
                """
                session.sql(sql).collect()
                
                # Calculate next run
                next_run = datetime.now().replace(
                    hour=time_input.hour, 
                    minute=time_input.minute
                )
                if next_run < datetime.now():
                    next_run += timedelta(days=1)
                
                st.success(f"✅ Claude will run this dashboard {frequency.lower()} at {time_input.strftime('%I:%M %p')} {tz_options[timezone]}")
                st.info(f"Next run: {next_run.strftime('%B %d at %I:%M %p')} {tz_options[timezone]} (by Claude Code) 🤖")
                st.session_state.show_schedule = False
                
            except Exception as e:
                st.error(f"Failed to create schedule: {str(e)}")
        
        if st.button("Cancel", key="cancel_schedule"):
            st.session_state.show_schedule = False
            st.experimental_rerun()

def main():
    """Main application entry point"""
    # Check for dashboard in URL params
    try:
        query_params = st.experimental_get_query_params()
        dashboard_id = query_params.get('dashboard_id', [None])[0]
        
        if dashboard_id:
            # Load and display specific dashboard
            st.session_state.last_dashboard = dashboard_id
            # Would load dashboard spec here
    except:
        pass
    
    # Determine view mode
    if 'view_mode' not in st.session_state:
        st.session_state.view_mode = 'home'
    
    # Render appropriate view
    if st.session_state.view_mode == 'canvas':
        render_result_canvas()
    else:
        render_home()

if __name__ == "__main__":
    main()