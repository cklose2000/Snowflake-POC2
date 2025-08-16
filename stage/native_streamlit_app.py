"""
🤖 Claude Code Executive Dashboard - All-Snowflake Native Version

This Streamlit app runs entirely within Snowflake using native procedures.
No external servers required - pure Snowflake architecture.

Architecture:
- UI: Streamlit-in-Snowflake
- NL Processing: COMPILE_NL_PLAN procedure (Claude API via External Access)
- Query Execution: RUN_PLAN procedure (calls whitelisted MCP procedures)
- Dashboard Management: SAVE_DASHBOARD_SPEC, CREATE_DASHBOARD_SCHEDULE
- Automation: Serverless TASK_RUN_SCHEDULES
- Storage: Named stages (@MCP.DASH_SPECS, @MCP.DASH_SNAPSHOTS)
- Audit: All events logged to ACTIVITY.EVENTS (Two-Table Law compliant)
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import json
import datetime as dt
from datetime import timezone, timedelta

# Configure Streamlit page
st.set_page_config(
    page_title="🤖 Claude Code Executive Dashboard - Native",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Snowflake session
if 'snowflake' not in st.session_state:
    from snowflake.snowpark.context import get_active_session
    st.session_state.snowflake = get_active_session()

session = st.session_state.snowflake

# Ensure we're using the correct role and database
try:
    session.sql("USE ROLE R_CLAUDE_AGENT").collect()
    session.sql("USE DATABASE CLAUDE_BI").collect()
    session.sql("USE SCHEMA MCP").collect()
except Exception as e:
    st.error(f"⚠️ Database connection issue: {e}")
    st.stop()

# ===================================================================
# HELPER FUNCTIONS
# ===================================================================

def call_procedure(proc_name, params=None):
    """Call a Snowflake procedure safely with error handling"""
    try:
        if params:
            params_json = json.dumps(params)
            df = session.sql(f"CALL MCP.{proc_name}(PARSE_JSON(?))", params=[params_json])
        else:
            df = session.sql(f"CALL MCP.{proc_name}()")
        
        rows = df.collect()
        if rows:
            # Return the first column of first row (procedure result)
            result = rows[0][0]
            if isinstance(result, str):
                return json.loads(result)
            return result
        return {"ok": False, "error": "No result returned"}
    except Exception as e:
        return {"ok": False, "error": str(e)}

def format_timestamp(ts_str):
    """Format timestamp for display"""
    try:
        if isinstance(ts_str, str):
            ts = dt.datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
        else:
            ts = ts_str
        return ts.strftime("%Y-%m-%d %H:%M UTC")
    except:
        return str(ts_str)

def create_chart(chart_type, data, title="Chart"):
    """Create Plotly chart from procedure data"""
    try:
        if not data or not isinstance(data, list):
            return None
        
        # Convert to DataFrame (assuming first row has column names)
        if len(data) > 1:
            columns = [f"col_{i}" for i in range(len(data[0]))]
            df = pd.DataFrame(data[1:], columns=columns)
        else:
            return None
        
        if chart_type == "metrics":
            # Create metric cards
            return df
        elif chart_type == "series" and len(df.columns) >= 2:
            # Time series chart
            fig = px.line(df, x=df.columns[0], y=df.columns[1], title=title)
            return fig
        elif chart_type == "topn" and len(df.columns) >= 2:
            # Bar chart
            fig = px.bar(df, x=df.columns[0], y=df.columns[1], title=title)
            return fig
        elif chart_type == "events":
            # Table view
            return df
        
    except Exception as e:
        st.error(f"Chart creation error: {e}")
        return None

# ===================================================================
# CLAUDE CODE STATUS COMPONENT
# ===================================================================

def show_claude_status():
    """Display Claude Code status and agent console"""
    
    # Claude Code header with status
    col1, col2, col3 = st.columns([2, 1, 1])
    
    with col1:
        st.markdown("# 🤖 Claude Code Executive Dashboard")
        st.markdown("**All-Snowflake Native Architecture** • Zero External Dependencies")
    
    with col2:
        # Claude mode toggle
        claude_mode = st.selectbox(
            "Claude Mode",
            ["Auto", "Approve"],
            help="Auto: Claude executes immediately, Approve: Show plan first"
        )
        st.session_state.claude_mode = claude_mode
    
    with col3:
        # System status
        status_color = "🟢" if session else "🔴"
        st.markdown(f"**Status:** {status_color} Connected")
        st.markdown("**Engine:** Snowpark Native")

# ===================================================================
# NATURAL LANGUAGE INTERFACE
# ===================================================================

def natural_language_interface():
    """Claude Code natural language query interface"""
    
    st.markdown("## 💬 Talk to Claude Code")
    
    # Example queries
    with st.expander("💡 Example queries to try"):
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("""
            **Time Series:**
            - "Show activity by hour for last 24 hours"
            - "Events per day for the past week"
            - "Trend analysis for last 48 hours"
            """)
        
        with col2:
            st.markdown("""
            **Rankings & Analysis:**
            - "Top 10 actions today"
            - "Most active users this week"
            - "Compare sources by activity"
            """)
    
    # Input interface
    col1, col2 = st.columns([4, 1])
    
    with col1:
        user_query = st.text_input(
            "What would you like Claude to analyze?",
            placeholder="Ask Claude: 'show me top 10 actions from last 24 hours'",
            key="nl_query"
        )
    
    with col2:
        execute_button = st.button("Ask Claude", type="primary")
    
    if execute_button and user_query:
        with st.spinner("🤖 Claude is analyzing your request..."):
            
            # Step 1: Compile natural language to plan
            st.write("**🧠 Claude is thinking...**")
            intent = {"text": user_query}
            
            compile_result = call_procedure("COMPILE_NL_PLAN", intent)
            
            if not compile_result.get("ok"):
                st.error(f"❌ Claude couldn't understand: {compile_result.get('error')}")
                return
            
            plan = compile_result.get("plan", {})
            source = compile_result.get("source", "unknown")
            
            # Show Claude's plan
            st.write(f"**📋 Claude's Plan** ({'Claude API' if source == 'claude_api' else 'Fallback'})")
            
            plan_col1, plan_col2 = st.columns(2)
            with plan_col1:
                st.json({
                    "procedure": plan.get("proc"),
                    "parameters": plan.get("params", {})
                })
            
            with plan_col2:
                st.markdown(f"""
                **Guardrails Applied:**
                - ✅ Procedure: `{plan.get("proc")}` (whitelisted)
                - ✅ Database: CLAUDE_BI.MCP only  
                - ✅ Role: R_CLAUDE_AGENT
                - ✅ Limits: Clamped to safe values
                """)
            
            # Approval flow
            if st.session_state.get("claude_mode") == "Approve":
                st.write("**⏸️ Waiting for your approval...**")
                
                col1, col2 = st.columns(2)
                with col1:
                    if st.button("✅ Execute Plan", type="primary"):
                        execute_plan = True
                    else:
                        execute_plan = False
                with col2:
                    if st.button("❌ Cancel"):
                        st.warning("Plan cancelled by user")
                        return
                
                if not execute_plan:
                    st.info("👆 Click 'Execute Plan' to proceed")
                    return
            
            # Step 2: Execute the plan
            st.write("**⚙️ Claude is executing the plan...**")
            
            execute_result = call_procedure("RUN_PLAN", plan)
            
            if not execute_result.get("ok"):
                st.error(f"❌ Execution failed: {execute_result.get('error')}")
                return
            
            # Step 3: Display results
            st.write("**📊 Claude's Results**")
            
            rows = execute_result.get("rows", [])
            row_count = execute_result.get("row_count", 0)
            procedure = execute_result.get("procedure")
            
            if row_count == 0:
                st.warning("No data found for your query")
                return
            
            # Parse the procedure result
            if rows and len(rows) > 0:
                try:
                    # The procedure returns JSON results
                    proc_result = json.loads(rows[0][0]) if isinstance(rows[0][0], str) else rows[0][0]
                    
                    if proc_result.get("ok"):
                        data = proc_result.get("data", [])
                        metadata = proc_result.get("metadata", {})
                        
                        # Create appropriate visualization
                        if procedure == "DASH_GET_SERIES":
                            # Time series chart
                            if isinstance(data, list) and data:
                                chart_data = []
                                for item in data:
                                    chart_data.append([
                                        item.get("time_bucket", ""),
                                        item.get("event_count", 0),
                                        item.get("unique_actors", 0)
                                    ])
                                
                                df = pd.DataFrame(chart_data, columns=["Time", "Events", "Unique Users"])
                                fig = px.line(df, x="Time", y="Events", title="Activity Over Time")
                                st.plotly_chart(fig, use_container_width=True)
                                
                                # Show summary
                                total_events = df["Events"].sum()
                                st.metric("Total Events", total_events)
                        
                        elif procedure == "DASH_GET_TOPN":
                            # Top N chart
                            if isinstance(data, list) and data:
                                chart_data = []
                                for item in data:
                                    chart_data.append([
                                        item.get("dimension", ""),
                                        item.get("count", 0)
                                    ])
                                
                                df = pd.DataFrame(chart_data, columns=["Item", "Count"])
                                fig = px.bar(df, x="Item", y="Count", title="Top Items")
                                st.plotly_chart(fig, use_container_width=True)
                        
                        elif procedure == "DASH_GET_METRICS":
                            # Metrics display
                            if isinstance(data, dict):
                                col1, col2, col3, col4 = st.columns(4)
                                
                                with col1:
                                    st.metric("Total Events", data.get("total_events", 0))
                                with col2:
                                    st.metric("Unique Users", data.get("unique_actors", 0))
                                with col3:
                                    st.metric("Unique Actions", data.get("unique_actions", 0))
                                with col4:
                                    st.metric("Events/Hour", f"{data.get('avg_events_per_hour', 0):.1f}")
                        
                        elif procedure == "DASH_GET_EVENTS":
                            # Events table
                            if isinstance(data, list) and data:
                                df = pd.DataFrame(data)
                                st.dataframe(df, use_container_width=True)
                        
                        # Show Claude's explanation
                        st.info(f"🤖 Claude executed `{procedure}` and found {len(data) if isinstance(data, list) else 'metrics'} results")
                    
                    else:
                        st.error(f"Procedure error: {proc_result.get('error', 'Unknown error')}")
                
                except Exception as parse_error:
                    st.error(f"Result parsing error: {parse_error}")
                    st.json(rows[0] if rows else "No data")

# ===================================================================
# DASHBOARD MANAGEMENT
# ===================================================================

def dashboard_management():
    """Dashboard creation and management interface"""
    
    st.markdown("## 📊 Dashboard Management")
    
    # Create new dashboard
    with st.expander("➕ Create New Dashboard"):
        
        st.markdown("### Dashboard Specification")
        
        col1, col2 = st.columns(2)
        
        with col1:
            title = st.text_input("Dashboard Title", "Executive Overview")
            description = st.text_area("Description", "Executive dashboard with key metrics")
            refresh_interval = st.selectbox("Refresh Interval", [300, 600, 1800, 3600], format_func=lambda x: f"{x//60} minutes")
        
        with col2:
            st.markdown("**Available Panel Types:**")
            st.markdown("- **Metrics**: KPIs and summary statistics")
            st.markdown("- **Series**: Time series charts") 
            st.markdown("- **TopN**: Rankings and leaderboards")
            st.markdown("- **Events**: Live event streams")
        
        # Panel configuration
        st.markdown("### Dashboard Panels")
        
        if "panels" not in st.session_state:
            st.session_state.panels = [
                {"type": "metrics", "title": "Key Metrics"},
                {"type": "series", "title": "Activity Trends"}
            ]
        
        for i, panel in enumerate(st.session_state.panels):
            col1, col2, col3, col4 = st.columns([2, 2, 2, 1])
            
            with col1:
                panel_type = st.selectbox(f"Panel {i+1} Type", 
                                        ["metrics", "series", "topn", "events"], 
                                        index=["metrics", "series", "topn", "events"].index(panel["type"]),
                                        key=f"panel_type_{i}")
            
            with col2:
                panel_title = st.text_input(f"Panel {i+1} Title", panel["title"], key=f"panel_title_{i}")
            
            with col3:
                if panel_type == "series":
                    interval = st.selectbox("Interval", ["hour", "day"], key=f"interval_{i}")
                elif panel_type == "topn":
                    dimension = st.selectbox("Dimension", ["action", "actor_id", "source"], key=f"dimension_{i}")
            
            with col4:
                if st.button("🗑️", key=f"remove_{i}", help="Remove panel"):
                    st.session_state.panels.pop(i)
                    st.rerun()
            
            # Update panel in session state
            st.session_state.panels[i] = {
                "type": panel_type,
                "title": panel_title,
                "params": {}
            }
        
        # Add panel button
        if st.button("➕ Add Panel"):
            st.session_state.panels.append({"type": "metrics", "title": f"Panel {len(st.session_state.panels) + 1}"})
            st.rerun()
        
        # Create dashboard button
        if st.button("🚀 Create Dashboard", type="primary"):
            if title and st.session_state.panels:
                
                dashboard_spec = {
                    "title": title,
                    "description": description,
                    "panels": st.session_state.panels,
                    "refresh_interval_sec": refresh_interval
                }
                
                with st.spinner("🤖 Claude is creating your dashboard..."):
                    result = call_procedure("SAVE_DASHBOARD_SPEC", dashboard_spec)
                    
                    if result.get("ok"):
                        st.success(f"✅ Dashboard created! ID: {result.get('dashboard_id')}")
                        st.json(result)
                    else:
                        st.error(f"❌ Creation failed: {result.get('error')}")
            else:
                st.warning("Please provide a title and at least one panel")
    
    # List existing dashboards
    st.markdown("### 📋 Existing Dashboards")
    
    with st.spinner("Loading dashboards..."):
        list_result = call_procedure("LIST_DASHBOARDS")
        
        if list_result.get("ok"):
            dashboards = list_result.get("dashboards", [])
            
            if dashboards:
                for dashboard in dashboards:
                    with st.container():
                        col1, col2, col3, col4 = st.columns([3, 1, 1, 1])
                        
                        with col1:
                            st.markdown(f"**{dashboard.get('title', 'Untitled')}**")
                            st.caption(f"ID: {dashboard.get('dashboard_id')} • {dashboard.get('panel_count', 0)} panels")
                        
                        with col2:
                            st.caption(format_timestamp(dashboard.get('created_at')))
                        
                        with col3:
                            if st.button("📅 Schedule", key=f"schedule_{dashboard.get('dashboard_id')}"):
                                st.session_state.schedule_dashboard = dashboard.get('dashboard_id')
                        
                        with col4:
                            if st.button("🔍 View", key=f"view_{dashboard.get('dashboard_id')}"):
                                st.session_state.view_dashboard = dashboard.get('dashboard_id')
            else:
                st.info("No dashboards created yet. Create your first dashboard above!")
        else:
            st.error(f"Failed to load dashboards: {list_result.get('error')}")

# ===================================================================
# SCHEDULING INTERFACE
# ===================================================================

def scheduling_interface():
    """Dashboard scheduling and automation"""
    
    if "schedule_dashboard" in st.session_state:
        dashboard_id = st.session_state.schedule_dashboard
        
        st.markdown(f"## ⏰ Schedule Dashboard: {dashboard_id}")
        
        col1, col2 = st.columns(2)
        
        with col1:
            frequency = st.selectbox("Frequency", ["DAILY", "WEEKDAYS", "WEEKLY"])
            time_str = st.time_input("Time", dt.time(9, 0))
            timezone = st.selectbox("Timezone", ["UTC", "America/New_York", "America/Chicago", "America/Los_Angeles"])
        
        with col2:
            st.markdown("**Delivery Methods:**")
            email_delivery = st.checkbox("Email", value=True)
            slack_delivery = st.checkbox("Slack")
            
            if email_delivery or slack_delivery:
                recipients = st.text_area("Recipients", "exec@company.com\nteam@company.com")
            else:
                recipients = ""
        
        if st.button("📅 Create Schedule"):
            schedule_spec = {
                "dashboard_id": dashboard_id,
                "frequency": frequency,
                "time": time_str.strftime("%H:%M"),
                "timezone": timezone,
                "deliveries": [d for d in (["email"] if email_delivery else []) + (["slack"] if slack_delivery else [])],
                "recipients": [r.strip() for r in recipients.split('\n') if r.strip()],
                "enabled": True
            }
            
            with st.spinner("Creating schedule..."):
                result = call_procedure("CREATE_DASHBOARD_SCHEDULE", schedule_spec)
                
                if result.get("ok"):
                    st.success(f"✅ Schedule created! ID: {result.get('schedule_id')}")
                    del st.session_state.schedule_dashboard
                    st.rerun()
                else:
                    st.error(f"❌ Failed to create schedule: {result.get('error')}")
        
        if st.button("Cancel"):
            del st.session_state.schedule_dashboard
            st.rerun()

# ===================================================================
# SYSTEM MONITORING
# ===================================================================

def system_monitoring():
    """System health and monitoring dashboard"""
    
    st.markdown("## 🔍 System Monitoring")
    
    # Two-Table Law validation
    st.markdown("### 🏛️ Two-Table Law Compliance")
    
    try:
        table_check = session.sql("""
            SELECT COUNT(*) as table_count
            FROM CLAUDE_BI.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_CATALOG = 'CLAUDE_BI'
              AND TABLE_SCHEMA IN ('LANDING', 'ACTIVITY')
              AND TABLE_TYPE IN ('BASE TABLE', 'DYNAMIC TABLE')
        """).collect()
        
        table_count = table_check[0]["TABLE_COUNT"]
        
        if table_count == 2:
            st.success(f"✅ Two-Table Law: Exactly {table_count} tables (COMPLIANT)")
        else:
            st.error(f"❌ Two-Table Law: Found {table_count} tables (VIOLATION)")
    
    except Exception as e:
        st.error(f"Error checking tables: {e}")
    
    # Recent activity
    st.markdown("### 📊 Recent Claude Code Activity")
    
    try:
        recent_activity = session.sql("""
            SELECT 
                action,
                actor_id,
                source,
                occurred_at,
                attributes:status::string as status
            FROM ACTIVITY.EVENTS
            WHERE source = 'CLAUDE_CODE' OR actor_id = 'CLAUDE_CODE_AI_AGENT'
            ORDER BY occurred_at DESC
            LIMIT 10
        """).collect()
        
        if recent_activity:
            activity_data = []
            for row in recent_activity:
                activity_data.append({
                    "Action": row["ACTION"],
                    "Actor": row["ACTOR_ID"],
                    "Source": row["SOURCE"],
                    "Status": row["STATUS"] or "success",
                    "Time": format_timestamp(row["OCCURRED_AT"])
                })
            
            st.dataframe(pd.DataFrame(activity_data), use_container_width=True)
        else:
            st.info("No recent Claude Code activity")
    
    except Exception as e:
        st.error(f"Error loading activity: {e}")
    
    # Task execution status
    st.markdown("### ⚙️ Serverless Task Status")
    
    try:
        task_status = session.sql("""
            SELECT 
                name,
                state,
                schedule,
                warehouse,
                last_committed_on
            FROM INFORMATION_SCHEMA.TASKS
            WHERE name = 'TASK_RUN_SCHEDULES'
        """).collect()
        
        if task_status:
            task = task_status[0]
            status_color = "🟢" if task["STATE"] == "started" else "🔴"
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Task Status", f"{status_color} {task['STATE']}")
            with col2:
                st.metric("Schedule", task["SCHEDULE"])
            with col3:
                st.metric("Warehouse", task["WAREHOUSE"])
        else:
            st.warning("Task not found")
    
    except Exception as e:
        st.error(f"Error checking task: {e}")

# ===================================================================
# MAIN APPLICATION
# ===================================================================

def main():
    """Main Streamlit application"""
    
    # Show Claude Code status
    show_claude_status()
    
    # Sidebar navigation
    st.sidebar.markdown("## 🤖 Claude Code Dashboard")
    st.sidebar.markdown("**All-Snowflake Native Architecture**")
    
    page = st.sidebar.selectbox(
        "Navigation",
        ["💬 Natural Language", "📊 Dashboard Management", "⏰ Scheduling", "🔍 System Monitoring"]
    )
    
    # Handle scheduling workflow
    if "schedule_dashboard" in st.session_state:
        scheduling_interface()
        return
    
    # Main content based on page selection
    if page == "💬 Natural Language":
        natural_language_interface()
    
    elif page == "📊 Dashboard Management":
        dashboard_management()
    
    elif page == "⏰ Scheduling":
        st.markdown("## ⏰ Dashboard Scheduling")
        st.info("Select a dashboard from the 'Dashboard Management' page to create a schedule.")
    
    elif page == "🔍 System Monitoring":
        system_monitoring()
    
    # Footer
    st.sidebar.markdown("---")
    st.sidebar.markdown("**🤖 Powered by Claude Code**")
    st.sidebar.markdown("Zero external dependencies • Pure Snowflake")
    st.sidebar.markdown(f"Connected to: `{session.get_current_database()}.{session.get_current_schema()}`")

if __name__ == "__main__":
    main()