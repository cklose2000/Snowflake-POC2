"""
Snowflake-native Streamlit Dashboard App
Universal template that reads dashboard specs from EVENTS table
Auto-refreshes every 5 minutes for mobile viewing
"""

import streamlit as st
from snowflake.snowpark.context import get_active_session
import json
from datetime import datetime, timedelta
import time
import pandas as pd

# Get Snowpark session
session = get_active_session()

# Configure page
st.set_page_config(
    page_title="Executive Dashboard", 
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Auto-refresh configuration
REFRESH_INTERVAL_SECONDS = 300  # 5 minutes

def get_dashboard_spec(dashboard_id):
    """Load dashboard specification from VW_DASHBOARDS view"""
    query = f"""
    SELECT 
        dashboard_id,
        title,
        spec,
        refresh_interval_sec,
        created_at,
        created_by
    FROM MCP.VW_DASHBOARDS
    WHERE dashboard_id = '{dashboard_id}'
    LIMIT 1
    """
    
    df = session.sql(query).to_pandas()
    
    if df.empty:
        return None
    
    row = df.iloc[0]
    return {
        'dashboard_id': row['DASHBOARD_ID'],
        'title': row['TITLE'],
        'spec': json.loads(row['SPEC']) if isinstance(row['SPEC'], str) else row['SPEC'],
        'refresh_interval_sec': row['REFRESH_INTERVAL_SEC'],
        'created_at': row['CREATED_AT'],
        'created_by': row['CREATED_BY']
    }

def execute_dashboard_proc(proc_name, params):
    """Execute dashboard procedure and return results"""
    try:
        # Build procedure call
        param_str = json.dumps(params) if params else 'NULL'
        
        if proc_name == 'DASH_GET_SERIES':
            sql = f"""
            CALL MCP.DASH_GET_SERIES(
                {params.get('start_ts', "DATEADD('hour', -24, CURRENT_TIMESTAMP())")},
                {params.get('end_ts', 'CURRENT_TIMESTAMP()')},
                '{params.get('interval_str', 'hour')}',
                {'PARSE_JSON(' + repr(json.dumps(params.get('filters'))) + ')' if params.get('filters') else 'NULL'},
                {repr(params.get('group_by')) if params.get('group_by') else 'NULL'}
            )
            """
        elif proc_name == 'DASH_GET_TOPN':
            sql = f"""
            CALL MCP.DASH_GET_TOPN(
                {params.get('start_ts', "DATEADD('hour', -24, CURRENT_TIMESTAMP())")},
                {params.get('end_ts', 'CURRENT_TIMESTAMP()')},
                '{params.get('dimension', 'action')}',
                {'PARSE_JSON(' + repr(json.dumps(params.get('filters'))) + ')' if params.get('filters') else 'NULL'},
                {params.get('n', 10)}
            )
            """
        elif proc_name == 'DASH_GET_EVENTS':
            sql = f"""
            CALL MCP.DASH_GET_EVENTS(
                {params.get('cursor_ts', "DATEADD('minute', -5, CURRENT_TIMESTAMP())")},
                {params.get('limit_rows', 50)}
            )
            """
        elif proc_name == 'DASH_GET_METRICS':
            sql = f"""
            CALL MCP.DASH_GET_METRICS(
                {params.get('start_ts', "DATEADD('hour', -24, CURRENT_TIMESTAMP())")},
                {params.get('end_ts', 'CURRENT_TIMESTAMP()')},
                {'PARSE_JSON(' + repr(json.dumps(params.get('filters'))) + ')' if params.get('filters') else 'NULL'}
            )
            """
        else:
            st.error(f"Unknown procedure: {proc_name}")
            return None
        
        # Execute and get result
        result_df = session.sql(sql).to_pandas()
        
        # Parse the VARIANT result
        if not result_df.empty:
            result_col = result_df.columns[0]
            result = result_df.iloc[0][result_col]
            
            # Parse JSON if it's a string
            if isinstance(result, str):
                result = json.loads(result)
            
            if result.get('ok'):
                return result.get('data', [])
            else:
                st.error(f"Procedure error: {result.get('error')}")
                return None
        
        return None
        
    except Exception as e:
        st.error(f"Error executing {proc_name}: {str(e)}")
        return None

def render_metric_panel(panel, data):
    """Render a metrics panel"""
    if not data:
        st.info("No metrics data available")
        return
    
    cols = st.columns(len(data))
    for i, metric in enumerate(data):
        with cols[i]:
            label = metric.get('label', metric.get('metric', 'Unknown'))
            value = metric.get('value', 0)
            delta = metric.get('delta')
            
            if delta:
                st.metric(label, value, delta)
            else:
                st.metric(label, value)

def render_series_panel(panel, data):
    """Render a time series chart"""
    if not data:
        st.info("No time series data available")
        return
    
    df = pd.DataFrame(data)
    
    # Rename columns for clarity
    if 'TIME_BUCKET' in df.columns:
        df['Time'] = pd.to_datetime(df['TIME_BUCKET'])
        df['Count'] = df.get('EVENT_COUNT', df.get('CNT', 0))
        
        # Set Time as index for line chart
        df = df.set_index('Time')
        
        # Create line chart using Streamlit
        st.subheader(panel.get('title', 'Time Series'))
        st.line_chart(df['Count'], height=400)
    else:
        st.dataframe(df)

def render_topn_panel(panel, data):
    """Render a top-N ranking chart"""
    if not data:
        st.info("No ranking data available")
        return
    
    df = pd.DataFrame(data)
    
    # Find the item and count columns
    item_col = None
    count_col = None
    
    # Look for ITEM column first (from procedure output)
    if 'ITEM' in df.columns:
        item_col = 'ITEM'
    elif 'item' in df.columns:
        item_col = 'item'
    else:
        # Find first non-count column
        for col in df.columns:
            if col.upper() not in ['CNT', 'COUNT', 'EVENT_COUNT']:
                item_col = col
                break
    
    # Find count column
    if 'COUNT' in df.columns:
        count_col = 'COUNT'
    elif 'count' in df.columns:
        count_col = 'count'
    elif 'CNT' in df.columns:
        count_col = 'CNT'
    else:
        count_col = df.columns[-1]  # Use last column as fallback
    
    if item_col and count_col and item_col in df.columns and count_col in df.columns:
        # Sort by count descending
        df = df.sort_values(by=count_col, ascending=False)
        
        # Set item as index for bar chart
        df = df.set_index(item_col)
        
        # Create bar chart using Streamlit
        st.subheader(panel.get('title', 'Top Items'))
        st.bar_chart(df[count_col], height=400)
    else:
        st.dataframe(df)

def render_events_panel(panel, data):
    """Render an events table"""
    if not data:
        st.info("No events available")
        return
    
    df = pd.DataFrame(data)
    
    # Format timestamp columns
    for col in df.columns:
        if 'TIME' in col.upper() or 'OCCURRED' in col.upper():
            try:
                df[col] = pd.to_datetime(df[col])
            except:
                pass
    
    st.dataframe(df, use_container_width=True, height=400)

def render_panel(panel):
    """Render a dashboard panel based on its type"""
    panel_type = panel.get('type', 'unknown')
    params = panel.get('params', {})
    
    # Map panel type to procedure
    proc_map = {
        'metric': 'DASH_GET_METRICS',
        'metrics': 'DASH_GET_METRICS',
        'series': 'DASH_GET_SERIES',
        'timeseries': 'DASH_GET_SERIES',
        'rank': 'DASH_GET_TOPN',
        'ranking': 'DASH_GET_TOPN',
        'topn': 'DASH_GET_TOPN',
        'events': 'DASH_GET_EVENTS',
        'table': 'DASH_GET_EVENTS',
        'stream': 'DASH_GET_EVENTS'
    }
    
    proc_name = proc_map.get(panel_type.lower())
    
    if not proc_name:
        st.warning(f"Unknown panel type: {panel_type}")
        return
    
    # Execute procedure and get data
    with st.spinner(f"Loading {panel.get('title', panel_type)}..."):
        data = execute_dashboard_proc(proc_name, params)
    
    # Render based on type
    if panel_type.lower() in ['metric', 'metrics']:
        render_metric_panel(panel, data)
    elif panel_type.lower() in ['series', 'timeseries']:
        render_series_panel(panel, data)
    elif panel_type.lower() in ['rank', 'ranking', 'topn']:
        render_topn_panel(panel, data)
    elif panel_type.lower() in ['events', 'table', 'stream']:
        render_events_panel(panel, data)

def main():
    """Main dashboard application"""
    
    # Get dashboard ID from query params
    try:
        query_params = st.experimental_get_query_params()
        dashboard_id = query_params.get('dashboard_id', [None])[0]
    except:
        # Fallback for older Streamlit versions
        dashboard_id = None
    
    if not dashboard_id:
        st.title("📊 Dashboard Selector")
        st.info("No dashboard_id parameter found. Please select a dashboard from the list below:")
        
        # Fetch available dashboards
        try:
            query = """
            SELECT 
                dashboard_id,
                title,
                created_at,
                created_by
            FROM MCP.VW_DASHBOARDS
            ORDER BY created_at DESC
            """
            df = session.sql(query).to_pandas()
            
            if df.empty:
                st.error("No dashboards found in the system")
                st.stop()
            
            # Create a dropdown with dashboard options
            dashboard_options = {}
            for _, row in df.iterrows():
                label = f"{row['TITLE'] or 'Untitled'} ({row['DASHBOARD_ID']}) - Created {row['CREATED_AT']}"
                dashboard_options[label] = row['DASHBOARD_ID']
            
            selected_label = st.selectbox(
                "Select a dashboard:",
                options=list(dashboard_options.keys())
            )
            
            if st.button("Load Dashboard"):
                dashboard_id = dashboard_options[selected_label]
                # Update URL with the selected dashboard_id
                st.experimental_set_query_params(dashboard_id=dashboard_id)
                st.experimental_rerun()
            
            st.divider()
            st.subheader("Available Dashboards:")
            st.dataframe(df)
            st.stop()
            
        except Exception as e:
            st.error(f"Error fetching dashboards: {str(e)}")
            st.stop()
    
    # Load dashboard specification
    dashboard = get_dashboard_spec(dashboard_id)
    
    if not dashboard:
        st.error(f"❌ Dashboard not found: {dashboard_id}")
        st.stop()
    
    # Display header
    st.title(dashboard['title'])
    
    # Dashboard metadata
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.caption(f"📊 Dashboard: {dashboard_id}")
    with col2:
        st.caption(f"🕐 Created: {dashboard['created_at']}")
    with col3:
        st.caption(f"👤 By: {dashboard['created_by']}")
    with col4:
        st.caption(f"🔄 Refresh: {datetime.now().strftime('%H:%M:%S')}")
    
    st.divider()
    
    # Get panels from spec
    spec = dashboard.get('spec', {})
    panels = spec.get('panels', [])
    
    if not panels:
        st.warning("No panels configured for this dashboard")
        st.json(spec)
        st.stop()
    
    # Render panels in grid layout
    for panel in panels:
        # Create container for each panel
        with st.container():
            if panel.get('title'):
                st.subheader(panel['title'])
            
            render_panel(panel)
            
            st.divider()
    
    # Add refresh button and auto-refresh
    col1, col2 = st.columns([1, 5])
    with col1:
        if st.button("🔄 Refresh Now"):
            st.experimental_rerun()
    with col2:
        st.info(f"Auto-refresh every {REFRESH_INTERVAL_SECONDS // 60} minutes")
    
    # Auto-refresh logic
    # Note: In production Streamlit on Snowflake, you might use st.experimental_rerun with a timer
    # For now, we'll add a placeholder that would trigger refresh
    placeholder = st.empty()
    
    # Add a hidden timestamp to track page age
    if 'last_refresh' not in st.session_state:
        st.session_state.last_refresh = datetime.now()
    
    # Check if it's time to refresh
    time_since_refresh = (datetime.now() - st.session_state.last_refresh).total_seconds()
    if time_since_refresh > REFRESH_INTERVAL_SECONDS:
        st.session_state.last_refresh = datetime.now()
        st.experimental_rerun()

# Run the app
if __name__ == "__main__":
    main()