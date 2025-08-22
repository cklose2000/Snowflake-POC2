"""
HappyFox Analytics Dashboard
Simple Streamlit app for self-serve ticket analytics
"""

import streamlit as st
import pandas as pd
from snowflake.snowpark import Session
from datetime import datetime, timedelta

# Page config
st.set_page_config(
    page_title="HappyFox Analytics",
    page_icon="🎫",
    layout="wide"
)

# Title
st.title("🎫 HappyFox Ticket Analytics")
st.markdown("Self-serve analytics dashboard for HappyFox tickets")

# Get Snowpark session
@st.cache_resource
def get_session():
    """Get Snowpark session from Streamlit connection"""
    return Session.builder.configs(st.connection("snowflake")._raw_conn).create()

session = get_session()

# Sidebar filters
st.sidebar.header("Filters")

# Product filter
products_df = session.sql("""
    SELECT DISTINCT product_prefix 
    FROM MCP.VW_HF_TICKETS_LATEST 
    WHERE product_prefix IS NOT NULL 
    ORDER BY product_prefix
""").to_pandas()

selected_products = st.sidebar.multiselect(
    "Select Products",
    options=products_df['PRODUCT_PREFIX'].tolist(),
    default=[]
)

# Status filter
lifecycle_states = st.sidebar.multiselect(
    "Lifecycle State",
    options=['Open', 'Closed', 'Unknown'],
    default=['Open', 'Closed']
)

# Age filter
age_filter = st.sidebar.select_slider(
    "Ticket Age (days)",
    options=[0, 7, 14, 30, 60, 90, 180, 365],
    value=(0, 365)
)

# Build filter query
filter_conditions = ["1=1"]

if selected_products:
    products_str = "', '".join(selected_products)
    filter_conditions.append(f"product_prefix IN ('{products_str}')")

if lifecycle_states:
    states_str = "', '".join(lifecycle_states)
    filter_conditions.append(f"lifecycle_state IN ('{states_str}')")

filter_conditions.append(f"age_days BETWEEN {age_filter[0]} AND {age_filter[1]}")

where_clause = " AND ".join(filter_conditions)

# Main content
tab1, tab2, tab3, tab4, tab5 = st.tabs(["📊 Overview", "📈 Trends", "👥 Agents", "🔍 Search", "📥 Export"])

with tab1:
    st.header("Overview")
    
    # Key metrics
    col1, col2, col3, col4 = st.columns(4)
    
    metrics_query = f"""
    SELECT 
        COUNT(*) as total_tickets,
        SUM(CASE WHEN lifecycle_state = 'Open' THEN 1 ELSE 0 END) as open_tickets,
        SUM(CASE WHEN lifecycle_state = 'Closed' THEN 1 ELSE 0 END) as closed_tickets,
        AVG(age_days) as avg_age
    FROM MCP.VW_HF_TICKETS_EXPORT
    WHERE {where_clause}
    """
    
    metrics = session.sql(metrics_query).to_pandas()
    
    with col1:
        st.metric("Total Tickets", f"{metrics['TOTAL_TICKETS'][0]:,}")
    
    with col2:
        st.metric("Open Tickets", f"{metrics['OPEN_TICKETS'][0]:,}")
    
    with col3:
        st.metric("Closed Tickets", f"{metrics['CLOSED_TICKETS'][0]:,}")
    
    with col4:
        st.metric("Avg Age (days)", f"{metrics['AVG_AGE'][0]:.1f}")
    
    # Product breakdown
    st.subheader("Tickets by Product")
    
    product_query = f"""
    SELECT 
        product_prefix,
        lifecycle_state,
        COUNT(*) as ticket_count
    FROM MCP.VW_HF_TICKETS_EXPORT
    WHERE {where_clause}
    GROUP BY product_prefix, lifecycle_state
    ORDER BY ticket_count DESC
    """
    
    product_df = session.sql(product_query).to_pandas()
    
    if not product_df.empty:
        # Pivot for stacked bar chart
        pivot_df = product_df.pivot(
            index='PRODUCT_PREFIX',
            columns='LIFECYCLE_STATE',
            values='TICKET_COUNT'
        ).fillna(0)
        
        st.bar_chart(pivot_df)
    
    # Age distribution
    st.subheader("Age Distribution")
    
    age_query = f"""
    SELECT 
        age_bucket,
        COUNT(*) as ticket_count
    FROM MCP.VW_HF_TICKETS_EXPORT
    WHERE {where_clause}
    GROUP BY age_bucket
    ORDER BY 
        CASE age_bucket
            WHEN '0-1 days' THEN 1
            WHEN '2-3 days' THEN 2
            WHEN '4-7 days' THEN 3
            WHEN '8-14 days' THEN 4
            WHEN '15-30 days' THEN 5
            WHEN '31-60 days' THEN 6
            WHEN '61-90 days' THEN 7
            WHEN '91-180 days' THEN 8
            ELSE 9
        END
    """
    
    age_df = session.sql(age_query).to_pandas()
    
    if not age_df.empty:
        st.bar_chart(age_df.set_index('AGE_BUCKET'))

with tab2:
    st.header("Trends")
    
    # Date range for trends
    trend_days = st.slider("Days to show", 7, 90, 30)
    
    trend_query = f"""
    WITH daily_stats AS (
        SELECT 
            DATE_TRUNC('day', created_at) as day,
            COUNT(*) as created,
            SUM(CASE WHEN lifecycle_state = 'Closed' THEN 1 ELSE 0 END) as closed
        FROM MCP.VW_HF_TICKETS_EXPORT
        WHERE created_at >= DATEADD('day', -{trend_days}, CURRENT_DATE())
            AND {where_clause}
        GROUP BY day
    )
    SELECT 
        day,
        created,
        closed,
        SUM(created - closed) OVER (ORDER BY day) as backlog
    FROM daily_stats
    ORDER BY day
    """
    
    trend_df = session.sql(trend_query).to_pandas()
    
    if not trend_df.empty:
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Daily Created vs Closed")
            chart_df = trend_df[['DAY', 'CREATED', 'CLOSED']].set_index('DAY')
            st.line_chart(chart_df)
        
        with col2:
            st.subheader("Backlog Growth")
            st.line_chart(trend_df[['DAY', 'BACKLOG']].set_index('DAY'))

with tab3:
    st.header("Agent Performance")
    
    agent_query = f"""
    SELECT 
        assignee_name,
        COUNT(*) as total_tickets,
        SUM(CASE WHEN lifecycle_state = 'Closed' THEN 1 ELSE 0 END) as closed_tickets,
        SUM(CASE WHEN lifecycle_state = 'Open' THEN 1 ELSE 0 END) as open_tickets,
        AVG(time_spent_minutes) as avg_time_spent,
        AVG(messages_count) as avg_messages
    FROM MCP.VW_HF_TICKETS_EXPORT
    WHERE assignee_name IS NOT NULL
        AND {where_clause}
    GROUP BY assignee_name
    ORDER BY total_tickets DESC
    LIMIT 20
    """
    
    agent_df = session.sql(agent_query).to_pandas()
    
    if not agent_df.empty:
        # Format columns
        agent_df['Resolution Rate %'] = (agent_df['CLOSED_TICKETS'] / agent_df['TOTAL_TICKETS'] * 100).round(1)
        agent_df['AVG_TIME_SPENT'] = agent_df['AVG_TIME_SPENT'].round(1)
        agent_df['AVG_MESSAGES'] = agent_df['AVG_MESSAGES'].round(1)
        
        # Display table
        st.dataframe(
            agent_df[['ASSIGNEE_NAME', 'TOTAL_TICKETS', 'CLOSED_TICKETS', 
                     'OPEN_TICKETS', 'Resolution Rate %', 'AVG_TIME_SPENT', 'AVG_MESSAGES']],
            use_container_width=True,
            hide_index=True
        )
        
        # Top performers chart
        st.subheader("Top 10 Agents by Volume")
        top_agents = agent_df.head(10)
        st.bar_chart(top_agents[['ASSIGNEE_NAME', 'TOTAL_TICKETS']].set_index('ASSIGNEE_NAME'))

with tab4:
    st.header("Ticket Search")
    
    # Search inputs
    col1, col2 = st.columns(2)
    
    with col1:
        search_term = st.text_input("Search in subject", "")
        ticket_id = st.text_input("Ticket ID (e.g., CM00457430)", "")
    
    with col2:
        assignee_search = st.text_input("Assignee name", "")
        date_range = st.date_input(
            "Created date range",
            value=(datetime.now() - timedelta(days=30), datetime.now()),
            max_value=datetime.now()
        )
    
    if st.button("Search"):
        search_conditions = [where_clause]
        
        if search_term:
            search_conditions.append(f"LOWER(subject) LIKE LOWER('%{search_term}%')")
        
        if ticket_id:
            search_conditions.append(f"display_id = '{ticket_id}'")
        
        if assignee_search:
            search_conditions.append(f"LOWER(assignee_name) LIKE LOWER('%{assignee_search}%')")
        
        if date_range and len(date_range) == 2:
            search_conditions.append(f"created_at BETWEEN '{date_range[0]}' AND '{date_range[1]}'")
        
        search_where = " AND ".join(search_conditions)
        
        search_query = f"""
        SELECT 
            ticket_id,
            display_id,
            subject,
            status,
            priority,
            assignee_name,
            created_at,
            age_days,
            lifecycle_state
        FROM MCP.VW_HF_TICKETS_EXPORT
        WHERE {search_where}
        ORDER BY created_at DESC
        LIMIT 100
        """
        
        results_df = session.sql(search_query).to_pandas()
        
        if not results_df.empty:
            st.success(f"Found {len(results_df)} tickets")
            st.dataframe(results_df, use_container_width=True, hide_index=True)
        else:
            st.warning("No tickets found matching your search criteria")

with tab5:
    st.header("Export Data")
    
    st.markdown("""
    Export ticket data to CSV for further analysis in Excel or other tools.
    """)
    
    # Export options
    export_type = st.radio(
        "What to export?",
        ["Current filtered view", "All tickets", "Specific product"]
    )
    
    if export_type == "Specific product":
        export_product = st.selectbox(
            "Select product",
            options=products_df['PRODUCT_PREFIX'].tolist()
        )
    
    # Build export query
    if export_type == "Current filtered view":
        export_query = f"""
        SELECT * FROM MCP.VW_HF_TICKETS_EXPORT
        WHERE {where_clause}
        """
    elif export_type == "All tickets":
        export_query = "SELECT * FROM MCP.VW_HF_TICKETS_EXPORT"
    else:
        export_query = f"""
        SELECT * FROM MCP.VW_HF_TICKETS_EXPORT
        WHERE product_prefix = '{export_product}'
        """
    
    # Preview
    if st.button("Preview Export (first 100 rows)"):
        preview_df = session.sql(f"{export_query} LIMIT 100").to_pandas()
        st.dataframe(preview_df, use_container_width=True, hide_index=True)
        st.info(f"Preview showing {len(preview_df)} rows")
    
    # Export button
    if st.button("📥 Download Full Export as CSV", type="primary"):
        with st.spinner("Preparing export..."):
            export_df = session.sql(export_query).to_pandas()
            
            # Convert to CSV
            csv = export_df.to_csv(index=False)
            
            # Download button
            st.download_button(
                label="💾 Click to Download CSV",
                data=csv,
                file_name=f"happyfox_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
            
            st.success(f"✅ Export ready! {len(export_df):,} tickets exported")

# Footer
st.markdown("---")
st.caption("HappyFox Analytics Dashboard | Data from ACTIVITY_STREAM | Two-table compliant")