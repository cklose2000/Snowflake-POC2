# HappyFox Analytics - Streamlit in Snowflake App
# 100% Snowflake Native - Zero External Dependencies
# Copy this code into Snowsight → Streamlit → Create App

import streamlit as st
import pandas as pd
from datetime import datetime

# Page config
st.set_page_config(
    page_title="HappyFox Analytics",
    page_icon="🎫",
    layout="wide"
)

# Get native session - no auth needed in SiS
from snowflake.snowpark.context import get_active_session
session = get_active_session()

# Title
st.title("🎫 HappyFox Ticket Analytics")
st.markdown("Self-serve dashboard • 100% Snowflake native • Two-table compliant")

# Sidebar filters
with st.sidebar:
    st.header("Filters")
    
    # Get products dynamically
    products = session.sql(
        "SELECT DISTINCT product_prefix FROM MCP.VW_HF_TICKETS_LATEST WHERE product_prefix IS NOT NULL ORDER BY 1"
    ).collect()
    product_list = ["All"] + [row[0] for row in products]
    
    selected_product = st.selectbox("Product", product_list)
    selected_status = st.selectbox("Status", ["All", "Open", "Closed", "Unknown"])
    age_range = st.slider("Age (days)", 0, 365, (0, 180))

# Build filter conditions
where_parts = []
if selected_product != "All":
    where_parts.append(f"product_prefix = '{selected_product}'")
if selected_status != "All":
    where_parts.append(f"lifecycle_state = '{selected_status}'")
where_parts.append(f"age_days BETWEEN {age_range[0]} AND {age_range[1]}")
where_clause = " AND ".join(where_parts)

# Main tabs
tab1, tab2, tab3, tab4 = st.tabs(["📊 Overview", "📈 Trends", "🔍 Details", "📥 Export"])

with tab1:
    # Key metrics
    metrics_query = f"""
    SELECT 
        COUNT(*) as total,
        SUM(CASE WHEN lifecycle_state = 'Open' THEN 1 ELSE 0 END) as open,
        SUM(CASE WHEN lifecycle_state = 'Closed' THEN 1 ELSE 0 END) as closed,
        ROUND(AVG(age_days), 1) as avg_age,
        ROUND(AVG(time_spent_minutes), 1) as avg_time
    FROM MCP.VW_HF_TICKETS_EXPORT
    WHERE {where_clause}
    """
    
    metrics = session.sql(metrics_query).collect()[0]
    
    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Total", f"{metrics[0]:,}")
    col2.metric("Open", f"{metrics[1]:,}")
    col3.metric("Closed", f"{metrics[2]:,}")
    col4.metric("Avg Age", f"{metrics[3]:.0f}d")
    col5.metric("Avg Time", f"{metrics[4]:.0f}m")
    
    # Product breakdown (if showing all products)
    if selected_product == "All":
        st.subheader("By Product")
        
        # Use the table function for efficient querying
        product_stats = session.sql("SELECT * FROM TABLE(MCP.GET_HAPPYFOX_PRODUCT_STATS()) ORDER BY total_tickets DESC").to_pandas()
        
        if not product_stats.empty:
            # Create columns for charts
            c1, c2 = st.columns(2)
            
            with c1:
                # Bar chart of ticket counts
                chart_data = product_stats[['PRODUCT_PREFIX', 'OPEN_TICKETS', 'CLOSED_TICKETS']].set_index('PRODUCT_PREFIX')
                st.bar_chart(chart_data)
            
            with c2:
                # Table with key metrics
                display_df = product_stats[['PRODUCT_PREFIX', 'TOTAL_TICKETS', 'OPEN_TICKETS', 'AVG_AGE_DAYS', 'AVG_RESOLUTION_HOURS']].head(10)
                display_df.columns = ['Product', 'Total', 'Open', 'Avg Age (d)', 'Avg Resolution (h)']
                st.dataframe(display_df, use_container_width=True, hide_index=True)
    
    # Age distribution
    st.subheader("Age Distribution")
    age_query = f"""
    SELECT 
        age_bucket,
        COUNT(*) as count
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
        st.bar_chart(age_df.set_index('AGE_BUCKET')['COUNT'])

with tab2:
    st.subheader("Ticket Trends")
    
    # Date range selector
    trend_days = st.slider("Days to show", 7, 90, 30)
    
    # Daily trend query
    trend_query = f"""
    WITH daily AS (
        SELECT 
            DATE_TRUNC('day', created_at) as day,
            COUNT(*) as created
        FROM MCP.VW_HF_TICKETS_EXPORT
        WHERE created_at >= DATEADD('day', -{trend_days}, CURRENT_DATE())
            AND {where_clause}
        GROUP BY day
    ),
    closed AS (
        SELECT 
            DATE_TRUNC('day', last_updated_at) as day,
            COUNT(*) as closed
        FROM MCP.VW_HF_TICKETS_EXPORT
        WHERE lifecycle_state = 'Closed'
            AND last_updated_at >= DATEADD('day', -{trend_days}, CURRENT_DATE())
            AND {where_clause}
        GROUP BY day
    )
    SELECT 
        COALESCE(d.day, c.day) as day,
        COALESCE(d.created, 0) as created,
        COALESCE(c.closed, 0) as closed,
        SUM(COALESCE(d.created, 0) - COALESCE(c.closed, 0)) 
            OVER (ORDER BY COALESCE(d.day, c.day)) as net_backlog
    FROM daily d
    FULL OUTER JOIN closed c ON d.day = c.day
    ORDER BY day
    """
    
    trend_df = session.sql(trend_query).to_pandas()
    
    if not trend_df.empty:
        # Created vs Closed
        st.line_chart(trend_df.set_index('DAY')[['CREATED', 'CLOSED']])
        
        # Net backlog growth
        st.subheader("Backlog Growth")
        st.line_chart(trend_df.set_index('DAY')['NET_BACKLOG'])

with tab3:
    st.subheader("Ticket Details")
    
    # Search filters
    col1, col2, col3 = st.columns(3)
    with col1:
        search_text = st.text_input("Search in subject", "")
    with col2:
        ticket_id = st.text_input("Ticket ID", "")
    with col3:
        assignee = st.text_input("Assignee", "")
    
    # Build search query
    detail_where = [where_clause]
    if search_text:
        detail_where.append(f"LOWER(subject) LIKE LOWER('%{search_text}%')")
    if ticket_id:
        detail_where.append(f"display_id = '{ticket_id}'")
    if assignee:
        detail_where.append(f"LOWER(assignee_name) LIKE LOWER('%{assignee}%')")
    
    detail_query = f"""
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
    WHERE {' AND '.join(detail_where)}
    ORDER BY created_at DESC
    LIMIT 500
    """
    
    detail_df = session.sql(detail_query).to_pandas()
    
    st.info(f"Showing {len(detail_df)} tickets (max 500)")
    
    if not detail_df.empty:
        # Format dates for display
        detail_df['CREATED_AT'] = pd.to_datetime(detail_df['CREATED_AT']).dt.strftime('%Y-%m-%d')
        
        st.dataframe(
            detail_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "TICKET_ID": st.column_config.NumberColumn("ID"),
                "DISPLAY_ID": st.column_config.TextColumn("Display ID"),
                "SUBJECT": st.column_config.TextColumn("Subject"),
                "AGE_DAYS": st.column_config.NumberColumn("Age (days)")
            }
        )

with tab4:
    st.subheader("Export Data")
    
    st.info("""
    Export filtered ticket data to CSV for analysis in Excel or other tools.
    The export will include all fields from the current filter.
    """)
    
    # Show current filter summary
    st.write("**Current filters:**")
    st.write(f"- Product: {selected_product}")
    st.write(f"- Status: {selected_status}")
    st.write(f"- Age: {age_range[0]} to {age_range[1]} days")
    
    # Count records that will be exported
    count_query = f"SELECT COUNT(*) as count FROM MCP.VW_HF_TICKETS_EXPORT WHERE {where_clause}"
    export_count = session.sql(count_query).collect()[0][0]
    
    st.metric("Records to export", f"{export_count:,}")
    
    # Export button
    if st.button("📥 Prepare Export", type="primary"):
        with st.spinner(f"Loading {export_count:,} records..."):
            export_query = f"""
            SELECT 
                ticket_id,
                display_id,
                product_prefix,
                subject,
                status,
                priority,
                category,
                assignee_name,
                assignee_email,
                created_at,
                last_updated_at,
                age_days,
                lifecycle_state,
                messages_count,
                time_spent_minutes,
                source_channel
            FROM MCP.VW_HF_TICKETS_EXPORT
            WHERE {where_clause}
            ORDER BY created_at DESC
            """
            
            export_df = session.sql(export_query).to_pandas()
            
            # Convert to CSV
            csv = export_df.to_csv(index=False)
            
            # Generate filename
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"happyfox_{selected_product.lower()}_{timestamp}.csv"
            
            st.success(f"✅ Export ready: {len(export_df):,} tickets")
            
            # Download button
            st.download_button(
                label="💾 Download CSV",
                data=csv,
                file_name=filename,
                mime="text/csv"
            )

# Footer
st.markdown("---")
col1, col2, col3 = st.columns(3)
with col1:
    st.caption("🏗️ 100% Snowflake Native")
with col2:
    st.caption("📊 Two-Table Compliant")
with col3:
    st.caption("🔒 Zero External Dependencies")