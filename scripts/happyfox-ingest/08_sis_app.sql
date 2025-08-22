-- ============================================================================
-- HAPPYFOX ANALYTICS: COMPLETE SNOWFLAKE-NATIVE STACK
-- Zero external dependencies - runs 100% inside Snowflake
-- ============================================================================

USE DATABASE CLAUDE_BI;
USE WAREHOUSE CLAUDE_WAREHOUSE;
USE SCHEMA MCP;

-- ============================================================================
-- PART 1: SQL TABLE FUNCTIONS FOR PROGRAMMATIC ACCESS
-- ============================================================================

-- Get tickets with flexible filtering
CREATE OR REPLACE FUNCTION MCP.GET_HAPPYFOX_TICKETS(
  product_filter VARCHAR DEFAULT NULL,
  status_filter VARCHAR DEFAULT NULL,
  age_min NUMBER DEFAULT 0,
  age_max NUMBER DEFAULT 9999
)
RETURNS TABLE(
  ticket_id NUMBER,
  display_id VARCHAR,
  product_prefix VARCHAR,
  subject VARCHAR,
  status VARCHAR,
  priority VARCHAR,
  category VARCHAR,
  assignee_name VARCHAR,
  created_at TIMESTAMP_NTZ,
  age_days NUMBER,
  lifecycle_state VARCHAR,
  messages_count NUMBER,
  time_spent_minutes NUMBER
)
AS $$
  SELECT 
    ticket_id,
    display_id,
    product_prefix,
    subject,
    status,
    priority,
    category,
    assignee_name,
    created_at,
    age_days,
    lifecycle_state,
    messages_count,
    time_spent_minutes
  FROM MCP.VW_HF_TICKETS_EXPORT
  WHERE (product_filter IS NULL OR product_prefix = product_filter)
    AND (status_filter IS NULL OR lifecycle_state = status_filter)
    AND age_days BETWEEN age_min AND age_max
$$;

-- Get product summary stats
CREATE OR REPLACE FUNCTION MCP.GET_HAPPYFOX_PRODUCT_STATS()
RETURNS TABLE(
  product_prefix VARCHAR,
  total_tickets NUMBER,
  open_tickets NUMBER,
  closed_tickets NUMBER,
  avg_age_days NUMBER,
  avg_resolution_hours NUMBER
)
AS $$
  SELECT 
    product_prefix,
    COUNT(*) as total_tickets,
    SUM(CASE WHEN lifecycle_state = 'Open' THEN 1 ELSE 0 END) as open_tickets,
    SUM(CASE WHEN lifecycle_state = 'Closed' THEN 1 ELSE 0 END) as closed_tickets,
    AVG(age_days) as avg_age_days,
    AVG(CASE 
      WHEN lifecycle_state = 'Closed' 
      THEN DATEDIFF('hour', created_at, last_updated_at) 
      ELSE NULL 
    END) as avg_resolution_hours
  FROM MCP.VW_HF_TICKETS_EXPORT
  GROUP BY product_prefix
$$;

-- ============================================================================
-- PART 2: NATIVE ALERTS FOR MONITORING
-- ============================================================================

-- Alert for high backlog
CREATE OR REPLACE ALERT MCP.HAPPYFOX_HIGH_BACKLOG_ALERT
  WAREHOUSE = CLAUDE_WAREHOUSE
  SCHEDULE = 'USING CRON 0 9 * * MON-FRI America/New_York'
  COMMENT = 'Alert when any product has >100 open tickets'
  IF (EXISTS (
    SELECT 1 
    FROM MCP.VW_HF_TICKETS_EXPORT
    WHERE lifecycle_state = 'Open'
    GROUP BY product_prefix
    HAVING COUNT(*) > 100
  ))
  THEN BEGIN
    -- Log to event table (maintains two-table compliance)
    INSERT INTO LANDING.RAW_EVENTS (PAYLOAD, ACTION, ACTOR, OCCURRED_AT, DEDUPE_KEY)
    SELECT 
      OBJECT_CONSTRUCT(
        'event_id', UUID_STRING(),
        'action', 'alert.backlog.high',
        'actor_id', 'SYSTEM',
        'source', 'SNOWFLAKE_ALERT',
        'attributes', OBJECT_CONSTRUCT(
          'alert_name', 'HAPPYFOX_HIGH_BACKLOG',
          'triggered_at', CURRENT_TIMESTAMP(),
          'products_affected', ARRAY_AGG(product_prefix),
          'open_counts', ARRAY_AGG(open_count)
        ),
        'occurred_at', CURRENT_TIMESTAMP()
      ),
      'alert.backlog.high',
      'SYSTEM',
      CURRENT_TIMESTAMP(),
      SHA2(CONCAT('alert|backlog|', CURRENT_DATE()::STRING), 256)
    FROM (
      SELECT product_prefix, COUNT(*) as open_count
      FROM MCP.VW_HF_TICKETS_EXPORT
      WHERE lifecycle_state = 'Open'
      GROUP BY product_prefix
      HAVING COUNT(*) > 100
    );
  END;

-- Initially suspend (activate when ready)
ALTER ALERT MCP.HAPPYFOX_HIGH_BACKLOG_ALERT SUSPEND;

-- ============================================================================
-- PART 3: STREAMLIT IN SNOWFLAKE APP (EMBEDDED CODE)
-- ============================================================================

-- Create the Streamlit app with all code embedded
CREATE OR REPLACE STREAMLIT MCP.HAPPYFOX_ANALYTICS
  MAIN_FILE = '/app.py'
  QUERY_WAREHOUSE = 'CLAUDE_WAREHOUSE'
  COMMENT = 'HappyFox ticket analytics dashboard - 100% Snowflake native';

-- Note: The actual Python code needs to be uploaded via Snowsight UI
-- Here's what goes in the app.py file when creating via UI:

/*
STREAMLIT APP CODE (Copy this to Snowsight Streamlit editor):
================================================================

import streamlit as st
import pandas as pd
from datetime import datetime, timedelta

# Page config
st.set_page_config(
    page_title="HappyFox Analytics",
    page_icon="🎫",
    layout="wide"
)

# Get native session - no auth needed in SiS
session = st.connection.session()

# Title
st.title("🎫 HappyFox Ticket Analytics")
st.markdown("Self-serve dashboard - 100% Snowflake native")

# Sidebar filters
with st.sidebar:
    st.header("Filters")
    
    # Get products
    products = session.sql(
        "SELECT DISTINCT product_prefix FROM MCP.VW_HF_TICKETS_LATEST ORDER BY 1"
    ).collect()
    product_list = [row[0] for row in products]
    
    selected_product = st.selectbox(
        "Product", 
        ["All"] + product_list
    )
    
    selected_status = st.selectbox(
        "Status",
        ["All", "Open", "Closed", "Unknown"]
    )
    
    age_range = st.slider(
        "Age (days)",
        0, 365, (0, 180)
    )

# Main tabs
tab1, tab2, tab3 = st.tabs(["📊 Overview", "📈 Details", "📥 Export"])

with tab1:
    # Build filter
    where_clause = []
    if selected_product != "All":
        where_clause.append(f"product_prefix = '{selected_product}'")
    if selected_status != "All":
        where_clause.append(f"lifecycle_state = '{selected_status}'")
    where_clause.append(f"age_days BETWEEN {age_range[0]} AND {age_range[1]}")
    
    where = " AND ".join(where_clause) if where_clause else "1=1"
    
    # Metrics
    metrics_query = f"""
    SELECT 
        COUNT(*) as total,
        SUM(CASE WHEN lifecycle_state = 'Open' THEN 1 ELSE 0 END) as open,
        SUM(CASE WHEN lifecycle_state = 'Closed' THEN 1 ELSE 0 END) as closed,
        AVG(age_days) as avg_age
    FROM MCP.VW_HF_TICKETS_EXPORT
    WHERE {where}
    """
    
    metrics = session.sql(metrics_query).collect()[0]
    
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total", f"{metrics[0]:,}")
    col2.metric("Open", f"{metrics[1]:,}")
    col3.metric("Closed", f"{metrics[2]:,}")
    col4.metric("Avg Age", f"{metrics[3]:.1f} days")
    
    # Chart by product
    if selected_product == "All":
        chart_query = f"""
        SELECT product_prefix, lifecycle_state, COUNT(*) as count
        FROM MCP.VW_HF_TICKETS_EXPORT
        WHERE {where}
        GROUP BY 1, 2
        ORDER BY 3 DESC
        LIMIT 20
        """
        
        chart_df = session.sql(chart_query).to_pandas()
        
        if not chart_df.empty:
            pivot = chart_df.pivot(
                index='PRODUCT_PREFIX',
                columns='LIFECYCLE_STATE',
                values='COUNT'
            ).fillna(0)
            st.bar_chart(pivot)

with tab2:
    # Detailed grid
    st.subheader("Ticket Details")
    
    detail_query = f"""
    SELECT 
        ticket_id,
        display_id,
        subject,
        status,
        priority,
        assignee_name,
        created_at,
        age_days
    FROM MCP.VW_HF_TICKETS_EXPORT
    WHERE {where}
    ORDER BY created_at DESC
    LIMIT 1000
    """
    
    detail_df = session.sql(detail_query).to_pandas()
    
    st.info(f"Showing {len(detail_df)} tickets (max 1000)")
    st.dataframe(detail_df, use_container_width=True)

with tab3:
    st.subheader("Export Data")
    
    export_query = f"""
    SELECT * FROM MCP.VW_HF_TICKETS_EXPORT
    WHERE {where}
    """
    
    if st.button("Prepare Export"):
        with st.spinner("Loading data..."):
            export_df = session.sql(export_query).to_pandas()
            csv = export_df.to_csv(index=False)
            
            st.success(f"Ready to export {len(export_df):,} tickets")
            
            st.download_button(
                label="📥 Download CSV",
                data=csv,
                file_name=f"happyfox_{selected_product}_{datetime.now():%Y%m%d_%H%M%S}.csv",
                mime="text/csv"
            )

# Footer
st.markdown("---")
st.caption("100% Snowflake Native | Two-Table Compliant | Zero External Dependencies")

================================================================
*/

-- ============================================================================
-- PART 4: GRANT PERMISSIONS
-- ============================================================================

-- Grant access to views and functions
GRANT USAGE ON SCHEMA MCP TO ROLE PUBLIC;
GRANT SELECT ON VIEW MCP.VW_HF_TICKETS_LATEST TO ROLE PUBLIC;
GRANT SELECT ON VIEW MCP.VW_HF_TICKETS_EXPORT TO ROLE PUBLIC;
GRANT SELECT ON VIEW MCP.VW_HF_TICKET_HISTORY TO ROLE PUBLIC;
GRANT USAGE ON FUNCTION MCP.GET_HAPPYFOX_TICKETS(VARCHAR, VARCHAR, NUMBER, NUMBER) TO ROLE PUBLIC;
GRANT USAGE ON FUNCTION MCP.GET_HAPPYFOX_PRODUCT_STATS() TO ROLE PUBLIC;

-- ============================================================================
-- PART 5: SAMPLE QUERIES FOR SNOWSIGHT DASHBOARDS
-- ============================================================================

-- Dashboard Query 1: Product Overview
/*
SELECT 
    product_prefix,
    COUNT(*) as total_tickets,
    SUM(CASE WHEN lifecycle_state = 'Open' THEN 1 ELSE 0 END) as open_tickets,
    ROUND(AVG(age_days), 1) as avg_age_days,
    ROUND(AVG(time_spent_minutes), 1) as avg_time_spent
FROM MCP.VW_HF_TICKETS_EXPORT
GROUP BY product_prefix
ORDER BY total_tickets DESC;
*/

-- Dashboard Query 2: Daily Trend
/*
SELECT 
    DATE_TRUNC('day', created_at) as day,
    COUNT(*) as tickets_created,
    SUM(CASE WHEN lifecycle_state = 'Closed' THEN 1 ELSE 0 END) as tickets_closed
FROM MCP.VW_HF_TICKETS_EXPORT
WHERE created_at >= DATEADD('day', -30, CURRENT_DATE())
GROUP BY day
ORDER BY day;
*/

-- Dashboard Query 3: Agent Leaderboard
/*
SELECT 
    assignee_name,
    COUNT(*) as tickets_handled,
    ROUND(AVG(time_spent_minutes), 1) as avg_time_spent,
    SUM(CASE WHEN lifecycle_state = 'Closed' THEN 1 ELSE 0 END) as closed_tickets
FROM MCP.VW_HF_TICKETS_EXPORT
WHERE assignee_name IS NOT NULL
GROUP BY assignee_name
ORDER BY tickets_handled DESC
LIMIT 20;
*/

-- ============================================================================
-- VERIFICATION
-- ============================================================================

-- Test table functions
SELECT * FROM TABLE(MCP.GET_HAPPYFOX_PRODUCT_STATS());
SELECT COUNT(*) FROM TABLE(MCP.GET_HAPPYFOX_TICKETS('GZ', 'Open', 0, 30));

-- Check alert status
SHOW ALERTS IN SCHEMA MCP;

-- ============================================================================
-- DEPLOYMENT COMPLETE
-- ============================================================================
-- Access your Streamlit app at:
-- https://app.snowflake.com/<account>/CLAUDE_BI/MCP/streamlits/HAPPYFOX_ANALYTICS
--
-- Or use Snowsight worksheets with the sample queries above
-- ============================================================================