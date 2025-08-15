"""
ACTIVITY_DASHBOARD - Generated Dashboard
Generated: 2025-08-14T10:15:57.940Z
"""

import streamlit as st
import snowflake.connector
import pandas as pd
import os
from datetime import datetime, timedelta

st.set_page_config(page_title="activity_dashboard", layout="wide")
st.title("📊 ACTIVITY DASHBOARD")

# Connect to Snowflake
@st.cache_resource
def get_connection():
    return snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USERNAME'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        database='CLAUDE_BI',
        schema='ANALYTICS',
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE')
    )

conn = get_connection()

# Load data

@st.cache_data(ttl=3600)
def load_activity_summary_data():
    query = "SELECT * FROM activity_dashboard_activity_summary_8968e8e1"
    return pd.read_sql(query, conn)

activity_summary_df = load_activity_summary_data()


# Display panels
col1, col2 = st.columns(2)


with col1:
    st.subheader("📈 ACTIVITY SUMMARY")
    if not activity_summary_df.empty:
        for col in activity_summary_df.columns:
            st.metric(col, activity_summary_df[col].iloc[0])

with col2:
    st.subheader("📊 ACTIVITY COUNTS")
    if not activity_counts_df.empty:
        st.line_chart(activity_counts_df.set_index('hour')['event_count'])

with col1:
    st.subheader("📋 TOP ACTIVITIES")
    st.dataframe(top_activities_df)

with col2:
    st.subheader("📋 SQL EXECUTIONS")
    st.dataframe(sql_executions_df)

# Footer
st.markdown("---")
st.caption(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Data Window: 24h")
