-- Role and permission setup
USE DATABASE CLAUDE_BI;

-- Create role for Claude Code operations
CREATE ROLE IF NOT EXISTS CLAUDE_BI_ROLE;

-- Grant database access
GRANT USAGE ON DATABASE CLAUDE_BI TO ROLE CLAUDE_BI_ROLE;
GRANT USAGE ON SCHEMA ANALYTICS TO ROLE CLAUDE_BI_ROLE;
GRANT USAGE ON SCHEMA ACTIVITY_CCODE TO ROLE CLAUDE_BI_ROLE;

-- Grant table permissions
GRANT SELECT, INSERT ON TABLE analytics.activity.events TO ROLE CLAUDE_BI_ROLE;
GRANT SELECT, INSERT ON TABLE analytics.activity_ccode.artifacts TO ROLE CLAUDE_BI_ROLE;
GRANT SELECT, INSERT ON TABLE analytics.activity_ccode.artifact_data TO ROLE CLAUDE_BI_ROLE;
GRANT SELECT, INSERT ON TABLE analytics.activity_ccode.audit_results TO ROLE CLAUDE_BI_ROLE;

-- Grant stage permissions
GRANT READ, WRITE ON STAGE analytics.activity_ccode.artifact_stage TO ROLE CLAUDE_BI_ROLE;

-- Grant warehouse access
GRANT USAGE ON WAREHOUSE CLAUDE_WAREHOUSE TO ROLE CLAUDE_BI_ROLE;

-- Row Level Security (customers can only see their own data)
CREATE OR REPLACE ROW ACCESS POLICY customer_isolation
AS (customer = CURRENT_USER()) ON analytics.activity.events;

ALTER TABLE analytics.activity.events ADD ROW ACCESS POLICY customer_isolation;