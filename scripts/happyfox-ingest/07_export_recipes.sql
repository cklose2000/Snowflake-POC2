-- ============================================================================
-- HappyFox Export Recipes
-- Purpose: One-click export procedures for end users
-- ============================================================================

USE DATABASE CLAUDE_BI;
USE WAREHOUSE CLAUDE_WAREHOUSE;

-- ============================================================================
-- RECIPE 1: Export All Tickets to CSV (Current snapshot)
-- ============================================================================

-- Create user stage if not exists for exports
CREATE STAGE IF NOT EXISTS MCP.HAPPYFOX_EXPORTS
  COMMENT = 'Stage for HappyFox ticket exports';

-- Export procedure
CREATE OR REPLACE PROCEDURE MCP.EXPORT_HAPPYFOX_TICKETS(
  PRODUCT_FILTER VARCHAR DEFAULT NULL,
  STATUS_FILTER VARCHAR DEFAULT NULL
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
  export_path VARCHAR;
  row_count NUMBER;
  query_text VARCHAR;
BEGIN
  -- Build dynamic query
  query_text := 'SELECT * FROM MCP.VW_HF_TICKETS_EXPORT WHERE 1=1';
  
  IF (PRODUCT_FILTER IS NOT NULL) THEN
    query_text := query_text || ' AND product_prefix = ''' || PRODUCT_FILTER || '''';
  END IF;
  
  IF (STATUS_FILTER IS NOT NULL) THEN
    query_text := query_text || ' AND lifecycle_state = ''' || STATUS_FILTER || '''';
  END IF;
  
  -- Set export path with timestamp
  export_path := 'happyfox_export_' || 
                 COALESCE(PRODUCT_FILTER, 'all') || '_' ||
                 TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MISS') || '.csv';
  
  -- Execute export
  EXECUTE IMMEDIATE 'COPY INTO @MCP.HAPPYFOX_EXPORTS/' || export_path || '
    FROM (' || query_text || ')
    FILE_FORMAT = (TYPE = CSV HEADER = TRUE FIELD_OPTIONALLY_ENCLOSED_BY = ''"'')
    SINGLE = TRUE
    OVERWRITE = TRUE';
  
  -- Get row count
  SELECT COUNT(*) INTO row_count 
  FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
  
  RETURN 'Export completed: ' || row_count || ' rows exported to @MCP.HAPPYFOX_EXPORTS/' || export_path;
END;
$$;

-- ============================================================================
-- RECIPE 2: Quick Export Commands (For Snowsight users)
-- ============================================================================

-- Export all open tickets
/*
COPY INTO @~/happyfox_open_tickets.csv
FROM (
  SELECT * FROM MCP.VW_HF_TICKETS_EXPORT 
  WHERE lifecycle_state = 'Open'
)
FILE_FORMAT = (TYPE = CSV HEADER = TRUE)
SINGLE = TRUE
OVERWRITE = TRUE;
*/

-- Export by product
/*
COPY INTO @~/happyfox_gz_tickets.csv
FROM (
  SELECT * FROM MCP.VW_HF_TICKETS_EXPORT 
  WHERE product_prefix = 'GZ'
)
FILE_FORMAT = (TYPE = CSV HEADER = TRUE)
SINGLE = TRUE
OVERWRITE = TRUE;
*/

-- Export aging report (tickets > 30 days)
/*
COPY INTO @~/happyfox_aging_report.csv
FROM (
  SELECT * FROM MCP.VW_HF_TICKETS_EXPORT 
  WHERE age_days > 30 
    AND lifecycle_state = 'Open'
  ORDER BY age_days DESC
)
FILE_FORMAT = (TYPE = CSV HEADER = TRUE)
SINGLE = TRUE
OVERWRITE = TRUE;
*/

-- ============================================================================
-- RECIPE 3: Agent Performance Export
-- ============================================================================

CREATE OR REPLACE PROCEDURE MCP.EXPORT_AGENT_PERFORMANCE()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
  export_path VARCHAR;
  row_count NUMBER;
BEGIN
  export_path := 'agent_performance_' || TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MISS') || '.csv';
  
  COPY INTO @MCP.HAPPYFOX_EXPORTS/:export_path
  FROM (
    SELECT
      assignee_name,
      assignee_email,
      COUNT(*) AS total_tickets,
      SUM(CASE WHEN lifecycle_state = 'Closed' THEN 1 ELSE 0 END) AS closed_tickets,
      SUM(CASE WHEN lifecycle_state = 'Open' THEN 1 ELSE 0 END) AS open_tickets,
      AVG(time_spent_minutes) AS avg_time_spent,
      AVG(messages_count) AS avg_messages,
      AVG(age_days) AS avg_ticket_age
    FROM MCP.VW_HF_TICKETS_EXPORT
    WHERE assignee_name IS NOT NULL
    GROUP BY 1, 2
    ORDER BY total_tickets DESC
  )
  FILE_FORMAT = (TYPE = CSV HEADER = TRUE)
  SINGLE = TRUE
  OVERWRITE = TRUE;
  
  SELECT COUNT(*) INTO row_count 
  FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
  
  RETURN 'Agent performance exported: ' || row_count || ' agents to @MCP.HAPPYFOX_EXPORTS/' || export_path;
END;
$$;

-- ============================================================================
-- RECIPE 4: Download from Stage (After export)
-- ============================================================================

-- List available exports
-- LS @MCP.HAPPYFOX_EXPORTS;

-- Download specific file (in SnowSQL or Snowsight)
-- GET @MCP.HAPPYFOX_EXPORTS/happyfox_export_all_20250122_120000.csv file:///tmp/;

-- ============================================================================
-- VERIFICATION
-- ============================================================================

-- Test export procedure
CALL MCP.EXPORT_HAPPYFOX_TICKETS('GZ', 'Open');

-- List exported files
LS @MCP.HAPPYFOX_EXPORTS;