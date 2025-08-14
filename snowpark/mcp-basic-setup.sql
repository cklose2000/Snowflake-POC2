-- Basic MCP Setup for Snowflake
-- Run this with ACCOUNTADMIN or CLAUDE_BI_ROLE with admin privileges

USE ROLE ACCOUNTADMIN;
USE DATABASE CLAUDE_BI;
USE WAREHOUSE CLAUDE_WAREHOUSE;

-- Create MCP schema for stored procedures
CREATE SCHEMA IF NOT EXISTS CLAUDE_BI.MCP;

USE SCHEMA CLAUDE_BI.MCP;

-- Create a simple validation stored procedure
CREATE OR REPLACE PROCEDURE VALIDATE_QUERY_PLAN(plan VARIANT)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
  source STRING;
  max_rows INTEGER;
  validation_result VARIANT;
BEGIN
  source := plan:source::STRING;
  max_rows := COALESCE(plan:top_n::INTEGER, 10000);
  
  -- Simple validation
  IF (max_rows > 10000) THEN
    RETURN OBJECT_CONSTRUCT('valid', FALSE, 'error', 'Row limit exceeds 10000');
  END IF;
  
  RETURN OBJECT_CONSTRUCT('valid', TRUE, 'message', 'Plan is valid');
END;
$$;

-- Create execution stored procedure
CREATE OR REPLACE PROCEDURE EXECUTE_QUERY_PLAN(plan VARIANT)
RETURNS TABLE(result VARIANT)
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
  source STRING;
  top_n INTEGER;
  sql_text STRING;
  res RESULTSET;
BEGIN
  -- Get parameters
  source := plan:source::STRING;
  top_n := COALESCE(plan:top_n::INTEGER, 100);
  
  -- Build SQL
  IF (source = 'VW_ACTIVITY_SUMMARY') THEN
    sql_text := 'SELECT * FROM CLAUDE_BI.ACTIVITY_CCODE.VW_ACTIVITY_SUMMARY';
  ELSEIF (source = 'VW_ACTIVITY_COUNTS_24H') THEN
    sql_text := 'SELECT * FROM CLAUDE_BI.ACTIVITY_CCODE.VW_ACTIVITY_COUNTS_24H LIMIT ' || top_n::STRING;
  ELSEIF (source = 'EVENTS') THEN
    sql_text := 'SELECT * FROM CLAUDE_BI.ACTIVITY.EVENTS ORDER BY TS DESC LIMIT ' || top_n::STRING;
  ELSE
    sql_text := 'SELECT \'Unknown source: ' || source || '\' as ERROR';
  END IF;
  
  -- Execute and return
  res := (EXECUTE IMMEDIATE :sql_text);
  RETURN TABLE(res);
END;
$$;

-- Grant access to current role
GRANT USAGE ON SCHEMA CLAUDE_BI.MCP TO ROLE CLAUDE_BI_ROLE;
GRANT USAGE ON PROCEDURE CLAUDE_BI.MCP.VALIDATE_QUERY_PLAN(VARIANT) TO ROLE CLAUDE_BI_ROLE;
GRANT USAGE ON PROCEDURE CLAUDE_BI.MCP.EXECUTE_QUERY_PLAN(VARIANT) TO ROLE CLAUDE_BI_ROLE;

-- Test the procedures
CALL CLAUDE_BI.MCP.VALIDATE_QUERY_PLAN(PARSE_JSON('{"source": "VW_ACTIVITY_SUMMARY", "top_n": 5}'));
CALL CLAUDE_BI.MCP.EXECUTE_QUERY_PLAN(PARSE_JSON('{"source": "VW_ACTIVITY_SUMMARY", "top_n": 5}'));