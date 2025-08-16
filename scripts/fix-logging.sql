-- Fix Logging Infrastructure
-- Adds top-level columns to RAW_EVENTS and updates LOG_CLAUDE_EVENT

-- @statement
USE DATABASE CLAUDE_BI;

-- @statement
USE SCHEMA MCP;

-- =====================================================
-- 1. Add top-level columns to RAW_EVENTS
-- =====================================================
-- @statement
ALTER TABLE CLAUDE_BI.LANDING.RAW_EVENTS
  ADD COLUMN action STRING;

-- @statement  
ALTER TABLE CLAUDE_BI.LANDING.RAW_EVENTS
  ADD COLUMN actor STRING;

-- @statement
ALTER TABLE CLAUDE_BI.LANDING.RAW_EVENTS
  ADD COLUMN occurred_at TIMESTAMP_TZ;

-- @statement
ALTER TABLE CLAUDE_BI.LANDING.RAW_EVENTS
  ADD COLUMN dedupe_key STRING;

-- =====================================================
-- 2. Update LOG_CLAUDE_EVENT to write top-level columns
-- =====================================================
-- @statement
CREATE OR REPLACE PROCEDURE CLAUDE_BI.MCP.LOG_CLAUDE_EVENT(
  event_payload VARIANT, 
  source_lane STRING DEFAULT 'CLAUDE_CODE'
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
'BEGIN
  -- Allow both R_APP_WRITE and ACCOUNTADMIN (for testing)
  IF (NOT IS_ROLE_IN_SESSION(''R_APP_WRITE'') AND 
      NOT IS_ROLE_IN_SESSION(''ACCOUNTADMIN'') AND
      NOT IS_ROLE_IN_SESSION(''R_CLAUDE_AGENT'')) THEN
    RETURN OBJECT_CONSTRUCT(
      ''ok'', FALSE,
      ''error'', ''forbidden'',
      ''need_role'', ''R_APP_WRITE or ACCOUNTADMIN'',
      ''current_role'', CURRENT_ROLE()
    );
  END IF;

  -- VALIDATION
  IF (event_payload IS NULL OR NOT IS_OBJECT(event_payload)) THEN
    RETURN OBJECT_CONSTRUCT(
      ''ok'', FALSE,
      ''error'', ''invalid_payload''
    );
  END IF;

  -- Extract canonical fields with safe fallbacks
  LET v_action STRING := COALESCE(event_payload:action::STRING, ''unknown'');
  LET v_actor STRING := COALESCE(
    event_payload:actor_id::STRING,
    event_payload:actor::STRING,
    CURRENT_USER()
  );
  LET v_occurred TIMESTAMP_TZ := COALESCE(
    TRY_TO_TIMESTAMP_TZ(event_payload:occurred_at),
    CURRENT_TIMESTAMP()
  );
  
  -- Generate dedupe key
  LET v_dedupe_key STRING := COALESCE(
    event_payload:event_id::STRING,
    SHA2(CONCAT(v_action, v_actor, v_occurred::STRING, TO_VARCHAR(event_payload)), 256)
  );

  -- EVENT ENRICHMENT
  LET enriched := OBJECT_INSERT(
    event_payload,
    ''_claude_meta'',
    OBJECT_CONSTRUCT(
      ''logged_at'', CURRENT_TIMESTAMP(),
      ''query_tag'', CURRENT_QUERY_TAG(),
      ''warehouse'', CURRENT_WAREHOUSE(),
      ''user'', CURRENT_USER(),
      ''role'', CURRENT_ROLE(),
      ''session'', CURRENT_SESSION()
    ),
    TRUE
  );

  -- INSERT EVENT with top-level columns
  INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS (
    action,
    actor,
    occurred_at,
    dedupe_key,
    payload,
    _source_lane,
    _recv_at
  )
  SELECT 
    :v_action,
    :v_actor,
    :v_occurred,
    :v_dedupe_key,
    :enriched,
    :source_lane,
    CURRENT_TIMESTAMP();

  RETURN OBJECT_CONSTRUCT(
    ''ok'', TRUE,
    ''event_id'', v_dedupe_key,
    ''action'', v_action
  );

EXCEPTION
  WHEN OTHER THEN
    RETURN OBJECT_CONSTRUCT(
      ''ok'', FALSE,
      ''error'', ''insert_failed'',
      ''details'', SQLERRM
    );
END;';

-- =====================================================
-- 3. Grant permissions
-- =====================================================
-- @statement
GRANT EXECUTE ON PROCEDURE CLAUDE_BI.MCP.LOG_CLAUDE_EVENT(VARIANT, STRING) 
TO ROLE R_CLAUDE_AGENT;

-- @statement
GRANT EXECUTE ON PROCEDURE CLAUDE_BI.MCP.LOG_CLAUDE_EVENT(VARIANT, STRING) 
TO ROLE ACCOUNTADMIN;