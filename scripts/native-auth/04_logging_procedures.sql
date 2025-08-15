-- ============================================================================
-- 04_logging_procedures.sql
-- Production-ready logging procedures with security hardening
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE CLAUDE_BI;
USE SCHEMA MCP;

-- ============================================================================
-- LOG_CLAUDE_EVENT - Direct logging with role guard
-- Primary path for Claude Code event logging
-- ============================================================================
CREATE OR REPLACE PROCEDURE LOG_CLAUDE_EVENT(
  event_payload VARIANT,
  source_lane STRING DEFAULT 'CLAUDE_CODE'
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
COMMENT = 'Production logging procedure for Claude Code events'
AS $$
BEGIN
  -- ============================================================================
  -- ROLE GUARD - Require write permission
  -- ============================================================================
  IF (NOT IS_ROLE_IN_SESSION('R_APP_WRITE')) THEN
    RETURN OBJECT_CONSTRUCT(
      'ok', FALSE,
      'error', 'forbidden',
      'need_role', 'R_APP_WRITE',
      'current_role', CURRENT_ROLE()
    );
  END IF;

  -- ============================================================================
  -- VALIDATION
  -- ============================================================================
  IF (event_payload IS NULL OR NOT IS_OBJECT(event_payload)) THEN
    RETURN OBJECT_CONSTRUCT(
      'ok', FALSE,
      'error', 'invalid_payload'
    );
  END IF;

  -- ============================================================================
  -- EVENT ENRICHMENT
  -- ============================================================================
  
  -- Generate event_id server-side if missing
  LET final_event_id := COALESCE(
    event_payload:event_id::STRING,
    SYSTEM$UUID()
  );
  
  -- Add Claude metadata
  LET enriched := OBJECT_INSERT(
    event_payload,
    '_claude_meta',
    OBJECT_CONSTRUCT(
      'logged_at', CURRENT_TIMESTAMP(),
      'query_tag', CURRENT_QUERY_TAG(),
      'warehouse', CURRENT_WAREHOUSE(),
      'ip', CURRENT_IP_ADDRESS(),
      'user', CURRENT_USER(),
      'role', CURRENT_ROLE(),
      'session', CURRENT_SESSION()
    ),
    TRUE
  );
  
  -- Add event_id if it was generated
  enriched := OBJECT_INSERT(enriched, 'event_id', final_event_id, TRUE);
  
  -- Add occurred_at if missing
  enriched := CASE
    WHEN enriched:occurred_at IS NULL
    THEN OBJECT_INSERT(enriched, 'occurred_at', CURRENT_TIMESTAMP()::STRING, TRUE)
    ELSE enriched
  END;

  -- ============================================================================
  -- OPTIONAL REDACTION (for PII protection)
  -- ============================================================================
  
  -- Redact sensitive fields if present
  -- Example: Remove email patterns from natural_language fields
  IF (enriched:attributes:natural_language IS NOT NULL) THEN
    LET redacted_text := REGEXP_REPLACE(
      enriched:attributes:natural_language::STRING,
      '[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}',
      '[REDACTED_EMAIL]'
    );
    enriched := OBJECT_INSERT(
      enriched,
      'attributes',
      OBJECT_INSERT(
        enriched:attributes,
        'natural_language',
        redacted_text,
        TRUE
      ),
      TRUE
    );
  END IF;

  -- ============================================================================
  -- INSERT EVENT
  -- ============================================================================
  INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS (
    payload,
    source_lane,
    ingested_at
  )
  SELECT 
    :enriched,
    :source_lane,
    CURRENT_TIMESTAMP();

  -- ============================================================================
  -- RETURN SUCCESS
  -- ============================================================================
  RETURN OBJECT_CONSTRUCT(
    'ok', TRUE,
    'event_id', final_event_id
  );

EXCEPTION
  WHEN OTHER THEN
    RETURN OBJECT_CONSTRUCT(
      'ok', FALSE,
      'error', 'insert_failed',
      'details', SQLERRM
    );
END;
$$;

-- ============================================================================
-- LOG_CLAUDE_EVENTS_BATCH - Batch logging for high volume
-- ============================================================================
CREATE OR REPLACE PROCEDURE LOG_CLAUDE_EVENTS_BATCH(
  events ARRAY,
  source_lane STRING DEFAULT 'CLAUDE_CODE'
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
COMMENT = 'Batch logging procedure for high-volume Claude Code events'
AS $$
DECLARE
  accepted INTEGER DEFAULT 0;
  rejected INTEGER DEFAULT 0;
  errors ARRAY DEFAULT ARRAY_CONSTRUCT();
BEGIN
  -- ============================================================================
  -- ROLE GUARD
  -- ============================================================================
  IF (NOT IS_ROLE_IN_SESSION('R_APP_WRITE')) THEN
    RETURN OBJECT_CONSTRUCT(
      'ok', FALSE,
      'error', 'forbidden',
      'need_role', 'R_APP_WRITE'
    );
  END IF;

  -- ============================================================================
  -- VALIDATION
  -- ============================================================================
  IF (events IS NULL OR ARRAY_SIZE(events) = 0) THEN
    RETURN OBJECT_CONSTRUCT(
      'ok', FALSE,
      'error', 'empty_batch'
    );
  END IF;
  
  -- Cap batch size at 1000 events
  IF (ARRAY_SIZE(events) > 1000) THEN
    RETURN OBJECT_CONSTRUCT(
      'ok', FALSE,
      'error', 'batch_too_large',
      'max_size', 1000,
      'provided', ARRAY_SIZE(events)
    );
  END IF;

  -- ============================================================================
  -- BATCH INSERT WITH ENRICHMENT
  -- ============================================================================
  
  -- Process each event
  FOR i IN 0 TO ARRAY_SIZE(events) - 1 DO
    LET event := events[i];
    
    -- Skip invalid events
    IF (event IS NULL OR NOT IS_OBJECT(event)) THEN
      rejected := rejected + 1;
      errors := ARRAY_APPEND(errors, OBJECT_CONSTRUCT(
        'index', i,
        'error', 'invalid_event'
      ));
      CONTINUE;
    END IF;
    
    -- Enrich event
    LET enriched := OBJECT_INSERT(
      event,
      '_claude_meta',
      OBJECT_CONSTRUCT(
        'logged_at', CURRENT_TIMESTAMP(),
        'query_tag', CURRENT_QUERY_TAG(),
        'warehouse', CURRENT_WAREHOUSE(),
        'ip', CURRENT_IP_ADDRESS(),
        'batch_id', SYSTEM$UUID(),
        'batch_index', i
      ),
      TRUE
    );
    
    -- Add event_id if missing
    enriched := CASE
      WHEN enriched:event_id IS NULL
      THEN OBJECT_INSERT(enriched, 'event_id', SYSTEM$UUID(), TRUE)
      ELSE enriched
    END;
    
    -- Add occurred_at if missing
    enriched := CASE
      WHEN enriched:occurred_at IS NULL
      THEN OBJECT_INSERT(enriched, 'occurred_at', CURRENT_TIMESTAMP()::STRING, TRUE)
      ELSE enriched
    END;
    
    -- Insert event
    BEGIN
      INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS (
        payload,
        source_lane,
        ingested_at
      )
      VALUES (
        :enriched,
        :source_lane,
        CURRENT_TIMESTAMP()
      );
      
      accepted := accepted + 1;
    EXCEPTION
      WHEN OTHER THEN
        rejected := rejected + 1;
        errors := ARRAY_APPEND(errors, OBJECT_CONSTRUCT(
          'index', i,
          'error', SQLERRM
        ));
    END;
  END FOR;

  -- ============================================================================
  -- RETURN RESULTS
  -- ============================================================================
  RETURN OBJECT_CONSTRUCT(
    'ok', accepted > 0,
    'accepted', accepted,
    'rejected', rejected,
    'total', ARRAY_SIZE(events),
    'errors', CASE WHEN ARRAY_SIZE(errors) > 0 THEN errors ELSE NULL END
  );
END;
$$;

-- ============================================================================
-- ROTATE_AGENT_KEY - Key rotation for agents
-- ============================================================================
CREATE OR REPLACE PROCEDURE ROTATE_AGENT_KEY(
  username STRING,
  new_public_key STRING
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
COMMENT = 'Rotate RSA key for agent authentication'
AS $$
BEGIN
  -- Require admin role
  IF (NOT IS_ROLE_IN_SESSION('R_APP_ADMIN')) THEN
    RETURN OBJECT_CONSTRUCT(
      'ok', FALSE,
      'error', 'forbidden',
      'need_role', 'R_APP_ADMIN'
    );
  END IF;
  
  -- Validate inputs
  IF (username IS NULL OR new_public_key IS NULL) THEN
    RETURN OBJECT_CONSTRUCT(
      'ok', FALSE,
      'error', 'invalid_parameters'
    );
  END IF;
  
  -- Update the key
  EXECUTE IMMEDIATE 'ALTER USER ' || username || ' SET RSA_PUBLIC_KEY = ''' || new_public_key || '''';
  
  -- Log the rotation
  INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS
  SELECT OBJECT_CONSTRUCT(
    'event_id', SYSTEM$UUID(),
    'action', 'security.key.rotated',
    'actor_id', CURRENT_USER(),
    'occurred_at', CURRENT_TIMESTAMP(),
    'object', OBJECT_CONSTRUCT(
      'type', 'user',
      'id', username
    ),
    'attributes', OBJECT_CONSTRUCT(
      'rotated_by', CURRENT_USER(),
      'rotated_at', CURRENT_TIMESTAMP()
    )
  ), 'SYSTEM', CURRENT_TIMESTAMP();
  
  RETURN OBJECT_CONSTRUCT(
    'ok', TRUE,
    'username', username,
    'rotated_at', CURRENT_TIMESTAMP()
  );
  
EXCEPTION
  WHEN OTHER THEN
    RETURN OBJECT_CONSTRUCT(
      'ok', FALSE,
      'error', 'rotation_failed',
      'details', SQLERRM
    );
END;
$$;

-- ============================================================================
-- GET_SESSION_METRICS - Get metrics for current session
-- ============================================================================
CREATE OR REPLACE PROCEDURE GET_SESSION_METRICS(
  session_id STRING DEFAULT NULL
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
COMMENT = 'Get metrics for a Claude Code session'
AS $$
DECLARE
  metrics VARIANT;
BEGIN
  -- Use current session if not provided
  LET target_session := COALESCE(session_id, CURRENT_SESSION()::STRING);
  
  -- Get metrics from events
  SELECT OBJECT_CONSTRUCT(
    'session_id', target_session,
    'total_events', COUNT(*),
    'unique_actions', COUNT(DISTINCT payload:action),
    'first_event', MIN(ingested_at),
    'last_event', MAX(ingested_at),
    'duration_minutes', DATEDIFF('minute', MIN(ingested_at), MAX(ingested_at)),
    'event_types', OBJECT_AGG(
      payload:action::STRING,
      COUNT(*)
    )
  ) INTO metrics
  FROM CLAUDE_BI.LANDING.RAW_EVENTS
  WHERE payload:session_id = target_session
    OR payload:_claude_meta:session = target_session;
  
  RETURN OBJECT_CONSTRUCT(
    'ok', TRUE,
    'metrics', metrics
  );
  
EXCEPTION
  WHEN OTHER THEN
    RETURN OBJECT_CONSTRUCT(
      'ok', FALSE,
      'error', 'metrics_failed',
      'details', SQLERRM
    );
END;
$$;

-- ============================================================================
-- Grant procedures to appropriate roles
-- ============================================================================

-- Logging procedures (require R_APP_WRITE)
GRANT USAGE ON PROCEDURE LOG_CLAUDE_EVENT(VARIANT, STRING) TO ROLE R_APP_WRITE;
GRANT USAGE ON PROCEDURE LOG_CLAUDE_EVENTS_BATCH(ARRAY, STRING) TO ROLE R_APP_WRITE;

-- Admin procedures
GRANT USAGE ON PROCEDURE ROTATE_AGENT_KEY(STRING, STRING) TO ROLE R_APP_ADMIN;

-- Read procedures
GRANT USAGE ON PROCEDURE GET_SESSION_METRICS(STRING) TO ROLE R_APP_READ;

-- ============================================================================
-- Create sequence for deterministic IDs (optional)
-- ============================================================================
CREATE OR REPLACE SEQUENCE CLAUDE_BI.ACTIVITY.EVENT_ID_SEQ
  START = 1000000
  INCREMENT = 1
  COMMENT = 'Optional sequence for deterministic event IDs';

GRANT USAGE ON SEQUENCE CLAUDE_BI.ACTIVITY.EVENT_ID_SEQ TO ROLE R_APP_WRITE;

-- ============================================================================
-- Success message
-- ============================================================================
SELECT 'Logging procedures created successfully!' AS status,
       'Next: Run 05_dynamic_tables.sql' AS next_action;