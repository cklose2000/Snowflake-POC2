-- ============================================================================
-- 19_log_procedures.sql
-- Robust batch logging procedures with error handling
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE CLAUDE_BI;
USE SCHEMA MCP;

-- ============================================================================
-- Main batch logging procedure with comprehensive error handling
-- ============================================================================

CREATE OR REPLACE PROCEDURE MCP.LOG_DEV_EVENT(
  batch ARRAY,
  source_lane STRING DEFAULT 'CLAUDE_CODE'
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
  accepted INTEGER DEFAULT 0;
  rejected INTEGER DEFAULT 0;
  oversized INTEGER DEFAULT 0;
  malformed INTEGER DEFAULT 0;
  error_msg STRING DEFAULT NULL;
  max_batch_size INTEGER;
  max_event_size INTEGER;
  event VARIANT;
  event_size INTEGER;
  validation_error STRING;
BEGIN
  -- Get configuration from context
  SELECT SYSTEM$GET_CONTEXT('MCP_LOGGING_CTX', 'max_batch_size')::INTEGER INTO max_batch_size;
  SELECT SYSTEM$GET_CONTEXT('MCP_LOGGING_CTX', 'max_event_size_bytes')::INTEGER INTO max_event_size;
  
  -- Default values if context not set
  max_batch_size := COALESCE(max_batch_size, 500);
  max_event_size := COALESCE(max_event_size, 102400);  -- 100KB
  
  -- Validate batch size
  IF (ARRAY_SIZE(batch) > max_batch_size) THEN
    -- Log batch size violation
    INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS VALUES (
      OBJECT_CONSTRUCT(
        'event_id', SHA2(CONCAT('batch_too_large_', CURRENT_TIMESTAMP()::STRING), 256),
        'action', 'quality.batch.oversized',
        'occurred_at', CURRENT_TIMESTAMP(),
        'actor_id', 'system',
        'source', 'quality',
        'attributes', OBJECT_CONSTRUCT(
          'batch_size', ARRAY_SIZE(batch),
          'max_size', max_batch_size,
          'source_lane', source_lane
        )
      ),
      'QUALITY_REJECT',
      CURRENT_TIMESTAMP()
    );
    
    RETURN OBJECT_CONSTRUCT(
      'success', FALSE,
      'error', 'Batch too large',
      'max_size', max_batch_size,
      'received', ARRAY_SIZE(batch)
    );
  END IF;
  
  -- Process each event with error handling
  FOR i IN 0 TO ARRAY_SIZE(batch) - 1 DO
    BEGIN
      event := batch[i];
      
      -- Validate event structure
      validation_error := NULL;
      
      -- Check required fields
      IF (event:action IS NULL) THEN
        validation_error := 'Missing required field: action';
      ELSEIF (event:session_id IS NULL) THEN
        validation_error := 'Missing required field: session_id';
      ELSEIF (event:idempotency_key IS NULL) THEN
        validation_error := 'Missing required field: idempotency_key';
      END IF;
      
      IF (validation_error IS NOT NULL) THEN
        -- Log validation error
        INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS VALUES (
          OBJECT_CONSTRUCT(
            'event_id', SHA2(CONCAT('validation_error_', i::STRING, '_', CURRENT_TIMESTAMP()::STRING), 256),
            'action', 'quality.event.invalid',
            'occurred_at', CURRENT_TIMESTAMP(),
            'actor_id', 'system',
            'source', 'quality',
            'attributes', OBJECT_CONSTRUCT(
              'error', validation_error,
              'event_index', i,
              'action', event:action,
              'session_id', event:session_id
            )
          ),
          'QUALITY_REJECT',
          CURRENT_TIMESTAMP()
        );
        rejected := rejected + 1;
        malformed := malformed + 1;
        CONTINUE;
      END IF;
      
      -- Check event size
      event_size := BYTE_LENGTH(TO_JSON(event));
      
      IF (event_size > max_event_size) THEN
        -- Log oversized event with truncated payload
        INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS VALUES (
          OBJECT_CONSTRUCT(
            'event_id', SHA2(CONCAT('oversized_', event:idempotency_key::STRING), 256),
            'action', 'quality.event.oversized',
            'occurred_at', CURRENT_TIMESTAMP(),
            'actor_id', 'system',
            'source', 'quality',
            'attributes', OBJECT_CONSTRUCT(
              'original_action', event:action::STRING,
              'session_id', event:session_id::STRING,
              'size_bytes', event_size,
              'max_size_bytes', max_event_size,
              'truncated_attributes', SUBSTR(TO_JSON(event:attributes), 1, 1000)
            )
          ),
          'QUALITY_REJECT',
          CURRENT_TIMESTAMP()
        );
        rejected := rejected + 1;
        oversized := oversized + 1;
        CONTINUE;
      END IF;
      
      -- Validate action namespace
      IF (NOT (event:action::STRING LIKE 'ccode.%' OR 
               event:action::STRING LIKE 'quality.%' OR
               event:action::STRING LIKE 'system.%')) THEN
        validation_error := 'Invalid action namespace: ' || event:action::STRING;
        rejected := rejected + 1;
        CONTINUE;
      END IF;
      
      -- Insert valid event
      INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS 
      VALUES (event, source_lane, CURRENT_TIMESTAMP());
      
      accepted := accepted + 1;
      
    EXCEPTION
      WHEN OTHER THEN
        -- Capture error for reporting
        error_msg := SQLERRM;
        rejected := rejected + 1;
        
        -- Log the error as a quality event
        BEGIN
          INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS VALUES (
            OBJECT_CONSTRUCT(
              'event_id', SHA2(CONCAT('insert_error_', i::STRING, '_', CURRENT_TIMESTAMP()::STRING), 256),
              'action', 'quality.event.insert_failed',
              'occurred_at', CURRENT_TIMESTAMP(),
              'actor_id', 'system',
              'source', 'quality',
              'attributes', OBJECT_CONSTRUCT(
                'error', error_msg,
                'event_index', i,
                'action', TRY_CAST(event:action AS STRING),
                'session_id', TRY_CAST(event:session_id AS STRING)
              )
            ),
            'DEAD_LETTER',
            CURRENT_TIMESTAMP()
          );
        EXCEPTION
          WHEN OTHER THEN
            -- If we can't even log the error, continue
            NULL;
        END;
    END;
  END FOR;
  
  -- Log batch processing summary if there were rejections
  IF (rejected > 0) THEN
    INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS VALUES (
      OBJECT_CONSTRUCT(
        'event_id', SHA2(CONCAT('batch_summary_', CURRENT_TIMESTAMP()::STRING), 256),
        'action', 'system.logging.batch_processed',
        'occurred_at', CURRENT_TIMESTAMP(),
        'actor_id', 'system',
        'source', 'system',
        'attributes', OBJECT_CONSTRUCT(
          'accepted', accepted,
          'rejected', rejected,
          'oversized', oversized,
          'malformed', malformed,
          'batch_size', ARRAY_SIZE(batch),
          'source_lane', source_lane,
          'last_error', error_msg
        )
      ),
      'SYSTEM',
      CURRENT_TIMESTAMP()
    );
  END IF;
  
  RETURN OBJECT_CONSTRUCT(
    'success', TRUE,
    'accepted', accepted,
    'rejected', rejected,
    'oversized', oversized,
    'malformed', malformed,
    'timestamp', CURRENT_TIMESTAMP(),
    'last_error', error_msg
  );
END;
$$;

-- ============================================================================
-- Single event logging procedure (wrapper around batch)
-- ============================================================================

CREATE OR REPLACE PROCEDURE MCP.LOG_SINGLE_EVENT(
  event VARIANT,
  source_lane STRING DEFAULT 'CLAUDE_CODE'
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
BEGIN
  -- Call batch procedure with single-item array
  RETURN MCP.LOG_DEV_EVENT(ARRAY_CONSTRUCT(event), source_lane);
END;
$$;

-- ============================================================================
-- Circuit breaker check procedure
-- ============================================================================

CREATE OR REPLACE FUNCTION MCP.CHECK_CIRCUIT_BREAKER(
  session_id STRING,
  action STRING
)
RETURNS BOOLEAN
LANGUAGE SQL
AS
$$
  -- Check if we've exceeded rate limits in the past minute
  WITH recent_events AS (
    SELECT COUNT(*) AS event_count
    FROM CLAUDE_BI.ACTIVITY.EVENTS
    WHERE attributes:session_id::STRING = session_id
      AND action = action
      AND occurred_at >= DATEADD('minute', -1, CURRENT_TIMESTAMP())
  )
  SELECT 
    CASE 
      WHEN event_count >= COALESCE(
        SYSTEM$GET_CONTEXT('MCP_LOGGING_CTX', 'circuit_breaker_threshold')::INTEGER,
        1000
      ) THEN FALSE  -- Circuit open, reject event
      ELSE TRUE     -- Circuit closed, allow event
    END
  FROM recent_events
$$;

-- ============================================================================
-- Procedure to log circuit breaker trips
-- ============================================================================

CREATE OR REPLACE PROCEDURE MCP.LOG_CIRCUIT_BREAK(
  session_id STRING,
  action STRING,
  event_count INTEGER
)
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
  INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS VALUES (
    OBJECT_CONSTRUCT(
      'event_id', SHA2(CONCAT_WS('|', 'circuit_break', session_id, action, CURRENT_TIMESTAMP()::STRING), 256),
      'action', 'quality.circuit.broken',
      'occurred_at', CURRENT_TIMESTAMP(),
      'actor_id', 'system',
      'source', 'quality',
      'attributes', OBJECT_CONSTRUCT(
        'session_id', session_id,
        'blocked_action', action,
        'event_count', event_count,
        'threshold', SYSTEM$GET_CONTEXT('MCP_LOGGING_CTX', 'circuit_breaker_threshold')::INTEGER,
        'window_seconds', SYSTEM$GET_CONTEXT('MCP_LOGGING_CTX', 'circuit_breaker_window_seconds')::INTEGER
      )
    ),
    'QUALITY_REJECT',
    CURRENT_TIMESTAMP()
  );
  
  RETURN 'Circuit breaker tripped for ' || session_id || ':' || action;
END;
$$;

-- ============================================================================
-- Compressed event logging for high-volume actions
-- ============================================================================

CREATE OR REPLACE PROCEDURE MCP.LOG_COMPRESSED_EVENT(
  session_id STRING,
  action STRING,
  path STRING,
  occurrence_count INTEGER,
  window_start TIMESTAMP_TZ,
  window_end TIMESTAMP_TZ,
  sample_attributes VARIANT
)
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
  INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS VALUES (
    OBJECT_CONSTRUCT(
      'event_id', SHA2(CONCAT_WS('|', 'compressed', session_id, action, path, window_start::STRING), 256),
      'action', action || '.compressed',
      'occurred_at', window_end,
      'actor_id', 'system',
      'source', 'compression',
      'attributes', OBJECT_CONSTRUCT(
        'session_id', session_id,
        'original_action', action,
        'path', path,
        'occurrences', occurrence_count,
        'window_start', window_start,
        'window_end', window_end,
        'window_duration_ms', DATEDIFF('millisecond', window_start, window_end),
        'sample_attributes', sample_attributes
      )
    ),
    'CLAUDE_CODE',
    CURRENT_TIMESTAMP()
  );
  
  RETURN 'Compressed ' || occurrence_count || ' events into 1';
END;
$$;

-- ============================================================================
-- Grant permissions
-- ============================================================================

GRANT EXECUTE ON PROCEDURE MCP.LOG_DEV_EVENT(ARRAY, STRING) TO ROLE MCP_LOGGER_ROLE;
GRANT EXECUTE ON PROCEDURE MCP.LOG_SINGLE_EVENT(VARIANT, STRING) TO ROLE MCP_LOGGER_ROLE;
GRANT EXECUTE ON FUNCTION MCP.CHECK_CIRCUIT_BREAKER(STRING, STRING) TO ROLE MCP_LOGGER_ROLE;
GRANT EXECUTE ON PROCEDURE MCP.LOG_CIRCUIT_BREAK(STRING, STRING, INTEGER) TO ROLE MCP_LOGGER_ROLE;
GRANT EXECUTE ON PROCEDURE MCP.LOG_COMPRESSED_EVENT(STRING, STRING, STRING, INTEGER, TIMESTAMP_TZ, TIMESTAMP_TZ, VARIANT) TO ROLE MCP_LOGGER_ROLE;

-- ============================================================================
-- Test the procedures
-- ============================================================================

-- Test single valid event
CALL MCP.LOG_SINGLE_EVENT(
  OBJECT_CONSTRUCT(
    'event_id', 'test_001',
    'action', 'ccode.test.event',
    'session_id', 'test_session_001',
    'idempotency_key', SHA2('test_001', 256),
    'occurred_at', CURRENT_TIMESTAMP(),
    'attributes', OBJECT_CONSTRUCT('test', TRUE)
  ),
  'TEST'
);

-- Test batch with mixed valid/invalid events
CALL MCP.LOG_DEV_EVENT(
  ARRAY_CONSTRUCT(
    OBJECT_CONSTRUCT(
      'action', 'ccode.test.valid',
      'session_id', 'test_session_002',
      'idempotency_key', SHA2('test_002', 256),
      'attributes', OBJECT_CONSTRUCT('valid', TRUE)
    ),
    OBJECT_CONSTRUCT(
      'action', 'ccode.test.missing_session'
      -- Missing session_id - should be rejected
    ),
    OBJECT_CONSTRUCT(
      'action', 'ccode.test.oversized',
      'session_id', 'test_session_003',
      'idempotency_key', SHA2('test_003', 256),
      'attributes', OBJECT_CONSTRUCT('data', REPEAT('x', 200000))  -- Too large
    )
  ),
  'TEST'
);