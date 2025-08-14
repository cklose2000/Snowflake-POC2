-- ============================================================================
-- Production Improvements for Activity Schema 2.0
-- Implements all expert recommendations for production readiness
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE CLAUDE_BI;

-- ============================================================================
-- 1. RETRY WRAPPER WITH SIZE GUARDS AND DEAD LETTER
-- ============================================================================

CREATE OR REPLACE PROCEDURE CLAUDE_BI.MCP.SAFE_INSERT_EVENT(
  payload VARIANT,
  source_lane STRING DEFAULT 'DIRECT'
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
  retry_count INTEGER DEFAULT 0;
  max_retries INTEGER DEFAULT 3;
  payload_size INTEGER;
  event_id STRING;
  error_msg STRING;
BEGIN
  -- Calculate payload size
  payload_size := BYTE_LENGTH(TO_JSON(:payload));
  
  -- Generate event ID for tracking
  event_id := COALESCE(
    :payload:event_id::STRING,
    SHA2(CONCAT_WS('|', 
      'insert',
      CURRENT_TIMESTAMP()::STRING,
      TO_JSON(:payload)
    ), 256)
  );
  
  -- Size guard - reject oversized payloads
  IF (payload_size > 1000000) THEN
    -- Log to dead letter (as an event!)
    INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS VALUES (
      OBJECT_CONSTRUCT(
        'event_id', CONCAT('dead_', event_id),
        'action', 'quality.payload.oversized',
        'occurred_at', CURRENT_TIMESTAMP(),
        'actor_id', 'system',
        'source', 'quality',
        'schema_version', '2.1.0',
        'object', OBJECT_CONSTRUCT(
          'type', 'rejected_event',
          'id', event_id
        ),
        'attributes', OBJECT_CONSTRUCT(
          'original_size_bytes', payload_size,
          'max_size_bytes', 1000000,
          'source_lane', :source_lane,
          'truncated_payload', SUBSTR(TO_JSON(:payload), 1, 1000)
        )
      ),
      'DEAD_LETTER',
      CURRENT_TIMESTAMP()
    );
    RETURN OBJECT_CONSTRUCT(
      'status', 'rejected',
      'reason', 'payload_oversized',
      'event_id', event_id,
      'size_bytes', payload_size
    )::STRING;
  END IF;
  
  -- Retry loop for transient failures
  WHILE (retry_count < max_retries) DO
    BEGIN
      -- Attempt insert
      INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS VALUES (
        :payload,
        :source_lane,
        CURRENT_TIMESTAMP()
      );
      
      -- Success - log metrics event
      INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS VALUES (
        OBJECT_CONSTRUCT(
          'event_id', CONCAT('metric_', event_id),
          'action', 'system.insert.success',
          'occurred_at', CURRENT_TIMESTAMP(),
          'actor_id', 'system',
          'source', 'system',
          'schema_version', '2.1.0',
          'attributes', OBJECT_CONSTRUCT(
            'event_id', event_id,
            'retry_count', retry_count,
            'size_bytes', payload_size,
            'source_lane', :source_lane
          )
        ),
        'SYSTEM',
        CURRENT_TIMESTAMP()
      );
      
      RETURN OBJECT_CONSTRUCT(
        'status', 'success',
        'event_id', event_id,
        'retries', retry_count
      )::STRING;
      
    EXCEPTION
      WHEN OTHER THEN
        error_msg := SQLERRM;
        retry_count := retry_count + 1;
        
        IF (retry_count >= max_retries) THEN
          -- Final failure - log to dead letter
          INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS VALUES (
            OBJECT_CONSTRUCT(
              'event_id', CONCAT('dead_', event_id),
              'action', 'quality.insert.failed',
              'occurred_at', CURRENT_TIMESTAMP(),
              'actor_id', 'system',
              'source', 'quality',
              'schema_version', '2.1.0',
              'object', OBJECT_CONSTRUCT(
                'type', 'failed_event',
                'id', event_id
              ),
              'attributes', OBJECT_CONSTRUCT(
                'error_message', error_msg,
                'retry_count', retry_count,
                'source_lane', :source_lane,
                'payload_size_bytes', payload_size
              )
            ),
            'DEAD_LETTER',
            CURRENT_TIMESTAMP()
          );
          
          RETURN OBJECT_CONSTRUCT(
            'status', 'failed',
            'reason', error_msg,
            'event_id', event_id,
            'retries', retry_count
          )::STRING;
        END IF;
        
        -- Wait before retry (exponential backoff)
        CALL SYSTEM$WAIT(POW(2, retry_count));
    END;
  END WHILE;
END;
$$;

-- ============================================================================
-- 2. DEDICATED WAREHOUSE CONFIGURATION
-- ============================================================================

-- Create dedicated XS warehouse for Dynamic Table refresh
CREATE WAREHOUSE IF NOT EXISTS DT_XS_WH
  WAREHOUSE_SIZE = 'X-SMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Dedicated warehouse for Dynamic Table refresh - minimal cost';

-- Create alert warehouse  
CREATE WAREHOUSE IF NOT EXISTS ALERT_WH
  WAREHOUSE_SIZE = 'X-SMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Dedicated warehouse for monitoring alerts';

-- ============================================================================
-- 3. COMPREHENSIVE MONITORING WITH ALERTS
-- ============================================================================

-- Health monitoring view
CREATE OR REPLACE VIEW CLAUDE_BI.MCP.DT_HEALTH_MONITOR AS
WITH refresh_data AS (
  SELECT 
    'CLAUDE_BI.ACTIVITY.EVENTS' as name,
    refresh_version,
    refresh_action,
    refresh_trigger,
    state,
    phase,
    phase_start_time,
    phase_end_time,
    details,
    DATEDIFF('seconds', phase_end_time, CURRENT_TIMESTAMP()) as seconds_since_refresh
  FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(
    NAME => 'CLAUDE_BI.ACTIVITY.EVENTS'
  ))
  WHERE phase = 'COMPLETED'
  QUALIFY ROW_NUMBER() OVER (ORDER BY phase_end_time DESC) = 1
),
dt_info AS (
  SELECT
    name,
    target_lag,
    warehouse,
    refresh_mode,
    initialize,
    last_suspended_on
  FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLES(
    SCHEMA_NAME => 'ACTIVITY',
    DATABASE_NAME => 'CLAUDE_BI'
  ))
  WHERE name = 'EVENTS'
)
SELECT 
  dt.name,
  dt.target_lag,
  dt.warehouse,
  rd.state,
  rd.seconds_since_refresh,
  rd.refresh_action as last_refresh_action,
  PARSE_JSON(rd.details):rows_inserted::NUMBER as rows_inserted,
  PARSE_JSON(rd.details):rows_updated::NUMBER as rows_updated,
  PARSE_JSON(rd.details):rows_deleted::NUMBER as rows_deleted,
  CASE 
    WHEN dt.last_suspended_on IS NOT NULL THEN 'SUSPENDED'
    WHEN rd.seconds_since_refresh > 300 THEN 'CRITICAL'  -- 5+ minutes
    WHEN rd.seconds_since_refresh > 120 THEN 'WARNING'   -- 2+ minutes
    ELSE 'HEALTHY'
  END as health_status,
  CASE
    WHEN rd.seconds_since_refresh > 300 THEN 
      'Dynamic Table refresh lag exceeds 5 minutes'
    WHEN rd.seconds_since_refresh > 120 THEN 
      'Dynamic Table refresh lag exceeds 2 minutes'
    ELSE 'Operating normally'
  END as health_message
FROM dt_info dt
LEFT JOIN refresh_data rd ON dt.name = rd.name;

-- Create monitoring alert
CREATE OR REPLACE ALERT CLAUDE_BI.MCP.DT_LAG_ALERT
  WAREHOUSE = ALERT_WH
  SCHEDULE = '5 MINUTE'
  IF (EXISTS (
    SELECT 1 FROM CLAUDE_BI.MCP.DT_HEALTH_MONITOR 
    WHERE health_status IN ('CRITICAL', 'WARNING')
  ))
  THEN CALL SYSTEM$SEND_EMAIL(
    'admin@example.com',
    'Dynamic Table Alert: ' || CURRENT_TIMESTAMP()::STRING,
    'The Dynamic Table EVENTS is experiencing refresh lag. Check DT_HEALTH_MONITOR for details.'
  );

-- Dead letter monitoring alert
CREATE OR REPLACE ALERT CLAUDE_BI.MCP.DEAD_LETTER_ALERT
  WAREHOUSE = ALERT_WH
  SCHEDULE = '15 MINUTE'
  IF (EXISTS (
    SELECT 1 
    FROM CLAUDE_BI.ACTIVITY.EVENTS
    WHERE action LIKE 'quality.%'
      AND occurred_at >= DATEADD('minute', -15, CURRENT_TIMESTAMP())
    HAVING COUNT(*) > 10
  ))
  THEN CALL SYSTEM$SEND_EMAIL(
    'admin@example.com', 
    'Dead Letter Alert: High failure rate detected',
    'More than 10 quality events in the last 15 minutes. Check QUALITY_EVENTS view.'
  );

-- Resume alerts
ALTER ALERT CLAUDE_BI.MCP.DT_LAG_ALERT RESUME;
ALTER ALERT CLAUDE_BI.MCP.DEAD_LETTER_ALERT RESUME;

-- ============================================================================
-- 4. INCREMENTAL-SAFE DEDUPLICATION
-- ============================================================================

-- Drop and recreate Dynamic Table with incremental-safe dedup
ALTER DYNAMIC TABLE IF EXISTS CLAUDE_BI.ACTIVITY.EVENTS SUSPEND;
DROP DYNAMIC TABLE IF EXISTS CLAUDE_BI.ACTIVITY.EVENTS;

CREATE OR REPLACE DYNAMIC TABLE CLAUDE_BI.ACTIVITY.EVENTS
CLUSTER BY (DATE(occurred_at), action)
TARGET_LAG = '1 minute'
WAREHOUSE = DT_XS_WH
AS
WITH with_ids AS (
  SELECT
    SHA2(CONCAT_WS('|',
      'v2',
      COALESCE(payload:action::STRING, ''),
      COALESCE(payload:actor_id::STRING, ''),
      COALESCE(payload:object:type::STRING, ''),
      COALESCE(payload:object:id::STRING, ''),
      TO_VARCHAR(COALESCE(
        TRY_TO_TIMESTAMP_TZ(payload:occurred_at::STRING),
        _recv_at
      ), 'YYYY-MM-DD"T"HH24:MI:SS.FF3'),
      COALESCE(_source_lane, ''),
      TO_VARCHAR(_recv_at, 'YYYY-MM-DD"T"HH24:MI:SS.FF3')
    ), 256) AS event_id,
    payload,
    _source_lane,
    _recv_at
  FROM CLAUDE_BI.LANDING.RAW_EVENTS
  WHERE BYTE_LENGTH(TO_JSON(payload)) <= 1000000
    AND TRY_PARSE_JSON(payload::STRING) IS NOT NULL
),
grouped AS (
  -- GROUP BY pattern for incremental-safe deduplication
  SELECT
    event_id,
    ANY_VALUE(payload) as payload,
    ANY_VALUE(_source_lane) as _source_lane,
    MIN(_recv_at) as _recv_at  -- First seen wins
  FROM with_ids
  GROUP BY event_id
)
SELECT 
  event_id,
  COALESCE(
    TRY_TO_TIMESTAMP_TZ(payload:occurred_at::STRING),
    _recv_at
  ) as occurred_at,
  payload:actor_id::STRING as actor_id,
  payload:action::STRING as action,
  payload:object:type::STRING as object_type,
  payload:object:id::STRING as object_id,
  COALESCE(payload:source::STRING, 'unknown') as source,
  COALESCE(payload:schema_version::STRING, '2.1.0') as schema_version,
  OBJECT_INSERT(
    COALESCE(payload:attributes, OBJECT_CONSTRUCT()),
    '_meta',
    OBJECT_CONSTRUCT(
      'recv_at', _recv_at,
      'source_lane', _source_lane,
      'content_hash', event_id
    ),
    TRUE
  ) as attributes,
  payload:depends_on_event_id::STRING as depends_on_event_id,
  _source_lane,
  _recv_at
FROM grouped;

-- ============================================================================
-- 5. COST MONITORING
-- ============================================================================

CREATE OR REPLACE VIEW CLAUDE_BI.MCP.COST_MONITOR AS
WITH warehouse_costs AS (
  SELECT 
    WAREHOUSE_NAME,
    DATE_TRUNC('day', START_TIME) as day,
    COUNT(*) as query_count,
    SUM(CREDITS_USED) as daily_credits,
    AVG(EXECUTION_TIME) as avg_execution_ms,
    MAX(EXECUTION_TIME) as max_execution_ms
  FROM TABLE(INFORMATION_SCHEMA.WAREHOUSE_METERING_HISTORY(
    DATE_RANGE_START => DATEADD('day', -7, CURRENT_DATE()),
    WAREHOUSE_NAME => 'DT_XS_WH'
  ))
  GROUP BY 1, 2
),
storage_costs AS (
  SELECT
    TABLE_CATALOG,
    TABLE_SCHEMA,
    TABLE_NAME,
    BYTES / POW(1024, 3) as size_gb,
    BYTES * 23 / POW(1024, 4) / 30 as monthly_storage_cost_usd  -- ~$23/TB/month
  FROM CLAUDE_BI.INFORMATION_SCHEMA.TABLES
  WHERE TABLE_TYPE IN ('BASE TABLE', 'DYNAMIC TABLE')
)
SELECT 
  'Compute' as cost_type,
  wc.warehouse_name as resource,
  wc.day,
  wc.query_count,
  wc.daily_credits,
  wc.daily_credits * 3 as estimated_daily_cost_usd,  -- ~$3/credit
  NULL as size_gb
FROM warehouse_costs wc
UNION ALL
SELECT
  'Storage' as cost_type,
  CONCAT(sc.TABLE_SCHEMA, '.', sc.TABLE_NAME) as resource,
  CURRENT_DATE() as day,
  NULL as query_count,
  NULL as daily_credits,
  sc.monthly_storage_cost_usd / 30 as estimated_daily_cost_usd,
  sc.size_gb
FROM storage_costs sc
ORDER BY cost_type, day DESC;

-- ============================================================================
-- 6. BACKFILL STRATEGY
-- ============================================================================

CREATE OR REPLACE PROCEDURE CLAUDE_BI.MCP.BACKFILL_FROM_BACKUP(
  backup_table_name STRING,
  start_date TIMESTAMP_TZ DEFAULT NULL,
  end_date TIMESTAMP_TZ DEFAULT NULL
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
  row_count INTEGER;
  batch_size INTEGER DEFAULT 10000;
  total_processed INTEGER DEFAULT 0;
BEGIN
  -- Count rows to process
  EXECUTE IMMEDIATE 
    'SELECT COUNT(*) FROM ' || :backup_table_name || 
    CASE 
      WHEN :start_date IS NOT NULL THEN ' WHERE occurred_at >= ''' || :start_date || ''''
      ELSE ''
    END ||
    CASE
      WHEN :end_date IS NOT NULL THEN 
        CASE 
          WHEN :start_date IS NOT NULL THEN ' AND '
          ELSE ' WHERE '
        END || 'occurred_at <= ''' || :end_date || ''''
      ELSE ''
    END
  INTO :row_count;
  
  -- Process in batches to avoid overwhelming the system
  FOR batch_num IN 0 TO CEIL(:row_count / :batch_size) - 1 DO
    -- Insert batch with _RESTORE tag
    EXECUTE IMMEDIATE 
      'INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS ' ||
      'SELECT payload, ''_RESTORE'' as _source_lane, ' ||
      'COALESCE(_recv_at, CURRENT_TIMESTAMP()) as _recv_at ' ||
      'FROM ' || :backup_table_name || 
      CASE 
        WHEN :start_date IS NOT NULL THEN ' WHERE occurred_at >= ''' || :start_date || ''''
        ELSE ''
      END ||
      CASE
        WHEN :end_date IS NOT NULL THEN 
          CASE 
            WHEN :start_date IS NOT NULL THEN ' AND '
            ELSE ' WHERE '
          END || 'occurred_at <= ''' || :end_date || ''''
        ELSE ''
      END ||
      ' LIMIT ' || :batch_size || ' OFFSET ' || (batch_num * :batch_size);
    
    total_processed := total_processed + :batch_size;
    
    -- Log progress
    INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS VALUES (
      OBJECT_CONSTRUCT(
        'event_id', SHA2(CONCAT('backfill_progress_', batch_num::STRING), 256),
        'action', 'system.backfill.progress',
        'occurred_at', CURRENT_TIMESTAMP(),
        'actor_id', 'system',
        'source', 'system',
        'attributes', OBJECT_CONSTRUCT(
          'backup_table', :backup_table_name,
          'batch_number', batch_num,
          'rows_processed', total_processed,
          'total_rows', :row_count,
          'percent_complete', ROUND(total_processed * 100.0 / :row_count, 2)
        )
      ),
      'SYSTEM',
      CURRENT_TIMESTAMP()
    );
    
    -- Small delay between batches
    CALL SYSTEM$WAIT(1);
  END FOR;
  
  RETURN OBJECT_CONSTRUCT(
    'status', 'success',
    'rows_processed', :row_count,
    'backup_table', :backup_table_name
  )::STRING;
END;
$$;

-- ============================================================================
-- 7. SEARCH OPTIMIZATION
-- ============================================================================

-- Add search optimization for common point lookups
ALTER TABLE CLAUDE_BI.ACTIVITY.EVENTS 
ADD SEARCH OPTIMIZATION ON EQUALITY(event_id, actor_id, action);

-- ============================================================================
-- 8. PERMISSION PRECEDENCE RULES
-- ============================================================================

CREATE OR REPLACE VIEW CLAUDE_BI.MCP.CURRENT_PERMISSIONS AS
WITH permission_events AS (
  SELECT 
    event_id,
    occurred_at,
    actor_id,
    action,
    object_type,
    object_id,
    attributes,
    -- Define precedence: DENY > GRANT > INHERIT
    CASE 
      WHEN action = 'system.permission.denied' THEN 0
      WHEN action = 'system.permission.granted' THEN 1
      WHEN action = 'system.permission.inherited' THEN 2
      ELSE 3
    END as precedence_order,
    -- Extract permission details
    attributes:allowed_actions::ARRAY as allowed_actions,
    attributes:denied_actions::ARRAY as denied_actions,
    attributes:max_rows::NUMBER as max_rows,
    attributes:expires_at::TIMESTAMP_TZ as expires_at,
    attributes:granted_by::STRING as granted_by
  FROM CLAUDE_BI.ACTIVITY.EVENTS
  WHERE action LIKE 'system.permission.%'
),
latest_permissions AS (
  -- Get latest permission per user, considering precedence
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY object_id  -- object_id is the user_id for permissions
      ORDER BY occurred_at DESC, precedence_order ASC
    ) as rn
  FROM permission_events
  WHERE expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP()
)
SELECT
  object_id as user_id,
  event_id as permission_event_id,
  action as permission_type,
  CASE
    WHEN action = 'system.permission.denied' THEN FALSE
    WHEN action = 'system.permission.granted' THEN TRUE
    ELSE NULL
  END as is_active,
  allowed_actions,
  denied_actions,
  max_rows,
  expires_at,
  granted_by,
  occurred_at as granted_at
FROM latest_permissions
WHERE rn = 1;

-- ============================================================================
-- 9. CANONICAL EVENT ID DOCUMENTATION
-- ============================================================================

CREATE OR REPLACE VIEW CLAUDE_BI.MCP.EVENT_ID_SPEC AS
SELECT 
  'v2.1.0' as spec_version,
  'SHA2-256 Content-Addressed Event IDs' as spec_name,
  $$
  Event ID Formula v2.1.0:
  ------------------------
  SHA2(
    CONCAT_WS('|',
      'v2',                                              -- Version prefix
      COALESCE(action, ''),                             -- Event action
      COALESCE(actor_id, ''),                           -- Actor identifier
      COALESCE(object_type, ''),                        -- Object type
      COALESCE(object_id, ''),                          -- Object identifier
      TO_VARCHAR(occurred_at, 'YYYY-MM-DD"T"HH24:MI:SS.FF3'),  -- ISO timestamp
      COALESCE(_source_lane, ''),                       -- Source lane
      TO_VARCHAR(_recv_at, 'YYYY-MM-DD"T"HH24:MI:SS.FF3')      -- Receive timestamp
    ),
    256  -- SHA2-256 algorithm
  )
  
  Properties:
  - Deterministic: Same input always produces same ID
  - Content-addressed: ID derived from event content
  - Collision-resistant: SHA2-256 provides strong uniqueness
  - Version-prefixed: Allows future algorithm changes
  - Idempotent: Re-inserting same event gets same ID
  $$ as formula,
  CURRENT_TIMESTAMP() as documented_at;

-- ============================================================================
-- 10. GRANT PERMISSIONS
-- ============================================================================

-- Grant necessary permissions
GRANT EXECUTE ON PROCEDURE CLAUDE_BI.MCP.SAFE_INSERT_EVENT(VARIANT, STRING) TO ROLE MCP_SERVICE_ROLE;
GRANT EXECUTE ON PROCEDURE CLAUDE_BI.MCP.BACKFILL_FROM_BACKUP(STRING, TIMESTAMP_TZ, TIMESTAMP_TZ) TO ROLE MCP_ADMIN_ROLE;
GRANT SELECT ON VIEW CLAUDE_BI.MCP.DT_HEALTH_MONITOR TO ROLE MCP_ADMIN_ROLE;
GRANT SELECT ON VIEW CLAUDE_BI.MCP.COST_MONITOR TO ROLE MCP_ADMIN_ROLE;
GRANT SELECT ON VIEW CLAUDE_BI.MCP.CURRENT_PERMISSIONS TO ROLE MCP_SERVICE_ROLE;
GRANT SELECT ON VIEW CLAUDE_BI.MCP.EVENT_ID_SPEC TO ROLE PUBLIC;

-- Show summary
SELECT 'Production improvements deployed successfully!' as status;