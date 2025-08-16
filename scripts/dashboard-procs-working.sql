-- Dashboard Stored Procedures (Working Version)
-- @statement
USE DATABASE CLAUDE_BI;

-- @statement
USE SCHEMA MCP;

-- @statement
CREATE OR REPLACE VIEW MCP.VW_DASHBOARDS AS
WITH latest_dashboards AS (
  SELECT 
    object_id AS dashboard_id,
    attributes:title::STRING AS title,
    attributes:spec AS spec,
    occurred_at AS created_at,
    actor_id AS created_by,
    IFNULL(attributes:refresh_interval_sec::NUMBER, 300) AS refresh_interval_sec,
    IFNULL(attributes:is_active::BOOLEAN, TRUE) AS is_active,
    ROW_NUMBER() OVER (PARTITION BY object_id ORDER BY occurred_at DESC) AS rn
  FROM ACTIVITY.EVENTS
  WHERE action IN ('dashboard.created', 'dashboard.updated')
    AND object_type = 'dashboard'
)
SELECT 
  dashboard_id,
  title,
  spec,
  created_at,
  created_by,
  refresh_interval_sec,
  is_active
FROM latest_dashboards
WHERE rn = 1 AND is_active = TRUE;

-- @statement
CREATE OR REPLACE PROCEDURE MCP.DASH_GET_SERIES(
  start_ts TIMESTAMP_TZ,
  end_ts TIMESTAMP_TZ,
  interval_str STRING,
  filters VARIANT,
  group_by STRING
)
RETURNS VARIANT
LANGUAGE SQL
AS
'BEGIN
  LET result VARIANT;
  
  SELECT ARRAY_AGG(
    OBJECT_CONSTRUCT(
      ''time_bucket'', time_bucket,
      ''event_count'', cnt
    )
  ) INTO result
  FROM (
    SELECT 
      TIME_SLICE(occurred_at, 1, interval_str) AS time_bucket,
      COUNT(*) AS cnt
    FROM ACTIVITY.EVENTS 
    WHERE occurred_at BETWEEN start_ts AND end_ts
    GROUP BY time_bucket
    ORDER BY time_bucket
  );
  
  RETURN OBJECT_CONSTRUCT(
    ''ok'', TRUE,
    ''data'', IFNULL(result, ARRAY_CONSTRUCT())
  );
END;';

-- @statement
CREATE OR REPLACE PROCEDURE MCP.DASH_GET_TOPN(
  start_ts TIMESTAMP_TZ,
  end_ts TIMESTAMP_TZ,
  dimension STRING,
  filters VARIANT,
  top_n NUMBER
)
RETURNS VARIANT
LANGUAGE SQL
AS
'BEGIN
  LET result VARIANT;
  
  IF (dimension = ''action'') THEN
    SELECT ARRAY_AGG(
      OBJECT_CONSTRUCT(
        ''item'', action,
        ''count'', cnt
      )
    ) INTO result
    FROM (
      SELECT action, COUNT(*) AS cnt
      FROM ACTIVITY.EVENTS
      WHERE occurred_at BETWEEN start_ts AND end_ts
      GROUP BY action
      ORDER BY cnt DESC
      LIMIT :top_n
    );
  ELSEIF (dimension = ''actor_id'') THEN
    SELECT ARRAY_AGG(
      OBJECT_CONSTRUCT(
        ''item'', actor_id,
        ''count'', cnt
      )
    ) INTO result
    FROM (
      SELECT actor_id, COUNT(*) AS cnt
      FROM ACTIVITY.EVENTS
      WHERE occurred_at BETWEEN start_ts AND end_ts
      GROUP BY actor_id
      ORDER BY cnt DESC
      LIMIT :top_n
    );
  ELSE
    SELECT ARRAY_AGG(
      OBJECT_CONSTRUCT(
        ''item'', object_type,
        ''count'', cnt
      )
    ) INTO result
    FROM (
      SELECT object_type, COUNT(*) AS cnt
      FROM ACTIVITY.EVENTS
      WHERE occurred_at BETWEEN start_ts AND end_ts
      GROUP BY object_type
      ORDER BY cnt DESC
      LIMIT :top_n
    );
  END IF;
  
  RETURN OBJECT_CONSTRUCT(
    ''ok'', TRUE,
    ''data'', IFNULL(result, ARRAY_CONSTRUCT())
  );
END;';

-- @statement
CREATE OR REPLACE PROCEDURE MCP.DASH_GET_EVENTS(
  cursor_ts TIMESTAMP_TZ,
  limit_rows NUMBER
)
RETURNS VARIANT
LANGUAGE SQL
AS
'BEGIN
  LET result VARIANT;
  
  SELECT ARRAY_AGG(
    OBJECT_CONSTRUCT(
      ''event_id'', event_id,
      ''occurred_at'', occurred_at,
      ''action'', action,
      ''actor_id'', actor_id,
      ''object_type'', object_type,
      ''object_id'', object_id
    )
  ) INTO result
  FROM (
    SELECT 
      event_id,
      occurred_at,
      action,
      actor_id,
      object_type,
      object_id
    FROM ACTIVITY.EVENTS
    WHERE occurred_at >= cursor_ts
    ORDER BY occurred_at DESC
    LIMIT :limit_rows
  );
  
  RETURN OBJECT_CONSTRUCT(
    ''ok'', TRUE,
    ''data'', IFNULL(result, ARRAY_CONSTRUCT())
  );
END;';

-- @statement
CREATE OR REPLACE PROCEDURE MCP.DASH_GET_METRICS(
  start_ts TIMESTAMP_TZ,
  end_ts TIMESTAMP_TZ,
  filters VARIANT
)
RETURNS VARIANT
LANGUAGE SQL
AS
'BEGIN
  LET total_events NUMBER;
  LET unique_actors NUMBER;
  LET unique_actions NUMBER;
  
  SELECT 
    COUNT(*) AS total,
    COUNT(DISTINCT actor_id) AS actors,
    COUNT(DISTINCT action) AS actions
  INTO 
    total_events,
    unique_actors,
    unique_actions
  FROM ACTIVITY.EVENTS
  WHERE occurred_at BETWEEN start_ts AND end_ts;
  
  RETURN OBJECT_CONSTRUCT(
    ''ok'', TRUE,
    ''data'', OBJECT_CONSTRUCT(
      ''total_events'', total_events,
      ''unique_actors'', unique_actors,
      ''unique_actions'', unique_actions,
      ''period_start'', start_ts,
      ''period_end'', end_ts
    )
  );
END;';

-- @statement
CREATE OR REPLACE PROCEDURE MCP.SAVE_DASHBOARD(
  dashboard_id STRING,
  title STRING,
  spec VARIANT,
  refresh_interval_sec NUMBER
)
RETURNS VARIANT
LANGUAGE SQL
AS
'BEGIN
  INSERT INTO LANDING.RAW_EVENTS (payload, _source_lane, _recv_at)
  VALUES (
    OBJECT_CONSTRUCT(
      ''event_id'', UUID_STRING(),
      ''action'', ''dashboard.created'',
      ''actor_id'', CURRENT_USER(),
      ''object'', OBJECT_CONSTRUCT(
        ''type'', ''dashboard'',
        ''id'', dashboard_id
      ),
      ''attributes'', OBJECT_CONSTRUCT(
        ''title'', title,
        ''spec'', spec,
        ''refresh_interval_sec'', refresh_interval_sec,
        ''is_active'', TRUE
      ),
      ''occurred_at'', CURRENT_TIMESTAMP()
    ),
    ''DASHBOARD_SYSTEM'',
    CURRENT_TIMESTAMP()
  );
  
  CALL LOG_CLAUDE_EVENT(OBJECT_CONSTRUCT(
    ''action'', ''dashboard.saved'',
    ''object'', OBJECT_CONSTRUCT(''type'', ''dashboard'', ''id'', dashboard_id),
    ''attributes'', OBJECT_CONSTRUCT(
      ''title'', title,
      ''refresh_sec'', refresh_interval_sec
    )
  ), ''DASHBOARD'');
  
  RETURN OBJECT_CONSTRUCT(
    ''ok'', TRUE,
    ''dashboard_id'', dashboard_id,
    ''url'', CONCAT(''/d/'', dashboard_id)
  );
END;';

-- @statement
CREATE OR REPLACE PROCEDURE MCP.GET_DASHBOARD(dashboard_id STRING)
RETURNS VARIANT
LANGUAGE SQL
AS
'DECLARE
  dash_record OBJECT;
BEGIN
  SELECT OBJECT_CONSTRUCT(
    ''dashboard_id'', dashboard_id,
    ''title'', title,
    ''spec'', spec,
    ''refresh_interval_sec'', refresh_interval_sec,
    ''created_at'', created_at,
    ''created_by'', created_by
  ) INTO dash_record
  FROM MCP.VW_DASHBOARDS
  WHERE dashboard_id = :dashboard_id
  LIMIT 1;
  
  IF (dash_record IS NULL) THEN
    RETURN OBJECT_CONSTRUCT(
      ''ok'', FALSE,
      ''error'', ''Dashboard not found''
    );
  ELSE
    RETURN OBJECT_CONSTRUCT(
      ''ok'', TRUE,
      ''data'', dash_record
    );
  END IF;
END;';