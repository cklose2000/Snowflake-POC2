-- Check dashboard creation progress in real-time
-- Look for recent dashboard creation activities

-- 1. Check latest dashboard creation attempts (last 10 minutes)
SELECT 
    ts,
    activity,
    customer,
    feature_json:creation_id::STRING as creation_id,
    feature_json:step::STRING as step,
    feature_json:error::STRING as error_message,
    feature_json:creation_time_ms::NUMBER as elapsed_ms,
    DATEDIFF('second', LAG(ts) OVER (PARTITION BY feature_json:creation_id ORDER BY ts), ts) as seconds_since_last_step
FROM CLAUDE_BI.ACTIVITY.EVENTS
WHERE activity LIKE 'ccode.dashboard_%'
  AND ts >= DATEADD('minute', -10, CURRENT_TIMESTAMP())
ORDER BY ts DESC
LIMIT 20;

-- 2. Check for stuck dashboard creations (started but not completed)
WITH creation_windows AS (
    SELECT 
        feature_json:creation_id::STRING as creation_id,
        MIN(ts) as start_time,
        MAX(ts) as last_activity,
        COUNT(*) as steps_completed,
        ARRAY_AGG(DISTINCT feature_json:step::STRING) as steps,
        MAX(CASE WHEN activity = 'ccode.dashboard_log_completion' THEN 1 ELSE 0 END) as is_complete,
        MAX(CASE WHEN activity = 'ccode.dashboard_creation_failed' THEN 1 ELSE 0 END) as is_failed
    FROM CLAUDE_BI.ACTIVITY.EVENTS
    WHERE activity LIKE 'ccode.dashboard_%'
      AND ts >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
    GROUP BY 1
)
SELECT 
    creation_id,
    start_time,
    last_activity,
    DATEDIFF('second', start_time, last_activity) as total_seconds,
    DATEDIFF('second', last_activity, CURRENT_TIMESTAMP()) as seconds_since_last_activity,
    steps_completed,
    CASE 
        WHEN is_complete = 1 THEN 'COMPLETED'
        WHEN is_failed = 1 THEN 'FAILED'
        WHEN seconds_since_last_activity > 300 THEN 'STUCK/TIMEOUT'
        ELSE 'IN_PROGRESS'
    END as status,
    steps
FROM creation_windows
ORDER BY start_time DESC;

-- 3. Check WebSocket/Bridge activity
SELECT 
    ts,
    activity,
    customer,
    feature_json
FROM CLAUDE_BI.ACTIVITY.EVENTS
WHERE ts >= DATEADD('minute', -5, CURRENT_TIMESTAMP())
  AND (
    activity LIKE 'ccode.bridge_%' 
    OR activity LIKE 'ccode.websocket_%'
    OR activity LIKE 'ccode.user_%'
  )
ORDER BY ts DESC
LIMIT 10;