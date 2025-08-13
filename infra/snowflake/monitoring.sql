-- Monitoring and maintenance queries

-- Activity volume by hour
CREATE OR REPLACE VIEW analytics.activity_ccode.hourly_activity AS
SELECT 
  DATE_TRUNC('hour', ts) AS hour,
  COUNT(*) AS event_count,
  COUNT(DISTINCT customer) AS unique_customers,
  COUNT(DISTINCT activity) AS unique_activities
FROM analytics.activity.events
GROUP BY 1
ORDER BY 1 DESC;

-- Most active customers
CREATE OR REPLACE VIEW analytics.activity_ccode.top_customers AS
SELECT 
  customer,
  COUNT(*) AS total_events,
  COUNT(DISTINCT activity) AS unique_activities,
  MIN(ts) AS first_seen,
  MAX(ts) AS last_seen
FROM analytics.activity.events
GROUP BY 1
ORDER BY 2 DESC
LIMIT 100;

-- Activity type breakdown
CREATE OR REPLACE VIEW analytics.activity_ccode.activity_breakdown AS
SELECT 
  activity,
  COUNT(*) AS event_count,
  COUNT(DISTINCT customer) AS unique_customers,
  AVG(TRY_CAST(feature_json:tokens_used AS INTEGER)) AS avg_tokens
FROM analytics.activity.events
WHERE activity LIKE 'ccode.%'
GROUP BY 1
ORDER BY 2 DESC;

-- Audit success rate
CREATE OR REPLACE VIEW analytics.activity_ccode.audit_metrics AS
SELECT 
  DATE_TRUNC('day', audit_ts) AS day,
  COUNT(*) AS total_audits,
  SUM(CASE WHEN passed THEN 1 ELSE 0 END) AS passed_audits,
  (passed_audits / NULLIF(total_audits, 0)) * 100 AS pass_rate
FROM analytics.activity_ccode.audit_results
GROUP BY 1
ORDER BY 1 DESC;

-- Query performance monitoring
CREATE OR REPLACE VIEW analytics.activity_ccode.query_performance AS
SELECT 
  TRY_CAST(feature_json:query_tag AS VARCHAR) AS query_tag,
  TRY_CAST(feature_json:template AS VARCHAR) AS template,
  TRY_CAST(feature_json:execution_time_ms AS INTEGER) AS execution_time_ms,
  TRY_CAST(feature_json:rows_returned AS INTEGER) AS rows_returned,
  ts
FROM analytics.activity.events
WHERE activity = 'ccode.sql_executed'
ORDER BY ts DESC
LIMIT 1000;