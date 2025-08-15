-- Enhanced Monitoring Views for Complete Activity Tracking
-- Covers SQL, Code, Git, NPM, and Dashboard operations

USE DATABASE CLAUDE_BI;
USE SCHEMA MCP;

-- =====================================================
-- View: All Claude Code Operations
-- =====================================================
CREATE OR REPLACE VIEW VW_CLAUDE_CODE_OPERATIONS AS
SELECT 
  event_id,
  occurred_at,
  action,
  session_id,
  attributes:sql_preview::STRING as sql_preview,
  attributes:sql_type::STRING as sql_type,
  attributes:execution_time_ms::NUMBER as execution_time_ms,
  attributes:success::BOOLEAN as success,
  attributes:rows_affected::NUMBER as rows_affected,
  attributes:error::STRING as error_message
FROM CLAUDE_BI.ACTIVITY.EVENTS
WHERE actor_id = 'CLAUDE_CODE_AI_AGENT'
  AND action LIKE 'ccode.%'
ORDER BY occurred_at DESC;

-- =====================================================
-- View: Claude Code Sessions
-- =====================================================
CREATE OR REPLACE VIEW VW_CLAUDE_CODE_SESSIONS AS
SELECT 
  session_id,
  MIN(occurred_at) as session_start,
  MAX(occurred_at) as session_end,
  TIMESTAMPDIFF(minute, MIN(occurred_at), MAX(occurred_at)) as duration_minutes,
  COUNT(*) as total_operations,
  SUM(CASE WHEN action = 'ccode.sql.executed' THEN 1 ELSE 0 END) as sql_count,
  SUM(CASE WHEN attributes:success = FALSE THEN 1 ELSE 0 END) as error_count,
  ARRAY_AGG(DISTINCT attributes:sql_type) as sql_types_used
FROM CLAUDE_BI.ACTIVITY.EVENTS
WHERE actor_id = 'CLAUDE_CODE_AI_AGENT'
  AND session_id IS NOT NULL
GROUP BY session_id
ORDER BY session_start DESC;

-- =====================================================
-- View: Daily Statistics
-- =====================================================
CREATE OR REPLACE VIEW VW_CLAUDE_CODE_DAILY_STATS AS
SELECT 
  DATE(occurred_at) as date,
  COUNT(DISTINCT session_id) as unique_sessions,
  COUNT(*) as total_operations,
  SUM(CASE WHEN attributes:sql_type LIKE 'DDL%' THEN 1 ELSE 0 END) as ddl_operations,
  SUM(CASE WHEN attributes:sql_type LIKE 'DML%' THEN 1 ELSE 0 END) as dml_operations,
  SUM(CASE WHEN attributes:sql_type = 'DQL_SELECT' THEN 1 ELSE 0 END) as select_queries,
  SUM(CASE WHEN attributes:success = FALSE THEN 1 ELSE 0 END) as failed_operations,
  AVG(attributes:execution_time_ms::NUMBER) as avg_execution_time_ms
FROM CLAUDE_BI.ACTIVITY.EVENTS
WHERE actor_id = 'CLAUDE_CODE_AI_AGENT'
  AND action = 'ccode.sql.executed'
GROUP BY DATE(occurred_at)
ORDER BY date DESC;

-- =====================================================
-- View: Code Activity (File Operations)
-- =====================================================
CREATE OR REPLACE VIEW VW_CODE_ACTIVITY AS
SELECT 
  event_id,
  occurred_at,
  action,
  object:type::STRING as object_type,
  object:id::STRING as object_id,
  attributes:size_bytes::NUMBER as file_size_bytes,
  attributes:editor::STRING as editor,
  attributes:filepath::STRING as filepath,
  session_id
FROM CLAUDE_BI.ACTIVITY.EVENTS
WHERE action LIKE 'code.%'
ORDER BY occurred_at DESC;

-- =====================================================
-- View: Git Commits
-- =====================================================
CREATE OR REPLACE VIEW VW_GIT_COMMITS AS
SELECT 
  object:id::STRING as commit_sha,
  occurred_at as commit_time,
  attributes:branch::STRING as branch,
  attributes:files_changed::NUMBER as files_changed,
  attributes:insertions::NUMBER as insertions,
  attributes:deletions::NUMBER as deletions,
  attributes:message_preview::STRING as message_preview,
  attributes:dedupe_key::STRING as dedupe_key
FROM CLAUDE_BI.ACTIVITY.EVENTS
WHERE action = 'git.commit'
ORDER BY occurred_at DESC;

-- =====================================================
-- View: Git Push Activity
-- =====================================================
CREATE OR REPLACE VIEW VW_GIT_PUSHES AS
SELECT 
  occurred_at as push_time,
  object:id::STRING as branch,
  attributes:remote::STRING as remote,
  attributes:commits_ahead::NUMBER as commits_pushed,
  attributes:from_sha::STRING as from_sha,
  attributes:to_sha::STRING as to_sha
FROM CLAUDE_BI.ACTIVITY.EVENTS
WHERE action = 'git.push'
ORDER BY occurred_at DESC;

-- =====================================================
-- View: Build/Test Results
-- =====================================================
CREATE OR REPLACE VIEW VW_BUILD_TEST_RESULTS AS
SELECT 
  DATE(occurred_at) as date,
  REPLACE(action, '.end', '') as operation,
  COUNT(*) as run_count,
  SUM(CASE WHEN attributes:exit_code = 0 THEN 1 ELSE 0 END) as success_count,
  SUM(CASE WHEN attributes:exit_code != 0 THEN 1 ELSE 0 END) as failure_count,
  ROUND(100.0 * SUM(CASE WHEN attributes:exit_code = 0 THEN 1 ELSE 0 END) / COUNT(*), 2) as success_rate
FROM CLAUDE_BI.ACTIVITY.EVENTS
WHERE action LIKE 'npm.%.end'
GROUP BY DATE(occurred_at), operation
ORDER BY date DESC, operation;

-- =====================================================
-- View: Complete Activity Timeline
-- =====================================================
CREATE OR REPLACE VIEW VW_ACTIVITY_TIMELINE AS
SELECT 
  occurred_at,
  action,
  CASE 
    WHEN action LIKE 'code.%' THEN 'Code'
    WHEN action LIKE 'git.%' THEN 'Git'
    WHEN action LIKE 'npm.%' THEN 'Build/Test'
    WHEN action LIKE 'ccode.sql%' THEN 'SQL'
    WHEN action LIKE 'dashboard.%' THEN 'Dashboard'
    WHEN action LIKE 'system.%' THEN 'System'
    ELSE 'Other'
  END as category,
  object:type::STRING as object_type,
  object:id::STRING as object_id,
  attributes,
  session_id,
  actor_id
FROM CLAUDE_BI.ACTIVITY.EVENTS
WHERE occurred_at > DATEADD(day, -7, CURRENT_TIMESTAMP())
ORDER BY occurred_at DESC;

-- =====================================================
-- View: Activity Summary by Category
-- =====================================================
CREATE OR REPLACE VIEW VW_ACTIVITY_SUMMARY_BY_CATEGORY AS
SELECT 
  DATE(occurred_at) as date,
  CASE 
    WHEN action LIKE 'code.%' THEN 'Code'
    WHEN action LIKE 'git.%' THEN 'Git'
    WHEN action LIKE 'npm.%' THEN 'Build/Test'
    WHEN action LIKE 'ccode.%' THEN 'SQL'
    WHEN action LIKE 'dashboard.%' THEN 'Dashboard'
    WHEN action LIKE 'system.%' THEN 'System'
    ELSE 'Other'
  END as category,
  COUNT(*) as event_count,
  COUNT(DISTINCT session_id) as unique_sessions
FROM CLAUDE_BI.ACTIVITY.EVENTS
WHERE occurred_at > DATEADD(day, -30, CURRENT_TIMESTAMP())
GROUP BY date, category
ORDER BY date DESC, category;

-- =====================================================
-- View: Recent Errors
-- =====================================================
CREATE OR REPLACE VIEW VW_RECENT_ERRORS AS
SELECT 
  occurred_at,
  action,
  attributes:error::STRING as error_message,
  attributes:sql_preview::STRING as sql_preview,
  attributes:exit_code::NUMBER as exit_code,
  session_id,
  actor_id
FROM CLAUDE_BI.ACTIVITY.EVENTS
WHERE (attributes:success = FALSE 
       OR attributes:error IS NOT NULL 
       OR attributes:exit_code > 0)
  AND occurred_at > DATEADD(day, -1, CURRENT_TIMESTAMP())
ORDER BY occurred_at DESC;

-- Grant permissions
GRANT SELECT ON ALL VIEWS IN SCHEMA MCP TO ROLE R_APP_READ;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA MCP TO ROLE R_APP_READ;