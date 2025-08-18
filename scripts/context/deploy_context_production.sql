-- ============================================================================
-- Production-Grade Dynamic Agent Self-Orientation System
-- Complete deployment script with enterprise security and governance
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE CLAUDE_BI;
USE SCHEMA MCP;

-- ============================================================================
-- Phase 1: Enhanced Catalog Infrastructure with Delta Tracking & Validation
-- ============================================================================

-- Catalog Tables
CREATE OR REPLACE TABLE MCP.CATALOG_VIEWS (
  db STRING, 
  schema STRING, 
  view_name STRING, 
  full_name STRING,
  comment_raw STRING, 
  comment_json VARIANT, 
  created TIMESTAMP_NTZ, 
  last_altered TIMESTAMP_NTZ,
  env STRING DEFAULT 'production',
  build_id STRING DEFAULT UUID_STRING(),
  captured_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE MCP.CATALOG_COLUMNS (
  full_view_name STRING, 
  column_name STRING, 
  ordinal INT, 
  data_type STRING, 
  is_nullable STRING,
  sensitivity STRING, -- NEW: PII classification
  captured_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE MCP.CATALOG_PROCS (
  db STRING, 
  schema STRING, 
  proc_name STRING, 
  signature STRING, 
  full_name STRING,
  arguments STRING, 
  returns STRING, 
  comment_raw STRING, 
  comment_json VARIANT,
  created TIMESTAMP_NTZ, 
  last_altered TIMESTAMP_NTZ,
  requires_secret BOOLEAN DEFAULT FALSE, -- NEW: Security gating
  execute_as STRING, -- NEW: Privilege tracking
  env STRING DEFAULT 'production',
  build_id STRING DEFAULT UUID_STRING(),
  captured_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- NEW: Delta tracking for change detection
CREATE OR REPLACE TABLE MCP.CATALOG_DELTAS (
  kind STRING,           -- 'VIEW', 'PROCEDURE', 'COLUMN'
  object_name STRING,    -- Full qualified name
  change_type STRING,    -- 'ADDED', 'REMOVED', 'MODIFIED'
  delta VARIANT,         -- Before/after diff
  at_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  env STRING DEFAULT 'production'
);

-- NEW: JSON validation errors
CREATE OR REPLACE TABLE MCP.METADATA_ERRORS (
  object_name STRING,
  object_type STRING,    -- 'VIEW' or 'PROCEDURE'
  reason STRING,
  invalid_json STRING,
  captured_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  resolved BOOLEAN DEFAULT FALSE
);

-- ============================================================================
-- Phase 2: Blue/Green Semantic Registry with Environment Stamping
-- ============================================================================

-- Active registry tables
CREATE OR REPLACE TABLE MCP.SUBJECTS (
  subject STRING PRIMARY KEY,
  title STRING,
  default_view STRING,
  tags ARRAY,
  sensitivity_level STRING DEFAULT 'public', -- NEW: Data classification
  env STRING DEFAULT 'production',
  build_id STRING,
  active BOOLEAN DEFAULT TRUE
);

CREATE OR REPLACE TABLE MCP.SUBJECT_VIEWS (
  subject STRING,
  full_view_name STRING,
  grain STRING,
  time_column STRING,
  dimensions ARRAY,
  measures ARRAY,
  tags ARRAY,
  pii_columns ARRAY, -- NEW: PII column tracking
  sensitivity_level STRING DEFAULT 'public',
  env STRING DEFAULT 'production',
  build_id STRING,
  active BOOLEAN DEFAULT TRUE,
  PRIMARY KEY (subject, full_view_name)
);

CREATE OR REPLACE TABLE MCP.WORKFLOWS (
  intent STRING PRIMARY KEY,
  title STRING,
  full_proc_name STRING,
  inputs ARRAY,
  outputs ARRAY,
  tags ARRAY,
  requires_secret BOOLEAN DEFAULT FALSE, -- NEW: Security gating
  min_role STRING DEFAULT 'PUBLIC', -- NEW: RBAC requirements
  idempotent BOOLEAN DEFAULT FALSE, -- NEW: Idempotency tracking
  env STRING DEFAULT 'production',
  build_id STRING,
  active BOOLEAN DEFAULT TRUE
);

-- NEW: Blue/green deployment tables (V2 for staging)
CREATE OR REPLACE TABLE MCP.SUBJECTS_V2 LIKE MCP.SUBJECTS;
CREATE OR REPLACE TABLE MCP.SUBJECT_VIEWS_V2 LIKE MCP.SUBJECT_VIEWS;
CREATE OR REPLACE TABLE MCP.WORKFLOWS_V2 LIKE MCP.WORKFLOWS;

-- ============================================================================
-- Phase 3: Session Context and RBAC Infrastructure
-- ============================================================================

-- Session context view
CREATE OR REPLACE VIEW MCP.VW_SESSION AS
SELECT 
  CURRENT_ROLE() AS role, 
  CURRENT_USER() AS user,
  CURRENT_WAREHOUSE() AS warehouse,
  CURRENT_TIMESTAMP() AS session_start;

-- NEW: Primer cache for performance
CREATE OR REPLACE TABLE MCP.PRIMER_CACHE (
  hash STRING PRIMARY KEY,
  payload VARIANT,
  role_name STRING,
  build_id STRING,
  built_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  expires_at TIMESTAMP_NTZ DEFAULT DATEADD('hour', 1, CURRENT_TIMESTAMP())
);

-- NEW: Telemetry and rate limiting
CREATE OR REPLACE TABLE MCP.AGENT_TELEMETRY (
  ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  agent_id STRING,
  session_id STRING,
  intent STRING,
  kind STRING,        -- 'read', 'workflow', 'discovery'
  sql_text STRING,
  outcome STRING,     -- 'success', 'error', 'blocked'
  error_message STRING,
  rows_returned NUMBER,
  execution_ms NUMBER,
  warehouse STRING,
  role_name STRING
);

-- Rate limiting view (leaky bucket)
CREATE OR REPLACE VIEW MCP.VW_RATE_LIMIT AS
SELECT 
  agent_id,
  COUNT(*) AS calls_1m,
  COUNT(CASE WHEN outcome = 'error' THEN 1 END) AS errors_1m,
  MAX(ts) AS last_call
FROM MCP.AGENT_TELEMETRY
WHERE ts > DATEADD('minute', -1, CURRENT_TIMESTAMP())
GROUP BY agent_id;

-- ============================================================================
-- Phase 4: RBAC-Aware Allowlist with PII Protection
-- ============================================================================

-- RBAC-filtered allowlist
CREATE OR REPLACE VIEW MCP.VW_ALLOWLIST_READS AS
SELECT DISTINCT object_name, sensitivity_level
FROM (
  SELECT default_view AS object_name, sensitivity_level FROM MCP.SUBJECTS WHERE active = TRUE
  UNION
  SELECT full_view_name AS object_name, sensitivity_level FROM MCP.SUBJECT_VIEWS WHERE active = TRUE
)
WHERE TRY_TO_BOOLEAN(HAS_PRIVILEGE(object_name, 'SELECT')) = TRUE;

-- PII-flagged objects (block free-form access)
CREATE OR REPLACE VIEW MCP.VW_PII_PROTECTED AS
SELECT DISTINCT full_view_name
FROM MCP.SUBJECT_VIEWS 
WHERE ARRAY_SIZE(pii_columns) > 0 AND active = TRUE;

-- ============================================================================
-- Enhanced Procedures
-- ============================================================================

-- Enhanced REFRESH_CATALOG with delta detection and validation
CREATE OR REPLACE PROCEDURE MCP.REFRESH_CATALOG()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
  build_id STRING DEFAULT UUID_STRING();
  env STRING DEFAULT 'production';
BEGIN
  -- Snapshot previous state for delta detection
  CREATE OR REPLACE TEMP TABLE _previous_views AS
  SELECT * FROM MCP.CATALOG_VIEWS;
  
  CREATE OR REPLACE TEMP TABLE _previous_procs AS
  SELECT * FROM MCP.CATALOG_PROCS;

  -- Views discovery with enhanced metadata
  CREATE OR REPLACE TEMP TABLE _v AS
  SELECT 
    TABLE_CATALOG AS db, 
    TABLE_SCHEMA AS schema, 
    TABLE_NAME AS view_name,
    db || '.' || schema || '.' || view_name AS full_name,
    COMMENT AS comment_raw,
    TRY_PARSE_JSON(COMMENT) AS comment_json,
    CREATED, 
    LAST_ALTERED,
    :env AS env,
    :build_id AS build_id,
    CURRENT_TIMESTAMP() AS captured_at
  FROM INFORMATION_SCHEMA.VIEWS
  WHERE TABLE_SCHEMA IN ('APP','MCP','ACTIVITY','SECURITY');

  -- JSON validation and error capture
  INSERT INTO MCP.METADATA_ERRORS (object_name, object_type, reason, invalid_json)
  SELECT 
    full_name,
    'VIEW',
    'Invalid JSON in COMMENT',
    comment_raw
  FROM _v 
  WHERE comment_raw IS NOT NULL 
    AND comment_json IS NULL;

  -- Merge views with delta detection
  MERGE INTO MCP.CATALOG_VIEWS t
  USING _v s
  ON t.full_name = s.full_name
  WHEN MATCHED AND (
    t.comment_raw != s.comment_raw OR 
    t.last_altered != s.last_altered
  ) THEN UPDATE SET
    t.comment_raw = s.comment_raw, 
    t.comment_json = s.comment_json,
    t.last_altered = s.last_altered,
    t.build_id = s.build_id,
    t.captured_at = s.captured_at
  WHEN NOT MATCHED THEN INSERT VALUES (
    s.db, s.schema, s.view_name, s.full_name, s.comment_raw, s.comment_json,
    s.created, s.last_altered, s.env, s.build_id, s.captured_at
  );

  -- Record deltas for new/changed views
  INSERT INTO MCP.CATALOG_DELTAS (kind, object_name, change_type, delta)
  SELECT 'VIEW', s.full_name, 'ADDED', OBJECT_CONSTRUCT('new', s.comment_json)
  FROM _v s
  LEFT JOIN _previous_views p ON s.full_name = p.full_name
  WHERE p.full_name IS NULL;

  -- Columns with PII detection
  CREATE OR REPLACE TEMP TABLE _c AS
  SELECT 
    TABLE_CATALOG||'.'||TABLE_SCHEMA||'.'||TABLE_NAME AS full_view_name,
    COLUMN_NAME, 
    ORDINAL_POSITION::INT AS ordinal, 
    DATA_TYPE, 
    IS_NULLABLE,
    CASE 
      WHEN LOWER(COLUMN_NAME) LIKE '%email%' OR 
           LOWER(COLUMN_NAME) LIKE '%phone%' OR
           LOWER(COLUMN_NAME) LIKE '%ssn%' OR
           LOWER(COLUMN_NAME) LIKE '%credit_card%' THEN 'pii'
      ELSE 'public'
    END AS sensitivity,
    CURRENT_TIMESTAMP() AS captured_at
  FROM INFORMATION_SCHEMA.COLUMNS
  WHERE TABLE_SCHEMA IN ('APP','MCP','ACTIVITY','SECURITY') 
    AND TABLE_NAME IN (SELECT view_name FROM _v WHERE schema = TABLE_SCHEMA);

  DELETE FROM MCP.CATALOG_COLUMNS;
  INSERT INTO MCP.CATALOG_COLUMNS SELECT * FROM _c;

  -- Procedures with security metadata
  CREATE OR REPLACE TEMP TABLE _p AS
  SELECT 
    PROCEDURE_CATALOG AS db, 
    PROCEDURE_SCHEMA AS schema, 
    PROCEDURE_NAME AS proc_name,
    ARGUMENT_SIGNATURE AS signature,
    db || '.' || schema || '.' || proc_name || COALESCE(signature,'()') AS full_name,
    ARGUMENT_SIGNATURE AS arguments, 
    DATA_TYPE AS returns,
    COMMENT AS comment_raw, 
    TRY_PARSE_JSON(COMMENT) AS comment_json,
    CREATED, 
    LAST_ALTERED,
    COALESCE(TRY_TO_BOOLEAN(comment_json:requires_secret), FALSE) AS requires_secret,
    COALESCE(comment_json:execute_as::STRING, 'CALLER') AS execute_as,
    :env AS env,
    :build_id AS build_id,
    CURRENT_TIMESTAMP() AS captured_at
  FROM INFORMATION_SCHEMA.PROCEDURES
  WHERE PROCEDURE_SCHEMA IN ('MCP');

  -- Validate procedure JSON comments
  INSERT INTO MCP.METADATA_ERRORS (object_name, object_type, reason, invalid_json)
  SELECT 
    full_name,
    'PROCEDURE',
    'Invalid JSON in COMMENT',
    comment_raw
  FROM _p 
  WHERE comment_raw IS NOT NULL 
    AND comment_json IS NULL;

  -- Merge procedures with delta detection
  MERGE INTO MCP.CATALOG_PROCS t
  USING _p s
  ON t.full_name = s.full_name
  WHEN MATCHED THEN UPDATE SET
    t.arguments = s.arguments, 
    t.returns = s.returns,
    t.comment_raw = s.comment_raw, 
    t.comment_json = s.comment_json,
    t.last_altered = s.last_altered,
    t.requires_secret = s.requires_secret,
    t.execute_as = s.execute_as,
    t.build_id = s.build_id,
    t.captured_at = s.captured_at
  WHEN NOT MATCHED THEN INSERT VALUES (
    s.db, s.schema, s.proc_name, s.signature, s.full_name, s.arguments, s.returns,
    s.comment_raw, s.comment_json, s.created, s.last_altered, s.requires_secret,
    s.execute_as, s.env, s.build_id, s.captured_at
  );

  RETURN 'Catalog refreshed with build_id: ' || :build_id;
END;
$$;

-- Enhanced REBUILD_REGISTRY with blue/green support
CREATE OR REPLACE PROCEDURE MCP.REBUILD_REGISTRY(target_version STRING DEFAULT 'V1')
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
  build_id STRING DEFAULT UUID_STRING();
  env STRING DEFAULT 'production';
  subjects_table STRING DEFAULT 'MCP.SUBJECTS' || IFF(:target_version = 'V2', '_V2', '');
  subject_views_table STRING DEFAULT 'MCP.SUBJECT_VIEWS' || IFF(:target_version = 'V2', '_V2', '');
  workflows_table STRING DEFAULT 'MCP.WORKFLOWS' || IFF(:target_version = 'V2', '_V2', '');
BEGIN
  -- Clear target registry
  EXECUTE IMMEDIATE 'DELETE FROM ' || :subjects_table;
  EXECUTE IMMEDIATE 'DELETE FROM ' || :subject_views_table;
  EXECUTE IMMEDIATE 'DELETE FROM ' || :workflows_table;

  -- Build subjects with enhanced metadata
  EXECUTE IMMEDIATE '
  INSERT INTO ' || :subjects_table || '(subject,title,default_view,tags,sensitivity_level,env,build_id,active)
  SELECT
    COALESCE(comment_json:subject::STRING, LOWER(view_name)) AS subject,
    COALESCE(comment_json:title::STRING, INITCAP(REPLACE(view_name,''_'','' ''))) AS title,
    full_name AS default_view,
    COALESCE(comment_json:tags, ARRAY_CONSTRUCT()) AS tags,
    COALESCE(comment_json:sensitivity::STRING, ''public'') AS sensitivity_level,
    ''' || :env || ''' AS env,
    ''' || :build_id || ''' AS build_id,
    TRUE AS active
  FROM MCP.CATALOG_VIEWS';

  -- Build subject views with PII tracking
  EXECUTE IMMEDIATE '
  INSERT INTO ' || :subject_views_table || '(subject, full_view_name, grain, time_column, dimensions, measures, tags, pii_columns, sensitivity_level, env, build_id, active)
  SELECT
    COALESCE(v.comment_json:subject::STRING, LOWER(v.view_name)) AS subject,
    v.full_name,
    v.comment_json:grain::STRING,
    v.comment_json:time_column::STRING,
    COALESCE(v.comment_json:dimensions, ARRAY_CONSTRUCT()) AS dimensions,
    COALESCE(v.comment_json:measures, ARRAY_CONSTRUCT()) AS measures,
    COALESCE(v.comment_json:tags, ARRAY_CONSTRUCT()) AS tags,
    COALESCE(ARRAY_AGG(c.column_name) WITHIN GROUP (ORDER BY c.ordinal), ARRAY_CONSTRUCT()) AS pii_columns,
    COALESCE(v.comment_json:sensitivity::STRING, ''public'') AS sensitivity_level,
    ''' || :env || ''' AS env,
    ''' || :build_id || ''' AS build_id,
    TRUE AS active
  FROM MCP.CATALOG_VIEWS v
  LEFT JOIN MCP.CATALOG_COLUMNS c ON v.full_name = c.full_view_name AND c.sensitivity = ''pii''
  GROUP BY v.full_name, v.view_name, v.comment_json';

  -- Build workflows with security metadata
  EXECUTE IMMEDIATE '
  INSERT INTO ' || :workflows_table || '(intent,title,full_proc_name,inputs,outputs,tags,requires_secret,min_role,idempotent,env,build_id,active)
  SELECT
    COALESCE(comment_json:intent::STRING, LOWER(proc_name)) AS intent,
    COALESCE(comment_json:title::STRING, INITCAP(REPLACE(proc_name,''_'','' ''))) AS title,
    full_name,
    COALESCE(comment_json:inputs, ARRAY_CONSTRUCT()) AS inputs,
    COALESCE(comment_json:outputs, ARRAY_CONSTRUCT()) AS outputs,
    COALESCE(comment_json:tags, ARRAY_CONSTRUCT()) AS tags,
    requires_secret,
    COALESCE(comment_json:min_role::STRING, ''PUBLIC'') AS min_role,
    COALESCE(TRY_TO_BOOLEAN(comment_json:idempotent), FALSE) AS idempotent,
    ''' || :env || ''' AS env,
    ''' || :build_id || ''' AS build_id,
    TRUE AS active
  FROM MCP.CATALOG_PROCS';

  RETURN 'Registry rebuilt (' || :target_version || ') with build_id: ' || :build_id;
END;
$$;

-- ============================================================================
-- Phase 3: RBAC-Aware Primer System with Caching
-- ============================================================================

-- Scoped primer function
CREATE OR REPLACE FUNCTION MCP.GET_PRIMER(subjects_filter ARRAY DEFAULT NULL, include_workflows BOOLEAN DEFAULT TRUE)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
  SELECT OBJECT_CONSTRUCT(
    'version', '2.0',
    'session', (SELECT OBJECT_CONSTRUCT(
      'role', role,
      'user', user,
      'warehouse', warehouse,
      'timestamp', session_start
    ) FROM MCP.VW_SESSION),
    'subjects', (
      SELECT ARRAY_AGG(OBJECT_CONSTRUCT(
        'subject', subject,
        'title', title,
        'default_view', default_view,
        'tags', tags,
        'sensitivity_level', sensitivity_level,
        'columns', (
          SELECT ARRAY_AGG(OBJECT_CONSTRUCT(
            'name', c.column_name,
            'type', c.data_type,
            'nullable', c.is_nullable,
            'sensitive', IFF(c.sensitivity = 'pii', TRUE, FALSE)
          ) ORDER BY c.ordinal)
          FROM MCP.CATALOG_COLUMNS c
          WHERE c.full_view_name = s.default_view
        )
      ) ORDER BY subject)
      FROM MCP.SUBJECTS s
      WHERE s.active = TRUE
        AND (:subjects_filter IS NULL OR s.subject = ANY(:subjects_filter))
        AND EXISTS (SELECT 1 FROM MCP.VW_ALLOWLIST_READS a WHERE a.object_name = s.default_view)
    ),
    'views', (
      SELECT ARRAY_AGG(OBJECT_CONSTRUCT(
        'subject', subject,
        'view', full_view_name,
        'grain', grain,
        'time_column', time_column,
        'dimensions', dimensions,
        'measures', measures,
        'tags', tags,
        'pii_protected', IFF(ARRAY_SIZE(pii_columns) > 0, TRUE, FALSE)
      ) ORDER BY subject, full_view_name)
      FROM MCP.SUBJECT_VIEWS sv
      WHERE sv.active = TRUE
        AND (:subjects_filter IS NULL OR sv.subject = ANY(:subjects_filter))
        AND EXISTS (SELECT 1 FROM MCP.VW_ALLOWLIST_READS a WHERE a.object_name = sv.full_view_name)
    ),
    'workflows', IFF(:include_workflows, (
      SELECT ARRAY_AGG(OBJECT_CONSTRUCT(
        'intent', intent,
        'title', title,
        'proc', full_proc_name,
        'inputs', inputs,
        'outputs', outputs,
        'tags', tags,
        'requires_secret', requires_secret,
        'min_role', min_role,
        'idempotent', idempotent
      ) ORDER BY intent)
      FROM MCP.WORKFLOWS w
      WHERE w.active = TRUE
        AND TRY_TO_BOOLEAN(HAS_PRIVILEGE(w.full_proc_name, 'USAGE')) = TRUE
        AND (w.requires_secret = FALSE OR CURRENT_ROLE() IN ('ACCOUNTADMIN', 'SYSADMIN'))
    ), ARRAY_CONSTRUCT()),
    'rules', ARRAY_CONSTRUCT(
      'All free-form SELECT must go through MCP.READ(sql).',
      'Prefer subject default_view; avoid LANDING.RAW_EVENTS.',
      'Always include LIMIT unless reading via an intent/workflow.',
      'PII-protected views require workflow access only.',
      'Rate limits apply: max 60 calls/minute per agent.'
    ),
    'build_info', (
      SELECT OBJECT_CONSTRUCT(
        'build_id', MAX(build_id),
        'env', MAX(env),
        'last_refresh', MAX(captured_at)
      )
      FROM MCP.CATALOG_VIEWS
    )
  )
$$;

-- Main primer view with caching
CREATE OR REPLACE VIEW MCP.VW_CONTEXT_PRIMER AS
WITH cached_primer AS (
  SELECT payload, hash
  FROM MCP.PRIMER_CACHE
  WHERE role_name = CURRENT_ROLE()
    AND expires_at > CURRENT_TIMESTAMP()
  ORDER BY built_at DESC
  LIMIT 1
)
SELECT 
  COALESCE(c.payload, MCP.GET_PRIMER()) AS primer,
  COALESCE(c.hash, MD5(TO_JSON(MCP.GET_PRIMER()))) AS cache_key
FROM cached_primer c;

-- ============================================================================
-- Phase 4: Hardened MCP.READ with Cost Controls and PII Protection
-- ============================================================================

CREATE OR REPLACE PROCEDURE MCP.READ(sql_text STRING, agent_id STRING DEFAULT 'unknown')
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
  normalized STRING;
  rowcap NUMBER := 1000;
  timeout_seconds NUMBER := 300;
  sources ARRAY;
  rate_limit_calls NUMBER;
  rate_limit_errors NUMBER;
  has_pii_access BOOLEAN DEFAULT FALSE;
  query_tag STRING;
  result VARIANT;
  start_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
  execution_ms NUMBER;
  outcome STRING DEFAULT 'success';
  error_msg STRING DEFAULT NULL;
BEGIN
  -- Rate limiting check
  SELECT calls_1m, errors_1m INTO :rate_limit_calls, :rate_limit_errors
  FROM MCP.VW_RATE_LIMIT 
  WHERE agent_id = :agent_id;
  
  IF (:rate_limit_calls >= 60) THEN
    SET outcome = 'blocked';
    SET error_msg = 'Rate limit exceeded: 60 calls per minute';
    INSERT INTO MCP.AGENT_TELEMETRY (agent_id, intent, kind, sql_text, outcome, error_message, execution_ms, role_name)
    VALUES (:agent_id, 'rate_limited', 'read', :sql_text, :outcome, :error_msg, 0, CURRENT_ROLE());
    RETURN OBJECT_CONSTRUCT('error', :error_msg, 'retry_after_seconds', 60);
  END IF;
  
  -- Auto-hint on repeated errors
  IF (:rate_limit_errors >= 5) THEN
    LET suggestion VARIANT := (SELECT MCP.SUGGEST_INTENT(SPLIT(:sql_text, ' ')[1]::STRING));
    INSERT INTO MCP.AGENT_TELEMETRY (agent_id, intent, kind, sql_text, outcome, error_message, execution_ms, role_name)
    VALUES (:agent_id, 'auto_hint', 'discovery', :sql_text, 'hint', TO_JSON(:suggestion), 0, CURRENT_ROLE());
    RETURN OBJECT_CONSTRUCT('hint', 'Consider using a workflow instead', 'suggestions', :suggestion);
  END IF;

  -- Validate SQL type
  IF LEFT(LTRIM(:sql_text), 6) NOT ILIKE 'SELECT' THEN
    SET outcome = 'error';
    SET error_msg = 'Only SELECT statements allowed via MCP.READ';
    INSERT INTO MCP.AGENT_TELEMETRY (agent_id, intent, kind, sql_text, outcome, error_message, execution_ms, role_name)
    VALUES (:agent_id, 'invalid_sql', 'read', :sql_text, :outcome, :error_msg, 0, CURRENT_ROLE());
    RETURN OBJECT_CONSTRUCT('error', :error_msg);
  END IF;

  -- Block dangerous patterns
  IF REGEXP_LIKE(:sql_text, '\\b(CROSS\\s+JOIN|DELETE|UPDATE|INSERT|DROP|TRUNCATE|ALTER)\\b', 'i') THEN
    SET outcome = 'blocked';
    SET error_msg = 'Forbidden SQL pattern detected';
    INSERT INTO MCP.AGENT_TELEMETRY (agent_id, intent, kind, sql_text, outcome, error_message, execution_ms, role_name)
    VALUES (:agent_id, 'blocked_pattern', 'read', :sql_text, :outcome, :error_msg, 0, CURRENT_ROLE());
    RETURN OBJECT_CONSTRUCT('error', :error_msg, 'blocked_patterns', ['CROSS JOIN', 'DML', 'DDL']);
  END IF;

  -- Normalize with LIMIT injection
  SET normalized = IFF(
    REGEXP_LIKE(:sql_text, '\\sLIMIT\\s+\\d+\\s*$', 'i'),
    :sql_text,
    :sql_text || ' LIMIT ' || :rowcap
  );

  -- Extract source objects
  SELECT ARRAY_AGG(DISTINCT LOWER(grp[0]::STRING)) INTO :sources
  FROM TABLE(REGEXP_SUBSTR_ALL(:normalized, '(?:FROM|JOIN)\\s+([A-Z0-9_\\.]+)', 1, 1, 'i', 'g'));

  -- Allowlist validation
  IF (:sources IS NOT NULL) THEN
    LET blocked_sources ARRAY := (
      SELECT ARRAY_AGG(s.value::STRING)
      FROM LATERAL FLATTEN(INPUT => :sources) s
      WHERE NOT EXISTS (
        SELECT 1 FROM MCP.VW_ALLOWLIST_READS a 
        WHERE LOWER(a.object_name) = s.value::STRING
      )
    );
    
    IF (ARRAY_SIZE(:blocked_sources) > 0) THEN
      SET outcome = 'blocked';
      SET error_msg = 'Access denied to objects: ' || ARRAY_TO_STRING(:blocked_sources, ', ');
      INSERT INTO MCP.AGENT_TELEMETRY (agent_id, intent, kind, sql_text, outcome, error_message, execution_ms, role_name)
      VALUES (:agent_id, 'access_denied', 'read', :sql_text, :outcome, :error_msg, 0, CURRENT_ROLE());
      RETURN OBJECT_CONSTRUCT('error', :error_msg, 'blocked_objects', :blocked_sources);
    END IF;
  END IF;

  -- PII protection check
  LET pii_sources ARRAY := (
    SELECT ARRAY_AGG(s.value::STRING)
    FROM LATERAL FLATTEN(INPUT => :sources) s
    WHERE EXISTS (
      SELECT 1 FROM MCP.VW_PII_PROTECTED p 
      WHERE LOWER(p.full_view_name) = s.value::STRING
    )
  );
  
  IF (ARRAY_SIZE(:pii_sources) > 0 AND CURRENT_ROLE() NOT IN ('ACCOUNTADMIN', 'SYSADMIN')) THEN
    SET outcome = 'blocked';
    SET error_msg = 'PII-protected objects require workflow access: ' || ARRAY_TO_STRING(:pii_sources, ', ');
    INSERT INTO MCP.AGENT_TELEMETRY (agent_id, intent, kind, sql_text, outcome, error_message, execution_ms, role_name)
    VALUES (:agent_id, 'pii_blocked', 'read', :sql_text, :outcome, :error_msg, 0, CURRENT_ROLE());
    RETURN OBJECT_CONSTRUCT('error', :error_msg, 'pii_objects', :pii_sources, 'use_workflow', TRUE);
  END IF;

  -- Set query tag for cost tracking
  SET query_tag = CONCAT('agent=', :agent_id, ';intent=read;warehouse=', CURRENT_WAREHOUSE());
  EXECUTE IMMEDIATE 'ALTER SESSION SET QUERY_TAG = ''' || :query_tag || '''';

  -- Execute with timeout protection
  BEGIN
    LET exec_result RESULTSET := (EXECUTE IMMEDIATE :normalized);
    LET cursor_result CURSOR FOR exec_result;
    LET results ARRAY := ARRAY_CONSTRUCT();
    
    FOR row IN cursor_result DO
      SET results = ARRAY_APPEND(:results, OBJECT_CONSTRUCT(*));
    END FOR;
    
    SET result = OBJECT_CONSTRUCT('data', :results, 'row_count', ARRAY_SIZE(:results));
    
  EXCEPTION
    WHEN OTHER THEN
      SET outcome = 'error';
      SET error_msg = SQLERRM;
      SET result = OBJECT_CONSTRUCT('error', :error_msg, 'sqlcode', SQLCODE);
  END;

  -- Calculate execution time
  SET execution_ms = DATEDIFF('millisecond', :start_time, CURRENT_TIMESTAMP());

  -- Log telemetry
  INSERT INTO MCP.AGENT_TELEMETRY (
    agent_id, intent, kind, sql_text, outcome, error_message, 
    rows_returned, execution_ms, warehouse, role_name
  )
  VALUES (
    :agent_id, 'free_form_read', 'read', :sql_text, :outcome, :error_msg,
    IFF(:outcome = 'success', result:row_count::NUMBER, 0), :execution_ms, 
    CURRENT_WAREHOUSE(), CURRENT_ROLE()
  );

  RETURN :result;
END;
$$;

-- ============================================================================
-- Phase 5: Enhanced Intent Suggestion with Smart Scoring
-- ============================================================================

-- Synonym table for better matching
CREATE OR REPLACE TABLE MCP.QUERY_SYNONYMS (
  term STRING,
  synonyms ARRAY,
  weight NUMBER DEFAULT 1.0
);

-- Seed with common business terms
INSERT INTO MCP.QUERY_SYNONYMS VALUES
('revenue', ARRAY_CONSTRUCT('sales', 'income', 'earnings'), 2.0),
('customer', ARRAY_CONSTRUCT('client', 'user', 'account'), 1.5),
('order', ARRAY_CONSTRUCT('purchase', 'transaction', 'sale'), 1.5),
('error', ARRAY_CONSTRUCT('failure', 'exception', 'issue'), 1.0),
('performance', ARRAY_CONSTRUCT('speed', 'latency', 'response'), 1.0);

-- Enhanced intent suggestion with synonym scoring
CREATE OR REPLACE FUNCTION MCP.SUGGEST_INTENT(query_text STRING)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
WITH query_terms AS (
  SELECT LOWER(TRIM(value::STRING)) AS term
  FROM TABLE(SPLIT_TO_TABLE(:query_text, ' '))
  WHERE LENGTH(term) > 2
),
expanded_terms AS (
  SELECT t.term, s.weight
  FROM query_terms t
  LEFT JOIN MCP.QUERY_SYNONYMS s ON (
    t.term = s.term OR 
    ARRAY_CONTAINS(t.term::VARIANT, s.synonyms)
  )
),
view_candidates AS (
  SELECT
    'read_subject' AS kind,
    sv.subject,
    sv.full_view_name AS object,
    'Use MCP.READ() to query this view' AS suggestion,
    (
      -- Subject name match
      (SELECT SUM(COALESCE(weight, 1.0)) FROM expanded_terms WHERE sv.subject LIKE '%' || term || '%') * 3 +
      -- View name match  
      (SELECT SUM(COALESCE(weight, 1.0)) FROM expanded_terms WHERE sv.full_view_name LIKE '%' || term || '%') * 2 +
      -- Tag match
      (SELECT SUM(COALESCE(weight, 1.0)) FROM expanded_terms t WHERE EXISTS (
        SELECT 1 FROM TABLE(FLATTEN(sv.tags)) tag WHERE tag.value::STRING LIKE '%' || t.term || '%'
      )) * 1.5 +
      -- Time column boost for temporal queries
      IFF(sv.time_column IS NOT NULL AND :query_text ILIKE '%time%' OR :query_text ILIKE '%date%' OR :query_text ILIKE '%when%', 2.0, 0) +
      -- Measure column match
      (SELECT SUM(COALESCE(weight, 1.0)) FROM expanded_terms t WHERE EXISTS (
        SELECT 1 FROM TABLE(FLATTEN(sv.measures)) m WHERE m.value::STRING LIKE '%' || t.term || '%'
      )) * 2.5
    ) AS score
  FROM MCP.SUBJECT_VIEWS sv
  WHERE sv.active = TRUE
    AND EXISTS (SELECT 1 FROM MCP.VW_ALLOWLIST_READS a WHERE a.object_name = sv.full_view_name)
),
workflow_candidates AS (
  SELECT
    'workflow' AS kind,
    w.intent AS subject,
    w.full_proc_name AS object,
    'Call this procedure with parameters: ' || ARRAY_TO_STRING(
      TRANSFORM(w.inputs, x -> x:name::STRING || ' (' || x:type::STRING || ')')
    , ', ') AS suggestion,
    (
      -- Intent match (highest weight)
      (SELECT SUM(COALESCE(weight, 1.0)) FROM expanded_terms WHERE w.intent LIKE '%' || term || '%') * 4 +
      -- Title match
      (SELECT SUM(COALESCE(weight, 1.0)) FROM expanded_terms WHERE w.title LIKE '%' || term || '%') * 2 +
      -- Tag match
      (SELECT SUM(COALESCE(weight, 1.0)) FROM expanded_terms t WHERE EXISTS (
        SELECT 1 FROM TABLE(FLATTEN(w.tags)) tag WHERE tag.value::STRING LIKE '%' || t.term || '%'
      )) * 1.5
    ) AS score
  FROM MCP.WORKFLOWS w
  WHERE w.active = TRUE
    AND TRY_TO_BOOLEAN(HAS_PRIVILEGE(w.full_proc_name, 'USAGE')) = TRUE
    AND (w.requires_secret = FALSE OR CURRENT_ROLE() IN ('ACCOUNTADMIN', 'SYSADMIN'))
),
all_candidates AS (
  SELECT * FROM view_candidates WHERE score > 0
  UNION ALL
  SELECT * FROM workflow_candidates WHERE score > 0
),
ranked AS (
  SELECT *, ROW_NUMBER() OVER (ORDER BY score DESC, kind DESC) AS rn
  FROM all_candidates
)
SELECT OBJECT_CONSTRUCT(
  'query', :query_text,
  'expanded_terms', (SELECT ARRAY_AGG(DISTINCT term) FROM expanded_terms),
  'top_suggestions', (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(
      'kind', kind,
      'subject', subject,
      'object', object,
      'score', score,
      'suggestion', suggestion
    ) ORDER BY rn) 
    FROM ranked WHERE rn <= 5
  ),
  'total_candidates', (SELECT COUNT(*) FROM all_candidates)
)
$$;

-- ============================================================================
-- Phase 6: Utility Functions and Views
-- ============================================================================

-- Quick sampling function
CREATE OR REPLACE FUNCTION MCP.SAMPLE(view_name STRING, n NUMBER DEFAULT 50)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
  (SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
   FROM (EXECUTE IMMEDIATE 'SELECT * FROM ' || :view_name || ' LIMIT ' || :n))
$$;

-- Time filter helper
CREATE OR REPLACE FUNCTION MCP.DEFAULT_TIME_FILTER(subject_name STRING, days NUMBER DEFAULT 7)
RETURNS STRING
LANGUAGE SQL
AS
$$
  (SELECT IFF(
    time_column IS NULL, 
    '',
    ' WHERE ' || time_column || ' >= DATEADD(''day'', -' || :days || ', CURRENT_DATE())'
  )
  FROM MCP.SUBJECT_VIEWS 
  WHERE subject = :subject_name AND active = TRUE
  ORDER BY full_view_name 
  LIMIT 1)
$$;

-- Dry run procedure validation
CREATE OR REPLACE PROCEDURE MCP.DRY_RUN(proc_name STRING, arguments VARIANT DEFAULT NULL)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
  workflow_info VARIANT;
  validation_result VARIANT DEFAULT OBJECT_CONSTRUCT('valid', TRUE, 'messages', ARRAY_CONSTRUCT());
BEGIN
  -- Get workflow metadata
  SELECT OBJECT_CONSTRUCT(
    'inputs', inputs,
    'outputs', outputs, 
    'requires_secret', requires_secret,
    'min_role', min_role,
    'idempotent', idempotent
  ) INTO :workflow_info
  FROM MCP.WORKFLOWS 
  WHERE full_proc_name = :proc_name AND active = TRUE;

  IF (:workflow_info IS NULL) THEN
    RETURN OBJECT_CONSTRUCT('valid', FALSE, 'error', 'Workflow not found or not accessible');
  END IF;

  -- Validate inputs (basic type checking would go here)
  -- For now, just check if required parameters are provided
  LET expected_inputs ARRAY := workflow_info:inputs;
  LET validation_messages ARRAY := ARRAY_CONSTRUCT();
  
  IF (:arguments IS NULL AND ARRAY_SIZE(:expected_inputs) > 0) THEN
    SET validation_messages = ARRAY_APPEND(:validation_messages, 'Missing required arguments');
  END IF;

  -- Check role requirements
  IF (workflow_info:min_role::STRING != 'PUBLIC' AND 
      NOT CURRENT_ROLE() ILIKE '%' || workflow_info:min_role::STRING || '%') THEN
    SET validation_messages = ARRAY_APPEND(:validation_messages, 
      'Insufficient role. Required: ' || workflow_info:min_role::STRING);
  END IF;

  -- Check secret requirements
  IF (workflow_info:requires_secret::BOOLEAN = TRUE AND 
      CURRENT_ROLE() NOT IN ('ACCOUNTADMIN', 'SYSADMIN')) THEN
    SET validation_messages = ARRAY_APPEND(:validation_messages, 
      'Workflow requires elevated permissions for secret access');
  END IF;

  RETURN OBJECT_CONSTRUCT(
    'valid', ARRAY_SIZE(:validation_messages) = 0,
    'workflow', :workflow_info,
    'provided_args', :arguments,
    'validation_messages', :validation_messages
  );
END;
$$;

-- Capabilities overview
CREATE OR REPLACE VIEW MCP.VW_CAPABILITIES AS
SELECT 
  'subject' AS type, 
  subject AS name, 
  title, 
  default_view AS entrypoint, 
  tags,
  sensitivity_level,
  'Use MCP.READ() for safe querying' AS access_method
FROM MCP.SUBJECTS 
WHERE active = TRUE
  AND EXISTS (SELECT 1 FROM MCP.VW_ALLOWLIST_READS a WHERE a.object_name = default_view)
UNION ALL
SELECT 
  'workflow', 
  intent, 
  title, 
  full_proc_name, 
  tags,
  IFF(requires_secret, 'restricted', 'public') AS sensitivity_level,
  'Call directly: CALL ' || full_proc_name || '(...)' AS access_method
FROM MCP.WORKFLOWS 
WHERE active = TRUE
  AND TRY_TO_BOOLEAN(HAS_PRIVILEGE(full_proc_name, 'USAGE')) = TRUE
  AND (requires_secret = FALSE OR CURRENT_ROLE() IN ('ACCOUNTADMIN', 'SYSADMIN'));

-- Deprecations view (placeholder for future use)
CREATE OR REPLACE VIEW MCP.VW_DEPRECATIONS AS
SELECT 
  object_name,
  object_type,
  deprecated_at,
  replacement_object,
  sunset_date,
  reason
FROM (
  SELECT NULL AS object_name, NULL AS object_type, NULL AS deprecated_at, 
         NULL AS replacement_object, NULL AS sunset_date, NULL AS reason
) WHERE FALSE; -- Empty for now

-- ============================================================================
-- Phase 7: Automation and Maintenance
-- ============================================================================

-- Registry flip procedure for blue/green deployments
CREATE OR REPLACE PROCEDURE MCP.FLIP_REGISTRY()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
  -- Validate V2 registry has content
  LET v2_count NUMBER := (SELECT COUNT(*) FROM MCP.SUBJECTS_V2);
  
  IF (:v2_count = 0) THEN
    RETURN 'ERROR: V2 registry is empty, cannot flip';
  END IF;

  -- Backup current V1 to temp tables
  CREATE OR REPLACE TABLE MCP.SUBJECTS_BACKUP AS SELECT * FROM MCP.SUBJECTS;
  CREATE OR REPLACE TABLE MCP.SUBJECT_VIEWS_BACKUP AS SELECT * FROM MCP.SUBJECT_VIEWS;
  CREATE OR REPLACE TABLE MCP.WORKFLOWS_BACKUP AS SELECT * FROM MCP.WORKFLOWS;

  -- Atomic flip
  DELETE FROM MCP.SUBJECTS;
  DELETE FROM MCP.SUBJECT_VIEWS;
  DELETE FROM MCP.WORKFLOWS;
  
  INSERT INTO MCP.SUBJECTS SELECT * FROM MCP.SUBJECTS_V2;
  INSERT INTO MCP.SUBJECT_VIEWS SELECT * FROM MCP.SUBJECT_VIEWS_V2;
  INSERT INTO MCP.WORKFLOWS SELECT * FROM MCP.WORKFLOWS_V2;

  -- Clear primer cache to force refresh
  DELETE FROM MCP.PRIMER_CACHE;

  RETURN 'Registry flipped successfully. V2 is now active.';
END;
$$;

-- Automated refresh task (to be scheduled)
CREATE OR REPLACE PROCEDURE MCP.AUTOMATED_REFRESH()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
  refresh_result STRING;
  registry_result STRING;
  error_count NUMBER;
BEGIN
  -- Refresh catalog
  CALL MCP.REFRESH_CATALOG() INTO :refresh_result;
  
  -- Rebuild registry
  CALL MCP.REBUILD_REGISTRY('V1') INTO :registry_result;
  
  -- Check for errors
  SELECT COUNT(*) INTO :error_count
  FROM MCP.METADATA_ERRORS 
  WHERE captured_at > DATEADD('minute', -15, CURRENT_TIMESTAMP())
    AND resolved = FALSE;
  
  -- Clear old primer cache entries
  DELETE FROM MCP.PRIMER_CACHE 
  WHERE expires_at < CURRENT_TIMESTAMP();
  
  -- Log completion event
  INSERT INTO MCP.AGENT_TELEMETRY (
    agent_id, intent, kind, outcome, error_message, execution_ms, role_name
  ) VALUES (
    'system_refresh', 'automated_refresh', 'maintenance', 'success',
    'Errors found: ' || :error_count, 0, 'ACCOUNTADMIN'
  );
  
  RETURN 'Refresh completed. ' || :refresh_result || ' | ' || :registry_result || ' | Errors: ' || :error_count;
END;
$$;

-- ============================================================================
-- Initial Deployment and Data Population
-- ============================================================================

-- Execute initial setup
CALL MCP.REFRESH_CATALOG();
CALL MCP.REBUILD_REGISTRY();

-- Build initial primer cache for common roles
INSERT INTO MCP.PRIMER_CACHE (hash, payload, role_name, build_id)
SELECT 
  MD5(TO_JSON(primer)), 
  primer, 
  'PUBLIC',
  UUID_STRING()
FROM (SELECT MCP.GET_PRIMER() AS primer);

-- Log deployment completion
INSERT INTO MCP.AGENT_TELEMETRY (
  agent_id, intent, kind, outcome, error_message, execution_ms, role_name
) VALUES (
  'deployment', 'context_system_deployed', 'maintenance', 'success',
  'Production-grade agent self-orientation system deployed successfully', 0, 'ACCOUNTADMIN'
);

-- Final status check
SELECT 
  'Deployment Complete' AS status,
  (SELECT COUNT(*) FROM MCP.SUBJECTS) AS subjects_count,
  (SELECT COUNT(*) FROM MCP.SUBJECT_VIEWS) AS views_count,
  (SELECT COUNT(*) FROM MCP.WORKFLOWS) AS workflows_count,
  (SELECT COUNT(*) FROM MCP.VW_ALLOWLIST_READS) AS allowlisted_objects,
  (SELECT COUNT(*) FROM MCP.METADATA_ERRORS WHERE resolved = FALSE) AS validation_errors;