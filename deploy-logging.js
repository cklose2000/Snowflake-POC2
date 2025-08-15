/**
 * Deploy the missing logging procedures using Claude Code agent
 */

const SnowflakeSimpleClient = require('./snowflake-mcp-client/dist/simple-client').default;
const fs = require('fs');

async function deployLogging() {
  console.log('🚀 Deploying Logging Procedures with Claude Code Agent\n');
  
  const config = {
    account: 'uec18397.us-east-1',
    username: 'CLAUDE_CODE_AI_AGENT',
    privateKeyPath: './claude_code_rsa_key.p8',
    warehouse: 'CLAUDE_WAREHOUSE',
    database: 'CLAUDE_BI',
    schema: 'MCP'
  };
  
  const client = new SnowflakeSimpleClient(config);
  
  try {
    await client.connect();
    console.log('✅ Connected as CLAUDE_CODE_AI_AGENT\n');
    
    // Read the SQL file
    const sqlContent = fs.readFileSync('scripts/native-auth/04_logging_procedures.sql', 'utf8');
    
    // Split into individual statements
    const statements = sqlContent
      .split(/^--\s*============+/m)
      .filter(s => s.trim())
      .map(section => {
        // Extract SQL between $$ delimiters or regular statements
        const matches = section.match(/CREATE OR REPLACE.*?(?:;|(?:\$\$.*?\$\$;))/s);
        return matches ? matches[0] : null;
      })
      .filter(s => s);
    
    console.log(`Found ${statements.length} SQL statements to execute\n`);
    
    // Execute key statements
    const procedures = [
      {
        name: 'LOG_CLAUDE_EVENT',
        sql: `CREATE OR REPLACE PROCEDURE LOG_CLAUDE_EVENT(
  event_payload VARIANT,
  source_lane STRING DEFAULT 'CLAUDE_CODE'
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS $$
BEGIN
  -- ROLE GUARD - Require write permission
  IF (NOT IS_ROLE_IN_SESSION('R_APP_WRITE')) THEN
    RETURN OBJECT_CONSTRUCT(
      'ok', FALSE,
      'error', 'forbidden',
      'need_role', 'R_APP_WRITE',
      'current_role', CURRENT_ROLE()
    );
  END IF;

  -- VALIDATION
  IF (event_payload IS NULL OR NOT IS_OBJECT(event_payload)) THEN
    RETURN OBJECT_CONSTRUCT(
      'ok', FALSE,
      'error', 'invalid_payload'
    );
  END IF;

  -- EVENT ENRICHMENT
  LET final_event_id := COALESCE(
    event_payload:event_id::STRING,
    UUID_STRING()
  );
  
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
  
  enriched := OBJECT_INSERT(enriched, 'event_id', final_event_id, TRUE);
  
  enriched := CASE
    WHEN enriched:occurred_at IS NULL
    THEN OBJECT_INSERT(enriched, 'occurred_at', CURRENT_TIMESTAMP()::STRING, TRUE)
    ELSE enriched
  END;

  -- INSERT EVENT
  INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS (
    PAYLOAD,
    _SOURCE_LANE,
    _RECV_AT
  )
  SELECT 
    :enriched,
    :source_lane,
    CURRENT_TIMESTAMP();

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
$$`
      },
      {
        name: 'LOG_CLAUDE_EVENTS_BATCH',
        sql: `CREATE OR REPLACE PROCEDURE LOG_CLAUDE_EVENTS_BATCH(
  events ARRAY,
  source_lane STRING DEFAULT 'CLAUDE_CODE'
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS $$
DECLARE
  accepted INTEGER DEFAULT 0;
  rejected INTEGER DEFAULT 0;
  errors ARRAY DEFAULT ARRAY_CONSTRUCT();
BEGIN
  -- ROLE GUARD
  IF (NOT IS_ROLE_IN_SESSION('R_APP_WRITE')) THEN
    RETURN OBJECT_CONSTRUCT(
      'ok', FALSE,
      'error', 'forbidden',
      'need_role', 'R_APP_WRITE'
    );
  END IF;

  -- VALIDATION
  IF (events IS NULL OR ARRAY_SIZE(events) = 0) THEN
    RETURN OBJECT_CONSTRUCT(
      'ok', FALSE,
      'error', 'empty_batch'
    );
  END IF;
  
  IF (ARRAY_SIZE(events) > 1000) THEN
    RETURN OBJECT_CONSTRUCT(
      'ok', FALSE,
      'error', 'batch_too_large',
      'max_size', 1000,
      'provided', ARRAY_SIZE(events)
    );
  END IF;

  -- Process each event
  FOR i IN 0 TO ARRAY_SIZE(events) - 1 DO
    LET event := events[i];
    
    IF (event IS NULL OR NOT IS_OBJECT(event)) THEN
      rejected := rejected + 1;
      errors := ARRAY_APPEND(errors, OBJECT_CONSTRUCT(
        'index', i,
        'error', 'invalid_event'
      ));
      CONTINUE;
    END IF;
    
    LET enriched := OBJECT_INSERT(
      event,
      '_claude_meta',
      OBJECT_CONSTRUCT(
        'logged_at', CURRENT_TIMESTAMP(),
        'query_tag', CURRENT_QUERY_TAG(),
        'warehouse', CURRENT_WAREHOUSE(),
        'ip', CURRENT_IP_ADDRESS(),
        'batch_id', UUID_STRING(),
        'batch_index', i
      ),
      TRUE
    );
    
    enriched := CASE
      WHEN enriched:event_id IS NULL
      THEN OBJECT_INSERT(enriched, 'event_id', UUID_STRING(), TRUE)
      ELSE enriched
    END;
    
    enriched := CASE
      WHEN enriched:occurred_at IS NULL
      THEN OBJECT_INSERT(enriched, 'occurred_at', CURRENT_TIMESTAMP()::STRING, TRUE)
      ELSE enriched
    END;
    
    BEGIN
      INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS (
        PAYLOAD,
        _SOURCE_LANE,
        _RECV_AT
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

  RETURN OBJECT_CONSTRUCT(
    'ok', accepted > 0,
    'accepted', accepted,
    'rejected', rejected,
    'total', ARRAY_SIZE(events),
    'errors', CASE WHEN ARRAY_SIZE(errors) > 0 THEN errors ELSE NULL END
  );
END;
$$`
      }
    ];
    
    // Deploy each procedure
    for (const proc of procedures) {
      console.log(`📝 Creating ${proc.name}...`);
      try {
        const result = await client.executeSql(proc.sql);
        if (result.success) {
          console.log(`✅ ${proc.name} created successfully\n`);
        } else {
          console.log(`❌ Failed to create ${proc.name}: ${result.error}\n`);
        }
      } catch (error) {
        console.log(`❌ Error creating ${proc.name}: ${error.message}\n`);
      }
    }
    
    // Grant procedures to R_APP_WRITE
    console.log('🔐 Granting procedures to R_APP_WRITE...\n');
    const grants = [
      'GRANT USAGE ON PROCEDURE LOG_CLAUDE_EVENT(VARIANT, STRING) TO ROLE R_APP_WRITE',
      'GRANT USAGE ON PROCEDURE LOG_CLAUDE_EVENTS_BATCH(ARRAY, STRING) TO ROLE R_APP_WRITE'
    ];
    
    for (const grant of grants) {
      const result = await client.executeSql(grant);
      if (result.success) {
        console.log(`✅ ${grant}`);
      } else {
        console.log(`❌ Failed: ${grant}`);
      }
    }
    
    // Verify deployment
    console.log('\n✅ DEPLOYMENT COMPLETE\n');
    console.log('Verifying procedures exist...');
    
    const verify = await client.executeSql(`
      SELECT name 
      FROM CLAUDE_BI.INFORMATION_SCHEMA.PROCEDURES
      WHERE PROCEDURE_SCHEMA = 'MCP'
        AND name IN ('LOG_CLAUDE_EVENT', 'LOG_CLAUDE_EVENTS_BATCH')
    `);
    
    if (verify.success && verify.data) {
      console.log('Found procedures:', verify.data.map(r => r.NAME).join(', '));
    }
    
    console.log('\n🎯 Ready to test logging!');
    
  } catch (error) {
    console.error('❌ Error:', error);
  } finally {
    await client.disconnect();
  }
}

deployLogging().catch(console.error);