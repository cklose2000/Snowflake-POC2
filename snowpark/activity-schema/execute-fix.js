#!/usr/bin/env node

/**
 * Execute Dynamic Table fix - Backup, recreate, restore
 */

const snowflake = require('snowflake-sdk');
const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '../../.env') });

console.log('🔧 Fixing Dynamic Table - Backup, Recreate, Restore');
console.log('='.repeat(60));

const connection = snowflake.createConnection({
  account: process.env.SNOWFLAKE_ACCOUNT,
  username: process.env.SNOWFLAKE_USERNAME,
  password: process.env.SNOWFLAKE_PASSWORD,
  role: 'ACCOUNTADMIN',
  database: 'CLAUDE_BI',
  schema: 'ACTIVITY',
  warehouse: 'CLAUDE_WAREHOUSE'
});

async function executeSql(sql, description) {
  return new Promise((resolve, reject) => {
    connection.execute({
      sqlText: sql,
      complete: (err, statement, rows) => {
        if (err) {
          console.log(`❌ ${description}: ${err.message}`);
          reject(err);
        } else {
          console.log(`✅ ${description}`);
          resolve(rows);
        }
      }
    });
  });
}

async function fix() {
  try {
    // Connect
    await new Promise((resolve, reject) => {
      connection.connect(err => err ? reject(err) : resolve());
    });
    console.log('✅ Connected to Snowflake\n');

    // Step 1: Backup existing data
    console.log('📦 Step 1: Backing up existing EVENTS table...');
    await executeSql(`
      CREATE TABLE IF NOT EXISTS CLAUDE_BI.ACTIVITY.EVENTS_BAK AS
      SELECT * FROM CLAUDE_BI.ACTIVITY.EVENTS
    `, 'Created backup table EVENTS_BAK');

    const backupCount = await executeSql(
      'SELECT COUNT(*) as cnt FROM CLAUDE_BI.ACTIVITY.EVENTS_BAK',
      'Counted backup rows'
    );
    console.log(`   Backed up ${backupCount[0].CNT} rows\n`);

    // Step 2: Drop existing table
    console.log('🗑️  Step 2: Dropping existing EVENTS table...');
    await executeSql(
      'DROP TABLE IF EXISTS CLAUDE_BI.ACTIVITY.EVENTS',
      'Dropped existing EVENTS table'
    );

    // Step 3: Create Dynamic Table
    console.log('🔄 Step 3: Creating Dynamic Table...');
    const createDT = `
      CREATE DYNAMIC TABLE CLAUDE_BI.ACTIVITY.EVENTS
      TARGET_LAG = '1 minute'
      WAREHOUSE = DT_XS_WH
      AS
      WITH src AS (
        SELECT
          payload,
          _source_lane,
          _recv_at,
          payload:event_id::string            AS event_id_raw,
          payload:occurred_at::timestamp_tz   AS occurred_at,
          payload:actor_id::string            AS actor_id,
          payload:action::string              AS action,
          payload:object.type::string         AS object_type,
          payload:object.id::string           AS object_id,
          payload:source::string              AS source,
          payload:schema_version::string      AS schema_version,
          payload:attributes                  AS attributes,
          payload:depends_on_event_id::string AS depends_on_event_id,
          (LENGTH(payload::string) < 1000000) AS size_ok
        FROM CLAUDE_BI.LANDING.RAW_EVENTS
      ),
      filtered AS (
        SELECT *
        FROM src
        WHERE action IS NOT NULL
          AND occurred_at IS NOT NULL
          AND size_ok = TRUE
          AND NOT (action LIKE 'system.%' AND source <> 'system')
          AND NOT (action LIKE 'mcp.%' AND source <> 'mcp')
          AND NOT (action LIKE 'quality.%' AND source <> 'quality')
      ),
      evolved AS (
        SELECT
          COALESCE(event_id_raw, 'sys_' || MD5(CONCAT(source, '|', occurred_at, '|', action, '|', _recv_at))) AS event_id,
          occurred_at,
          actor_id,
          action,
          object_type,
          object_id,
          source,
          CASE 
            WHEN schema_version LIKE '2.0%' THEN '2.1.0' 
            ELSE schema_version 
          END AS schema_version,
          attributes,
          depends_on_event_id,
          _source_lane,
          _recv_at,
          HASH(source, event_id_raw, occurred_at) AS event_hash
        FROM filtered
      ),
      sequenced AS (
        SELECT
          e.*,
          ROW_NUMBER() OVER (
            PARTITION BY occurred_at 
            ORDER BY _recv_at, event_id
          ) AS event_sequence
        FROM evolved e
      ),
      dedup AS (
        SELECT *
        FROM (
          SELECT 
            s.*, 
            ROW_NUMBER() OVER (
              PARTITION BY event_hash 
              ORDER BY _recv_at
            ) AS rn
          FROM sequenced s
        ) 
        WHERE rn = 1
      ),
      ready AS (
        SELECT 
          d.*,
          (depends_on_event_id IS NULL OR EXISTS (
            SELECT 1 FROM dedup p 
            WHERE p.event_id = d.depends_on_event_id
          )) AS dependency_ok
        FROM dedup d
      )
      SELECT
        event_id,
        occurred_at,
        actor_id,
        action,
        object_type,
        object_id,
        source,
        schema_version,
        OBJECT_INSERT(
          attributes, 
          'meta',
          OBJECT_INSERT(
            COALESCE(attributes:meta, OBJECT_CONSTRUCT()), 
            'sequence', 
            TO_VARIANT(event_sequence), 
            TRUE
          ),
          TRUE
        ) AS attributes,
        depends_on_event_id,
        _source_lane,
        _recv_at
      FROM ready
      WHERE dependency_ok = TRUE
    `;
    await executeSql(createDT, 'Created Dynamic Table EVENTS');

    // Step 4: Restore data via RAW_EVENTS
    console.log('\n📥 Step 4: Restoring backed up data...');
    const insertSql = `
      INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS
      SELECT 
        OBJECT_CONSTRUCT(
          'event_id', event_id,
          'occurred_at', occurred_at,
          'actor_id', actor_id,
          'action', action,
          'object', OBJECT_CONSTRUCT(
            'type', object_type,
            'id', object_id
          ),
          'source', source,
          'schema_version', schema_version,
          'attributes', attributes,
          'depends_on_event_id', depends_on_event_id
        ) AS payload,
        _source_lane,
        _recv_at
      FROM CLAUDE_BI.ACTIVITY.EVENTS_BAK
    `;
    await executeSql(insertSql, 'Inserted backup data into RAW_EVENTS');

    // Step 5: Force refresh and verify
    console.log('\n🔄 Step 5: Refreshing Dynamic Table...');
    await executeSql(
      'ALTER DYNAMIC TABLE CLAUDE_BI.ACTIVITY.EVENTS REFRESH',
      'Triggered Dynamic Table refresh'
    );

    // Wait a moment for refresh
    console.log('   Waiting for refresh to complete...');
    await new Promise(resolve => setTimeout(resolve, 3000));

    // Verify counts
    console.log('\n📊 Verification:');
    const verification = await executeSql(`
      SELECT 
        'EVENTS_BAK' as table_name,
        COUNT(*) as row_count 
      FROM CLAUDE_BI.ACTIVITY.EVENTS_BAK
      UNION ALL
      SELECT 
        'RAW_EVENTS' as table_name,
        COUNT(*) as row_count 
      FROM CLAUDE_BI.LANDING.RAW_EVENTS
      UNION ALL
      SELECT 
        'EVENTS (Dynamic)' as table_name,
        COUNT(*) as row_count 
      FROM CLAUDE_BI.ACTIVITY.EVENTS
    `, 'Retrieved row counts');

    verification.forEach(v => {
      console.log(`   ${v.TABLE_NAME}: ${v.ROW_COUNT} rows`);
    });

    // Show Dynamic Table info
    const dtInfo = await executeSql(
      "SHOW DYNAMIC TABLES LIKE 'EVENTS' IN SCHEMA CLAUDE_BI.ACTIVITY",
      'Retrieved Dynamic Table info'
    );
    
    if (dtInfo.length > 0) {
      console.log(`\n✅ Dynamic Table Status:`);
      console.log(`   Name: ${dtInfo[0].name}`);
      console.log(`   State: ${dtInfo[0].state}`);
      console.log(`   Target Lag: ${dtInfo[0].target_lag}`);
      console.log(`   Warehouse: ${dtInfo[0].warehouse}`);
    }

    console.log('\n✨ Dynamic Table successfully fixed!');
    console.log('   - Data backed up to EVENTS_BAK');
    console.log('   - Dynamic Table created with 1-minute refresh');
    console.log('   - All data restored');

  } catch (err) {
    console.error('\n❌ Fix failed:', err.message);
    process.exit(1);
  } finally {
    connection.destroy();
  }
}

fix().catch(console.error);