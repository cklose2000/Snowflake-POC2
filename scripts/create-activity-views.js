#!/usr/bin/env node

// Create Activity Views for Dashboard Factory
const snowflake = require('snowflake-sdk');
require('dotenv').config();

async function createActivityViews() {
  console.log('🚀 Creating Activity Views for Dashboard Factory\n');
  
  const connection = snowflake.createConnection({
    account: process.env.SNOWFLAKE_ACCOUNT,
    username: process.env.SNOWFLAKE_USERNAME,
    password: process.env.SNOWFLAKE_PASSWORD,
    role: process.env.SNOWFLAKE_ROLE,
    warehouse: process.env.SNOWFLAKE_WAREHOUSE,
    database: process.env.SNOWFLAKE_DATABASE,
    schema: process.env.SNOWFLAKE_SCHEMA
  });
  
  await new Promise((resolve, reject) => {
    connection.connect((err, conn) => {
      if (err) reject(err);
      else {
        console.log('✅ Connected to Snowflake\n');
        resolve(conn);
      }
    });
  });
  
  // Execute SQL helper
  const executeSQL = async (sql, description) => {
    return new Promise((resolve, reject) => {
      connection.execute({
        sqlText: sql,
        complete: (err, stmt, rows) => {
          if (err) {
            console.error(`❌ ${description}: ${err.message}`);
            resolve(false); // Continue on error
          } else {
            console.log(`✅ ${description}`);
            resolve(rows);
          }
        }
      });
    });
  };
  
  // Set context
  await executeSQL('USE DATABASE CLAUDE_BI', 'Set database context');
  await executeSQL('USE SCHEMA ACTIVITY_CCODE', 'Set schema context');
  
  // Create views
  const views = [
    {
      name: 'VW_ACTIVITY_COUNTS_24H',
      sql: `CREATE OR REPLACE VIEW ACTIVITY_CCODE.VW_ACTIVITY_COUNTS_24H AS
        SELECT
          e.activity,
          e.customer,
          COUNT(*) AS events_24h,
          MIN(e.ts) AS first_seen,
          MAX(e.ts) AS last_seen
        FROM CLAUDE_BI.ACTIVITY.EVENTS e
        WHERE e.ts >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
        GROUP BY e.activity, e.customer
        ORDER BY events_24h DESC`
    },
    {
      name: 'VW_LLM_TELEMETRY',
      sql: `CREATE OR REPLACE VIEW ACTIVITY_CCODE.VW_LLM_TELEMETRY AS
        SELECT
          e.customer,
          e.activity,
          e.ts,
          e.feature_json:model::STRING AS model,
          e.feature_json:prompt_tokens::NUMBER AS prompt_tokens,
          e.feature_json:completion_tokens::NUMBER AS completion_tokens,
          e.feature_json:total_tokens::NUMBER AS total_tokens,
          e.feature_json:latency_ms::NUMBER AS latency_ms,
          e.feature_json:template::STRING AS template_used,
          e._session_id AS session_id
        FROM CLAUDE_BI.ACTIVITY.EVENTS e
        WHERE e.activity IN ('ccode.user_asked', 'ccode.claude_responded', 'ccode.llm_invoked')
          AND e.ts >= DATEADD('day', -7, CURRENT_TIMESTAMP())
        ORDER BY e.ts DESC`
    },
    {
      name: 'VW_SQL_EXECUTIONS',
      sql: `CREATE OR REPLACE VIEW ACTIVITY_CCODE.VW_SQL_EXECUTIONS AS
        WITH sql_events AS (
          SELECT
            e.customer,
            e.ts,
            e.feature_json:query_id::STRING AS query_id,
            e._query_tag AS query_tag,
            e.feature_json:template::STRING AS template,
            e.feature_json:row_count::NUMBER AS row_count,
            e._session_id AS session_id
          FROM CLAUDE_BI.ACTIVITY.EVENTS e
          WHERE e.activity = 'ccode.sql_executed'
            AND e.ts >= DATEADD('day', -7, CURRENT_TIMESTAMP())
        )
        SELECT
          se.customer,
          se.ts,
          se.query_id,
          se.query_tag,
          se.template,
          se.row_count,
          se.session_id,
          NULL AS bytes_scanned,
          NULL AS duration_ms,
          NULL AS credits_used,
          TRUE AS success,
          NULL AS error_message
        FROM sql_events se
        ORDER BY se.ts DESC`
    },
    {
      name: 'VW_DASHBOARD_OPERATIONS',
      sql: `CREATE OR REPLACE VIEW ACTIVITY_CCODE.VW_DASHBOARD_OPERATIONS AS
        SELECT
          e.activity,
          e.customer,
          e.ts,
          e.link AS streamlit_url,
          e.feature_json:spec_id::STRING AS spec_id,
          e.feature_json:panels::NUMBER AS panel_count,
          e.feature_json:schedule::STRING AS schedule_mode,
          e.feature_json:creation_time_ms::NUMBER AS creation_time_ms,
          e.feature_json:error::STRING AS error_message,
          e._session_id AS session_id,
          LAG(e.ts) OVER (PARTITION BY e.feature_json:spec_id ORDER BY e.ts) AS previous_operation_ts,
          DATEDIFF('minute', LAG(e.ts) OVER (PARTITION BY e.feature_json:spec_id ORDER BY e.ts), e.ts) AS minutes_since_last_op
        FROM CLAUDE_BI.ACTIVITY.EVENTS e
        WHERE e.activity IN (
          'ccode.dashboard_created',
          'ccode.dashboard_refreshed',
          'ccode.dashboard_destroyed',
          'ccode.dashboard_failed'
        )
        ORDER BY e.ts DESC`
    },
    {
      name: 'VW_SAFESQL_TEMPLATES',
      sql: `CREATE OR REPLACE VIEW ACTIVITY_CCODE.VW_SAFESQL_TEMPLATES AS
        SELECT
          e.customer,
          e.ts,
          e.feature_json:template::STRING AS template,
          e.feature_json:params::VARIANT AS params,
          e.feature_json:row_count::NUMBER AS rows_returned,
          e.feature_json:execution_time_ms::NUMBER AS execution_time_ms,
          COUNT(*) OVER (
            PARTITION BY e.feature_json:template::STRING
            ORDER BY e.ts
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
          ) AS template_running_count
        FROM CLAUDE_BI.ACTIVITY.EVENTS e
        WHERE e.activity = 'ccode.sql_executed'
          AND e.feature_json:template IS NOT NULL
          AND e.ts >= DATEADD('day', -30, CURRENT_TIMESTAMP())
        ORDER BY e.ts DESC`
    },
    {
      name: 'VW_ACTIVITY_SUMMARY',
      sql: `CREATE OR REPLACE VIEW ACTIVITY_CCODE.VW_ACTIVITY_SUMMARY AS
        WITH hourly_stats AS (
          SELECT
            DATE_TRUNC('hour', ts) AS hour,
            COUNT(*) AS events_per_hour,
            COUNT(DISTINCT customer) AS unique_customers_per_hour,
            COUNT(DISTINCT activity) AS unique_activities_per_hour
          FROM CLAUDE_BI.ACTIVITY.EVENTS
          WHERE ts >= DATEADD('hour', -24, CURRENT_TIMESTAMP())
          GROUP BY 1
        )
        SELECT
          CURRENT_TIMESTAMP() AS as_of_time,
          (SELECT COUNT(*) FROM CLAUDE_BI.ACTIVITY.EVENTS WHERE ts >= DATEADD('hour', -24, CURRENT_TIMESTAMP())) AS total_events_24h,
          (SELECT COUNT(DISTINCT customer) FROM CLAUDE_BI.ACTIVITY.EVENTS WHERE ts >= DATEADD('hour', -24, CURRENT_TIMESTAMP())) AS unique_customers_24h,
          (SELECT COUNT(DISTINCT activity) FROM CLAUDE_BI.ACTIVITY.EVENTS WHERE ts >= DATEADD('hour', -24, CURRENT_TIMESTAMP())) AS unique_activities_24h,
          (SELECT MAX(events_per_hour) FROM hourly_stats) AS peak_events_per_hour,
          (SELECT hour FROM hourly_stats WHERE events_per_hour = (SELECT MAX(events_per_hour) FROM hourly_stats) LIMIT 1) AS peak_hour,
          (SELECT customer FROM CLAUDE_BI.ACTIVITY.EVENTS 
           WHERE ts >= DATEADD('hour', -24, CURRENT_TIMESTAMP()) 
           GROUP BY customer 
           ORDER BY COUNT(*) DESC 
           LIMIT 1) AS most_active_customer,
          (SELECT activity FROM CLAUDE_BI.ACTIVITY.EVENTS 
           WHERE ts >= DATEADD('hour', -24, CURRENT_TIMESTAMP()) 
           GROUP BY activity 
           ORDER BY COUNT(*) DESC 
           LIMIT 1) AS most_common_activity`
    }
  ];
  
  console.log('Creating Activity Views...\n');
  for (const view of views) {
    await executeSQL(view.sql, `Created ${view.name}`);
  }
  
  // Verify views
  console.log('\n📋 Verifying Views...\n');
  const verifyResult = await executeSQL(
    `SELECT VIEW_NAME FROM INFORMATION_SCHEMA.VIEWS 
     WHERE TABLE_SCHEMA = 'ACTIVITY_CCODE' 
     ORDER BY VIEW_NAME`,
    'List views'
  );
  
  if (verifyResult && verifyResult.length > 0) {
    console.log('Found views:');
    verifyResult.forEach(row => console.log(`  - ${row.VIEW_NAME}`));
  }
  
  connection.destroy();
  console.log('\n✅ Activity Views creation complete!');
}

createActivityViews().catch(console.error);