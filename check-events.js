const snowflake = require('snowflake-sdk');
require('dotenv').config();

const connection = snowflake.createConnection({
  account: process.env.SNOWFLAKE_ACCOUNT,
  username: process.env.SNOWFLAKE_USERNAME,
  password: process.env.SNOWFLAKE_PASSWORD,
  database: 'CLAUDE_BI',
  warehouse: 'CLAUDE_WAREHOUSE',
  schema: 'ACTIVITY'
});

connection.connect((err, conn) => {
  if (err) {
    console.error('Failed to connect:', err);
    process.exit(1);
  }
  
  console.log('Connected to Snowflake');
  
  // Get event summary
  const query = `
    SELECT 
      action,
      COUNT(*) as event_count,
      MIN(occurred_at) as earliest,
      MAX(occurred_at) as latest,
      ARRAY_AGG(DISTINCT actor_id) WITHIN GROUP (ORDER BY actor_id) as actors,
      ARRAY_AGG(DISTINCT source) WITHIN GROUP (ORDER BY source) as sources
    FROM CLAUDE_BI.ACTIVITY.EVENTS
    GROUP BY action
    ORDER BY event_count DESC, action
    LIMIT 20
  `;
  
  connection.execute({
    sqlText: query,
    complete: (err, stmt, rows) => {
      if (err) {
        console.error('Query failed:', err);
        connection.destroy();
        return;
      }
      
      console.log('\n=== Event Summary in ACTIVITY.EVENTS ===\n');
      console.log('Action                                    | Count | Sources');
      console.log('-'.repeat(70));
      
      rows.forEach(row => {
        const action = row.ACTION.padEnd(40);
        const count = String(row.EVENT_COUNT).padEnd(5);
        console.log(`${action} | ${count} | ${JSON.stringify(row.SOURCES)}`);
      });
      
      // Get total count
      connection.execute({
        sqlText: 'SELECT COUNT(*) as total FROM CLAUDE_BI.ACTIVITY.EVENTS',
        complete: (err2, stmt2, rows2) => {
          if (!err2 && rows2.length > 0) {
            console.log(`\nTotal events: ${rows2[0].TOTAL}`);
          }
          
          // Get sample of recent events
          connection.execute({
            sqlText: `
              SELECT 
                event_id,
                occurred_at,
                action,
                actor_id,
                object_type,
                object_id,
                source,
                _source_lane
              FROM CLAUDE_BI.ACTIVITY.EVENTS
              ORDER BY occurred_at DESC
              LIMIT 15
            `,
            complete: (err3, stmt3, rows3) => {
              if (!err3 && rows3.length > 0) {
                console.log('\n=== Most Recent 15 Events ===\n');
                console.log('Timestamp            | Action                     | Actor        | Object');
                console.log('-'.repeat(90));
                
                rows3.forEach(row => {
                  const timestamp = new Date(row.OCCURRED_AT).toISOString().slice(0, 19);
                  const action = (row.ACTION || '').slice(0, 25).padEnd(25);
                  const actor = (row.ACTOR_ID || '').slice(0, 12).padEnd(12);
                  const object = row.OBJECT_TYPE && row.OBJECT_ID ? 
                    `${row.OBJECT_TYPE}:${row.OBJECT_ID}`.slice(0, 20) : '';
                  
                  console.log(`${timestamp} | ${action} | ${actor} | ${object}`);
                });
                
                // Check for test events
                connection.execute({
                  sqlText: `
                    SELECT 
                      action,
                      COUNT(*) as count
                    FROM CLAUDE_BI.ACTIVITY.EVENTS
                    WHERE action LIKE 'test.%'
                       OR action LIKE 'stress.%'
                       OR _source_lane IN ('TEST', 'STRESS_TEST', '_RESTORE')
                    GROUP BY action
                    ORDER BY count DESC
                  `,
                  complete: (err4, stmt4, rows4) => {
                    if (!err4 && rows4.length > 0) {
                      console.log('\n=== Test/Stress Events ===\n');
                      rows4.forEach(row => {
                        console.log(`${row.ACTION}: ${row.COUNT}`);
                      });
                    }
                    
                    // Check for quality events
                    connection.execute({
                      sqlText: `
                        SELECT 
                          action,
                          COUNT(*) as count
                        FROM CLAUDE_BI.ACTIVITY.EVENTS
                        WHERE action LIKE 'quality.%'
                           OR action LIKE 'system.%'
                        GROUP BY action
                        ORDER BY count DESC
                      `,
                      complete: (err5, stmt5, rows5) => {
                        if (!err5 && rows5.length > 0) {
                          console.log('\n=== System/Quality Events ===\n');
                          rows5.forEach(row => {
                            console.log(`${row.ACTION}: ${row.COUNT}`);
                          });
                        }
                        
                        connection.destroy();
                        console.log('\nConnection closed.');
                      }
                    });
                  }
                });
              } else {
                connection.destroy();
              }
            }
          });
        }
      });
    }
  });
});