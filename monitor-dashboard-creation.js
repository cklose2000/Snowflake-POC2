#!/usr/bin/env node

// Monitor dashboard creation progress in real-time
// Shows what's happening when "Claude is thinking..."

const snowflake = require('snowflake-sdk');
require('dotenv').config();

async function monitorProgress() {
  console.log('🔍 Dashboard Creation Monitor\n');
  
  // Connect to Snowflake
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
      else resolve(conn);
    });
  });

  // Query for recent dashboard creation progress
  const progressSQL = `
    SELECT 
      ts,
      activity,
      customer,
      feature_json:creation_id::STRING as creation_id,
      feature_json:step::STRING as step,
      feature_json:error::STRING as error_message,
      feature_json:creation_time_ms::NUMBER as elapsed_ms,
      DATEDIFF('second', LAG(ts) OVER (PARTITION BY feature_json:creation_id ORDER BY ts), ts) as step_duration_sec
    FROM CLAUDE_BI.ACTIVITY.EVENTS
    WHERE activity LIKE 'ccode.dashboard_%'
      AND ts >= DATEADD('minute', -10, CURRENT_TIMESTAMP())
    ORDER BY ts DESC
    LIMIT 50
  `;

  const results = await new Promise((resolve, reject) => {
    connection.execute({
      sqlText: progressSQL,
      complete: (err, stmt, rows) => {
        if (err) reject(err);
        else resolve(rows);
      }
    });
  });

  // Group by creation ID
  const creations = {};
  results.forEach(row => {
    const id = row.CREATION_ID;
    if (!id) return;
    
    if (!creations[id]) {
      creations[id] = {
        id: id,
        customer: row.CUSTOMER,
        steps: [],
        startTime: null,
        lastActivity: row.TS,
        totalElapsed: 0,
        status: 'IN_PROGRESS'
      };
    }
    
    creations[id].steps.push({
      timestamp: row.TS,
      activity: row.ACTIVITY,
      step: row.STEP,
      duration: row.STEP_DURATION_SEC,
      elapsed: row.ELAPSED_MS,
      error: row.ERROR_MESSAGE
    });
    
    // Track earliest timestamp
    if (!creations[id].startTime || row.TS < creations[id].startTime) {
      creations[id].startTime = row.TS;
    }
    
    // Check for completion or failure
    if (row.ACTIVITY === 'ccode.dashboard_log_completion') {
      creations[id].status = 'COMPLETED';
      creations[id].totalElapsed = row.ELAPSED_MS;
    } else if (row.ACTIVITY === 'ccode.dashboard_creation_failed') {
      creations[id].status = 'FAILED';
      creations[id].error = row.ERROR_MESSAGE;
    }
  });

  // Display results
  console.log('=' .repeat(80));
  console.log('RECENT DASHBOARD CREATIONS (Last 10 minutes)');
  console.log('=' .repeat(80));

  Object.values(creations).forEach(creation => {
    const timeSinceLastActivity = (Date.now() - new Date(creation.lastActivity)) / 1000;
    
    // Determine actual status
    if (creation.status === 'IN_PROGRESS' && timeSinceLastActivity > 300) {
      creation.status = 'STUCK/TIMEOUT';
    }
    
    console.log(`\n📊 Creation ID: ${creation.id}`);
    console.log(`   Customer: ${creation.customer}`);
    console.log(`   Status: ${creation.status}`);
    console.log(`   Started: ${creation.startTime}`);
    console.log(`   Last Activity: ${creation.lastActivity} (${Math.round(timeSinceLastActivity)}s ago)`);
    
    if (creation.totalElapsed) {
      console.log(`   Total Time: ${(creation.totalElapsed / 1000).toFixed(1)}s`);
    }
    
    if (creation.error) {
      console.log(`   ❌ Error: ${creation.error}`);
    }
    
    // Show step progression
    console.log('\n   Steps Completed:');
    creation.steps.reverse().forEach((step, i) => {
      const icon = step.error ? '❌' : '✅';
      const duration = step.duration ? ` (+${step.duration}s)` : '';
      console.log(`   ${i+1}. ${icon} ${step.step || step.activity}${duration}`);
    });
    
    // Show what's likely happening
    if (creation.status === 'IN_PROGRESS') {
      const lastStep = creation.steps[creation.steps.length - 1];
      console.log(`\n   ⏳ Currently on: ${lastStep.step || 'unknown step'}`);
      
      // Predict next step
      const stepOrder = [
        'analyze_conversation',
        'generate_spec',
        'validate_spec',
        'preflight_checks',
        'create_objects',  // This is where it might get stuck!
        'generate_streamlit',
        'deploy_app',
        'log_completion'
      ];
      
      const currentIndex = stepOrder.indexOf(lastStep.step);
      if (currentIndex >= 0 && currentIndex < stepOrder.length - 1) {
        console.log(`   ⏭️ Next step: ${stepOrder[currentIndex + 1]}`);
      }
    }
  });

  // Check for WebSocket issues
  const wsCheckSQL = `
    SELECT COUNT(*) as ws_events
    FROM CLAUDE_BI.ACTIVITY.EVENTS
    WHERE ts >= DATEADD('minute', -1, CURRENT_TIMESTAMP())
      AND activity LIKE 'ccode.websocket_%'
  `;
  
  const wsResult = await new Promise((resolve, reject) => {
    connection.execute({
      sqlText: wsCheckSQL,
      complete: (err, stmt, rows) => {
        if (err) reject(err);
        else resolve(rows[0]);
      }
    });
  });

  console.log('\n' + '='.repeat(80));
  console.log('SYSTEM STATUS');
  console.log('='.repeat(80));
  console.log(`WebSocket Events (last minute): ${wsResult.WS_EVENTS}`);
  
  if (wsResult.WS_EVENTS === 0) {
    console.log('⚠️ No WebSocket activity - connection may be broken');
  }

  // Recommendations
  console.log('\n' + '='.repeat(80));
  console.log('RECOMMENDATIONS');
  console.log('='.repeat(80));
  
  const hasStuck = Object.values(creations).some(c => c.status === 'STUCK/TIMEOUT');
  if (hasStuck) {
    console.log('❗ Stuck dashboard creations detected. Possible causes:');
    console.log('   1. Snowflake object creation failed (check privileges)');
    console.log('   2. WebSocket disconnected (refresh browser)');
    console.log('   3. Server timeout (check integrated-server.js logs)');
    console.log('   4. Large dataset processing (check warehouse size)');
  }

  connection.destroy();
}

// Run monitor
monitorProgress().catch(console.error);