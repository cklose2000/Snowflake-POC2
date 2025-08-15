#!/usr/bin/env node

/**
 * Simulate the activation flow locally
 * This mimics what the activation gateway would do
 */

const snowflake = require('snowflake-sdk');
require('dotenv').config();

// The activation code from the URL
const ACTIVATION_CODE = 'ACT_6CF4GIRFJ1C';

async function simulateActivation() {
  console.log('🔗 Simulating Activation Flow');
  console.log(`📋 Activation Code: ${ACTIVATION_CODE}\n`);

  // Connect to Snowflake
  const connection = snowflake.createConnection({
    account: process.env.SNOWFLAKE_ACCOUNT,
    username: process.env.SNOWFLAKE_USERNAME,
    password: process.env.SNOWFLAKE_PASSWORD,
    database: 'CLAUDE_BI',
    warehouse: process.env.SNOWFLAKE_WAREHOUSE || 'CLAUDE_WAREHOUSE'
  });

  try {
    await new Promise((resolve, reject) => {
      connection.connect((err) => {
        if (err) reject(err);
        else resolve();
      });
    });

    console.log('✅ Connected to Snowflake');

    // Step 1: Validate the activation code
    console.log('\n🔍 Step 1: Validating activation code...');
    
    const validationResult = await new Promise((resolve, reject) => {
      connection.execute({
        sqlText: `
          SELECT 
            payload:attributes:username::STRING as username,
            payload:attributes:allowed_tools::ARRAY as allowed_tools,
            payload:attributes:max_rows::NUMBER as max_rows,
            payload:attributes:daily_runtime_seconds::NUMBER as daily_runtime_seconds,
            payload:attributes:expires_at::TIMESTAMP_TZ as token_expires_at,
            payload:attributes:activation_expires_at::TIMESTAMP_TZ as activation_expires_at,
            CASE 
              WHEN payload:attributes:activation_expires_at::TIMESTAMP_TZ < CURRENT_TIMESTAMP() THEN 'EXPIRED'
              ELSE 'VALID'
            END as status
          FROM CLAUDE_BI.LANDING.RAW_EVENTS
          WHERE payload:action::STRING = 'system.activation.created'
            AND payload:attributes:activation_code::STRING = ?
          ORDER BY _recv_at DESC
          LIMIT 1
        `,
        binds: [ACTIVATION_CODE],
        complete: (err, stmt, rows) => {
          if (err) reject(err);
          else resolve(rows);
        }
      });
    });

    if (!validationResult.length) {
      console.log('❌ Activation code not found');
      return;
    }

    const activation = validationResult[0];
    console.log('✅ Activation found:', {
      username: activation.USERNAME,
      status: activation.STATUS,
      expires_at: activation.ACTIVATION_EXPIRES_AT
    });

    if (activation.STATUS === 'EXPIRED') {
      console.log('❌ Activation has expired');
      return;
    }

    // Step 2: Generate the user token
    console.log('\n🎫 Step 2: Generating user token...');
    
    const tokenId = Math.random().toString(36).substr(2, 16) + Math.random().toString(36).substr(2, 16);
    const token = `tk_${tokenId}_user_${activation.USERNAME}`;  // Ensure at least 40 chars
    console.log('✅ Generated token:', token.substring(0, 20) + '...');

    // Step 3: Store the token in Snowflake
    console.log('\n💾 Step 3: Storing token in Snowflake...');
    
    const tokenEvent = {
      event_id: `evt_${Math.random().toString(36).substr(2, 16)}`,
      action: 'system.token.created',
      actor_id: 'activation_gateway',
      object: {
        type: 'user_token',
        id: token
      },
      attributes: {
        username: activation.USERNAME,
        token_hash: 'SHA256_HASH_PLACEHOLDER', // In real system this would be hashed
        allowed_tools: activation.ALLOWED_TOOLS,
        max_rows: activation.MAX_ROWS,
        daily_runtime_seconds: activation.DAILY_RUNTIME_SECONDS,
        expires_at: activation.TOKEN_EXPIRES_AT,
        created_from_activation: ACTIVATION_CODE
      },
      occurred_at: new Date().toISOString()
    };

    await new Promise((resolve, reject) => {
      connection.execute({
        sqlText: "INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS SELECT PARSE_JSON(?), 'SYSTEM', ?",
        binds: [JSON.stringify(tokenEvent), new Date().toISOString()],
        complete: (err, stmt, rows) => {
          if (err) reject(err);
          else resolve(rows);
        }
      });
    });

    console.log('✅ Token stored in Snowflake');

    // Step 4: Mark activation as used
    console.log('\n✅ Step 4: Marking activation as used...');
    
    const usedEvent = {
      event_id: `evt_${Math.random().toString(36).substr(2, 16)}`,
      action: 'system.activation.used',
      actor_id: 'activation_gateway',
      object: {
        type: 'activation',
        id: ACTIVATION_CODE
      },
      attributes: {
        username: activation.USERNAME,
        used_at: new Date().toISOString(),
        token_created: token.substring(0, 20) + '...'
      },
      occurred_at: new Date().toISOString()
    };

    await new Promise((resolve, reject) => {
      connection.execute({
        sqlText: "INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS SELECT PARSE_JSON(?), 'SYSTEM', ?",
        binds: [JSON.stringify(usedEvent), new Date().toISOString()],
        complete: (err, stmt, rows) => {
          if (err) reject(err);
          else resolve(rows);
        }
      });
    });

    console.log('✅ Activation marked as used');

    // Step 5: Generate the deeplink (what would normally happen)
    console.log('\n🔗 Step 5: Generate deeplink for Claude Code...');
    
    const deeplink = `claudecode://activate?token=${token}&user=${activation.USERNAME}`;
    console.log(`✅ Deeplink: ${deeplink}`);

    console.log('\n🎉 Activation Complete!');
    console.log('\n📋 Summary:');
    console.log(`• User: ${activation.USERNAME}`);
    console.log(`• Token: ${token}`);
    console.log(`• Tools: ${JSON.stringify(activation.ALLOWED_TOOLS)}`);
    console.log(`• Max rows: ${activation.MAX_ROWS}`);
    console.log(`• Daily runtime: ${activation.DAILY_RUNTIME_SECONDS} seconds`);
    
    console.log('\n💻 Next step: Sarah would run this to store the token:');
    console.log(`cd snowflake-mcp-client && node dist/cli.js login --token "${token}"`);

  } catch (error) {
    console.error('❌ Error:', error.message);
  } finally {
    await new Promise((resolve) => {
      connection.destroy(() => resolve());
    });
  }
}

simulateActivation().catch(console.error);