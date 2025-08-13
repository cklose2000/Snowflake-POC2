// Script to run Snowflake setup SQL
const snowflake = require('snowflake-sdk');
const fs = require('fs');
const path = require('path');
require('dotenv').config();

async function runSetup() {
  console.log('🚀 Running Snowflake setup...');
  
  // Create connection
  const connection = snowflake.createConnection({
    account: process.env.SNOWFLAKE_ACCOUNT,
    username: process.env.SNOWFLAKE_USERNAME,
    password: process.env.SNOWFLAKE_PASSWORD,
    database: process.env.SNOWFLAKE_DATABASE,
    schema: process.env.SNOWFLAKE_SCHEMA,
    warehouse: process.env.SNOWFLAKE_WAREHOUSE,
    role: process.env.SNOWFLAKE_ROLE
  });

  // Connect
  await new Promise((resolve, reject) => {
    connection.connect((err, conn) => {
      if (err) {
        console.error('❌ Failed to connect:', err.message);
        reject(err);
      } else {
        console.log('✅ Connected to Snowflake');
        resolve(conn);
      }
    });
  });

  // Read setup SQL
  const setupSQL = fs.readFileSync(
    path.join(__dirname, '..', 'infra', 'snowflake', 'setup.sql'),
    'utf8'
  );

  // Split SQL statements (simple split by semicolon and line break)
  const statements = setupSQL
    .split(/;\s*\n/)
    .filter(stmt => stmt.trim() && !stmt.trim().startsWith('--'))
    .map(stmt => stmt.trim() + ';');

  console.log(`📝 Found ${statements.length} SQL statements to execute`);

  // Execute each statement
  for (let i = 0; i < statements.length; i++) {
    const stmt = statements[i];
    
    // Skip comments
    if (stmt.trim().startsWith('--')) continue;
    
    console.log(`Executing statement ${i + 1}/${statements.length}...`);
    
    try {
      await new Promise((resolve, reject) => {
        connection.execute({
          sqlText: stmt,
          complete: (err, statement, rows) => {
            if (err) {
              console.error(`❌ Error in statement ${i + 1}:`, err.message);
              console.error('Statement:', stmt.substring(0, 100) + '...');
              reject(err);
            } else {
              console.log(`✅ Statement ${i + 1} executed successfully`);
              resolve(rows);
            }
          }
        });
      });
    } catch (error) {
      console.error(`⚠️  Continuing after error in statement ${i + 1}`);
      // Continue with next statement
    }
  }

  // Disconnect
  connection.destroy();
  console.log('🎉 Setup complete!');
}

runSetup().catch(console.error);