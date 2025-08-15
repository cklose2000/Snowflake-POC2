/**
 * Minimal Activation Gateway for Claude Code
 * 
 * This 100-line service handles activation links and delivers tokens
 * to Claude Code without users ever seeing the raw token.
 */

const express = require('express');
const snowflake = require('snowflake-sdk');
const rateLimit = require('express-rate-limit');
const helmet = require('helmet');
require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 3000;

// Security headers
app.use(helmet());
app.use(express.json());

// Rate limiting
const limiter = rateLimit({
  windowMs: 15 * 60 * 1000, // 15 minutes
  max: 10, // limit each IP to 10 requests per windowMs
  message: 'Too many activation attempts, please try again later'
});
app.use('/activate', limiter);

// Snowflake connection
const snowflakeConn = snowflake.createConnection({
  account: process.env.SNOWFLAKE_ACCOUNT,
  username: process.env.GATEWAY_SERVICE_USER || 'MCP_SERVICE_USER',
  password: process.env.GATEWAY_SERVICE_PASSWORD,
  database: 'CLAUDE_BI',
  warehouse: 'MCP_XS_WH',
  schema: 'MCP',
  role: 'MCP_SERVICE_ROLE'
});

// Connect to Snowflake on startup
snowflakeConn.connect((err, conn) => {
  if (err) {
    console.error('Failed to connect to Snowflake:', err);
    process.exit(1);
  } else {
    console.log('Connected to Snowflake');
  }
});

/**
 * Execute Snowflake stored procedure
 */
async function executeProc(procName, params) {
  return new Promise((resolve, reject) => {
    snowflakeConn.execute({
      sqlText: `CALL ${procName}(?)`,
      binds: params,
      complete: (err, stmt, rows) => {
        if (err) {
          reject(err);
        } else {
          resolve(rows[0]);
        }
      }
    });
  });
}

/**
 * GET /activate/:code - Display activation page
 */
app.get('/activate/:code', async (req, res) => {
  const { code } = req.params;
  
  // Simple HTML page with activation button
  res.send(`
    <!DOCTYPE html>
    <html>
    <head>
      <title>Claude Code Activation</title>
      <style>
        body { font-family: system-ui; max-width: 600px; margin: 100px auto; padding: 20px; }
        .button { background: #007bff; color: white; padding: 12px 24px; border: none; 
                  border-radius: 6px; font-size: 16px; cursor: pointer; }
        .button:hover { background: #0056b3; }
        .code { font-family: monospace; background: #f4f4f4; padding: 4px 8px; border-radius: 4px; }
      </style>
    </head>
    <body>
      <h1>Activate Claude Code Access</h1>
      <p>You're about to activate Claude Code with activation code:</p>
      <p class="code">${code}</p>
      <p>Click the button below to complete activation. Claude Code will open automatically.</p>
      <form method="POST" action="/activate/${code}">
        <button type="submit" class="button">Activate Claude Code</button>
      </form>
    </body>
    </html>
  `);
});

/**
 * POST /activate/:code - Finalize activation and redirect
 */
app.post('/activate/:code', async (req, res) => {
  const { code } = req.params;
  
  try {
    // Call Snowflake procedure to finalize activation
    const result = await executeProc('ADMIN.FINALIZE_ACTIVATION', [code]);
    const response = JSON.parse(result.FINALIZE_ACTIVATION);
    
    if (response.success) {
      // Redirect to Claude Code deeplink
      // The token is in the URL but user never sees it - goes straight to app
      res.redirect(response.deeplink);
    } else {
      res.status(400).send(`
        <h1>Activation Failed</h1>
        <p>${response.error}</p>
        <p><a href="/">Go back</a></p>
      `);
    }
  } catch (error) {
    console.error('Activation error:', error);
    res.status(500).send(`
      <h1>Activation Error</h1>
      <p>An error occurred during activation. Please contact your administrator.</p>
    `);
  }
});

/**
 * GET /health - Health check endpoint
 */
app.get('/health', (req, res) => {
  res.json({ status: 'healthy', service: 'activation-gateway' });
});

// Start server
app.listen(PORT, () => {
  console.log(`Activation gateway running on port ${PORT}`);
  console.log(`Activation URLs will be: http://localhost:${PORT}/activate/[code]`);
});