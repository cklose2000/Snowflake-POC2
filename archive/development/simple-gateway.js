#!/usr/bin/env node

/**
 * Simple local activation gateway for testing
 * Handles activation URLs on localhost:3000
 */

const express = require('express');
const app = express();
const PORT = 3000;

app.use(express.json());

// Handle activation requests
app.get('/activate/:activationCode', async (req, res) => {
  const { activationCode } = req.params;
  
  console.log(`🔗 Activation request for: ${activationCode}`);
  
  try {
    // For demo, just generate a token and show the deeplink
    const tokenId = Math.random().toString(36).substr(2, 16) + Math.random().toString(36).substr(2, 16);
    const token = `tk_${tokenId}_user_ck_dev_test1`;
    
    console.log(`✅ Generated token: ${token}`);
    
    // Create the deeplink
    const deeplink = `claudecode://activate?token=${token}&user=ck_dev_test1`;
    
    // Return a simple HTML page that triggers the deeplink
    const html = `
    <!DOCTYPE html>
    <html>
    <head>
        <title>Claude Code Activation</title>
        <style>
            body { font-family: -apple-system, sans-serif; max-width: 600px; margin: 50px auto; padding: 20px; }
            .success { background: #d4edda; border: 1px solid #c3e6cb; padding: 15px; border-radius: 5px; margin: 20px 0; }
            .token { background: #f8f9fa; padding: 10px; border-radius: 3px; font-family: monospace; }
            .deeplink { background: #007bff; color: white; padding: 12px 20px; text-decoration: none; border-radius: 5px; display: inline-block; margin: 10px 0; }
        </style>
    </head>
    <body>
        <h1>🎉 Claude Code Activation Successful!</h1>
        
        <div class="success">
            <strong>Activation Complete!</strong><br>
            User: ck_dev_test1<br>
            Permissions: ANALYST role<br>
            Max rows: 10,000 per query<br>
            Daily runtime: 2 hours
        </div>
        
        <h3>Your Token:</h3>
        <div class="token">${token}</div>
        
        <h3>Next Steps:</h3>
        <p>1. Click the button below to launch Claude Code automatically:</p>
        <a href="${deeplink}" class="deeplink">🚀 Launch Claude Code</a>
        
        <p>2. Or manually run this command in terminal:</p>
        <div class="token">cd snowflake-mcp-client && node dist/cli.js login --token "${token}"</div>
        
        <p>3. Start querying your data:</p>
        <div class="token">"Show me sales data for the last 30 days"</div>
        
        <script>
        // Auto-redirect to deeplink after 3 seconds
        setTimeout(() => {
            window.location.href = '${deeplink}';
        }, 3000);
        </script>
    </body>
    </html>`;
    
    res.send(html);
    
  } catch (error) {
    console.error('❌ Activation failed:', error.message);
    res.status(500).send(`
      <h1>❌ Activation Failed</h1>
      <p>Error: ${error.message}</p>
      <p>Please contact your admin for a new activation link.</p>
    `);
  }
});

// Health check
app.get('/', (req, res) => {
  res.send(`
    <h1>Claude Code Activation Gateway</h1>
    <p>Status: ✅ Running</p>
    <p>Activation URLs: http://localhost:3000/activate/[code]</p>
  `);
});

app.listen(PORT, () => {
  console.log(`🚀 Simple activation gateway running on http://localhost:${PORT}`);
  console.log(`📋 Ready to handle activation URLs like:`);
  console.log(`   http://localhost:${PORT}/activate/ACT_FEJ6NIZZGNC`);
  console.log(`\n✅ Sarah can now use this URL instead of mcp.company.com`);
});