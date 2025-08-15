# Claude Code Activation Gateway

A minimal (100-line) service for secure token delivery to Claude Code users without exposing raw tokens.

## How It Works

1. Admin creates activation link via Snowflake procedure
2. User clicks activation link (e.g., `https://mcp.example.com/activate/act_abc123`)
3. Gateway validates activation code with Snowflake
4. Gateway receives token from Snowflake (user never sees it)
5. Gateway redirects to `claudecode://activate?token=...` deeplink
6. Claude Code stores token in OS keychain automatically

## Setup

### 1. Install Dependencies

```bash
npm install
```

### 2. Configure Environment

Copy `.env.example` to `.env` and configure:

```bash
cp .env.example .env
# Edit .env with your Snowflake credentials
```

### 3. Configure Snowflake

Set the gateway URL in Snowflake:

```sql
USE ROLE ACCOUNTADMIN;
ALTER SESSION SET MCP_CONFIG = OBJECT_CONSTRUCT(
  'activation_gateway_url', 'https://your-gateway.com/activate'
);
```

### 4. Run the Gateway

Development:
```bash
npm run dev
```

Production:
```bash
npm start
```

## Production Deployment

### Using PM2

```bash
# Install PM2
npm install -g pm2

# Start service
pm2 start index.js --name activation-gateway

# Save PM2 config
pm2 save
pm2 startup
```

### Using Docker

```dockerfile
FROM node:18-alpine
WORKDIR /app
COPY package*.json ./
RUN npm ci --only=production
COPY . .
EXPOSE 3000
USER node
CMD ["node", "index.js"]
```

```bash
docker build -t activation-gateway .
docker run -p 3000:3000 --env-file .env activation-gateway
```

### HTTPS Configuration

For production, use a reverse proxy (nginx/Caddy) or configure SSL:

```javascript
// Add to index.js for direct HTTPS
const https = require('https');
const fs = require('fs');

if (process.env.SSL_CERT_PATH && process.env.SSL_KEY_PATH) {
  const options = {
    cert: fs.readFileSync(process.env.SSL_CERT_PATH),
    key: fs.readFileSync(process.env.SSL_KEY_PATH)
  };
  https.createServer(options, app).listen(443);
}
```

## Security Features

- **Rate Limiting**: 10 activations per IP per 15 minutes
- **Helmet**: Security headers (CSP, HSTS, etc.)
- **Token Never Exposed**: User never sees raw token
- **One-Time Use**: Activation codes expire after use
- **Time Limited**: Activation codes expire after 30 minutes

## API Endpoints

### GET /activate/:code
Display activation page with confirmation button

### POST /activate/:code
Finalize activation and redirect to Claude Code

### GET /health
Health check for monitoring

## Monitoring

Check gateway health:
```bash
curl https://your-gateway.com/health
```

View activation logs in Snowflake:
```sql
SELECT * FROM ADMIN.PENDING_ACTIVATIONS
WHERE status = 'PENDING'
ORDER BY created_at DESC;
```

## Troubleshooting

### Activation fails with "code not found"
- Check code hasn't expired (30 minute default)
- Verify code hasn't been used already
- Check Snowflake connectivity

### Deeplink doesn't open Claude Code
- Ensure Claude Code is installed
- Register `claudecode://` protocol handler
- Check browser allows custom protocol handlers

### Rate limit exceeded
- Default: 10 attempts per 15 minutes per IP
- Adjust in `index.js` if needed
- Consider IP allowlisting for admins

## Support

For issues, check:
1. Gateway logs: `pm2 logs activation-gateway`
2. Snowflake audit: `SELECT * FROM CLAUDE_BI.ACTIVITY.EVENTS WHERE action LIKE 'system.activation.%'`
3. Network connectivity to Snowflake