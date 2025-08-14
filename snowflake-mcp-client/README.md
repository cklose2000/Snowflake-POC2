# Snowflake MCP Thin Client

A production-ready thin client for secure Snowflake integration with Claude Code using token-based authentication.

## 🔑 Key Features

- **Key-Pair Authentication**: Service account uses private key authentication (no passwords)
- **Token-Based Access**: Users authenticate with secure tokens stored in OS keychain
- **Server-Side Security**: Token hashing with pepper, budget enforcement, rate limiting
- **Operational Robustness**: Retry logic, session management, comprehensive error handling
- **Observability**: Query tagging, request logging, usage tracking

## 🚀 Quick Start

### 1. Install Dependencies
```bash
npm install
npm run build
```

### 2. Set Environment Variables
```bash
export SNOWFLAKE_ACCOUNT="uec18397.us-east-1"
export MCP_SERVICE_USER="MCP_SERVICE_USER"
export SF_PK_PATH="/path/to/service/private/key.pem"
export MCP_SERVICE_ROLE="MCP_SERVICE_ROLE"
export MCP_SERVICE_WAREHOUSE="MCP_XS_WH"
export SNOWFLAKE_DATABASE="CLAUDE_BI"
```

### 3. Deploy Enhanced Procedures
```bash
# Deploy the enhanced stored procedures
snowsql -f ../snowpark/activity-schema/28_enhanced_procedures.sql
```

### 4. Login with Token
```bash
# Get token from admin and login
node dist/cli.js login
```

### 5. Test Integration
```bash
# Comprehensive test suite
node dist/cli.js test --verbose
```

## 📋 CLI Commands

### Authentication
```bash
# Login with token (prompts securely)
snowflake-mcp login

# Login with token parameter (not recommended)
snowflake-mcp login --token tk_abc123...

# Check authentication status
snowflake-mcp status

# Remove stored token
snowflake-mcp logout
```

### Testing
```bash
# Run comprehensive test suite
snowflake-mcp test

# Verbose test output
snowflake-mcp test --verbose

# Test specific query
snowflake-mcp query "Show me recent user activity"
snowflake-mcp query "List sales data" --limit 50
```

### Setup
```bash
# Setup service account credentials
snowflake-mcp setup
```

## 🔧 Architecture

### Security Model
```
User (Sarah) → Token (tk_abc123) → Thin Client → Service Account → Procedures → Events
```

### Key Components

1. **Service Account**: Single account with key-pair auth for all database operations
2. **User Tokens**: Individual tokens with permissions, stored in OS keychain
3. **Stored Procedures**: All business logic server-side with budget enforcement
4. **Event Logging**: Complete audit trail in Activity Schema

### Token Flow
1. Admin creates user activation: `CALL ADMIN.CREATE_ACTIVATION('sarah', 'email')`
2. User clicks activation URL → gets deeplink with token
3. CLI stores token in OS keychain (account-scoped)
4. Every request includes token → server validates → enforces limits

## 📊 Usage Tracking

### Budget Enforcement
- **Daily Runtime Limits**: Configurable per user (e.g., 2 hours/day)
- **Row Limits**: Per-query limits (e.g., 10K rows max)
- **Tool Access**: Granular permissions (VIEWER/ANALYST/ADMIN)

### Observability
```sql
-- Monitor client adoption
SELECT * FROM MCP.CLIENT_ADOPTION;

-- Check user activity
SELECT * FROM CLAUDE_BI.ACTIVITY.EVENTS 
WHERE action LIKE 'mcp.%' 
ORDER BY occurred_at DESC;
```

## 🔐 Security Features

### Server-Side Token Hashing
```sql
-- Tokens are hashed with pepper server-side
SELECT SHA2(token || pepper, 256) FROM ...
```

### Account-Scoped Storage
```typescript
// Tokens stored per Snowflake account
const serviceName = `SnowflakeMCP:${account}`;
await keytar.setPassword(serviceName, 'user_token', token);
```

### Circuit Breaker
- Retry logic for transient failures
- Session keep-alive configuration  
- Query timeouts and statement limits

## 📚 API Reference

### Core Client Class
```typescript
import { SnowflakeMCPClient } from '@growthzone/snowflake-mcp-client';

const client = new SnowflakeMCPClient();

// Natural language queries
const result = await client.query('Show me sales data');

// List available sources
const sources = await client.listSources(true);

// Validate query plans
const validation = await client.validatePlan(queryParams);

// Log development events
await client.logEvent('analysis.completed', { rows: 1000 });

// Test connection
await client.test();
```

### Tool Methods
```typescript
// Available tools (based on user permissions)
await client.call('compose_query_plan', { intent_text: 'sales data' });
await client.call('list_sources', { include_columns: true });
await client.call('validate_plan', queryParams);
await client.call('create_dashboard', { title: 'Sales Dashboard' });
await client.call('get_user_status', {});
```

## 🛠️ Development

### Build and Test
```bash
# Build TypeScript
npm run build

# Run CLI directly
node dist/cli.js --help

# Test without installing
node dist/cli.js test
```

### Environment Setup
```bash
# Development with embedded fallbacks
export SNOWFLAKE_MCP_TOKEN="tk_dev_token_here"

# Production with keychain
snowflake-mcp login
```

## 🚨 Troubleshooting

### Common Issues

**❌ "No MCP token found"**
```bash
# Solution: Login with token
snowflake-mcp login
```

**❌ "Token validation failed"**
```bash
# Check token hasn't expired
snowflake-mcp status

# Get new token from admin
snowflake-mcp login
```

**❌ "Service connection failed"**
```bash
# Check environment variables
echo $SNOWFLAKE_ACCOUNT
echo $SF_PK_PATH

# Verify private key file
ls -la $SF_PK_PATH

# Test with setup command
snowflake-mcp setup
```

**❌ "Access denied to tool"**
```bash
# Check user permissions
snowflake-mcp test --verbose

# Admin needs to grant tool access
CALL ADMIN.UPDATE_USER_PERMISSIONS('username', ['compose_query', 'list_sources']);
```

### Debug Mode
```bash
# Verbose output
snowflake-mcp test --verbose

# Check current status
snowflake-mcp status

# Test specific query
snowflake-mcp query "test query" --limit 10
```

## 📈 Monitoring

### Client Health
```sql
-- Query performance
SELECT 
  tool,
  AVG(attributes:execution_time_ms::number) as avg_latency_ms,
  COUNT(*) as requests
FROM CLAUDE_BI.ACTIVITY.EVENTS
WHERE action = 'mcp.request'
  AND DATE(occurred_at) = CURRENT_DATE()
GROUP BY tool;
```

### User Activity
```sql  
-- Daily active users
SELECT 
  DATE(occurred_at) as date,
  COUNT(DISTINCT actor_id) as active_users
FROM CLAUDE_BI.ACTIVITY.EVENTS
WHERE action LIKE 'mcp.%'
GROUP BY date
ORDER BY date DESC;
```

### Error Tracking
```sql
-- Failed requests
SELECT * FROM CLAUDE_BI.ACTIVITY.EVENTS
WHERE action = 'mcp.request'
  AND attributes:success::boolean = false
ORDER BY occurred_at DESC;
```

## 🎯 Next Steps

1. **Claude Code Integration**: Configure Claude Code to use this client
2. **Team Rollout**: Create tokens for team members
3. **Monitoring Setup**: Create dashboards for adoption and performance
4. **Documentation**: Train users on token management

---

## 👥 Complete User Journey: Sarah Gets Claude Code Access

### 📞 **Step 1: Sarah Calls You (Admin)**

**Sarah**: "Hey, I heard about Claude Code being able to connect to our Snowflake data. Can you help me set that up?"

**You**: "Absolutely! I'll create a secure token for you. Give me 5 minutes."

---

### 💻 **Step 2: You (Admin) Create Sarah's Access**

**On Your Machine (Admin)**
```bash
# Terminal 1: Connect to Snowflake as admin
snowsql -a uec18397.us-east-1 -u admin_user

# Create Sarah's activation
USE DATABASE CLAUDE_BI;
CALL ADMIN.CREATE_ACTIVATION('sarah_marketing', 'sarah@company.com');

# Output shows:
# {
#   "activation_id": "ACT_abc123def456",
#   "activation_url": "https://mcp.company.com/activate/abc123def456",
#   "expires_at": "2025-08-15 22:30:00",
#   "deeplink_ready": true
# }
```

**What You Do:**
1. Copy the activation URL: `https://mcp.company.com/activate/abc123def456`
2. Send it to Sarah via Slack/email with instructions

**Your Message to Sarah:**
```
Hi Sarah! 

Your Claude Code access is ready. Click this link to activate:
https://mcp.company.com/activate/abc123def456

This will automatically configure Claude Code on your machine.
The link expires in 24 hours.

Let me know if you have any issues!
```

---

### 📱 **Step 3: Sarah Clicks the Activation Link**

**On Sarah's Machine**
1. **Sarah clicks the activation URL in Slack**
2. **Browser opens** to `https://mcp.company.com/activate/abc123def456`
3. **Activation gateway processes** the request:
   ```javascript
   // Gateway validates activation code
   // Generates token: tk_a1b2c3d4e5f6...xyz_user
   // Creates deeplink: claudecode://activate?token=tk_a1b2c3...&user=sarah_marketing
   ```
4. **Browser redirects** to the deeplink
5. **Claude Code launches** automatically (if installed)

**What Sarah Sees:**
```
🎉 Activation Successful!

Claude Code is now configured for Snowflake access.
User: sarah_marketing
Account: uec18397.us-east-1
Permissions: ANALYST role

Claude Code will open automatically...
```

---

### 💻 **Step 4: Claude Code Auto-Configures**

**On Sarah's Machine (Automatic)**
When the deeplink `claudecode://activate?token=tk_abc123...&user=sarah` is triggered:

1. **Claude Code receives the deeplink**
2. **Token is automatically stored** in OS keychain:
   ```bash
   # Automatic behind the scenes:
   keychain.setPassword('SnowflakeMCP:uec18397.us-east-1', 'user_token', 'tk_abc123...')
   ```
3. **Configuration is validated**:
   ```bash
   # Claude Code runs internally:
   snowflake-mcp-client.test()
   ```
4. **Success notification** shown to Sarah

**What Sarah Sees in Claude Code:**
```
✅ Snowflake Integration Activated

You now have access to company data via natural language queries.
Try asking: "Show me last week's marketing campaign performance"

Available tools:
• Query data with natural language
• Create dashboards  
• List available data sources
• Export results

Your daily limits:
• 10,000 rows per query
• 2 hours of runtime per day
```

---

### 🚀 **Step 5: Sarah Uses Claude Code**

**Sarah's First Query**
**Sarah types in Claude Code:** "Show me marketing campaign performance for the last 30 days"

**Behind the Scenes:**
1. **Claude Code calls the thin client:**
   ```javascript
   const client = new SnowflakeMCPClient();
   await client.query("Show me marketing campaign performance for the last 30 days");
   ```

2. **Thin client authenticates:**
   ```javascript
   // Gets token from keychain
   token = keychain.getPassword('SnowflakeMCP:uec18397.us-east-1', 'user_token')
   // Calls stored procedure
   CALL CLAUDE_BI.MCP.HANDLE_REQUEST('tools/call', {...}, token)
   ```

3. **Snowflake validates and executes:**
   ```sql
   -- Server hashes token with pepper
   -- Checks Sarah's permissions
   -- Enforces her 10K row limit
   -- Executes safe query against marketing data
   -- Logs the activity
   ```

4. **Results returned to Sarah:**
   ```
   📊 Marketing Campaign Performance (Last 30 Days)
   
   ✅ Query completed in 1.2 seconds
   📈 Found 2,847 campaign records
   
   Top Campaigns by ROI:
   1. Summer Sale Email - 340% ROI
   2. Social Media Boost - 280% ROI  
   3. Google Ads Promo - 210% ROI
   
   [Interactive charts and data table shown]
   
   💡 Tip: Try "Break this down by channel" for more detail
   ```

---

### 📋 **Step 6: Monitoring & Management**

**You (Admin) Can Monitor:**
```sql
-- Check Sarah's usage
SELECT 
  DATE(occurred_at) as date,
  COUNT(*) as queries,
  SUM(attributes:runtime_seconds::NUMBER) as runtime_used
FROM CLAUDE_BI.ACTIVITY.EVENTS
WHERE actor_id = 'sarah_marketing'
  AND action = 'mcp.query.executed'
GROUP BY date
ORDER BY date DESC;

-- Monitor all users
SELECT * FROM MCP.CLIENT_ADOPTION;
```

**Sarah Can Check Her Status:**
```bash
# In terminal or Claude Code
snowflake-mcp status

# Output:
📊 Status: ✅ Active
User: sarah_marketing  
Daily runtime used: 847s / 7200s (12%)
Queries today: 23
Last query: 2 minutes ago
```

---

### 🎯 **Complete Flow Summary**

| **Step** | **Who** | **Where** | **What Happens** |
|----------|---------|-----------|------------------|
| 1 | Sarah | Phone/Slack | Asks for Claude Code access |
| 2 | Admin | Terminal | `CALL ADMIN.CREATE_ACTIVATION(...)` |
| 3 | Admin | Slack | Sends activation URL to Sarah |
| 4 | Sarah | Browser | Clicks activation URL |
| 5 | Gateway | Server | Validates → generates token → deeplink |
| 6 | Sarah | Claude Code | Auto-launched, token stored |
| 7 | Sarah | Claude Code | Asks natural language questions |
| 8 | System | Snowflake | Validates, executes, returns results |
| 9 | Admin | Dashboard | Monitors usage and adoption |

---

### 🔐 **Security Throughout**

- **Token never visible** to Sarah (deeplink handles it)
- **No database credentials** shared
- **Server-side validation** of every request  
- **Budget enforcement** prevents overuse
- **Complete audit trail** in Activity Schema
- **Token can be revoked** instantly by admin

---

### ⚡ **Time to Value**

- **Admin setup**: 30 seconds (one SQL call)
- **Sarah activation**: 1 minute (click link)
- **First query**: Immediate
- **Total time**: Under 2 minutes from request to working!

---

### 🛟 **If Something Goes Wrong**

**Sarah**: "It's not working!"

**You debug:**
```bash
# Check activation status
SELECT * FROM MCP.PENDING_ACTIVATIONS WHERE username = 'sarah_marketing';

# Check token validity  
SELECT * FROM MCP.ACTIVE_TOKENS WHERE username = 'sarah_marketing';

# Check recent errors
SELECT * FROM CLAUDE_BI.ACTIVITY.EVENTS 
WHERE actor_id = 'sarah_marketing' 
  AND action LIKE '%error%' 
ORDER BY occurred_at DESC;
```

**Quick fixes:**
- **Token expired**: Create new activation
- **Permission denied**: Update user permissions
- **Query failed**: Check user limits/role access

---

**Ready to ship!** 🚀 This thin client provides enterprise-grade security and operational robustness for Snowflake + Claude Code integration with a seamless user experience from request to results in under 2 minutes.