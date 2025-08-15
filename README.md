# Snowflake POC2 - Native Authentication with Claude Code Logging

**Pure 2-table architecture. Native Snowflake auth. Claude Code logging via RSA keys. Production-ready.**

## 🔐 Authentication: Native Snowflake Users & Roles

This system uses **native Snowflake authentication** - no custom tokens, no gateways, just Snowflake's built-in security:

- **Identity = Snowflake users**
- **Authorization = Snowflake roles**
- **Humans use passwords**
- **AI agents use RSA key-pairs**
- **All access through stored procedures**

## 🚀 Quick Start

### 1. Deploy Native Auth System

```bash
# As Snowflake admin
snowsql -a <your-account> -u <admin-user> -r ACCOUNTADMIN << 'EOF'
-- Run the native auth setup scripts
-- See scripts/native-auth/ for details
EOF
```

### 2. For Humans (Password Auth)

```bash
# Set credentials (see .env.example)
export SNOWFLAKE_ACCOUNT=<your-account>
export SNOWFLAKE_USERNAME=<username>
export SNOWFLAKE_PASSWORD=<password>
export SNOWFLAKE_ROLE=<role>
export SNOWFLAKE_WAREHOUSE=CLAUDE_WAREHOUSE

# Test connection
snowsql -q "SELECT CURRENT_USER()"
```

### 3. For AI Agents (Key-Pair Auth)

```bash
# Set credentials (see .env.claude_code.example)
export SNOWFLAKE_ACCOUNT=<your-account>
export SNOWFLAKE_USERNAME=<agent-username>
export SF_PK_PATH=/path/to/private_key.p8
export SNOWFLAKE_ROLE=<agent-role>
export SNOWFLAKE_WAREHOUSE=CLAUDE_AGENT_WH

# Test connection
snowsql --private-key-path $SF_PK_PATH -q "SELECT CURRENT_USER()"
```

## 🏛️ Architecture: The Two-Table Law

### THIS SYSTEM HAS EXACTLY TWO TABLES. ONLY TWO. FOREVER.

```sql
1. CLAUDE_BI.LANDING.RAW_EVENTS     -- All ingestion
2. CLAUDE_BI.ACTIVITY.EVENTS        -- Dynamic Table (auto-refresh)
```

**Everything else is a VIEW or an EVENT. No exceptions.**

## 🔑 Security Model

### Role Hierarchy

```
ACCOUNTADMIN
    └── R_APP_ADMIN (provisioning)
            └── R_APP_WRITE (insert events)
                    └── R_APP_READ (query data)
                            └── R_ACTOR_* (user-specific)
```

### Core Procedures

| Procedure | Mode | Purpose | Required Role |
|-----------|------|---------|---------------|
| `PROVISION_ACTOR` | OWNER | Create users | R_APP_ADMIN |
| `SAFE_INSERT_EVENT` | OWNER + Guard | Insert events | R_APP_WRITE |
| `LOG_CLAUDE_EVENT` | OWNER + Guard | Log Claude Code events | R_APP_WRITE |
| `LOG_CLAUDE_EVENTS_BATCH` | OWNER + Guard | Batch event logging | R_APP_WRITE |
| `LIST_SOURCES` | CALLER | List data sources | R_APP_READ |

## 📁 Structure

```
scripts/native-auth/
├── 01_security_foundation.sql   # Roles, policies, grants
├── 02_provision_actor.sql       # User provisioning
├── 03_workload_procedures.sql   # Core procedures
├── 04_logging_procedures.sql    # Claude Code logging
├── 05_dynamic_tables.sql        # Dynamic table & monitors
├── 06_monitoring_views.sql      # Observability views
└── Test users & verification scripts

snowflake-mcp-client/
├── src/
│   ├── simple-client.ts         # Direct Snowflake connection
│   └── simple-cli.ts            # CLI for testing
└── package.json

NATIVE_AUTH.md                   # Complete auth guide
```

## 🧪 Test Users

### Example: Human User (Read-Only)
- Username: Generated from email
- Password: Set during provisioning
- Role: `R_ACTOR_HUM_<hash>`
- Permissions: Read only

### Example: AI Agent (Read-Write)
- Username: Generated from email
- Auth: RSA key-pair
- Role: `R_ACTOR_AGT_<hash>`
- Permissions: Read + Write

## 🚀 Using the Simple Client

```bash
cd snowflake-mcp-client
npm install
npm run build

# Check status
npx ts-node src/simple-cli.ts status

# List sources
npx ts-node src/simple-cli.ts sources

# Run tests
npx ts-node src/simple-cli.ts test
```

## 🔄 Migration from Token System

The old token-based authentication has been replaced with native Snowflake auth. Benefits:

- ✅ No custom token management
- ✅ Native role-based security
- ✅ Simpler codebase (40% less code)
- ✅ Better audit trails
- ✅ Enterprise-ready security

## 📚 Documentation

- [NATIVE_AUTH.md](./NATIVE_AUTH.md) - Complete native auth guide
- [CLAUDE.md](./CLAUDE.md) - Two-table architecture rules

## 🛡️ Security Features

- **Password Policy**: 14+ chars, mixed case, numbers, special chars
- **Session Policy**: 4-hour idle timeout
- **Network Policy**: IP allowlist ready
- **Resource Monitors**: Daily credit limits for agents
- **Query Tagging**: Full observability
- **Key Rotation**: Monthly for agents

## 🎯 Core Principles

1. **Two tables only** - RAW_EVENTS and EVENTS
2. **Native auth** - Snowflake users and roles
3. **Stored procedures** - All access through procedures
4. **Event-driven** - Everything is an event
5. **Production-ready** - Enterprise security built-in

## 📊 Connection Details

```bash
# Environment Variables (see .env.example)
SNOWFLAKE_ACCOUNT=<your-account>
SNOWFLAKE_DATABASE=CLAUDE_BI
SNOWFLAKE_SCHEMA=MCP

# For humans
SNOWFLAKE_USERNAME=<user>
SNOWFLAKE_PASSWORD=<password>
SNOWFLAKE_ROLE=R_ACTOR_HUM_<hash>
SNOWFLAKE_WAREHOUSE=CLAUDE_WAREHOUSE

# For agents
SNOWFLAKE_USERNAME=<agent>
SF_PK_PATH=/path/to/key.p8
SNOWFLAKE_ROLE=R_ACTOR_AGT_<hash>
SNOWFLAKE_WAREHOUSE=CLAUDE_AGENT_WH
```

## 🚫 What NOT to Do

- ❌ **NEVER create new tables** - Use events
- ❌ **NEVER bypass procedures** - Always use the API
- ❌ **NEVER share credentials** - Each user gets their own
- ❌ **NEVER skip key rotation** - Monthly for agents

## 🔍 Claude Code Logging Integration

### Full Observability with RSA Authentication

Claude Code (AI agent) connects via RSA key authentication and logs all operations:

```javascript
// All Claude Code operations are logged automatically
const client = new SnowflakeSimpleClient({
  account: 'your-account',
  username: 'CLAUDE_CODE_AI_AGENT',
  privateKeyPath: './claude_code_rsa_key.p8',
  warehouse: 'CLAUDE_AGENT_WH'
});

// Direct logging via stored procedure
await client.logEvent({
  action: 'ccode.query.executed',
  session_id: sessionId,
  attributes: { /* metadata */ }
});
```

### Logging Features

- **Auto-batching**: Switches to batch mode at high volume
- **Query tagging**: Structured tags for every operation
- **Session tracking**: Full session lifecycle logging
- **Error tracking**: Automatic error event capture
- **Performance metrics**: Execution times and resource usage

### Recent Activity Query

```sql
-- View last 10 events
SELECT * FROM CLAUDE_BI.ACTIVITY.EVENTS 
ORDER BY OCCURRED_AT DESC 
LIMIT 10;
```

## ✨ Ready to Use

The system is fully deployed with:
- Native Snowflake authentication
- Claude Code agent with RSA key-pair auth
- Production logging procedures (LOG_CLAUDE_EVENT)
- Dynamic table with 1-minute refresh
- Resource monitors and credit limits
- Complete audit trail with query tagging
- 7 monitoring views for observability

**All operations through Claude Code are automatically logged!**