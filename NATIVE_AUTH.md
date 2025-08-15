# Native Snowflake Authentication Guide

## Overview

This system uses **native Snowflake users and roles** instead of custom token management. Every human and AI agent gets a dedicated Snowflake user with appropriate role-based access control (RBAC).

**Key Principles:**
- ✅ Identity = Snowflake users
- ✅ Authorization = Snowflake roles  
- ✅ All access through stored procedures
- ✅ EXECUTE AS OWNER for writes (with guards)
- ✅ EXECUTE AS CALLER for reads

## Quick Start

### 1. Deploy Security Foundation

```bash
# As Snowflake admin, run these SQL scripts in order:
snowsql -f scripts/native-auth/01_security_foundation.sql
snowsql -f scripts/native-auth/02_provision_actor.sql
snowsql -f scripts/native-auth/03_workload_procedures.sql
snowsql -f scripts/native-auth/04_provision_users.sql
```

### 2. Provision a Human User (Password Auth)

```sql
CALL CLAUDE_BI.ADMIN.PROVISION_ACTOR(
  'sarah@company.com',    -- email
  'HUMAN',                 -- actor type
  FALSE,                   -- can_write (read-only)
  'PASSWORD',              -- auth mode
  'TempPassword123!@#'     -- initial password
);
```

**Output includes:**
- Username: `SARAH_COMPANY_COM`
- Role: `R_ACTOR_HUM_<hash>`
- Complete `.env` configuration
- Test command

### 3. Provision an AI Agent (Key-Pair Auth)

```sql
CALL CLAUDE_BI.ADMIN.PROVISION_ACTOR(
  'claude.code@ai.agent',  -- email
  'AGENT',                 -- actor type
  TRUE,                    -- can_write
  'KEYPAIR',               -- auth mode
  NULL                     -- no password needed
);
```

**Then generate RSA keys:**

```bash
# Generate private key
openssl genrsa -out claude_code_key.pem 2048

# Convert to PKCS8 format (required by Node.js SDK)
openssl pkcs8 -topk8 -inform PEM -outform PEM -nocrypt \
  -in claude_code_key.pem -out claude_code_key.p8

# Extract public key
openssl rsa -in claude_code_key.pem -pubout -out claude_code_key.pub

# Get public key content (remove headers)
cat claude_code_key.pub | grep -v "BEGIN" | grep -v "END" | tr -d '\n'
```

**Upload public key (within 10 minutes):**

```sql
ALTER USER CLAUDE_CODE_AI_AGENT 
SET RSA_PUBLIC_KEY = '<public_key_content>';
```

## Configuration Files

### Human User (.env)

```bash
# Sarah - Marketing Analyst
SNOWFLAKE_ACCOUNT=your-account.us-east-1
SNOWFLAKE_USERNAME=SARAH_COMPANY_COM
SNOWFLAKE_PASSWORD=<password_after_change>
SNOWFLAKE_ROLE=R_ACTOR_HUM_ABC12345
SNOWFLAKE_WAREHOUSE=CLAUDE_WAREHOUSE
SNOWFLAKE_DATABASE=CLAUDE_BI
SNOWFLAKE_SCHEMA=MCP
```

### AI Agent (.env)

```bash
# Claude Code - AI Agent
SNOWFLAKE_ACCOUNT=your-account.us-east-1
SNOWFLAKE_USERNAME=CLAUDE_CODE_AI_AGENT
SF_PK_PATH=/path/to/claude_code_key.p8
SNOWFLAKE_ROLE=R_ACTOR_AGT_DEF67890
SNOWFLAKE_WAREHOUSE=CLAUDE_AGENT_WH
SNOWFLAKE_DATABASE=CLAUDE_BI
SNOWFLAKE_SCHEMA=MCP
```

## Using the Simple Client

### Installation

```bash
cd snowflake-mcp-client
npm install
npm run build
```

### CLI Commands

```bash
# Check connection and permissions
npx snowflake-simple status

# List available data sources
npx snowflake-simple sources

# Run a natural language query
npx snowflake-simple query "show recent user signups"

# Insert an event (requires write permission)
npx snowflake-simple insert-event --action "test.event" --source "CLI"

# Run comprehensive tests
npx snowflake-simple test

# Show configuration
npx snowflake-simple config
```

### Programmatic Usage

```typescript
import { SnowflakeSimpleClient } from './simple-client';

const client = new SnowflakeSimpleClient();

// Check user status
const status = await client.getUserStatus();
console.log('User:', status.data.username);
console.log('Can Write:', status.data.permissions.can_write);

// Natural language query
const result = await client.query('show orders from last week');
console.log('Results:', result.data);

// Insert event (if authorized)
const event = {
  action: 'user.signup',
  actor_id: 'user123',
  attributes: { plan: 'premium' }
};
await client.insertEvent(event, 'APPLICATION');

// Disconnect when done
await client.disconnect();
```

## Security Model

### Role Hierarchy

```
SECURITYADMIN
    └── R_APP_ADMIN (provisioning, management)
            └── R_APP_WRITE (event insertion)
                    └── R_APP_READ (query data)
                            └── R_ACTOR_* (individual user roles)
```

### Stored Procedures

| Procedure | Execution Mode | Purpose | Required Role |
|-----------|---------------|---------|---------------|
| `SAFE_INSERT_EVENT` | OWNER + Guard | Insert events safely | R_APP_WRITE |
| `COMPOSE_QUERY_PLAN` | CALLER | Build query from text | R_APP_READ |
| `VALIDATE_QUERY_PLAN` | CALLER | Validate query plan | R_APP_READ |
| `EXECUTE_QUERY_PLAN` | CALLER | Execute validated plan | R_APP_READ |
| `LIST_SOURCES` | CALLER | List data sources | R_APP_READ |
| `GET_USER_STATUS` | CALLER | Check permissions | R_APP_READ |

### Security Features

- **Password Policy**: 14+ chars, mixed case, numbers, special chars
- **Session Policy**: 4-hour idle timeout
- **Network Policy**: Configurable IP allowlist
- **Resource Monitor**: Daily credit limits for agents
- **Query Tagging**: Full audit trail
- **Key Rotation**: Monthly reminders for agents

## Migration from Token System

### Safe Migration Order

1. **Deploy new system** (scripts 01-04)
2. **Test with new users** (Sarah, Claude Code)
3. **Verify functionality** works correctly
4. **Switch production users** one by one
5. **Delete token infrastructure** (see cleanup list below)

### Files to Delete After Migration

```bash
# Directories to remove
rm -rf activation-gateway/
rm -rf mcp-server/src/auth/
rm -rf claude-code-auth/

# Scripts to remove
rm deploy-activation-system.js
rm deploy-auth*.js
rm setup-service-user.js
rm simple-gateway.js
rm store-token-simple.js
rm simulate-activation.js
rm fix-warehouse-access.js
rm test-auth-*.js
rm test-mcp-secure-flow.js
rm test-token-storage.js

# SQL files to remove (token-related)
rm snowpark/activity-schema/12_token_security.sql
rm snowpark/activity-schema/13_mcp_handler.sql
rm snowpark/activity-schema/14_mcp_user_admin.sql
rm snowpark/activity-schema/23_token_pepper_security.sql
rm snowpark/activity-schema/24_activation_system.sql
rm snowpark/activity-schema/25_token_lifecycle.sql
rm snowpark/activity-schema/26_security_monitoring.sql
rm snowpark/activity-schema/27_emergency_procedures.sql

# Client files to remove
rm snowflake-mcp-client/src/auth.ts
rm snowflake-mcp-client/src/cli.ts  # Keep simple-cli.ts
rm snowflake-mcp-client/src/index.ts # Keep simple-client.ts
```

## Troubleshooting

### Connection Issues

```bash
# Check configuration
npx snowflake-simple config

# Test connection
npx snowflake-simple status

# Verify credentials
snowsql -a $SNOWFLAKE_ACCOUNT -u $SNOWFLAKE_USERNAME
```

### Permission Denied

```sql
-- Check user's roles
SHOW GRANTS TO USER <username>;

-- Check role hierarchy
SHOW GRANTS TO ROLE <role_name>;

-- Verify procedure grants
SHOW GRANTS ON PROCEDURE SAFE_INSERT_EVENT(VARIANT, STRING);
```

### Key-Pair Auth Issues

```bash
# Verify key format (should be PKCS8)
openssl pkcs8 -in your_key.p8 -nocrypt -topk8

# Check key in Snowflake
DESC USER <agent_username>;
```

### Enforce Key Deadlines

```sql
-- Run periodically to disable users who missed key upload
CALL CLAUDE_BI.ADMIN.ENFORCE_KEY_DEADLINES();
```

## Best Practices

1. **Humans use passwords** - Easier to manage, can add SSO later
2. **Agents use key-pairs** - More secure for headless operations
3. **Rotate keys monthly** - Use `ROTATE_AGENT_KEY` procedure
4. **Set resource monitors** - Prevent runaway costs
5. **Use query tags** - Enable observability
6. **Test before delete** - Validate new auth before removing old system
7. **Audit everything** - All actions logged as events

## Advanced Topics

### Adding SSO/OAuth Later

The system is designed to easily add SSO:
1. Configure Snowflake OAuth or External IdP
2. Users authenticate via SSO
3. Same procedures and roles still apply
4. No code changes needed

### Custom Role Patterns

```sql
-- Department-specific roles
CREATE ROLE R_DEPT_MARKETING;
CREATE ROLE R_DEPT_ENGINEERING;

-- Grant to actor roles as needed
GRANT ROLE R_DEPT_MARKETING TO ROLE R_ACTOR_HUM_<hash>;
```

### Row-Level Security

```sql
-- Create secure views with filters
CREATE SECURE VIEW V_USER_OWN_DATA AS
SELECT * FROM EVENTS
WHERE actor_id = CURRENT_USER();

-- Procedures automatically respect view security
```

## Support

For issues or questions:
1. Check this guide first
2. Run `npx snowflake-simple test` for diagnostics
3. Review audit events in `CLAUDE_BI.ACTIVITY.EVENTS`
4. Contact your Snowflake administrator