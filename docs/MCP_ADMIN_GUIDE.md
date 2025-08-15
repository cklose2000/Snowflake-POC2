# MCP Admin Guide - Snowflake Event-Based Access System

## Overview

This guide covers the administration of the MCP (Model Context Protocol) system where users exist only as events in Snowflake, and all access is controlled through token-based authentication.

## Table of Contents
1. [Architecture Overview](#architecture-overview)
2. [User Management](#user-management)
3. [Permission Management](#permission-management)
4. [Token Security](#token-security)
5. [Monitoring & Auditing](#monitoring--auditing)
6. [Cost Management](#cost-management)
7. [Troubleshooting](#troubleshooting)

## Architecture Overview

### The Two-Table Law
- **LANDING.RAW_EVENTS**: Append-only ingestion table
- **ACTIVITY.EVENTS**: Dynamic Table with 1-minute refresh lag

Everything else (users, permissions, audit logs) exists as events in these tables.

### Key Components
- **MCP.HANDLE_REQUEST**: Main entry point for all MCP requests
- **Token Hashing**: Only SHA256(token + pepper) stored, never raw tokens
- **Replay Protection**: Nonce validation prevents request replay
- **Eventually Consistent**: 60-second permission lag due to Dynamic Table

## User Management

### Creating a New User

```sql
CALL MCP.CREATE_MCP_USER(
  'sarah_marketing',      -- username
  'sarah@company.com',    -- email
  'ANALYST'              -- role template: VIEWER, ANALYST, or ADMIN
);
```

**Returns:**
```json
{
  "success": true,
  "username": "sarah_marketing",
  "token": "tk_abc123xyz...",  // Share this ONCE with user
  "delivery_url": "https://secure.company.com/claim-token/...",
  "delivery_expires_seconds": 300,
  "allowed_tools": ["compose_query", "list_sources", "export_data"],
  "max_rows": 10000,
  "daily_runtime_seconds": 7200
}
```

**Important:** 
- The token is returned ONLY ONCE
- Share the delivery_url with the user immediately
- URL expires in 5 minutes

### Role Templates

| Template | Tools | Max Rows | Daily Runtime | Use Case |
|----------|-------|----------|---------------|----------|
| VIEWER | list_sources, compose_query | 1,000 | 30 min | Read-only dashboards |
| ANALYST | + export_data, create_dashboard | 10,000 | 2 hours | Data analysis |
| ADMIN | + manage_users, view_audit | 100,000 | 8 hours | System administration |

### Revoking Access

```sql
CALL MCP.REVOKE_MCP_USER(
  'sarah_marketing',
  'Security policy violation'  -- reason
);
```

### Updating Permissions

```sql
CALL MCP.UPDATE_USER_PERMISSIONS(
  'sarah_marketing',
  ARRAY_CONSTRUCT('compose_query', 'export_data'),  -- new tools
  50000,  -- new row limit
  14400   -- 4 hours runtime
);
```

### Bulk User Import

1. Create CSV file with headers: `username,email,department`
2. Upload to stage: `PUT file://users.csv @user_imports/`
3. Import users:

```sql
CALL MCP.IMPORT_USERS_FROM_STAGE(
  '@user_imports/users.csv',
  'VIEWER'  -- default role for all imported users
);
```

## Permission Management

### How Permissions Work

1. **Latest Event Wins**: Most recent permission event determines access
2. **DENY Takes Precedence**: Revocation overrides grant at same timestamp
3. **Token-Scoped**: Permissions tied to specific token hash
4. **Time-Limited**: All tokens have expiration dates

### Viewing Current Permissions

```sql
-- All users and their current status
SELECT * FROM MCP.CURRENT_USERS;

-- Permission timeline for specific user
SELECT * FROM MCP.PERMISSION_TIMELINE 
WHERE username = 'sarah_marketing';

-- Active tokens (shows only hash prefix)
SELECT * FROM MCP.ACTIVE_TOKENS;
```

## Token Security

### Token Lifecycle

1. **Generation**: Cryptographically secure random token
2. **Hashing**: SHA256(token + server_pepper) stored in events
3. **Delivery**: One-time URL, expires in 5 minutes
4. **Usage**: Token sent with each request, validated against hash
5. **Rotation**: Periodic rotation recommended

### Rotating a User's Token

```sql
CALL MCP.ROTATE_USER_TOKEN(
  'sarah_marketing',
  'Scheduled rotation'  -- reason
);
```

**Returns new token and delivery URL**

### Security Best Practices

1. **Never log raw tokens**
2. **Rotate pepper periodically** (requires coordination)
3. **Monitor failed authentication attempts**
4. **Set appropriate token TTLs**
5. **Use one-time URLs for token delivery**

## Monitoring & Auditing

### Key Monitoring Views

```sql
-- Real-time request activity
SELECT * FROM MCP.REQUEST_ACTIVITY 
ORDER BY occurred_at DESC LIMIT 100;

-- User sessions analysis
SELECT * FROM MCP.USER_SESSIONS 
WHERE username = 'sarah_marketing';

-- Failed requests
SELECT * FROM MCP.FAILED_REQUESTS 
WHERE error_category = 'AUTH_ERROR';

-- Security events
SELECT * FROM MCP.SECURITY_AUDIT 
WHERE security_event_type = 'REPLAY_ATTACK';

-- Daily summary
SELECT * FROM MCP.DAILY_ACTIVITY_SUMMARY;
```

### Setting Up Alerts

```sql
-- Check configured alerts
SHOW ALERTS IN SCHEMA MCP;

-- Resume/suspend alerts
ALTER ALERT MCP.DT_LAG_ALERT RESUME;
ALTER ALERT MCP.BUDGET_EXCEEDED_ALERT SUSPEND;
```

### Audit Trail

Every action creates an event:
- User creation: `system.user.created`
- Permission grants: `system.permission.granted`
- Token operations: `system.token.*`
- MCP requests: `mcp.request.*`
- Security events: `security.*`

## Cost Management

### Monitoring Costs

```sql
-- Warehouse costs (last 30 days)
SELECT * FROM MCP.WAREHOUSE_COSTS 
ORDER BY usage_date DESC;

-- Storage costs
SELECT * FROM MCP.STORAGE_COSTS;

-- User runtime budgets
SELECT * FROM MCP.USER_RUNTIME_BUDGET 
WHERE budget_status IN ('WARNING', 'EXCEEDED');

-- Monthly summary
SELECT * FROM MCP.MONTHLY_COST_SUMMARY;

-- Optimization recommendations
SELECT * FROM MCP.COST_OPTIMIZATION;
```

### Cost Controls

1. **Runtime Budgets**: Each user has daily runtime limit
2. **Row Limits**: Prevent runaway queries
3. **Auto-Suspend Warehouses**: XS warehouses with 60s auto-suspend
4. **Dynamic Table Lag**: 1-minute lag balances cost vs freshness

### Warehouse Configuration

```sql
-- Check warehouse settings
SHOW WAREHOUSES LIKE 'DT_XS_WH';

-- Modify auto-suspend
ALTER WAREHOUSE DT_XS_WH SET AUTO_SUSPEND = 120;
```

## Troubleshooting

### Common Issues

#### 1. User Can't Authenticate
```sql
-- Check if user exists
SELECT * FROM MCP.CURRENT_USERS 
WHERE username = 'sarah_marketing';

-- Check token status
SELECT * FROM MCP.PERMISSION_TIMELINE 
WHERE username = 'sarah_marketing'
ORDER BY occurred_at DESC;

-- Check for failed auth attempts
SELECT * FROM MCP.FAILED_REQUESTS 
WHERE username = 'sarah_marketing'
  AND error_category = 'AUTH_ERROR';
```

#### 2. Permission Lag
- Dynamic Table has 1-minute refresh lag
- Recent permission changes may take up to 60 seconds
- Check DT health:
```sql
SELECT * FROM MCP.DT_HEALTH;
```

#### 3. Budget Exceeded
```sql
-- Check user's budget usage
SELECT * FROM MCP.USER_RUNTIME_BUDGET 
WHERE username = 'sarah_marketing';

-- Increase budget if needed
CALL MCP.UPDATE_USER_PERMISSIONS(
  'sarah_marketing',
  NULL,  -- keep existing tools
  NULL,  -- keep existing row limit
  28800  -- increase to 8 hours
);
```

#### 4. Replay Attack Detected
```sql
-- Check security events
SELECT * FROM MCP.SECURITY_AUDIT 
WHERE security_event_type = 'REPLAY_ATTACK'
ORDER BY occurred_at DESC;

-- Rotate token if compromised
CALL MCP.ROTATE_USER_TOKEN(
  'sarah_marketing',
  'Security incident - potential compromise'
);
```

### Health Checks

```sql
-- Dynamic Table health
SELECT * FROM MCP.DT_HEALTH;

-- Quality events (bad data)
SELECT * FROM MCP.QUALITY_EVENTS 
ORDER BY occurred_at DESC LIMIT 10;

-- System performance
SELECT * FROM MCP.QUERY_PERFORMANCE 
WHERE execution_seconds > 10;
```

## Best Practices

1. **User Provisioning**
   - Use role templates for consistency
   - Document why custom permissions are needed
   - Set appropriate expiration dates

2. **Security**
   - Rotate tokens every 90 days
   - Monitor failed authentication attempts
   - Review security audit logs weekly

3. **Cost Management**
   - Review monthly cost summary
   - Act on optimization recommendations
   - Adjust runtime budgets based on usage

4. **Monitoring**
   - Set up email alerts for critical events
   - Review daily activity summary
   - Track tool usage patterns

## Emergency Procedures

### Disable All Access
```sql
-- Revoke all active permissions
FOR user_record IN (SELECT username FROM MCP.CURRENT_USERS WHERE status = 'ACTIVE') DO
  CALL MCP.REVOKE_MCP_USER(user_record.username, 'Emergency shutdown');
END FOR;
```

### Force Dynamic Table Refresh
```sql
ALTER DYNAMIC TABLE CLAUDE_BI.ACTIVITY.EVENTS REFRESH;
```

### Restore User Access
```sql
-- Re-enable specific user
CALL MCP.CREATE_MCP_USER(
  'sarah_marketing',
  'sarah@company.com',
  'VIEWER'  -- Start with minimal permissions
);
```

## Support

For issues not covered in this guide:
1. Check audit logs for error details
2. Review the test suite results
3. Contact the data platform team

Remember: Everything is an event. If something happened, it's in the EVENTS table.