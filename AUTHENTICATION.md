# Claude Code Authentication System

## Overview

This system implements secure token-based authentication for Claude Code's MCP server integration with Snowflake. The architecture follows the Two-Table Law while providing enterprise-grade security features.

## 🔑 Key Features

- **Token-based Authentication**: SHA256 hashed tokens with pepper
- **Event-driven User Management**: All users and permissions stored as events
- **One-click Activation**: Secure activation links with deeplink delivery
- **Role-based Access Control**: Template-based permissions (VIEWER, ANALYST, ADMIN)
- **Usage Tracking**: Runtime limits and activity monitoring
- **Security Monitoring**: Dashboard and threat detection
- **Emergency Procedures**: Kill switches and incident response

## 🏗️ Architecture

### Core Tables (Two-Table Law Compliance)
```sql
-- Only these two tables exist:
CLAUDE_BI.LANDING.RAW_EVENTS      -- Ingestion layer
CLAUDE_BI.ACTIVITY.EVENTS         -- Dynamic table for queries
```

### Authentication Schemas
```sql
CLAUDE_BI.ADMIN_SECRETS    -- Secure pepper storage
CLAUDE_BI.MCP              -- Token functions and views
CLAUDE_BI.SECURITY         -- Monitoring and dashboards
```

## 🔐 Token Security

### Token Format
```
tk_[32_hex_chars]_user
Example: tk_a1b2c3d4e5f6789012345678901234567890_user
```

### Security Layers
1. **Pepper**: Server-side secret for token hashing
2. **SHA256 Hashing**: `SHA2(token + pepper, 256)`
3. **Metadata Extraction**: Prefix/suffix for validation
4. **Never Stored**: Only hashes stored, never raw tokens

### Key Functions
```sql
-- Generate new tokens
SELECT MCP.GENERATE_SECURE_TOKEN();

-- Hash with pepper
SELECT MCP.HASH_TOKEN_WITH_PEPPER('tk_...');

-- Extract metadata
SELECT MCP.EXTRACT_TOKEN_METADATA('tk_...');
```

## 👥 User Management

### Creating Users (Event-based)
```sql
-- User creation is an EVENT
INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS VALUES (
  OBJECT_CONSTRUCT(
    'action', 'system.user.created',
    'object', OBJECT_CONSTRUCT('type', 'user', 'id', 'username'),
    'attributes', OBJECT_CONSTRUCT('email', 'user@company.com'),
    ...
  ), 'ADMIN', CURRENT_TIMESTAMP()
);

-- Permission grant is an EVENT  
INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS VALUES (
  OBJECT_CONSTRUCT(
    'action', 'system.permission.granted',
    'attributes', OBJECT_CONSTRUCT(
      'token_hash', 'sha256_hash_here',
      'allowed_tools', ['compose_query', 'list_sources'],
      'max_rows', 10000,
      'expires_at', DATEADD('day', 30, CURRENT_TIMESTAMP())
    ),
    ...
  ), 'ADMIN', CURRENT_TIMESTAMP()
);
```

### Role Templates
```sql
-- VIEWER: Basic query access
allowed_tools: ['compose_query', 'list_sources']
max_rows: 1000
daily_runtime: 1800 seconds (30 min)

-- ANALYST: Enhanced access
allowed_tools: ['compose_query', 'list_sources', 'export_data']  
max_rows: 10000
daily_runtime: 7200 seconds (2 hours)

-- ADMIN: Full access
allowed_tools: ['compose_query', 'list_sources', 'export_data', 'manage_users']
max_rows: 100000
daily_runtime: 28800 seconds (8 hours)
```

## 🎯 One-Click Activation

### Activation Flow
1. Admin creates activation via: `CALL ADMIN.CREATE_ACTIVATION('username', 'email')`
2. User receives activation URL: `https://gateway.company.com/activate/ABC123`
3. User clicks link → gateway validates → returns deeplink
4. Deeplink: `claudecode://activate?token=tk_abc123...&user=username`
5. Claude Code CLI stores token in OS keychain

### Security Features
- **Time-limited**: Activation codes expire (default: 24 hours)
- **Single-use**: Codes are marked as used after activation
- **Rate limiting**: Prevents activation code enumeration
- **Audit trail**: All activations logged as events

## 🛡️ MCP Server Integration

### Tool Authentication
Every MCP tool call can include a `token` parameter:

```json
{
  "method": "tools/call",
  "params": {
    "name": "compose_query_plan",
    "arguments": {
      "intent_text": "Show me user activity",
      "token": "tk_abc123..."
    }
  }
}
```

### Authentication Flow
1. Extract token from tool arguments
2. Hash token with pepper: `MCP.HASH_TOKEN_WITH_PEPPER(token)`
3. Look up user by hash in `ACTIVITY.EVENTS`
4. Validate token expiry and permissions
5. Check tool access: `tool_name IN user.allowed_tools`
6. Apply user limits (max_rows, daily_runtime)
7. Execute tool with user context
8. Log usage: runtime tracking, audit trail

### User Context
```typescript
interface UserContext {
  username: string;
  allowedTools: string[];
  maxRows: number;
  dailyRuntimeSeconds: number;
  expiresAt: Date;
  tokenPrefix: string;
  usageTracking?: {
    dailyRuntimeUsed: number;
    lastUsed: Date;
  };
}
```

## 📊 Monitoring & Security

### Active Tokens View
```sql
SELECT * FROM MCP.ACTIVE_TOKENS;
-- Shows: username, token_hint, status, age_days, expires_at
```

### Security Dashboard
```sql
SELECT * FROM SECURITY.DASHBOARD;
-- Shows: active_tokens, pending_activations, threat_level
```

### Usage Tracking
```sql
-- Daily runtime usage per user
SELECT 
  actor_id AS username,
  SUM(attributes:runtime_seconds::NUMBER) AS daily_runtime_used
FROM CLAUDE_BI.ACTIVITY.EVENTS
WHERE action = 'mcp.tool.executed'
  AND DATE(occurred_at) = CURRENT_DATE()
GROUP BY actor_id;
```

## 🚨 Emergency Procedures

### Revoke All Tokens
```sql
CALL EMERGENCY.REVOKE_ALL_TOKENS('CONFIRM_EMERGENCY_2024');
-- Requires confirmation code
```

### Revoke Single User
```sql
-- Create revocation event
INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS VALUES (
  OBJECT_CONSTRUCT(
    'action', 'system.permission.revoked',
    'object', OBJECT_CONSTRUCT('type', 'user', 'id', 'username'),
    'attributes', OBJECT_CONSTRUCT('reason', 'security_incident'),
    ...
  ), 'ADMIN', CURRENT_TIMESTAMP()
);
```

### Block IPs
```sql
CALL EMERGENCY.BLOCK_IP_RANGE('192.168.1.0/24', 'Suspicious activity');
```

## 🔧 Deployment & Setup

### 1. Deploy Authentication Infrastructure
```bash
node deploy-auth-fixed.js
```

### 2. Test System
```bash
node test-auth-simple.js
```

### 3. Start Activation Gateway
```bash
cd activation-gateway
npm install && npm start
```

### 4. Install Claude Code CLI Helper
```bash
cd claude-code-auth
npm install -g .
```

## 💻 Claude Code CLI Integration

### Pairing Process
```bash
# User receives activation URL and clicks it
# Browser redirects to claudecode://activate?token=...

# CLI automatically stores token in keychain
claude-code-pair status
# Shows: ✅ Authenticated as username@company.com

# Use with tools
claude-code query "Show me recent activity" --authenticated
```

### Token Storage
- **macOS**: Keychain Access (`keytar` package)
- **Windows**: Windows Credential Store  
- **Linux**: Secret Service API
- **Token never appears in CLI history or logs**

## 🔍 Testing & Validation

### Test Token Generation
```sql
SELECT MCP.GENERATE_SECURE_TOKEN() AS token;
```

### Test Authentication
```sql
-- Hash test token
SELECT MCP.HASH_TOKEN_WITH_PEPPER('tk_test123...') AS hash;

-- Look up user
SELECT * FROM MCP.ACTIVE_TOKENS WHERE token_hint LIKE 'tk_test%';
```

### Test MCP Integration
```bash
# Test authenticated tool call
echo '{"method":"tools/call","params":{"name":"list_sources","arguments":{"token":"tk_abc123..."}}}' | \
  node mcp-server/dist/index.js
```

## 🔐 Security Best Practices

### For Admins
1. **Rotate Pepper**: Change `ADMIN_SECRETS.GET_PEPPER()` annually
2. **Monitor Dashboard**: Check `SECURITY.DASHBOARD` daily
3. **Audit Tokens**: Review `MCP.ACTIVE_TOKENS` weekly
4. **Emergency Drills**: Test revocation procedures monthly

### For Users
1. **Secure Activation**: Only click activation links from trusted admins
2. **Token Protection**: Never share activation URLs or deeplinks
3. **Report Issues**: Suspicious activity should be reported immediately
4. **Regular Rotation**: Request new tokens quarterly

### For Developers
1. **No Token Logging**: Never log raw tokens in application code
2. **Hash Validation**: Always validate tokens server-side
3. **Rate Limiting**: Implement activation and authentication rate limits
4. **Audit Everything**: Log all authentication and authorization events

## 📋 Troubleshooting

### Token Issues
```sql
-- Check if token exists
SELECT * FROM MCP.ACTIVE_TOKENS WHERE token_hint = 'tk_abc1...xyz9';

-- Check token expiry
SELECT username, expires_at, status FROM MCP.ACTIVE_TOKENS 
WHERE expires_at < CURRENT_TIMESTAMP();
```

### Activation Issues
```sql
-- Check pending activations
SELECT * FROM MCP.PENDING_ACTIVATIONS WHERE status = 'PENDING';

-- Check expired activations  
SELECT * FROM MCP.PENDING_ACTIVATIONS WHERE status = 'EXPIRED';
```

### MCP Server Issues
```bash
# Check MCP server logs
tail -f /var/log/mcp-server.log

# Test connection
node test-mcp-connection.js

# Verify authentication
node test-token-auth.js tk_your_token_here
```

## 🚀 Next Steps

1. **Scale Testing**: Load test with multiple concurrent users
2. **Integration**: Connect with company SSO/LDAP
3. **Monitoring**: Set up alerts for suspicious activity
4. **Documentation**: Create user training materials
5. **Automation**: Implement automated token rotation

---

*This authentication system maintains the Two-Table architecture while providing enterprise-grade security. All user data exists as events, ensuring compliance with the core architectural constraints while enabling robust access control.*