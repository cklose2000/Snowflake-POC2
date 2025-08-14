# Activity Schema 2.0 - Event-Based Data Warehouse

A hyper-simple data warehouse implementation using ONLY two tables, with all user permissions tracked as events rather than in separate admin tables.

## 🎯 Core Principles

1. **Two Tables Only**: `LANDING.RAW_EVENTS` (write) + `ACTIVITY.EVENTS` (read)
2. **Everything is an Event**: Business data, permissions, audit logs - all events
3. **No Direct Access**: Users can only execute MCP procedures
4. **Latest Event Wins**: Permissions determined by most recent event
5. **Complete Audit Trail**: Every action creates an immutable event

## 📁 File Structure

```
activity-schema/
├── 01_setup_database.sql      # Database, schemas, warehouses
├── 02_create_raw_events.sql   # Landing table
├── 03_create_dynamic_table.sql # Dynamic Table with deduplication
├── 04_create_roles.sql        # MCP security roles
├── 05_mcp_procedures.sql      # Core query execution
├── 06_monitoring_views.sql    # Permission & activity views
├── 07_user_management.sql     # User creation procedures
├── 08_test_setup.sql          # Test data and users
├── 09_monitoring_queries.sql  # Dashboard queries
├── test-mcp-access.js         # Permission validation tests
├── activity_contract.json     # Schema documentation
└── deploy-activity-schema.js  # Automated deployment
```

## 🚀 Quick Start

### 1. Deploy the Schema

```bash
cd snowpark/activity-schema
node deploy-activity-schema.js
```

This will:
- Create database and schemas
- Set up landing and dynamic tables
- Create roles and procedures
- Deploy monitoring views
- Load test data
- Create test users

### 2. Test the System

```bash
node test-mcp-access.js
```

This validates:
- Users can only execute procedures
- Permission events control access
- Row limits are enforced
- Direct table access is blocked

## 🔐 Security Model

### Roles

- **MCP_USER_ROLE**: Can only execute `EXECUTE_QUERY_PLAN` procedure
- **MCP_SERVICE_ROLE**: Used by procedures (EXECUTE AS OWNER)
- **MCP_ADMIN_ROLE**: Manages users and permissions

### Permission Flow

1. Admin grants permission → Creates `system.permission.granted` event
2. User executes query → Procedure checks latest permission event
3. Query validated against allowed actions, row limits, runtime budget
4. Execution logged as `mcp.query.executed` event
5. Results returned via `RESULT_SCAN` pattern

## 📊 Monitoring

### Key Views

- `CURRENT_USER_PERMISSIONS` - Active permissions per user
- `QUERY_ACTIVITY_LAST_24H` - Recent query activity
- `USER_RUNTIME_LAST_24H` - Runtime usage for rate limiting
- `PERMISSION_CHANGES_LAST_30D` - Permission audit trail

### Example Queries

```sql
-- Who has what permissions?
SELECT * FROM CLAUDE_BI.MCP.CURRENT_USER_PERMISSIONS
WHERE status = 'ACTIVE';

-- Runtime budget usage
SELECT * FROM CLAUDE_BI.MCP.USER_RUNTIME_LAST_24H
ORDER BY seconds_used DESC;

-- System health
SELECT * FROM CLAUDE_BI.MCP.SYSTEM_HEALTH_HOURLY
WHERE hour >= DATEADD('hour', -24, CURRENT_TIMESTAMP());
```

## 🧪 Test Users

Created by `08_test_setup.sql`:

| User | Department | Access Level | Row Limit | Runtime Budget |
|------|------------|--------------|-----------|----------------|
| sarah_marketing | Marketing | Limited | 10,000 | 60s |
| john_analyst | Analytics | Broad | 50,000 | 300s |
| intern_viewer | Marketing | Minimal | 1,000 | 30s |
| exec_dashboard | Executive | Full | 100,000 | 600s |

Default password: `TempPassword123!` (must change on first login)

## 🔄 Event Flow

### Business Event
```json
{
  "event_id": "ord_123",
  "action": "order.placed",
  "occurred_at": "2024-01-15T10:30:00Z",
  "actor_id": "customer_42",
  "source": "ecommerce",
  "object": {"type": "order", "id": "ORD_00001"},
  "attributes": {"amount": 99.99, "items": 3}
}
```

### Permission Event
```json
{
  "event_id": "sys_456",
  "action": "system.permission.granted",
  "occurred_at": "2024-01-15T09:00:00Z",
  "actor_id": "admin",
  "source": "system",
  "object": {"type": "user", "id": "john_analyst"},
  "attributes": {
    "allowed_actions": ["order.placed", "user.signup"],
    "max_rows": 50000,
    "daily_runtime_budget_s": 300
  }
}
```

## 🎯 Benefits

- **Zero Admin Tables**: Everything flows through 2 tables
- **Complete Audit**: Every action is an immutable event
- **Temporal Permissions**: Query who had access when
- **Native Security**: Snowflake authentication, no external auth
- **Minimal Maintenance**: No schema changes for permission updates
- **Cost Efficient**: Two XS warehouses, pay per use

## 🛠️ Maintenance

### Grant New Permissions
```sql
CALL CLAUDE_BI.MCP.GRANT_USER_PERMISSION(
  'username',
  ARRAY_CONSTRUCT('order.placed', 'user.signup'),
  10000,     -- max_rows
  120,       -- runtime_budget_seconds
  FALSE,     -- can_export
  DATEADD('year', 1, CURRENT_TIMESTAMP())
);
```

### Revoke Permissions
```sql
CALL CLAUDE_BI.MCP.REVOKE_USER_PERMISSION('username', 'Policy violation');
```

### Create New User
```sql
CALL CLAUDE_BI.MCP.CREATE_MCP_USER(
  'new_user',
  'user@company.com',
  'Department',
  ARRAY_CONSTRUCT('order.placed'),
  10000,
  120
);
```

## 📈 Production Considerations

1. **Dynamic Table Lag**: Currently 1 minute - adjust based on needs
2. **Warehouse Sizing**: XS suitable for most workloads
3. **Retention**: Consider archiving old events after X days
4. **Monitoring**: Set up alerts for permission changes and rate limit hits
5. **Password Management**: Integrate with your identity provider

## 🔍 Troubleshooting

### User Can't Connect
- Check password (default: `TempPassword123!`)
- Verify user exists: `SHOW USERS LIKE 'username';`
- Check permissions: `SELECT * FROM CURRENT_USER_PERMISSIONS`

### Query Rejected
- Check runtime budget: `SELECT * FROM USER_RUNTIME_LAST_24H`
- Verify allowed actions match requested actions
- Check row limit not exceeded

### Missing Events
- Dynamic Table refresh lag (1 minute)
- Check `LANDING.RAW_EVENTS` for pending events
- Verify Dynamic Table is running

## 📝 Contract

See `activity_contract.json` for complete schema documentation including:
- Event structure
- Reserved namespaces
- Permission attributes
- Security model
- Deployment order