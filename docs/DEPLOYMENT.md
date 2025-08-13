# Deployment Guide

## Prerequisites

### System Requirements
- Node.js 16.0 or higher
- npm 8.0 or higher
- Git
- Claude Code CLI installed globally
- Snowflake account with appropriate permissions

### Snowflake Requirements
- Database: CLAUDE_BI (will be created)
- Warehouse: CLAUDE_WAREHOUSE
- Role: CLAUDE_BI_ROLE
- Schemas: ANALYTICS, ACTIVITY_CCODE

## Quick Start

### 1. Clone Repository
```bash
git clone <repository-url>
cd SnowflakePOC2
```

### 2. Run Setup Script
```bash
chmod +x scripts/setup.sh
./scripts/setup.sh
```

### 3. Configure Environment
Edit `.env` file with your Snowflake credentials:
```bash
SNOWFLAKE_ACCOUNT=your-account.snowflakecomputing.com
SNOWFLAKE_USERNAME=your-username
SNOWFLAKE_PASSWORD=your-password
SNOWFLAKE_DATABASE=CLAUDE_BI
SNOWFLAKE_SCHEMA=ANALYTICS
SNOWFLAKE_WAREHOUSE=CLAUDE_WAREHOUSE
SNOWFLAKE_ROLE=CLAUDE_BI_ROLE
```

### 4. Deploy Snowflake Schema
```bash
# Using snowsql
snowsql -f infra/snowflake/setup.sql

# Or using npm script (requires snowsql)
npm run setup:db
```

### 5. Install Dependencies
```bash
npm install
```

### 6. Start Development Server
```bash
npm run dev
```

This starts:
- Claude Code Bridge on port 3001
- WebSocket server on port 8080
- UI Shell on port 3000

## Production Deployment

### 1. Build Applications

#### Build UI Shell
```bash
cd apps/ui-shell
npm run build
npm run tauri build
```

#### Build Bridge
```bash
cd apps/ccode-bridge
npm run build
```

### 2. Environment Configuration

Create production `.env`:
```bash
NODE_ENV=production
LOG_LEVEL=warn
BRIDGE_PORT=3001
UI_PORT=3000

# Snowflake Production
SNOWFLAKE_ACCOUNT=prod-account.snowflakecomputing.com
SNOWFLAKE_USERNAME=service-user
SNOWFLAKE_PASSWORD=${SNOWFLAKE_PASSWORD}  # Use secrets manager
SNOWFLAKE_DATABASE=CLAUDE_BI
SNOWFLAKE_SCHEMA=ANALYTICS
SNOWFLAKE_WAREHOUSE=CLAUDE_WAREHOUSE_PROD
SNOWFLAKE_ROLE=CLAUDE_BI_ROLE
```

### 3. Deploy Infrastructure

#### Snowflake Setup
```sql
-- Run as ACCOUNTADMIN
USE ROLE ACCOUNTADMIN;

-- Create production warehouse
CREATE WAREHOUSE IF NOT EXISTS CLAUDE_WAREHOUSE_PROD
  WITH WAREHOUSE_SIZE = 'MEDIUM'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  MIN_CLUSTER_COUNT = 1
  MAX_CLUSTER_COUNT = 3;

-- Create production role
CREATE ROLE IF NOT EXISTS CLAUDE_BI_ROLE;

-- Grant permissions
GRANT USAGE ON WAREHOUSE CLAUDE_WAREHOUSE_PROD TO ROLE CLAUDE_BI_ROLE;

-- Deploy schema
USE ROLE CLAUDE_BI_ROLE;
-- Run setup.sql
```

#### Resource Monitoring
```sql
-- Set up production monitoring
CREATE OR REPLACE RESOURCE MONITOR claude_bi_monitor_prod
  WITH CREDIT_QUOTA = 1000
  TRIGGERS 
    ON 50 PERCENT DO NOTIFY
    ON 75 PERCENT DO NOTIFY
    ON 90 PERCENT DO SUSPEND
    ON 100 PERCENT DO SUSPEND_IMMEDIATE;

ALTER WAREHOUSE CLAUDE_WAREHOUSE_PROD 
  SET RESOURCE_MONITOR = claude_bi_monitor_prod;
```

### 4. Deploy Applications

#### Using Docker
```dockerfile
# Dockerfile for Bridge
FROM node:18-alpine
WORKDIR /app
COPY package*.json ./
RUN npm ci --production
COPY . .
EXPOSE 3001 8080
CMD ["node", "apps/ccode-bridge/src/index.js"]
```

```bash
# Build and run
docker build -t snowflakepoc2-bridge .
docker run -p 3001:3001 -p 8080:8080 --env-file .env snowflakepoc2-bridge
```

#### Using PM2
```bash
# Install PM2
npm install -g pm2

# Start Bridge
pm2 start apps/ccode-bridge/src/index.js --name ccode-bridge

# Save PM2 configuration
pm2 save
pm2 startup
```

### 5. Victory Audit

Before production deployment, run Victory Audit:
```bash
npm run validate:victory
```

All success claims must be verified (pass rate ≥ 95%).

## Monitoring

### Application Logs
```bash
# View Bridge logs
pm2 logs ccode-bridge

# Monitor in real-time
pm2 monit
```

### Snowflake Monitoring
```sql
-- Activity volume
SELECT DATE_TRUNC('hour', ts) as hour, COUNT(*) as events
FROM analytics.activity.events
WHERE ts > CURRENT_TIMESTAMP - INTERVAL '24 hours'
GROUP BY 1 ORDER BY 1;

-- Audit pass rate
SELECT 
  COUNT(*) as total_audits,
  SUM(CASE WHEN passed THEN 1 ELSE 0 END) as passed,
  (passed / total_audits) * 100 as pass_rate
FROM analytics.activity_ccode.audit_results
WHERE audit_ts > CURRENT_TIMESTAMP - INTERVAL '24 hours';

-- Query performance
SELECT 
  template,
  AVG(execution_time_ms) as avg_time,
  MAX(execution_time_ms) as max_time,
  COUNT(*) as query_count
FROM analytics.activity.events
WHERE activity = 'ccode.sql_executed'
  AND ts > CURRENT_TIMESTAMP - INTERVAL '1 hour'
GROUP BY template;
```

## Troubleshooting

### Connection Issues
1. Verify `.env` file exists and has correct values
2. Test Snowflake connection:
```bash
snowsql -a $SNOWFLAKE_ACCOUNT -u $SNOWFLAKE_USERNAME
```
3. Check network connectivity to Snowflake

### Permission Errors
```sql
-- Verify role grants
SHOW GRANTS TO ROLE CLAUDE_BI_ROLE;

-- Check current role
SELECT CURRENT_ROLE();

-- Switch role
USE ROLE CLAUDE_BI_ROLE;
```

### Activity Logging Issues
```sql
-- Check if events are being logged
SELECT COUNT(*) FROM analytics.activity.events
WHERE ts > CURRENT_TIMESTAMP - INTERVAL '5 minutes';

-- Check for errors
SELECT * FROM analytics.activity.events
WHERE activity = 'ccode.error_occurred'
ORDER BY ts DESC LIMIT 10;
```

### WebSocket Connection Failed
1. Check if port 8080 is available:
```bash
lsof -i :8080
```
2. Verify Bridge is running:
```bash
pm2 status ccode-bridge
```
3. Check browser console for errors

## Backup and Recovery

### Backup Activity Data
```sql
-- Create backup table
CREATE TABLE analytics.activity.events_backup AS
SELECT * FROM analytics.activity.events;

-- Export to stage
COPY INTO @analytics.activity_ccode.artifact_stage/backups/events_
FROM analytics.activity.events
FILE_FORMAT = (TYPE = 'JSON')
SINGLE = FALSE
MAX_FILE_SIZE = 536870912;
```

### Restore from Backup
```sql
-- From backup table
INSERT INTO analytics.activity.events
SELECT * FROM analytics.activity.events_backup
WHERE ts > '2024-01-01';

-- From stage
COPY INTO analytics.activity.events
FROM @analytics.activity_ccode.artifact_stage/backups/
FILE_FORMAT = (TYPE = 'JSON')
ON_ERROR = 'CONTINUE';
```

## Security Hardening

### 1. Use Service Accounts
Create dedicated Snowflake user for production:
```sql
CREATE USER claude_bi_service
  PASSWORD = 'strong_password'
  DEFAULT_ROLE = CLAUDE_BI_ROLE
  DEFAULT_WAREHOUSE = CLAUDE_WAREHOUSE_PROD
  MUST_CHANGE_PASSWORD = FALSE;

GRANT ROLE CLAUDE_BI_ROLE TO USER claude_bi_service;
```

### 2. Network Policies
```sql
CREATE NETWORK POLICY claude_bi_policy
  ALLOWED_IP_LIST = ('10.0.0.0/8', '172.16.0.0/12')
  BLOCKED_IP_LIST = ();

ALTER USER claude_bi_service SET NETWORK_POLICY = claude_bi_policy;
```

### 3. Secrets Management
Use environment variables or secrets manager:
```bash
# AWS Secrets Manager
export SNOWFLAKE_PASSWORD=$(aws secretsmanager get-secret-value \
  --secret-id snowflake-claude-bi \
  --query SecretString --output text)
```

### 4. TLS/SSL
Ensure all connections use TLS:
```javascript
snowflake.createConnection({
  // ... other config
  insecureConnect: false
});
```

## Performance Tuning

### 1. Warehouse Sizing
```sql
-- Monitor warehouse utilization
SELECT 
  WAREHOUSE_NAME,
  AVG(AVG_RUNNING) as avg_queries,
  MAX(AVG_RUNNING) as peak_queries
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_LOAD_HISTORY
WHERE WAREHOUSE_NAME = 'CLAUDE_WAREHOUSE_PROD'
  AND START_TIME > CURRENT_TIMESTAMP - INTERVAL '7 days'
GROUP BY WAREHOUSE_NAME;
```

### 2. Query Optimization
```sql
-- Enable query acceleration
ALTER WAREHOUSE CLAUDE_WAREHOUSE_PROD 
  SET QUERY_ACCELERATION_MAX_SCALE_FACTOR = 8;

-- Add clustering keys
ALTER TABLE analytics.activity.events
  CLUSTER BY (customer, DATE_TRUNC('day', ts));
```

### 3. Caching Strategy
```sql
-- Result caching (automatic)
ALTER SESSION SET USE_CACHED_RESULT = TRUE;

-- Metadata caching
ALTER WAREHOUSE CLAUDE_WAREHOUSE_PROD
  SET STATEMENT_TIMEOUT_IN_SECONDS = 3600;
```

## Health Checks

### Application Health Endpoint
```javascript
// Add to Bridge
app.get('/health', (req, res) => {
  res.json({
    status: 'healthy',
    timestamp: new Date(),
    connections: {
      snowflake: snowflakeConnected,
      websocket: wss.clients.size
    }
  });
});
```

### Monitoring Script
```bash
#!/bin/bash
# health-check.sh

# Check Bridge
curl -f http://localhost:3001/health || exit 1

# Check WebSocket
wscat -c ws://localhost:8080 -x '{"type":"ping"}' || exit 1

# Check Snowflake
snowsql -q "SELECT 1" || exit 1

echo "All systems operational"
```

## Rollback Procedure

### 1. Application Rollback
```bash
# Using PM2
pm2 stop ccode-bridge
git checkout previous-version
npm install
pm2 restart ccode-bridge

# Using Docker
docker stop snowflakepoc2-bridge
docker run -p 3001:3001 -p 8080:8080 --env-file .env snowflakepoc2-bridge:previous
```

### 2. Schema Rollback
Keep versioned migration scripts:
```sql
-- rollback/v1.0.0_to_v0.9.0.sql
ALTER TABLE analytics.activity.events
  DROP COLUMN IF EXISTS new_column;
```

## Success Metrics

Monitor these KPIs post-deployment:
- First token latency: < 300ms
- Card ready p95: < 8s
- Ingestion lag p95: < 5s
- Audit pass rate: ≥ 95%
- Error rate: < 1%
- Warehouse utilization: < 70%