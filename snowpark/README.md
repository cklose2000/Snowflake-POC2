# Snowpark Container Services MCP Server

## 🚀 Overview

This directory contains the MCP (Model Context Protocol) server implementation that runs entirely within Snowflake using Snowpark Container Services. This achieves **zero external infrastructure** while maintaining the security boundary for controlled Snowflake access.

## 🏗️ Architecture

```
┌─────────────────────────────────────────┐
│           SNOWFLAKE ACCOUNT             │
├─────────────────────────────────────────┤
│  ┌─────────────────────────────────┐    │
│  │  Snowpark Container Services    │    │
│  │  ┌────────────────────────┐     │    │
│  │  │   MCP Server Container │     │    │
│  │  │  - Python FastAPI      │     │    │
│  │  │  - Contract validation │     │    │
│  │  │  - SafeSQL templates   │     │    │
│  │  │  - Rate limiting       │     │    │
│  │  │  - Query tagging       │     │    │
│  │  └────────────────────────┘     │    │
│  └─────────────────────────────────┘    │
│                    ↓                     │
│  ┌─────────────────────────────────┐    │
│  │     CLAUDE_BI.ACTIVITY.EVENTS   │    │
│  └─────────────────────────────────┘    │
└─────────────────────────────────────────┘
```

## 📁 Directory Structure

```
snowpark/
├── mcp_server.py          # Python FastAPI MCP server
├── requirements.txt       # Python dependencies
├── Dockerfile            # Container definition
├── service.yaml          # Snowpark service specification
├── deploy.sh            # Deployment script
├── contracts/           # Schema contracts (copied from main)
├── templates/           # SQL templates
└── README.md           # This file
```

## 🔧 Setup Instructions

### Prerequisites

1. Snowflake account with Snowpark Container Services enabled
2. Docker installed locally
3. SnowSQL CLI installed
4. Appropriate Snowflake roles (ACCOUNTADMIN for setup)

### Step 1: Run SQL Setup

Execute the setup SQL to create roles, warehouses, and permissions:

```bash
snowsql -f ../infra/snowflake/mcp-setup.sql
```

This creates:
- `MCP_SERVICE_ROLE` - For container service
- `MCP_EXECUTOR_ROLE` - For query execution
- `MCP_USER_ROLE` - For end users
- `MCP_XS_WH` - Extra small warehouse
- `MCP_COMPUTE_POOL` - Container compute pool
- Resource monitors with 10 credit daily limit

### Step 2: Build and Deploy Container

```bash
# Set environment variables
export SNOWFLAKE_ACCOUNT="your-account"
export SNOWFLAKE_USERNAME="your-username"
export SNOWFLAKE_PASSWORD="your-password"

# Run deployment script
./deploy.sh
```

The script will:
1. Build Docker image
2. Push to Snowflake registry
3. Upload configuration files
4. Create the container service

### Step 3: Get Service Endpoint

```sql
SELECT SYSTEM$GET_SERVICE_ENDPOINT('CLAUDE_BI.PUBLIC.MCP_SERVER', 'api');
```

Returns something like:
```
https://your-account.snowflakecomputing.com/api/services/CLAUDE_BI/PUBLIC/MCP_SERVER
```

### Step 4: Update UI Configuration

Update your UI to use the Snowpark endpoint:

```javascript
const mcpClient = new SnowparkMCPClient({
  baseUrl: 'https://your-account.snowflakecomputing.com/api/services/CLAUDE_BI/PUBLIC/MCP_SERVER',
  token: snowflakeAuthToken  // From Snowflake auth
});
```

## 🔌 API Endpoints

### Health Check
```
GET /health
```

### List Tools
```
GET /tools
```

### Compose Query Plan
```
POST /tools/compose_query_plan
{
  "intent_text": "Show me activity summary",
  "source": "VW_ACTIVITY_SUMMARY",
  "top_n": 10
}
```

### Validate Plan
```
POST /tools/validate_plan
{
  "source": "VW_ACTIVITY_COUNTS_24H",
  "dimensions": ["HOUR"],
  "measures": [{"fn": "SUM", "column": "EVENT_COUNT"}]
}
```

### List Sources
```
GET /tools/list_sources?include_columns=true
```

### Create Dashboard
```
POST /tools/create_dashboard
{
  "title": "Activity Dashboard",
  "queries": [...]
}
```

## 🔒 Security Features

1. **Role-Based Access**: Three-tier role hierarchy
2. **Resource Limits**: 10 credit daily limit, 30-second query timeout
3. **Query Tagging**: All queries tagged with user and timestamp
4. **Contract Validation**: Schema contract enforced on all queries
5. **SQL Injection Prevention**: Parameterized queries only
6. **Row Limits**: Maximum 10,000 rows per query
7. **Activity Logging**: All operations logged to Activity Schema

## 📊 Monitoring

### Check Service Status
```sql
SELECT SYSTEM$GET_SERVICE_STATUS('CLAUDE_BI.PUBLIC.MCP_SERVER');
```

### View Service Logs
```sql
SELECT SYSTEM$GET_SERVICE_LOGS('CLAUDE_BI.PUBLIC.MCP_SERVER', 'mcp-gateway', 100);
```

### Monitor Usage
```sql
-- MCP usage is automatically tracked hourly
SELECT * FROM CLAUDE_BI.ACTIVITY.EVENTS 
WHERE activity = 'ccode.mcp.usage_tracked'
ORDER BY ts DESC;
```

### Resource Monitor Status
```sql
SHOW RESOURCE MONITORS LIKE 'MCP_DAILY_MONITOR';
```

## 🧪 Testing

### Test Stored Procedures
```sql
-- Test validation
CALL CLAUDE_BI.MCP.VALIDATE_QUERY_PLAN(PARSE_JSON('{
  "source": "ACTIVITY_CCODE.VW_ACTIVITY_SUMMARY",
  "top_n": 10
}'));

-- Test execution
CALL CLAUDE_BI.MCP.EXECUTE_QUERY_PLAN(PARSE_JSON('{
  "source": "ACTIVITY_CCODE.VW_ACTIVITY_SUMMARY",
  "top_n": 5
}'));
```

### Test Container Service
```bash
# Test health endpoint
curl https://your-account.snowflakecomputing.com/api/services/CLAUDE_BI/PUBLIC/MCP_SERVER/health

# Test tools listing
curl https://your-account.snowflakecomputing.com/api/services/CLAUDE_BI/PUBLIC/MCP_SERVER/tools
```

## 🛠️ Troubleshooting

### Service Won't Start
```sql
-- Check compute pool status
SHOW COMPUTE POOLS;

-- Check service details
DESCRIBE SERVICE CLAUDE_BI.PUBLIC.MCP_SERVER;

-- View error logs
SELECT SYSTEM$GET_SERVICE_LOGS('CLAUDE_BI.PUBLIC.MCP_SERVER', 'mcp-gateway', 100);
```

### Permission Issues
```sql
-- Verify role grants
SHOW GRANTS TO ROLE MCP_EXECUTOR_ROLE;
SHOW GRANTS TO ROLE MCP_SERVICE_ROLE;
```

### Resource Limit Reached
```sql
-- Check resource monitor
SHOW RESOURCE MONITORS LIKE 'MCP_DAILY_MONITOR';

-- Reset if needed (requires ACCOUNTADMIN)
ALTER RESOURCE MONITOR MCP_DAILY_MONITOR SET CREDIT_QUOTA = 10;
```

## 💰 Cost Management

- **Compute Pool**: XS instance (~$0.003/credit)
- **Daily Limit**: 10 credits (~$0.03/day)
- **Monthly Max**: ~$1/month for light usage
- **Auto-suspend**: After 60 seconds of inactivity

## 🔄 Updates and Maintenance

### Update Container Image
```bash
# Build new version
docker build -t mcp-server:v2 .

# Push to registry
docker tag mcp-server:v2 ${REGISTRY_URL}/${IMAGE_REPO}/mcp-server:v2
docker push ${REGISTRY_URL}/${IMAGE_REPO}/mcp-server:v2

# Update service
ALTER SERVICE CLAUDE_BI.PUBLIC.MCP_SERVER 
  SET image = '/CLAUDE_BI/PUBLIC/MCP_REPO/mcp-server:v2';
```

### Scale Service
```sql
-- Scale up compute pool
ALTER COMPUTE POOL MCP_COMPUTE_POOL SET MAX_NODES = 4;

-- Scale down
ALTER COMPUTE POOL MCP_COMPUTE_POOL SET MAX_NODES = 2;
```

## 🎯 Benefits of Snowpark Deployment

1. **Zero External Infrastructure**: Everything runs in Snowflake
2. **Native Security**: Snowflake handles auth, network, encryption
3. **Cost Efficiency**: Pay only for actual compute used
4. **Auto-scaling**: Snowflake manages scaling based on load
5. **High Availability**: Built-in redundancy and failover
6. **Simplified Operations**: No servers to manage
7. **Unified Billing**: All costs on Snowflake bill
8. **Compliance**: Inherits Snowflake's compliance certifications

## 📝 Notes

- The container uses local OAuth authentication (no passwords)
- All queries are automatically tagged for tracking
- Activity logging happens automatically
- Resource monitors prevent runaway costs
- The service auto-suspends when idle