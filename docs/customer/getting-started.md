# Getting Started - Snowflake Enterprise Dashboard System

**Get your dashboard system running in 15 minutes**

## Prerequisites

### Required
- **Snowflake Account** (Standard edition or higher)
- **Node.js 18+** for web interface
- **Git** for cloning the repository

### For Claude Code Integration
- **RSA Key Pair** for authentication
- **Claude Code CLI** configured and working

## 🚀 Quick Setup (15 minutes)

### Step 1: Clone and Install (2 minutes)
```bash
git clone <your-repo-url>
cd SnowflakePOC2
npm install
```

### Step 2: Configure Authentication (5 minutes)

#### Option A: RSA Key-Pair (Recommended for Production)
```bash
# Generate RSA key pair (if you don't have one)
openssl genpkey -algorithm RSA -out claude_code_rsa_key.p8 -pkcs8 -pkeyopt rsa_keygen_bits:2048
openssl rsa -pubout -in claude_code_rsa_key.p8 -out claude_code_rsa_key.pub

# Add public key to Snowflake user
# In Snowflake console:
# ALTER USER CLAUDE_CODE_AI_AGENT SET RSA_PUBLIC_KEY='<your-public-key>';
```

#### Option B: Password Authentication (Development Only)
```bash
# Create .env file
echo "SNOWFLAKE_PASSWORD=your-password" > .env
```

### Step 3: Deploy the System (5 minutes)
```bash
# Deploy all procedures and infrastructure
npm run deploy:native

# Verify deployment
npm run test:integration
```

### Step 4: Start the Dashboard (1 minute)
```bash
# Start the web server
npm start

# Open the dashboard
open http://localhost:3000/dashboard.html
```

### Step 5: Verify Everything Works (2 minutes)
```bash
# Check security compliance
npm run check:guards

# Test Claude Code access
sf status
sf sql "CALL MCP.DASH_GET_METRICS(DATEADD('hour', -1, CURRENT_TIMESTAMP()), CURRENT_TIMESTAMP(), NULL)"
```

## 🎯 What You Get

### Executive Dashboard
- **Real-time Metrics**: Live system health and activity
- **Time Series Charts**: Trends over time with smart grouping
- **Top Rankings**: Most active users, frequent actions
- **Event Stream**: Real-time activity feed

### Claude Code Integration
- **Secure Access**: RSA-authenticated API calls
- **Complete Logging**: Every operation tracked
- **Five Core Procedures**: Ready for immediate use

### Compliance & Security
- **Two-Table Architecture**: Prevents schema sprawl
- **Audit Trail**: Complete activity history
- **Repository Guards**: Automated compliance checking

## 📊 Using the Dashboard

### Quick Presets
Click any preset for instant insights:
- **Today**: Last 24 hours of activity
- **Last Hour**: Recent activity with 5-minute intervals
- **This Week**: Weekly trends and patterns

### Custom Queries
Use the natural language interface:
```
"Show me the top 10 actions from yesterday"
"What were the error rates this morning?"
"Give me a time series of user activity for the last week"
```

### Real-time Monitoring
The dashboard auto-refreshes every 5 minutes. For faster updates:
```bash
# Force refresh data
sf sql "SELECT SYSTEM$REFRESH_DYNAMIC_TABLE('ACTIVITY.EVENTS')"
```

## 🔧 Common Tasks

### Adding Custom Dashboard Views
1. Create new procedure in `scripts/sql/`
2. Follow the existing patterns from `DASH_GET_SERIES`
3. Deploy with `npm run sql:deploy`
4. Add to UI in `ui/js/dashboard.js`

### Monitoring System Health
```bash
# Check all systems
npm run check:guards

# Test procedures
sf sql "CALL MCP.TEST_ALL()"

# View recent activity
sf sql "SELECT * FROM MCP.VW_CLAUDE_CODE_OPERATIONS LIMIT 10"
```

### Troubleshooting
```bash
# Check connection
sf status

# Verify tables exist
sf sql "SELECT COUNT(*) FROM ACTIVITY.EVENTS"

# Check procedure status
sf sql "SHOW PROCEDURES IN SCHEMA MCP"
```

## 🔐 Security Notes

### Key Management
- **Never commit keys to git** - they're automatically moved to `~/.snowflake-keys/`
- **Rotate keys regularly** - update both Snowflake and local files
- **Use environment variables** for production deployments

### Access Control
- **Claude Code agents** can only access via MCP procedures
- **No direct table access** - everything goes through controlled procedures
- **Complete audit trail** - every operation is logged

### Compliance
The system automatically enforces:
- **Two-Table Law**: No additional tables allowed
- **Secret Protection**: No credentials in git
- **Procedure Security**: All procedures are EXECUTE AS OWNER

## 📞 Support

### Self-Service
1. **Check the logs**: `sf sql "SELECT * FROM MCP.VW_RECENT_ERRORS"`
2. **Run diagnostics**: `npm run check:guards`
3. **Verify connection**: `sf status`

### Documentation
- [API Reference](./api-guide.md) - All dashboard procedures
- [Architecture Guide](../../CLAUDE.md) - System design principles
- [Repository Map](../REPO_MAP.md) - Navigate the codebase

### Advanced Topics
- [Performance Tuning](../development/) - Optimization techniques
- [Custom Procedures](../development/) - Adding new analytics
- [Security Configuration](../../archive/development/) - Advanced auth setup

---

## 🎉 You're Ready!

Your dashboard system is now running and ready for production use. The system will:

✅ **Auto-log all Claude Code operations**  
✅ **Provide real-time dashboard analytics**  
✅ **Enforce security and compliance automatically**  
✅ **Scale with your data and usage**  

**Next Steps**: Explore the [API documentation](./api-guide.md) to build custom analytics or check out the [repository map](../REPO_MAP.md) to understand the codebase structure.