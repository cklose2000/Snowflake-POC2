# 🗺️ Repository Map - Snowflake POC2

**Your complete guide to navigating the clean, organized repository structure**

## 🚀 Quick Start Commands

```bash
# Get help with all available commands
npm run help

# Check system status and security
npm run check:guards
npm run cli:status

# Test connections
npm run test:connection
npm run test:integration

# Deploy procedures
npm run sql:deploy -- --list          # See all modes
npm run sql:deploy -- demo            # Deploy demo mode
npm run deploy:native                 # Deploy via traditional script

# Authentication variants
npm run deploy:auth:keypair           # RSA key-pair auth
npm run deploy:auth:password          # Password auth
```

## 📁 Directory Structure

```
/
├── 📁 scripts/                 # All executable scripts (organized)
│   ├── 📁 deploy/              # Deployment scripts  
│   │   ├── 🚀 cli.js           # ⭐ UNIFIED CLI DISPATCHER
│   │   ├── 🛠️ sql-deployer.js   # SQL mode selector
│   │   ├── deploy-auth-keypair.js    # RSA key-pair deployment
│   │   ├── deploy-auth-password.js   # Password deployment
│   │   ├── deploy-native-procedures.js
│   │   └── ... (all deploy-*.js files)
│   │
│   ├── 📁 checks/              # Security & validation
│   │   ├── 🛡️ repo-guards.js    # ⭐ SECURITY ENFORCER  
│   │   ├── check-events.js
│   │   ├── check-privileges.js
│   │   └── verify-logging.js
│   │
│   ├── 📁 sql/                 # SQL procedures & variants
│   │   ├── 📋 README.md        # SQL mode documentation
│   │   ├── dashboard-procs.sql (MAIN - production)
│   │   ├── dashboard-procs-simple.sql (DEMO)
│   │   ├── dashboard-procs-variant.sql (MCP testing)
│   │   └── ... (all SQL variants)
│   │
│   └── 📁 setup/               # Migration & setup tools
│       ├── refactor-safe.sh    # Zero-break migration
│       └── rollback.sh         # Emergency rollback
│
├── 📁 tests/                   # All test files (organized)
│   ├── 📁 integration/         # Integration tests
│   │   └── test-dashboard.js
│   ├── 📁 scripts/             # Test scripts
│   │   ├── test-auth-system.js
│   │   ├── test-key-connection.js
│   │   ├── test-mcp-integration.js
│   │   └── ... (all test-*.js files)
│   └── 📁 unit/                # Unit tests (for future)
│
├── 📁 packages/                # NPM workspaces (future)
│   ├── 📁 snowflake-cli/       # SF client wrapper
│   ├── 📁 claude-code-auth/    # Auth utilities  
│   ├── 📁 mcp-logger/          # Logging utilities
│   └── 📁 activation-gateway/  # Gateway service
│
├── 📁 src/                     # Core application (unchanged)
│   ├── dashboard-server.js
│   ├── nl-compiler.js
│   └── ... (existing code)
│
├── 📁 docs/                    # Documentation
│   ├── 🗺️ REPO_MAP.md          # This file!
│   ├── dashboard-guide.md      # (future - consolidated)
│   └── auth-guide.md           # (future - consolidated)
│
├── 📁 apps/                    # Applications
│   └── 📁 streamlit/           # Streamlit apps (future)
│
└── 🔗 Root symlinks            # Backward compatibility
    ├── deploy-*.js → scripts/deploy/
    ├── test-*.js → tests/scripts/  
    └── check-*.js → scripts/checks/
```

## 🎯 Command Categories

### 🚀 Deployment Commands
| Command | Purpose | Script Location |
|---------|---------|----------------|
| `npm run deploy:auth:keypair` | Deploy with RSA keys | `scripts/deploy/deploy-auth-keypair.js` |
| `npm run deploy:auth:password` | Deploy with password | `scripts/deploy/deploy-auth-password.js` |
| `npm run deploy:native` | Deploy procedures | `scripts/deploy/deploy-native-procedures.js` |
| `npm run sql:deploy` | Deploy SQL with mode selection | `scripts/deploy/sql-deployer.js` |

### 🧪 Testing Commands  
| Command | Purpose | Script Location |
|---------|---------|----------------|
| `npm run test:integration` | Full integration test | `tests/integration/test-dashboard.js` |
| `npm run test:connection` | Test SF connection | `tests/scripts/test-key-connection.js` |
| `npm run test:auth` | Test auth systems | `tests/scripts/test-auth-system.js` |
| `npm run test:mcp` | Test MCP integration | `tests/scripts/test-mcp-integration.js` |

### 🛡️ Security & Validation
| Command | Purpose | Script Location |
|---------|---------|----------------|
| `npm run check:guards` | **Run all security guards** | `scripts/checks/repo-guards.js` |
| `npm run check:events` | Check event logging | `scripts/checks/check-events.js` |
| `npm run check:privileges` | Check user privileges | `scripts/checks/check-privileges.js` |
| `npm run verify:logging` | Verify logging system | `scripts/checks/verify-logging.js` |

### 🔧 Utilities
| Command | Purpose | Script Location |
|---------|---------|----------------|
| `npm run help` | Show all commands | `scripts/deploy/cli.js` |
| `npm run cli:status` | System status check | `scripts/deploy/cli.js` |
| `npm run sql:deploy -- --list` | List SQL modes | `scripts/deploy/sql-deployer.js` |

## ⭐ Key Features

### 🚀 Unified CLI Dispatcher
**Location**: `scripts/deploy/cli.js`

Single entry point for all operations. Routes commands to appropriate scripts:

```bash
node scripts/deploy/cli.js <command>
# OR via npm:
npm run <command>
```

### 🛡️ Repository Guards (The Killer Feature)
**Location**: `scripts/checks/repo-guards.js`

Enforces critical constraints:
- ✅ **Two-Table Law**: Only `LANDING.RAW_EVENTS` & `ACTIVITY.EVENTS`
- ✅ **No secrets in git**: No `.env`, `*.p8`, `*.pem` tracked
- ✅ **Proper permissions**: All `MCP.*` procedures are `EXECUTE AS OWNER`
- ✅ **Safe agent access**: No direct `CORE` schema access

### 🛠️ SQL Mode Selector
**Location**: `scripts/deploy/sql-deployer.js`

Deploy different procedure variants:
- `main` - Production (11,562 bytes)
- `demo` - Quick demos (3,859 bytes)  
- `variant` - MCP testing (7,625 bytes)
- `hotfix` - Bug fixes (7,612 bytes)

## 🔗 Backward Compatibility

All old file paths still work via symlinks:
```bash
node deploy-auth.js          # → scripts/deploy/deploy-auth.js
node test-connection.js      # → tests/scripts/test-key-connection.js
node check-events.js         # → scripts/checks/check-events.js
```

## 🌍 Environment Variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `SF_CLI` | `~/bin/sf` | Path to Snowflake CLI |
| `SF_PK_PATH` | `./claude_code_rsa_key.p8` | RSA private key path |
| `PROC_MODE` | `main` | SQL deployment mode |

## 📋 Common Workflows

### 🔄 Daily Development
```bash
npm run cli:status          # Check system health
npm run check:guards        # Verify security & compliance
npm run test:connection     # Test SF connection
npm run sql:deploy -- demo  # Deploy demo procedures
npm run test:integration    # Run integration tests
```

### 🚀 Production Deployment
```bash
npm run check:guards        # Must pass first!
npm run test:integration    # Verify functionality  
npm run sql:deploy -- main  # Deploy production procedures
npm run deploy:native       # Deploy supporting infrastructure
```

### 🐛 Debugging Issues
```bash
npm run cli:status          # Check environment setup
npm run test:connection     # Verify SF connectivity
npm run check:events        # Check event logging
npm run verify:logging      # Verify logging system
```

### 🔧 SQL Development
```bash
npm run sql:deploy -- --list     # See available modes
npm run sql:deploy -- working    # Deploy development version
npm run test:integration         # Test changes
npm run sql:deploy -- main       # Deploy to production
```

## 🔄 Migration Status

✅ **Phase 1**: Safe migration with symlinks (zero breakage)
✅ **Phase 2**: Unified CLI dispatcher created
✅ **Phase 3**: Repository guards implemented
✅ **Phase 6**: End-to-end workflow tested

The refactor is **production-ready** with full backward compatibility!

## 🆘 Emergency Procedures

### Rollback Everything
```bash
./scripts/setup/rollback.sh
```

### Check What Changed
```bash
git status
cat refactor-manifest.json | jq '.symlinks_created'
```

### Verify System Health
```bash
npm run check:guards
npm run test:integration
```

## 📚 Related Documentation

- [Two-Table Law](../CLAUDE.md) - Architectural constraints
- [SQL Modes](../scripts/sql/README.md) - SQL variant documentation  
- [Migration Safety](../scripts/setup/) - Rollback procedures
- [Security Guards](../scripts/checks/repo-guards.js) - Enforcement details

---

🎉 **The repository is now clean, organized, and production-ready while maintaining 100% backward compatibility!**