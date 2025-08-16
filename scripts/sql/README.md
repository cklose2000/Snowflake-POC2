# SQL Procedure Variants

This directory contains different variants of dashboard procedures, each serving specific purposes in the development and deployment lifecycle.

## 🎯 Deployment Modes

### `dashboard-procs.sql` (MAIN) - 11,562 bytes
**Production default with full error handling**

- **Purpose**: All 5 procedures with comprehensive error handling
- **Use Case**: Production deployments requiring maximum stability
- **Features**: Complete error handling, full logging, input validation
- **Deploy**: `npm run sql:deploy -- main` or `npm run sql:deploy`

### `dashboard-procs-simple.sql` (DEMO) - 3,859 bytes  
**Simplified for quick demos/testing**

- **Purpose**: Minimal procedures for fast deployment and demos
- **Use Case**: Development, demos, quick testing cycles
- **Features**: Simplified logic, faster deployment, core functionality only
- **Deploy**: `npm run sql:deploy -- demo`

### `dashboard-procs-variant.sql` (VARIANT) - 7,625 bytes
**Single VARIANT parameter pattern**

- **Purpose**: For MCP integration testing with VARIANT parameters
- **Use Case**: Testing MCP integration with single VARIANT parameter pattern
- **Features**: Unified parameter interface, MCP-compatible signatures
- **Deploy**: `npm run sql:deploy -- variant`

### `dashboard-procs-fixed.sql` (HOTFIX) - 7,612 bytes
**Bug fixes for specific issues**

- **Purpose**: Contains specific bug fixes - temporary until merged to main
- **Use Case**: Production hotfixes, urgent issue resolution
- **Features**: Targeted fixes for known issues
- **Deploy**: `npm run sql:deploy -- hotfix`

### `dashboard-procs-working.sql` (WORKING) - 6,499 bytes
**Development version**

- **Purpose**: Work-in-progress procedures for development
- **Use Case**: Active development, experimental features
- **Features**: Latest development changes, may be unstable
- **Deploy**: `npm run sql:deploy -- working`

### `dashboard-procs-final.sql` (FINAL) - 7,981 bytes
**Final tested version**

- **Purpose**: Finalized procedures after testing
- **Use Case**: Pre-production testing, release candidates
- **Features**: Tested and validated changes ready for production
- **Deploy**: `npm run sql:deploy -- final`

## 🚀 Quick Usage

```bash
# List all available modes
npm run sql:deploy -- --list

# Deploy production version (default)
npm run sql:deploy

# Deploy demo version for testing
npm run sql:deploy -- demo

# Deploy with dry run (see what would be executed)
npm run sql:deploy -- main --dry-run

# Deploy specific mode
npm run sql:deploy -- --mode variant
```

## 🔍 Mode Selection Logic

The SQL deployer (`scripts/deploy/sql-deployer.js`) selects files based on mode:

```javascript
const SQL_MODES = {
  'main': { file: 'dashboard-procs.sql' },      // Production default
  'demo': { file: 'dashboard-procs-simple.sql' }, // Quick demos
  'variant': { file: 'dashboard-procs-variant.sql' }, // MCP testing
  // ... etc
};
```

## 📋 Core Procedures

All variants implement these core procedures (with varying levels of functionality):

- **`DASH_GET_SERIES`** - Time series data aggregation
- **`DASH_GET_TOPN`** - Top-N ranking queries  
- **`DASH_GET_EVENTS`** - Recent events stream
- **`DASH_GET_METRICS`** - Summary metrics calculation
- **`DASH_GET_PIVOT`** - Dynamic pivot operations

## 🛡️ Two-Table Law Compliance

All SQL variants strictly comply with the Two-Table Law:
- Only use `LANDING.RAW_EVENTS` and `ACTIVITY.EVENTS` tables
- No additional tables created or referenced
- All procedures are `EXECUTE AS OWNER`

## 🔧 Development Workflow

1. **Development**: Start with `working` mode for active development
2. **Testing**: Use `demo` mode for quick validation
3. **Bug Fixes**: Use `hotfix` mode for urgent production issues
4. **Integration**: Use `variant` mode for MCP testing
5. **Pre-Production**: Use `final` mode for release candidates
6. **Production**: Use `main` mode for stable deployments

## 🎛️ Environment Variables

The SQL deployer respects these environment variables:

- `SF_CLI` - Path to SF CLI (default: `~/bin/sf`)
- `SF_PK_PATH` - Path to RSA private key (default: `./claude_code_rsa_key.p8`)

## 📊 File Size Comparison

| Mode | File | Size (bytes) | Purpose |
|------|------|-------------|---------|
| MAIN | dashboard-procs.sql | 11,562 | Production default |
| FINAL | dashboard-procs-final.sql | 7,981 | Release candidate |
| HOTFIX | dashboard-procs-fixed.sql | 7,612 | Bug fixes |
| VARIANT | dashboard-procs-variant.sql | 7,625 | MCP testing |
| WORKING | dashboard-procs-working.sql | 6,499 | Development |
| DEMO | dashboard-procs-simple.sql | 3,859 | Quick demos |

## 🔗 Related

- [Unified CLI Documentation](../deploy/cli.js) - Main CLI dispatcher
- [Repository Guards](../checks/repo-guards.js) - Security enforcement
- [Two-Table Law](../../CLAUDE.md) - Architectural constraints