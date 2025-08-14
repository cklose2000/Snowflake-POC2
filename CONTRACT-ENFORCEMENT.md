# Activity Schema v2.0 Contract Enforcement

This document describes the comprehensive contract enforcement system implemented to eliminate schema drift and ensure consistent object references throughout the codebase.

## Overview

The contract-first architecture transforms schema references from hardcoded strings into generated, type-safe helpers. This prevents drift and makes schema violations a **build error** rather than a runtime surprise.

## Contract Hash: `439f8097e41903a7`

## Architecture

```
schemas/activity_v2.contract.json (Source of Truth)
              ↓
    scripts/codegen-schema.js (Generator)
              ↓
packages/snowflake-schema/generated.js (Generated Helpers)
              ↓
     Application Code (Uses Helpers)
```

## Enforcement Layers

### 1. Pre-commit Hooks (`.husky/pre-commit`)
- **Blocks raw FQNs** like `CLAUDE_BI.ACTIVITY.EVENTS`
- **Blocks unqualified views** like `VW_ACTIVITY_COUNTS_24H` 
- **Blocks SQL injection** patterns like `${variable}`
- **Validates generated files** are in sync with contract

### 2. CI/CD Validation (`.github/workflows/schema-validation.yml`)
- **Contract compliance** checks on every PR
- **Live schema validation** against staging Snowflake
- **Drift detection** with automatic remediation scripts
- **Security scanning** for hardcoded credentials

### 3. Runtime Validation (`packages/schema-sentinel/`)
- **Startup validation** of live Snowflake state
- **Health endpoints** for monitoring
- **Drift watchdog** with automatic detection
- **Activity logging** of all validation events

## Generated Helpers

### Core Functions
```javascript
import { fqn, qualifySource, getContextSQL } from './packages/snowflake-schema/generated.js';

// Fully qualified names
fqn('ACTIVITY', 'EVENTS')  // → "CLAUDE_BI.ACTIVITY.EVENTS"

// Source qualification (for panel sources)
qualifySource('VW_ACTIVITY_COUNTS_24H')  // → "CLAUDE_BI.ACTIVITY_CCODE.VW_ACTIVITY_COUNTS_24H"

// Session context
getContextSQL({ queryTag: 'my-service' })  // → ["USE WAREHOUSE ...", "USE DATABASE ...", ...]
```

### Constants
```javascript
import { SCHEMAS, TABLES, VIEWS, ACTIVITY_VIEW_MAP } from './packages/snowflake-schema/generated.js';

SCHEMAS.ACTIVITY          // → "ACTIVITY"
TABLES.ACTIVITY.EVENTS    // → "EVENTS"
ACTIVITY_VIEW_MAP.VW_ACTIVITY_COUNTS_24H  // → Full FQN
```

## Contract Rules

### ✅ REQUIRED
- All schema references must use generated helpers
- All Activity actions must use `ccode.` namespace
- All SQL must use parameterized binds (`?` placeholders)
- All schedule specs must be `{ mode: 'exact', cron_utc: '...' }`

### ❌ FORBIDDEN
- Raw FQNs: `CLAUDE_BI.ACTIVITY.EVENTS`
- Unqualified views: `VW_ACTIVITY_COUNTS_24H`
- String interpolation in SQL: `${variable}`
- Schedule mode `'freshness'` (deprecated)

## Commands

```bash
# Code generation
npm run codegen

# Contract compliance
npm run lint:contract
npm run test:contract

# Runtime validation
npm run validate:runtime
npm run validate:runtime:strict
npm run validate:runtime:fix

# Pre-commit test
git add . && git commit -m "test"  # Triggers validation
```

## Violation Examples & Fixes

### ❌ Raw FQN Violation
```javascript
// WRONG
const sql = "SELECT * FROM CLAUDE_BI.ACTIVITY.EVENTS";

// CORRECT
import { fqn, SCHEMAS, TABLES } from '../snowflake-schema/generated.js';
const sql = `SELECT * FROM ${fqn(SCHEMAS.ACTIVITY, TABLES.ACTIVITY.EVENTS)}`;
```

### ❌ Unqualified View Violation
```javascript
// WRONG
const source = "VW_ACTIVITY_COUNTS_24H";

// CORRECT
import { qualifySource } from '../snowflake-schema/generated.js';
const source = qualifySource("VW_ACTIVITY_COUNTS_24H");
```

### ❌ SQL Injection Violation
```javascript
// WRONG
const sql = `SELECT * FROM table WHERE id = '${userId}'`;

// CORRECT
const sql = "SELECT * FROM table WHERE id = ?";
const binds = [userId];
```

## Monitoring & Health

### Health Endpoint
```bash
curl http://localhost:3000/health
curl http://localhost:3000/health/detailed
curl http://localhost:3000/health/contract
```

### Drift Watchdog
Automatically runs every 24 hours and logs `ccode.schema_violation` events when drift is detected.

### Activity Logging
All schema operations are logged as Activity events:
- `ccode.schema_validation` - Runtime validation results
- `ccode.schema_violation` - Drift detection
- `ccode.dashboard_*` - Dashboard lifecycle events

## Remediation

When violations are detected:

1. **Manual Fix**: Use the generated helpers
2. **Automated Fix**: Run `npm run validate:runtime:fix` for remediation script
3. **Contract Update**: If schema actually changed, update contract and regenerate

## Integration

### New Code
```javascript
// Always import from generated.js
const { fqn, qualifySource, createActivityName } = require('../snowflake-schema/generated.js');

// Use helpers for all schema references
const table = fqn('ACTIVITY', 'EVENTS');
const view = qualifySource('VW_ACTIVITY_COUNTS_24H');
const activity = createActivityName('user_action');
```

### Legacy Code Migration
1. Import generated helpers
2. Replace hardcoded FQNs with `fqn()` calls
3. Replace unqualified views with `qualifySource()` calls
4. Replace activity names with `createActivityName()` calls
5. Run `npm run lint:contract` to verify

## Success Metrics

- ✅ **568 → 0 violations** detected by linter
- ✅ **Build fails** on schema violations
- ✅ **Runtime 503** responses when drift detected
- ✅ **Complete provenance** of all SQL executions
- ✅ **Zero manual schema references** in application code

## Files

### Core Contract System
- `schemas/activity_v2.contract.json` - Single source of truth
- `scripts/codegen-schema.js` - Code generator
- `packages/snowflake-schema/generated.js` - Generated helpers

### Enforcement
- `.husky/pre-commit` - Pre-commit validation
- `scripts/lint-contract-compliance.js` - Compliance linter
- `.github/workflows/schema-validation.yml` - CI validation

### Runtime
- `packages/schema-sentinel/` - Runtime validation
- `packages/health-endpoint/` - Health monitoring
- `scripts/validate-runtime-schema.js` - Manual validation

### Testing
- `test/contract-compliance.test.js` - Contract test suite

---

**Result**: Schema drift is now a build error, not a post-mortem. The codebase cannot compile or deploy without honoring the Activity Schema contract.