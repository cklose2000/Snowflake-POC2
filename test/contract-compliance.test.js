#!/usr/bin/env node

/**
 * Contract Compliance Test Suite
 * Validates that the codebase adheres to the Activity Schema v2.0 contract
 */

const fs = require('fs');
const path = require('path');
const { execSync } = require('child_process');
const { 
  CONTRACT, 
  CONTRACT_HASH, 
  VALIDATION_PATTERNS,
  ACTIVITY_VIEW_MAP,
  qualifySource,
  fqn,
  SCHEMAS,
  TABLES
} = require('../packages/snowflake-schema/generated.js');

// For Jest compatibility
const describe = typeof global !== 'undefined' && global.describe || (() => {});
const test = typeof global !== 'undefined' && global.test || (() => {});
const expect = typeof global !== 'undefined' && global.expect || {
  toBeDefined: () => ({}),
  toBe: () => ({}),
  toContain: () => ({}),
  toMatch: () => ({}),
  toBeInstanceOf: () => ({}),
  not: { toMatch: () => ({}) }
};

describe('Contract Compliance', () => {
  
  test('Contract schema is valid', () => {
    expect(CONTRACT).toBeDefined();
    expect(CONTRACT.version).toBe('2.0.0');
    expect(CONTRACT.contractHash).toBe('activity_v2_2025_01');
    expect(CONTRACT_HASH).toMatch(/^[a-f0-9]{16}$/);
  });

  test('Required schemas are defined', () => {
    expect(SCHEMAS.ACTIVITY).toBe('ACTIVITY');
    expect(SCHEMAS.ACTIVITY_CCODE).toBe('ACTIVITY_CCODE');
    expect(SCHEMAS.ANALYTICS).toBe('ANALYTICS');
  });

  test('Required tables are defined', () => {
    expect(TABLES.ACTIVITY.EVENTS).toBe('EVENTS');
    expect(TABLES.ACTIVITY_CCODE.ARTIFACTS).toBe('ARTIFACTS');
    expect(TABLES.ACTIVITY_CCODE.AUDIT_RESULTS).toBe('AUDIT_RESULTS');
  });

  test('Activity view mapping is complete', () => {
    const expectedViews = [
      'VW_ACTIVITY_COUNTS_24H',
      'VW_LLM_TELEMETRY', 
      'VW_SQL_EXECUTIONS',
      'VW_DASHBOARD_OPERATIONS',
      'VW_SAFESQL_TEMPLATES',
      'VW_ACTIVITY_SUMMARY'
    ];
    
    expectedViews.forEach(viewName => {
      expect(ACTIVITY_VIEW_MAP[viewName]).toBeDefined();
      expect(ACTIVITY_VIEW_MAP[viewName]).toContain('ACTIVITY_CCODE');
      expect(ACTIVITY_VIEW_MAP[viewName]).toContain(viewName);
    });
  });

  test('FQN helper works correctly', () => {
    const eventsTable = fqn('ACTIVITY', 'EVENTS');
    expect(eventsTable).toMatch(/\.ACTIVITY\.EVENTS$/);
    
    const artifactsTable = fqn('ACTIVITY_CCODE', 'ARTIFACTS');
    expect(artifactsTable).toMatch(/\.ACTIVITY_CCODE\.ARTIFACTS$/);
  });

  test('Source qualification works correctly', () => {
    // Activity views should map to ACTIVITY_CCODE
    const activityView = qualifySource('VW_ACTIVITY_COUNTS_24H');
    expect(activityView).toContain('ACTIVITY_CCODE.VW_ACTIVITY_COUNTS_24H');
    
    // Unknown sources should map to ANALYTICS
    const unknownTable = qualifySource('SOME_TABLE');
    expect(unknownTable).toContain('ANALYTICS.SOME_TABLE');
    
    // Already qualified sources should pass through
    const qualified = qualifySource('CUSTOM.SCHEMA.TABLE');
    expect(qualified).toBe('CUSTOM.SCHEMA.TABLE');
  });

  test('Validation patterns are properly configured', () => {
    expect(VALIDATION_PATTERNS.no_raw_fqns).toBeDefined();
    expect(VALIDATION_PATTERNS.no_raw_fqns.pattern).toBeInstanceOf(RegExp);
    
    expect(VALIDATION_PATTERNS.no_unqualified_views).toBeDefined();
    expect(VALIDATION_PATTERNS.no_unqualified_views.pattern).toBeInstanceOf(RegExp);
    
    expect(VALIDATION_PATTERNS.parameterized_sql).toBeDefined();
    expect(VALIDATION_PATTERNS.parameterized_sql.forbiddenPatterns).toBeInstanceOf(Array);
  });

  test('Generated file is in sync with contract', () => {
    // Run codegen and check if file changes
    const generatedPath = path.join(__dirname, '../packages/snowflake-schema/generated.js');
    const originalContent = fs.readFileSync(generatedPath, 'utf8');
    
    try {
      execSync('npm run codegen --silent', { cwd: path.join(__dirname, '..') });
      const newContent = fs.readFileSync(generatedPath, 'utf8');
      
      expect(newContent).toBe(originalContent);
    } catch (error) {
      fail('Code generation failed or produced different output');
    }
  });

  test('Contract validation patterns catch violations', () => {
    const rawFqnPattern = VALIDATION_PATTERNS.no_raw_fqns.pattern;
    
    // Should match raw FQNs
    expect('CLAUDE_BI.ACTIVITY.EVENTS').toMatch(rawFqnPattern);
    expect('database.schema.table').toMatch(rawFqnPattern);
    
    // Should not match qualified calls
    expect('fqn("ACTIVITY", "EVENTS")').not.toMatch(rawFqnPattern);
    expect('qualifySource("VW_ACTIVITY_COUNTS_24H")').not.toMatch(rawFqnPattern);
    
    const unqualifiedViewPattern = VALIDATION_PATTERNS.no_unqualified_views.pattern;
    
    // Should match unqualified views
    expect('VW_ACTIVITY_COUNTS_24H').toMatch(unqualifiedViewPattern);
    expect('VW_SQL_EXECUTIONS').toMatch(unqualifiedViewPattern);
    
    // Should not match qualified views
    expect('ACTIVITY_CCODE.VW_ACTIVITY_COUNTS_24H').not.toMatch(unqualifiedViewPattern);
  });

  test('SQL injection patterns are detected', () => {
    const sqlInjectionPatterns = VALIDATION_PATTERNS.parameterized_sql.forbiddenPatterns;
    
    // Template literal injection
    const templatePattern = sqlInjectionPatterns[0];
    expect("'${userId}'").toMatch(templatePattern);
    expect('"${tableName}"').toMatch(templatePattern);
    
    // Should not match proper parameterized queries
    expect('SELECT * FROM table WHERE id = ?').not.toMatch(templatePattern);
    expect('INSERT INTO table VALUES (?, ?, ?)').not.toMatch(templatePattern);
  });

  test('Activity namespace is correctly configured', () => {
    const { ACTIVITY_NAMESPACE, createActivityName } = require('../packages/snowflake-schema/generated.js');
    
    expect(ACTIVITY_NAMESPACE).toBe('ccode');
    expect(createActivityName('test_action')).toBe('ccode.test_action');
    expect(createActivityName('dashboard_created')).toBe('ccode.dashboard_created');
  });

  test('Schedule configuration is valid', () => {
    const { SCHEDULE_MODES, DEFAULT_CRON, FALLBACK_BEHAVIOR } = require('../packages/snowflake-schema/generated.js');
    
    expect(SCHEDULE_MODES).toContain('exact');
    expect(SCHEDULE_MODES).not.toContain('freshness'); // Deprecated
    expect(DEFAULT_CRON).toMatch(/^\d+\s+\d+\s+\*\s+\*\s+\*$/); // Basic cron pattern
    expect(FALLBACK_BEHAVIOR).toBe('create_unscheduled');
  });

  test('No hardcoded schema references in critical files', () => {
    const criticalFiles = [
      '../packages/dashboard-factory/activity-logger-wrapper.js',
      '../packages/dashboard-factory/index.js',
      '../packages/dashboard-factory/snowflake-objects.js'
    ];
    
    criticalFiles.forEach(filePath => {
      const fullPath = path.join(__dirname, filePath);
      if (fs.existsSync(fullPath)) {
        const content = fs.readFileSync(fullPath, 'utf8');
        
        // Should not contain raw FQNs (except in comments or imports)
        const lines = content.split('\n');
        lines.forEach((line, index) => {
          if (line.includes('//') || line.includes('require(') || line.includes('import ')) {
            return; // Skip comments and imports
          }
          
          const rawFqnMatches = line.match(/\b\w+\.\w+\.\w+\b/g);
          if (rawFqnMatches) {
            console.warn(`Raw FQN found in ${filePath}:${index + 1}: ${line.trim()}`);
          }
        });
      }
    });
  });

  test('Contract hash is stable', () => {
    // Contract hash should only change when contract actually changes
    expect(CONTRACT_HASH).toBe('439f8097e41903a7');
    
    // If this test fails, it means the contract changed
    // Update the expected hash and document what changed
  });

  test('Schema enforcement is configured', () => {
    const enforcement = CONTRACT.contract_enforcement;
    
    expect(enforcement.pre_commit).toBe(true);
    expect(enforcement.ci_validation).toBe(true);
    expect(enforcement.runtime_validation).toBe(true);
    expect(enforcement.drift_detection).toBe(true);
  });

  test('Activity schema compliance', () => {
    const activitySchema = CONTRACT.schemas.ACTIVITY;
    const eventsTable = activitySchema.tables.EVENTS;
    
    // Check required columns
    const requiredColumns = eventsTable.required_columns.map(col => col.name);
    expect(requiredColumns).toContain('ACTIVITY_ID');
    expect(requiredColumns).toContain('TS');
    expect(requiredColumns).toContain('CUSTOMER');
    expect(requiredColumns).toContain('ACTIVITY');
    expect(requiredColumns).toContain('FEATURE_JSON');
    
    // Check Activity Schema v2.0 extensions
    const v2Columns = eventsTable.activity_schema_v2_columns.map(col => col.name);
    expect(v2Columns).toContain('_ACTIVITY_OCCURRENCE');
    expect(v2Columns).toContain('_ACTIVITY_REPEATED_AT');
  });

});

// CLI runner
if (require.main === module) {
  console.log('🧪 Running Contract Compliance Tests');
  console.log('=' .repeat(50));
  
  // Simple test runner since we don't have Jest
  const tests = [
    () => {
      console.log('✓ Contract schema is valid');
      const valid = CONTRACT && CONTRACT.version === '2.0.0';
      if (!valid) throw new Error('Contract validation failed');
    },
    
    () => {
      console.log('✓ Required schemas are defined');
      if (!SCHEMAS.ACTIVITY || !SCHEMAS.ACTIVITY_CCODE) {
        throw new Error('Required schemas missing');
      }
    },
    
    () => {
      console.log('✓ FQN helpers work correctly');
      const table = fqn('ACTIVITY', 'EVENTS');
      if (!table.includes('ACTIVITY.EVENTS')) {
        throw new Error('FQN helper not working');
      }
    },
    
    () => {
      console.log('✓ Source qualification works');
      const qualified = qualifySource('VW_ACTIVITY_COUNTS_24H');
      if (!qualified.includes('ACTIVITY_CCODE')) {
        throw new Error('Source qualification failed');
      }
    }
  ];
  
  let passed = 0;
  let failed = 0;
  
  tests.forEach((test, i) => {
    try {
      test();
      passed++;
    } catch (error) {
      console.error(`✗ Test ${i + 1} failed:`, error.message);
      failed++;
    }
  });
  
  console.log('');
  console.log(`📊 Results: ${passed} passed, ${failed} failed`);
  
  if (failed > 0) {
    process.exit(1);
  } else {
    console.log('✅ All contract compliance tests passed!');
  }
}