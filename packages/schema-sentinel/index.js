#!/usr/bin/env node

/**
 * Schema Sentinel - Runtime Contract Validation
 * Validates that live Snowflake state matches the Activity Schema v2.0 contract
 * Used for startup validation and drift detection
 */

const { 
  CONTRACT, 
  DB, 
  SCHEMAS, 
  TABLES, 
  VIEWS,
  ACTIVITY_VIEW_MAP,
  CONTRACT_HASH,
  getContextSQL,
  createActivityName 
} = require('../snowflake-schema/generated.js');

class SchemaSentinel {
  constructor(snowflakeConnection, options = {}) {
    this.snowflake = snowflakeConnection;
    this.options = {
      throwOnDrift: options.throwOnDrift !== false, // Default true
      logActivity: options.logActivity !== false,   // Default true
      strictMode: options.strictMode || false,      // Default false
      skipViewChecks: options.skipViewChecks || false,
      ...options
    };
    
    this.validationResults = {
      passed: false,
      contractHash: CONTRACT_HASH,
      checkedAt: new Date().toISOString(),
      issues: [],
      warnings: [],
      schemaState: {}
    };
  }

  /**
   * Main validation entry point
   * Returns { passed: boolean, issues: [], warnings: [], schemaState: {} }
   */
  async validateContract() {
    console.log(`🛡️  Schema Sentinel: Validating contract ${CONTRACT_HASH}`);
    
    try {
      // Set session context first
      await this.setSessionContext();
      
      // Run validation checks
      await this.validateDatabaseAndSchemas();
      await this.validateRequiredTables();
      await this.validateActivityViews();
      await this.validateWarehouseAccess();
      await this.validatePermissions();
      
      // Determine overall status
      this.validationResults.passed = this.validationResults.issues.length === 0;
      
      // Log activity if enabled
      if (this.options.logActivity) {
        await this.logValidationActivity();
      }
      
      // Report results
      this.reportResults();
      
      // Throw if drift detected and throwOnDrift is true
      if (!this.validationResults.passed && this.options.throwOnDrift) {
        throw new Error(`Schema drift detected: ${this.validationResults.issues.length} issues found`);
      }
      
      return this.validationResults;
      
    } catch (error) {
      this.validationResults.passed = false;
      this.validationResults.issues.push({
        type: 'validation_error',
        message: `Validation failed: ${error.message}`,
        fatal: true
      });
      
      if (this.options.logActivity) {
        await this.logValidationActivity();
      }
      
      if (this.options.throwOnDrift) {
        throw error;
      }
      
      return this.validationResults;
    }
  }

  async setSessionContext() {
    console.log('🔧 Setting session context...');
    
    try {
      const contextStatements = getContextSQL({
        queryTag: `schema_sentinel_${CONTRACT_HASH}_${Date.now()}`
      });
      
      for (const sql of contextStatements) {
        await this.executeSQL(sql, 'Set context');
      }
      
      console.log('✅ Session context set');
    } catch (error) {
      this.validationResults.issues.push({
        type: 'context_error',
        message: `Failed to set session context: ${error.message}`,
        fatal: true
      });
      throw error;
    }
  }

  async validateDatabaseAndSchemas() {
    console.log('🔧 Validating database and schema structure...');
    
    try {
      // Check database exists and is accessible
      const dbResult = await this.executeSQL('SELECT CURRENT_DATABASE()', 'Check database');
      const currentDB = dbResult.rows[0]?.CURRENT_DATABASE;
      
      if (!currentDB) {
        this.validationResults.issues.push({
          type: 'database_missing',
          message: 'No current database set',
          remediation: `Run: USE DATABASE ${DB}`
        });
        return;
      }
      
      if (currentDB !== DB.replace(/process\.env\.\w+\s*\|\|\s*'([^']+)'/, '$1')) {
        this.validationResults.warnings.push({
          type: 'database_mismatch',
          message: `Expected database ${DB}, got ${currentDB}`,
          impact: 'low'
        });
      }
      
      // Check required schemas exist
      const schemaCheck = await this.executeSQL(`
        SELECT SCHEMA_NAME 
        FROM INFORMATION_SCHEMA.SCHEMATA 
        WHERE CATALOG_NAME = CURRENT_DATABASE()
          AND SCHEMA_NAME IN ('${Object.values(SCHEMAS).join("', '")}')
        ORDER BY SCHEMA_NAME
      `, 'Check schemas');
      
      const existingSchemas = new Set(schemaCheck.rows.map(row => row.SCHEMA_NAME));
      const requiredSchemas = Object.values(SCHEMAS);
      
      for (const schema of requiredSchemas) {
        if (!existingSchemas.has(schema)) {
          this.validationResults.issues.push({
            type: 'schema_missing',
            schema: schema,
            message: `Required schema ${schema} not found`,
            remediation: `Run: CREATE SCHEMA IF NOT EXISTS ${schema}`
          });
        }
      }
      
      this.validationResults.schemaState.database = currentDB;
      this.validationResults.schemaState.schemas = Array.from(existingSchemas);
      
      console.log(`✅ Database: ${currentDB}, Schemas: ${existingSchemas.size}/${requiredSchemas.length}`);
      
    } catch (error) {
      this.validationResults.issues.push({
        type: 'database_validation_error',
        message: `Database validation failed: ${error.message}`,
        fatal: true
      });
    }
  }

  async validateRequiredTables() {
    console.log('🔧 Validating required tables...');
    
    try {
      // Check ACTIVITY.EVENTS table (most critical)
      await this.validateActivityEventsTable();
      
      // Check other required tables
      for (const [schemaName, schema] of Object.entries(CONTRACT.schemas)) {
        if (!schema.tables) continue;
        
        for (const [tableName, tableDefinition] of Object.entries(schema.tables)) {
          await this.validateTable(schemaName, tableName, tableDefinition);
        }
      }
      
    } catch (error) {
      this.validationResults.issues.push({
        type: 'table_validation_error',
        message: `Table validation failed: ${error.message}`,
        fatal: true
      });
    }
  }

  async validateActivityEventsTable() {
    console.log('🔧 Validating ACTIVITY.EVENTS table...');
    
    try {
      // Check table exists
      const tableCheck = await this.executeSQL(`
        SELECT COUNT(*) as exists_count
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = 'ACTIVITY' 
          AND TABLE_NAME = 'EVENTS'
      `, 'Check ACTIVITY.EVENTS');
      
      if (tableCheck.rows[0]?.EXISTS_COUNT === 0) {
        this.validationResults.issues.push({
          type: 'critical_table_missing',
          table: 'ACTIVITY.EVENTS',
          message: 'ACTIVITY.EVENTS table not found - core functionality will fail',
          remediation: 'Run: npm run bootstrap-schema',
          fatal: true
        });
        return;
      }
      
      // Check required columns
      const columnCheck = await this.executeSQL(`
        SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'ACTIVITY' 
          AND TABLE_NAME = 'EVENTS'
        ORDER BY ORDINAL_POSITION
      `, 'Check ACTIVITY.EVENTS columns');
      
      const existingColumns = new Set(columnCheck.rows.map(row => row.COLUMN_NAME));
      const requiredColumns = CONTRACT.schemas.ACTIVITY.tables.EVENTS.required_columns.map(col => col.name);
      
      for (const requiredCol of requiredColumns) {
        if (!existingColumns.has(requiredCol)) {
          this.validationResults.issues.push({
            type: 'column_missing',
            table: 'ACTIVITY.EVENTS',
            column: requiredCol,
            message: `Required column ${requiredCol} missing from ACTIVITY.EVENTS`,
            remediation: 'Run: npm run bootstrap-schema'
          });
        }
      }
      
      // Check for data
      const dataCheck = await this.executeSQL(`
        SELECT 
          COUNT(*) as total_events,
          COUNT(DISTINCT customer) as unique_customers,
          MIN(ts) as earliest_event,
          MAX(ts) as latest_event
        FROM ACTIVITY.EVENTS
        WHERE ts >= DATEADD('day', -7, CURRENT_TIMESTAMP())
      `, 'Check ACTIVITY.EVENTS data');
      
      const stats = dataCheck.rows[0];
      this.validationResults.schemaState.activityEvents = {
        totalEvents: stats.TOTAL_EVENTS,
        uniqueCustomers: stats.UNIQUE_CUSTOMERS,
        earliestEvent: stats.EARLIEST_EVENT,
        latestEvent: stats.LATEST_EVENT
      };
      
      if (stats.TOTAL_EVENTS === 0) {
        this.validationResults.warnings.push({
          type: 'no_activity_data',
          message: 'No activity events found in last 7 days',
          impact: 'medium',
          suggestion: 'Generate activity by using the system'
        });
      }
      
      console.log(`✅ ACTIVITY.EVENTS: ${stats.TOTAL_EVENTS} events, ${stats.UNIQUE_CUSTOMERS} customers`);
      
    } catch (error) {
      this.validationResults.issues.push({
        type: 'activity_events_error',
        message: `ACTIVITY.EVENTS validation failed: ${error.message}`,
        fatal: true
      });
    }
  }

  async validateTable(schemaName, tableName, tableDefinition) {
    try {
      const tableCheck = await this.executeSQL(`
        SELECT COUNT(*) as exists_count
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = '${schemaName}' 
          AND TABLE_NAME = '${tableName}'
      `, `Check ${schemaName}.${tableName}`);
      
      if (tableCheck.rows[0]?.EXISTS_COUNT === 0) {
        this.validationResults.issues.push({
          type: 'table_missing',
          table: `${schemaName}.${tableName}`,
          message: `Table ${schemaName}.${tableName} not found`,
          remediation: 'Run appropriate bootstrap script'
        });
      }
    } catch (error) {
      this.validationResults.warnings.push({
        type: 'table_check_error',
        table: `${schemaName}.${tableName}`,
        message: `Could not validate table: ${error.message}`
      });
    }
  }

  async validateActivityViews() {
    if (this.options.skipViewChecks) {
      console.log('⏭️  Skipping view validation (skipViewChecks=true)');
      return;
    }
    
    console.log('🔧 Validating Activity views...');
    
    try {
      const viewNames = Object.keys(ACTIVITY_VIEW_MAP);
      const existingViews = [];
      const missingViews = [];
      
      for (const viewName of viewNames) {
        try {
          const viewCheck = await this.executeSQL(`
            SELECT COUNT(*) as exists_count
            FROM INFORMATION_SCHEMA.VIEWS 
            WHERE TABLE_SCHEMA = 'ACTIVITY_CCODE' 
              AND TABLE_NAME = '${viewName}'
          `, `Check view ${viewName}`);
          
          if (viewCheck.rows[0]?.EXISTS_COUNT > 0) {
            existingViews.push(viewName);
            
            // Test view accessibility
            try {
              await this.executeSQL(`SELECT COUNT(*) FROM ACTIVITY_CCODE.${viewName} LIMIT 1`, `Test ${viewName}`);
            } catch (accessError) {
              this.validationResults.warnings.push({
                type: 'view_access_error',
                view: viewName,
                message: `View exists but not accessible: ${accessError.message}`
              });
            }
          } else {
            missingViews.push(viewName);
          }
        } catch (error) {
          this.validationResults.warnings.push({
            type: 'view_check_error',
            view: viewName,
            message: `Could not check view: ${error.message}`
          });
        }
      }
      
      this.validationResults.schemaState.activityViews = {
        expected: viewNames.length,
        found: existingViews.length,
        existing: existingViews,
        missing: missingViews
      };
      
      if (missingViews.length > 0) {
        this.validationResults.issues.push({
          type: 'views_missing',
          views: missingViews,
          message: `${missingViews.length} Activity views missing: ${missingViews.join(', ')}`,
          remediation: 'Run: npm run bootstrap-activity-views'
        });
      }
      
      console.log(`✅ Activity views: ${existingViews.length}/${viewNames.length} found`);
      
    } catch (error) {
      this.validationResults.issues.push({
        type: 'view_validation_error',
        message: `View validation failed: ${error.message}`,
        fatal: false
      });
    }
  }

  async validateWarehouseAccess() {
    console.log('🔧 Validating warehouse access...');
    
    try {
      const warehouseCheck = await this.executeSQL('SELECT CURRENT_WAREHOUSE()', 'Check warehouse');
      const currentWarehouse = warehouseCheck.rows[0]?.CURRENT_WAREHOUSE;
      
      if (!currentWarehouse) {
        this.validationResults.issues.push({
          type: 'no_warehouse',
          message: 'No warehouse is active',
          remediation: 'Set SNOWFLAKE_WAREHOUSE environment variable and restart',
          fatal: true
        });
        return;
      }
      
      this.validationResults.schemaState.warehouse = currentWarehouse;
      console.log(`✅ Warehouse: ${currentWarehouse}`);
      
    } catch (error) {
      this.validationResults.issues.push({
        type: 'warehouse_error',
        message: `Warehouse validation failed: ${error.message}`,
        fatal: true
      });
    }
  }

  async validatePermissions() {
    console.log('🔧 Validating permissions...');
    
    try {
      const roleCheck = await this.executeSQL('SELECT CURRENT_ROLE()', 'Check role');
      const currentRole = roleCheck.rows[0]?.CURRENT_ROLE;
      
      this.validationResults.schemaState.role = currentRole;
      
      // Test basic permissions
      const permissions = {
        canSelect: false,
        canInsert: false,
        canCreateView: false,
        canCreateTask: false
      };
      
      // Test SELECT on ACTIVITY.EVENTS
      try {
        await this.executeSQL('SELECT COUNT(*) FROM ACTIVITY.EVENTS LIMIT 1', 'Test SELECT');
        permissions.canSelect = true;
      } catch (error) {
        this.validationResults.issues.push({
          type: 'permission_select',
          message: 'Cannot SELECT from ACTIVITY.EVENTS',
          remediation: 'Grant SELECT permissions'
        });
      }
      
      // Test INSERT permission (create a test record)
      try {
        const testId = `test_${Date.now()}`;
        await this.executeSQL(`
          INSERT INTO ACTIVITY.EVENTS (activity_id, ts, customer, activity, feature_json)
          SELECT '${testId}', CURRENT_TIMESTAMP(), 'schema_sentinel', 'ccode.permission_test', 
                 PARSE_JSON('{"test": true}')
        `, 'Test INSERT');
        
        // Clean up test record
        await this.executeSQL(`DELETE FROM ACTIVITY.EVENTS WHERE activity_id = '${testId}'`, 'Clean test');
        permissions.canInsert = true;
      } catch (error) {
        this.validationResults.warnings.push({
          type: 'permission_insert',
          message: 'Cannot INSERT into ACTIVITY.EVENTS - activity logging may fail',
          impact: 'medium'
        });
      }
      
      this.validationResults.schemaState.permissions = permissions;
      console.log(`✅ Role: ${currentRole}, Permissions: ${Object.values(permissions).filter(Boolean).length}/4`);
      
    } catch (error) {
      this.validationResults.warnings.push({
        type: 'permission_check_error',
        message: `Permission validation failed: ${error.message}`
      });
    }
  }

  async logValidationActivity() {
    try {
      const activityId = `sentinel_${Date.now()}_${Math.random().toString(36).substr(2, 8)}`;
      
      await this.executeSQL(`
        INSERT INTO ACTIVITY.EVENTS (
          activity_id, ts, customer, activity, feature_json,
          _source_system, _source_version, _query_tag
        )
        VALUES (
          '${activityId}',
          CURRENT_TIMESTAMP(),
          'schema_sentinel',
          '${createActivityName('schema_validation')}',
          PARSE_JSON('${JSON.stringify({
            contract_hash: CONTRACT_HASH,
            passed: this.validationResults.passed,
            issues_count: this.validationResults.issues.length,
            warnings_count: this.validationResults.warnings.length,
            schema_state: this.validationResults.schemaState
          })}'),
          'schema_sentinel',
          '1.0.0',
          'schema_sentinel_${CONTRACT_HASH}'
        )
      `, 'Log validation activity');
      
    } catch (error) {
      console.warn('⚠️ Failed to log validation activity:', error.message);
    }
  }

  reportResults() {
    console.log('\n📊 Schema Validation Report');
    console.log('=' .repeat(50));
    
    if (this.validationResults.passed) {
      console.log('✅ All validations passed!');
    } else {
      console.log(`❌ ${this.validationResults.issues.length} issue(s) found`);
    }
    
    if (this.validationResults.warnings.length > 0) {
      console.log(`⚠️  ${this.validationResults.warnings.length} warning(s)`);
    }
    
    // Schema state summary
    console.log('\n📋 Schema State:');
    const state = this.validationResults.schemaState;
    console.log(`   Database: ${state.database || 'unknown'}`);
    console.log(`   Warehouse: ${state.warehouse || 'unknown'}`);
    console.log(`   Role: ${state.role || 'unknown'}`);
    console.log(`   Schemas: ${state.schemas?.length || 0} found`);
    
    if (state.activityEvents) {
      console.log(`   Activity Events: ${state.activityEvents.totalEvents} (${state.activityEvents.uniqueCustomers} customers)`);
    }
    
    if (state.activityViews) {
      console.log(`   Activity Views: ${state.activityViews.found}/${state.activityViews.expected}`);
    }
    
    // List critical issues
    const criticalIssues = this.validationResults.issues.filter(issue => issue.fatal);
    if (criticalIssues.length > 0) {
      console.log('\n🚨 Critical Issues:');
      criticalIssues.forEach((issue, i) => {
        console.log(`   ${i + 1}. ${issue.message}`);
        if (issue.remediation) {
          console.log(`      Fix: ${issue.remediation}`);
        }
      });
    }
  }

  async executeSQL(sql, description = 'SQL') {
    return new Promise((resolve, reject) => {
      this.snowflake.execute({
        sqlText: sql,
        complete: (err, stmt, rows) => {
          if (err) {
            reject(err);
          } else {
            resolve({ rows: rows || [], stmt });
          }
        }
      });
    });
  }

  // Static helper for quick validation
  static async validate(snowflakeConnection, options = {}) {
    const sentinel = new SchemaSentinel(snowflakeConnection, options);
    return await sentinel.validateContract();
  }

  // Generate remediation script
  generateRemediationScript() {
    const issues = this.validationResults.issues;
    const script = ['#!/bin/bash', '# Schema Remediation Script', ''];
    
    if (issues.some(i => i.type === 'schema_missing')) {
      script.push('echo "Creating missing schemas..."');
      script.push('npm run bootstrap-schema');
      script.push('');
    }
    
    if (issues.some(i => i.type === 'views_missing')) {
      script.push('echo "Creating missing Activity views..."');
      script.push('npm run bootstrap-activity-views');
      script.push('');
    }
    
    script.push('echo "Remediation complete. Re-run validation to verify."');
    script.push('npm run validate-schema');
    
    return script.join('\n');
  }
}

module.exports = SchemaSentinel;