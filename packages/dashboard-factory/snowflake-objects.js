// Snowflake Object Manager - Creates Views, Tasks, and Dynamic Tables
// Implements binary scheduling: Tasks vs Dynamic Tables based on spec.schedule.mode

const { generateObjectNames } = require('./schema');

class SnowflakeObjectManager {
  constructor(snowflakeConnection) {
    this.snowflake = snowflakeConnection;
    this.version = '1.0.0';
    
    // Track created objects for cleanup
    this.createdObjects = new Map();
    
    // Preflight check requirements
    this.preflightChecks = {
      change_tracking: 'SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE CHANGE_TRACKING = \'ON\' LIMIT 1',
      dynamic_tables_feature: 'SHOW PARAMETERS LIKE \'ENABLE_DYNAMIC_TABLES_FEATURE\'',
      warehouse_exists: 'SHOW WAREHOUSES',
      schema_permissions: 'SELECT CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_SCHEMA()'
    };
  }

  // Main creation method: spec → Snowflake objects
  async createDashboardObjects(spec) {
    console.log(`🏗️ Creating Snowflake objects for dashboard: ${spec.name}`);
    
    const startTime = Date.now();
    const results = {
      objectsCreated: 0,
      views: [],
      tasks: [],
      dynamicTables: [],
      errors: []
    };

    try {
      // Create base views for each panel
      for (const panel of spec.panels) {
        const panelResult = await this.createPanelObjects(spec, panel);
        
        results.views.push(...panelResult.views);
        results.tasks.push(...panelResult.tasks);
        results.dynamicTables.push(...panelResult.dynamicTables);
        results.objectsCreated += panelResult.count;
      }

      // Create warehouse and resource monitor if needed
      const infraResult = await this.createInfrastructure(spec);
      results.objectsCreated += infraResult.count;

      console.log(`✅ Created ${results.objectsCreated} objects in ${Date.now() - startTime}ms`);
      return results;

    } catch (error) {
      console.error(`❌ Object creation failed: ${error.message}`);
      
      // Attempt cleanup on failure
      try {
        await this.dropDashboardObjects(spec);
      } catch (cleanupError) {
        console.error(`❌ Cleanup failed: ${cleanupError.message}`);
      }
      
      throw error;
    }
  }

  // Create objects for a single panel
  async createPanelObjects(spec, panel) {
    const objectNames = generateObjectNames(spec, panel.id);
    const results = { views: [], tasks: [], dynamicTables: [], count: 0 };

    console.log(`📊 Creating objects for panel: ${panel.id}`);

    // Step 1: Create base view with SafeSQL template
    const baseViewSQL = this.generateBaseViewSQL(panel, objectNames.base_table);
    await this.executeSQL(baseViewSQL, `CREATE VIEW ${objectNames.base_table}`);
    results.views.push(objectNames.base_table);
    results.count++;

    // Step 2: Create top-N view if needed
    if (panel.top_n && panel.type === 'table') {
      const topViewSQL = this.generateTopViewSQL(panel, objectNames);
      await this.executeSQL(topViewSQL, `CREATE VIEW ${objectNames.top_view}`);
      results.views.push(objectNames.top_view);
      results.count++;
    }

    // Step 3: Create scheduling object based on mode
    if (spec.schedule.mode === 'exact') {
      // Use Tasks for exact scheduling
      const taskSQL = this.generateTaskSQL(spec, panel, objectNames);
      await this.executeSQL(taskSQL, `CREATE TASK ${objectNames.task}`);
      results.tasks.push(objectNames.task);
      results.count++;
    } else if (spec.schedule.mode === 'freshness') {
      // Use Dynamic Tables for freshness-based scheduling
      const dynamicTableSQL = this.generateDynamicTableSQL(spec, panel, objectNames);
      await this.executeSQL(dynamicTableSQL, `CREATE DYNAMIC TABLE ${objectNames.dynamic_table}`);
      results.dynamicTables.push(objectNames.dynamic_table);
      results.count++;
    }

    return results;
  }

  // Generate base view SQL using SafeSQL patterns
  generateBaseViewSQL(panel, viewName) {
    const { metric, source, group_by, window } = panel;
    
    // Build time window filter
    let timeFilter = '';
    if (window) {
      if (window.days) {
        timeFilter = `AND order_date >= CURRENT_DATE - ${window.days}`;
      } else if (window.weeks) {
        timeFilter = `AND order_date >= CURRENT_DATE - ${window.weeks * 7}`;
      } else if (window.months) {
        timeFilter = `AND order_date >= ADD_MONTHS(CURRENT_DATE, -${window.months})`;
      } else if (window.quarters) {
        timeFilter = `AND order_date >= ADD_MONTHS(CURRENT_DATE, -${window.quarters * 3})`;
      } else if (window.years) {
        timeFilter = `AND order_date >= ADD_YEARS(CURRENT_DATE, -${window.years})`;
      }
    }

    // Build GROUP BY clause
    const groupByColumns = group_by || ['customer_name'];
    const groupByClause = groupByColumns.join(', ');
    const selectColumns = groupByColumns.map(col => `${col}`).join(', ');

    return `
      CREATE OR REPLACE VIEW ${viewName} AS
      SELECT 
        ${selectColumns},
        ${metric} as metric_value,
        COUNT(*) as row_count,
        MAX(order_date) as last_update_date
      FROM ${source}
      WHERE 1=1 
        ${timeFilter}
      GROUP BY ${groupByClause}
      ORDER BY metric_value DESC
    `.trim();
  }

  // Generate top-N view SQL
  generateTopViewSQL(panel, objectNames) {
    return `
      CREATE OR REPLACE VIEW ${objectNames.top_view} AS
      SELECT * FROM ${objectNames.base_table}
      ORDER BY metric_value DESC
      LIMIT ${panel.top_n}
    `.trim();
  }

  // Generate Task SQL for exact scheduling
  generateTaskSQL(spec, panel, objectNames) {
    const { cron_utc } = spec.schedule;
    const refreshTarget = panel.top_n ? objectNames.top_view : objectNames.base_table;
    
    return `
      CREATE OR REPLACE TASK ${objectNames.task}
        WAREHOUSE = '${generateObjectNames(spec).warehouse}'
        SCHEDULE = 'USING CRON ${cron_utc} UTC'
      AS
        -- Refresh data by recreating the view with current timestamp
        CREATE OR REPLACE VIEW ${refreshTarget} AS (
          SELECT 
            *,
            CURRENT_TIMESTAMP as refresh_timestamp
          FROM (
            ${this.generateBaseViewSQL(panel, 'temp_base').replace('CREATE OR REPLACE VIEW temp_base AS', '')}
          )
          ${panel.top_n ? `ORDER BY metric_value DESC LIMIT ${panel.top_n}` : ''}
        )
    `.trim();
  }

  // Generate Dynamic Table SQL for freshness scheduling
  generateDynamicTableSQL(spec, panel, objectNames) {
    const { target_lag } = spec.schedule;
    
    return `
      CREATE OR REPLACE DYNAMIC TABLE ${objectNames.dynamic_table}
        TARGET_LAG = '${target_lag}'
        WAREHOUSE = '${generateObjectNames(spec).warehouse}'
        AS (
          ${this.generateBaseViewSQL(panel, 'temp_base').replace('CREATE OR REPLACE VIEW temp_base AS', '')}
          ${panel.top_n ? `ORDER BY metric_value DESC LIMIT ${panel.top_n}` : ''}
        )
    `.trim();
  }

  // Create infrastructure objects (warehouse, resource monitor)
  async createInfrastructure(spec) {
    const objectNames = generateObjectNames(spec);
    let count = 0;

    try {
      // Create dedicated warehouse for dashboard
      const warehouseSQL = `
        CREATE WAREHOUSE IF NOT EXISTS ${objectNames.warehouse}
        WITH 
          WAREHOUSE_SIZE = 'XSMALL'
          AUTO_SUSPEND = 60
          AUTO_RESUME = TRUE
          INITIALLY_SUSPENDED = TRUE
          COMMENT = 'Dashboard warehouse for ${spec.name}'
      `;
      await this.executeSQL(warehouseSQL, `CREATE WAREHOUSE ${objectNames.warehouse}`);
      count++;

      // Create resource monitor for cost control
      const resourceMonitorSQL = `
        CREATE RESOURCE MONITOR IF NOT EXISTS ${objectNames.resource_monitor}
        WITH 
          CREDIT_QUOTA = 10
          FREQUENCY = 'DAILY'
          START_TIMESTAMP = CURRENT_TIMESTAMP
          NOTIFY_AT = 80, 100
          TRIGGERS 
            ON 100 PERCENT DO SUSPEND
      `;
      await this.executeSQL(resourceMonitorSQL, `CREATE RESOURCE MONITOR ${objectNames.resource_monitor}`);
      count++;

      // Apply resource monitor to warehouse
      const applyMonitorSQL = `ALTER WAREHOUSE ${objectNames.warehouse} SET RESOURCE_MONITOR = '${objectNames.resource_monitor}'`;
      await this.executeSQL(applyMonitorSQL, `APPLY RESOURCE MONITOR`);

    } catch (error) {
      console.log(`⚠️ Infrastructure creation warning: ${error.message}`);
      // Continue - infrastructure is optional
    }

    return { count };
  }

  // Run preflight checks before object creation with fail-fast validation
  async runPreflightChecks(spec) {
    console.log(`🔍 Running preflight checks for: ${spec.name}`);
    
    const results = {
      passed: true,
      issues: [],
      warnings: [],
      checks: {},
      cost_estimate: 0,
      estimated_objects: 0
    };

    try {
      // Check 1: Privileges (CREATE VIEW, TASK, STREAMLIT)
      console.log('🔧 Checking privileges...');
      try {
        const privilegeChecks = [
          `SHOW GRANTS TO ROLE ${results.checks.permissions?.CURRENT_ROLE || 'CURRENT_ROLE'}`,
          'SELECT CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE()'
        ];
        
        const permissionResult = await this.executeSQL(privilegeChecks[1], 'CHECK PERMISSIONS');
        results.checks.permissions = permissionResult.resultSet[0];
        console.log(`✅ Current context: ${results.checks.permissions.CURRENT_ROLE}`);
        
        // Test CREATE VIEW privilege
        try {
          const testViewSQL = 'CREATE OR REPLACE VIEW test_dashboard_privilege_check AS SELECT 1 as test';
          await this.executeSQL(testViewSQL, 'TEST CREATE VIEW');
          await this.executeSQL('DROP VIEW IF EXISTS test_dashboard_privilege_check', 'CLEANUP TEST VIEW');
          console.log(`✅ CREATE VIEW privilege confirmed`);
        } catch (error) {
          results.issues.push('insufficient_view_privileges');
          results.passed = false;
          console.log(`❌ CREATE VIEW privilege missing: ${error.message}`);
        }
        
        // Test warehouse usability
        const warehouseName = results.checks.permissions.CURRENT_WAREHOUSE;
        if (!warehouseName) {
          results.issues.push('no_warehouse_context');
          results.passed = false;
          console.log(`❌ No warehouse in current context`);
        } else {
          console.log(`✅ Warehouse context: ${warehouseName}`);
        }
      } catch (error) {
        results.issues.push('permission_check_failed');
        results.passed = false;
        console.log(`❌ Permission check failed: ${error.message}`);
      }

      // Check 2: Dynamic Tables prerequisites (if DT mode)
      if (spec.schedule.mode === 'freshness') {
        console.log('🔧 Checking Dynamic Tables prerequisites...');
        try {
          // Check if any source tables have change tracking
          const changeTrackingChecks = [];
          for (const panel of spec.panels) {
            const checkSQL = `
              SELECT TABLE_NAME, CHANGE_TRACKING 
              FROM INFORMATION_SCHEMA.TABLES 
              WHERE TABLE_NAME = '${panel.source.toUpperCase()}' 
                AND CHANGE_TRACKING = 'ON'
            `;
            changeTrackingChecks.push(this.executeSQL(checkSQL, `CHECK CHANGE TRACKING ${panel.source}`));
          }
          
          const changeTrackingResults = await Promise.all(changeTrackingChecks);
          const hasChangeTracking = changeTrackingResults.some(result => result.resultSet?.length > 0);
          
          if (!hasChangeTracking) {
            results.issues.push('change_tracking_missing');
            results.passed = false;
            console.log(`❌ No source tables have change tracking enabled - will fallback to Tasks`);
            // Auto-fallback suggestion
            results.fallback_suggestion = {
              from: 'freshness',
              to: 'exact',
              reason: 'change_tracking_missing'
            };
          } else {
            console.log(`✅ Change tracking available on source tables`);
          }
        } catch (error) {
          results.warnings.push('change_tracking_check_failed');
          console.log(`⚠️ Change tracking check failed: ${error.message}`);
        }
      }

      // Check 3: Name collisions (idempotent naming check)
      console.log('🔧 Checking for name collisions...');
      const specHash = require('./schema').generateSpecHash(spec);
      const objectNames = require('./schema').generateObjectNames(spec);
      
      try {
        // Check if any objects with this spec hash already exist
        const existingObjects = [];
        const objectChecks = [
          `SELECT COUNT(*) as count FROM INFORMATION_SCHEMA.VIEWS WHERE VIEW_NAME LIKE '%${specHash}'`,
          `SELECT COUNT(*) as count FROM INFORMATION_SCHEMA.TASKS WHERE NAME LIKE '%${specHash}'`
        ];
        
        for (const checkSQL of objectChecks) {
          try {
            const result = await this.executeSQL(checkSQL, 'CHECK EXISTING OBJECTS');
            if (result.resultSet[0]?.COUNT > 0) {
              existingObjects.push(checkSQL.includes('VIEWS') ? 'views' : 'tasks');
            }
          } catch (error) {
            console.log(`⚠️ Object existence check failed: ${error.message}`);
          }
        }
        
        if (existingObjects.length > 0) {
          results.warnings.push('objects_exist_will_replace');
          console.log(`⚠️ Existing objects found with hash ${specHash} - will be replaced`);
        } else {
          console.log(`✅ No name collisions detected for hash ${specHash}`);
        }
        
        results.checks.spec_hash = specHash;
        results.checks.object_names = objectNames;
      } catch (error) {
        results.warnings.push('name_collision_check_failed');
        console.log(`⚠️ Name collision check failed: ${error.message}`);
      }

      // Check 4: Cost estimate (quick byte scan guard)
      console.log('🔧 Estimating cost and data size...');
      let totalEstimatedBytes = 0;
      let estimatedObjects = 0;
      
      for (const panel of spec.panels) {
        try {
          // Quick sample to estimate data size
          const sampleSQL = `
            SELECT 
              COUNT(*) as estimated_rows,
              COUNT(*) * 100 as estimated_bytes  -- Rough estimate
            FROM ${panel.source} 
            ${panel.window ? this.buildTimeFilter(panel.window) : ''}
            LIMIT 1000
          `;
          
          const sampleResult = await this.executeSQL(sampleSQL, `ESTIMATE SIZE ${panel.source}`);
          const estimatedRows = sampleResult.resultSet[0]?.ESTIMATED_ROWS || 0;
          const estimatedBytes = sampleResult.resultSet[0]?.ESTIMATED_BYTES || 0;
          
          totalEstimatedBytes += estimatedBytes;
          estimatedObjects += (panel.top_n ? 2 : 1) + 1; // views + task/dt
          
          results.checks[`panel_${panel.id}_size`] = {
            estimated_rows: estimatedRows,
            estimated_bytes: estimatedBytes,
            has_window: !!panel.window,
            top_n: panel.top_n
          };
          
          // Warn on large datasets
          if (estimatedBytes > 10000000) { // >10MB
            results.warnings.push(`large_dataset_${panel.id}`);
            console.log(`⚠️ Panel ${panel.id} processes large dataset: ${Math.round(estimatedBytes/1000000)}MB`);
          } else {
            console.log(`✅ Panel ${panel.id} size estimate: ${Math.round(estimatedBytes/1000)}KB`);
          }
          
        } catch (error) {
          results.warnings.push(`size_estimate_failed_${panel.source}`);
          console.log(`⚠️ Size estimation failed for ${panel.source}: ${error.message}`);
          estimatedObjects += 2; // Default estimate
        }
      }
      
      // Cost estimate (very rough)
      results.cost_estimate = Math.max(totalEstimatedBytes / 10000000 * 0.01, 0.001); // ~$0.01 per 10MB
      results.estimated_objects = estimatedObjects;
      
      const maxCost = parseFloat(process.env.MAX_DASHBOARD_COST_USD) || 0.10;
      if (results.cost_estimate > maxCost) {
        results.warnings.push('high_cost_estimate');
        console.log(`⚠️ Estimated cost $${results.cost_estimate.toFixed(3)} exceeds limit $${maxCost}`);
      } else {
        console.log(`✅ Estimated cost: $${results.cost_estimate.toFixed(3)} (${estimatedObjects} objects)`);
      }

      // Check 5: Source table accessibility
      console.log('🔧 Verifying source table access...');
      for (const panel of spec.panels) {
        try {
          const tableCheckSQL = `SELECT COUNT(*) as row_count FROM ${panel.source} LIMIT 1`;
          const tableResult = await this.executeSQL(tableCheckSQL, `CHECK TABLE ${panel.source}`);
          results.checks[`table_${panel.source}`] = {
            accessible: true,
            sample_count: tableResult.resultSet[0]?.ROW_COUNT || 0
          };
          console.log(`✅ Table ${panel.source} accessible (${results.checks[`table_${panel.source}`].sample_count} rows sampled)`);
        } catch (error) {
          results.issues.push(`source_table_inaccessible_${panel.source}`);
          results.passed = false;
          results.checks[`table_${panel.source}`] = {
            accessible: false,
            error: error.message
          };
          console.log(`❌ Table ${panel.source} not accessible: ${error.message}`);
        }
      }

    } catch (error) {
      results.issues.push('preflight_error');
      results.passed = false;
      console.error(`❌ Preflight check error: ${error.message}`);
    }

    console.log(`🔍 Preflight results: ${results.passed ? 'PASSED' : 'FAILED'} (${results.issues.length} issues, ${results.warnings.length} warnings)`);
    console.log(`📊 Estimated: ${results.estimated_objects} objects, $${results.cost_estimate.toFixed(3)} cost`);
    return results;
  }

  // Drop all objects for a dashboard (cleanup)
  async dropDashboardObjects(spec) {
    console.log(`🗑️ Dropping objects for dashboard: ${spec.name}`);
    
    let objectsDropped = 0;
    const errors = [];

    try {
      // Drop panel objects
      for (const panel of spec.panels) {
        const objectNames = generateObjectNames(spec, panel.id);
        
        // Drop in reverse order of creation
        const dropCommands = [
          { sql: `DROP TASK IF EXISTS ${objectNames.task}`, type: 'task' },
          { sql: `DROP DYNAMIC TABLE IF EXISTS ${objectNames.dynamic_table}`, type: 'dynamic_table' },
          { sql: `DROP VIEW IF EXISTS ${objectNames.top_view}`, type: 'view' },
          { sql: `DROP VIEW IF EXISTS ${objectNames.base_table}`, type: 'view' }
        ];

        for (const cmd of dropCommands) {
          try {
            await this.executeSQL(cmd.sql, `DROP ${cmd.type.toUpperCase()}`);
            objectsDropped++;
          } catch (error) {
            errors.push(`${cmd.type}: ${error.message}`);
            // Continue with other drops
          }
        }
      }

      // Drop infrastructure (optional - may be shared)
      const objectNames = generateObjectNames(spec);
      const infraDropCommands = [
        { sql: `ALTER WAREHOUSE IF EXISTS ${objectNames.warehouse} UNSET RESOURCE_MONITOR`, type: 'warehouse_monitor' },
        { sql: `DROP RESOURCE MONITOR IF EXISTS ${objectNames.resource_monitor}`, type: 'resource_monitor' },
        { sql: `DROP WAREHOUSE IF EXISTS ${objectNames.warehouse}`, type: 'warehouse' }
      ];

      for (const cmd of infraDropCommands) {
        try {
          await this.executeSQL(cmd.sql, `DROP ${cmd.type.toUpperCase()}`);
          objectsDropped++;
        } catch (error) {
          console.log(`⚠️ Infrastructure drop warning: ${error.message}`);
          // Infrastructure drops are best-effort
        }
      }

    } catch (error) {
      console.error(`❌ Drop operation error: ${error.message}`);
      errors.push(`general: ${error.message}`);
    }

    console.log(`🗑️ Dropped ${objectsDropped} objects with ${errors.length} errors`);
    return { objectsDropped, errors };
  }

  // Build time filter clause for window specifications
  buildTimeFilter(window) {
    if (!window) return '';
    
    let filterClause = 'WHERE ';
    if (window.days) {
      filterClause += `order_date >= CURRENT_DATE - ${window.days}`;
    } else if (window.weeks) {
      filterClause += `order_date >= CURRENT_DATE - ${window.weeks * 7}`;
    } else if (window.months) {
      filterClause += `order_date >= ADD_MONTHS(CURRENT_DATE, -${window.months})`;
    } else if (window.quarters) {
      filterClause += `order_date >= ADD_MONTHS(CURRENT_DATE, -${window.quarters * 3})`;
    } else if (window.years) {
      filterClause += `order_date >= ADD_YEARS(CURRENT_DATE, -${window.years})`;
    } else {
      return ''; // No valid window specification
    }
    
    return filterClause;
  }

  // Execute SQL with error handling and logging
  async executeSQL(sql, description = 'SQL') {
    try {
      console.log(`🔧 ${description}...`);
      // Context is set once at connection time
      const result = await this.snowflake.execute({ sqlText: sql });
      return result;
    } catch (error) {
      console.error(`❌ ${description} failed: ${error.message}`);
      throw error;
    }
  }

  // Get manager version and capabilities
  getVersion() {
    return {
      version: this.version,
      capabilities: {
        scheduling_modes: ['exact', 'freshness'],
        object_types: ['views', 'tasks', 'dynamic_tables', 'warehouses', 'resource_monitors'],
        preflight_checks: Object.keys(this.preflightChecks).length,
        safesql_templates: true,
        idempotent_naming: true,
        cleanup_support: true
      }
    };
  }

  // Test object creation without actually creating (dry run)
  async dryRunCreation(spec) {
    console.log(`🧪 Dry run for dashboard: ${spec.name}`);
    
    const plan = {
      objects_to_create: 0,
      views: [],
      tasks: [],
      dynamic_tables: [],
      sql_statements: []
    };

    // Simulate object creation for each panel
    for (const panel of spec.panels) {
      const objectNames = generateObjectNames(spec, panel.id);
      
      // Base view
      plan.views.push(objectNames.base_table);
      plan.sql_statements.push({
        type: 'CREATE VIEW',
        name: objectNames.base_table,
        sql: this.generateBaseViewSQL(panel, objectNames.base_table)
      });
      plan.objects_to_create++;

      // Top view if needed
      if (panel.top_n && panel.type === 'table') {
        plan.views.push(objectNames.top_view);
        plan.sql_statements.push({
          type: 'CREATE VIEW',
          name: objectNames.top_view,
          sql: this.generateTopViewSQL(panel, objectNames)
        });
        plan.objects_to_create++;
      }

      // Scheduling object
      if (spec.schedule.mode === 'exact') {
        plan.tasks.push(objectNames.task);
        plan.sql_statements.push({
          type: 'CREATE TASK',
          name: objectNames.task,
          sql: this.generateTaskSQL(spec, panel, objectNames)
        });
        plan.objects_to_create++;
      } else if (spec.schedule.mode === 'freshness') {
        plan.dynamic_tables.push(objectNames.dynamic_table);
        plan.sql_statements.push({
          type: 'CREATE DYNAMIC TABLE',
          name: objectNames.dynamic_table,
          sql: this.generateDynamicTableSQL(spec, panel, objectNames)
        });
        plan.objects_to_create++;
      }
    }

    console.log(`🧪 Dry run complete: ${plan.objects_to_create} objects planned`);
    return plan;
  }
}

module.exports = SnowflakeObjectManager;