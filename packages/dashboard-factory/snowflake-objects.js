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

  // Run preflight checks before object creation
  async runPreflightChecks(spec) {
    console.log(`🔍 Running preflight checks for: ${spec.name}`);
    
    const results = {
      passed: true,
      issues: [],
      warnings: [],
      checks: {}
    };

    try {
      // Check 1: Schema permissions
      const permissionResult = await this.executeSQL(
        this.preflightChecks.schema_permissions, 
        'CHECK PERMISSIONS'
      );
      results.checks.permissions = permissionResult.resultSet[0];
      console.log(`✅ Permissions: ${results.checks.permissions.CURRENT_ROLE}`);

      // Check 2: Warehouse availability (for Task scheduling)
      if (spec.schedule.mode === 'exact') {
        try {
          const warehouseResult = await this.executeSQL(
            this.preflightChecks.warehouse_exists, 
            'CHECK WAREHOUSES'
          );
          results.checks.warehouses = warehouseResult.resultSet?.length || 0;
          console.log(`✅ Warehouses available: ${results.checks.warehouses}`);
        } catch (error) {
          results.warnings.push('warehouse_check_failed');
          console.log(`⚠️ Warehouse check failed: ${error.message}`);
        }
      }

      // Check 3: Dynamic Tables prerequisites (for freshness scheduling)
      if (spec.schedule.mode === 'freshness') {
        try {
          const changeTrackingResult = await this.executeSQL(
            this.preflightChecks.change_tracking, 
            'CHECK CHANGE TRACKING'
          );
          
          if (!changeTrackingResult.resultSet?.length) {
            results.issues.push('change_tracking_missing');
            results.passed = false;
            console.log(`❌ Change tracking not enabled on any tables`);
          } else {
            console.log(`✅ Change tracking available`);
          }
        } catch (error) {
          results.issues.push('change_tracking_check_failed');
          results.passed = false;
          console.log(`❌ Change tracking check failed: ${error.message}`);
        }
      }

      // Check 4: Source table availability
      for (const panel of spec.panels) {
        try {
          const tableCheckSQL = `SELECT COUNT(*) as row_count FROM ${panel.source} LIMIT 1`;
          const tableResult = await this.executeSQL(tableCheckSQL, `CHECK TABLE ${panel.source}`);
          results.checks[`table_${panel.source}`] = tableResult.resultSet[0]?.ROW_COUNT || 0;
          console.log(`✅ Table ${panel.source} accessible`);
        } catch (error) {
          results.issues.push(`source_table_missing_${panel.source}`);
          results.passed = false;
          console.log(`❌ Table ${panel.source} not accessible: ${error.message}`);
        }
      }

    } catch (error) {
      results.issues.push('preflight_error');
      results.passed = false;
      console.error(`❌ Preflight check error: ${error.message}`);
    }

    console.log(`🔍 Preflight results: ${results.passed ? 'PASSED' : 'FAILED'} (${results.issues.length} issues, ${results.warnings.length} warnings)`);
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

  // Execute SQL with error handling and logging
  async executeSQL(sql, description = 'SQL') {
    try {
      console.log(`🔧 ${description}...`);
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