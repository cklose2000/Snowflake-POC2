// Snowflake Object Manager - Creates Views, Tasks, and Dynamic Tables
// Implements binary scheduling: Tasks vs Dynamic Tables based on spec.schedule.mode

const { generateObjectNames } = require('./schema');
const cfg = require('../snowflake-schema/config');

class SnowflakeObjectManager {
  constructor(snowflakeConnection) {
    this.snowflake = snowflakeConnection;
    this.cfg = cfg;  // Schema configuration for FQN resolution
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
      // Create warehouse and resource monitor FIRST (needed by tasks)
      const infraResult = await this.createInfrastructure(spec);
      results.objectsCreated += infraResult.count;

      // Create base views for each panel
      let allScheduled = true;
      for (const panel of spec.panels) {
        const panelResult = await this.createPanelObjects(spec, panel);
        
        results.views.push(...panelResult.views);
        results.tasks.push(...panelResult.tasks);
        results.dynamicTables.push(...panelResult.dynamicTables);
        results.objectsCreated += panelResult.count;
        
        // Track if any panel failed to schedule
        if (panelResult.scheduled === false) {
          allScheduled = false;
        }
      }
      
      // Pass scheduling status up
      results.scheduled = allScheduled;

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

    // Step 3: Create scheduling object based on mode (graceful degradation)
    let scheduled = false;
    if (spec.schedule.mode === 'exact') {
      // Try to create Task - continue without if no privileges
      try {
        const taskSQL = this.generateTaskSQL(spec, panel, objectNames);
        await this.executeSQL(taskSQL, `CREATE TASK ${objectNames.task}`);
        results.tasks.push(objectNames.task);
        results.count++;
        scheduled = true;
      } catch (error) {
        if (/insufficient privileges|not authorized/i.test(error.message)) {
          console.log(`⚠️ Task creation skipped (no privileges): ${objectNames.task}`);
          results.scheduled = false;  // Track that scheduling failed
        } else {
          throw error;  // Re-throw non-privilege errors
        }
      }
    } else if (spec.schedule.mode === 'freshness') {
      // Try Dynamic Tables - also graceful degradation
      try {
        const dynamicTableSQL = this.generateDynamicTableSQL(spec, panel, objectNames);
        await this.executeSQL(dynamicTableSQL, `CREATE DYNAMIC TABLE ${objectNames.dynamic_table}`);
        results.dynamicTables.push(objectNames.dynamic_table);
        results.count++;
        scheduled = true;
      } catch (error) {
        if (/insufficient privileges|not authorized/i.test(error.message)) {
          console.log(`⚠️ Dynamic table creation skipped (no privileges): ${objectNames.dynamic_table}`);
          results.scheduled = false;
        } else {
          throw error;
        }
      }
    }
    
    // Store scheduling status
    results.scheduled = scheduled;

    return results;
  }

  // Generate base view SQL for Activity-native panels
  generateBaseViewSQL(panel, viewName) {
    const { type, source, x, y, metric, limit } = panel;
    
    // Activity views already have fixed windows, no additional filtering needed
    // v1: All views are pre-filtered with appropriate time windows
    
    // Use config module to qualify source with correct schema
    const qualifiedSource = this.cfg.qualifySource(source);
    
    // Different SQL generation based on panel type
    if (type === 'chart' || type === 'table') {
      // Chart or table with group by
      const groupByColumns = panel.group_by || [];
      const groupByClause = groupByColumns.length > 0 ? `GROUP BY ${groupByColumns.join(', ')}` : '';
      const orderByClause = groupByColumns.length > 0 ? `ORDER BY ${metric} DESC` : 'ORDER BY 1';
      
      return `
        CREATE OR REPLACE VIEW ${viewName} AS
        SELECT ${groupByColumns.length > 0 ? groupByColumns.join(', ') + ',' : ''}
          ${metric} as metric_value
        FROM ${qualifiedSource}
        ${groupByClause}
        ${orderByClause}
        ${panel.top_n ? `LIMIT ${panel.top_n}` : 'LIMIT 100'}
      `.trim();
      
    } else if (type === 'histogram' && metric) {
      // Histogram for distribution
      return `
        CREATE OR REPLACE VIEW ${viewName} AS
        SELECT 
          ${metric} as value,
          COUNT(*) as frequency,
          PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ${metric}) OVER() as median,
          PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY ${metric}) OVER() as p95
        FROM ${qualifiedSource}
        WHERE ${metric} IS NOT NULL
        GROUP BY ${metric}
        ORDER BY value
      `.trim();
      
    } else if (type === 'timeseries' && x && y) {
      // Time series with grain
      const grain = panel.grain || 'hour';
      return `
        CREATE OR REPLACE VIEW ${viewName} AS
        SELECT 
          DATE_TRUNC('${grain}', ${x}) as time_bucket,
          SUM(${y}) as metric_value,
          COUNT(*) as event_count
        FROM ${qualifiedSource}
        GROUP BY time_bucket
        ORDER BY time_bucket DESC
      `.trim();
      
    } else if (type === 'metrics') {
      // Single-row metrics view
      return `
        CREATE OR REPLACE VIEW ${viewName} AS
        SELECT * FROM ${source}
      `.trim();
      
    } else {
      // Default: simple select with limit
      return `
        CREATE OR REPLACE VIEW ${viewName} AS
        SELECT * 
        FROM ${qualifiedSource}
        ${limit ? `LIMIT ${limit}` : 'LIMIT 100'}
      `.trim();
    }
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
        WAREHOUSE = '${process.env.SNOWFLAKE_WAREHOUSE}'
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
        WAREHOUSE = '${process.env.SNOWFLAKE_WAREHOUSE}'
        AS (
          ${this.generateBaseViewSQL(panel, 'temp_base').replace('CREATE OR REPLACE VIEW temp_base AS', '')}
          ${panel.top_n ? `ORDER BY metric_value DESC LIMIT ${panel.top_n}` : ''}
        )
    `.trim();
  }

  // Create infrastructure objects (warehouse, resource monitor)
  async createInfrastructure(spec) {
    // Skip warehouse creation - use existing CLAUDE_WAREHOUSE
    console.log(`📦 Using existing warehouse: ${process.env.SNOWFLAKE_WAREHOUSE}`);
    return { count: 0 };
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
      // Check 0: Activity-native dashboard verification (v1 ONLY uses Activity data)
      console.log('🔧 Checking Activity data availability...');
      try {
        // Check if we have Activity events in the requested window
        const windowChecks = {
          '24h': "ts >= DATEADD('hour', -24, CURRENT_TIMESTAMP())",
          '7d': "ts >= DATEADD('day', -7, CURRENT_TIMESTAMP())",
          '30d': "ts >= DATEADD('day', -30, CURRENT_TIMESTAMP())"
        };
        
        // Determine time window from spec
        const timeWindow = spec.timeWindow?.window || '24h';
        const whereClause = windowChecks[timeWindow] || windowChecks['24h'];
        
        // Check Activity.EVENTS has data
        const activityCheckSQL = `
          SELECT 
            COUNT(*) AS event_count,
            COUNT(DISTINCT customer) AS unique_customers,
            COUNT(DISTINCT activity) AS unique_activities,
            MIN(ts) AS earliest_event,
            MAX(ts) AS latest_event
          FROM CLAUDE_BI.ACTIVITY.EVENTS
          WHERE ${whereClause}
        `;
        
        const activityResult = await this.executeSQL(activityCheckSQL, 'CHECK ACTIVITY DATA');
        const activityStats = activityResult.resultSet[0];
        
        results.checks.activity_data = {
          event_count: activityStats.EVENT_COUNT,
          unique_customers: activityStats.UNIQUE_CUSTOMERS,
          unique_activities: activityStats.UNIQUE_ACTIVITIES,
          earliest_event: activityStats.EARLIEST_EVENT,
          latest_event: activityStats.LATEST_EVENT,
          time_window: timeWindow
        };
        
        if (activityStats.EVENT_COUNT === 0) {
          results.warnings.push('no_activity_data_in_window');
          console.log(`⚠️ No Activity data found in ${timeWindow} window`);
          console.log(`💡 Suggestion: Generate activity by running SafeSQL queries or wait for user interactions`);
          
          // Offer to generate sample activity
          results.generate_activity_suggestion = {
            method: 'run_safesql_query',
            description: 'Execute a SafeSQL query to generate activity events',
            alternative: 'Wait for natural user interactions to accumulate'
          };
        } else {
          console.log(`✅ Activity data found: ${activityStats.EVENT_COUNT} events from ${activityStats.UNIQUE_CUSTOMERS} customers`);
        }
        
        // Check if Activity views exist
        const viewCheckSQL = `
          SELECT VIEW_NAME 
          FROM INFORMATION_SCHEMA.VIEWS 
          WHERE TABLE_SCHEMA = 'ACTIVITY_CCODE'
            AND VIEW_NAME IN (
              'VW_ACTIVITY_COUNTS_24H',
              'VW_LLM_TELEMETRY',
              'VW_SQL_EXECUTIONS',
              'VW_DASHBOARD_OPERATIONS',
              'VW_SAFESQL_TEMPLATES',
              'VW_ACTIVITY_SUMMARY'
            )
          ORDER BY VIEW_NAME
        `;
        
        const viewResult = await this.executeSQL(viewCheckSQL, 'CHECK ACTIVITY VIEWS');
        const existingViews = viewResult.resultSet.map(row => row.VIEW_NAME);
        
        results.checks.activity_views = {
          expected: 6,
          found: existingViews.length,
          views: existingViews
        };
        
        if (existingViews.length < 6) {
          results.issues.push('missing_activity_views');
          results.passed = false;
          console.log(`❌ Missing Activity views: expected 6, found ${existingViews.length}`);
          console.log(`💡 Fix: Run 'npm run bootstrap-activity-views' or execute scripts/bootstrap_activity_views.sql`);
        } else {
          console.log(`✅ All 6 Activity views present in ACTIVITY_CCODE schema`);
        }
        
      } catch (error) {
        results.issues.push('activity_check_failed');
        results.passed = false;
        console.log(`❌ Activity data check failed: ${error.message}`);
      }

      // Check 1: Warehouse context (CRITICAL - must have warehouse)
      console.log('🔧 Checking warehouse context...');
      try {
        const contextResult = await this.executeSQL(
          'SELECT CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE()',
          'CHECK CONTEXT'
        );
        results.checks.context = contextResult.resultSet[0];
        
        // Verify warehouse is set
        if (!results.checks.context.CURRENT_WAREHOUSE) {
          // Try to set from environment
          const envWarehouse = process.env.SNOWFLAKE_WAREHOUSE || this.cfg.getWarehouse();
          if (envWarehouse) {
            console.log(`⚠️ No warehouse set, using ${envWarehouse}`);
            try {
              await this.executeSQL(`USE WAREHOUSE ${envWarehouse}`, 'SET WAREHOUSE');
              results.checks.warehouse_set = envWarehouse;
              console.log(`✅ Warehouse set: ${envWarehouse}`);
            } catch (e) {
              results.issues.push('cannot_set_warehouse');
              console.log(`❌ Cannot set warehouse: ${e.message}`);
            }
          } else {
            results.issues.push('no_warehouse_available');
            results.passed = false;
            console.log(`❌ No warehouse available - operations will fail`);
          }
        } else {
          console.log(`✅ Warehouse active: ${results.checks.context.CURRENT_WAREHOUSE}`);
        }
        
        // Verify database/schema context
        if (!results.checks.context.CURRENT_DATABASE) {
          console.log(`⚠️ No database set, using ${this.cfg.getDatabase()}`);
          await this.executeSQL(`USE DATABASE ${this.cfg.getDatabase()}`, 'SET DATABASE');
        }
      } catch (error) {
        results.issues.push('context_check_failed');
        console.log(`❌ Context check failed: ${error.message}`);
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

      // Check 4: Cost estimate (Activity views are pre-aggregated, low cost)
      console.log('🔧 Estimating cost and data size...');
      let totalEstimatedBytes = 0;
      let estimatedObjects = 0;
      
      for (const panel of spec.panels) {
        try {
          // Activity views are already aggregated, so cost is minimal
          // Just check row count in the view
          const sampleSQL = `
            SELECT 
              COUNT(*) as estimated_rows,
              COUNT(*) * 100 as estimated_bytes  -- Rough estimate
            FROM ${panel.source} 
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

      // Check 5: Source view accessibility (Activity views)
      console.log('🔧 Verifying Activity view access...');
      for (const panel of spec.panels) {
        try {
          // Check if this is an Activity view
          const isActivityView = panel.source?.includes('ACTIVITY_CCODE');
          const sourceType = isActivityView ? 'Activity view' : 'table';
          
          const tableCheckSQL = `SELECT COUNT(*) as row_count FROM ${panel.source} LIMIT 1`;
          const tableResult = await this.executeSQL(tableCheckSQL, `CHECK ${sourceType.toUpperCase()} ${panel.source}`);
          results.checks[`source_${panel.source}`] = {
            accessible: true,
            sample_count: tableResult.resultSet[0]?.ROW_COUNT || 0,
            type: sourceType
          };
          console.log(`✅ ${sourceType} ${panel.source} accessible (${results.checks[`source_${panel.source}`].sample_count} rows)`);
        } catch (error) {
          results.issues.push(`source_inaccessible_${panel.source}`);
          results.passed = false;
          results.checks[`source_${panel.source}`] = {
            accessible: false,
            error: error.message
          };
          console.log(`❌ Source ${panel.source} not accessible: ${error.message}`);
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

  // Unified execute wrapper with binds support and query_id capture
  async exec(sqlText, binds = {}, label = 'SQL') {
    console.log(`🔧 ${label}...`);
    return new Promise((resolve, reject) => {
      this.snowflake.execute({
        sqlText,
        binds,  // Supports both named and positional binds
        complete: (err, stmt, rows) => {
          if (err) {
            console.error(`❌ ${label} failed: ${err.message}`);
            reject(err);
          } else {
            // Capture query_id for telemetry
            let queryId = null;
            if (stmt && stmt.getQueryId) {
              queryId = stmt.getQueryId();
            }
            
            // Return standardized result
            resolve({ 
              rows: rows || [], 
              stmt,
              queryId,
              // Backward compatibility
              resultSet: rows || []
            });
          }
        }
      });
    });
  }

  // Legacy wrapper for backward compatibility
  async executeSQL(sql, description = 'SQL') {
    return this.exec(sql, {}, description);
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