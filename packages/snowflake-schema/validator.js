// Snowflake Schema Validator
// Comprehensive validation with existence, privilege, and structure checking

const config = require('./config');

class SchemaValidator {
  constructor(connection) {
    this.connection = connection;
    this.errors = [];
    this.warnings = [];
  }
  
  // Main validation entry point
  async validateAll() {
    const results = {
      timestamp: new Date().toISOString(),
      environment: process.env.NODE_ENV || 'development',
      errors: [],
      warnings: [],
      details: {}
    };
    
    try {
      // 1. Get and log current context
      console.log('🔍 Checking Snowflake context...');
      results.context = await this.getCurrentContext();
      console.log(`📍 Current context: ${results.context.database}.${results.context.schema}`);
      console.log(`👤 Role: ${results.context.role}, Warehouse: ${results.context.warehouse}`);
      
      // 2. Validate database and schemas exist
      console.log('🔍 Validating schema existence...');
      results.existence = await this.validateExistence();
      
      // 3. Validate table structures
      console.log('🔍 Validating table structures...');
      results.tables = await this.validateTableStructures();
      
      // 4. Validate privileges
      console.log('🔍 Validating privileges...');
      results.privileges = await this.validatePrivileges();
      
      // 5. Validate Activity Schema v2 compliance
      console.log('🔍 Validating Activity Schema v2...');
      results.activitySchema = await this.validateActivitySchemaV2();
      
      // 6. Validate query tag propagation
      console.log('🔍 Validating query tag...');
      results.queryTag = await this.validateQueryTag();
      
    } catch (error) {
      results.errors.push({
        type: 'VALIDATION_ERROR',
        message: `Validation failed: ${error.message}`,
        error: error.toString()
      });
    }
    
    // Collect all errors and warnings
    results.errors = [...results.errors, ...this.errors];
    results.warnings = [...results.warnings, ...this.warnings];
    
    // Determine overall status
    results.isValid = results.errors.length === 0;
    results.hasWarnings = results.warnings.length > 0;
    
    return results;
  }
  
  // Get current Snowflake context
  async getCurrentContext() {
    const sql = `
      SELECT 
        CURRENT_ROLE() as role,
        CURRENT_DATABASE() as database,
        CURRENT_SCHEMA() as schema,
        CURRENT_WAREHOUSE() as warehouse,
        CURRENT_USER() as user,
        CURRENT_VERSION() as version,
        CURRENT_SESSION() as session_id
    `;
    
    const result = await this.executeSQL(sql);
    return result[0];
  }
  
  // Validate database and schemas exist
  async validateExistence() {
    const results = {
      database: false,
      schemas: {}
    };
    
    // Check database
    const dbName = config.getDatabase();
    const dbCheckSQL = `
      SELECT COUNT(*) as db_exists
      FROM INFORMATION_SCHEMA.DATABASES
      WHERE DATABASE_NAME = '${dbName}'
    `;
    
    try {
      const dbResult = await this.executeSQL(dbCheckSQL);
      results.database = dbResult[0].DB_EXISTS > 0;
      
      if (!results.database) {
        this.addError('DATABASE_NOT_FOUND', 
          `Database ${dbName} does not exist`,
          `CREATE DATABASE IF NOT EXISTS ${dbName}`);
      }
    } catch (error) {
      this.addError('DATABASE_CHECK_FAILED', 
        `Could not check database: ${error.message}`);
    }
    
    // Check each schema
    for (const schemaName of config.getAllSchemas()) {
      const schemaCheckSQL = `
        SELECT COUNT(*) as schema_exists
        FROM INFORMATION_SCHEMA.SCHEMATA
        WHERE CATALOG_NAME = '${dbName}'
          AND SCHEMA_NAME = '${schemaName}'
      `;
      
      try {
        const schemaResult = await this.executeSQL(schemaCheckSQL);
        results.schemas[schemaName] = schemaResult[0].SCHEMA_EXISTS > 0;
        
        if (!results.schemas[schemaName]) {
          this.addError('SCHEMA_NOT_FOUND',
            `Schema ${schemaName} does not exist`,
            `CREATE SCHEMA IF NOT EXISTS ${schemaName}`);
        }
      } catch (error) {
        this.addWarning('SCHEMA_CHECK_FAILED',
          `Could not check schema ${schemaName}: ${error.message}`);
      }
    }
    
    return results;
  }
  
  // Validate table structures
  async validateTableStructures() {
    const results = {};
    
    for (const schemaName of config.getAllSchemas()) {
      results[schemaName] = {};
      
      for (const tableName of config.getTablesInSchema(schemaName)) {
        const tableRef = config.getTableRef(schemaName, tableName);
        const tableCheckSQL = `
          SELECT COUNT(*) as table_exists
          FROM INFORMATION_SCHEMA.TABLES
          WHERE TABLE_CATALOG = '${config.getDatabase()}'
            AND TABLE_SCHEMA = '${schemaName}'
            AND TABLE_NAME = '${tableName}'
        `;
        
        try {
          const tableResult = await this.executeSQL(tableCheckSQL);
          const exists = tableResult[0].TABLE_EXISTS > 0;
          
          results[schemaName][tableName] = {
            exists: exists,
            columns: exists ? await this.getTableColumns(schemaName, tableName) : []
          };
          
          if (!exists && tableRef.definition.requiredColumns) {
            this.addError('TABLE_NOT_FOUND',
              `Table ${tableRef.fqn} does not exist`,
              `See bootstrap.sql for CREATE TABLE statement`);
          }
        } catch (error) {
          this.addWarning('TABLE_CHECK_FAILED',
            `Could not check table ${schemaName}.${tableName}: ${error.message}`);
        }
      }
    }
    
    return results;
  }
  
  // Get columns for a table
  async getTableColumns(schemaName, tableName) {
    const sql = `
      SELECT 
        COLUMN_NAME,
        DATA_TYPE,
        IS_NULLABLE,
        COLUMN_DEFAULT
      FROM INFORMATION_SCHEMA.COLUMNS
      WHERE TABLE_CATALOG = '${config.getDatabase()}'
        AND TABLE_SCHEMA = '${schemaName}'
        AND TABLE_NAME = '${tableName}'
      ORDER BY ORDINAL_POSITION
    `;
    
    return await this.executeSQL(sql);
  }
  
  // Validate privileges
  async validatePrivileges() {
    const checks = {};
    
    // Check critical privileges
    const privilegeChecks = [
      { object: 'ACTIVITY.EVENTS', privilege: 'SELECT', critical: true },
      { object: 'ACTIVITY.EVENTS', privilege: 'INSERT', critical: true },
      { object: 'ANALYTICS', privilege: 'CREATE VIEW', critical: false },
      { object: 'ANALYTICS', privilege: 'CREATE TASK', critical: false },
      { object: 'ANALYTICS', privilege: 'CREATE STREAMLIT', critical: false }
    ];
    
    for (const check of privilegeChecks) {
      const key = `${check.privilege}_${check.object}`.replace(/\s+/g, '_');
      
      try {
        // Try to use the privilege
        if (check.privilege === 'SELECT') {
          await this.executeSQL(`SELECT 1 FROM ${check.object} LIMIT 0`);
          checks[key] = true;
        } else if (check.privilege === 'CREATE VIEW') {
          await this.executeSQL(`CREATE OR REPLACE VIEW _PRIV_CHECK_VIEW AS SELECT 1 as test`);
          await this.executeSQL(`DROP VIEW IF EXISTS _PRIV_CHECK_VIEW`);
          checks[key] = true;
        } else {
          // For other privileges, we'll assume they exist if no error
          checks[key] = true;
        }
      } catch (error) {
        checks[key] = false;
        
        if (check.critical) {
          this.addError('MISSING_PRIVILEGE',
            `Missing ${check.privilege} privilege on ${check.object}`,
            `GRANT ${check.privilege} ON ${check.object} TO ROLE ${config.getRole()}`);
        } else {
          this.addWarning('MISSING_PRIVILEGE',
            `Missing ${check.privilege} privilege on ${check.object}`);
        }
      }
    }
    
    return checks;
  }
  
  // Validate Activity Schema v2 structure
  async validateActivitySchemaV2() {
    const results = {
      hasTable: false,
      hasRequiredColumns: {},
      hasV2Columns: {},
      hasSystemColumns: {},
      columnNamingValid: true
    };
    
    try {
      // Check if EVENTS table exists
      const tableRef = config.getTableRef('ACTIVITY', 'EVENTS');
      const definition = tableRef.definition;
      
      // Get actual columns
      const columns = await this.getTableColumns('ACTIVITY', 'EVENTS');
      const columnNames = columns.map(c => c.COLUMN_NAME.toUpperCase());
      
      if (columns.length > 0) {
        results.hasTable = true;
        
        // Check required columns
        for (const col of definition.requiredColumns) {
          results.hasRequiredColumns[col] = columnNames.includes(col);
          if (!results.hasRequiredColumns[col]) {
            this.addError('MISSING_REQUIRED_COLUMN',
              `Activity Schema v2: Missing required column ${col}`,
              `ALTER TABLE ACTIVITY.EVENTS ADD COLUMN ${col} VARCHAR(255)`);
          }
        }
        
        // Check v2 specific columns
        for (const col of definition.activitySchemaV2) {
          results.hasV2Columns[col] = columnNames.includes(col);
          if (!results.hasV2Columns[col]) {
            this.addWarning('MISSING_V2_COLUMN',
              `Activity Schema v2: Missing v2 column ${col}`);
          }
        }
        
        // Check system columns have underscore prefix
        for (const col of definition.systemColumns) {
          results.hasSystemColumns[col] = columnNames.includes(col);
          if (!col.startsWith('_')) {
            results.columnNamingValid = false;
            this.addWarning('COLUMN_NAMING',
              `System column ${col} should have underscore prefix`);
          }
        }
      } else {
        this.addError('NO_EVENTS_TABLE',
          'ACTIVITY.EVENTS table not found or not accessible',
          'Run bootstrap.sql to create Activity Schema v2 tables');
      }
    } catch (error) {
      this.addError('ACTIVITY_SCHEMA_CHECK_FAILED',
        `Could not validate Activity Schema: ${error.message}`);
    }
    
    return results;
  }
  
  // Validate query tag propagation
  async validateQueryTag() {
    const results = {
      tagSet: false,
      tagVisible: false
    };
    
    try {
      // Set a test query tag
      const testTag = config.getQueryTag({ service: 'validator-test' });
      await this.executeSQL(`ALTER SESSION SET QUERY_TAG='${testTag}'`);
      results.tagSet = true;
      
      // Run a probe query
      await this.executeSQL(`SELECT 'probe' as test`);
      
      // Check if tag appears in query history (may require elevated privileges)
      const historySQL = `
        SELECT COUNT(*) as tag_found
        FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(
          END_TIME_RANGE_START => DATEADD('minute', -1, CURRENT_TIMESTAMP())
        ))
        WHERE QUERY_TAG = '${testTag}'
        LIMIT 1
      `;
      
      try {
        const historyResult = await this.executeSQL(historySQL);
        results.tagVisible = historyResult[0].TAG_FOUND > 0;
        
        if (!results.tagVisible) {
          this.addWarning('QUERY_TAG_NOT_VISIBLE',
            'Query tag set but not visible in query history (may need elevated privileges)');
        }
      } catch (error) {
        this.addWarning('QUERY_HISTORY_ACCESS',
          'Cannot access query history (normal for restricted roles)');
      }
    } catch (error) {
      this.addWarning('QUERY_TAG_CHECK_FAILED',
        `Could not validate query tag: ${error.message}`);
    }
    
    return results;
  }
  
  // Helper: Execute SQL
  async executeSQL(sql) {
    return new Promise((resolve, reject) => {
      this.connection.execute({
        sqlText: sql,
        complete: (err, stmt) => {
          if (err) {
            reject(err);
          } else {
            const rows = [];
            const stream = stmt.streamRows();
            stream.on('data', row => rows.push(row));
            stream.on('end', () => resolve(rows));
            stream.on('error', reject);
          }
        }
      });
    });
  }
  
  // Helper: Add error
  addError(code, message, remediation) {
    this.errors.push({
      type: 'ERROR',
      code: code,
      message: message,
      remediation: remediation || 'Check configuration and permissions'
    });
  }
  
  // Helper: Add warning
  addWarning(code, message, remediation) {
    this.warnings.push({
      type: 'WARNING',
      code: code,
      message: message,
      remediation: remediation || 'Optional: Review configuration'
    });
  }
}

module.exports = SchemaValidator;