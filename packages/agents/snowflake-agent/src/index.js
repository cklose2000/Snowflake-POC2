// Snowflake Agent - SafeSQL execution only (v1)
const snowflake = require('snowflake-sdk');
const { SAFESQL_TEMPLATES } = require('./templates.js');
const { fqn, qualifySource, createActivityName, SCHEMAS, TABLES, ACTIVITY_VIEW_MAP, DB, getContextSQL } = require('../../snowflake-schema/generated.js');

class SnowflakeAgent {
  constructor() {
    this.connection = null;
    this.initializeConnection();
  }

  async initializeConnection() {
    // CRITICAL: Follow CLAUDE.md guidance exactly - use env vars only
    this.connection = snowflake.createConnection({
      account: process.env.SNOWFLAKE_ACCOUNT,
      username: process.env.SNOWFLAKE_USERNAME,
      password: process.env.SNOWFLAKE_PASSWORD,
      database: process.env.SNOWFLAKE_DATABASE,
      schema: process.env.SNOWFLAKE_SCHEMA,
      warehouse: process.env.SNOWFLAKE_WAREHOUSE,
      role: process.env.SNOWFLAKE_ROLE
    });

    return new Promise((resolve, reject) => {
      this.connection.connect((err, conn) => {
        if (err) {
          reject(err);
        } else {
          // Always set context immediately using generated helpers
          const contextSQL = getContextSQL({ queryTag: 'snowflake-agent-init' });
          conn.execute({
            sqlText: contextSQL.join('; '),
            complete: () => resolve(conn)
          });
        }
      });
    });
  }

  async executeTemplate(templateName, params) {
    const template = SAFESQL_TEMPLATES[templateName];
    if (!template) {
      throw new Error(`Unknown SafeSQL template: ${templateName}`);
    }

    // Validate parameters
    this.validateParams(template, params);

    // Build SQL with parameters
    const { sql, binds } = this.buildSQL(template, params);

    // Set query tag for tracking using generated helpers
    const queryTag = `ccode-ui-${Date.now()}`;
    await this.setQueryTag(queryTag);

    // Execute query
    const result = await this.executeQuery(sql, binds);

    // Create artifact if results > 10 rows
    if (result.length > 10) {
      return this.createArtifact(result, queryTag, templateName);
    }

    return {
      preview: result,
      rowCount: result.length,
      queryTag,
      template: templateName
    };
  }

  validateParams(template, params) {
    // Ensure all required parameters are present
    for (const param of template.required) {
      if (!(param in params)) {
        throw new Error(`Missing required parameter: ${param}`);
      }
    }

    // Validate parameter types and values
    if (template.validation) {
      template.validation(params);
    }
  }

  buildSQL(template, params) {
    // Use parameterized queries with binds
    if (template.parameterBuilder) {
      const binds = template.parameterBuilder(params);
      return { sql: template.sql, binds };
    }
    
    // Fallback for simple templates without parameters
    return { sql: template.sql, binds: [] };
  }

  async setQueryTag(tag) {
    return this.executeQuery('ALTER SESSION SET QUERY_TAG = ?', [tag]);
  }

  executeQuery(sql, params = []) {
    return new Promise((resolve, reject) => {
      this.connection.execute({
        sqlText: sql,
        binds: params,
        complete: (err, stmt, rows) => {
          if (err) reject(err);
          else resolve(rows);
        }
      });
    });
  }

  async createArtifact(data, queryTag, template) {
    const artifactId = `art_${Date.now()}`;
    let storageType, storageLocation;
    
    // Store in Snowflake based on result size
    if (data.length <= 1000) {
      // Small results: store directly in artifact_data table
      await this.storeInTable(artifactId, data);
      storageType = 'table';
      storageLocation = fqn('ACTIVITY_CCODE', 'ARTIFACT_DATA');
    } else {
      // Large results: store in internal stage
      await this.storeInStage(artifactId, data);
      storageType = 'stage';
      storageLocation = `@${fqn('ACTIVITY_CCODE', 'ARTIFACT_STAGE')}/${artifactId}.json`;
    }
    
    // Create artifact metadata record using generated helpers
    const artifactsTable = fqn('ACTIVITY_CCODE', TABLES.ACTIVITY_CCODE.ARTIFACTS);
    await this.executeQuery(`
      INSERT INTO ${artifactsTable} (
        artifact_id, sample, row_count, schema_json, 
        storage_type, storage_location, bytes, customer, created_by_activity
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    `, [
      artifactId,
      JSON.stringify(data.slice(0, 10)), // Sample preview
      data.length,
      JSON.stringify(this.extractSchema(data)),
      storageType,
      storageLocation,
      JSON.stringify(data).length,
      'system', // customer placeholder
      queryTag
    ]);
    
    return {
      artifact_id: artifactId,
      preview: data.slice(0, 10),
      rowCount: data.length,
      queryTag,
      template,
      storage_type: storageType,
      access_url: `/api/artifacts/${artifactId}`
    };
  }

  async storeInTable(artifactId, data) {
    // Store each row as JSON in artifact_data table
    const artifactDataTable = fqn('ACTIVITY_CCODE', 'ARTIFACT_DATA');
    const insertSQL = `
      INSERT INTO ${artifactDataTable} (artifact_id, row_number, row_data)
      VALUES ${data.map((_, i) => `(?, ?, ?)`).join(', ')}
    `;
    
    const values = data.flatMap((row, index) => [
      artifactId, 
      index + 1, 
      JSON.stringify(row)
    ]);
    
    return this.executeQuery(insertSQL, values);
  }

  async storeInStage(artifactId, data) {
    // Store large results in internal stage as compressed JSON
    const jsonData = JSON.stringify(data);
    const stagePath = `@${fqn('ACTIVITY_CCODE', 'ARTIFACT_STAGE')}/${artifactId}.json`;
    
    // Use Snowflake's PUT command to store in internal stage
    return this.executeQuery(
      'PUT ? ? AUTO_COMPRESS = TRUE',
      [`data://text/json;base64,${Buffer.from(jsonData).toString('base64')}`, stagePath]
    );
  }

  extractSchema(data) {
    if (data.length === 0) return [];
    
    const firstRow = data[0];
    return Object.keys(firstRow).map(key => ({
      column_name: key,
      data_type: typeof firstRow[key],
      sample_value: firstRow[key]
    }));
  }
}

// CLI interface for Claude Code
if (process.argv[2]) {
  const agent = new SnowflakeAgent();
  const input = JSON.parse(process.argv[2]);
  
  agent.executeTemplate(input.template, input.params)
    .then(result => console.log(JSON.stringify(result)))
    .catch(error => {
      console.error(JSON.stringify({ error: error.message }));
      process.exit(1);
    });
}

module.exports = SnowflakeAgent;