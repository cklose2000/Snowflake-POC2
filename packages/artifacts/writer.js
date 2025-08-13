// Artifact Writer - Pure Snowflake Storage
import snowflake from 'snowflake-sdk';

export class ArtifactWriter {
  constructor(connection) {
    this.connection = connection;
  }

  async storeResults(data, metadata) {
    const artifactId = `art_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
    
    // Determine storage strategy based on size
    const storageStrategy = this.selectStorageStrategy(data);
    
    switch (storageStrategy.type) {
      case 'inline':
        return this.storeInline(artifactId, data, metadata);
      case 'table':
        return this.storeInTable(artifactId, data, metadata);
      case 'stage':
        return this.storeInStage(artifactId, data, metadata);
      default:
        throw new Error(`Unknown storage strategy: ${storageStrategy.type}`);
    }
  }

  selectStorageStrategy(data) {
    const rowCount = data.length;
    const estimatedSize = JSON.stringify(data).length;
    
    if (rowCount <= 10 && estimatedSize < 32768) {
      return { type: 'inline' }; // Store in artifacts.sample directly
    } else if (rowCount <= 1000 && estimatedSize < 16777216) {
      return { type: 'table' }; // Store in artifact_data table
    } else {
      return { type: 'stage' }; // Store in internal stage
    }
  }

  async storeInline(artifactId, data, metadata) {
    // Small results stored directly in artifacts table
    await this.executeQuery(`
      INSERT INTO analytics.activity_ccode.artifacts (
        artifact_id, sample, row_count, schema_json, storage_type, 
        storage_location, bytes, customer, created_by_activity
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    `, [
      artifactId,
      JSON.stringify(data), // Full data as sample
      data.length,
      JSON.stringify(this.extractSchema(data)),
      'inline',
      'artifacts.sample',
      JSON.stringify(data).length,
      metadata.customer,
      metadata.queryTag
    ]);

    return {
      artifact_id: artifactId,
      storage_type: 'inline',
      preview: data,
      full_data_available: true
    };
  }

  async storeInTable(artifactId, data, metadata) {
    // Medium results stored in dedicated table
    const insertSQL = `
      INSERT INTO analytics.activity_ccode.artifact_data (artifact_id, row_number, row_data)
      VALUES ${data.map((_, i) => `(?, ?, ?)`).join(', ')}
    `;
    
    const values = data.flatMap((row, index) => [
      artifactId, 
      index + 1, 
      JSON.stringify(row)
    ]);
    
    await this.executeQuery(insertSQL, values);
    
    // Create metadata record
    await this.executeQuery(`
      INSERT INTO analytics.activity_ccode.artifacts (
        artifact_id, sample, row_count, schema_json, storage_type,
        storage_location, bytes, customer, created_by_activity
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    `, [
      artifactId,
      JSON.stringify(data.slice(0, 10)),
      data.length,
      JSON.stringify(this.extractSchema(data)),
      'table',
      'analytics.activity_ccode.artifact_data',
      JSON.stringify(data).length,
      metadata.customer,
      metadata.queryTag
    ]);

    return {
      artifact_id: artifactId,
      storage_type: 'table',
      preview: data.slice(0, 10),
      full_data_available: true
    };
  }

  async storeInStage(artifactId, data, metadata) {
    // Large results stored in internal stage
    const jsonData = JSON.stringify(data, null, 2);
    const stagePath = `@analytics.activity_ccode.artifact_stage/${artifactId}.json`;
    
    // Use PUT command with inline data
    const base64Data = Buffer.from(jsonData).toString('base64');
    await this.executeQuery(`
      PUT 'data://application/json;base64,${base64Data}' '${stagePath}' 
      AUTO_COMPRESS = TRUE 
      OVERWRITE = TRUE
    `);
    
    // Get compressed size from stage
    const stageInfo = await this.executeQuery(`
      LIST @analytics.activity_ccode.artifact_stage/${artifactId}.json
    `);
    
    const compressedBytes = stageInfo[0]?.SIZE || null;
    
    // Create metadata record
    await this.executeQuery(`
      INSERT INTO analytics.activity_ccode.artifacts (
        artifact_id, sample, row_count, schema_json, storage_type,
        storage_location, bytes, compressed_bytes, customer, created_by_activity
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `, [
      artifactId,
      JSON.stringify(data.slice(0, 10)),
      data.length,
      JSON.stringify(this.extractSchema(data)),
      'stage',
      stagePath,
      jsonData.length,
      compressedBytes,
      metadata.customer,
      metadata.queryTag
    ]);

    return {
      artifact_id: artifactId,
      storage_type: 'stage',
      preview: data.slice(0, 10),
      full_data_available: true,
      compressed_size: compressedBytes
    };
  }

  extractSchema(data) {
    if (data.length === 0) return [];
    
    const firstRow = data[0];
    return Object.keys(firstRow).map(key => ({
      column_name: key,
      data_type: this.inferType(firstRow[key]),
      nullable: firstRow[key] === null,
      sample_value: firstRow[key]
    }));
  }

  inferType(value) {
    if (value === null) return 'NULL';
    if (typeof value === 'number') {
      return Number.isInteger(value) ? 'INTEGER' : 'FLOAT';
    }
    if (typeof value === 'boolean') return 'BOOLEAN';
    if (value instanceof Date) return 'TIMESTAMP';
    if (typeof value === 'object') return 'VARIANT';
    return 'VARCHAR';
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
}

export default ArtifactWriter;