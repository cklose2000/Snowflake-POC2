// API Server with SafeSQL Template Engine
const express = require('express');
const snowflake = require('snowflake-sdk');
const SafeSQLTemplateEngine = require('../../../packages/safesql/template-engine');
require('dotenv').config();

class APIServer {
  constructor(port = 3001) {
    this.app = express();
    this.port = port;
    this.snowflakeConn = null;
    this.templateEngine = null;
    
    this.setupMiddleware();
    this.setupRoutes();
  }

  setupMiddleware() {
    this.app.use(express.json());
    this.app.use(express.static(process.cwd()));
    
    // CORS for development
    this.app.use((req, res, next) => {
      res.header('Access-Control-Allow-Origin', '*');
      res.header('Access-Control-Allow-Headers', 'Content-Type');
      res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
      next();
    });
  }

  async initSnowflake() {
    const connection = snowflake.createConnection({
      account: process.env.SNOWFLAKE_ACCOUNT,
      username: process.env.SNOWFLAKE_USERNAME,
      password: process.env.SNOWFLAKE_PASSWORD,
      role: process.env.SNOWFLAKE_ROLE,
      warehouse: process.env.SNOWFLAKE_WAREHOUSE,
      database: process.env.SNOWFLAKE_DATABASE,
      schema: process.env.SNOWFLAKE_SCHEMA
    });

    return new Promise((resolve, reject) => {
      connection.connect((err, conn) => {
        if (err) {
          console.error('❌ Snowflake connection failed:', err.message);
          reject(err);
        } else {
          console.log('✅ Connected to Snowflake');
          this.snowflakeConn = conn;
          this.templateEngine = new SafeSQLTemplateEngine(conn);
          resolve(conn);
        }
      });
    });
  }

  setupRoutes() {
    // Health check
    this.app.get('/health', (req, res) => {
      res.json({
        status: 'healthy',
        snowflake: this.snowflakeConn ? 'connected' : 'disconnected',
        templates: this.templateEngine ? this.templateEngine.getTemplateList().length : 0,
        timestamp: new Date()
      });
    });

    // List available templates
    this.app.get('/api/templates', (req, res) => {
      if (!this.templateEngine) {
        return res.status(503).json({ error: 'Template engine not initialized' });
      }
      
      res.json({
        templates: this.templateEngine.getTemplateList()
      });
    });

    // Validate template parameters
    this.app.post('/api/validate', (req, res) => {
      const { template, params } = req.body;
      
      if (!this.templateEngine) {
        return res.status(503).json({ error: 'Template engine not initialized' });
      }

      const validation = this.templateEngine.validateTemplate(template, params);
      res.json(validation);
    });

    // Execute SafeSQL template
    this.app.post('/api/query', async (req, res) => {
      const { template, params } = req.body;
      
      if (!this.templateEngine) {
        return res.status(503).json({ error: 'Template engine not initialized' });
      }

      try {
        const result = await this.templateEngine.execute(template, params);
        res.json(result);
      } catch (error) {
        res.status(400).json({ 
          error: error.message,
          template: template,
          params: params
        });
      }
    });

    // Activity logging endpoint (improved)
    this.app.post('/api/activity', async (req, res) => {
      const { activity, customer, feature_json } = req.body;
      
      if (!this.snowflakeConn) {
        return res.status(503).json({ error: 'Snowflake not connected' });
      }

      // Validate activity namespace
      if (!activity.startsWith('ccode.')) {
        return res.status(400).json({ 
          error: 'Invalid activity. Must use ccode.* namespace',
          received: activity
        });
      }

      const activityId = `act_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
      
      // Use parameterized query
      const sql = `INSERT INTO ACTIVITY.EVENTS (
        activity_id, ts, customer, activity, feature_json, _source_system
      ) SELECT ?, CURRENT_TIMESTAMP(), ?, ?, PARSE_JSON(?), ?`;

      const binds = [
        activityId,
        customer || 'api_user',
        activity,
        JSON.stringify(feature_json || {}),
        'api_server'
      ];

      this.snowflakeConn.execute({
        sqlText: sql,
        binds: binds,
        complete: (err, stmt, rows) => {
          if (err) {
            res.status(500).json({ 
              error: err.message,
              sql: sql.substring(0, 100) + '...'
            });
          } else {
            res.json({ 
              success: true, 
              activity_id: activityId,
              message: 'Activity logged successfully'
            });
          }
        }
      });
    });

    // Artifact storage endpoint
    this.app.post('/api/artifact', async (req, res) => {
      const { data, metadata } = req.body;
      
      if (!this.snowflakeConn) {
        return res.status(503).json({ error: 'Snowflake not connected' });
      }

      const artifactId = `art_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
      const dataStr = JSON.stringify(data);
      const bytes = Buffer.byteLength(dataStr);
      
      // Determine storage strategy based on size
      let storageType, storageLocation;
      
      if (bytes < 1000) {
        // Inline storage for small data
        storageType = 'inline';
        storageLocation = 'artifacts.sample_data';
      } else if (bytes < 16000000) { // 16MB limit for VARIANT
        // Table storage for medium data
        storageType = 'table';
        storageLocation = 'artifact_data.data';
      } else {
        // Stage storage for large data
        storageType = 'stage';
        storageLocation = '@ACTIVITY_CCODE.ARTIFACT_STAGE/' + artifactId;
      }

      const sql = `INSERT INTO ACTIVITY_CCODE.ARTIFACTS (
        artifact_id, sample_data, row_count, schema_json, 
        storage_type, storage_location, bytes, customer
      ) SELECT ?, PARSE_JSON(?), ?, PARSE_JSON(?), ?, ?, ?, ?`;

      const binds = [
        artifactId,
        JSON.stringify(data.slice(0, 10)), // Sample of first 10 rows
        data.length,
        JSON.stringify(metadata || {}),
        storageType,
        storageLocation,
        bytes,
        req.body.customer || 'api_user'
      ];

      this.snowflakeConn.execute({
        sqlText: sql,
        binds: binds,
        complete: (err, stmt, rows) => {
          if (err) {
            res.status(500).json({ error: err.message });
          } else {
            res.json({ 
              success: true,
              artifact_id: artifactId,
              storage_type: storageType,
              bytes: bytes
            });
          }
        }
      });
    });

    // Serve test UI at root
    this.app.get('/', (req, res) => {
      res.sendFile('test-ui.html', { root: process.cwd() });
    });
  }

  async start() {
    try {
      await this.initSnowflake();
      
      this.app.listen(this.port, () => {
        console.log(`\n🌉 API Server running on http://localhost:${this.port}`);
        console.log('\n📊 Available endpoints:');
        console.log('  GET  /health         - Health check');
        console.log('  GET  /api/templates  - List SafeSQL templates');
        console.log('  POST /api/validate   - Validate template params');
        console.log('  POST /api/query      - Execute SafeSQL template');
        console.log('  POST /api/activity   - Log activity event');
        console.log('  POST /api/artifact   - Store result artifact');
        console.log('\n✨ API server ready!');
      });
    } catch (error) {
      console.error('Failed to start server:', error);
      process.exit(1);
    }
  }
}

module.exports = APIServer;