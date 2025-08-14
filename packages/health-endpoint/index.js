#!/usr/bin/env node

/**
 * Health Endpoint and Drift Watchdog
 * Provides runtime health checks and schema drift detection
 */

const SchemaSentinel = require('../schema-sentinel');
const { CONTRACT_HASH, CONTRACT_VERSION, DB, SCHEMAS, fqn } = require('../snowflake-schema/generated.js');
// Note: Install express if needed: npm install express

class HealthService {
  constructor(snowflakeConnection, options = {}) {
    this.snowflake = snowflakeConnection;
    this.options = {
      port: options.port || 3000,
      enableDriftWatchdog: options.enableDriftWatchdog !== false,
      watchdogInterval: options.watchdogInterval || 24 * 60 * 60 * 1000, // 24 hours
      ...options
    };
    
    this.app = express();
    this.lastValidation = null;
    this.watchdogTimer = null;
    
    this.setupRoutes();
    if (this.options.enableDriftWatchdog) {
      this.startDriftWatchdog();
    }
  }

  setupRoutes() {
    this.app.use(express.json());
    
    // Basic health check
    this.app.get('/health', async (req, res) => {
      try {
        const health = await this.getBasicHealth();
        res.status(health.status === 'healthy' ? 200 : 503).json(health);
      } catch (error) {
        res.status(500).json({
          status: 'error',
          message: error.message,
          timestamp: new Date().toISOString()
        });
      }
    });

    // Detailed health with schema validation
    this.app.get('/health/detailed', async (req, res) => {
      try {
        const skipValidation = req.query.skipValidation === 'true';
        const health = await this.getDetailedHealth(skipValidation);
        res.status(health.status === 'healthy' ? 200 : 503).json(health);
      } catch (error) {
        res.status(500).json({
          status: 'error',
          message: error.message,
          timestamp: new Date().toISOString()
        });
      }
    });

    // Schema validation endpoint
    this.app.post('/health/validate-schema', async (req, res) => {
      try {
        const options = {
          throwOnDrift: false,
          logActivity: req.body.logActivity !== false,
          skipViewChecks: req.body.skipViewChecks === true
        };
        
        const result = await SchemaSentinel.validate(this.snowflake, options);
        this.lastValidation = result;
        
        res.status(result.passed ? 200 : 503).json({
          status: result.passed ? 'valid' : 'drift_detected',
          validation: result,
          timestamp: new Date().toISOString()
        });
      } catch (error) {
        res.status(500).json({
          status: 'validation_error',
          message: error.message,
          timestamp: new Date().toISOString()
        });
      }
    });

    // Contract information
    this.app.get('/health/contract', (req, res) => {
      res.json({
        contractHash: CONTRACT_HASH,
        contractVersion: CONTRACT_VERSION,
        database: DB,
        schemas: SCHEMAS,
        enforcementEnabled: true,
        generatedAt: new Date().toISOString()
      });
    });

    // Drift watchdog status
    this.app.get('/health/watchdog', (req, res) => {
      res.json({
        enabled: this.options.enableDriftWatchdog,
        interval: this.options.watchdogInterval,
        lastCheck: this.lastValidation?.checkedAt || null,
        nextCheck: this.watchdogTimer ? 
          new Date(Date.now() + this.options.watchdogInterval).toISOString() : null,
        status: this.lastValidation?.passed ? 'healthy' : 'drift_detected'
      });
    });

    // Force drift check
    this.app.post('/health/watchdog/check', async (req, res) => {
      try {
        await this.runDriftCheck();
        res.json({
          status: 'check_initiated',
          timestamp: new Date().toISOString()
        });
      } catch (error) {
        res.status(500).json({
          status: 'check_failed',
          message: error.message,
          timestamp: new Date().toISOString()
        });
      }
    });
  }

  async getBasicHealth() {
    const health = {
      status: 'healthy',
      timestamp: new Date().toISOString(),
      service: 'claude-code-dashboard-factory',
      version: '1.0.0',
      contract: {
        hash: CONTRACT_HASH,
        version: CONTRACT_VERSION
      },
      uptime: process.uptime(),
      memory: process.memoryUsage(),
      pid: process.pid
    };

    // Quick database connectivity check
    try {
      await new Promise((resolve, reject) => {
        this.snowflake.execute({
          sqlText: 'SELECT 1 as health_check',
          complete: (err, stmt, rows) => {
            if (err) reject(err);
            else resolve(rows);
          }
        });
      });
      
      health.database = 'connected';
    } catch (error) {
      health.status = 'unhealthy';
      health.database = 'disconnected';
      health.error = error.message;
    }

    return health;
  }

  async getDetailedHealth(skipValidation = false) {
    const basic = await this.getBasicHealth();
    
    const detailed = {
      ...basic,
      database: {
        status: basic.database,
        target: DB,
        schemas: Object.values(SCHEMAS)
      },
      schemaValidation: null,
      contractEnforcement: {
        preCommitHooks: true,
        ciValidation: true,
        runtimeValidation: true,
        driftDetection: this.options.enableDriftWatchdog
      }
    };

    // Run schema validation if not skipped and database is connected
    if (!skipValidation && basic.database === 'connected') {
      try {
        const validation = await SchemaSentinel.validate(this.snowflake, {
          throwOnDrift: false,
          logActivity: false,
          skipViewChecks: true // Fast health check
        });
        
        detailed.schemaValidation = {
          status: validation.passed ? 'valid' : 'drift_detected',
          checkedAt: validation.checkedAt,
          issuesCount: validation.issues.length,
          warningsCount: validation.warnings.length
        };
        
        if (!validation.passed) {
          detailed.status = 'degraded';
        }
        
        this.lastValidation = validation;
      } catch (error) {
        detailed.schemaValidation = {
          status: 'validation_failed',
          error: error.message
        };
        detailed.status = 'degraded';
      }
    }

    return detailed;
  }

  startDriftWatchdog() {
    console.log('🐕 Starting drift watchdog (interval: ' + this.options.watchdogInterval + 'ms)');
    
    // Run initial check after 30 seconds
    setTimeout(() => this.runDriftCheck(), 30000);
    
    // Set up recurring checks
    this.watchdogTimer = setInterval(() => {
      this.runDriftCheck();
    }, this.options.watchdogInterval);
  }

  async runDriftCheck() {
    console.log('🔍 Running drift watchdog check...');
    
    try {
      const validation = await SchemaSentinel.validate(this.snowflake, {
        throwOnDrift: false,
        logActivity: true, // Log watchdog activity
        skipViewChecks: false
      });
      
      this.lastValidation = validation;
      
      if (!validation.passed) {
        console.warn('⚠️ Schema drift detected: ' + validation.issues.length + ' issues');
        
        // Log drift as activity
        await this.logDriftDetected(validation);
        
        // TODO: Send alerts (email, Slack, etc.)
        
      } else {
        console.log('✅ No schema drift detected');
      }
      
    } catch (error) {
      console.error('❌ Drift check failed:', error.message);
    }
  }

  async logDriftDetected(validation) {
    try {
      const activityId = 'drift_' + Date.now() + '_' + Math.random().toString(36).substr(2, 8);
      
      await new Promise((resolve, reject) => {
        const eventsTable = fqn('ACTIVITY', 'EVENTS');
        this.snowflake.execute({
          sqlText: `
            INSERT INTO ${eventsTable} (
              activity_id, ts, customer, activity, feature_json,
              _source_system, _source_version, _query_tag
            )
            VALUES (?, CURRENT_TIMESTAMP(), ?, ?, PARSE_JSON(?), ?, ?, ?)
          `,
          binds: [
            activityId,
            'drift_watchdog',
            'ccode.schema_violation',
            JSON.stringify({
              contract_hash: CONTRACT_HASH,
              issues_count: validation.issues.length,
              warnings_count: validation.warnings.length,
              critical_issues: validation.issues.filter(i => i.fatal).length,
              detected_by: 'drift_watchdog'
            }),
            'health_service',
            '1.0.0',
            'drift_watchdog_' + CONTRACT_HASH
          ],
          complete: (err, stmt) => {
            if (err) reject(err);
            else resolve(stmt);
          }
        });
      });
      
      console.log('📝 Logged drift detection as ' + activityId);
      
    } catch (error) {
      console.warn('⚠️ Failed to log drift detection:', error.message);
    }
  }

  start() {
    return new Promise((resolve) => {
      this.server = this.app.listen(this.options.port, () => {
        console.log('🏥 Health service started on port ' + this.options.port);
        console.log('📋 Contract: ' + CONTRACT_HASH);
        console.log('🐕 Drift watchdog: ' + (this.options.enableDriftWatchdog ? 'enabled' : 'disabled'));
        resolve();
      });
    });
  }

  stop() {
    if (this.watchdogTimer) {
      clearInterval(this.watchdogTimer);
      this.watchdogTimer = null;
    }
    
    if (this.server) {
      return new Promise((resolve) => {
        this.server.close(() => {
          console.log('🏥 Health service stopped');
          resolve();
        });
      });
    }
  }
}

module.exports = HealthService;