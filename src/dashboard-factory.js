/**
 * Dashboard Factory - Consolidated dashboard generation
 * Merges 12 files into one focused module
 */

const crypto = require('crypto');
const SchemaContract = require('./schema-contract');

class DashboardFactory {
  constructor(snowflakeConn, activityLogger) {
    this.conn = snowflakeConn;
    this.logger = activityLogger;
  }

  /**
   * Generate dashboard spec from conversation
   */
  async generateSpec(conversation) {
    // Analyze conversation for dashboard intent
    const intent = this.analyzeIntent(conversation);
    
    if (!intent.isDashboard) {
      throw new Error('No dashboard intent detected in conversation');
    }

    // Generate spec based on intent
    const spec = {
      name: intent.name || 'activity_dashboard',
      description: intent.description || 'Activity dashboard',
      panels: this.generatePanels(intent),
      schedule: {
        mode: 'exact',
        cron_utc: '0 */6 * * *'  // Every 6 hours
      },
      timeWindow: {
        window: intent.timeWindow || '24h'
      }
    };

    // Add spec hash for idempotency
    spec.hash = this.generateSpecHash(spec);

    await this.logger.log('spec_generated', {
      panels: spec.panels.length,
      hash: spec.hash
    });

    return spec;
  }

  /**
   * Create dashboard from spec
   */
  async create(spec) {
    const startTime = Date.now();
    
    try {
      // Log start
      await this.logger.log('dashboard_creating', {
        name: spec.name,
        panels: spec.panels.length
      });

      // Create views for each panel
      const results = {
        dashboard_id: `dashboard_${spec.hash}`,
        objectsCreated: 0,
        views: [],
        errors: []
      };

      for (const panel of spec.panels) {
        try {
          const viewName = await this.createPanelView(spec, panel);
          results.views.push(viewName);
          results.objectsCreated++;
        } catch (error) {
          results.errors.push({
            panel: panel.id,
            error: error.message
          });
        }
      }

      // Generate Streamlit code
      const streamlitCode = this.generateStreamlit(spec, results.views);
      results.streamlitFile = `generated_${spec.hash}.py`;

      // Save Streamlit file
      await this.saveStreamlitFile(results.streamlitFile, streamlitCode);

      // Log completion
      await this.logger.log('dashboard_created', {
        dashboard_id: results.dashboard_id,
        duration_ms: Date.now() - startTime,
        objects_created: results.objectsCreated
      });

      return results;

    } catch (error) {
      await this.logger.log('dashboard_failed', {
        error: error.message
      });
      throw error;
    }
  }

  /**
   * Analyze conversation for dashboard intent
   */
  analyzeIntent(conversation) {
    const text = Array.isArray(conversation) 
      ? conversation.map(m => m.content || m.message).join(' ')
      : String(conversation);

    const dashboardKeywords = [
      'dashboard', 'chart', 'graph', 'visualization',
      'show', 'display', 'metrics', 'analytics'
    ];

    const isDashboard = dashboardKeywords.some(keyword => 
      text.toLowerCase().includes(keyword)
    );

    // Extract time window
    let timeWindow = '24h';
    if (text.includes('7 days') || text.includes('week')) {
      timeWindow = '7d';
    } else if (text.includes('30 days') || text.includes('month')) {
      timeWindow = '30d';
    }

    return {
      isDashboard,
      name: 'activity_dashboard',
      description: 'Activity metrics dashboard',
      timeWindow
    };
  }

  /**
   * Generate panels based on intent
   */
  generatePanels(intent) {
    // Standard Activity dashboard panels
    return [
      {
        id: 'activity_summary',
        type: 'metrics',
        source: SchemaContract.qualifySource('VW_ACTIVITY_SUMMARY')
      },
      {
        id: 'activity_counts',
        type: 'chart',
        source: SchemaContract.qualifySource('VW_ACTIVITY_COUNTS_24H'),
        x: 'hour',
        y: 'event_count',
        metric: 'event_count'
      },
      {
        id: 'top_activities',
        type: 'table',
        source: SchemaContract.qualifySource('VW_ACTIVITY_COUNTS_24H'),
        metric: 'event_count',
        top_n: 10
      },
      {
        id: 'sql_executions',
        type: 'table',
        source: SchemaContract.qualifySource('VW_SQL_EXECUTIONS'),
        metric: 'execution_count',
        top_n: 10
      }
    ];
  }

  /**
   * Create view for panel
   */
  async createPanelView(spec, panel) {
    const viewName = `${spec.name}_${panel.id}_${spec.hash.substring(0, 8)}`;
    const qualifiedViewName = SchemaContract.fqn('ANALYTICS', viewName);

    let sql = '';
    
    switch (panel.type) {
      case 'metrics':
        sql = `CREATE OR REPLACE VIEW ${qualifiedViewName} AS 
               SELECT * FROM ${panel.source}`;
        break;
        
      case 'chart':
        sql = `CREATE OR REPLACE VIEW ${qualifiedViewName} AS
               SELECT ${panel.x}, ${panel.y}
               FROM ${panel.source}
               ORDER BY ${panel.x}`;
        break;
        
      case 'table':
        sql = `CREATE OR REPLACE VIEW ${qualifiedViewName} AS
               SELECT * FROM ${panel.source}
               ORDER BY ${panel.metric} DESC
               LIMIT ${panel.top_n || 100}`;
        break;
        
      default:
        sql = `CREATE OR REPLACE VIEW ${qualifiedViewName} AS
               SELECT * FROM ${panel.source}
               LIMIT 100`;
    }

    await this.execute(sql);
    return viewName;
  }

  /**
   * Generate Streamlit dashboard code
   */
  generateStreamlit(spec, views) {
    return `"""
${spec.name.toUpperCase()} - Generated Dashboard
Generated: ${new Date().toISOString()}
"""

import streamlit as st
import snowflake.connector
import pandas as pd
import os
from datetime import datetime, timedelta

st.set_page_config(page_title="${spec.name}", layout="wide")
st.title("📊 ${spec.name.replace(/_/g, ' ').toUpperCase()}")

# Connect to Snowflake
@st.cache_resource
def get_connection():
    return snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USERNAME'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        database='${SchemaContract.contract.database}',
        schema='ANALYTICS',
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE')
    )

conn = get_connection()

# Load data
${views.map((view, i) => `
@st.cache_data(ttl=3600)
def load_${spec.panels[i].id}_data():
    query = "SELECT * FROM ${view}"
    return pd.read_sql(query, conn)

${spec.panels[i].id}_df = load_${spec.panels[i].id}_data()
`).join('')}

# Display panels
col1, col2 = st.columns(2)

${spec.panels.map((panel, i) => {
  if (panel.type === 'metrics') {
    return `
with col1:
    st.subheader("📈 ${panel.id.replace(/_/g, ' ').toUpperCase()}")
    if not ${panel.id}_df.empty:
        for col in ${panel.id}_df.columns:
            st.metric(col, ${panel.id}_df[col].iloc[0])`;
  } else if (panel.type === 'chart') {
    return `
with col${i % 2 + 1}:
    st.subheader("📊 ${panel.id.replace(/_/g, ' ').toUpperCase()}")
    if not ${panel.id}_df.empty:
        st.line_chart(${panel.id}_df.set_index('${panel.x}')['${panel.y}'])`;
  } else {
    return `
with col${i % 2 + 1}:
    st.subheader("📋 ${panel.id.replace(/_/g, ' ').toUpperCase()}")
    st.dataframe(${panel.id}_df)`;
  }
}).join('\n')}

# Footer
st.markdown("---")
st.caption(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Data Window: ${spec.timeWindow.window}")
`;
  }

  /**
   * Save Streamlit file
   */
  async saveStreamlitFile(filename, code) {
    const fs = require('fs').promises;
    const path = require('path');
    
    const dir = path.join(__dirname, '../generated-dashboards');
    await fs.mkdir(dir, { recursive: true });
    
    const filepath = path.join(dir, filename);
    await fs.writeFile(filepath, code);
    
    return filepath;
  }

  /**
   * Generate spec hash
   */
  generateSpecHash(spec) {
    const content = JSON.stringify({
      name: spec.name,
      panels: spec.panels.map(p => p.id)
    });
    return crypto.createHash('md5').update(content).digest('hex').substring(0, 8);
  }

  /**
   * Execute SQL
   */
  execute(sql, binds = []) {
    return new Promise((resolve, reject) => {
      this.conn.execute({
        sqlText: sql,
        binds,
        complete: (err, stmt, rows) => {
          if (err) {
            reject(err);
          } else {
            resolve({ rows, stmt });
          }
        }
      });
    });
  }

  /**
   * Drop dashboard objects
   */
  async drop(dashboardId) {
    // Implementation for cleanup
    const sql = `
      SELECT table_name 
      FROM information_schema.views 
      WHERE table_name LIKE '${dashboardId}%'
    `;
    
    const result = await this.execute(sql);
    
    for (const row of result.rows) {
      await this.execute(`DROP VIEW IF EXISTS ${row.TABLE_NAME}`);
    }
    
    await this.logger.log('dashboard_dropped', { dashboard_id: dashboardId });
  }
}

module.exports = DashboardFactory;