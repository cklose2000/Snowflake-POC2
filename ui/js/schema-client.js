/**
 * Schema Client - Validates all operations against contract
 * Fetches schema from server and provides validation functions
 */

// Schema fetched from /meta/schema
let SCHEMA = null;
let USER_META = null;

/**
 * Boot the schema from server
 */
async function bootSchema() {
  try {
    const [schemaRes, userRes] = await Promise.all([
      fetch('/meta/schema'),
      fetch('/meta/user')
    ]);
    
    SCHEMA = await schemaRes.json();
    USER_META = await userRes.json();
    
    console.log('Schema loaded:', {
      views: Object.keys(SCHEMA.views || {}),
      tables: Object.keys(SCHEMA.tables || {}),
      hash: SCHEMA.hash
    });
    
    return SCHEMA;
  } catch (error) {
    console.error('Failed to load schema:', error);
    throw error;
  }
}

/**
 * Validate a panel spec against schema
 */
function validatePanel(panel) {
  if (!SCHEMA) {
    throw new Error('Schema not loaded');
  }
  
  const cols = SCHEMA.views[panel.source];
  if (!cols) {
    throw new Error(`Unknown view: ${panel.source}`);
  }
  
  // Collect all column references
  const needed = [
    panel.x,
    panel.y, 
    panel.metric,
    ...(panel.group_by || [])
  ].filter(Boolean).map(c => c.toUpperCase());
  
  // Validate each column exists
  for (const col of needed) {
    if (!cols.includes(col)) {
      throw new Error(`Column ${col} not found in ${panel.source}. Available: ${cols.join(', ')}`);
    }
  }
  
  return true;
}

/**
 * Get schema-driven query suggestions
 */
function getSuggestions() {
  if (!SCHEMA?.views) return [];
  
  const suggestions = [];
  
  // Only add suggestions for views that exist
  if (SCHEMA.views.VW_ACTIVITY_COUNTS_24H) {
    suggestions.push({
      icon: '📊',
      label: '24-Hour Activity Trend',
      description: 'Hourly activity counts for the last 24 hours',
      panel: {
        source: 'VW_ACTIVITY_COUNTS_24H',
        x: 'HOUR',
        metric: 'EVENT_COUNT',
        type: 'time_series'
      }
    });
    
    suggestions.push({
      icon: '🔝',
      label: 'Top Activities',
      description: 'Most frequent activities',
      panel: {
        source: 'VW_ACTIVITY_COUNTS_24H',
        metric: 'EVENT_COUNT',
        group_by: ['ACTIVITY'],
        type: 'ranking',
        top_n: 10
      }
    });
    
    suggestions.push({
      icon: '👥',
      label: 'Active Users',
      description: 'Unique users over time',
      panel: {
        source: 'VW_ACTIVITY_COUNTS_24H',
        x: 'HOUR',
        metric: 'UNIQUE_CUSTOMERS',
        type: 'time_series'
      }
    });
  }
  
  if (SCHEMA.views.VW_ACTIVITY_SUMMARY) {
    suggestions.push({
      icon: '📈',
      label: 'Activity Summary',
      description: 'Overall statistics',
      panel: {
        source: 'VW_ACTIVITY_SUMMARY',
        type: 'metrics'
      }
    });
  }
  
  // Add live feed suggestion (doesn't need view)
  suggestions.push({
    icon: '⚡',
    label: 'Live Activity Feed',
    description: 'Real-time activity stream',
    panel: {
      type: 'live_feed',
      limit: 50
    }
  });
  
  return suggestions;
}

/**
 * Build SQL from panel spec (schema-aware)
 */
function buildPanelSQL(panel) {
  if (!SCHEMA) {
    throw new Error('Schema not loaded');
  }
  
  // Special case for live feed
  if (panel.type === 'live_feed') {
    return `
      SELECT activity_id, ts, customer, activity, feature_json
      FROM CLAUDE_BI.ACTIVITY.EVENTS
      ORDER BY ts DESC
      LIMIT ${panel.limit || 50}
    `;
  }
  
  // Validate panel first
  validatePanel(panel);
  
  // Get fully qualified view name
  const source = `CLAUDE_BI.ACTIVITY_CCODE.${panel.source}`;
  
  // Build SQL based on panel type
  let sql = '';
  
  switch (panel.type) {
    case 'time_series':
      sql = `
        SELECT ${panel.x}, ${panel.metric}
        FROM ${source}
        ORDER BY ${panel.x}
      `;
      break;
      
    case 'ranking':
      if (panel.group_by && panel.group_by.length > 0) {
        const groupCols = panel.group_by.join(', ');
        sql = `
          SELECT ${groupCols}, 
                 SUM(${panel.metric}) AS METRIC_VALUE
          FROM ${source}
          GROUP BY ${groupCols}
          ORDER BY METRIC_VALUE DESC
          LIMIT ${panel.top_n || 10}
        `;
      } else {
        sql = `
          SELECT ${panel.metric}
          FROM ${source}
          ORDER BY ${panel.metric} DESC
          LIMIT ${panel.top_n || 10}
        `;
      }
      break;
      
    case 'metrics':
      sql = `SELECT * FROM ${source}`;
      break;
      
    default:
      sql = `SELECT * FROM ${source} LIMIT 100`;
  }
  
  return sql.trim();
}

/**
 * Check if schema has changed
 */
function hasSchemaChanged(newHash) {
  return SCHEMA && SCHEMA.hash !== newHash;
}

/**
 * Get available columns for a view
 */
function getViewColumns(viewName) {
  if (!SCHEMA?.views) return [];
  return SCHEMA.views[viewName] || [];
}

/**
 * Get theme preference
 */
function getTheme() {
  return USER_META?.theme || 'dark';
}

/**
 * Get user timezone
 */
function getTimezone() {
  return USER_META?.timezone || 'UTC';
}