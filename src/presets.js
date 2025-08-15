/**
 * Dashboard Preset Configurations
 * One-click SafeSQL buttons for executives
 */

const PRESETS = {
  // Time-based presets
  time: [
    {
      id: 'today',
      label: 'Today',
      icon: '📅',
      description: 'Last 24 hours',
      proc: 'DASH_GET_SERIES',
      params: {
        start_ts: 'DATEADD(hour, -24, CURRENT_TIMESTAMP())',
        end_ts: 'CURRENT_TIMESTAMP()',
        interval_str: 'hour',
        filters: null,
        group_by: null
      }
    },
    {
      id: 'last_6h_15min',
      label: 'Last 6h (15-min)',
      icon: '⏰',
      description: '15-minute buckets for last 6 hours',
      proc: 'DASH_GET_SERIES',
      params: {
        start_ts: 'DATEADD(hour, -6, CURRENT_TIMESTAMP())',
        end_ts: 'CURRENT_TIMESTAMP()',
        interval_str: '15 minute',
        filters: null,
        group_by: null
      }
    },
    {
      id: 'last_hour_5min',
      label: 'Last Hour (5-min)',
      icon: '⚡',
      description: '5-minute buckets for last hour',
      proc: 'DASH_GET_SERIES',
      params: {
        start_ts: 'DATEADD(hour, -1, CURRENT_TIMESTAMP())',
        end_ts: 'CURRENT_TIMESTAMP()',
        interval_str: '5 minute',
        filters: null,
        group_by: null
      }
    },
    {
      id: 'this_week',
      label: 'This Week',
      icon: '📊',
      description: 'Daily buckets for current week',
      proc: 'DASH_GET_SERIES',
      params: {
        start_ts: 'DATE_TRUNC(week, CURRENT_DATE())',
        end_ts: 'CURRENT_TIMESTAMP()',
        interval_str: 'day',
        filters: null,
        group_by: null
      }
    }
  ],

  // Top-N presets
  rankings: [
    {
      id: 'top_actions',
      label: 'Top Actions',
      icon: '🎯',
      description: 'Most frequent actions today',
      proc: 'DASH_GET_TOPN',
      params: {
        start_ts: 'DATEADD(hour, -24, CURRENT_TIMESTAMP())',
        end_ts: 'CURRENT_TIMESTAMP()',
        dimension: 'action',
        filters: null,
        n: 10
      }
    },
    {
      id: 'top_users',
      label: 'Top Users',
      icon: '👥',
      description: 'Most active users today',
      proc: 'DASH_GET_TOPN',
      params: {
        start_ts: 'DATEADD(hour, -24, CURRENT_TIMESTAMP())',
        end_ts: 'CURRENT_TIMESTAMP()',
        dimension: 'actor_id',
        filters: null,
        n: 10
      }
    },
    {
      id: 'top_errors',
      label: 'Top Errors',
      icon: '⚠️',
      description: 'Most frequent errors today',
      proc: 'DASH_GET_TOPN',
      params: {
        start_ts: 'DATEADD(hour, -24, CURRENT_TIMESTAMP())',
        end_ts: 'CURRENT_TIMESTAMP()',
        dimension: 'action',
        filters: { 'action': "LIKE 'error.%'" },
        n: 10
      }
    }
  ],

  // Metrics presets
  metrics: [
    {
      id: 'summary_today',
      label: 'Today\'s Summary',
      icon: '📈',
      description: 'Key metrics for today',
      proc: 'DASH_GET_METRICS',
      params: {
        start_ts: 'DATEADD(hour, -24, CURRENT_TIMESTAMP())',
        end_ts: 'CURRENT_TIMESTAMP()',
        filters: null
      }
    },
    {
      id: 'summary_hour',
      label: 'Last Hour',
      icon: '⏱️',
      description: 'Key metrics for last hour',
      proc: 'DASH_GET_METRICS',
      params: {
        start_ts: 'DATEADD(hour, -1, CURRENT_TIMESTAMP())',
        end_ts: 'CURRENT_TIMESTAMP()',
        filters: null
      }
    }
  ],

  // Live stream preset
  live: [
    {
      id: 'live_stream',
      label: 'Live Activity',
      icon: '🔴',
      description: 'Real-time event stream',
      proc: 'DASH_GET_EVENTS',
      params: {
        cursor_ts: 'DATEADD(minute, -5, CURRENT_TIMESTAMP())',
        limit_rows: 50
      }
    }
  ]
};

/**
 * Get all presets organized by category
 */
function getAllPresets() {
  return PRESETS;
}

/**
 * Get a specific preset by ID
 */
function getPresetById(presetId) {
  for (const category of Object.values(PRESETS)) {
    const preset = category.find(p => p.id === presetId);
    if (preset) return preset;
  }
  return null;
}

/**
 * Convert preset params to SQL-ready values
 * Handles timestamp expressions
 */
function resolvePresetParams(params) {
  const resolved = {};
  
  for (const [key, value] of Object.entries(params)) {
    if (typeof value === 'string' && 
        (value.includes('CURRENT_') || value.includes('DATEADD') || value.includes('DATE_TRUNC'))) {
      // This is a SQL expression that needs to be evaluated server-side
      resolved[key] = { sql_expr: value };
    } else {
      resolved[key] = value;
    }
  }
  
  return resolved;
}

/**
 * Generate HTML for preset buttons
 */
function generatePresetButtons(category = 'time') {
  const presets = PRESETS[category] || [];
  
  return presets.map(preset => `
    <button 
      onclick="executePreset('${preset.id}')"
      class="preset-btn"
      data-preset-id="${preset.id}"
      title="${preset.description}">
      <span class="preset-icon">${preset.icon}</span>
      <span class="preset-label">${preset.label}</span>
    </button>
  `).join('');
}

/**
 * Create a custom preset from user configuration
 */
function createCustomPreset(config) {
  return {
    id: `custom_${Date.now()}`,
    label: config.label || 'Custom Query',
    icon: config.icon || '🔧',
    description: config.description || 'Custom dashboard query',
    proc: config.proc,
    params: config.params
  };
}

/**
 * Save a preset for reuse (localStorage)
 */
function saveCustomPreset(preset) {
  const saved = JSON.parse(localStorage.getItem('customPresets') || '[]');
  saved.push(preset);
  localStorage.setItem('customPresets', JSON.stringify(saved));
  return preset;
}

/**
 * Load saved custom presets
 */
function loadCustomPresets() {
  return JSON.parse(localStorage.getItem('customPresets') || '[]');
}

// Export for use in other modules
if (typeof module !== 'undefined' && module.exports) {
  module.exports = {
    PRESETS,
    getAllPresets,
    getPresetById,
    resolvePresetParams,
    generatePresetButtons,
    createCustomPreset,
    saveCustomPreset,
    loadCustomPresets
  };
}