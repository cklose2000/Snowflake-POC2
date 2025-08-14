/**
 * Dashboard Main Logic
 * Handles UI interactions, WebSocket communication, and data updates
 */

// Global state
let ws = null;
let updateBuffer = [];
let updateTimer = null;
let activityFeed = [];
let currentPanel = 'query'; // For mobile navigation

// Initialize dashboard on load
document.addEventListener('DOMContentLoaded', init);

async function init() {
  try {
    // Load schema first
    await bootSchema();
    
    // Render query suggestions
    renderSuggestions();
    
    // Connect WebSocket
    connectWebSocket();
    
    // Initialize charts
    initCharts();
    
    // Set up mobile panel if needed
    setupMobileNavigation();
    
    // Load initial data
    loadInitialData();
    
  } catch (error) {
    console.error('Failed to initialize dashboard:', error);
    showToast('Failed to load dashboard', 'error');
  }
}

/**
 * Render query suggestions from schema
 */
function renderSuggestions() {
  const container = document.getElementById('suggestions');
  const suggestions = getSuggestions();
  
  if (suggestions.length === 0) {
    container.innerHTML = '<p class="text-sm opacity-60">No suggestions available</p>';
    return;
  }
  
  container.innerHTML = suggestions.map((s, i) => `
    <button 
      onclick='executeSuggestion(${JSON.stringify(s.panel).replace(/'/g, "&apos;")})'
      class="w-full p-3 bg-[var(--bg)] hover:bg-opacity-80 rounded-lg text-left transition group">
      <div class="flex items-start gap-3">
        <span class="text-xl">${s.icon}</span>
        <div class="flex-1">
          <div class="font-medium">${s.label}</div>
          <div class="text-xs opacity-60">${s.description}</div>
        </div>
      </div>
    </button>
  `).join('');
}

/**
 * Execute a suggestion panel
 */
async function executeSuggestion(panel) {
  try {
    // Validate against schema
    if (panel.source) {
      validatePanel(panel);
    }
    
    // Send to server
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({
        type: 'execute_panel',
        panel: panel
      }));
      
      showToast('Executing query...', 'info');
    } else {
      showToast('Not connected to server', 'error');
    }
    
  } catch (error) {
    showToast(`Schema error: ${error.message}`, 'error');
  }
}

/**
 * Execute custom query
 */
function executeCustomQuery() {
  const textarea = document.getElementById('custom-query');
  const query = textarea.value.trim();
  
  if (!query) {
    showToast('Please enter a query', 'warning');
    return;
  }
  
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify({
      type: 'chat',
      message: query
    }));
    
    showToast('Processing: ' + query.substring(0, 50) + '...', 'info');
    // Don't clear immediately - let user see what was sent
    // Clear after a delay or on successful response
    setTimeout(() => {
      textarea.value = '';
    }, 2000);
  } else {
    showToast('Not connected to server', 'error');
  }
}

/**
 * WebSocket connection management
 */
function connectWebSocket() {
  ws = new WebSocket('ws://localhost:8080');
  
  ws.onopen = () => {
    console.log('WebSocket connected');
    showToast('Connected to server', 'success');
  };
  
  ws.onmessage = (event) => {
    const data = JSON.parse(event.data);
    handleWSMessage(data);
  };
  
  ws.onerror = (error) => {
    console.error('WebSocket error:', error);
    showToast('Connection error', 'error');
  };
  
  ws.onclose = () => {
    console.log('WebSocket disconnected');
    showToast('Disconnected - reconnecting...', 'warning');
    setTimeout(connectWebSocket, 3000);
  };
}

/**
 * Handle WebSocket messages with batching
 */
function handleWSMessage(data) {
  // Add to buffer
  updateBuffer.push(data);
  
  // Batch updates every 500ms
  if (!updateTimer) {
    updateTimer = setTimeout(() => {
      flushUpdates();
      updateTimer = null;
    }, 500);
  }
}

/**
 * Flush buffered updates
 */
function flushUpdates() {
  for (const update of updateBuffer) {
    switch (update.type) {
      case 'query_result':
        handleQueryResult(update.result);
        break;
        
      case 'dashboard_complete':
        handleDashboardComplete(update.result);
        break;
        
      case 'activity':
        addToActivityFeed(update);
        break;
        
      case 'metrics':
        updateMetrics(update.data);
        break;
        
      case 'response':
        showToast(update.message, 'info');
        break;
        
      case 'error':
        showToast(update.message || update.error, 'error');
        break;
    }
  }
  updateBuffer = [];
}

/**
 * Handle query result
 */
function handleQueryResult(result) {
  if (!result || !result.rows) {
    showToast('No data returned', 'warning');
    return;
  }
  
  // Clear the query input on successful result
  const textarea = document.getElementById('custom-query');
  if (textarea && textarea.value) {
    textarea.value = '';
  }
  
  // Update charts based on data structure or query type hint
  const queryType = result.queryType;
  
  if (queryType === 'time_series' || result.rows[0]?.HOUR !== undefined) {
    // Time series data
    const labels = result.rows.map(r => formatTime(r.HOUR));
    const values = result.rows.map(r => r.ACTIVITY_COUNT || r.EVENT_COUNT || 0);
    updateChart('trend', { labels, values });
    
    // Switch to viz panel on mobile
    if (window.innerWidth < 768) {
      showMobilePanel('viz');
    }
    
  } else if (queryType === 'ranking' || (result.rows[0]?.ACTIVITY !== undefined && result.rows[0]?.EVENT_COUNT !== undefined)) {
    // Ranking data
    const labels = result.rows.map(r => r.ACTIVITY);
    const values = result.rows.map(r => r.METRIC_VALUE || r.EVENT_COUNT || 0);
    updateChart('ranking', { labels, values });
    
    // Switch to viz panel on mobile
    if (window.innerWidth < 768) {
      showMobilePanel('viz');
    }
    
  } else if (queryType === 'metrics' || result.rows[0]?.TOTAL_EVENTS !== undefined) {
    // Metrics data
    updateMetrics(result.rows[0]);
    
  } else if (queryType === 'feed' || result.rows[0]?.activity_id !== undefined) {
    // Activity feed data
    result.rows.forEach(row => {
      addToActivityFeed({
        activity: row.activity,
        customer: row.customer,
        ts: row.ts,
        activity_id: row.activity_id
      });
    });
    
    // Switch to activity panel on mobile
    if (window.innerWidth < 768) {
      showMobilePanel('activity');
    }
  }
  
  showToast(`Loaded ${result.rowCount} rows`, 'success');
}

/**
 * Handle dashboard completion
 */
function handleDashboardComplete(result) {
  if (result.errors && result.errors.length > 0) {
    showToast(`Dashboard created with ${result.errors.length} errors`, 'warning');
  } else {
    showToast(`Dashboard created: ${result.dashboard_id}`, 'success');
  }
  
  // Switch to viz panel on mobile
  if (window.innerWidth < 768) {
    showMobilePanel('viz');
  }
}

/**
 * Add item to activity feed
 */
function addToActivityFeed(activity) {
  // Add to beginning of feed
  activityFeed.unshift(activity);
  
  // Keep only last 50 items for performance
  if (activityFeed.length > 50) {
    activityFeed = activityFeed.slice(0, 50);
  }
  
  // Update UI
  renderActivityFeed();
}

/**
 * Render activity feed with virtual scrolling
 */
function renderActivityFeed() {
  const container = document.getElementById('activity-feed');
  
  // Only render visible items (simple virtual scrolling)
  const visibleItems = activityFeed.slice(0, 20);
  
  container.innerHTML = visibleItems.map(item => `
    <div class="activity-item bg-[var(--bg)] p-3 rounded-lg">
      <div class="flex justify-between items-start">
        <div>
          <div class="text-sm font-medium">${item.activity || 'Unknown'}</div>
          <div class="text-xs opacity-60">${item.customer || 'System'}</div>
        </div>
        <div class="text-xs opacity-40">${formatTime(item.ts)}</div>
      </div>
    </div>
  `).join('');
}

/**
 * Update metric cards
 */
function updateMetrics(data) {
  if (data.TOTAL_EVENTS !== undefined) {
    document.getElementById('metric-total').textContent = formatNumber(data.TOTAL_EVENTS);
  }
  if (data.UNIQUE_CUSTOMERS !== undefined) {
    document.getElementById('metric-users').textContent = formatNumber(data.UNIQUE_CUSTOMERS);
  }
}

/**
 * Mobile panel navigation
 */
function setupMobileNavigation() {
  // Set initial panel visibility
  if (window.innerWidth < 768) {
    document.querySelectorAll('.mobile-panel').forEach(panel => {
      panel.classList.remove('active');
    });
    document.getElementById('query-panel').classList.add('active');
  }
}

function showMobilePanel(panelName) {
  // Only on mobile
  if (window.innerWidth >= 768) return;
  
  // Hide all panels
  document.querySelectorAll('.mobile-panel').forEach(panel => {
    panel.classList.remove('active');
  });
  
  // Show selected panel
  document.getElementById(`${panelName}-panel`).classList.add('active');
  
  // Update tab active state
  document.querySelectorAll('.mobile-tab').forEach(tab => {
    tab.classList.remove('active');
  });
  document.querySelector(`[data-panel="${panelName}"]`).classList.add('active');
  
  currentPanel = panelName;
}

/**
 * Theme toggle
 */
function toggleTheme() {
  const html = document.documentElement;
  if (html.classList.contains('dark')) {
    html.classList.remove('dark');
    html.classList.add('light');
    localStorage.setItem('theme', 'light');
  } else {
    html.classList.remove('light');
    html.classList.add('dark');
    localStorage.setItem('theme', 'dark');
  }
}

/**
 * Load initial data
 */
function loadInitialData() {
  // Request summary metrics
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify({
      type: 'execute_panel',
      panel: {
        source: 'VW_ACTIVITY_SUMMARY',
        type: 'metrics'
      }
    }));
  }
}

/**
 * Toast notifications
 */
function showToast(message, type = 'info') {
  const container = document.getElementById('toast-container');
  const toast = document.createElement('div');
  toast.className = `toast ${type}`;
  toast.textContent = message;
  
  container.appendChild(toast);
  
  // Auto remove after 3 seconds
  setTimeout(() => {
    toast.style.opacity = '0';
    setTimeout(() => toast.remove(), 300);
  }, 3000);
}

/**
 * Utility functions
 */
function formatTime(timestamp) {
  if (!timestamp) return '';
  const date = new Date(timestamp);
  const now = new Date();
  const diff = now - date;
  
  if (diff < 60000) return 'Just now';
  if (diff < 3600000) return `${Math.floor(diff / 60000)}m ago`;
  if (diff < 86400000) return `${Math.floor(diff / 3600000)}h ago`;
  
  return date.toLocaleDateString();
}

function formatNumber(num) {
  if (num === null || num === undefined) return '-';
  if (num >= 1000000) return `${(num / 1000000).toFixed(1)}M`;
  if (num >= 1000) return `${(num / 1000).toFixed(1)}K`;
  return num.toString();
}

// Initialize theme from localStorage
const savedTheme = localStorage.getItem('theme');
if (savedTheme) {
  document.documentElement.classList.toggle('dark', savedTheme === 'dark');
  document.documentElement.classList.toggle('light', savedTheme === 'light');
}