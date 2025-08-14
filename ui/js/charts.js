/**
 * Chart.js Configuration
 * Handles chart initialization and updates
 */

let trendChart = null;
let rankingChart = null;

/**
 * Initialize charts on page load
 */
function initCharts() {
  // Get computed styles for theming
  const styles = getComputedStyle(document.documentElement);
  const accentColor = styles.getPropertyValue('--accent').trim();
  const borderColor = styles.getPropertyValue('--border').trim();
  const textColor = styles.getPropertyValue('--text').trim();
  const textDimColor = styles.getPropertyValue('--text-dim').trim();
  
  // Configure Chart.js defaults
  Chart.defaults.color = textColor;
  Chart.defaults.borderColor = borderColor;
  Chart.defaults.font.family = '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif';
  
  // Initialize trend chart (time series)
  const trendCtx = document.getElementById('trend-chart');
  if (trendCtx) {
    trendChart = new Chart(trendCtx.getContext('2d'), {
      type: 'line',
      data: {
        labels: [],
        datasets: [{
          label: 'Activity Count',
          data: [],
          borderColor: accentColor,
          backgroundColor: hexToRgba(accentColor, 0.1),
          fill: true,
          tension: 0.4,
          pointRadius: 3,
          pointHoverRadius: 5,
          pointBackgroundColor: accentColor,
          pointBorderColor: '#fff',
          pointBorderWidth: 2
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        interaction: {
          intersect: false,
          mode: 'index'
        },
        plugins: {
          legend: {
            display: false
          },
          tooltip: {
            backgroundColor: styles.getPropertyValue('--surface').trim(),
            titleColor: textColor,
            bodyColor: textDimColor,
            borderColor: borderColor,
            borderWidth: 1,
            padding: 12,
            displayColors: false,
            callbacks: {
              label: (context) => {
                return `Count: ${formatNumber(context.parsed.y)}`;
              }
            }
          }
        },
        scales: {
          x: {
            grid: {
              color: borderColor,
              display: true,
              drawBorder: false
            },
            ticks: {
              color: textDimColor,
              maxRotation: 45,
              minRotation: 0,
              autoSkip: true,
              maxTicksLimit: 8
            }
          },
          y: {
            beginAtZero: true,
            grid: {
              color: borderColor,
              display: true,
              drawBorder: false
            },
            ticks: {
              color: textDimColor,
              callback: (value) => formatNumber(value)
            }
          }
        }
      }
    });
  }
  
  // Initialize ranking chart (horizontal bar)
  const rankingCtx = document.getElementById('ranking-chart');
  if (rankingCtx) {
    rankingChart = new Chart(rankingCtx.getContext('2d'), {
      type: 'bar',
      data: {
        labels: [],
        datasets: [{
          label: 'Count',
          data: [],
          backgroundColor: accentColor,
          borderColor: accentColor,
          borderWidth: 0,
          barThickness: 'flex',
          maxBarThickness: 40
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        indexAxis: 'y',
        plugins: {
          legend: {
            display: false
          },
          tooltip: {
            backgroundColor: styles.getPropertyValue('--surface').trim(),
            titleColor: textColor,
            bodyColor: textDimColor,
            borderColor: borderColor,
            borderWidth: 1,
            padding: 12,
            displayColors: false,
            callbacks: {
              label: (context) => {
                return `Count: ${formatNumber(context.parsed.x)}`;
              }
            }
          }
        },
        scales: {
          x: {
            beginAtZero: true,
            grid: {
              color: borderColor,
              display: true,
              drawBorder: false
            },
            ticks: {
              color: textDimColor,
              callback: (value) => formatNumber(value)
            }
          },
          y: {
            grid: {
              display: false,
              drawBorder: false
            },
            ticks: {
              color: textDimColor,
              autoSkip: false,
              callback: function(value, index) {
                const label = this.getLabelForValue(value);
                // Truncate long labels
                return label.length > 20 ? label.substr(0, 20) + '...' : label;
              }
            }
          }
        }
      }
    });
  }
}

/**
 * Update chart with new data
 */
function updateChart(chartId, data) {
  if (!data || !data.labels || !data.values) {
    console.warn('Invalid chart data:', data);
    return;
  }
  
  // Cap data points for performance
  if (data.labels.length > 500) {
    data = downsampleData(data, 500);
  }
  
  if (chartId === 'trend' && trendChart) {
    trendChart.data.labels = data.labels;
    trendChart.data.datasets[0].data = data.values;
    trendChart.update('none'); // Skip animation for performance
    
  } else if (chartId === 'ranking' && rankingChart) {
    // Only show top 10 for ranking
    const topN = 10;
    rankingChart.data.labels = data.labels.slice(0, topN);
    rankingChart.data.datasets[0].data = data.values.slice(0, topN);
    rankingChart.update('none'); // Skip animation for performance
  }
}

/**
 * Downsample data for performance
 */
function downsampleData(data, maxPoints) {
  const { labels, values } = data;
  const step = Math.ceil(labels.length / maxPoints);
  
  const downsampled = {
    labels: [],
    values: []
  };
  
  for (let i = 0; i < labels.length; i += step) {
    downsampled.labels.push(labels[i]);
    
    // Average values in the window
    let sum = 0;
    let count = 0;
    for (let j = i; j < Math.min(i + step, values.length); j++) {
      sum += values[j];
      count++;
    }
    downsampled.values.push(Math.round(sum / count));
  }
  
  return downsampled;
}

/**
 * Update chart theme when toggled
 */
function updateChartTheme() {
  const styles = getComputedStyle(document.documentElement);
  const accentColor = styles.getPropertyValue('--accent').trim();
  const borderColor = styles.getPropertyValue('--border').trim();
  const textColor = styles.getPropertyValue('--text').trim();
  const textDimColor = styles.getPropertyValue('--text-dim').trim();
  
  // Update Chart.js defaults
  Chart.defaults.color = textColor;
  Chart.defaults.borderColor = borderColor;
  
  // Update existing charts
  if (trendChart) {
    trendChart.options.plugins.tooltip.backgroundColor = styles.getPropertyValue('--surface').trim();
    trendChart.options.plugins.tooltip.titleColor = textColor;
    trendChart.options.plugins.tooltip.bodyColor = textDimColor;
    trendChart.options.scales.x.grid.color = borderColor;
    trendChart.options.scales.x.ticks.color = textDimColor;
    trendChart.options.scales.y.grid.color = borderColor;
    trendChart.options.scales.y.ticks.color = textDimColor;
    trendChart.update();
  }
  
  if (rankingChart) {
    rankingChart.options.plugins.tooltip.backgroundColor = styles.getPropertyValue('--surface').trim();
    rankingChart.options.plugins.tooltip.titleColor = textColor;
    rankingChart.options.plugins.tooltip.bodyColor = textDimColor;
    rankingChart.options.scales.x.grid.color = borderColor;
    rankingChart.options.scales.x.ticks.color = textDimColor;
    rankingChart.options.scales.y.ticks.color = textDimColor;
    rankingChart.update();
  }
}

/**
 * Utility: Convert hex to rgba
 */
function hexToRgba(hex, alpha) {
  const r = parseInt(hex.slice(1, 3), 16);
  const g = parseInt(hex.slice(3, 5), 16);
  const b = parseInt(hex.slice(5, 7), 16);
  return `rgba(${r}, ${g}, ${b}, ${alpha})`;
}

/**
 * Utility: Format number for display
 */
function formatNumber(num) {
  if (num === null || num === undefined) return '0';
  if (num >= 1000000) return `${(num / 1000000).toFixed(1)}M`;
  if (num >= 1000) return `${(num / 1000).toFixed(1)}K`;
  return num.toString();
}

// Listen for theme changes
document.addEventListener('themeChanged', updateChartTheme);