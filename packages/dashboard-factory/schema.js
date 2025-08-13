// Frozen Dashboard Spec Schema v1 - DO NOT MODIFY
// Add new fields in v2+ only, maintain backward compatibility

const Ajv = require('ajv');
const addFormats = require('ajv-formats');

// Create AJV instance with less strict validation for conditionals
const ajv = new Ajv({ strict: false, allErrors: true });
addFormats(ajv);

// Frozen Dashboard Spec Schema v1
const DASHBOARD_SPEC_SCHEMA = {
  $schema: "http://json-schema.org/draft-07/schema#",
  type: "object",
  additionalProperties: false,
  required: ["name", "timezone", "panels", "schedule"],
  properties: {
    name: {
      type: "string",
      pattern: "^[a-z][a-z0-9_]{2,63}$",
      description: "Dashboard name: lowercase, alphanumeric + underscore, 3-64 chars"
    },
    timezone: {
      type: "string",
      enum: [
        "America/New_York", "America/Chicago", "America/Denver", "America/Los_Angeles",
        "Europe/London", "Europe/Paris", "Europe/Berlin", "Europe/Rome",
        "Asia/Tokyo", "Asia/Shanghai", "Asia/Kolkata", "Australia/Sydney",
        "UTC"
      ],
      description: "IANA timezone for schedule display (cron always stored as UTC)"
    },
    panels: {
      type: "array",
      minItems: 1,
      maxItems: 6,
      items: {
        type: "object",
        additionalProperties: false,
        required: ["id", "type", "source", "metric"],
        properties: {
          id: {
            type: "string",
            pattern: "^[a-z][a-z0-9_]{2,31}$",
            description: "Panel ID: lowercase, alphanumeric + underscore, 3-32 chars"
          },
          type: {
            type: "string",
            enum: ["table", "timeseries", "metric", "chart"],
            description: "Panel visualization type"
          },
          source: {
            type: "string",
            pattern: "^[a-zA-Z][a-zA-Z0-9_]{2,63}$",
            description: "Source table/view name (must exist in Snowflake)"
          },
          metric: {
            type: "string",
            enum: [
              "COUNT(*)", "SUM(revenue)", "AVG(revenue)", "MAX(revenue)", "MIN(revenue)",
              "SUM(quantity)", "AVG(quantity)", "COUNT(DISTINCT customer_id)",
              "COUNT(DISTINCT order_id)", "SUM(profit)", "AVG(profit)"
            ],
            description: "Allowed SafeSQL aggregations only - no raw SQL"
          },
          group_by: {
            type: "array",
            maxItems: 3,
            items: {
              type: "string",
              pattern: "^[a-zA-Z][a-zA-Z0-9_]{1,63}$"
            },
            description: "Column names for GROUP BY (max 3 for performance)"
          },
          window: {
            type: "object",
            additionalProperties: false,
            properties: {
              days: { type: "integer", minimum: 1, maximum: 730 },
              weeks: { type: "integer", minimum: 1, maximum: 104 },
              months: { type: "integer", minimum: 1, maximum: 24 },
              quarters: { type: "integer", minimum: 1, maximum: 8 },
              years: { type: "integer", minimum: 1, maximum: 3 }
            },
            minProperties: 1,
            maxProperties: 1,
            description: "Time window for data filtering (exactly one unit)"
          },
          top_n: {
            type: "integer",
            minimum: 1,
            maximum: 1000,
            description: "Limit for Top-N results (max 1000 for performance)"
          },
          grain: {
            type: "string",
            enum: ["hour", "day", "week", "month", "quarter", "year"],
            description: "Time grain for timeseries panels only"
          }
        }
      }
    },
    schedule: {
      type: "object",
      additionalProperties: false,
      required: ["mode"],
      properties: {
        mode: {
          type: "string",
          enum: ["exact", "freshness"],
          description: "Scheduling mode: exact (Tasks) or freshness (Dynamic Tables)"
        },
        cron_utc: {
          type: "string",
          pattern: "^[0-9*,/-]+ [0-9*,/-]+ [0-9*,/-]+ [0-9*,/-]+ [0-9*,/-]+$",
          description: "5-field cron expression in UTC (required for mode=exact)"
        },
        target_lag: {
          type: "string",
          enum: ["15 minutes", "30 minutes", "1 hour", "2 hours", "4 hours", "6 hours", "12 hours", "1 day"],
          description: "Freshness target lag (required for mode=freshness)"
        }
      },
      if: { properties: { mode: { const: "exact" } } },
      then: { required: ["cron_utc"] },
      else: { required: ["target_lag"] }
    }
  }
};

// Pre-compile the validator for performance
const validateDashboardSpec = ajv.compile(DASHBOARD_SPEC_SCHEMA);

// Validation helper with detailed error messages
function validateSpec(spec) {
  const isValid = validateDashboardSpec(spec);
  
  if (!isValid) {
    const errors = validateDashboardSpec.errors.map(err => ({
      path: err.instancePath || 'root',
      message: err.message,
      value: err.data
    }));
    
    return {
      valid: false,
      errors: errors,
      summary: `Dashboard spec validation failed: ${errors.length} error(s)`
    };
  }
  
  return {
    valid: true,
    errors: [],
    summary: 'Dashboard spec validation passed'
  };
}

// Generate spec hash for idempotent naming
function generateSpecHash(spec) {
  const crypto = require('crypto');
  const specString = JSON.stringify(spec, Object.keys(spec).sort());
  return crypto.createHash('md5').update(specString).digest('hex').substring(0, 8);
}

// Generate object names with consistent pattern
function generateObjectNames(spec, panelId = null) {
  const hash = generateSpecHash(spec);
  const prefix = spec.name;
  
  if (panelId) {
    return {
      base_table: `${prefix}__${panelId}__${hash}`,
      top_view: `${prefix}__${panelId}_top__${hash}`,
      task: `${prefix}__${panelId}_refresh__${hash}`,
      dynamic_table: `${prefix}__${panelId}_dt__${hash}`
    };
  }
  
  return {
    streamlit_app: `${prefix}_dashboard_${hash}`,
    warehouse: `${prefix.toUpperCase()}_DASHBOARD_WH`,
    resource_monitor: `${prefix.toUpperCase()}_DASHBOARD_MONITOR`
  };
}

// Timezone conversion helpers
function convertCronToLocalDisplay(cronUtc, timezone) {
  // Simple conversion for common patterns
  // Full implementation would use a timezone library
  const hour = parseInt(cronUtc.split(' ')[1]);
  
  const timezoneOffsets = {
    'America/New_York': -5, // EST (simplified - doesn't handle DST)
    'America/Chicago': -6,
    'America/Denver': -7,  
    'America/Los_Angeles': -8,
    'Europe/London': 0,
    'Europe/Paris': 1,
    'UTC': 0
  };
  
  const offset = timezoneOffsets[timezone] || 0;
  const localHour = (hour + offset + 24) % 24;
  
  const timeStr = localHour === 0 ? '12:00 AM' : 
                  localHour < 12 ? `${localHour}:00 AM` :
                  localHour === 12 ? '12:00 PM' : 
                  `${localHour - 12}:00 PM`;
  
  const tzAbbrev = timezone.split('/')[1]?.replace('_', ' ') || timezone;
  
  return `${timeStr} ${tzAbbrev} daily`;
}

// Example valid specs for testing
const EXAMPLE_SPECS = {
  sales_executive: {
    name: "sales_exec_dashboard",
    timezone: "America/New_York",
    panels: [
      {
        id: "top_customers",
        type: "table", 
        source: "fact_sales",
        metric: "SUM(revenue)",
        group_by: ["customer_name"],
        window: { days: 90 },
        top_n: 10
      },
      {
        id: "qtr_trends",
        type: "timeseries",
        source: "fact_sales", 
        metric: "SUM(revenue)",
        group_by: ["region"],
        grain: "quarter",
        window: { quarters: 8 }
      }
    ],
    schedule: {
      mode: "exact",
      cron_utc: "0 12 * * *"
    }
  },
  
  ops_monitoring: {
    name: "ops_dashboard",
    timezone: "UTC",
    panels: [
      {
        id: "error_counts",
        type: "metric",
        source: "system_logs",
        metric: "COUNT(*)",
        group_by: ["error_type"],
        window: { hours: 24 }
      }
    ],
    schedule: {
      mode: "freshness", 
      target_lag: "15 minutes"
    }
  }
};

module.exports = {
  DASHBOARD_SPEC_SCHEMA,
  validateSpec,
  generateSpecHash,
  generateObjectNames,
  convertCronToLocalDisplay,
  EXAMPLE_SPECS,
  
  // Version info for backward compatibility
  SCHEMA_VERSION: "1.0.0",
  SCHEMA_FROZEN_DATE: "2025-08-13"
};