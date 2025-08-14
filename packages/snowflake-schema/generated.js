// AUTO-GENERATED FILE - DO NOT EDIT
// Generated from schemas/activity_v2.contract.json
// Contract Hash: 439f8097e41903a7
// Generated: 2025-08-13T19:27:33.498Z

/**
 * Type-safe schema definitions generated from Activity Schema v2.0 contract
 * This is the ONLY source for schema object references in the application
 */

// Environment Configuration
export const DB = process.env.SNOWFLAKE_DATABASE || 'CLAUDE_BI';
export const WAREHOUSE = process.env.SNOWFLAKE_WAREHOUSE || 'CLAUDE_WAREHOUSE';
export const ROLE = process.env.SNOWFLAKE_ROLE || 'CLAUDE_BI_ROLE';
export const DEFAULT_SCHEMA = process.env.SNOWFLAKE_SCHEMA || 'ANALYTICS';

// Contract metadata
export const CONTRACT_VERSION = "2.0.0";
export const CONTRACT_HASH = "439f8097e41903a7";

// Schema definitions (const assertions for type safety)
export const SCHEMAS = {
  ACTIVITY: "ACTIVITY",
  ACTIVITY_CCODE: "ACTIVITY_CCODE",
  ANALYTICS: "ANALYTICS",
};

// Table definitions by schema
export const TABLES = {
  ACTIVITY: {
    EVENTS: "EVENTS",
  },
  ACTIVITY_CCODE: {
    ARTIFACTS: "ARTIFACTS",
    AUDIT_RESULTS: "AUDIT_RESULTS",
  },
  ANALYTICS: {
    SCHEMA_VERSION: "SCHEMA_VERSION",
  },
};

// View definitions by schema
export const VIEWS = {
  ACTIVITY_CCODE: {
    VW_ACTIVITY_COUNTS_24H: "VW_ACTIVITY_COUNTS_24H",
    VW_LLM_TELEMETRY: "VW_LLM_TELEMETRY",
    VW_SQL_EXECUTIONS: "VW_SQL_EXECUTIONS",
    VW_DASHBOARD_OPERATIONS: "VW_DASHBOARD_OPERATIONS",
    VW_SAFESQL_TEMPLATES: "VW_SAFESQL_TEMPLATES",
    VW_ACTIVITY_SUMMARY: "VW_ACTIVITY_SUMMARY",
  },
};

// Fully Qualified Name helpers
export function fqn(schema, object) {
  return `${DB}.${SCHEMAS[schema]}.${object}`;
}

export function twoPartName(schema, object) {
  return `${SCHEMAS[schema]}.${object}`;
}

// Activity view mapping for panel sources
export const ACTIVITY_VIEW_MAP = {
  "VW_ACTIVITY_COUNTS_24H": fqn("ACTIVITY_CCODE", "VW_ACTIVITY_COUNTS_24H"),
  "VW_LLM_TELEMETRY": fqn("ACTIVITY_CCODE", "VW_LLM_TELEMETRY"),
  "VW_SQL_EXECUTIONS": fqn("ACTIVITY_CCODE", "VW_SQL_EXECUTIONS"),
  "VW_DASHBOARD_OPERATIONS": fqn("ACTIVITY_CCODE", "VW_DASHBOARD_OPERATIONS"),
  "VW_SAFESQL_TEMPLATES": fqn("ACTIVITY_CCODE", "VW_SAFESQL_TEMPLATES"),
  "VW_ACTIVITY_SUMMARY": fqn("ACTIVITY_CCODE", "VW_ACTIVITY_SUMMARY"),
};

// Source qualification helper (replaces qualifySource)
export function qualifySource(source) {
  // Already qualified?
  if (source.includes('.')) return source;
  
  // Known Activity views map to ACTIVITY_CCODE schema
  if (source in ACTIVITY_VIEW_MAP) {
    return ACTIVITY_VIEW_MAP[source];
  }
  
  // Default to ANALYTICS schema
  return fqn("ANALYTICS", source);
}

// Context SQL generation
export function getContextSQL(options = {}) {
  const statements = [
    WAREHOUSE && `USE WAREHOUSE ${WAREHOUSE}`,
    `USE DATABASE ${DB}`,
    `USE SCHEMA ${DEFAULT_SCHEMA}`
  ].filter(Boolean);
  
  if (options.queryTag) {
    statements.push(`ALTER SESSION SET QUERY_TAG = '${options.queryTag}'`);
  }
  
  return statements;
}

// Activity namespace helpers
export const ACTIVITY_NAMESPACE = "ccode";
export const STANDARD_ACTIVITIES = [
  "ccode.user_asked",
  "ccode.sql_executed",
  "ccode.artifact_created",
  "ccode.audit_passed",
  "ccode.audit_failed",
  "ccode.bridge_started",
  "ccode.agent_invoked",
  "ccode.dashboard_created",
  "ccode.dashboard_failed",
  "ccode.dashboard_destroyed",
  "ccode.schema_violation",
];

export function createActivityName(action) {
  return `${ACTIVITY_NAMESPACE}.${action}`;
}

// Schedule configuration
export const SCHEDULE_MODES = ["exact"];
export const DEFAULT_CRON = "0 12 * * *";
export const FALLBACK_BEHAVIOR = "create_unscheduled";

// Schema validation patterns
export const VALIDATION_PATTERNS = {
  no_raw_fqns: {
    pattern: new RegExp("\\b\\w+\\.\\w+\\.\\w+\\b", "g"),
    exceptions: ["packages/snowflake-schema/generated.js"],
    description: "All schema references must use generated helpers"
  },
  no_unqualified_views: {
    pattern: new RegExp("\\bVW_[A-Z0-9_]+", "g"),
    requiredPrefix: "ACTIVITY_CCODE",
    description: "All VW_* references must be schema-qualified"
  },
  parameterized_sql: {
    forbiddenPatterns: ["'\\$\\{[^}]+\\}'","\"\\$\\{[^}]+\\}\"","\\+ [a-zA-Z_][a-zA-Z0-9_]* \\+"].map(p => new RegExp(p, "g")),
    description: "All SQL must use parameter binds"
  },
};

// Table reference helpers with column validation
// TableReference shape: { fqn, twoPartName, schema, table, requiredColumns }

export function getTableRef(schema, table) {
  const schemaName = SCHEMAS[schema];
  if (!schemaName) {
    throw new Error(`Unknown schema: ${schema}`);
  }
  
  // Get required columns from contract
  const tableDefinition = getTableDefinition(schema, table);
  
  return {
    fqn: fqn(schema, table),
    twoPartName: twoPartName(schema, table),
    schema: schemaName,
    table,
    requiredColumns: tableDefinition?.required_columns?.map(col => col.name) || []
  };
}

function getTableDefinition(schema, table) {
  const tableDefinitions = {
  "ACTIVITY": {
    "description": "Core activity event stream - Activity Schema v2.0 compliant",
    "tables": {
      "EVENTS": {
        "description": "Primary event stream table",
        "required_columns": [
          {
            "name": "ACTIVITY_ID",
            "type": "VARCHAR(255)",
            "constraints": [
              "NOT NULL",
              "PRIMARY KEY"
            ]
          },
          {
            "name": "TS",
            "type": "TIMESTAMP_NTZ",
            "constraints": [
              "NOT NULL"
            ]
          },
          {
            "name": "CUSTOMER",
            "type": "VARCHAR(255)",
            "constraints": [
              "NOT NULL"
            ]
          },
          {
            "name": "ACTIVITY",
            "type": "VARCHAR(255)",
            "constraints": [
              "NOT NULL"
            ]
          },
          {
            "name": "FEATURE_JSON",
            "type": "VARIANT",
            "constraints": [
              "NOT NULL"
            ]
          }
        ],
        "activity_schema_v2_columns": [
          {
            "name": "_ACTIVITY_OCCURRENCE",
            "type": "INTEGER",
            "description": "1st, 2nd, 3rd occurrence for this customer"
          },
          {
            "name": "_ACTIVITY_REPEATED_AT",
            "type": "TIMESTAMP_NTZ",
            "description": "Performance optimization for repeated activities"
          }
        ],
        "system_columns": [
          {
            "name": "_SOURCE_SYSTEM",
            "type": "VARCHAR(255)",
            "description": "Always 'claude_code' for this implementation"
          },
          {
            "name": "_SOURCE_VERSION",
            "type": "VARCHAR(255)",
            "description": "Bridge/factory version"
          },
          {
            "name": "_SESSION_ID",
            "type": "VARCHAR(255)",
            "description": "UI session tracking"
          },
          {
            "name": "_QUERY_TAG",
            "type": "VARCHAR(255)",
            "description": "Snowflake query correlation"
          }
        ],
        "optional_columns": [
          {
            "name": "ANONYMOUS_CUSTOMER_ID",
            "type": "VARCHAR(255)",
            "description": "Pre-identification tracking"
          },
          {
            "name": "REVENUE_IMPACT",
            "type": "FLOAT",
            "description": "Money in/out"
          },
          {
            "name": "LINK",
            "type": "VARCHAR(255)",
            "description": "Reference URL"
          }
        ]
      }
    }
  },
  "ACTIVITY_CCODE": {
    "description": "Claude Code specific activity views and artifacts",
    "tables": {
      "ARTIFACTS": {
        "description": "Artifact storage and metadata",
        "required_columns": [
          {
            "name": "ARTIFACT_ID",
            "type": "VARCHAR(255)",
            "constraints": [
              "NOT NULL",
              "PRIMARY KEY"
            ]
          },
          {
            "name": "SAMPLE",
            "type": "VARIANT",
            "description": "Preview data (≤10 rows)"
          },
          {
            "name": "ROW_COUNT",
            "type": "INTEGER",
            "description": "Full result size"
          },
          {
            "name": "SCHEMA_JSON",
            "type": "VARIANT",
            "description": "Column metadata"
          },
          {
            "name": "S3_URL",
            "type": "VARCHAR(500)",
            "description": "Full data location"
          },
          {
            "name": "BYTES",
            "type": "BIGINT",
            "description": "Size metrics"
          },
          {
            "name": "CREATED_TS",
            "type": "TIMESTAMP_NTZ"
          },
          {
            "name": "CUSTOMER",
            "type": "VARCHAR(255)"
          },
          {
            "name": "CREATED_BY_ACTIVITY",
            "type": "VARCHAR(255)",
            "description": "References activity.events.activity_id"
          }
        ]
      },
      "AUDIT_RESULTS": {
        "description": "SafeSQL audit results",
        "required_columns": [
          {
            "name": "AUDIT_ID",
            "type": "VARCHAR(255)",
            "constraints": [
              "NOT NULL",
              "PRIMARY KEY"
            ]
          },
          {
            "name": "TS",
            "type": "TIMESTAMP_NTZ",
            "constraints": [
              "NOT NULL"
            ]
          },
          {
            "name": "PASSED",
            "type": "BOOLEAN",
            "constraints": [
              "NOT NULL"
            ]
          },
          {
            "name": "DETAILS",
            "type": "VARIANT"
          }
        ]
      }
    },
    "views": {
      "VW_ACTIVITY_COUNTS_24H": {
        "description": "Activity counts by type and customer for last 24 hours",
        "time_window": "24h",
        "base_table": "ACTIVITY.EVENTS"
      },
      "VW_LLM_TELEMETRY": {
        "description": "LLM usage telemetry including tokens and latency",
        "time_window": "7d",
        "base_table": "ACTIVITY.EVENTS"
      },
      "VW_SQL_EXECUTIONS": {
        "description": "SQL execution telemetry with cost and performance",
        "time_window": "7d",
        "base_table": "ACTIVITY.EVENTS"
      },
      "VW_DASHBOARD_OPERATIONS": {
        "description": "Dashboard lifecycle events",
        "time_window": "all",
        "base_table": "ACTIVITY.EVENTS"
      },
      "VW_SAFESQL_TEMPLATES": {
        "description": "SafeSQL template usage patterns",
        "time_window": "30d",
        "base_table": "ACTIVITY.EVENTS"
      },
      "VW_ACTIVITY_SUMMARY": {
        "description": "High-level activity metrics overview",
        "time_window": "24h",
        "base_table": "ACTIVITY.EVENTS"
      }
    }
  },
  "ANALYTICS": {
    "description": "Default schema for generated objects",
    "is_default": true,
    "tables": {
      "SCHEMA_VERSION": {
        "description": "Schema versioning and migration tracking",
        "required_columns": [
          {
            "name": "VERSION",
            "type": "VARCHAR(50)",
            "constraints": [
              "NOT NULL",
              "PRIMARY KEY"
            ]
          },
          {
            "name": "APPLIED_AT",
            "type": "TIMESTAMP_NTZ",
            "constraints": [
              "NOT NULL"
            ]
          },
          {
            "name": "APPLIED_BY",
            "type": "VARCHAR(255)",
            "constraints": [
              "NOT NULL"
            ]
          },
          {
            "name": "DESCRIPTION",
            "type": "TEXT"
          }
        ]
      }
    }
  }
};
  return tableDefinitions[schema]?.tables?.[table];
}

// Export contract for runtime validation
export const CONTRACT = {
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "Activity Schema v2.0 Contract",
  "description": "Single source of truth for all Snowflake schema definitions",
  "version": "2.0.0",
  "contractHash": "activity_v2_2025_01",
  "environment": {
    "database": "${SNOWFLAKE_DATABASE:-CLAUDE_BI}",
    "warehouse": "${SNOWFLAKE_WAREHOUSE:-CLAUDE_WAREHOUSE}",
    "role": "${SNOWFLAKE_ROLE:-CLAUDE_BI_ROLE}",
    "default_schema": "${SNOWFLAKE_SCHEMA:-ANALYTICS}"
  },
  "schemas": {
    "ACTIVITY": {
      "description": "Core activity event stream - Activity Schema v2.0 compliant",
      "tables": {
        "EVENTS": {
          "description": "Primary event stream table",
          "required_columns": [
            {
              "name": "ACTIVITY_ID",
              "type": "VARCHAR(255)",
              "constraints": [
                "NOT NULL",
                "PRIMARY KEY"
              ]
            },
            {
              "name": "TS",
              "type": "TIMESTAMP_NTZ",
              "constraints": [
                "NOT NULL"
              ]
            },
            {
              "name": "CUSTOMER",
              "type": "VARCHAR(255)",
              "constraints": [
                "NOT NULL"
              ]
            },
            {
              "name": "ACTIVITY",
              "type": "VARCHAR(255)",
              "constraints": [
                "NOT NULL"
              ]
            },
            {
              "name": "FEATURE_JSON",
              "type": "VARIANT",
              "constraints": [
                "NOT NULL"
              ]
            }
          ],
          "activity_schema_v2_columns": [
            {
              "name": "_ACTIVITY_OCCURRENCE",
              "type": "INTEGER",
              "description": "1st, 2nd, 3rd occurrence for this customer"
            },
            {
              "name": "_ACTIVITY_REPEATED_AT",
              "type": "TIMESTAMP_NTZ",
              "description": "Performance optimization for repeated activities"
            }
          ],
          "system_columns": [
            {
              "name": "_SOURCE_SYSTEM",
              "type": "VARCHAR(255)",
              "description": "Always 'claude_code' for this implementation"
            },
            {
              "name": "_SOURCE_VERSION",
              "type": "VARCHAR(255)",
              "description": "Bridge/factory version"
            },
            {
              "name": "_SESSION_ID",
              "type": "VARCHAR(255)",
              "description": "UI session tracking"
            },
            {
              "name": "_QUERY_TAG",
              "type": "VARCHAR(255)",
              "description": "Snowflake query correlation"
            }
          ],
          "optional_columns": [
            {
              "name": "ANONYMOUS_CUSTOMER_ID",
              "type": "VARCHAR(255)",
              "description": "Pre-identification tracking"
            },
            {
              "name": "REVENUE_IMPACT",
              "type": "FLOAT",
              "description": "Money in/out"
            },
            {
              "name": "LINK",
              "type": "VARCHAR(255)",
              "description": "Reference URL"
            }
          ]
        }
      }
    },
    "ACTIVITY_CCODE": {
      "description": "Claude Code specific activity views and artifacts",
      "tables": {
        "ARTIFACTS": {
          "description": "Artifact storage and metadata",
          "required_columns": [
            {
              "name": "ARTIFACT_ID",
              "type": "VARCHAR(255)",
              "constraints": [
                "NOT NULL",
                "PRIMARY KEY"
              ]
            },
            {
              "name": "SAMPLE",
              "type": "VARIANT",
              "description": "Preview data (≤10 rows)"
            },
            {
              "name": "ROW_COUNT",
              "type": "INTEGER",
              "description": "Full result size"
            },
            {
              "name": "SCHEMA_JSON",
              "type": "VARIANT",
              "description": "Column metadata"
            },
            {
              "name": "S3_URL",
              "type": "VARCHAR(500)",
              "description": "Full data location"
            },
            {
              "name": "BYTES",
              "type": "BIGINT",
              "description": "Size metrics"
            },
            {
              "name": "CREATED_TS",
              "type": "TIMESTAMP_NTZ"
            },
            {
              "name": "CUSTOMER",
              "type": "VARCHAR(255)"
            },
            {
              "name": "CREATED_BY_ACTIVITY",
              "type": "VARCHAR(255)",
              "description": "References activity.events.activity_id"
            }
          ]
        },
        "AUDIT_RESULTS": {
          "description": "SafeSQL audit results",
          "required_columns": [
            {
              "name": "AUDIT_ID",
              "type": "VARCHAR(255)",
              "constraints": [
                "NOT NULL",
                "PRIMARY KEY"
              ]
            },
            {
              "name": "TS",
              "type": "TIMESTAMP_NTZ",
              "constraints": [
                "NOT NULL"
              ]
            },
            {
              "name": "PASSED",
              "type": "BOOLEAN",
              "constraints": [
                "NOT NULL"
              ]
            },
            {
              "name": "DETAILS",
              "type": "VARIANT"
            }
          ]
        }
      },
      "views": {
        "VW_ACTIVITY_COUNTS_24H": {
          "description": "Activity counts by type and customer for last 24 hours",
          "time_window": "24h",
          "base_table": "ACTIVITY.EVENTS"
        },
        "VW_LLM_TELEMETRY": {
          "description": "LLM usage telemetry including tokens and latency",
          "time_window": "7d",
          "base_table": "ACTIVITY.EVENTS"
        },
        "VW_SQL_EXECUTIONS": {
          "description": "SQL execution telemetry with cost and performance",
          "time_window": "7d",
          "base_table": "ACTIVITY.EVENTS"
        },
        "VW_DASHBOARD_OPERATIONS": {
          "description": "Dashboard lifecycle events",
          "time_window": "all",
          "base_table": "ACTIVITY.EVENTS"
        },
        "VW_SAFESQL_TEMPLATES": {
          "description": "SafeSQL template usage patterns",
          "time_window": "30d",
          "base_table": "ACTIVITY.EVENTS"
        },
        "VW_ACTIVITY_SUMMARY": {
          "description": "High-level activity metrics overview",
          "time_window": "24h",
          "base_table": "ACTIVITY.EVENTS"
        }
      }
    },
    "ANALYTICS": {
      "description": "Default schema for generated objects",
      "is_default": true,
      "tables": {
        "SCHEMA_VERSION": {
          "description": "Schema versioning and migration tracking",
          "required_columns": [
            {
              "name": "VERSION",
              "type": "VARCHAR(50)",
              "constraints": [
                "NOT NULL",
                "PRIMARY KEY"
              ]
            },
            {
              "name": "APPLIED_AT",
              "type": "TIMESTAMP_NTZ",
              "constraints": [
                "NOT NULL"
              ]
            },
            {
              "name": "APPLIED_BY",
              "type": "VARCHAR(255)",
              "constraints": [
                "NOT NULL"
              ]
            },
            {
              "name": "DESCRIPTION",
              "type": "TEXT"
            }
          ]
        }
      }
    }
  },
  "activity_namespace": {
    "prefix": "ccode",
    "standard_activities": [
      "ccode.user_asked",
      "ccode.sql_executed",
      "ccode.artifact_created",
      "ccode.audit_passed",
      "ccode.audit_failed",
      "ccode.bridge_started",
      "ccode.agent_invoked",
      "ccode.dashboard_created",
      "ccode.dashboard_failed",
      "ccode.dashboard_destroyed",
      "ccode.schema_violation"
    ]
  },
  "scheduling": {
    "modes": [
      "exact"
    ],
    "exact_mode": {
      "description": "Task-based scheduling with cron expressions",
      "default_cron": "0 12 * * *",
      "fallback_behavior": "create_unscheduled"
    },
    "deprecated_modes": [
      "freshness"
    ],
    "deprecation_reason": "Dynamic Tables require change tracking not available in Activity views"
  },
  "safesql_templates": {
    "v1_allowed": [
      "describe_table",
      "sample_top",
      "top_n",
      "time_series",
      "breakdown",
      "comparison"
    ]
  },
  "validation_rules": {
    "no_raw_fqns": {
      "description": "All schema references must use generated helpers",
      "pattern": "\\b\\w+\\.\\w+\\.\\w+\\b",
      "exceptions": [
        "packages/snowflake-schema/generated.js"
      ]
    },
    "no_unqualified_views": {
      "description": "All VW_* references must be schema-qualified",
      "pattern": "\\bVW_[A-Z0-9_]+",
      "required_prefix": "ACTIVITY_CCODE"
    },
    "parameterized_sql": {
      "description": "All SQL must use parameter binds",
      "forbidden_patterns": [
        "'\\$\\{[^}]+\\}'",
        "\"\\$\\{[^}]+\\}\"",
        "\\+ [a-zA-Z_][a-zA-Z0-9_]* \\+"
      ]
    }
  },
  "contract_enforcement": {
    "pre_commit": true,
    "ci_validation": true,
    "runtime_validation": true,
    "drift_detection": true
  }
};
