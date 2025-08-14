/**
 * GENERATED FILE - DO NOT EDIT
 * Generated from: contracts/database.contract.json
 * Generated at: 2025-08-14T11:56:17.151Z
 * Contract version: 2.0.0
 */

export const DB = 'CLAUDE_BI' as const;

export const SCHEMAS = {
  ACTIVITY: 'ACTIVITY',
  ACTIVITY_CCODE: 'ACTIVITY_CCODE',
  ANALYTICS: 'ANALYTICS',
} as const;

export const SOURCES = {
  EVENTS: {
    schema: 'ACTIVITY',
    type: 'table' as const,
    columns: ['ACTIVITY_ID', 'TS', 'CUSTOMER', 'ACTIVITY', 'FEATURE_JSON', 'ANONYMOUS_CUSTOMER_ID', 'REVENUE_IMPACT', 'LINK', '_SOURCE_SYSTEM', '_SOURCE_VERSION', '_SESSION_ID', '_QUERY_TAG', '_ACTIVITY_OCCURRENCE', '_ACTIVITY_REPEATED_AT'],
  },
  VW_ACTIVITY_COUNTS_24H: {
    schema: 'ACTIVITY_CCODE',
    type: 'view' as const,
    columns: ['HOUR', 'ACTIVITY', 'EVENT_COUNT', 'UNIQUE_CUSTOMERS'],
    description: 'Hourly activity counts for last 24 hours',
  },
  VW_ACTIVITY_SUMMARY: {
    schema: 'ACTIVITY_CCODE',
    type: 'view' as const,
    columns: ['TOTAL_EVENTS', 'UNIQUE_CUSTOMERS', 'UNIQUE_ACTIVITIES', 'LAST_EVENT'],
    description: 'Summary metrics for last 24 hours',
  },
  VW_LLM_TELEMETRY: {
    schema: 'ACTIVITY_CCODE',
    type: 'view' as const,
    columns: ['TS', 'CUSTOMER', 'MODEL', 'PROMPT_TOKENS', 'COMPLETION_TOKENS', 'LATENCY_MS'],
    description: 'LLM usage telemetry',
  },
  VW_SQL_EXECUTIONS: {
    schema: 'ACTIVITY_CCODE',
    type: 'view' as const,
    columns: ['TS', 'CUSTOMER', 'QUERY_ID', 'QUERY_TAG', 'BYTES_SCANNED', 'DURATION_MS', 'SUCCESS'],
    description: 'SQL execution history',
  },
  VW_DASHBOARD_OPERATIONS: {
    schema: 'ACTIVITY_CCODE',
    type: 'view' as const,
    columns: ['TS', 'CUSTOMER', 'DASHBOARD_ID', 'ACTION', 'SUCCESS'],
    description: 'Dashboard creation and access logs',
  },
  ARTIFACTS: {
    schema: 'ACTIVITY_CCODE',
    type: 'table' as const,
    columns: ['ARTIFACT_ID', 'SAMPLE', 'ROW_COUNT', 'SCHEMA_JSON', 'S3_URL', 'BYTES', 'CREATED_TS', 'CUSTOMER', 'CREATED_BY_ACTIVITY'],
  },
  AUDIT_RESULTS: {
    schema: 'ACTIVITY_CCODE',
    type: 'table' as const,
    columns: ['AUDIT_ID', 'ACTIVITY_ID', 'PASSED', 'FINDINGS', 'REMEDIATION', 'CREATED_TS'],
  },
  SCHEMA_VERSION: {
    schema: 'ANALYTICS',
    type: 'table' as const,
    columns: ['VERSION', 'APPLIED_TS', 'APPLIED_BY'],
  },
} as const;

export const ALLOWED_AGGS = ['COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'COUNT_DISTINCT'] as const;

export const ALLOWED_GRAINS = ['MINUTE', 'HOUR', 'DAY', 'WEEK', 'MONTH', 'QUARTER', 'YEAR'] as const;

export const ALLOWED_OPERATORS = ['=', '!=', '>', '>=', '<', '<=', 'IN', 'NOT IN', 'LIKE', 'BETWEEN'] as const;

export const SECURITY = {
  maxRowsPerQuery: 10000,
  maxBytesScanned: '5GB',
  queryTimeoutSeconds: 300,
  allowedRoles: ['CLAUDE_BI_ROLE', 'CLAUDE_BI_READONLY'],
} as const;

export function fqn(source: keyof typeof SOURCES): string {
  const sourceInfo = SOURCES[source];
  return `${DB}.${sourceInfo.schema}.${source}`;
}

export const CONTRACT_HASH = '75ca4013916b1fa4' as const;

// Type definitions
export type SourceName = keyof typeof SOURCES;
export type SchemaName = keyof typeof SCHEMAS;
export type AggregationFunction = typeof ALLOWED_AGGS[number];
export type TimeGrain = typeof ALLOWED_GRAINS[number];
export type Operator = typeof ALLOWED_OPERATORS[number];