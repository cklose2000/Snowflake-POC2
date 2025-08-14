import { z } from 'zod';
import { SnowflakeClient } from '../clients/snowflake-client.js';
import { SqlRenderer } from '../renderers/sql-renderer.js';
import { SecurityValidator } from '../validators/security-validator.js';
import { loadSchemaContract } from '../utils/schema-loader.js';

// Input schema for the tool
const ComposeQueryPlanInputSchema = z.object({
  intent_text: z.string().describe('Natural language description of the query intent'),
  source: z.string().optional().describe('Specific view or table to query'),
  dimensions: z.array(z.string()).optional().describe('Columns to group by'),
  measures: z.array(z.object({
    fn: z.enum(['COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'COUNT_DISTINCT']),
    column: z.string()
  })).optional().describe('Aggregation functions to apply'),
  filters: z.array(z.object({
    column: z.string(),
    operator: z.enum(['=', '!=', '>', '>=', '<', '<=', 'IN', 'NOT IN', 'LIKE', 'BETWEEN']),
    value: z.any()
  })).optional().describe('Filter conditions'),
  grain: z.enum(['MINUTE', 'HOUR', 'DAY', 'WEEK', 'MONTH', 'QUARTER', 'YEAR']).optional(),
  top_n: z.number().min(1).max(10000).optional().describe('Limit results to top N rows'),
  order_by: z.array(z.object({
    column: z.string(),
    direction: z.enum(['ASC', 'DESC'])
  })).optional()
});

// Query plan interface
interface QueryPlan {
  source: string;
  dimensions?: string[];
  measures?: Array<{ fn: string; column: string }>;
  filters?: Array<{ column: string; operator: string; value?: any }>;
  grain?: string;
  top_n?: number;
  order_by?: Array<{ column: string; direction: string }>;
  validated: boolean;
  sql?: string;
}

// Tool definition
export const composeQueryPlanTool = {
  description: 'Compose and execute a validated query plan based on natural language intent',
  inputSchema: ComposeQueryPlanInputSchema.strict(),
  
  async execute(args: z.infer<typeof ComposeQueryPlanInputSchema>, client: SnowflakeClient) {
    try {
      // Load schema contract
      const contract = await loadSchemaContract();
      
      // Parse intent if source not specified
      let source = args.source;
      if (!source && args.intent_text) {
        source = inferSourceFromIntent(args.intent_text, contract);
      }
      
      if (!source) {
        // Return available sources for clarification
        return {
          success: false,
          needs_clarification: true,
          message: 'Please specify a data source',
          available_sources: getAvailableSources(contract)
        };
      }
      
      // Build query plan
      const plan: QueryPlan = {
        source,
        dimensions: args.dimensions,
        measures: args.measures,
        filters: args.filters,
        grain: args.grain,
        top_n: args.top_n,
        order_by: args.order_by,
        validated: false
      };
      
      // Validate plan against schema contract
      const validation = validateQueryPlan(plan, contract);
      if (!validation.valid) {
        return {
          success: false,
          errors: validation.errors,
          plan
        };
      }
      
      plan.validated = true;
      
      // Render SQL from plan
      const renderer = new SqlRenderer(contract);
      const sql = renderer.renderQueryPlan(plan);
      plan.sql = sql;
      
      // Validate SQL security
      const securityValidator = new SecurityValidator(contract);
      const securityCheck = securityValidator.validateSQL(sql);
      if (!securityCheck.valid) {
        return {
          success: false,
          security_errors: securityCheck.errors,
          plan
        };
      }
      
      // Execute query
      const startTime = Date.now();
      const result = await client.executeQuery(sql);
      const executionTime = Date.now() - startTime;
      
      // Log activity
      await client.logActivity('ccode.query_executed', {
        plan,
        execution_time_ms: executionTime,
        rows_returned: result.rows.length,
        bytes_scanned: result.bytesScanned
      });
      
      return {
        success: true,
        plan,
        sql,
        results: result.rows,
        metadata: {
          execution_time_ms: executionTime,
          row_count: result.rows.length,
          bytes_scanned: result.bytesScanned,
          query_id: result.queryId
        }
      };
      
    } catch (error) {
      console.error('Query plan error:', error);
      
      // Log error
      await client.logActivity('ccode.error_occurred', {
        tool: 'compose_query_plan',
        error: error instanceof Error ? error.message : 'Unknown error'
      });
      
      return {
        success: false,
        error: error instanceof Error ? error.message : 'Query planning failed'
      };
    }
  }
};

// Helper functions
function inferSourceFromIntent(intent: string, contract: any): string | undefined {
  const lowerIntent = intent.toLowerCase();
  
  // Check for specific view keywords
  if (lowerIntent.includes('hour') || lowerIntent.includes('24')) {
    return 'VW_ACTIVITY_COUNTS_24H';
  }
  if (lowerIntent.includes('summary') || lowerIntent.includes('total')) {
    return 'VW_ACTIVITY_SUMMARY';
  }
  if (lowerIntent.includes('llm') || lowerIntent.includes('model')) {
    return 'VW_LLM_TELEMETRY';
  }
  if (lowerIntent.includes('sql') || lowerIntent.includes('query')) {
    return 'VW_SQL_EXECUTIONS';
  }
  if (lowerIntent.includes('dashboard')) {
    return 'VW_DASHBOARD_OPERATIONS';
  }
  
  // Default to raw events if unclear
  if (lowerIntent.includes('event') || lowerIntent.includes('activity')) {
    return 'EVENTS';
  }
  
  return undefined;
}

function getAvailableSources(contract: any): string[] {
  const sources: string[] = [];
  
  // Add views
  if (contract.schemas.ACTIVITY_CCODE?.views) {
    sources.push(...Object.keys(contract.schemas.ACTIVITY_CCODE.views));
  }
  
  // Add base tables
  if (contract.schemas.ACTIVITY?.tables) {
    sources.push(...Object.keys(contract.schemas.ACTIVITY.tables));
  }
  
  return sources;
}

function validateQueryPlan(plan: QueryPlan, contract: any): { valid: boolean; errors?: string[] } {
  const errors: string[] = [];
  
  // Validate source exists
  const sourceInfo = findSourceInfo(plan.source, contract);
  if (!sourceInfo) {
    errors.push(`Unknown source: ${plan.source}`);
    return { valid: false, errors };
  }
  
  // Validate columns
  const validColumns = sourceInfo.columns || [];
  
  // Check dimensions
  if (plan.dimensions) {
    for (const dim of plan.dimensions) {
      if (!validColumns.includes(dim.toUpperCase())) {
        errors.push(`Invalid dimension column: ${dim}`);
      }
    }
  }
  
  // Check measures
  if (plan.measures) {
    for (const measure of plan.measures) {
      if (!contract.allowed_aggregations.includes(measure.fn)) {
        errors.push(`Invalid aggregation function: ${measure.fn}`);
      }
      if (!validColumns.includes(measure.column.toUpperCase())) {
        errors.push(`Invalid measure column: ${measure.column}`);
      }
    }
  }
  
  // Check filters
  if (plan.filters) {
    for (const filter of plan.filters) {
      if (!validColumns.includes(filter.column.toUpperCase())) {
        errors.push(`Invalid filter column: ${filter.column}`);
      }
      if (!contract.allowed_operators.includes(filter.operator)) {
        errors.push(`Invalid operator: ${filter.operator}`);
      }
    }
  }
  
  // Check grain
  if (plan.grain && !contract.allowed_grains.includes(plan.grain)) {
    errors.push(`Invalid time grain: ${plan.grain}`);
  }
  
  // Check row limit
  if (plan.top_n && plan.top_n > contract.security.max_rows_per_query) {
    errors.push(`Row limit ${plan.top_n} exceeds maximum ${contract.security.max_rows_per_query}`);
  }
  
  return { valid: errors.length === 0, errors: errors.length > 0 ? errors : undefined };
}

function findSourceInfo(source: string, contract: any): any {
  // Check views
  for (const schema of Object.values(contract.schemas) as any[]) {
    if (schema.views && schema.views[source]) {
      return schema.views[source];
    }
    if (schema.tables && schema.tables[source]) {
      return schema.tables[source];
    }
  }
  return null;
}