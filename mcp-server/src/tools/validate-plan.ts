import { z } from 'zod';
import { SnowflakeClient } from '../clients/snowflake-client.js';
import { loadSchemaContract, getSourceColumns } from '../utils/schema-loader.js';
import { SqlRenderer } from '../renderers/sql-renderer.js';
import { SecurityValidator } from '../validators/security-validator.js';

const ValidatePlanInputSchema = z.object({
  plan: z.object({
    source: z.string(),
    dimensions: z.array(z.string()).optional(),
    measures: z.array(z.object({
      fn: z.string(),
      column: z.string()
    })).optional(),
    filters: z.array(z.object({
      column: z.string(),
      operator: z.string(),
      value: z.any()
    })).optional(),
    grain: z.string().optional(),
    top_n: z.number().optional(),
    order_by: z.array(z.object({
      column: z.string(),
      direction: z.enum(['ASC', 'DESC'])
    })).optional()
  }).describe('Query plan to validate'),
  dry_run: z.boolean().optional().describe('Perform SQL compilation check without executing')
});

export const validatePlanTool = {
  description: 'Validate a query plan against the schema contract and security rules',
  inputSchema: ValidatePlanInputSchema.strict(),
  
  async execute(args: z.infer<typeof ValidatePlanInputSchema>, client: SnowflakeClient) {
    try {
      const contract = await loadSchemaContract();
      const { plan, dry_run } = args;
      
      const errors: string[] = [];
      const warnings: string[] = [];
      
      // Validate source exists
      const sourceColumns = getSourceColumns(plan.source, contract);
      if (sourceColumns.length === 0) {
        errors.push(`Unknown source: ${plan.source}`);
        return {
          success: false,
          valid: false,
          errors
        };
      }
      
      // Validate dimensions
      if (plan.dimensions) {
        for (const dim of plan.dimensions) {
          if (!sourceColumns.includes(dim.toUpperCase())) {
            errors.push(`Invalid dimension column: ${dim} not in ${plan.source}`);
          }
        }
      }
      
      // Validate measures
      if (plan.measures) {
        for (const measure of plan.measures) {
          if (!contract.allowed_aggregations.includes(measure.fn)) {
            errors.push(`Invalid aggregation function: ${measure.fn}`);
          }
          if (!sourceColumns.includes(measure.column.toUpperCase())) {
            errors.push(`Invalid measure column: ${measure.column} not in ${plan.source}`);
          }
        }
      }
      
      // Validate filters
      if (plan.filters) {
        for (const filter of plan.filters) {
          if (!sourceColumns.includes(filter.column.toUpperCase())) {
            errors.push(`Invalid filter column: ${filter.column} not in ${plan.source}`);
          }
          if (!contract.allowed_operators.includes(filter.operator)) {
            errors.push(`Invalid operator: ${filter.operator}`);
          }
        }
      }
      
      // Validate grain
      if (plan.grain && !contract.allowed_grains.includes(plan.grain)) {
        errors.push(`Invalid time grain: ${plan.grain}`);
      }
      
      // Validate row limit
      if (plan.top_n) {
        if (plan.top_n > contract.security.max_rows_per_query) {
          errors.push(`Row limit ${plan.top_n} exceeds maximum ${contract.security.max_rows_per_query}`);
        }
        if (plan.top_n < 1) {
          errors.push(`Invalid row limit: ${plan.top_n}`);
        }
      }
      
      // Check for common issues
      if (plan.measures && plan.measures.length > 0 && (!plan.dimensions || plan.dimensions.length === 0)) {
        warnings.push('Aggregations without GROUP BY will return single row');
      }
      
      if (!plan.top_n && !plan.dimensions) {
        warnings.push('No limit specified - default limit will be applied');
      }
      
      // If no errors, try to render and validate SQL
      let sql: string | undefined;
      if (errors.length === 0) {
        try {
          const renderer = new SqlRenderer(contract);
          sql = renderer.renderQueryPlan(plan);
          
          // Validate SQL security
          const securityValidator = new SecurityValidator(contract);
          const securityCheck = securityValidator.validateSQL(sql);
          
          if (!securityCheck.valid) {
            errors.push(...(securityCheck.errors || []));
          }
          
          // Dry run if requested
          if (dry_run && errors.length === 0) {
            try {
              await client.connect();
              // This will validate SQL compilation without executing
              await client.executeQuery(`EXPLAIN ${sql}`);
            } catch (error) {
              errors.push(`SQL compilation failed: ${error instanceof Error ? error.message : 'Unknown error'}`);
            }
          }
          
        } catch (error) {
          errors.push(`SQL rendering failed: ${error instanceof Error ? error.message : 'Unknown error'}`);
        }
      }
      
      return {
        success: errors.length === 0,
        valid: errors.length === 0,
        errors: errors.length > 0 ? errors : undefined,
        warnings: warnings.length > 0 ? warnings : undefined,
        sql,
        plan: {
          ...plan,
          validated: errors.length === 0
        }
      };
      
    } catch (error) {
      return {
        success: false,
        valid: false,
        error: error instanceof Error ? error.message : 'Validation failed'
      };
    }
  }
};