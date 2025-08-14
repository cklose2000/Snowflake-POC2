import { z } from 'zod';
import { SnowflakeClient } from '../clients/snowflake-client.js';
import { loadSchemaContract, getSourceColumns } from '../utils/schema-loader.js';

const ListSourcesInputSchema = z.object({
  include_columns: z.boolean().optional().describe('Include column information for each source')
});

export const listSourcesTool = {
  description: 'List all available data sources (views and tables) with their schemas',
  inputSchema: ListSourcesInputSchema.strict(),
  
  async execute(args: z.infer<typeof ListSourcesInputSchema>, client: SnowflakeClient) {
    try {
      const contract = await loadSchemaContract();
      const sources: any[] = [];
      
      // Process each schema
      for (const schemaName of Object.keys(contract.schemas)) {
        const schema = contract.schemas[schemaName];
        
        // Add views
        if (schema.views) {
          for (const viewName of Object.keys(schema.views)) {
            const view = schema.views[viewName];
            sources.push({
              name: viewName,
              type: 'view',
              schema: schemaName,
              fqn: `${contract.database}.${schemaName}.${viewName}`,
              description: view.description || null,
              columns: args.include_columns ? (view.columns || []) : undefined
            });
          }
        }
        
        // Add tables
        if (schema.tables) {
          for (const tableName of Object.keys(schema.tables)) {
            const table = schema.tables[tableName];
            const columns = args.include_columns ? getSourceColumns(tableName, contract) : undefined;
            sources.push({
              name: tableName,
              type: 'table',
              schema: schemaName,
              fqn: `${contract.database}.${schemaName}.${tableName}`,
              columns
            });
          }
        }
      }
      
      return {
        success: true,
        database: contract.database,
        sources,
        metadata: {
          total_sources: sources.length,
          views: sources.filter(s => s.type === 'view').length,
          tables: sources.filter(s => s.type === 'table').length
        }
      };
      
    } catch (error) {
      return {
        success: false,
        error: error instanceof Error ? error.message : 'Failed to list sources'
      };
    }
  }
};