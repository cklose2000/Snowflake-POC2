import { z } from 'zod';
import { SnowflakeClient } from '../clients/snowflake-client.js';
import { loadSchemaContract } from '../utils/schema-loader.js';
import { StreamlitGenerator } from '../renderers/streamlit-generator.js';

const CreateDashboardInputSchema = z.object({
  title: z.string().describe('Dashboard title'),
  description: z.string().optional().describe('Dashboard description'),
  queries: z.array(z.object({
    name: z.string(),
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
      top_n: z.number().optional()
    }),
    chart_type: z.enum(['line', 'bar', 'pie', 'table', 'metric']).optional()
  })).describe('Query plans for dashboard panels'),
  schedule: z.object({
    enabled: z.boolean(),
    time: z.string().optional().describe('Time in format HH:MM'),
    timezone: z.string().optional().describe('Timezone (e.g., America/New_York)'),
    frequency: z.enum(['daily', 'weekly', 'monthly']).optional()
  }).optional().describe('Refresh schedule configuration'),
  refresh_method: z.enum(['task', 'dynamic', 'manual']).optional().default('manual')
});

export const createDashboardTool = {
  description: 'Create a Snowflake Streamlit dashboard from query plans',
  inputSchema: CreateDashboardInputSchema.strict(),
  
  async execute(args: z.infer<typeof CreateDashboardInputSchema>, client: SnowflakeClient) {
    try {
      await client.connect();
      const contract = await loadSchemaContract();
      
      // Generate unique dashboard ID
      const dashboardId = `dashboard_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
      
      // Create views for each query
      const createdViews: string[] = [];
      const viewErrors: string[] = [];
      
      for (const query of args.queries) {
        const viewName = `${dashboardId}_${query.name.toLowerCase().replace(/\s+/g, '_')}`;
        const fqViewName = `${contract.database}.ANALYTICS.${viewName}`;
        
        try {
          // Generate SQL from query plan
          const { SqlRenderer } = await import('../renderers/sql-renderer.js');
          const renderer = new SqlRenderer(contract);
          const sql = renderer.renderQueryPlan(query.plan);
          
          // Create view
          const createViewSQL = `CREATE OR REPLACE VIEW ${fqViewName} AS ${sql}`;
          await client.executeQuery(createViewSQL);
          createdViews.push(viewName);
          
        } catch (error) {
          viewErrors.push(`Failed to create view for ${query.name}: ${error instanceof Error ? error.message : 'Unknown error'}`);
        }
      }
      
      if (viewErrors.length > 0 && createdViews.length === 0) {
        return {
          success: false,
          errors: viewErrors
        };
      }
      
      // Generate Streamlit code
      const generator = new StreamlitGenerator(contract);
      const streamlitCode = generator.generateDashboard({
        dashboardId,
        title: args.title,
        description: args.description,
        queries: args.queries.map((q, i) => ({
          ...q,
          viewName: createdViews[i]
        })),
        database: contract.database
      });
      
      // Create Streamlit app in Snowflake
      const appName = `${dashboardId}_app`;
      const createAppSQL = `
        CREATE OR REPLACE STREAMLIT ${contract.database}.ANALYTICS.${appName}
        ROOT_LOCATION = '@${contract.database}.ANALYTICS.streamlit_stage/${dashboardId}'
        MAIN_FILE = 'app.py'
        QUERY_WAREHOUSE = '${process.env.SNOWFLAKE_WAREHOUSE || 'CLAUDE_WAREHOUSE'}'
      `;
      
      // Note: In production, you'd upload the streamlit code to stage first
      // For now, we'll return the code and URL
      
      // Create schedule if requested
      let scheduleDetails: string | undefined;
      if (args.schedule?.enabled) {
        const taskName = `${dashboardId}_refresh`;
        const schedule = args.schedule;
        
        let cronExpression = '0 8 * * *'; // Default: daily at 8am
        if (schedule.time) {
          const [hour, minute] = schedule.time.split(':');
          cronExpression = `${minute || '0'} ${hour || '8'} * * *`;
        }
        
        if (schedule.frequency === 'weekly') {
          cronExpression = `${cronExpression.split(' ').slice(0, 2).join(' ')} * * 1`; // Mondays
        } else if (schedule.frequency === 'monthly') {
          cronExpression = `${cronExpression.split(' ').slice(0, 2).join(' ')} 1 * *`; // First of month
        }
        
        const createTaskSQL = `
          CREATE OR REPLACE TASK ${contract.database}.ANALYTICS.${taskName}
          WAREHOUSE = '${process.env.SNOWFLAKE_WAREHOUSE || 'CLAUDE_WAREHOUSE'}'
          SCHEDULE = 'USING CRON ${cronExpression} ${schedule.timezone || 'UTC'}'
          AS
          -- Refresh dashboard views
          ${createdViews.map(v => `ALTER VIEW ${contract.database}.ANALYTICS.${v} REFRESH;`).join('\n')}
        `;
        
        try {
          await client.executeQuery(createTaskSQL);
          await client.executeQuery(`ALTER TASK ${contract.database}.ANALYTICS.${taskName} RESUME`);
          scheduleDetails = `Task ${taskName} created with schedule: ${cronExpression} ${schedule.timezone || 'UTC'}`;
        } catch (error) {
          viewErrors.push(`Failed to create schedule: ${error instanceof Error ? error.message : 'Unknown error'}`);
        }
      }
      
      // Log dashboard creation
      await client.logActivity('ccode.dashboard_created', {
        dashboard_id: dashboardId,
        title: args.title,
        panels: args.queries.length,
        views_created: createdViews.length,
        scheduled: args.schedule?.enabled || false
      });
      
      // Generate dashboard URL
      const dashboardUrl = `https://${process.env.SNOWFLAKE_ACCOUNT}.snowflakecomputing.com/streamlit-apps/${appName}`;
      
      return {
        success: true,
        dashboard_id: dashboardId,
        dashboard_url: dashboardUrl,
        artifacts_created: {
          views: createdViews,
          app: appName,
          task: scheduleDetails ? `${dashboardId}_refresh` : undefined
        },
        schedule_details: scheduleDetails,
        streamlit_code: streamlitCode,
        warnings: viewErrors.length > 0 ? viewErrors : undefined,
        instructions: [
          `1. Upload the Streamlit code to stage: @${contract.database}.ANALYTICS.streamlit_stage/${dashboardId}/app.py`,
          `2. Execute: ${createAppSQL}`,
          `3. Access dashboard at: ${dashboardUrl}`
        ]
      };
      
    } catch (error) {
      console.error('Dashboard creation error:', error);
      
      await client.logActivity('ccode.error_occurred', {
        tool: 'create_dashboard',
        error: error instanceof Error ? error.message : 'Unknown error'
      });
      
      return {
        success: false,
        error: error instanceof Error ? error.message : 'Dashboard creation failed'
      };
    }
  }
};