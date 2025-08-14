#!/usr/bin/env node
import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import {
  CallToolRequestSchema,
  ListToolsRequestSchema,
  ErrorCode,
  McpError
} from '@modelcontextprotocol/sdk/types.js';
import { composeQueryPlanTool } from './tools/compose-query-plan.js';
import { createDashboardTool } from './tools/create-dashboard.js';
import { listSourcesTool } from './tools/list-sources.js';
import { validatePlanTool } from './tools/validate-plan.js';
import { SnowflakeClient } from './clients/snowflake-client.js';
import * as dotenv from 'dotenv';

// Load environment variables
dotenv.config({ path: '../.env' });

// Initialize server
const server = new Server(
  {
    name: 'snowflake-mcp-server',
    version: '1.0.0',
  },
  {
    capabilities: {
      tools: {},
    },
  }
);

// Initialize Snowflake client
let snowflakeClient: SnowflakeClient;

// Tool registry
const tools = {
  compose_query_plan: composeQueryPlanTool,
  create_dashboard: createDashboardTool,
  list_sources: listSourcesTool,
  validate_plan: validatePlanTool
};

// Handle tool listing
server.setRequestHandler(ListToolsRequestSchema, async () => {
  return {
    tools: Object.entries(tools).map(([name, tool]) => ({
      name,
      description: tool.description,
      inputSchema: tool.inputSchema
    }))
  };
});

// Handle tool execution
server.setRequestHandler(CallToolRequestSchema, async (request) => {
  const { name, arguments: args } = request.params;
  
  const tool = tools[name as keyof typeof tools];
  if (!tool) {
    throw new McpError(
      ErrorCode.MethodNotFound,
      `Tool ${name} not found`
    );
  }

  try {
    // Ensure Snowflake client is initialized
    if (!snowflakeClient) {
      snowflakeClient = new SnowflakeClient();
      await snowflakeClient.connect();
    }

    // Execute tool with Snowflake client
    const result = await tool.execute(args as any, snowflakeClient);
    
    return {
      content: [
        {
          type: 'text',
          text: JSON.stringify(result, null, 2)
        }
      ]
    };
  } catch (error) {
    console.error(`Tool ${name} error:`, error);
    
    if (error instanceof McpError) {
      throw error;
    }
    
    throw new McpError(
      ErrorCode.InternalError,
      `Tool execution failed: ${error instanceof Error ? error.message : 'Unknown error'}`
    );
  }
});

// Start server
async function main() {
  const transport = new StdioServerTransport();
  await server.connect(transport);
  console.error('MCP Server started on stdio');
}

main().catch((error) => {
  console.error('Server error:', error);
  process.exit(1);
});