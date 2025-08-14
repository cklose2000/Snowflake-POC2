#!/usr/bin/env node

const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

// Load contract
const contractPath = path.join(__dirname, '../contracts/database.contract.json');
const contract = JSON.parse(fs.readFileSync(contractPath, 'utf-8'));

// Generate TypeScript constants
function generateTypeScriptConstants() {
  const lines = [];
  
  // Header
  lines.push('/**');
  lines.push(' * GENERATED FILE - DO NOT EDIT');
  lines.push(` * Generated from: contracts/database.contract.json`);
  lines.push(` * Generated at: ${new Date().toISOString()}`);
  lines.push(` * Contract version: ${contract.version}`);
  lines.push(' */');
  lines.push('');
  
  // Database constant
  lines.push(`export const DB = '${contract.database}' as const;`);
  lines.push('');
  
  // Schemas
  lines.push('export const SCHEMAS = {');
  for (const schema of Object.keys(contract.schemas)) {
    lines.push(`  ${schema}: '${schema}',`);
  }
  lines.push('} as const;');
  lines.push('');
  
  // Sources (views and tables)
  lines.push('export const SOURCES = {');
  for (const schemaName of Object.keys(contract.schemas)) {
    const schema = contract.schemas[schemaName];
    
    if (schema.views) {
      for (const viewName of Object.keys(schema.views)) {
        const view = schema.views[viewName];
        lines.push(`  ${viewName}: {`);
        lines.push(`    schema: '${schemaName}',`);
        lines.push(`    type: 'view' as const,`);
        lines.push(`    columns: [${view.columns.map(c => `'${c}'`).join(', ')}],`);
        if (view.description) {
          lines.push(`    description: '${view.description}',`);
        }
        lines.push(`  },`);
      }
    }
    
    if (schema.tables) {
      for (const tableName of Object.keys(schema.tables)) {
        const table = schema.tables[tableName];
        const allColumns = [
          ...(table.required_columns || []),
          ...(table.optional_columns || []),
          ...(table.columns || [])
        ];
        const uniqueColumns = [...new Set(allColumns)];
        
        lines.push(`  ${tableName}: {`);
        lines.push(`    schema: '${schemaName}',`);
        lines.push(`    type: 'table' as const,`);
        lines.push(`    columns: [${uniqueColumns.map(c => `'${c}'`).join(', ')}],`);
        lines.push(`  },`);
      }
    }
  }
  lines.push('} as const;');
  lines.push('');
  
  // Allowed aggregations
  lines.push(`export const ALLOWED_AGGS = [${contract.allowed_aggregations.map(a => `'${a}'`).join(', ')}] as const;`);
  lines.push('');
  
  // Allowed grains
  lines.push(`export const ALLOWED_GRAINS = [${contract.allowed_grains.map(g => `'${g}'`).join(', ')}] as const;`);
  lines.push('');
  
  // Allowed operators
  lines.push(`export const ALLOWED_OPERATORS = [${contract.allowed_operators.map(o => `'${o}'`).join(', ')}] as const;`);
  lines.push('');
  
  // Security constants
  lines.push('export const SECURITY = {');
  lines.push(`  maxRowsPerQuery: ${contract.security.max_rows_per_query},`);
  lines.push(`  maxBytesScanned: '${contract.security.max_bytes_scanned}',`);
  lines.push(`  queryTimeoutSeconds: ${contract.security.query_timeout_seconds},`);
  lines.push(`  allowedRoles: [${contract.security.allowed_roles.map(r => `'${r}'`).join(', ')}],`);
  lines.push('} as const;');
  lines.push('');
  
  // Helper function
  lines.push('export function fqn(source: keyof typeof SOURCES): string {');
  lines.push('  const sourceInfo = SOURCES[source];');
  lines.push("  return `${DB}.${sourceInfo.schema}.${source}`;");
  lines.push('}');
  lines.push('');
  
  // Contract hash
  const hash = crypto.createHash('md5').update(JSON.stringify(contract)).digest('hex').substring(0, 16);
  lines.push(`export const CONTRACT_HASH = '${hash}' as const;`);
  lines.push('');
  
  // Type definitions
  lines.push('// Type definitions');
  lines.push('export type SourceName = keyof typeof SOURCES;');
  lines.push('export type SchemaName = keyof typeof SCHEMAS;');
  lines.push('export type AggregationFunction = typeof ALLOWED_AGGS[number];');
  lines.push('export type TimeGrain = typeof ALLOWED_GRAINS[number];');
  lines.push('export type Operator = typeof ALLOWED_OPERATORS[number];');
  
  return lines.join('\n');
}

// Write TypeScript file
const outputPath = path.join(__dirname, '../mcp-server/src/generated/schema-constants.ts');
const outputDir = path.dirname(outputPath);

if (!fs.existsSync(outputDir)) {
  fs.mkdirSync(outputDir, { recursive: true });
}

fs.writeFileSync(outputPath, generateTypeScriptConstants());

console.log(`✅ Generated TypeScript constants: ${outputPath}`);
console.log(`   Contract version: ${contract.version}`);
console.log(`   Schemas: ${Object.keys(contract.schemas).length}`);
console.log(`   Total sources: ${
  Object.values(contract.schemas).reduce((acc, s) => 
    acc + (Object.keys(s.views || {}).length + Object.keys(s.tables || {}).length), 0)
}`);