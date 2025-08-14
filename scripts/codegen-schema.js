#!/usr/bin/env node

/**
 * Schema Code Generator
 * Reads activity_views.contract.json and generates a typed schema API
 * This ensures compile-time safety for all database references
 */

const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

// Read the contract
const contractPath = path.join(__dirname, '../contracts/activity_views.contract.json');
const contract = JSON.parse(fs.readFileSync(contractPath, 'utf8'));

// Generate the schema module
function generateSchemaModule(contract) {
  const lines = [];
  
  // Header
  lines.push('/**');
  lines.push(' * GENERATED FILE - DO NOT EDIT');
  lines.push(` * Generated from: contracts/activity_views.contract.json`);
  lines.push(` * Generated at: ${new Date().toISOString()}`);
  lines.push(` * Contract version: ${contract.version}`);
  lines.push(' */');
  lines.push('');
  
  // Database and schemas
  lines.push(`const DB = process.env.SNOWFLAKE_DATABASE || '${contract.db}';`);
  lines.push('');
  lines.push('const SCHEMAS = {');
  for (const [key, value] of Object.entries(contract.schemas)) {
    lines.push(`  ${key}: '${value}',`);
  }
  lines.push('};');
  lines.push('');
  
  // Tables
  lines.push('const TABLES = {');
  for (const [schema, tables] of Object.entries(contract.tables || {})) {
    lines.push(`  ${schema}: {`);
    for (const [table, config] of Object.entries(tables)) {
      lines.push(`    ${table}: '${table}',`);
    }
    lines.push('  },');
  }
  lines.push('};');
  lines.push('');
  
  // Table columns
  lines.push('const TABLE_COLUMNS = {');
  for (const [schema, tables] of Object.entries(contract.tables || {})) {
    for (const [table, config] of Object.entries(tables)) {
      const key = `${schema}_${table}`;
      lines.push(`  ${key}: [`);
      for (const col of config.columns) {
        lines.push(`    '${col}',`);
      }
      lines.push('  ],');
    }
  }
  lines.push('};');
  lines.push('');
  
  // Views
  lines.push('const VIEWS = {');
  for (const [schema, views] of Object.entries(contract.views || {})) {
    lines.push(`  ${schema}: {`);
    for (const viewName of Object.keys(views)) {
      lines.push(`    ${viewName}: '${viewName}',`);
    }
    lines.push('  },');
  }
  lines.push('};');
  lines.push('');
  
  // View columns
  lines.push('const VIEW_COLUMNS = {');
  for (const [schema, views] of Object.entries(contract.views || {})) {
    for (const [view, config] of Object.entries(views)) {
      lines.push(`  ${view}: [`);
      for (const col of config.columns) {
        lines.push(`    '${col}',`);
      }
      lines.push('  ],');
    }
  }
  lines.push('};');
  lines.push('');
  
  // Helper functions
  lines.push('/**');
  lines.push(' * Get fully qualified name for a database object');
  lines.push(' * @param {string} schema - Schema name');
  lines.push(' * @param {string} object - Table or view name');
  lines.push(' * @returns {string} Fully qualified name');
  lines.push(' */');
  lines.push('function fqn(schema, object) {');
  lines.push('  if (!SCHEMAS[schema]) {');
  lines.push(`    throw new Error(\`Unknown schema: \${schema}\`);`);
  lines.push('  }');
  lines.push(`  return \`\${DB}.\${SCHEMAS[schema]}.\${object}\`;`);
  lines.push('}');
  lines.push('');
  
  lines.push('/**');
  lines.push(' * Get two-part name for a database object (after USE DATABASE)');
  lines.push(' * @param {string} schema - Schema name');
  lines.push(' * @param {string} object - Table or view name');
  lines.push(' * @returns {string} Two-part name');
  lines.push(' */');
  lines.push('function twoPartName(schema, object) {');
  lines.push('  if (!SCHEMAS[schema]) {');
  lines.push(`    throw new Error(\`Unknown schema: \${schema}\`);`);
  lines.push('  }');
  lines.push(`  return \`\${SCHEMAS[schema]}.\${object}\`;`);
  lines.push('}');
  lines.push('');
  
  lines.push('/**');
  lines.push(' * Validate that columns exist for a view');
  lines.push(' * @param {string} viewName - View name');
  lines.push(' * @param {string[]} columns - Column names to validate');
  lines.push(' * @throws {Error} If view not found or columns invalid');
  lines.push(' */');
  lines.push('function assertColumnsExist(viewName, columns) {');
  lines.push('  const allowedColumns = VIEW_COLUMNS[viewName];');
  lines.push('  if (!allowedColumns) {');
  lines.push(`    throw new Error(\`Unknown view: \${viewName}\`);`);
  lines.push('  }');
  lines.push('  ');
  lines.push('  for (const col of columns) {');
  lines.push('    const upperCol = col.toUpperCase();');
  lines.push('    if (!allowedColumns.includes(upperCol)) {');
  lines.push(`      throw new Error(\`Column \${col} not found in \${viewName}. Allowed: \${allowedColumns.join(', ')}\`);`);
  lines.push('    }');
  lines.push('  }');
  lines.push('}');
  lines.push('');
  
  lines.push('/**');
  lines.push(' * Normalize column name to uppercase');
  lines.push(' * @param {string} column - Column name');
  lines.push(' * @returns {string} Uppercase column name');
  lines.push(' */');
  lines.push('function normalizeColumn(column) {');
  lines.push('  return column.toUpperCase();');
  lines.push('}');
  lines.push('');
  
  // Calculate contract hash
  const contractHash = crypto
    .createHash('sha256')
    .update(JSON.stringify(contract))
    .digest('hex')
    .substring(0, 16);
  
  lines.push('/**');
  lines.push(' * Contract hash for validation');
  lines.push(' */');
  lines.push(`const CONTRACT_HASH = '${contractHash}';`);
  lines.push('');
  
  // View map for quick lookups
  lines.push('/**');
  lines.push(' * Map of view names to fully qualified names');
  lines.push(' */');
  lines.push('const VIEW_FQN_MAP = {');
  for (const [schema, views] of Object.entries(contract.views || {})) {
    for (const viewName of Object.keys(views)) {
      lines.push(`  ${viewName}: fqn('${schema}', '${viewName}'),`);
    }
  }
  lines.push('};');
  lines.push('');
  
  // Exports
  lines.push('module.exports = {');
  lines.push('  // Core objects');
  lines.push('  DB,');
  lines.push('  SCHEMAS,');
  lines.push('  TABLES,');
  lines.push('  VIEWS,');
  lines.push('  TABLE_COLUMNS,');
  lines.push('  VIEW_COLUMNS,');
  lines.push('  CONTRACT_HASH,');
  lines.push('  VIEW_FQN_MAP,');
  lines.push('  ');
  lines.push('  // Helper functions');
  lines.push('  fqn,');
  lines.push('  twoPartName,');
  lines.push('  assertColumnsExist,');
  lines.push('  normalizeColumn,');
  lines.push('  ');
  lines.push('  // Static exports for known views');
  for (const [schema, views] of Object.entries(contract.views || {})) {
    for (const viewName of Object.keys(views)) {
      lines.push(`  ${viewName}: '${viewName}',`);
    }
  }
  lines.push('};');
  
  return lines.join('\n');
}

// Generate and write the module
const outputPath = path.join(__dirname, '../src/generated-schema.js');
const moduleContent = generateSchemaModule(contract);

fs.writeFileSync(outputPath, moduleContent);
console.log(`✅ Generated schema module: ${outputPath}`);
console.log(`   Contract version: ${contract.version}`);
console.log(`   Tables: ${Object.values(contract.tables || {}).reduce((sum, t) => sum + Object.keys(t).length, 0)}`);
console.log(`   Views: ${Object.values(contract.views || {}).reduce((sum, v) => sum + Object.keys(v).length, 0)}`);