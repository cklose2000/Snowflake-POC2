#!/usr/bin/env node

/**
 * Schema Code Generator
 * Reads activity_v2.contract.json and generates type-safe schema exports
 * This is the ONLY place that generates schema object references
 */

const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

// Load contract
const contractPath = path.join(__dirname, '../schemas/activity_v2.contract.json');
const contract = JSON.parse(fs.readFileSync(contractPath, 'utf8'));

// Environment variable substitution
function substituteEnvVars(value) {
  if (typeof value !== 'string' || value === null || value === undefined) {
    return `"${value}"`;
  }
  
  try {
    // Handle template literals like ${VAR:-default}
    const result = value.replace(/\$\{([^}:]+)(?::[-]?([^}]*))?\}/g, (match, envVar, defaultValue) => {
      return `process.env.${envVar}${defaultValue ? ` || '${defaultValue}'` : ''}`;
    });
    
    // If no substitution occurred, wrap in quotes
    if (result === value) {
      return `"${value}"`;
    }
    
    return result;
  } catch (error) {
    console.error('Error in substituteEnvVars with value:', value, 'error:', error.message);
    throw error;
  }
}

// Generate contract hash for change detection
function generateContractHash() {
  const contractString = JSON.stringify(contract, null, 2);
  return crypto.createHash('sha256').update(contractString).digest('hex').substring(0, 16);
}

// Generate TypeScript/JavaScript code
function generateSchemaCode() {
  const contractHash = generateContractHash();
  
  let code = `// AUTO-GENERATED FILE - DO NOT EDIT
// Generated from schemas/activity_v2.contract.json
// Contract Hash: ${contractHash}
// Generated: ${new Date().toISOString()}

/**
 * Type-safe schema definitions generated from Activity Schema v2.0 contract
 * This is the ONLY source for schema object references in the application
 */

// Environment Configuration
export const DB = ${substituteEnvVars(contract.environment.database)};
export const WAREHOUSE = ${substituteEnvVars(contract.environment.warehouse)};
export const ROLE = ${substituteEnvVars(contract.environment.role)};
export const DEFAULT_SCHEMA = ${substituteEnvVars(contract.environment.default_schema)};

// Contract metadata
export const CONTRACT_VERSION = "${contract.version}";
export const CONTRACT_HASH = "${contractHash}";

// Schema definitions (const assertions for type safety)
export const SCHEMAS = {
`;

  // Generate schema constants
  Object.keys(contract.schemas).forEach(schemaName => {
    code += `  ${schemaName}: "${schemaName}",\n`;
  });
  
  code += `};\n\n`;

  // Generate table definitions
  code += `// Table definitions by schema\n`;
  code += `export const TABLES = {\n`;
  
  Object.entries(contract.schemas).forEach(([schemaName, schema]) => {
    if (schema.tables) {
      code += `  ${schemaName}: {\n`;
      Object.keys(schema.tables).forEach(tableName => {
        code += `    ${tableName}: "${tableName}",\n`;
      });
      code += `  },\n`;
    }
  });
  
  code += `};\n\n`;

  // Generate view definitions
  code += `// View definitions by schema\n`;
  code += `export const VIEWS = {\n`;
  
  Object.entries(contract.schemas).forEach(([schemaName, schema]) => {
    if (schema.views) {
      code += `  ${schemaName}: {\n`;
      Object.keys(schema.views).forEach(viewName => {
        code += `    ${viewName}: "${viewName}",\n`;
      });
      code += `  },\n`;
    }
  });
  
  code += `};\n\n`;

  // Generate FQN helper functions
  code += `// Fully Qualified Name helpers\n`;
  code += `export function fqn(schema, object) {\n`;
  code += `  return \`\${DB}.\${SCHEMAS[schema]}.\${object}\`;\n`;
  code += `}\n\n`;

  code += `export function twoPartName(schema, object) {\n`;
  code += `  return \`\${SCHEMAS[schema]}.\${object}\`;\n`;
  code += `}\n\n`;

  // Generate Activity view mapping
  code += `// Activity view mapping for panel sources\n`;
  code += `export const ACTIVITY_VIEW_MAP = {\n`;
  
  if (contract.schemas.ACTIVITY_CCODE?.views) {
    Object.keys(contract.schemas.ACTIVITY_CCODE.views).forEach(viewName => {
      code += `  "${viewName}": fqn("ACTIVITY_CCODE", "${viewName}"),\n`;
    });
  }
  
  code += `};\n\n`;

  // Generate source qualification helper
  code += `// Source qualification helper (replaces qualifySource)\n`;
  code += `export function qualifySource(source) {\n`;
  code += `  // Already qualified?\n`;
  code += `  if (source.includes('.')) return source;\n`;
  code += `  \n`;
  code += `  // Known Activity views map to ACTIVITY_CCODE schema\n`;
  code += `  if (source in ACTIVITY_VIEW_MAP) {\n`;
  code += `    return ACTIVITY_VIEW_MAP[source];\n`;
  code += `  }\n`;
  code += `  \n`;
  code += `  // Default to ANALYTICS schema\n`;
  code += `  return fqn("ANALYTICS", source);\n`;
  code += `}\n\n`;

  // Generate context SQL helper
  code += `// Context SQL generation\n`;
  code += `export function getContextSQL(options = {}) {\n`;
  code += `  const statements = [\n`;
  code += `    WAREHOUSE && \`USE WAREHOUSE \${WAREHOUSE}\`,\n`;
  code += `    \`USE DATABASE \${DB}\`,\n`;
  code += `    \`USE SCHEMA \${DEFAULT_SCHEMA}\`\n`;
  code += `  ].filter(Boolean);\n`;
  code += `  \n`;
  code += `  if (options.queryTag) {\n`;
  code += `    statements.push(\`ALTER SESSION SET QUERY_TAG = '\${options.queryTag}'\`);\n`;
  code += `  }\n`;
  code += `  \n`;
  code += `  return statements;\n`;
  code += `}\n\n`;

  // Generate activity namespace helpers
  code += `// Activity namespace helpers\n`;
  code += `export const ACTIVITY_NAMESPACE = "${contract.activity_namespace.prefix}";\n`;
  code += `export const STANDARD_ACTIVITIES = [\n`;
  contract.activity_namespace.standard_activities.forEach(activity => {
    code += `  "${activity}",\n`;
  });
  code += `];\n\n`;

  code += `export function createActivityName(action) {\n`;
  code += `  return \`\${ACTIVITY_NAMESPACE}.\${action}\`;\n`;
  code += `}\n\n`;

  // Generate schedule helpers
  code += `// Schedule configuration\n`;
  code += `export const SCHEDULE_MODES = ${JSON.stringify(contract.scheduling.modes)};\n`;
  code += `export const DEFAULT_CRON = "${contract.scheduling.exact_mode.default_cron}";\n`;
  code += `export const FALLBACK_BEHAVIOR = "${contract.scheduling.exact_mode.fallback_behavior}";\n\n`;

  // Generate validation helpers
  code += `// Schema validation patterns\n`;
  code += `export const VALIDATION_PATTERNS = {\n`;
  Object.entries(contract.validation_rules).forEach(([ruleName, rule]) => {
    code += `  ${ruleName}: {\n`;
    
    // Handle both pattern and forbidden_patterns
    if (rule.pattern) {
      code += `    pattern: new RegExp("${rule.pattern.replace(/\\/g, '\\\\')}", "g"),\n`;
    }
    if (rule.exceptions) {
      code += `    exceptions: ${JSON.stringify(rule.exceptions)},\n`;
    }
    if (rule.required_prefix) {
      code += `    requiredPrefix: "${rule.required_prefix}",\n`;
    }
    if (rule.forbidden_patterns) {
      code += `    forbiddenPatterns: ${JSON.stringify(rule.forbidden_patterns)}.map(p => new RegExp(p, "g")),\n`;
    }
    code += `    description: "${rule.description}"\n`;
    code += `  },\n`;
  });
  code += `};\n\n`;

  // Generate table reference helpers
  code += `// Table reference helpers with column validation\n`;
  code += `// TableReference shape: { fqn, twoPartName, schema, table, requiredColumns }\n\n`;

  code += `export function getTableRef(schema, table) {\n`;
  code += `  const schemaName = SCHEMAS[schema];\n`;
  code += `  if (!schemaName) {\n`;
  code += `    throw new Error(\`Unknown schema: \${schema}\`);\n`;
  code += `  }\n`;
  code += `  \n`;
  
  // Add table validation logic
  code += `  // Get required columns from contract\n`;
  code += `  const tableDefinition = getTableDefinition(schema, table);\n`;
  code += `  \n`;
  code += `  return {\n`;
  code += `    fqn: fqn(schema, table),\n`;
  code += `    twoPartName: twoPartName(schema, table),\n`;
  code += `    schema: schemaName,\n`;
  code += `    table,\n`;
  code += `    requiredColumns: tableDefinition?.required_columns?.map(col => col.name) || []\n`;
  code += `  };\n`;
  code += `}\n\n`;

  // Helper to get table definition
  code += `function getTableDefinition(schema, table) {\n`;
  code += `  const tableDefinitions = ${JSON.stringify(contract.schemas, null, 2)};\n`;
  code += `  return tableDefinitions[schema]?.tables?.[table];\n`;
  code += `}\n\n`;

  // Export contract for validation
  code += `// Export contract for runtime validation\n`;
  code += `export const CONTRACT = ${JSON.stringify(contract, null, 2)};\n`;

  return code;
}

// Main execution
async function main() {
  console.log('🏗️ Generating schema code from contract...');
  
  try {
    // Generate the code
    const generatedCode = generateSchemaCode();
    
    // Ensure output directory exists
    const outputDir = path.join(__dirname, '../packages/snowflake-schema');
    if (!fs.existsSync(outputDir)) {
      fs.mkdirSync(outputDir, { recursive: true });
    }
    
    // Write generated file
    const outputPath = path.join(outputDir, 'generated.js');
    fs.writeFileSync(outputPath, generatedCode);
    
    console.log(`✅ Generated schema code: ${outputPath}`);
    console.log(`📋 Contract hash: ${generateContractHash()}`);
    
    // Validate that current config.js is compatible
    const configPath = path.join(outputDir, 'config.js');
    if (fs.existsSync(configPath)) {
      console.log('⚠️  Existing config.js detected - will need migration to generated.js');
    }
    
  } catch (error) {
    console.error('❌ Code generation failed:', error.message);
    console.error('Stack trace:', error.stack);
    process.exit(1);
  }
}

// Run if called directly
if (require.main === module) {
  main();
}

module.exports = {
  generateSchemaCode,
  generateContractHash,
  contract
};