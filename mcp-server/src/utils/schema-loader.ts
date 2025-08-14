import { readFile } from 'fs/promises';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));

let cachedContract: any = null;

export async function loadSchemaContract(): Promise<any> {
  if (cachedContract) {
    return cachedContract;
  }
  
  try {
    // Load contract from contracts directory
    const contractPath = join(__dirname, '../../..', 'contracts', 'database.contract.json');
    const contractData = await readFile(contractPath, 'utf-8');
    cachedContract = JSON.parse(contractData);
    
    // Validate contract structure
    if (!cachedContract.version || !cachedContract.database || !cachedContract.schemas) {
      throw new Error('Invalid contract structure');
    }
    
    return cachedContract;
  } catch (error) {
    throw new Error(`Failed to load schema contract: ${error instanceof Error ? error.message : 'Unknown error'}`);
  }
}

export function getSourceColumns(source: string, contract: any): string[] {
  // Check in all schemas
  for (const schemaName of Object.keys(contract.schemas)) {
    const schema = contract.schemas[schemaName];
    
    // Check views
    if (schema.views && schema.views[source]) {
      return schema.views[source].columns || [];
    }
    
    // Check tables
    if (schema.tables && schema.tables[source]) {
      const table = schema.tables[source];
      const columns = [
        ...(table.required_columns || []),
        ...(table.optional_columns || []),
        ...(table.columns || [])
      ];
      return [...new Set(columns)]; // Remove duplicates
    }
  }
  
  return [];
}

export function getFullyQualifiedName(source: string, contract: any): string {
  // Determine schema based on source location
  for (const schemaName of Object.keys(contract.schemas)) {
    const schema = contract.schemas[schemaName];
    
    if (schema.views && schema.views[source]) {
      return `${contract.database}.${schemaName}.${source}`;
    }
    
    if (schema.tables && schema.tables[source]) {
      return `${contract.database}.${schemaName}.${source}`;
    }
  }
  
  // Default fallback
  return `${contract.database}.ACTIVITY_CCODE.${source}`;
}