export class SecurityValidator {
  private contract: any;
  
  constructor(contract: any) {
    this.contract = contract;
  }
  
  validateSQL(sql: string): { valid: boolean; errors?: string[] } {
    const errors: string[] = [];
    const upperSQL = sql.toUpperCase();
    
    // Check for forbidden operations
    for (const forbidden of this.contract.security.forbidden_operations) {
      if (upperSQL.includes(forbidden + ' ')) {
        errors.push(`Forbidden operation: ${forbidden}`);
      }
    }
    
    // Ensure only SELECT is used
    if (!upperSQL.trim().startsWith('SELECT')) {
      errors.push('Only SELECT statements are allowed');
    }
    
    // Check for SQL injection patterns
    const injectionPatterns = [
      /;\s*DROP/i,
      /;\s*DELETE/i,
      /;\s*INSERT/i,
      /;\s*UPDATE/i,
      /--\s*$/m,
      /\/\*.*\*\//,
      /EXEC\s*\(/i,
      /EXECUTE\s*\(/i
    ];
    
    for (const pattern of injectionPatterns) {
      if (pattern.test(sql)) {
        errors.push('Potential SQL injection detected');
        break;
      }
    }
    
    // Validate database references
    const dbPattern = new RegExp(`${this.contract.database}\\.`, 'gi');
    const matches = sql.match(dbPattern);
    if (matches) {
      // Check all database references are to allowed database
      const otherDbPattern = /\b[A-Z_]+\.[A-Z_]+\.[A-Z_]+/gi;
      const allRefs = sql.match(otherDbPattern) || [];
      for (const ref of allRefs) {
        if (!ref.startsWith(this.contract.database)) {
          errors.push(`Invalid database reference: ${ref}`);
        }
      }
    }
    
    // Check for LIMIT clause
    if (!upperSQL.includes('LIMIT')) {
      errors.push('LIMIT clause is required for all queries');
    } else {
      // Extract limit value
      const limitMatch = upperSQL.match(/LIMIT\s+(\d+)/);
      if (limitMatch) {
        const limit = parseInt(limitMatch[1]);
        if (limit > this.contract.security.max_rows_per_query) {
          errors.push(`Row limit ${limit} exceeds maximum ${this.contract.security.max_rows_per_query}`);
        }
      }
    }
    
    // Check for system tables/views access
    const systemPatterns = [
      /INFORMATION_SCHEMA\./i,
      /SNOWFLAKE\./i,
      /ACCOUNT_USAGE\./i
    ];
    
    for (const pattern of systemPatterns) {
      if (pattern.test(sql)) {
        errors.push('Access to system tables/views is not allowed');
      }
    }
    
    return { 
      valid: errors.length === 0, 
      errors: errors.length > 0 ? errors : undefined 
    };
  }
  
  validateRole(role: string): boolean {
    return this.contract.security.allowed_roles.includes(role);
  }
  
  getQueryTimeout(): number {
    return this.contract.security.query_timeout_seconds * 1000;
  }
  
  getMaxBytesScanned(): string {
    return this.contract.security.max_bytes_scanned;
  }
}