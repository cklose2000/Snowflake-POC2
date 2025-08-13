// SafeSQL Validator - Ensures only safe SQL patterns are executed
export class SafeSQLValidator {
  constructor() {
    this.allowedTemplates = [
      'describe_table',
      'sample_top',
      'top_n',
      'time_series',
      'breakdown',
      'comparison'
    ];

    this.bannedPatterns = [
      /\bDROP\s+/i,
      /\bTRUNCATE\s+/i,
      /\bDELETE\s+FROM/i,
      /\bINSERT\s+INTO(?!.*analytics\.activity)/i, // Allow only activity inserts
      /\bUPDATE\s+/i,
      /\bALTER\s+TABLE/i,
      /\bCREATE\s+(?!.*TEMPORARY)/i, // Allow only temp tables
      /\bGRANT\s+/i,
      /\bREVOKE\s+/i,
      /\bEXEC(?:UTE)?\s+/i,
      /\bCALL\s+/i,
      /--/g, // SQL comments that could hide malicious code
      /\/\*/g, // Block comments
      /\bxp_/i, // Extended stored procedures
      /\bsp_/i, // System stored procedures
      /\bDBCC\s+/i, // Database console commands
      /\bSHUTDOWN\s+/i,
      /\bUSE\s+(?!DATABASE|SCHEMA)/i // Only allow USE DATABASE/SCHEMA
    ];

    this.selectStarRestriction = {
      pattern: /SELECT\s+\*/i,
      allowedIn: ['sample_top'], // Only template that allows SELECT *
      maxRows: 1000
    };
  }

  validateSQL(sql, template = null) {
    const validation = {
      valid: true,
      errors: [],
      warnings: [],
      sanitized: sql
    };

    // Check if template is allowed
    if (template && !this.allowedTemplates.includes(template)) {
      validation.valid = false;
      validation.errors.push(`Template '${template}' is not allowed`);
      return validation;
    }

    // Check for banned patterns
    for (const pattern of this.bannedPatterns) {
      if (pattern.test(sql)) {
        validation.valid = false;
        validation.errors.push(`Banned SQL pattern detected: ${pattern.source}`);
      }
    }

    // Check SELECT * restriction
    if (this.selectStarRestriction.pattern.test(sql)) {
      if (!template || !this.selectStarRestriction.allowedIn.includes(template)) {
        validation.valid = false;
        validation.errors.push('SELECT * is only allowed in sample_top template');
      } else if (!sql.includes('LIMIT') || this.extractLimit(sql) > this.selectStarRestriction.maxRows) {
        validation.valid = false;
        validation.errors.push(`SELECT * must have LIMIT <= ${this.selectStarRestriction.maxRows}`);
      }
    }

    // Check for proper table qualification
    if (!this.hasQualifiedTables(sql)) {
      validation.warnings.push('Tables should be fully qualified (schema.table)');
    }

    // Check for parameterization
    if (this.hasUnparameterizedValues(sql)) {
      validation.warnings.push('Consider using parameterized queries for values');
    }

    // Sanitize SQL (remove potentially dangerous elements)
    validation.sanitized = this.sanitizeSQL(sql);

    return validation;
  }

  extractLimit(sql) {
    const match = sql.match(/LIMIT\s+(\d+)/i);
    return match ? parseInt(match[1]) : Infinity;
  }

  hasQualifiedTables(sql) {
    // Check if tables are schema-qualified
    const tableReferences = sql.match(/FROM\s+(\w+)(?:\s|,|$)/gi);
    if (!tableReferences) return true;

    return tableReferences.every(ref => {
      const table = ref.replace(/FROM\s+/i, '').trim();
      return table.includes('.') || table.toLowerCase() === 'dual';
    });
  }

  hasUnparameterizedValues(sql) {
    // Check for hardcoded values that should be parameters
    const valuePatterns = [
      /WHERE\s+\w+\s*=\s*'[^']+'/i,
      /WHERE\s+\w+\s*=\s*\d+/i,
      /AND\s+\w+\s*=\s*'[^']+'/i,
      /AND\s+\w+\s*=\s*\d+/i
    ];

    return valuePatterns.some(pattern => pattern.test(sql));
  }

  sanitizeSQL(sql) {
    // Remove comments
    let sanitized = sql.replace(/--.*$/gm, '');
    sanitized = sanitized.replace(/\/\*[\s\S]*?\*\//g, '');
    
    // Trim excessive whitespace
    sanitized = sanitized.replace(/\s+/g, ' ').trim();
    
    return sanitized;
  }

  validateTemplate(template, params) {
    const validation = {
      valid: true,
      errors: [],
      warnings: []
    };

    // Check required parameters
    if (template.required) {
      for (const param of template.required) {
        if (!(param in params)) {
          validation.valid = false;
          validation.errors.push(`Missing required parameter: ${param}`);
        }
      }
    }

    // Check parameter types
    if (template.paramTypes) {
      for (const [param, expectedType] of Object.entries(template.paramTypes)) {
        if (param in params) {
          const actualType = typeof params[param];
          if (actualType !== expectedType) {
            validation.valid = false;
            validation.errors.push(`Parameter ${param} should be ${expectedType}, got ${actualType}`);
          }
        }
      }
    }

    // Run custom validation if provided
    if (template.validation) {
      try {
        template.validation(params);
      } catch (error) {
        validation.valid = false;
        validation.errors.push(error.message);
      }
    }

    return validation;
  }

  buildSafeQuery(template, params) {
    // Build query from template with proper escaping
    let sql = template.sql;
    
    for (const [key, value] of Object.entries(params)) {
      const placeholder = `{{${key}}}`;
      const sanitizedValue = this.escapeValue(value);
      sql = sql.replace(new RegExp(placeholder, 'g'), sanitizedValue);
    }
    
    // Remove conditional blocks that weren't used
    sql = sql.replace(/{{#if\s+\w+}}[\s\S]*?{{\/if}}/g, '');
    
    return sql;
  }

  escapeValue(value) {
    if (value === null || value === undefined) {
      return 'NULL';
    }
    
    if (typeof value === 'number') {
      return value.toString();
    }
    
    if (typeof value === 'boolean') {
      return value ? 'TRUE' : 'FALSE';
    }
    
    // Escape single quotes in strings
    if (typeof value === 'string') {
      return `'${value.replace(/'/g, "''")}'`;
    }
    
    // For objects/arrays, convert to JSON string
    return `'${JSON.stringify(value).replace(/'/g, "''")}'`;
  }
}

export default SafeSQLValidator;