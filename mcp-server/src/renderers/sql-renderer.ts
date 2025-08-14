export class SqlRenderer {
  private contract: any;
  
  constructor(contract: any) {
    this.contract = contract;
  }
  
  renderQueryPlan(plan: any): string {
    const parts: string[] = [];
    
    // SELECT clause
    parts.push(this.renderSelectClause(plan));
    
    // FROM clause
    parts.push(this.renderFromClause(plan));
    
    // WHERE clause
    if (plan.filters && plan.filters.length > 0) {
      parts.push(this.renderWhereClause(plan));
    }
    
    // GROUP BY clause
    if (plan.dimensions && plan.dimensions.length > 0) {
      parts.push(this.renderGroupByClause(plan));
    }
    
    // ORDER BY clause
    if (plan.order_by && plan.order_by.length > 0) {
      parts.push(this.renderOrderByClause(plan));
    }
    
    // LIMIT clause
    if (plan.top_n) {
      parts.push(`LIMIT ${plan.top_n}`);
    } else {
      // Always add a default limit for safety
      parts.push(`LIMIT ${this.contract.security.max_rows_per_query}`);
    }
    
    return parts.join('\n');
  }
  
  private renderSelectClause(plan: any): string {
    const columns: string[] = [];
    
    // Add dimensions
    if (plan.dimensions) {
      for (const dim of plan.dimensions) {
        if (plan.grain && this.isTimeColumn(dim)) {
          columns.push(`DATE_TRUNC('${plan.grain}', ${this.escapeColumn(dim)}) AS ${this.escapeColumn(dim)}`);
        } else {
          columns.push(this.escapeColumn(dim));
        }
      }
    }
    
    // Add measures
    if (plan.measures) {
      for (const measure of plan.measures) {
        const aggFn = measure.fn === 'COUNT_DISTINCT' ? 'COUNT(DISTINCT' : measure.fn + '(';
        const alias = `${measure.fn}_${measure.column}`.toUpperCase();
        columns.push(`${aggFn}${this.escapeColumn(measure.column)}) AS ${alias}`);
      }
    }
    
    // Default to * if no columns specified
    if (columns.length === 0) {
      columns.push('*');
    }
    
    return `SELECT ${columns.join(',\n       ')}`;
  }
  
  private renderFromClause(plan: any): string {
    const fqn = this.getFullyQualifiedName(plan.source);
    return `FROM ${fqn}`;
  }
  
  private renderWhereClause(plan: any): string {
    const conditions: string[] = [];
    
    for (const filter of plan.filters) {
      const column = this.escapeColumn(filter.column);
      const value = this.renderValue(filter.value);
      
      switch (filter.operator) {
        case 'IN':
        case 'NOT IN':
          const values = Array.isArray(filter.value) 
            ? filter.value.map((v: any) => this.renderValue(v)).join(', ')
            : value;
          conditions.push(`${column} ${filter.operator} (${values})`);
          break;
        case 'BETWEEN':
          if (Array.isArray(filter.value) && filter.value.length === 2) {
            conditions.push(`${column} BETWEEN ${this.renderValue(filter.value[0])} AND ${this.renderValue(filter.value[1])}`);
          }
          break;
        default:
          conditions.push(`${column} ${filter.operator} ${value}`);
      }
    }
    
    // Add default time window for activity views
    if (plan.source.includes('24H')) {
      conditions.push(`TS >= CURRENT_TIMESTAMP - INTERVAL '24 hours'`);
    }
    
    return conditions.length > 0 ? `WHERE ${conditions.join('\n  AND ')}` : '';
  }
  
  private renderGroupByClause(plan: any): string {
    const columns = plan.dimensions.map((dim: string) => {
      if (plan.grain && this.isTimeColumn(dim)) {
        return `DATE_TRUNC('${plan.grain}', ${this.escapeColumn(dim)})`;
      }
      return this.escapeColumn(dim);
    });
    
    return `GROUP BY ${columns.join(', ')}`;
  }
  
  private renderOrderByClause(plan: any): string {
    const orderColumns = plan.order_by.map((ob: any) => 
      `${this.escapeColumn(ob.column)} ${ob.direction || 'ASC'}`
    );
    
    return `ORDER BY ${orderColumns.join(', ')}`;
  }
  
  private getFullyQualifiedName(source: string): string {
    // Determine schema based on source
    let schema = 'ACTIVITY_CCODE';
    
    if (source === 'EVENTS') {
      schema = 'ACTIVITY';
    } else if (source === 'SCHEMA_VERSION') {
      schema = 'ANALYTICS';
    }
    
    return `${this.contract.database}.${schema}.${source}`;
  }
  
  private escapeColumn(column: string): string {
    // Ensure column is uppercase and properly escaped
    return column.toUpperCase();
  }
  
  private renderValue(value: any): string {
    if (value === null) {
      return 'NULL';
    }
    if (typeof value === 'string') {
      // Escape single quotes
      return `'${value.replace(/'/g, "''")}'`;
    }
    if (typeof value === 'number') {
      return value.toString();
    }
    if (typeof value === 'boolean') {
      return value ? 'TRUE' : 'FALSE';
    }
    if (value instanceof Date) {
      return `'${value.toISOString()}'::TIMESTAMP`;
    }
    return `'${JSON.stringify(value)}'`;
  }
  
  private isTimeColumn(column: string): boolean {
    const timeColumns = ['TS', 'HOUR', 'CREATED_TS', 'LAST_EVENT', 'APPLIED_TS'];
    return timeColumns.includes(column.toUpperCase());
  }
}