# SafeSQL Documentation

## Overview

SafeSQL is a template-based SQL execution system that ensures only safe, validated SQL patterns are executed against Snowflake. In v1, **only SafeSQL templates are allowed** - no raw SQL execution.

## Core Principles

1. **Template-Only Execution**: All SQL must use predefined templates
2. **Parameter Validation**: Required parameters are enforced
3. **Pattern Blocking**: Dangerous SQL patterns are rejected
4. **Qualified Tables**: Schema.table naming required
5. **Result Limits**: Automatic row limits enforced

## Available Templates (v1)

### 1. describe_table
```javascript
{
  template: 'describe_table',
  params: {
    schema: 'analytics',
    table: 'events'
  }
}
```
Returns column metadata for a table.

### 2. sample_top
```javascript
{
  template: 'sample_top',
  params: {
    schema: 'analytics',
    table: 'events',
    n: 100  // Max 1000
  }
}
```
**ONLY template allowing SELECT ***. Returns top N rows.

### 3. top_n
```javascript
{
  template: 'top_n',
  params: {
    schema: 'analytics',
    table: 'events',
    dimension: 'customer',
    metric: 'COUNT(*)',
    date_column: 'ts',
    start_date: '2024-01-01',
    end_date: '2024-01-31',
    n: 10  // Max 100
  }
}
```
Returns top N values by metric.

### 4. time_series
```javascript
{
  template: 'time_series',
  params: {
    grain: 'day',  // hour|day|week|month|quarter|year
    schema: 'analytics',
    table: 'events',
    date_column: 'ts',
    metric: 'COUNT(*)',
    start_date: '2024-01-01',
    end_date: '2024-01-31'
  }
}
```
Aggregates metrics over time periods.

### 5. breakdown
```javascript
{
  template: 'breakdown',
  params: {
    schema: 'analytics',
    table: 'events',
    dimensions: 'activity, customer',
    metric: 'COUNT(*)',
    date_column: 'ts',
    start_date: '2024-01-01',
    end_date: '2024-01-31',
    limit: 100
  }
}
```
Groups by multiple dimensions.

### 6. comparison
```javascript
{
  template: 'comparison',
  params: {
    schema: 'analytics',
    table: 'events',
    metric: 'COUNT(*)',
    date_column: 'ts',
    start_date_a: '2024-01-01',
    end_date_a: '2024-01-15',
    start_date_b: '2024-01-16',
    end_date_b: '2024-01-31'
  }
}
```
Compares metrics between two periods.

## Banned Patterns

The following SQL patterns are **strictly prohibited**:

- `DROP` statements
- `TRUNCATE` operations
- `DELETE FROM` (except Activity Schema inserts)
- `UPDATE` statements
- `ALTER TABLE/SCHEMA/DATABASE`
- `CREATE` (except TEMPORARY objects)
- `GRANT/REVOKE` permissions
- `EXECUTE/CALL` procedures
- SQL comments (`--`, `/* */`)
- System procedures (`sp_`, `xp_`)
- `SHUTDOWN` commands

## Validation Rules

### 1. Template Validation
- Template name must be in allowed list
- All required parameters must be present
- Parameter types must match expected types
- Custom validation rules per template

### 2. SQL Validation
- No banned patterns detected
- Tables properly qualified (schema.table)
- SELECT * only in sample_top template
- LIMIT required for SELECT * (max 1000)

### 3. Parameter Sanitization
- Single quotes escaped in strings
- NULL values handled properly
- Numbers validated as numeric
- Booleans converted to TRUE/FALSE
- Objects converted to JSON strings

## Usage Example

```javascript
import { SafeSQLValidator } from 'safesql';
import { SAFESQL_TEMPLATES } from 'templates';

const validator = new SafeSQLValidator();

// Validate template and parameters
const validation = validator.validateTemplate(
  SAFESQL_TEMPLATES.top_n,
  {
    schema: 'analytics',
    table: 'events',
    dimension: 'customer',
    metric: 'COUNT(*)',
    date_column: 'ts',
    start_date: '2024-01-01',
    end_date: '2024-01-31',
    n: 10
  }
);

if (!validation.valid) {
  console.error('Validation failed:', validation.errors);
  return;
}

// Build safe query
const sql = validator.buildSafeQuery(template, params);

// Execute through Snowflake Agent
const result = await snowflakeAgent.executeTemplate('top_n', params);
```

## Error Handling

### Validation Errors
```javascript
{
  valid: false,
  errors: [
    'Missing required parameter: date_column',
    'Parameter n exceeds maximum: 100'
  ],
  warnings: [
    'Consider using parameterized queries for values'
  ]
}
```

### Execution Errors
- Template not found
- Parameter validation failed
- SQL pattern banned
- Result size exceeded

## Security Considerations

1. **No Direct SQL**: Users cannot execute arbitrary SQL
2. **Input Sanitization**: All inputs escaped/validated
3. **Result Limits**: Automatic caps on result sizes
4. **Audit Trail**: All executions logged to Activity Schema
5. **Row-Level Security**: Customer isolation enforced

## Future Enhancements (v2)

1. **Conditional Templates**: Dynamic WHERE clauses
2. **Join Templates**: Safe multi-table queries
3. **Aggregation Helpers**: Complex calculations
4. **Custom Templates**: User-defined safe patterns
5. **Raw SQL Mode**: With enhanced validation

## Best Practices

1. Always use the most specific template
2. Provide date ranges to limit data scanned
3. Use breakdown instead of multiple top_n queries
4. Cache results using artifacts for repeated access
5. Monitor query tags for performance tracking