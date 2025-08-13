// SafeSQL Templates - Only allowed SQL patterns in v1
export const SAFESQL_TEMPLATES = {
  describe_table: {
    sql: `
      SELECT column_name, data_type, is_nullable, column_default
      FROM information_schema.columns 
      WHERE table_schema = '{{schema}}' AND table_name = '{{table}}'
      ORDER BY ordinal_position
    `,
    required: ['schema', 'table'],
    maxRows: 1000
  },

  sample_top: {
    sql: `SELECT * FROM {{schema}}.{{table}} LIMIT {{n}}`,
    required: ['schema', 'table', 'n'],
    maxRows: 1000,
    allowSelectStar: true,  // ONLY template that allows SELECT *
    validation: (params) => {
      if (params.n > 1000) throw new Error('Sample size cannot exceed 1000 rows');
    }
  },

  top_n: {
    sql: `
      SELECT {{dimension}}, {{metric}} as metric_value
      FROM {{schema}}.{{table}}
      WHERE {{date_column}} BETWEEN '{{start_date}}' AND '{{end_date}}'
        {{#if filters}}AND {{filters}}{{/if}}
      GROUP BY {{dimension}}
      ORDER BY metric_value DESC
      LIMIT {{n}}
    `,
    required: ['schema', 'table', 'dimension', 'metric', 'date_column', 'start_date', 'end_date', 'n'],
    maxRows: 100,
    validation: (params) => {
      if (params.n > 100) throw new Error('Top N cannot exceed 100 rows');
      if (params.dimension.includes('*')) throw new Error('SELECT * not allowed in top_n template');
    }
  },

  time_series: {
    sql: `
      SELECT DATE_TRUNC('{{grain}}', {{date_column}}) as time_period,
             {{metric}} as metric_value
      FROM {{schema}}.{{table}}  
      WHERE {{date_column}} BETWEEN '{{start_date}}' AND '{{end_date}}'
        {{#if filters}}AND {{filters}}{{/if}}
      GROUP BY time_period
      ORDER BY time_period
    `,
    required: ['grain', 'schema', 'table', 'date_column', 'metric', 'start_date', 'end_date'],
    maxRows: 1000,
    validation: (params) => {
      const validGrains = ['hour', 'day', 'week', 'month', 'quarter', 'year'];
      if (!validGrains.includes(params.grain)) {
        throw new Error(`Invalid grain. Must be one of: ${validGrains.join(', ')}`);
      }
    }
  },

  breakdown: {
    sql: `
      SELECT {{dimensions}}, {{metric}} as metric_value
      FROM {{schema}}.{{table}}
      WHERE {{date_column}} BETWEEN '{{start_date}}' AND '{{end_date}}'
        {{#if filters}}AND {{filters}}{{/if}}
      GROUP BY {{dimensions}}
      ORDER BY metric_value DESC
      LIMIT {{limit}}
    `,
    required: ['schema', 'table', 'dimensions', 'metric', 'date_column', 'start_date', 'end_date'],
    maxRows: 1000
  },

  comparison: {
    sql: `
      WITH period_a AS (
        SELECT {{metric}} as metric_a
        FROM {{schema}}.{{table}}
        WHERE {{date_column}} BETWEEN '{{start_date_a}}' AND '{{end_date_a}}'
          {{#if filters}}AND {{filters}}{{/if}}
      ),
      period_b AS (
        SELECT {{metric}} as metric_b
        FROM {{schema}}.{{table}}
        WHERE {{date_column}} BETWEEN '{{start_date_b}}' AND '{{end_date_b}}'
          {{#if filters}}AND {{filters}}{{/if}}
      )
      SELECT 
        (SELECT metric_a FROM period_a) as period_a_value,
        (SELECT metric_b FROM period_b) as period_b_value,
        ((SELECT metric_b FROM period_b) - (SELECT metric_a FROM period_a)) as difference,
        (((SELECT metric_b FROM period_b) - (SELECT metric_a FROM period_a)) / (SELECT metric_a FROM period_a)) * 100 as percent_change
    `,
    required: ['schema', 'table', 'metric', 'date_column', 'start_date_a', 'end_date_a', 'start_date_b', 'end_date_b'],
    maxRows: 1
  }
};