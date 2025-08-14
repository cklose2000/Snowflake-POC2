// SafeSQL Templates - Only allowed SQL patterns in v1
const { fqn, qualifySource, createActivityName, SCHEMAS, TABLES, ACTIVITY_VIEW_MAP, DB } = require('../../snowflake-schema/generated.js');

const SAFESQL_TEMPLATES = {
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
    sql: 'SELECT * FROM ? LIMIT ?',
    required: ['schema', 'table', 'n'],
    maxRows: 1000,
    allowSelectStar: true,  // ONLY template that allows SELECT *
    parameterBuilder: (params) => {
      const qualifiedTable = qualifySource(`${params.schema}.${params.table}`);
      return [qualifiedTable, params.n];
    },
    validation: (params) => {
      if (params.n > 1000) throw new Error('Sample size cannot exceed 1000 rows');
    }
  },

  top_n: {
    sql: `
      SELECT ?, ? as metric_value
      FROM ?
      WHERE ? BETWEEN ? AND ?
      GROUP BY ?
      ORDER BY metric_value DESC
      LIMIT ?
    `,
    required: ['schema', 'table', 'dimension', 'metric', 'date_column', 'start_date', 'end_date', 'n'],
    maxRows: 100,
    parameterBuilder: (params) => {
      const qualifiedTable = qualifySource(`${params.schema}.${params.table}`);
      return [
        params.dimension, params.metric, qualifiedTable,
        params.date_column, params.start_date, params.end_date,
        params.dimension, params.n
      ];
    },
    validation: (params) => {
      if (params.n > 100) throw new Error('Top N cannot exceed 100 rows');
      if (params.dimension.includes('*')) throw new Error('SELECT * not allowed in top_n template');
    }
  },

  time_series: {
    sql: `
      SELECT DATE_TRUNC(?, ?) as time_period,
             ? as metric_value
      FROM ?
      WHERE ? BETWEEN ? AND ?
      GROUP BY time_period
      ORDER BY time_period
    `,
    required: ['grain', 'schema', 'table', 'date_column', 'metric', 'start_date', 'end_date'],
    maxRows: 1000,
    parameterBuilder: (params) => {
      const qualifiedTable = qualifySource(`${params.schema}.${params.table}`);
      return [
        params.grain, params.date_column, params.metric, qualifiedTable,
        params.date_column, params.start_date, params.end_date
      ];
    },
    validation: (params) => {
      const validGrains = ['hour', 'day', 'week', 'month', 'quarter', 'year'];
      if (!validGrains.includes(params.grain)) {
        throw new Error(`Invalid grain. Must be one of: ${validGrains.join(', ')}`);
      }
    }
  },

  breakdown: {
    sql: `
      SELECT ?, ? as metric_value
      FROM ?
      WHERE ? BETWEEN ? AND ?
      GROUP BY ?
      ORDER BY metric_value DESC
      LIMIT ?
    `,
    required: ['schema', 'table', 'dimensions', 'metric', 'date_column', 'start_date', 'end_date', 'limit'],
    maxRows: 1000,
    parameterBuilder: (params) => {
      const qualifiedTable = qualifySource(`${params.schema}.${params.table}`);
      return [
        params.dimensions, params.metric, qualifiedTable,
        params.date_column, params.start_date, params.end_date,
        params.dimensions, params.limit || 1000
      ];
    }
  },

  comparison: {
    sql: `
      WITH period_a AS (
        SELECT ? as metric_a
        FROM ?
        WHERE ? BETWEEN ? AND ?
      ),
      period_b AS (
        SELECT ? as metric_b
        FROM ?
        WHERE ? BETWEEN ? AND ?
      )
      SELECT 
        (SELECT metric_a FROM period_a) as period_a_value,
        (SELECT metric_b FROM period_b) as period_b_value,
        ((SELECT metric_b FROM period_b) - (SELECT metric_a FROM period_a)) as difference,
        (((SELECT metric_b FROM period_b) - (SELECT metric_a FROM period_a)) / (SELECT metric_a FROM period_a)) * 100 as percent_change
    `,
    required: ['schema', 'table', 'metric', 'date_column', 'start_date_a', 'end_date_a', 'start_date_b', 'end_date_b'],
    maxRows: 1,
    parameterBuilder: (params) => {
      const qualifiedTable = qualifySource(`${params.schema}.${params.table}`);
      return [
        params.metric, qualifiedTable, params.date_column, params.start_date_a, params.end_date_a,
        params.metric, qualifiedTable, params.date_column, params.start_date_b, params.end_date_b
      ];
    }
  }
};

module.exports = { SAFESQL_TEMPLATES };