-- Example: Dynamic Pivot Procedure using JavaScript
-- This procedure creates pivot tables dynamically based on input parameters

-- @statement
CREATE OR REPLACE PROCEDURE DYNAMIC_PIVOT_EVENTS(
  start_ts TIMESTAMP_TZ,
  end_ts TIMESTAMP_TZ,
  row_dimension STRING,    -- e.g., 'actor_id', 'DATE(occurred_at)'
  column_dimension STRING,  -- e.g., 'action'
  value_metric STRING      -- e.g., 'COUNT(*)', 'COUNT(DISTINCT session_id)'
)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
COMMENT = 'Creates a pivot table from event data with dynamic columns'
AS
$$
  try {
    // First, get the unique values for the column dimension
    var columnQuery = `
      SELECT DISTINCT ${COLUMN_DIMENSION} AS col_value
      FROM ACTIVITY.EVENTS
      WHERE occurred_at BETWEEN ?
        AND ?
        AND ${COLUMN_DIMENSION} IS NOT NULL
      ORDER BY col_value
      LIMIT 50  -- Limit columns for readability
    `;
    
    var columnStmt = snowflake.createStatement({
      sqlText: columnQuery,
      binds: [START_TS, END_TS]
    });
    
    var columnResult = columnStmt.execute();
    var columns = [];
    
    while (columnResult.next()) {
      columns.push(columnResult.getColumnValue(1));
    }
    
    if (columns.length === 0) {
      return {
        ok: false,
        error: "No data found for the specified column dimension"
      };
    }
    
    // Build the pivot query
    var pivotCases = columns.map(col => 
      `SUM(CASE WHEN ${COLUMN_DIMENSION} = '${col}' THEN ${VALUE_METRIC} ELSE 0 END) AS "${col}"`
    ).join(',\n    ');
    
    var pivotQuery = `
      SELECT 
        ${ROW_DIMENSION} AS row_label,
        ${pivotCases}
      FROM ACTIVITY.EVENTS
      WHERE occurred_at BETWEEN ?
        AND ?
      GROUP BY ${ROW_DIMENSION}
      ORDER BY row_label
      LIMIT 1000
    `;
    
    var pivotStmt = snowflake.createStatement({
      sqlText: pivotQuery,
      binds: [START_TS, END_TS]
    });
    
    var pivotResult = pivotStmt.execute();
    var data = [];
    
    while (pivotResult.next()) {
      var row = {row_label: pivotResult.getColumnValue(1)};
      for (var i = 0; i < columns.length; i++) {
        row[columns[i]] = pivotResult.getColumnValue(i + 2);
      }
      data.push(row);
    }
    
    return {
      ok: true,
      data: data,
      metadata: {
        row_dimension: ROW_DIMENSION,
        column_dimension: COLUMN_DIMENSION,
        value_metric: VALUE_METRIC,
        column_count: columns.length,
        row_count: data.length
      }
    };
    
  } catch (err) {
    return {
      ok: false,
      error: err.message,
      error_code: err.code,
      stack_trace: err.stackTraceTxt
    };
  }
$$;

-- Example usage:
-- CALL DYNAMIC_PIVOT_EVENTS(
--   DATEADD('day', -7, CURRENT_TIMESTAMP()),
--   CURRENT_TIMESTAMP(),
--   'DATE(occurred_at)',
--   'action',
--   'COUNT(*)'
-- );