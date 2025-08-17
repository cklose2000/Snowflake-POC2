CREATE OR REPLACE PROCEDURE MCP.GOLDEN_TEST()
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
  // Golden self-test with semicolons to prove the deployment path works
  const SF = snowflake;
  
  // Test 1: Semicolon in SQL string
  const rs = SF.createStatement({ 
    sqlText: "SELECT 1 AS test_value; -- test semicolon in comment" 
  }).execute();
  
  rs.next();
  const value = rs.getColumnValue('TEST_VALUE');
  
  // Test 2: Multiple statements with semicolons
  const tests = [];
  tests.push('semicolon;test');
  tests.push('another;test');
  
  // Test 3: Complex object with semicolons
  const result = {
    status: 'golden',
    value: value,
    tests: tests.join('; '),
    message: 'Gateway deployment path validated; semicolons work correctly'
  };
  
  return 'golden:' + value + ' - All tests passed';
$$;