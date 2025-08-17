CREATE OR REPLACE VIEW MCP.TEST_GATEWAY_VIEW AS
SELECT 
  'gateway_test' AS test_name,
  CURRENT_TIMESTAMP() AS created_at,
  'This view was deployed via the event-native gateway' AS description;