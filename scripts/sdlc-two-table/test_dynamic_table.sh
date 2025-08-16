#!/bin/bash
# ============================================================================
# test_dynamic_table.sh
# Automated test runner for Dynamic Table functionality
# ============================================================================

set -e  # Exit on error

echo "=================================================="
echo "Dynamic Table Test Suite"
echo "Testing LANDING.RAW_EVENTS → ACTIVITY.EVENTS"
echo "=================================================="

# Check for required environment variable
if [ -z "$SF_PK_PATH" ]; then
  echo "Setting SF_PK_PATH to default: ./claude_code_rsa_key.p8"
  export SF_PK_PATH="./claude_code_rsa_key.p8"
fi

# Base path for sf command
SF_CMD="SF_PK_PATH=$SF_PK_PATH ~/bin/sf"

echo ""
echo "Step 1: Deploy test procedures"
echo "-------------------------------"
$SF_CMD exec-file /Users/chandler/claude7/GrowthZone/SnowflakePOC2/scripts/sdlc-two-table/19_dt_test_suite.sql

echo ""
echo "Step 2: Deploy monitoring views"
echo "--------------------------------"
$SF_CMD exec-file /Users/chandler/claude7/GrowthZone/SnowflakePOC2/scripts/sdlc-two-table/20_dt_monitoring.sql

echo ""
echo "Step 3: Check initial health status"
echo "------------------------------------"
$SF_CMD sql "SELECT * FROM MCP.VW_DT_HEALTH;"

echo ""
echo "Step 4: Check current consistency"
echo "----------------------------------"
$SF_CMD sql "
SELECT 
  'LANDING.RAW_EVENTS' as table_name,
  COUNT(*) as row_count
FROM CLAUDE_BI.LANDING.RAW_EVENTS
UNION ALL
SELECT 
  'ACTIVITY.EVENTS' as table_name,
  COUNT(*) as row_count
FROM CLAUDE_BI.ACTIVITY.EVENTS;"

echo ""
echo "Step 5: Run comprehensive test suite"
echo "-------------------------------------"
echo "This will take several minutes due to lag testing..."
$SF_CMD sql "CALL MCP.RUN_DT_TEST_SUITE();"

echo ""
echo "Step 6: Check for alerts"
echo "-------------------------"
$SF_CMD sql "SELECT * FROM MCP.VW_DT_ALERTS;"

echo ""
echo "Step 7: View dashboard summary"
echo "-------------------------------"
$SF_CMD sql "SELECT * FROM MCP.VW_DT_DASHBOARD;"

echo ""
echo "Step 8: Check lag statistics"
echo "-----------------------------"
$SF_CMD sql "SELECT * FROM MCP.VW_DT_LAG_MONITOR;"

echo ""
echo "Step 9: View deduplication effectiveness"
echo "-----------------------------------------"
$SF_CMD sql "SELECT * FROM MCP.VW_DT_DEDUP_STATS;"

echo ""
echo "Step 10: Check filter statistics"
echo "---------------------------------"
$SF_CMD sql "SELECT * FROM MCP.VW_DT_FILTER_STATS;"

echo ""
echo "Step 11: Performance metrics"
echo "-----------------------------"
$SF_CMD sql "SELECT * FROM MCP.VW_DT_PERFORMANCE ORDER BY hour DESC LIMIT 5;"

echo ""
echo "Step 12: Generate recommendations"
echo "----------------------------------"
$SF_CMD sql "
WITH recommendations AS (
  SELECT 
    CASE 
      WHEN health_status != 'HEALTHY' THEN 'CRITICAL: Dynamic Table health issues detected'
      ELSE NULL
    END as health_rec,
    CASE 
      WHEN pending_promotion_count > 100 THEN 'WARNING: High pending promotion count - check refresh'
      ELSE NULL
    END as pending_rec,
    CASE 
      WHEN max_lag_seconds > 120 THEN 'WARNING: Lag exceeds target - consider reducing target lag'
      ELSE NULL
    END as lag_rec,
    CASE 
      WHEN filter_rate > 20 THEN 'INFO: High filter rate - review data quality'
      ELSE NULL
    END as filter_rec
  FROM MCP.VW_DT_DASHBOARD
)
SELECT 
  ARRAY_CONSTRUCT_COMPACT(
    health_rec, 
    pending_rec, 
    lag_rec, 
    filter_rec
  ) as recommendations
FROM recommendations;"

echo ""
echo "=================================================="
echo "Dynamic Table Test Suite Complete!"
echo "=================================================="
echo ""
echo "Summary:"
echo "  • Test procedures deployed and executed"
echo "  • Monitoring views created"
echo "  • Health metrics collected"
echo "  • Performance analyzed"
echo ""
echo "Next Steps:"
echo "  1. Review test results above"
echo "  2. Check VW_DT_ALERTS for any issues"
echo "  3. Monitor VW_DT_DASHBOARD regularly"
echo "  4. Consider enabling INCREMENTAL refresh mode if appropriate"
echo ""

# Optional: Save test results to file
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULTS_FILE="dt_test_results_${TIMESTAMP}.txt"

echo "Saving detailed results to: $RESULTS_FILE"
$SF_CMD sql "
SELECT 
  'Test Run: ' || CURRENT_TIMESTAMP() as header,
  '==================' as separator
UNION ALL
SELECT 
  'Health Status', health_status
FROM MCP.VW_DT_HEALTH
UNION ALL
SELECT 
  'Sync Status', sync_status
FROM MCP.VW_DT_HEALTH
UNION ALL
SELECT 
  'Pending Events', TO_VARCHAR(pending_promotion_count)
FROM MCP.VW_DT_HEALTH
UNION ALL
SELECT 
  'Active Alerts', TO_VARCHAR(COUNT(*))
FROM MCP.VW_DT_ALERTS;" > $RESULTS_FILE

echo ""
echo "Test results saved to: $RESULTS_FILE"
echo "Test suite execution complete!"