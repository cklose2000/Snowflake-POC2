#!/bin/bash
# ============================================================================
# deploy_hardened_ddl.sh
# Deploy the complete hardened DDL versioning system
# ============================================================================

set -e  # Exit on error

echo "=================================================="
echo "Deploying Hardened DDL Versioning System"
echo "=================================================="

# Check for required environment variable
if [ -z "$SF_PK_PATH" ]; then
  echo "Setting SF_PK_PATH to default: ./claude_code_rsa_key.p8"
  export SF_PK_PATH="./claude_code_rsa_key.p8"
fi

# Base path for sf command
SF_CMD="SF_PK_PATH=$SF_PK_PATH ~/bin/sf exec-file"

echo ""
echo "Step 1: Deploy SAFE_DDL wrapper procedure"
echo "------------------------------------------"
$SF_CMD /Users/chandler/claude7/GrowthZone/SnowflakePOC2/scripts/sdlc-two-table/12_safe_ddl_wrapper.sql

echo ""
echo "Step 2: Deploy hardened DDL_DEPLOY procedure"
echo "---------------------------------------------"
$SF_CMD /Users/chandler/claude7/GrowthZone/SnowflakePOC2/scripts/sdlc-two-table/13_ddl_deploy_hardened.sql

echo ""
echo "Step 3: Configure privilege-based security"
echo "-------------------------------------------"
$SF_CMD /Users/chandler/claude7/GrowthZone/SnowflakePOC2/scripts/sdlc-two-table/14_ddl_security.sql

echo ""
echo "Step 4: Create consistency views"
echo "---------------------------------"
$SF_CMD /Users/chandler/claude7/GrowthZone/SnowflakePOC2/scripts/sdlc-two-table/15_ddl_consistency.sql

echo ""
echo "Step 5: Deploy compliance monitoring"
echo "-------------------------------------"
$SF_CMD /Users/chandler/claude7/GrowthZone/SnowflakePOC2/scripts/sdlc-two-table/16_ddl_compliance.sql

echo ""
echo "Step 6: Deploy ALTER/DROP support"
echo "----------------------------------"
$SF_CMD /Users/chandler/claude7/GrowthZone/SnowflakePOC2/scripts/sdlc-two-table/17_ddl_alter_drop.sql

echo ""
echo "Step 7: Verify security configuration"
echo "--------------------------------------"
SF_PK_PATH=$SF_PK_PATH ~/bin/sf sql "
SELECT 
  role_name,
  role_type,
  ddl_privilege_count,
  can_use_safe_ddl,
  security_status,
  CASE 
    WHEN security_status = 'CORRECT' THEN '✓ Secure'
    ELSE '✗ SECURITY VIOLATION - ' || role_name || ' has DDL privileges!'
  END as status_message
FROM MCP.VW_DDL_SECURITY_STATUS
ORDER BY security_status DESC, role_name;"

echo ""
echo "Step 8: Run initial compliance check"
echo "-------------------------------------"
SF_PK_PATH=$SF_PK_PATH ~/bin/sf sql "CALL MCP.DDL_COMPLIANCE_CHECK();"

echo ""
echo "Step 9: Enable compliance monitoring task (optional)"
echo "-----------------------------------------------------"
echo "To enable automated compliance checks every 4 hours, run:"
echo "SF_PK_PATH=$SF_PK_PATH ~/bin/sf sql \"ALTER TASK MCP.TASK_DDL_COMPLIANCE_CHECK RESUME;\""

echo ""
echo "=================================================="
echo "Hardened DDL Versioning System Deployed!"
echo "=================================================="
echo ""
echo "Key Components Installed:"
echo "  ✓ SAFE_DDL - Single entry point for all DDL"
echo "  ✓ DDL_DEPLOY_HARDENED - Race-proof versioning"
echo "  ✓ Privilege-based security - Agents have no DDL rights"
echo "  ✓ Consistency views - Immediate read-after-write"
echo "  ✓ Compliance monitoring - Detect bypass attempts"
echo "  ✓ ALTER/DROP support - With soft delete capability"
echo "  ✓ Complete audit trail - All DDL as events"
echo ""
echo "Next Steps:"
echo "  1. Review the agent guide: 18_ddl_agent_guide.sql"
echo "  2. Test with: CALL MCP.SAFE_DDL('<your DDL>', 'reason');"
echo "  3. Monitor compliance: SELECT * FROM MCP.VW_DDL_COMPLIANCE_MONITOR;"
echo ""
echo "Security Status:"
SF_PK_PATH=$SF_PK_PATH ~/bin/sf sql "
SELECT 
  'Agents with DDL privileges: ' || COUNT(*) as security_check,
  CASE 
    WHEN COUNT(*) = 0 THEN '✓ SECURE - No agents can bypass SAFE_DDL'
    ELSE '✗ WARNING - Some agents have direct DDL access!'
  END as status
FROM MCP.VW_DDL_SECURITY_STATUS
WHERE role_type = 'AGENT' 
  AND ddl_privilege_count > 0;"

echo ""
echo "Deployment complete!"