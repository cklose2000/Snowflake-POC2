#!/bin/bash

# Deploy Authentication System for Claude Code
# This script deploys all SQL files in the correct order

set -e  # Exit on error

echo "🚀 Starting Claude Code Authentication System Deployment"
echo "======================================================="

# Load environment variables
source .env

# Function to execute SQL file
execute_sql() {
    local file=$1
    local description=$2
    
    echo ""
    echo "📄 Deploying: $description"
    echo "   File: $file"
    
    snowsql \
        --accountname "$SNOWFLAKE_ACCOUNT" \
        --username "$SNOWFLAKE_USERNAME" \
        --password "$SNOWFLAKE_PASSWORD" \
        --dbname "$SNOWFLAKE_DATABASE" \
        --schemaname "$SNOWFLAKE_SCHEMA" \
        --warehouse "$SNOWFLAKE_WAREHOUSE" \
        --rolename "$SNOWFLAKE_ROLE" \
        --file "$file" \
        --quiet \
        --variable DATABASE="$SNOWFLAKE_DATABASE"
    
    if [ $? -eq 0 ]; then
        echo "   ✅ Success"
    else
        echo "   ❌ Failed"
        exit 1
    fi
}

# Deploy in order
echo ""
echo "📦 Phase 1: Token Security Infrastructure"
execute_sql "snowpark/activity-schema/23_token_pepper_security.sql" "Secure pepper storage and token functions"

echo ""
echo "📦 Phase 2: Activation System"
execute_sql "snowpark/activity-schema/24_activation_system.sql" "One-click activation procedures"

echo ""
echo "📦 Phase 3: Token Lifecycle Management"
execute_sql "snowpark/activity-schema/25_token_lifecycle.sql" "Session tracking and automated rotation"

echo ""
echo "📦 Phase 4: Security Monitoring"
execute_sql "snowpark/activity-schema/26_security_monitoring.sql" "Security views and alerts"

echo ""
echo "📦 Phase 5: Emergency Procedures"
execute_sql "snowpark/activity-schema/27_emergency_procedures.sql" "Emergency response procedures"

echo ""
echo "======================================================="
echo "✅ Authentication System Deployment Complete!"
echo ""
echo "Next steps:"
echo "1. Start the activation gateway:"
echo "   cd activation-gateway && npm install && npm start"
echo ""
echo "2. Install Claude Code CLI helper:"
echo "   cd claude-code-auth && npm install -g ."
echo ""
echo "3. Create your first user:"
echo "   snowsql -q \"CALL ADMIN.CREATE_ACTIVATION('your_username', ARRAY_CONSTRUCT('compose_query', 'list_sources'), 10000, 3600, 90, 30);\""
echo ""
echo "4. Check system status:"
echo "   snowsql -q \"SELECT * FROM SECURITY.DASHBOARD;\""
echo ""