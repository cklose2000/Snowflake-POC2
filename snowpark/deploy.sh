#!/bin/bash

# Snowpark Container Services MCP Deployment Script
# Builds and deploys the MCP server to Snowflake

set -e

echo "🚀 Snowpark MCP Server Deployment"
echo "=================================="

# Configuration
SNOWFLAKE_ACCOUNT=${SNOWFLAKE_ACCOUNT:-"your-account"}
SNOWFLAKE_REGION=${SNOWFLAKE_REGION:-"us-east-1"}
REGISTRY_URL="${SNOWFLAKE_ACCOUNT}.registry.snowflakecomputing.com"
IMAGE_REPO="CLAUDE_BI/PUBLIC/MCP_REPO"
IMAGE_NAME="mcp-server"
IMAGE_TAG="latest"

# Full image path
FULL_IMAGE="${REGISTRY_URL}/${IMAGE_REPO}/${IMAGE_NAME}:${IMAGE_TAG}"

echo "📦 Building Docker image..."
docker build -t ${IMAGE_NAME}:${IMAGE_TAG} .

echo "🏷️  Tagging image for Snowflake registry..."
docker tag ${IMAGE_NAME}:${IMAGE_TAG} ${FULL_IMAGE}

echo "🔐 Logging into Snowflake registry..."
docker login ${REGISTRY_URL} -u ${SNOWFLAKE_USERNAME} -p ${SNOWFLAKE_PASSWORD}

echo "⬆️  Pushing image to Snowflake..."
docker push ${FULL_IMAGE}

echo "📋 Uploading configuration files to stage..."
# Use SnowSQL to upload files
snowsql -a ${SNOWFLAKE_ACCOUNT} \
        -u ${SNOWFLAKE_USERNAME} \
        -d CLAUDE_BI \
        -s PUBLIC \
        -w CLAUDE_WAREHOUSE \
        -r ACCOUNTADMIN \
        --query "
-- Upload service specification
PUT file://service.yaml @CLAUDE_BI.PUBLIC.MCP_STAGE/service.yaml OVERWRITE=TRUE;

-- Upload contracts
PUT file://contracts/*.json @CLAUDE_BI.PUBLIC.MCP_STAGE/contracts/ OVERWRITE=TRUE;

-- Upload templates (if any)
PUT file://templates/*.sql @CLAUDE_BI.PUBLIC.MCP_STAGE/templates/ OVERWRITE=TRUE;
"

echo "🚀 Creating/updating service..."
snowsql -a ${SNOWFLAKE_ACCOUNT} \
        -u ${SNOWFLAKE_USERNAME} \
        -d CLAUDE_BI \
        -s PUBLIC \
        -w CLAUDE_WAREHOUSE \
        -r MCP_SERVICE_ROLE \
        --query "
-- Drop existing service if it exists
DROP SERVICE IF EXISTS CLAUDE_BI.PUBLIC.MCP_SERVER;

-- Create new service
CREATE SERVICE CLAUDE_BI.PUBLIC.MCP_SERVER
  IN COMPUTE POOL MCP_COMPUTE_POOL
  FROM @CLAUDE_BI.PUBLIC.MCP_STAGE/service.yaml
  COMMENT = 'MCP server running in Snowpark Container Services';

-- Wait for service to be ready
CALL SYSTEM$WAIT_FOR_SERVICE('CLAUDE_BI.PUBLIC.MCP_SERVER', 300);

-- Get service status
SELECT SYSTEM$GET_SERVICE_STATUS('CLAUDE_BI.PUBLIC.MCP_SERVER');

-- Get service endpoint
SELECT SYSTEM$GET_SERVICE_ENDPOINT('CLAUDE_BI.PUBLIC.MCP_SERVER', 'api') AS endpoint;
"

echo "✅ Deployment complete!"
echo ""
echo "📊 Service Information:"
echo "  - Service: CLAUDE_BI.PUBLIC.MCP_SERVER"
echo "  - Compute Pool: MCP_COMPUTE_POOL"
echo "  - Image: ${FULL_IMAGE}"
echo ""
echo "🔍 To check service status:"
echo "  snowsql --query \"SELECT SYSTEM\$GET_SERVICE_STATUS('CLAUDE_BI.PUBLIC.MCP_SERVER');\""
echo ""
echo "🌐 To get service endpoint:"
echo "  snowsql --query \"SELECT SYSTEM\$GET_SERVICE_ENDPOINT('CLAUDE_BI.PUBLIC.MCP_SERVER', 'api');\""