#!/bin/bash

# ============================================================================
# HappyFox Data Upload Script
# Purpose: Upload JSONL data file to Snowflake stage for ingestion
# ============================================================================

set -e  # Exit on error

# Configuration
JSONL_FILE="/Users/chandler/claude7/GrowthZone/SnowflakePOC2/happyfox-aging-cli/data-full/hf_tickets_complete.jsonl"
SNOWFLAKE_STAGE="@CLAUDE_BI.LANDING.STG_HAPPYFOX_HISTORICAL"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "============================================================================"
echo "HappyFox Data Upload to Snowflake"
echo "============================================================================"

# Check if file exists
if [ ! -f "$JSONL_FILE" ]; then
    echo -e "${RED}Error: JSONL file not found at $JSONL_FILE${NC}"
    echo "Please ensure the HappyFox data extraction has completed."
    exit 1
fi

# Get file size
FILE_SIZE=$(ls -lh "$JSONL_FILE" | awk '{print $5}')
LINE_COUNT=$(wc -l < "$JSONL_FILE")

echo -e "${GREEN}Found data file:${NC}"
echo "  Path: $JSONL_FILE"
echo "  Size: $FILE_SIZE"
echo "  Lines: $LINE_COUNT"
echo ""

# Check if SF_PK_PATH is set
if [ -z "$SF_PK_PATH" ]; then
    echo -e "${YELLOW}Warning: SF_PK_PATH not set. Setting to default.${NC}"
    export SF_PK_PATH="./claude_code_rsa_key.p8"
fi

# Upload to Snowflake
echo "Uploading to Snowflake stage..."
echo "Target: $SNOWFLAKE_STAGE"
echo ""

# Execute the PUT command
$SF_PK_PATH ./claude_code_rsa_key.p8 ~/bin/sf sql "
PUT file://$JSONL_FILE $SNOWFLAKE_STAGE 
AUTO_COMPRESS=TRUE 
PARALLEL=8
OVERWRITE=TRUE;
"

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Upload successful!${NC}"
    echo ""
    echo "Next steps:"
    echo "1. Run the initial load procedure:"
    echo "   ~/bin/sf sql 'CALL CLAUDE_BI.LANDING.LOAD_HAPPYFOX_HISTORICAL();'"
    echo ""
    echo "2. Verify the load:"
    echo "   ~/bin/sf sql 'SELECT * FROM TABLE(CLAUDE_BI.LANDING.GET_HAPPYFOX_LOAD_STATUS());'"
    echo ""
    echo "3. Check the data:"
    echo "   ~/bin/sf sql 'SELECT COUNT(*) FROM CLAUDE_BI.MCP.VW_HF_TICKETS;'"
else
    echo -e "${RED}✗ Upload failed. Please check your Snowflake connection.${NC}"
    exit 1
fi

echo "============================================================================"