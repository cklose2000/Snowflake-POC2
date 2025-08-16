-- ============================================================================
-- Deploy Hardened SDLC System
-- This script deploys the hardened v2 versions of SDLC procedures and views
-- ============================================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE CLAUDE_BI;
USE SCHEMA MCP;

-- Note: The v2 files contain procedures with JavaScript that need special handling
-- They have been tested and work correctly when deployed individually
-- This deployment script references them for manual execution

-- Step 1: Deploy core views first (no JavaScript, can run directly)
-- Run: scripts/sdlc-two-table/02_core_views_v2.sql

-- Step 2: Deploy concurrency procedures 
-- Run: scripts/sdlc-two-table/03_concurrency_procedures_v2.sql
-- Contains: SDLC_CREATE_WORK with internal ID generation, mandatory concurrency

-- Step 3: Deploy agent integration
-- Run: scripts/sdlc-two-table/04_agent_integration_v2.sql  
-- Contains: SDLC_CLAIM_NEXT with retry logic and version checking

-- For now, let's create a simple test work item manually to verify the system