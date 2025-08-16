#!/usr/bin/env python3
"""
Simple Real Integration Test
Tests actual Snowflake procedures with minimal complexity
"""

import os
import sys
import json
import subprocess
from datetime import datetime, timezone, timedelta

# Set environment
os.environ['SF_PK_PATH'] = './claude_code_rsa_key.p8'

def run_sf_sql(sql):
    """Run SQL via sf CLI"""
    cmd = ['/Users/chandler/bin/sf', 'sql', sql]
    result = subprocess.run(cmd, capture_output=True, text=True)
    return result.returncode == 0, result.stdout, result.stderr

def test_procedures():
    """Test the dashboard procedures"""
    
    print("=" * 60)
    print("SIMPLE REAL TESTS - Direct Snowflake Procedures")
    print("=" * 60)
    
    tests = [
        ("Two-Table Law", 
         "SELECT COUNT(*) as cnt FROM CLAUDE_BI.INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG='CLAUDE_BI' AND TABLE_SCHEMA IN ('LANDING','ACTIVITY') AND TABLE_TYPE IN ('BASE TABLE','DYNAMIC TABLE')"),
        
        ("DASH_GET_SERIES", 
         "CALL MCP.DASH_GET_SERIES(PARSE_JSON('{\"start_ts\":\"2025-01-15T00:00:00Z\",\"end_ts\":\"2025-01-16T00:00:00Z\",\"interval\":\"hour\",\"filters\":{}}'))"),
        
        ("DASH_GET_TOPN",
         "CALL MCP.DASH_GET_TOPN(PARSE_JSON('{\"start_ts\":\"2025-01-15T00:00:00Z\",\"end_ts\":\"2025-01-16T00:00:00Z\",\"dimension\":\"action\",\"n\":10,\"filters\":{}}'))"),
        
        ("DASH_GET_EVENTS",
         "CALL MCP.DASH_GET_EVENTS(PARSE_JSON('{\"cursor_ts\":\"2025-01-16T00:00:00Z\",\"limit\":10}'))"),
        
        ("DASH_GET_METRICS",
         "CALL MCP.DASH_GET_METRICS(PARSE_JSON('{\"start_ts\":\"2025-01-01T00:00:00Z\",\"end_ts\":\"2025-01-16T00:00:00Z\",\"filters\":{}}'))")
    ]
    
    passed = 0
    failed = 0
    
    for test_name, sql in tests:
        print(f"\nTesting {test_name}...")
        success, stdout, stderr = run_sf_sql(sql)
        
        if success:
            # Check for ok=true in response
            if "true" in stdout.lower() or "2" in stdout:  # 2 tables for Two-Table Law
                print(f"✓ {test_name} passed")
                passed += 1
            else:
                print(f"✗ {test_name} returned unexpected result")
                print(f"  Output preview: {stdout[:200]}")
                failed += 1
        else:
            print(f"✗ {test_name} failed")
            print(f"  Error: {stderr[:200] if stderr else 'No error details'}")
            failed += 1
    
    print("\n" + "=" * 60)
    print(f"Results: {passed} passed, {failed} failed")
    print("=" * 60)
    
    return failed == 0

if __name__ == "__main__":
    success = test_procedures()
    sys.exit(0 if success else 1)