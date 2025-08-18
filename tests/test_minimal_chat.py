#!/usr/bin/env python3
"""
Test the minimal chat functionality locally
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dashboards.minimal_chat.app_simple import parse_user_input, build_query, format_results
import pandas as pd
from datetime import datetime, timedelta

def test_parse_user_input():
    """Test input parsing"""
    print("Testing input parsing...")
    
    test_cases = [
        ("Show me activity today", {"query_type": "summary", "time_range": "24 hours"}),
        ("Count events this week", {"query_type": "count", "time_range": "7 days"}),
        ("Who are the top users?", {"query_type": "top_actors", "time_range": "7 days"}),
        ("Show top actions this month", {"query_type": "top_actions", "time_range": "30 days"}),
        ("Recent errors", {"query_type": "errors", "time_range": "7 days"}),
        ("What changed?", {"query_type": "changes", "time_range": "7 days"}),
    ]
    
    for input_text, expected in test_cases:
        result = parse_user_input(input_text)
        print(f"  Input: '{input_text}'")
        print(f"    Type: {result['query_type']} (expected: {expected['query_type']})")
        print(f"    Range: {result['time_range']} (expected: {expected['time_range']})")
        assert result['query_type'] == expected['query_type'], f"Failed for: {input_text}"
        print("    ✓ Passed")
    
    print("✅ All parsing tests passed!\n")

def test_build_query():
    """Test query building"""
    print("Testing query building...")
    
    test_cases = [
        {
            "input": {"query_type": "count", "time_filter": "OCCURRED_AT >= CURRENT_TIMESTAMP() - INTERVAL '7 days'"},
            "expected_keywords": ["COUNT(*)", "UNIQUE_ACTORS", "ACTIVITY.EVENTS"]
        },
        {
            "input": {"query_type": "top_actors", "time_filter": "OCCURRED_AT >= CURRENT_TIMESTAMP() - INTERVAL '7 days'"},
            "expected_keywords": ["ACTOR_ID", "GROUP BY", "ORDER BY", "LIMIT 10"]
        },
        {
            "input": {"query_type": "errors", "time_filter": "OCCURRED_AT >= CURRENT_TIMESTAMP() - INTERVAL '24 hours'"},
            "expected_keywords": ["error", "fail", "ORDER BY OCCURRED_AT DESC"]
        },
    ]
    
    for test in test_cases:
        query = build_query(test["input"])
        print(f"  Query type: {test['input']['query_type']}")
        for keyword in test["expected_keywords"]:
            assert keyword.lower() in query.lower(), f"Missing keyword '{keyword}' in query"
            print(f"    ✓ Contains '{keyword}'")
    
    print("✅ All query building tests passed!\n")

def test_format_results():
    """Test result formatting"""
    print("Testing result formatting...")
    
    # Test count formatting
    df_count = pd.DataFrame({
        'TOTAL_EVENTS': [1234],
        'UNIQUE_ACTORS': [56],
        'UNIQUE_ACTIONS': [78]
    })
    
    result = format_results(df_count, "count", "7 days")
    print("  Count format:")
    print(f"    {result[:50]}...")
    assert "1,234" in result
    assert "56" in result
    print("    ✓ Passed")
    
    # Test top actors formatting
    df_actors = pd.DataFrame({
        'ACTOR_ID': ['user1', 'user2', 'user3'],
        'EVENT_COUNT': [100, 75, 50],
        'UNIQUE_ACTIONS': [10, 8, 5]
    })
    
    result = format_results(df_actors, "top_actors", "7 days")
    print("  Top actors format:")
    print(f"    {result[:50]}...")
    assert "user1" in result
    assert "100" in result
    print("    ✓ Passed")
    
    # Test error formatting (no errors)
    df_no_errors = pd.DataFrame()
    result = format_results(df_no_errors, "errors", "24 hours")
    print("  No errors format:")
    print(f"    {result}")
    assert "No errors found" in result
    print("    ✓ Passed")
    
    # Test error formatting (with errors)
    df_errors = pd.DataFrame({
        'OCCURRED_AT': [datetime.now(), datetime.now() - timedelta(hours=1)],
        'ACTION': ['system.error', 'api.fail'],
        'ACTOR_ID': ['user1', 'user2'],
        'ERROR_MESSAGE': ['Connection failed', None],
        'ERROR_CODE': ['500', None]
    })
    
    result = format_results(df_errors, "errors", "24 hours")
    print("  Errors format:")
    print(f"    {result[:50]}...")
    assert "system.error" in result
    print("    ✓ Passed")
    
    print("✅ All formatting tests passed!\n")

def test_full_flow():
    """Test complete flow from input to formatted output"""
    print("Testing full flow...")
    
    test_inputs = [
        "Show me a summary of activity",
        "Who are the top users this week?",
        "Any errors today?",
        "What are the most common actions?",
    ]
    
    for user_input in test_inputs:
        print(f"\n  User: '{user_input}'")
        
        # Parse
        parsed = parse_user_input(user_input)
        print(f"    Parsed as: {parsed['query_type']}")
        
        # Build query
        query = build_query(parsed)
        print(f"    Query length: {len(query)} chars")
        print(f"    Query preview: {query[:100]}...")
        
        # Would execute here in real scenario
        print("    ✓ Flow completed")
    
    print("\n✅ All flow tests passed!\n")

if __name__ == "__main__":
    print("=" * 60)
    print("Testing Minimal Chat Functionality")
    print("=" * 60 + "\n")
    
    test_parse_user_input()
    test_build_query()
    test_format_results()
    test_full_flow()
    
    print("=" * 60)
    print("✅ ALL TESTS PASSED!")
    print("=" * 60)