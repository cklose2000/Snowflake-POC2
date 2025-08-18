#!/usr/bin/env python3
"""
Test chat parsing logic without Streamlit dependencies
"""

def parse_user_input(user_input: str) -> dict:
    """
    Simple pattern matching to understand user intent
    """
    input_lower = user_input.lower()
    
    # Detect time range
    if "hour" in input_lower or "today" in input_lower:
        time_range = "24 hours"
        time_filter = "OCCURRED_AT >= CURRENT_TIMESTAMP() - INTERVAL '24 hours'"
    elif "week" in input_lower:
        time_range = "7 days"
        time_filter = "OCCURRED_AT >= CURRENT_TIMESTAMP() - INTERVAL '7 days'"
    elif "month" in input_lower:
        time_range = "30 days"
        time_filter = "OCCURRED_AT >= CURRENT_TIMESTAMP() - INTERVAL '30 days'"
    else:
        time_range = "7 days"
        time_filter = "OCCURRED_AT >= CURRENT_TIMESTAMP() - INTERVAL '7 days'"
    
    # Detect query type
    query_type = "summary"  # default
    
    if any(word in input_lower for word in ["count", "how many", "total"]):
        query_type = "count"
    elif any(word in input_lower for word in ["top", "most", "highest"]):
        if "user" in input_lower or "actor" in input_lower:
            query_type = "top_actors"
        elif "action" in input_lower:
            query_type = "top_actions"
        elif "source" in input_lower:
            query_type = "top_sources"
        else:
            query_type = "top_actions"  # default to actions
    elif any(word in input_lower for word in ["recent", "latest", "last"]):
        query_type = "recent"
    elif any(word in input_lower for word in ["error", "fail", "problem"]):
        query_type = "errors"
    elif "change" in input_lower:
        query_type = "changes"
    
    return {
        "query_type": query_type,
        "time_filter": time_filter,
        "time_range": time_range
    }

def test_parsing():
    """Test various user inputs"""
    test_cases = [
        ("Show me activity today", "summary", "24 hours"),
        ("How many events this week?", "count", "7 days"),
        ("Who are the top users?", "top_actors", "7 days"),
        ("Show top actions this month", "top_actions", "30 days"),
        ("Any errors today?", "errors", "24 hours"),
        ("What changed recently?", "changes", "7 days"),
        ("Show me recent events", "recent", "7 days"),
        ("Most active users this week", "top_actors", "7 days"),
    ]
    
    print("Testing chat parsing logic...")
    print("=" * 60)
    
    passed = 0
    failed = 0
    
    for input_text, expected_type, expected_range in test_cases:
        result = parse_user_input(input_text)
        
        type_match = result["query_type"] == expected_type
        range_match = result["time_range"] == expected_range
        
        if type_match and range_match:
            print(f"✅ PASS: '{input_text}'")
            print(f"   Type: {result['query_type']} | Range: {result['time_range']}")
            passed += 1
        else:
            print(f"❌ FAIL: '{input_text}'")
            print(f"   Expected: {expected_type} / {expected_range}")
            print(f"   Got: {result['query_type']} / {result['time_range']}")
            failed += 1
    
    print("=" * 60)
    print(f"Results: {passed} passed, {failed} failed")
    
    if failed == 0:
        print("✅ All tests passed!")
    else:
        print(f"⚠️ {failed} tests failed")

if __name__ == "__main__":
    test_parsing()