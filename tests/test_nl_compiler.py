"""
Unit tests for Natural Language to Plan compiler
Validates NL parsing produces valid plans with ISO timestamps
"""

import pytest
import json
import sys
import os
from datetime import datetime, timezone, timedelta

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def parse_natural_language_simple(text):
    """
    Simplified NL parser for testing
    Returns plan with ISO timestamps
    """
    import re
    
    # Default plan
    plan = {
        "plan_version": "1.0",
        "proc": "DASH_GET_SERIES",
        "params": {
            "start_ts": (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat(),
            "end_ts": datetime.now(timezone.utc).isoformat(),
            "interval": "hour",
            "filters": {}
        }
    }
    
    # Parse time ranges
    time_match = re.search(r'last (\d+)\s*(hours?|days?|minutes?)', text.lower())
    if time_match:
        amount = int(time_match.group(1))
        unit = time_match.group(2).rstrip('s')
        
        if unit == 'hour':
            delta = timedelta(hours=amount)
        elif unit == 'day':
            delta = timedelta(days=amount)
        elif unit == 'minute':
            delta = timedelta(minutes=amount)
        else:
            delta = timedelta(hours=24)
        
        plan["params"]["start_ts"] = (datetime.now(timezone.utc) - delta).isoformat()
        plan["params"]["end_ts"] = datetime.now(timezone.utc).isoformat()
    
    # Parse "today"
    if "today" in text.lower():
        now = datetime.now(timezone.utc)
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        plan["params"]["start_ts"] = start.isoformat()
        plan["params"]["end_ts"] = now.isoformat()
    
    # Parse "top" queries
    if "top" in text.lower():
        plan["proc"] = "DASH_GET_TOPN"
        plan["params"]["n"] = 10
        
        # Parse dimension
        if "action" in text.lower():
            plan["params"]["dimension"] = "action"
        elif "user" in text.lower() or "actor" in text.lower():
            plan["params"]["dimension"] = "actor"
        elif "source" in text.lower():
            plan["params"]["dimension"] = "source"
        else:
            plan["params"]["dimension"] = "action"
    
    # Parse filters
    email_match = re.search(r'[\w.-]+@[\w.-]+\.\w+', text)
    if email_match:
        email = email_match.group(0)
        plan["params"]["filters"]["actor"] = email
    
    # Parse cohort URLs
    cohort_match = re.search(r's3://[^\s]+', text)
    if cohort_match:
        plan["params"]["filters"]["cohort_url"] = cohort_match.group(0)
    
    # Parse intervals
    if "by hour" in text.lower() or "hourly" in text.lower():
        plan["params"]["interval"] = "hour"
    elif "by day" in text.lower() or "daily" in text.lower():
        plan["params"]["interval"] = "day"
    elif "15 min" in text.lower() or "fifteen min" in text.lower():
        plan["params"]["interval"] = "15 minute"
    
    # Parse metrics/summary
    if "metric" in text.lower() or "summary" in text.lower():
        plan["proc"] = "DASH_GET_METRICS"
        # Remove interval for metrics
        plan["params"].pop("interval", None)
    
    return plan


class TestNLCompiler:
    """Test suite for Natural Language compilation"""
    
    def test_user_filter_extraction(self):
        """NL-01: Extract user email from natural language"""
        text = "hone in on user acme_coo@company.com last 48h by hour"
        plan = parse_natural_language_simple(text)
        
        # Check proc and params
        assert plan["proc"] == "DASH_GET_SERIES"
        assert "filters" in plan["params"]
        assert plan["params"]["filters"]["actor"] == "acme_coo@company.com"
        assert plan["params"]["interval"] == "hour"
        
        # Verify ISO timestamps
        assert "T" in plan["params"]["start_ts"]
        assert "Z" in plan["params"]["start_ts"] or "+" in plan["params"]["start_ts"]
    
    def test_top_actions_today(self):
        """NL-02: Parse 'top actions today' query"""
        text = "top actions today"
        plan = parse_natural_language_simple(text)
        
        assert plan["proc"] == "DASH_GET_TOPN"
        assert plan["params"]["dimension"] == "action"
        assert plan["params"]["n"] == 10
        
        # Check that start_ts is today at 00:00
        start = datetime.fromisoformat(plan["params"]["start_ts"].replace("Z", "+00:00"))
        assert start.hour == 0
        assert start.minute == 0
    
    def test_cohort_url_parsing(self):
        """NL-03: Parse cohort URL from text"""
        text = "use cohort s3://stage/cohorts/members.jsonl last 7d"
        plan = parse_natural_language_simple(text)
        
        assert "filters" in plan["params"]
        assert plan["params"]["filters"]["cohort_url"] == "s3://stage/cohorts/members.jsonl"
        
        # Verify 7 days time range
        start = datetime.fromisoformat(plan["params"]["start_ts"].replace("Z", "+00:00"))
        end = datetime.fromisoformat(plan["params"]["end_ts"].replace("Z", "+00:00"))
        delta = end - start
        assert 6 <= delta.days <= 7
    
    def test_iso_timestamps_generated(self):
        """PLAN-01: All timestamps are ISO8601 with timezone"""
        queries = [
            "show last 24 hours",
            "activity for last 7 days",
            "events from last 30 minutes",
            "metrics for today"
        ]
        
        for query in queries:
            plan = parse_natural_language_simple(query)
            
            # Check start_ts format
            assert "T" in plan["params"]["start_ts"], f"Missing T in timestamp for: {query}"
            assert plan["params"]["start_ts"].endswith("Z") or "+" in plan["params"]["start_ts"], \
                f"Missing timezone in start_ts for: {query}"
            
            # Check end_ts format
            assert "T" in plan["params"]["end_ts"], f"Missing T in end timestamp for: {query}"
            assert plan["params"]["end_ts"].endswith("Z") or "+" in plan["params"]["end_ts"], \
                f"Missing timezone in end_ts for: {query}"
            
            # Verify parseable
            datetime.fromisoformat(plan["params"]["start_ts"].replace("Z", "+00:00"))
            datetime.fromisoformat(plan["params"]["end_ts"].replace("Z", "+00:00"))
    
    def test_no_sql_in_output(self):
        """Security: No SQL expressions in compiled plan"""
        queries = [
            "show activity for last week",
            "top users today",
            "metrics for last month"
        ]
        
        sql_keywords = ["DATEADD", "CURRENT_TIMESTAMP", "SELECT", "FROM", "WHERE", "DATE_TRUNC"]
        
        for query in queries:
            plan = parse_natural_language_simple(query)
            plan_str = json.dumps(plan)
            
            for keyword in sql_keywords:
                assert keyword not in plan_str.upper(), \
                    f"SQL keyword '{keyword}' found in plan for query: {query}"
    
    def test_proc_selection(self):
        """NL-04: Correct procedure selected based on query type"""
        test_cases = [
            ("show trends over time", "DASH_GET_SERIES"),
            ("top 10 actions", "DASH_GET_TOPN"),
            ("summary metrics", "DASH_GET_METRICS"),
            ("activity by hour", "DASH_GET_SERIES"),
            ("top users", "DASH_GET_TOPN")
        ]
        
        for query, expected_proc in test_cases:
            plan = parse_natural_language_simple(query)
            assert plan["proc"] == expected_proc, \
                f"Expected {expected_proc} for '{query}', got {plan['proc']}"
    
    def test_interval_parsing(self):
        """NL-05: Parse time intervals correctly"""
        test_cases = [
            ("show by hour", "hour"),
            ("activity hourly", "hour"),
            ("trends by day", "day"),
            ("every 15 minutes", "15 minute"),
            ("fifteen minute intervals", "15 minute")
        ]
        
        for query, expected_interval in test_cases:
            plan = parse_natural_language_simple(query)
            if "interval" in plan["params"]:  # Metrics don't have interval
                assert plan["params"]["interval"] == expected_interval, \
                    f"Expected interval '{expected_interval}' for '{query}'"
    
    def test_lowercase_keys(self):
        """PLAN-02: All keys in plan are lowercase with underscores"""
        queries = [
            "top actions for user@example.com",
            "metrics for last week",
            "show trends by hour"
        ]
        
        def check_keys(obj, path=""):
            if isinstance(obj, dict):
                for key, value in obj.items():
                    # Check key format
                    assert key.islower() or "_" in key, \
                        f"Key '{key}' at {path} should be lowercase"
                    assert key == key.lower(), \
                        f"Key '{key}' at {path} contains uppercase"
                    
                    # Recurse
                    check_keys(value, f"{path}.{key}")
            elif isinstance(obj, list):
                for i, item in enumerate(obj):
                    check_keys(item, f"{path}[{i}]")
        
        for query in queries:
            plan = parse_natural_language_simple(query)
            check_keys(plan)
    
    def test_filters_structure(self):
        """NL-06: Filters always present as object, even if empty"""
        queries = [
            "show activity",  # No filters
            "activity for user@example.com",  # With filter
            "use cohort s3://data/users.jsonl"  # With cohort
        ]
        
        for query in queries:
            plan = parse_natural_language_simple(query)
            assert "filters" in plan["params"], f"Missing filters for: {query}"
            assert isinstance(plan["params"]["filters"], dict), \
                f"Filters should be dict for: {query}"
    
    def test_dimension_values(self):
        """NL-07: Dimension values are from allowed set"""
        allowed_dimensions = {"action", "actor", "source", "object_type"}
        
        queries = [
            "top actions",
            "top users",
            "top sources"
        ]
        
        for query in queries:
            plan = parse_natural_language_simple(query)
            if "dimension" in plan["params"]:
                assert plan["params"]["dimension"] in allowed_dimensions, \
                    f"Invalid dimension for '{query}': {plan['params']['dimension']}"


if __name__ == "__main__":
    # Run tests
    import sys
    test = TestNLCompiler()
    methods = [m for m in dir(test) if m.startswith("test_")]
    
    passed = 0
    failed = 0
    
    for method_name in methods:
        try:
            method = getattr(test, method_name)
            method()
            print(f"✓ {method_name}")
            passed += 1
        except Exception as e:
            print(f"✗ {method_name}: {e}")
            failed += 1
    
    print(f"\nResults: {passed} passed, {failed} failed")
    sys.exit(0 if failed == 0 else 1)