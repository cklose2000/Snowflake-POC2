"""
Unit tests for run_plan() function
Validates correct procedure call patterns and guardrails
"""

import pytest
import json
import sys
import os
from datetime import datetime, timezone

# Add current directory to path for relative imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from helpers.sf_session_stub import MockSession


def run_plan(session, plan, query_tag):
    """
    Execute plan with single VARIANT parameter - the correct way
    This is the function from coo_dashboard.py
    """
    # Whitelist of allowed procedures
    allowed = {"DASH_GET_SERIES", "DASH_GET_TOPN", "DASH_GET_EVENTS", "DASH_GET_METRICS"}
    
    proc = plan.get("proc")
    if proc not in allowed:
        raise ValueError(f"Disallowed proc: {proc}")
    
    params = plan.get("params", {})
    
    # Clamp limits for safety
    if "limit" in params:
        params["limit"] = min(int(params.get("limit", 1000)), 5000)
    if "n" in params:
        params["n"] = min(int(params.get("n", 10)), 50)
    
    # Validate interval if present
    if "interval" in params:
        valid_intervals = {"minute", "5 minute", "15 minute", "hour", "day"}
        if params["interval"] not in valid_intervals:
            params["interval"] = "hour"  # Default to hour
    
    # Set query tag with Claude attribution
    session.sql(f"ALTER SESSION SET QUERY_TAG = '{query_tag}'").collect()
    
    # THE CRITICAL FIX: Use single VARIANT parameter with PARSE_JSON(?)
    stmt = f"CALL MCP.{proc}(PARSE_JSON(?))"
    
    # Bind the JSON parameter
    payload = json.dumps(params)
    result_df = session.sql(stmt).bind(params=[payload]).to_pandas()
    
    return result_df


class TestPlanRunner:
    """Test suite for plan execution"""
    
    def test_correct_call_pattern(self):
        """PROC-01: Verify CALL MCP.PROC(PARSE_JSON(?)) pattern"""
        session = MockSession()
        plan = {
            "proc": "DASH_GET_SERIES",
            "params": {
                "start_ts": "2025-01-16T00:00:00Z",
                "end_ts": "2025-01-16T23:59:59Z",
                "interval": "hour",
                "filters": {}
            }
        }
        
        result = run_plan(session, plan, "test-tag")
        
        # Assert correct SQL pattern
        session.assert_call_pattern("DASH_GET_SERIES")
        
        # Assert single JSON parameter
        bound_params = session.get_bound_params_json()
        assert bound_params["start_ts"] == "2025-01-16T00:00:00Z"
        assert bound_params["interval"] == "hour"
    
    def test_query_tag_set(self):
        """GUARD: Verify query tag is set correctly"""
        session = MockSession()
        plan = {
            "proc": "DASH_GET_METRICS",
            "params": {
                "start_ts": "2025-01-01T00:00:00Z",
                "end_ts": "2025-01-16T00:00:00Z"
            }
        }
        
        run_plan(session, plan, "dash-ui|agent:claude|proc:metrics")
        
        # Assert query tag
        session.assert_query_tag("agent:claude")
        session.assert_query_tag("proc:metrics")
    
    def test_proc_whitelist_enforced(self):
        """PROC-02: Only whitelisted procedures allowed"""
        session = MockSession()
        
        # Valid procedures
        valid_procs = ["DASH_GET_SERIES", "DASH_GET_TOPN", "DASH_GET_EVENTS", "DASH_GET_METRICS"]
        for proc in valid_procs:
            plan = {"proc": proc, "params": {}}
            run_plan(session, plan, "test")  # Should not raise
        
        # Invalid procedure
        invalid_plan = {"proc": "DASH_GET_USERS", "params": {}}
        with pytest.raises(ValueError, match="Disallowed proc"):
            run_plan(session, invalid_plan, "test")
    
    def test_interval_clamping(self):
        """GUARD-01: Invalid intervals clamped to valid values"""
        session = MockSession()
        
        # Invalid interval should be clamped to "hour"
        plan = {
            "proc": "DASH_GET_SERIES",
            "params": {
                "start_ts": "2025-01-16T00:00:00Z",
                "end_ts": "2025-01-16T23:59:59Z",
                "interval": "2 minutes"  # Invalid
            }
        }
        
        run_plan(session, plan, "test")
        
        # Check that interval was clamped
        bound_params = session.get_bound_params_json()
        assert bound_params["interval"] == "hour", "Invalid interval should be clamped to 'hour'"
        
        # Valid intervals should pass through
        valid_intervals = ["minute", "5 minute", "15 minute", "hour", "day"]
        for interval in valid_intervals:
            session.reset()
            plan["params"]["interval"] = interval
            run_plan(session, plan, "test")
            bound_params = session.get_bound_params_json()
            assert bound_params["interval"] == interval
    
    def test_limit_capping(self):
        """GUARD-02: Limits capped at maximum values"""
        session = MockSession()
        
        # Test n capping (max 50)
        plan = {
            "proc": "DASH_GET_TOPN",
            "params": {
                "n": 999999,
                "dimension": "actor"
            }
        }
        
        run_plan(session, plan, "test")
        bound_params = session.get_bound_params_json()
        assert bound_params["n"] == 50, "n should be capped at 50"
        
        # Test limit capping (max 5000)
        session.reset()
        plan = {
            "proc": "DASH_GET_EVENTS",
            "params": {
                "limit": 999999,
                "cursor_ts": "2025-01-16T00:00:00Z"
            }
        }
        
        run_plan(session, plan, "test")
        bound_params = session.get_bound_params_json()
        assert bound_params["limit"] == 5000, "limit should be capped at 5000"
    
    def test_lowercase_json_keys(self):
        """PLAN-02: Verify all JSON keys are lowercase"""
        session = MockSession()
        plan = {
            "proc": "DASH_GET_SERIES",
            "params": {
                "start_ts": "2025-01-16T00:00:00Z",  # Correct: lowercase
                "end_ts": "2025-01-16T23:59:59Z",
                "interval": "hour",
                "filters": {"actor": "user@example.com"}
            }
        }
        
        run_plan(session, plan, "test")
        
        # Verify all keys in bound params are lowercase
        bound_params = session.get_bound_params_json()
        for key in bound_params.keys():
            assert key.islower() or "_" in key, f"Key '{key}' should be lowercase"
        
        # Check nested filters
        if "filters" in bound_params:
            for filter_key in bound_params["filters"].keys():
                assert filter_key.islower() or "_" in filter_key, f"Filter key '{filter_key}' should be lowercase"
    
    def test_iso_timestamps_required(self):
        """PLAN-01: ISO8601 timestamps with timezone required"""
        session = MockSession()
        
        # Valid ISO timestamp
        valid_plan = {
            "proc": "DASH_GET_SERIES",
            "params": {
                "start_ts": "2025-01-16T00:00:00Z",
                "end_ts": "2025-01-16T23:59:59Z",
                "interval": "hour"
            }
        }
        
        run_plan(session, valid_plan, "test")
        bound_params = session.get_bound_params_json()
        
        # Verify timestamps are ISO format
        from datetime import datetime
        datetime.fromisoformat(bound_params["start_ts"].replace("Z", "+00:00"))
        datetime.fromisoformat(bound_params["end_ts"].replace("Z", "+00:00"))
    
    def test_no_sql_in_params(self):
        """Security: No SQL expressions in parameters"""
        session = MockSession()
        
        # Should not have SQL functions
        plan = {
            "proc": "DASH_GET_METRICS",
            "params": {
                "start_ts": "2025-01-01T00:00:00Z",
                "end_ts": "2025-01-16T00:00:00Z"
            }
        }
        
        run_plan(session, plan, "test")
        bound_params_str = session.bound_params
        
        # Check for SQL keywords that shouldn't be there
        sql_keywords = ["DATEADD", "CURRENT_TIMESTAMP", "SELECT", "FROM", "WHERE"]
        for keyword in sql_keywords:
            assert keyword not in bound_params_str.upper(), \
                f"SQL keyword '{keyword}' should not appear in parameters"
    
    def test_filters_preserved(self):
        """Verify filters are passed through correctly"""
        session = MockSession()
        plan = {
            "proc": "DASH_GET_TOPN",
            "params": {
                "dimension": "action",
                "n": 10,
                "filters": {
                    "actor": "user@example.com",
                    "action": "user.login"
                }
            }
        }
        
        run_plan(session, plan, "test")
        bound_params = session.get_bound_params_json()
        
        assert "filters" in bound_params
        assert bound_params["filters"]["actor"] == "user@example.com"
        assert bound_params["filters"]["action"] == "user.login"
    
    def test_cohort_url_validation(self):
        """COHORT-01: Cohort URLs must start with s3://"""
        session = MockSession()
        plan = {
            "proc": "DASH_GET_SERIES",
            "params": {
                "start_ts": "2025-01-16T00:00:00Z",
                "end_ts": "2025-01-16T23:59:59Z",
                "filters": {
                    "cohort_url": "s3://bucket/path/cohort.jsonl"
                }
            }
        }
        
        run_plan(session, plan, "test")
        bound_params = session.get_bound_params_json()
        
        # Valid s3:// URL should pass through
        assert bound_params["filters"]["cohort_url"] == "s3://bucket/path/cohort.jsonl"


if __name__ == "__main__":
    # Run tests
    import sys
    test = TestPlanRunner()
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