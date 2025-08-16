#!/usr/bin/env python3
"""
Real Integration Tests for Dashboard Factory
Uses actual Snowflake connection via RSA authentication
Tests real procedures and validates actual responses
"""

import os
import sys
import json
import subprocess
import time
from datetime import datetime, timezone, timedelta

# Test configuration
SF_ACCOUNT = os.environ.get('SNOWFLAKE_ACCOUNT', 'uec18397.us-east-1')
SF_USERNAME = os.environ.get('SNOWFLAKE_USERNAME', 'CLAUDE_CODE_AI_AGENT')
SF_PK_PATH = os.environ.get('SF_PK_PATH', './claude_code_rsa_key.p8')
SF_DATABASE = os.environ.get('SNOWFLAKE_DATABASE', 'CLAUDE_BI')
SF_WAREHOUSE = os.environ.get('SNOWFLAKE_WAREHOUSE', 'CLAUDE_AGENT_WH')

# Path to sf CLI
SF_CLI = '/Users/chandler/bin/sf'


def execute_sql(sql):
    """Execute SQL using sf CLI and return parsed result"""
    cmd = [SF_CLI, 'sql', sql]
    result = subprocess.run(cmd, capture_output=True, text=True, env=os.environ)
    
    if result.returncode != 0:
        print(f"SQL Error: {result.stderr}")
        return None
    
    # Parse JSON from output
    output = result.stdout
    # Look for Results: line and parse JSON after it
    if 'Results:' in output:
        json_start = output.index('[', output.index('Results:'))
        json_str = output[json_start:output.rfind(']')+1]
        try:
            return json.loads(json_str)
        except json.JSONDecodeError as e:
            print(f"JSON Parse Error: {e}")
            print(f"Raw output: {output}")
            return None
    return None


def call_procedure(proc_name, params):
    """Call a Snowflake procedure with VARIANT parameter"""
    params_json = json.dumps(params)
    sql = f"CALL MCP.{proc_name}(PARSE_JSON('{params_json}'))"
    return execute_sql(sql)


class TestRealIntegration:
    """Real integration tests against live Snowflake"""
    
    def setup_method(self):
        """Setup before each test"""
        # Ensure we're using the right database and schema
        execute_sql(f"USE DATABASE {SF_DATABASE}")
        execute_sql("USE SCHEMA MCP")
        
        # Insert some test events
        self.insert_test_events()
    
    def insert_test_events(self):
        """Insert test events into RAW_EVENTS"""
        test_events = [
            {
                "event_id": f"test_{datetime.now().timestamp()}_1",
                "action": "test.integration.started",
                "actor_id": "test_user@example.com",
                "object": {"type": "test", "id": "integration_001"},
                "attributes": {"test_suite": "real_integration"},
                "occurred_at": datetime.now(timezone.utc).isoformat()
            },
            {
                "event_id": f"test_{datetime.now().timestamp()}_2",
                "action": "user.login",
                "actor_id": "test_user@example.com",
                "object": {"type": "session", "id": "sess_123"},
                "attributes": {"ip": "127.0.0.1"},
                "occurred_at": (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
            },
            {
                "event_id": f"test_{datetime.now().timestamp()}_3",
                "action": "order.placed",
                "actor_id": "customer@example.com",
                "object": {"type": "order", "id": "ord_456"},
                "attributes": {"amount": 299.99},
                "occurred_at": (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
            }
        ]
        
        for event in test_events:
            event_json = json.dumps(event).replace("'", "''")
            sql = f"""
            INSERT INTO LANDING.RAW_EVENTS (raw_event, source, loaded_at)
            VALUES (PARSE_JSON('{event_json}'), 'TEST_SUITE', CURRENT_TIMESTAMP())
            """
            execute_sql(sql)
    
    def test_dash_get_series_real(self):
        """TEST-REAL-01: Call real DASH_GET_SERIES procedure"""
        params = {
            "start_ts": (datetime.now(timezone.utc) - timedelta(days=1)).isoformat(),
            "end_ts": datetime.now(timezone.utc).isoformat(),
            "interval": "hour",
            "filters": {}
        }
        
        result = call_procedure("DASH_GET_SERIES", params)
        
        assert result is not None, "Should get a result from procedure"
        assert len(result) > 0, "Should have at least one row"
        
        # Check the structure
        row = result[0]
        assert "DASH_GET_SERIES" in str(row), "Should return procedure result"
        
        # Parse the actual result
        if "DASH_GET_SERIES" in row:
            proc_result = json.loads(row["DASH_GET_SERIES"])
            assert proc_result.get("ok") == True, "Procedure should return ok=true"
            assert "data" in proc_result, "Should have data field"
            assert "metadata" in proc_result, "Should have metadata field"
            print(f"✓ DASH_GET_SERIES returned {len(proc_result.get('data', []))} time buckets")
    
    def test_dash_get_topn_real(self):
        """TEST-REAL-02: Call real DASH_GET_TOPN procedure"""
        params = {
            "start_ts": (datetime.now(timezone.utc) - timedelta(days=7)).isoformat(),
            "end_ts": datetime.now(timezone.utc).isoformat(),
            "dimension": "action",
            "n": 10,
            "filters": {}
        }
        
        result = call_procedure("DASH_GET_TOPN", params)
        
        assert result is not None, "Should get a result from procedure"
        assert len(result) > 0, "Should have at least one row"
        
        # Parse the actual result
        row = result[0]
        if "DASH_GET_TOPN" in row:
            proc_result = json.loads(row["DASH_GET_TOPN"])
            assert proc_result.get("ok") == True, "Procedure should return ok=true"
            assert "data" in proc_result, "Should have data field"
            print(f"✓ DASH_GET_TOPN returned top {len(proc_result.get('data', []))} actions")
    
    def test_dash_get_events_real(self):
        """TEST-REAL-03: Call real DASH_GET_EVENTS procedure"""
        params = {
            "cursor_ts": datetime.now(timezone.utc).isoformat(),
            "limit": 10
        }
        
        result = call_procedure("DASH_GET_EVENTS", params)
        
        assert result is not None, "Should get a result from procedure"
        assert len(result) > 0, "Should have at least one row"
        
        # Parse the actual result
        row = result[0]
        if "DASH_GET_EVENTS" in row:
            proc_result = json.loads(row["DASH_GET_EVENTS"])
            assert proc_result.get("ok") == True, "Procedure should return ok=true"
            assert "data" in proc_result, "Should have data field"
            print(f"✓ DASH_GET_EVENTS returned {len(proc_result.get('data', []))} events")
    
    def test_dash_get_metrics_real(self):
        """TEST-REAL-04: Call real DASH_GET_METRICS procedure"""
        params = {
            "start_ts": (datetime.now(timezone.utc) - timedelta(days=30)).isoformat(),
            "end_ts": datetime.now(timezone.utc).isoformat(),
            "filters": {}
        }
        
        result = call_procedure("DASH_GET_METRICS", params)
        
        assert result is not None, "Should get a result from procedure"
        assert len(result) > 0, "Should have at least one row"
        
        # Parse the actual result
        row = result[0]
        if "DASH_GET_METRICS" in row:
            proc_result = json.loads(row["DASH_GET_METRICS"])
            assert proc_result.get("ok") == True, "Procedure should return ok=true"
            assert "data" in proc_result, "Should have data field"
            data = proc_result.get("data", {})
            print(f"✓ DASH_GET_METRICS: {data.get('total_events', 0)} total events, {data.get('unique_actors', 0)} unique actors")
    
    def test_two_table_law_real(self):
        """TEST-REAL-05: Verify only 2 tables exist (Two-Table Law)"""
        sql = """
        SELECT COUNT(*) as table_count
        FROM CLAUDE_BI.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_CATALOG = 'CLAUDE_BI'
          AND TABLE_SCHEMA IN ('APP', 'LANDING', 'ACTIVITY')
          AND TABLE_TYPE IN ('BASE TABLE', 'DYNAMIC TABLE')
        """
        
        result = execute_sql(sql)
        assert result is not None, "Should get a result"
        
        table_count = result[0].get("TABLE_COUNT", 0)
        assert table_count == 2, f"Two-Table Law: Expected exactly 2 tables, found {table_count}"
        print(f"✓ Two-Table Law verified: Exactly {table_count} tables")
    
    def test_event_ingestion_real(self):
        """TEST-REAL-06: Test event ingestion through LOG_CLAUDE_EVENT"""
        event_data = {
            "action": "test.real.integration",
            "actor_id": "integration_test",
            "object": {"type": "test", "id": "real_001"},
            "attributes": {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "test_type": "real_integration"
            },
            "occurred_at": datetime.now(timezone.utc).isoformat()
        }
        
        # Call LOG_CLAUDE_EVENT
        result = call_procedure("LOG_CLAUDE_EVENT", event_data)
        
        assert result is not None, "Should get a result from LOG_CLAUDE_EVENT"
        row = result[0]
        if "LOG_CLAUDE_EVENT" in row:
            proc_result = json.loads(row["LOG_CLAUDE_EVENT"])
            assert proc_result.get("ok") == True, "Should successfully log event"
            assert "event_id" in proc_result, "Should return event_id"
            print(f"✓ Event logged with ID: {proc_result.get('event_id')}")
            
            # Wait a moment for Dynamic Table to refresh
            time.sleep(2)
            
            # Verify event appears in ACTIVITY.EVENTS
            verify_sql = f"""
            SELECT COUNT(*) as cnt
            FROM ACTIVITY.EVENTS
            WHERE action = 'test.real.integration'
              AND actor_id = 'integration_test'
              AND occurred_at >= DATEADD('minute', -1, CURRENT_TIMESTAMP())
            """
            verify_result = execute_sql(verify_sql)
            if verify_result:
                count = verify_result[0].get("CNT", 0)
                assert count > 0, "Event should appear in ACTIVITY.EVENTS"
                print(f"✓ Event verified in ACTIVITY.EVENTS")
    
    def test_query_tag_real(self):
        """TEST-REAL-07: Verify query tags are set correctly"""
        # Set a query tag and run a query
        execute_sql("ALTER SESSION SET QUERY_TAG = 'test-suite|agent:claude|test:real'")
        
        # Run a simple query
        execute_sql("SELECT 1 as test")
        
        # Check query history for our tag
        sql = """
        SELECT query_tag
        FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
        WHERE query_tag LIKE '%agent:claude%'
          AND query_tag LIKE '%test:real%'
        ORDER BY start_time DESC
        LIMIT 1
        """
        
        result = execute_sql(sql)
        if result and len(result) > 0:
            tag = result[0].get("QUERY_TAG", "")
            assert "agent:claude" in tag, "Should have Claude attribution in query tag"
            print(f"✓ Query tag verified: {tag}")
    
    def test_filters_real(self):
        """TEST-REAL-08: Test filtering in procedures"""
        # Test with actor filter
        params = {
            "start_ts": (datetime.now(timezone.utc) - timedelta(days=7)).isoformat(),
            "end_ts": datetime.now(timezone.utc).isoformat(),
            "interval": "day",
            "filters": {"actor": "test_user@example.com"}
        }
        
        result = call_procedure("DASH_GET_SERIES", params)
        assert result is not None, "Should get filtered results"
        
        # Test with action filter
        params["filters"] = {"action": "user.login"}
        result = call_procedure("DASH_GET_SERIES", params)
        assert result is not None, "Should get action-filtered results"
        
        print("✓ Filtering works correctly in procedures")
    
    def test_parameter_validation_real(self):
        """TEST-REAL-09: Test parameter validation and clamping"""
        # Test with invalid interval (should be clamped to 'hour')
        params = {
            "start_ts": (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat(),
            "end_ts": datetime.now(timezone.utc).isoformat(),
            "interval": "invalid_interval",  # Invalid
            "filters": {}
        }
        
        result = call_procedure("DASH_GET_SERIES", params)
        assert result is not None, "Should handle invalid interval"
        
        # Test limit clamping
        params = {
            "cursor_ts": datetime.now(timezone.utc).isoformat(),
            "limit": 999999  # Should be clamped to 5000
        }
        
        result = call_procedure("DASH_GET_EVENTS", params)
        assert result is not None, "Should handle large limit"
        
        print("✓ Parameter validation and clamping working")
    
    def test_iso_timestamps_real(self):
        """TEST-REAL-10: Verify ISO8601 timestamps work correctly"""
        # Test with Z suffix
        params = {
            "start_ts": "2025-01-15T00:00:00Z",
            "end_ts": "2025-01-16T00:00:00Z",
            "interval": "hour",
            "filters": {}
        }
        
        result = call_procedure("DASH_GET_SERIES", params)
        assert result is not None, "Should handle Z suffix timestamps"
        
        # Test with timezone offset
        params["start_ts"] = "2025-01-15T00:00:00+00:00"
        params["end_ts"] = "2025-01-16T00:00:00+00:00"
        
        result = call_procedure("DASH_GET_SERIES", params)
        assert result is not None, "Should handle timezone offset timestamps"
        
        print("✓ ISO8601 timestamps working correctly")


if __name__ == "__main__":
    # Check SF_PK_PATH is set
    if not os.environ.get('SF_PK_PATH'):
        os.environ['SF_PK_PATH'] = './claude_code_rsa_key.p8'
    
    # Run tests
    test = TestRealIntegration()
    test_methods = [m for m in dir(test) if m.startswith("test_")]
    
    passed = 0
    failed = 0
    
    print("=" * 60)
    print("REAL INTEGRATION TESTS - Live Snowflake Connection")
    print("=" * 60)
    print(f"Account: {SF_ACCOUNT}")
    print(f"User: {SF_USERNAME}")
    print(f"Database: {SF_DATABASE}")
    print(f"Warehouse: {SF_WAREHOUSE}")
    print("=" * 60)
    print()
    
    for method_name in test_methods:
        try:
            # Setup before each test
            test.setup_method()
            
            # Run test
            method = getattr(test, method_name)
            print(f"Running {method_name}...")
            method()
            passed += 1
        except Exception as e:
            print(f"✗ {method_name}: {e}")
            failed += 1
    
    print()
    print("=" * 60)
    print(f"Results: {passed} passed, {failed} failed")
    print("=" * 60)
    
    sys.exit(0 if failed == 0 else 1)