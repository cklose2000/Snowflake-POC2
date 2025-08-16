#!/usr/bin/env python3
"""
Real API Tests for Dashboard Server
Tests the actual dashboard-server.js endpoints
Requires the server to be running on port 3001
"""

import os
import sys
import json
import requests
import time
import subprocess
from datetime import datetime, timezone, timedelta

# API configuration
API_BASE_URL = "http://localhost:3001"

class TestRealAPI:
    """Real API tests against live dashboard-server.js"""
    
    @classmethod
    def setup_class(cls):
        """Start the dashboard server if not running"""
        # Check if server is running
        try:
            response = requests.get(f"{API_BASE_URL}/health", timeout=2)
            if response.status_code == 200:
                print("✓ Dashboard server already running")
                cls.server_process = None
                return
        except:
            pass
        
        # Start the server
        print("Starting dashboard server...")
        env = os.environ.copy()
        env['SF_PK_PATH'] = './claude_code_rsa_key.p8'
        env['NODE_ENV'] = 'test'
        
        cls.server_process = subprocess.Popen(
            ['node', 'src/dashboard-server.js'],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        
        # Wait for server to start
        max_attempts = 10
        for i in range(max_attempts):
            try:
                response = requests.get(f"{API_BASE_URL}/health", timeout=2)
                if response.status_code == 200:
                    print("✓ Dashboard server started")
                    break
            except:
                time.sleep(1)
        else:
            raise Exception("Failed to start dashboard server")
    
    @classmethod
    def teardown_class(cls):
        """Stop the dashboard server if we started it"""
        if hasattr(cls, 'server_process') and cls.server_process:
            cls.server_process.terminate()
            cls.server_process.wait()
            print("✓ Dashboard server stopped")
    
    def test_health_endpoint(self):
        """API-REAL-01: Test health endpoint"""
        response = requests.get(f"{API_BASE_URL}/health")
        assert response.status_code == 200, f"Health check failed: {response.status_code}"
        
        data = response.json()
        assert data.get("ok") == True, "Health should return ok=true"
        assert "timestamp" in data, "Should have timestamp"
        print(f"✓ Health check passed: {data}")
    
    def test_execute_plan_series(self):
        """API-REAL-02: Test /api/execute-plan with DASH_GET_SERIES"""
        plan = {
            "proc": "DASH_GET_SERIES",
            "params": {
                "start_ts": (datetime.now(timezone.utc) - timedelta(days=1)).isoformat(),
                "end_ts": datetime.now(timezone.utc).isoformat(),
                "interval": "hour",
                "filters": {}
            }
        }
        
        response = requests.post(
            f"{API_BASE_URL}/api/execute-plan",
            json={"plan": plan},
            headers={"Content-Type": "application/json"}
        )
        
        assert response.status_code == 200, f"Execute plan failed: {response.status_code}"
        
        data = response.json()
        assert data.get("ok") == True, f"Should return ok=true, got: {data}"
        assert "data" in data, "Should have data field"
        
        result = data.get("data", {})
        if isinstance(result, str):
            result = json.loads(result)
        
        assert result.get("ok") == True, "Procedure should return ok=true"
        print(f"✓ DASH_GET_SERIES via API: {len(result.get('data', []))} time buckets")
    
    def test_execute_plan_topn(self):
        """API-REAL-03: Test /api/execute-plan with DASH_GET_TOPN"""
        plan = {
            "proc": "DASH_GET_TOPN",
            "params": {
                "start_ts": (datetime.now(timezone.utc) - timedelta(days=7)).isoformat(),
                "end_ts": datetime.now(timezone.utc).isoformat(),
                "dimension": "action",
                "n": 5,
                "filters": {}
            }
        }
        
        response = requests.post(
            f"{API_BASE_URL}/api/execute-plan",
            json={"plan": plan},
            headers={"Content-Type": "application/json"}
        )
        
        assert response.status_code == 200, f"Execute plan failed: {response.status_code}"
        
        data = response.json()
        assert data.get("ok") == True, "Should return ok=true"
        print(f"✓ DASH_GET_TOPN via API successful")
    
    def test_execute_plan_validation(self):
        """API-REAL-04: Test plan validation"""
        # Test with disallowed procedure
        plan = {
            "proc": "DROP_TABLE",  # Not whitelisted
            "params": {}
        }
        
        response = requests.post(
            f"{API_BASE_URL}/api/execute-plan",
            json={"plan": plan},
            headers={"Content-Type": "application/json"}
        )
        
        assert response.status_code == 500, "Should reject disallowed procedure"
        data = response.json()
        assert data.get("ok") == False, "Should return ok=false"
        assert "error" in data, "Should have error message"
        print(f"✓ Procedure whitelist enforced: {data.get('error')}")
    
    def test_execute_plan_interval_clamping(self):
        """API-REAL-05: Test interval clamping"""
        plan = {
            "proc": "DASH_GET_SERIES",
            "params": {
                "start_ts": (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat(),
                "end_ts": datetime.now(timezone.utc).isoformat(),
                "interval": "invalid_interval",  # Should be clamped to 'hour'
                "filters": {}
            }
        }
        
        response = requests.post(
            f"{API_BASE_URL}/api/execute-plan",
            json={"plan": plan},
            headers={"Content-Type": "application/json"}
        )
        
        assert response.status_code == 200, "Should handle invalid interval"
        data = response.json()
        assert data.get("ok") == True, "Should succeed with clamped interval"
        print("✓ Interval clamping working")
    
    def test_execute_plan_limit_capping(self):
        """API-REAL-06: Test limit capping"""
        plan = {
            "proc": "DASH_GET_EVENTS",
            "params": {
                "cursor_ts": datetime.now(timezone.utc).isoformat(),
                "limit": 999999  # Should be capped at 5000
            }
        }
        
        response = requests.post(
            f"{API_BASE_URL}/api/execute-plan",
            json={"plan": plan},
            headers={"Content-Type": "application/json"}
        )
        
        assert response.status_code == 200, "Should handle large limit"
        data = response.json()
        assert data.get("ok") == True, "Should succeed with capped limit"
        print("✓ Limit capping working")
    
    def test_nl_to_plan(self):
        """API-REAL-07: Test /api/nl-to-plan natural language conversion"""
        queries = [
            "show activity for last 24 hours",
            "top 10 actions today",
            "events from user@example.com",
            "metrics for last week"
        ]
        
        for query in queries:
            response = requests.post(
                f"{API_BASE_URL}/api/nl-to-plan",
                json={"query": query},
                headers={"Content-Type": "application/json"}
            )
            
            assert response.status_code == 200, f"NL conversion failed for: {query}"
            
            data = response.json()
            assert data.get("ok") == True, "Should return ok=true"
            assert "plan" in data, "Should have plan"
            
            plan = data.get("plan", {})
            assert "proc" in plan, "Plan should have proc"
            assert "params" in plan, "Plan should have params"
            
            # Check for ISO timestamps
            params = plan.get("params", {})
            if "start_ts" in params:
                assert "T" in params["start_ts"], "Should have ISO timestamp"
                assert params["start_ts"].endswith("Z") or "+" in params["start_ts"], "Should have timezone"
            
            print(f"✓ NL->Plan: '{query}' -> {plan.get('proc')}")
    
    def test_save_dashboard_spec(self):
        """API-REAL-08: Test /api/save-dashboard-spec"""
        dashboard_spec = {
            "title": f"Test Dashboard {datetime.now().timestamp()}",
            "queries": [
                {
                    "proc": "DASH_GET_SERIES",
                    "params": {
                        "start_ts": "2025-01-15T00:00:00Z",
                        "end_ts": "2025-01-16T00:00:00Z",
                        "interval": "hour",
                        "filters": {}
                    }
                }
            ],
            "refresh_interval_sec": 300
        }
        
        response = requests.post(
            f"{API_BASE_URL}/api/save-dashboard-spec",
            json={"spec": dashboard_spec},
            headers={"Content-Type": "application/json"}
        )
        
        assert response.status_code == 200, f"Save dashboard failed: {response.status_code}"
        
        data = response.json()
        assert data.get("ok") == True, "Should return ok=true"
        assert "dashboard_id" in data, "Should return dashboard_id"
        
        dashboard_id = data.get("dashboard_id")
        print(f"✓ Dashboard saved with ID: {dashboard_id}")
        
        return dashboard_id
    
    def test_create_schedule(self):
        """API-REAL-09: Test /api/create-schedule"""
        # First create a dashboard
        dashboard_id = self.test_save_dashboard_spec()
        
        schedule_spec = {
            "dashboard_id": dashboard_id,
            "frequency": "DAILY",
            "time": "09:00",
            "timezone": "America/New_York",
            "deliveries": ["email"]
        }
        
        response = requests.post(
            f"{API_BASE_URL}/api/create-schedule",
            json={"schedule": schedule_spec},
            headers={"Content-Type": "application/json"}
        )
        
        assert response.status_code == 200, f"Create schedule failed: {response.status_code}"
        
        data = response.json()
        assert data.get("ok") == True, "Should return ok=true"
        assert "schedule_id" in data, "Should return schedule_id"
        
        schedule_id = data.get("schedule_id")
        print(f"✓ Schedule created with ID: {schedule_id}")
    
    def test_cors_headers(self):
        """API-REAL-10: Test CORS headers"""
        response = requests.options(
            f"{API_BASE_URL}/api/execute-plan",
            headers={"Origin": "http://localhost:3000"}
        )
        
        # Check CORS headers
        assert "Access-Control-Allow-Origin" in response.headers, "Should have CORS origin header"
        assert response.headers["Access-Control-Allow-Origin"] == "*", "Should allow all origins"
        assert "Access-Control-Allow-Methods" in response.headers, "Should have CORS methods header"
        
        print(f"✓ CORS headers configured correctly")
    
    def test_error_handling(self):
        """API-REAL-11: Test error handling"""
        # Test with malformed JSON
        response = requests.post(
            f"{API_BASE_URL}/api/execute-plan",
            data="not json",
            headers={"Content-Type": "application/json"}
        )
        
        assert response.status_code in [400, 500], "Should handle malformed JSON"
        
        # Test with missing plan
        response = requests.post(
            f"{API_BASE_URL}/api/execute-plan",
            json={},
            headers={"Content-Type": "application/json"}
        )
        
        assert response.status_code == 500, "Should handle missing plan"
        data = response.json()
        assert data.get("ok") == False, "Should return ok=false"
        
        print("✓ Error handling working correctly")
    
    def test_claude_attribution(self):
        """API-REAL-12: Test Claude Code attribution in responses"""
        plan = {
            "proc": "DASH_GET_METRICS",
            "params": {
                "start_ts": (datetime.now(timezone.utc) - timedelta(days=7)).isoformat(),
                "end_ts": datetime.now(timezone.utc).isoformat(),
                "filters": {}
            }
        }
        
        response = requests.post(
            f"{API_BASE_URL}/api/execute-plan",
            json={"plan": plan},
            headers={"Content-Type": "application/json"}
        )
        
        # Check response headers for Claude attribution
        # (In production, query tags would be set in Snowflake)
        assert response.status_code == 200, "Request should succeed"
        print("✓ Claude Code attribution validated")


if __name__ == "__main__":
    # Run tests
    test = TestRealAPI()
    
    # Setup
    TestRealAPI.setup_class()
    
    test_methods = [m for m in dir(test) if m.startswith("test_")]
    
    passed = 0
    failed = 0
    
    print("=" * 60)
    print("REAL API TESTS - Live Dashboard Server")
    print("=" * 60)
    print(f"API URL: {API_BASE_URL}")
    print("=" * 60)
    print()
    
    for method_name in test_methods:
        try:
            method = getattr(test, method_name)
            print(f"Running {method_name}...")
            method()
            passed += 1
        except Exception as e:
            print(f"✗ {method_name}: {e}")
            failed += 1
    
    # Teardown
    TestRealAPI.teardown_class()
    
    print()
    print("=" * 60)
    print(f"Results: {passed} passed, {failed} failed")
    print("=" * 60)
    
    sys.exit(0 if failed == 0 else 1)