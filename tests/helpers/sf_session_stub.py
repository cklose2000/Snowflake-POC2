"""
Mock Snowpark Session for testing
Captures SQL statements and bound parameters
"""

import json
import pandas as pd
from typing import List, Dict, Any, Optional


class MockSession:
    """Mock Snowpark Session that captures SQL and parameters"""
    
    def __init__(self):
        self.last_sql = None
        self.bound_params = None
        self.sql_history = []
        self.query_tag = None
        self.mock_result = None
        
    def sql(self, stmt: str):
        """Capture SQL statement"""
        self.last_sql = stmt
        self.sql_history.append(stmt)
        
        # Handle ALTER SESSION for query tag
        if "ALTER SESSION SET QUERY_TAG" in stmt:
            import re
            match = re.search(r"QUERY_TAG\s*=\s*'([^']*)'", stmt)
            if match:
                self.query_tag = match.group(1)
        
        return self
    
    def bind(self, params: List[str]):
        """Capture bound parameters"""
        assert len(params) == 1, f"Expected single JSON parameter, got {len(params)}"
        self.bound_params = params[0]
        
        # Validate it's valid JSON
        try:
            json.loads(self.bound_params)
        except json.JSONDecodeError:
            raise ValueError(f"Bound parameter is not valid JSON: {self.bound_params}")
        
        return self
    
    def collect(self):
        """Mock collect() for ALTER SESSION etc"""
        return []
    
    def to_pandas(self):
        """Return mock DataFrame result"""
        if self.mock_result is not None:
            return self.mock_result
            
        # Default mock result for procedure calls
        return pd.DataFrame([{
            "RESULT": json.dumps({
                "ok": True,
                "data": [
                    {"actor": "user1", "count": 100},
                    {"actor": "user2", "count": 50}
                ]
            })
        }])
    
    def set_mock_result(self, result: pd.DataFrame):
        """Set a specific mock result for the next query"""
        self.mock_result = result
    
    def assert_call_pattern(self, expected_proc: str):
        """Assert the SQL follows the correct CALL pattern"""
        assert self.last_sql is not None, "No SQL statement captured"
        assert f"CALL MCP.{expected_proc}(PARSE_JSON(?))" in self.last_sql, \
            f"Expected CALL MCP.{expected_proc}(PARSE_JSON(?)), got: {self.last_sql}"
        
        # Ensure no SELECT wrapper
        assert not self.last_sql.strip().startswith("SELECT"), \
            f"SQL should not start with SELECT: {self.last_sql}"
        
        # Ensure no named parameters
        assert "=>" not in self.last_sql, \
            f"SQL should not contain named parameters (=>): {self.last_sql}"
    
    def assert_query_tag(self, expected_tag_part: str):
        """Assert query tag contains expected part"""
        assert self.query_tag is not None, "No query tag set"
        assert expected_tag_part in self.query_tag, \
            f"Expected '{expected_tag_part}' in query tag, got: {self.query_tag}"
    
    def get_bound_params_json(self) -> Dict[str, Any]:
        """Get bound parameters as parsed JSON"""
        assert self.bound_params is not None, "No parameters bound"
        return json.loads(self.bound_params)
    
    def reset(self):
        """Reset the mock for next test"""
        self.last_sql = None
        self.bound_params = None
        self.sql_history = []
        self.query_tag = None
        self.mock_result = None


class MockSessionBuilder:
    """Mock for Session.builder pattern"""
    
    @staticmethod
    def getOrCreate():
        return MockSession()


# Export a mock module-like interface
class MockSnowparkContext:
    """Mock for get_active_session"""
    
    def __init__(self):
        self.session = MockSession()
    
    def get_active_session(self):
        return self.session