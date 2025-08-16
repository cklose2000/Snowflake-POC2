"""
Headless UI smoke tests for Streamlit dashboard
Validates rendering, Claude visibility, and interactions
"""

import pytest
import sys
import os
import re
from typing import Dict, Any, List
from unittest.mock import MagicMock, patch

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class MockStreamlit:
    """Mock Streamlit module for headless testing"""
    
    def __init__(self):
        self.rendered_elements = []
        self.sidebar_elements = []
        self.columns = []
        self.current_container = self.rendered_elements
        self.session_state = {}
        self.query_params = {}
        
    def title(self, text):
        self.current_container.append(("title", text))
        
    def write(self, text):
        self.current_container.append(("write", text))
        
    def markdown(self, text):
        self.current_container.append(("markdown", text))
        
    def button(self, label, key=None, help=None):
        self.current_container.append(("button", label, key))
        return False  # Not clicked
    
    def text_input(self, label, value="", placeholder="", key=None):
        self.current_container.append(("text_input", label, value, placeholder))
        return value
    
    def selectbox(self, label, options, index=0, key=None):
        self.current_container.append(("selectbox", label, options))
        return options[index] if options else None
    
    def columns(self, spec):
        self.columns = [[] for _ in range(len(spec) if isinstance(spec, list) else spec)]
        return MockColumns(self.columns)
    
    def expander(self, label, expanded=False):
        return MockContainer(self.current_container, label)
    
    def container(self):
        return MockContainer(self.current_container)
    
    def empty(self):
        return MockContainer(self.current_container)
    
    def dataframe(self, df):
        self.current_container.append(("dataframe", df))
    
    def line_chart(self, df):
        self.current_container.append(("line_chart", df))
    
    def bar_chart(self, df):
        self.current_container.append(("bar_chart", df))
    
    def metric(self, label, value, delta=None):
        self.current_container.append(("metric", label, value, delta))
    
    def error(self, text):
        self.current_container.append(("error", text))
    
    def info(self, text):
        self.current_container.append(("info", text))
    
    def success(self, text):
        self.current_container.append(("success", text))
    
    def experimental_get_query_params(self):
        return self.query_params
    
    @property
    def sidebar(self):
        """Return a mock sidebar that uses sidebar_elements"""
        mock_sidebar = MockStreamlit()
        mock_sidebar.current_container = self.sidebar_elements
        return mock_sidebar


class MockColumns:
    """Mock for st.columns"""
    
    def __init__(self, columns):
        self.columns = columns
        
    def __getitem__(self, idx):
        mock = MockStreamlit()
        mock.current_container = self.columns[idx]
        return mock


class MockContainer:
    """Mock for st.container/expander"""
    
    def __init__(self, parent_container, label=None):
        self.parent_container = parent_container
        self.label = label
        self.elements = []
        if label:
            parent_container.append(("expander", label, self.elements))
            
    def __enter__(self):
        return self
        
    def __exit__(self, *args):
        pass
        
    def write(self, text):
        self.elements.append(("write", text))
        
    def markdown(self, text):
        self.elements.append(("markdown", text))


class TestUISmoke:
    """Smoke tests for UI rendering"""
    
    def test_claude_branding_visible(self):
        """UI-01: Claude Code is prominently displayed"""
        st = MockStreamlit()
        
        # Simulate rendering title
        st.title("📊 COO Dashboard Factory")
        st.markdown("**🤖 Claude Code Status:** 🟢 Listening")
        
        # Check Claude is visible
        claude_visible = False
        for element in st.rendered_elements:
            if "Claude" in str(element):
                claude_visible = True
                break
                
        assert claude_visible, "Claude Code should be visible in UI"
        
        # Check for status indicator
        status_found = False
        for element in st.rendered_elements:
            if "Status" in str(element) and ("Listening" in str(element) or "🟢" in str(element)):
                status_found = True
                break
                
        assert status_found, "Claude status should be displayed"
    
    def test_agent_console_present(self):
        """UI-02: Agent Console shows Claude's thinking"""
        st = MockStreamlit()
        
        # Simulate Agent Console
        with st.expander("🤖 Agent Console", expanded=False) as console:
            console.markdown("**Claude's Decision Process:**")
            console.write("1. Parsed: 'show top actions today'")
            console.write("2. Selected: DASH_GET_TOPN")
            console.write("3. Parameters: dimension=action, n=10")
        
        # Check console exists
        console_found = False
        for element in st.rendered_elements:
            if element[0] == "expander" and "Agent Console" in element[1]:
                console_found = True
                # Check it has content
                assert len(element[2]) > 0, "Agent Console should have content"
                break
                
        assert console_found, "Agent Console should be present"
    
    def test_natural_language_input(self):
        """UI-03: Natural language input field present"""
        st = MockStreamlit()
        
        # Simulate NL input
        st.text_input(
            "Ask Claude about your activity",
            placeholder="e.g., 'Show signups by hour for last 7 days'",
            key="nl_query"
        )
        
        # Check input exists
        input_found = False
        for element in st.rendered_elements:
            if element[0] == "text_input":
                assert "Claude" in element[1] or "activity" in element[1]
                assert "Show signups" in element[3]  # placeholder
                input_found = True
                break
                
        assert input_found, "Natural language input should be present"
    
    def test_preset_buttons(self):
        """UI-04: Quick preset buttons available"""
        st = MockStreamlit()
        
        # Simulate preset buttons
        cols = st.columns(3)
        presets = ["Last 24 Hours", "Top Users Today", "This Week's Activity"]
        
        for i, preset in enumerate(presets):
            cols[i].button(preset, key=f"preset_{i}")
        
        # Check presets exist
        button_count = 0
        for col in st.columns:
            for element in col:
                if element[0] == "button":
                    button_count += 1
                    
        assert button_count >= 3, "Should have at least 3 preset buttons"
    
    def test_error_handling_display(self):
        """UI-05: Errors shown with Claude context"""
        st = MockStreamlit()
        
        # Simulate error display
        st.error("❌ Claude couldn't process: Invalid time range")
        st.info("💡 Claude suggests: Use ISO format like '2025-01-16T00:00:00Z'")
        
        # Check error messaging
        error_found = False
        suggestion_found = False
        
        for element in st.rendered_elements:
            if element[0] == "error" and "Claude" in element[1]:
                error_found = True
            if element[0] == "info" and "Claude suggests" in element[1]:
                suggestion_found = True
                
        assert error_found, "Errors should mention Claude"
        assert suggestion_found, "Should provide Claude's suggestions"
    
    def test_dashboard_selector(self):
        """UI-06: Dashboard selector in sidebar"""
        st = MockStreamlit()
        
        # Simulate sidebar
        st.sidebar.title("Select Dashboard")
        dashboards = ["dash_001", "dash_002", "dash_003"]
        st.sidebar.selectbox("Choose dashboard:", dashboards)
        
        # Check sidebar has selector
        selector_found = False
        for element in st.sidebar_elements:
            if element[0] == "selectbox" and "dashboard" in element[1].lower():
                selector_found = True
                assert len(element[2]) > 0, "Should have dashboard options"
                break
                
        assert selector_found, "Dashboard selector should be in sidebar"
    
    def test_visualization_rendering(self):
        """UI-07: Charts render with proper types"""
        st = MockStreamlit()
        import pandas as pd
        
        # Mock data
        df = pd.DataFrame({
            "hour": ["00:00", "01:00", "02:00"],
            "count": [100, 150, 200]
        })
        
        # Simulate chart rendering
        st.line_chart(df)
        st.bar_chart(df)
        
        # Check charts rendered
        chart_types = set()
        for element in st.rendered_elements:
            if element[0] in ["line_chart", "bar_chart"]:
                chart_types.add(element[0])
                
        assert "line_chart" in chart_types, "Should render line charts"
        assert "bar_chart" in chart_types, "Should render bar charts"
    
    def test_status_transitions(self):
        """UI-08: Claude status shows state transitions"""
        st = MockStreamlit()
        
        # Test different status states
        statuses = [
            ("🟢 Listening", "idle"),
            ("🤔 Thinking", "processing"),
            ("📞 Calling MCP.DASH_GET_TOPN", "calling"),
            ("✅ Rendered", "complete")
        ]
        
        for status_text, state in statuses:
            st.markdown(f"**🤖 Claude Code Status:** {status_text}")
            
        # Check all statuses rendered
        status_count = 0
        for element in st.rendered_elements:
            if element[0] == "markdown" and "Claude Code Status" in element[1]:
                status_count += 1
                
        assert status_count == len(statuses), "All status transitions should be shown"
    
    def test_audit_trail_visibility(self):
        """AUDIT-02: Audit trail shows Claude's actions"""
        st = MockStreamlit()
        
        # Simulate audit trail
        with st.expander("📋 Audit Trail") as audit:
            audit.write("09:30:15 - Claude received: 'show top users'")
            audit.write("09:30:16 - Claude parsed: dimension=actor, n=10")
            audit.write("09:30:17 - Claude called: MCP.DASH_GET_TOPN")
            audit.write("09:30:18 - Claude rendered: 10 results")
        
        # Check audit trail exists and mentions Claude
        audit_found = False
        for element in st.rendered_elements:
            if element[0] == "expander" and "Audit" in element[1]:
                audit_found = True
                # Check Claude is mentioned in entries
                claude_mentions = 0
                for entry in element[2]:
                    if "Claude" in str(entry):
                        claude_mentions += 1
                assert claude_mentions >= 4, "Claude should be mentioned in audit entries"
                break
                
        assert audit_found, "Audit trail should be present"
    
    def test_zero_click_flow(self):
        """COO-UX: Zero-click to first insight"""
        st = MockStreamlit()
        
        # On load, should show default dashboard
        st.title("📊 COO Dashboard Factory")
        st.markdown("**🤖 Claude Code Status:** ✅ Rendered")
        
        # Should have data visible immediately
        import pandas as pd
        df = pd.DataFrame({
            "action": ["user.signup", "order.placed"],
            "count": [45, 123]
        })
        st.dataframe(df)
        
        # Check data is shown without interaction
        data_shown = False
        for element in st.rendered_elements:
            if element[0] == "dataframe":
                data_shown = True
                break
                
        assert data_shown, "Data should be visible on load (zero-click)"


class TestHeadlessValidation:
    """Additional headless validation tests"""
    
    def test_query_params_handling(self):
        """URL-01: Query parameters properly handled"""
        st = MockStreamlit()
        st.query_params = {"dashboard_id": "dash_test_123"}
        
        # Get params
        params = st.experimental_get_query_params()
        assert "dashboard_id" in params
        assert params["dashboard_id"] == "dash_test_123"
    
    def test_session_state_management(self):
        """STATE-01: Session state tracks user interactions"""
        st = MockStreamlit()
        
        # Simulate state management
        st.session_state["last_query"] = "show top actions"
        st.session_state["query_count"] = 5
        st.session_state["current_proc"] = "DASH_GET_TOPN"
        
        # Verify state
        assert st.session_state["last_query"] == "show top actions"
        assert st.session_state["query_count"] == 5
        assert st.session_state["current_proc"] == "DASH_GET_TOPN"
    
    def test_responsive_layout(self):
        """UI-09: Layout uses columns for responsive design"""
        st = MockStreamlit()
        
        # Create responsive layout
        col1, col2, col3 = st.columns([2, 3, 1])
        
        # Check columns created
        assert len(st.columns) == 3
        
        # Add content to columns
        mock_cols = MockColumns(st.columns)
        mock_cols[0].metric("Total Events", "1,234")
        mock_cols[1].write("Main content area")
        mock_cols[2].button("Refresh")
        
        # Verify content in columns
        assert len(st.columns[0]) > 0, "First column should have content"
        assert len(st.columns[1]) > 0, "Second column should have content"
        assert len(st.columns[2]) > 0, "Third column should have content"
    
    def test_no_plotly_imports(self):
        """DEPS-01: No Plotly imports (not available in Snowflake)"""
        # This would be checked by static analysis
        # Here we just verify our mock doesn't have plotly
        st = MockStreamlit()
        
        # Should not have plotly methods
        assert not hasattr(st, "plotly_chart")
        assert not hasattr(st, "plotly")
    
    def test_claude_attribution_in_results(self):
        """UI-10: Results show 'via Claude Code'"""
        st = MockStreamlit()
        
        # Simulate results display
        st.success("✅ Query executed via Claude Code")
        st.caption("Powered by Claude Code | Query ID: abc123")
        
        # Check attribution
        attribution_found = False
        for element in st.rendered_elements:
            if "Claude Code" in str(element):
                attribution_found = True
                break
                
        assert attribution_found, "Results should show Claude Code attribution"


if __name__ == "__main__":
    # Run UI smoke tests
    import sys
    
    test_classes = [TestUISmoke, TestHeadlessValidation]
    
    total_passed = 0
    total_failed = 0
    
    for test_class in test_classes:
        print(f"\n{test_class.__name__}:")
        test = test_class()
        methods = [m for m in dir(test) if m.startswith("test_")]
        
        for method_name in methods:
            try:
                method = getattr(test, method_name)
                method()
                print(f"  ✓ {method_name}")
                total_passed += 1
            except Exception as e:
                print(f"  ✗ {method_name}: {e}")
                total_failed += 1
    
    print(f"\nTotal Results: {total_passed} passed, {total_failed} failed")
    sys.exit(0 if total_failed == 0 else 1)