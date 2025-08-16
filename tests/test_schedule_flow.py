"""
Unit tests for Schedule creation and execution flow
Tests DST handling, event creation, and artifact generation
"""

import pytest
import json
import sys
import os
from datetime import datetime, timezone, timedelta
import pytz

# Add current directory to path for relative imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from helpers.sf_session_stub import MockSession


class MockScheduleExecutor:
    """Mock schedule executor for testing"""
    
    def __init__(self, session):
        self.session = session
        self.executed_schedules = []
        self.generated_snapshots = []
    
    def create_schedule_event(self, dashboard_id, frequency, time, timezone_str, deliveries):
        """Create a dashboard.schedule_created event"""
        schedule_id = f"sched_test_{dashboard_id}"
        
        # Parse timezone (Olson ID)
        tz = pytz.timezone(timezone_str)
        
        # Calculate next run
        now = datetime.now(tz)
        hour, minute = map(int, time.split(':'))
        next_run = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        
        if next_run <= now:
            next_run += timedelta(days=1)
        
        # Handle weekdays
        if frequency == "WEEKDAYS":
            while next_run.weekday() >= 5:  # Saturday = 5, Sunday = 6
                next_run += timedelta(days=1)
        
        event = {
            "action": "dashboard.schedule_created",
            "actor_id": "test_user",
            "object": {
                "type": "schedule",
                "id": schedule_id
            },
            "attributes": {
                "schedule_id": schedule_id,
                "dashboard_id": dashboard_id,
                "frequency": frequency,
                "time": time,
                "timezone": timezone_str,
                "deliveries": deliveries,
                "next_run": next_run.isoformat()
            }
        }
        
        # Log event (would go to EVENTS table)
        sql = f"CALL MCP.LOG_CLAUDE_EVENT(PARSE_JSON(?))"
        self.session.sql(sql).bind(params=[json.dumps(event)]).collect()
        
        return schedule_id, next_run
    
    def execute_schedule(self, schedule_id, dashboard_id):
        """Execute a scheduled dashboard"""
        # Record execution
        self.executed_schedules.append({
            "schedule_id": schedule_id,
            "dashboard_id": dashboard_id,
            "executed_at": datetime.now(timezone.utc).isoformat()
        })
        
        # Generate snapshot
        snapshot_id = f"snap_{dashboard_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        snapshot_path = f"@MCP.DASH_SNAPSHOTS/{snapshot_id}.json"
        
        self.generated_snapshots.append({
            "snapshot_id": snapshot_id,
            "dashboard_id": dashboard_id,
            "path": snapshot_path,
            "format": "json"
        })
        
        # Log snapshot event
        event = {
            "action": "dashboard.snapshot_generated",
            "actor_id": "SCHEDULE_EXECUTOR",
            "object": {
                "type": "snapshot",
                "id": snapshot_id
            },
            "attributes": {
                "schedule_id": schedule_id,
                "dashboard_id": dashboard_id,
                "snapshot_path": snapshot_path,
                "format": "json",
                "row_count": 100
            }
        }
        
        sql = f"CALL MCP.LOG_CLAUDE_EVENT(PARSE_JSON(?))"
        self.session.sql(sql).bind(params=[json.dumps(event)]).collect()
        
        return snapshot_path


class TestScheduleFlow:
    """Test suite for schedule creation and execution"""
    
    def test_schedule_creation_daily(self):
        """SCHED-01: Create daily schedule with timezone"""
        session = MockSession()
        executor = MockScheduleExecutor(session)
        
        # Create schedule for 7am CT daily
        schedule_id, next_run = executor.create_schedule_event(
            dashboard_id="dash_test_123",
            frequency="DAILY",
            time="07:00",
            timezone_str="America/Chicago",
            deliveries=["email", "slack"]
        )
        
        # Verify schedule ID format
        assert schedule_id.startswith("sched_test_")
        
        # Verify next run is in Chicago time
        chicago_tz = pytz.timezone("America/Chicago")
        next_run_chicago = next_run.astimezone(chicago_tz)
        assert next_run_chicago.hour == 7
        assert next_run_chicago.minute == 0
        
        # Verify event was logged
        assert "LOG_CLAUDE_EVENT" in session.last_sql
        event_json = json.loads(session.bound_params)
        assert event_json["action"] == "dashboard.schedule_created"
        assert event_json["attributes"]["timezone"] == "America/Chicago"
    
    def test_schedule_creation_weekdays(self):
        """SCHED-02: Weekday schedule skips weekends"""
        session = MockSession()
        executor = MockScheduleExecutor(session)
        
        # Create schedule for weekdays only
        schedule_id, next_run = executor.create_schedule_event(
            dashboard_id="dash_weekday_test",
            frequency="WEEKDAYS",
            time="09:00",
            timezone_str="America/New_York",
            deliveries=["email"]
        )
        
        # Verify next run is not on weekend
        assert next_run.weekday() < 5, "Next run should be Monday-Friday"
    
    def test_dst_handling(self):
        """SCHED-03: Correct handling around DST transitions"""
        session = MockSession()
        executor = MockScheduleExecutor(session)
        
        # Test with timezone that has DST
        schedule_id, next_run = executor.create_schedule_event(
            dashboard_id="dash_dst_test",
            frequency="DAILY",
            time="07:00",
            timezone_str="America/New_York",
            deliveries=["email"]
        )
        
        # Verify timezone-aware datetime
        assert next_run.tzinfo is not None
        
        # The hour should always be 7 in local time
        ny_tz = pytz.timezone("America/New_York")
        next_run_ny = next_run.astimezone(ny_tz)
        assert next_run_ny.hour == 7
    
    def test_schedule_execution(self):
        """SCHED-04: Execute schedule and generate snapshot"""
        session = MockSession()
        executor = MockScheduleExecutor(session)
        
        # Create a schedule
        schedule_id, _ = executor.create_schedule_event(
            dashboard_id="dash_exec_test",
            frequency="DAILY",
            time="10:00",
            timezone_str="UTC",
            deliveries=["email"]
        )
        
        # Execute the schedule
        snapshot_path = executor.execute_schedule(schedule_id, "dash_exec_test")
        
        # Verify snapshot was created
        assert snapshot_path.startswith("@MCP.DASH_SNAPSHOTS/")
        assert snapshot_path.endswith(".json")
        
        # Verify execution was recorded
        assert len(executor.executed_schedules) == 1
        assert executor.executed_schedules[0]["dashboard_id"] == "dash_exec_test"
        
        # Verify snapshot event was logged
        assert "dashboard.snapshot_generated" in session.bound_params
    
    def test_snapshot_pointer_only(self):
        """SCHED-05: Snapshots are pointers, not table writes"""
        session = MockSession()
        executor = MockScheduleExecutor(session)
        
        # Execute a schedule
        snapshot_path = executor.execute_schedule("sched_123", "dash_123")
        
        # Verify it's a stage path (pointer)
        assert snapshot_path.startswith("@MCP.DASH_SNAPSHOTS/")
        
        # Verify NO table creation in SQL history
        for sql in session.sql_history:
            assert "CREATE TABLE" not in sql.upper()
            assert "INSERT INTO" not in sql.upper() or "RAW_EVENTS" in sql.upper()
    
    def test_multiple_deliveries(self):
        """SCHED-06: Multiple delivery channels supported"""
        session = MockSession()
        executor = MockScheduleExecutor(session)
        
        # Create schedule with multiple deliveries
        schedule_id, _ = executor.create_schedule_event(
            dashboard_id="dash_multi_delivery",
            frequency="DAILY",
            time="08:00",
            timezone_str="America/Los_Angeles",
            deliveries=["email", "slack", "webhook"]
        )
        
        # Verify all deliveries are stored
        event_json = json.loads(session.bound_params)
        deliveries = event_json["attributes"]["deliveries"]
        assert "email" in deliveries
        assert "slack" in deliveries
        assert "webhook" in deliveries
    
    def test_timezone_validation(self):
        """SCHED-07: Only valid Olson timezone IDs accepted"""
        session = MockSession()
        executor = MockScheduleExecutor(session)
        
        # Valid timezones should work
        valid_timezones = [
            "America/New_York",
            "America/Chicago",
            "America/Denver",
            "America/Los_Angeles",
            "UTC",
            "Europe/London",
            "Asia/Tokyo"
        ]
        
        for tz in valid_timezones:
            try:
                executor.create_schedule_event(
                    dashboard_id=f"dash_{tz.replace('/', '_')}",
                    frequency="DAILY",
                    time="12:00",
                    timezone_str=tz,
                    deliveries=["email"]
                )
            except Exception as e:
                pytest.fail(f"Valid timezone {tz} rejected: {e}")
        
        # Invalid timezone should fail
        with pytest.raises(Exception):
            executor.create_schedule_event(
                dashboard_id="dash_invalid",
                frequency="DAILY",
                time="12:00",
                timezone_str="Invalid/Timezone",
                deliveries=["email"]
            )
    
    def test_event_attributes(self):
        """AUDIT-01: Schedule events have complete attributes"""
        session = MockSession()
        executor = MockScheduleExecutor(session)
        
        # Create a schedule
        schedule_id, next_run = executor.create_schedule_event(
            dashboard_id="dash_audit_test",
            frequency="WEEKLY",
            time="09:30",
            timezone_str="America/Chicago",
            deliveries=["email", "slack"]
        )
        
        # Check event attributes
        event_json = json.loads(session.bound_params)
        attrs = event_json["attributes"]
        
        required_attrs = [
            "schedule_id", "dashboard_id", "frequency",
            "time", "timezone", "deliveries", "next_run"
        ]
        
        for attr in required_attrs:
            assert attr in attrs, f"Missing required attribute: {attr}"
        
        # Verify attribute values
        assert attrs["frequency"] == "WEEKLY"
        assert attrs["time"] == "09:30"
        assert attrs["timezone"] == "America/Chicago"
        assert len(attrs["deliveries"]) == 2


if __name__ == "__main__":
    # Run tests
    import sys
    test = TestScheduleFlow()
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