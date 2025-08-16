#!/usr/bin/env python3
"""
Dashboard Schedule Executor
External worker that processes dashboard schedules
Runs every minute to check for due schedules
"""

import json
import os
import sys
from datetime import datetime, timedelta
import snowflake.connector
from snowflake.connector import DictCursor
import hashlib
import time
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('schedule-executor')

class ScheduleExecutor:
    def __init__(self):
        """Initialize connection to Snowflake"""
        self.conn = None
        self.connect()
    
    def connect(self):
        """Connect to Snowflake using RSA authentication"""
        try:
            # Read private key
            with open(os.environ.get('SF_PK_PATH', './claude_code_rsa_key.p8'), 'rb') as key_file:
                private_key = key_file.read()
            
            self.conn = snowflake.connector.connect(
                account=os.environ.get('SNOWFLAKE_ACCOUNT', 'uec18397.us-east-1'),
                user=os.environ.get('SNOWFLAKE_USERNAME', 'CLAUDE_CODE_AI_AGENT'),
                private_key=private_key,
                warehouse=os.environ.get('SNOWFLAKE_WAREHOUSE', 'CLAUDE_AGENT_WH'),
                database='CLAUDE_BI',
                schema='MCP'
            )
            logger.info("Connected to Snowflake")
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {e}")
            raise
    
    def get_due_schedules(self):
        """Fetch schedules that are due to run"""
        try:
            cursor = self.conn.cursor(DictCursor)
            
            # Query for schedule events
            query = """
            WITH schedule_events AS (
                SELECT 
                    payload:attributes:schedule_id::STRING as schedule_id,
                    payload:attributes:dashboard_id::STRING as dashboard_id,
                    payload:attributes:frequency::STRING as frequency,
                    payload:attributes:time::STRING as scheduled_time,
                    payload:attributes:timezone::STRING as timezone,
                    payload:attributes:deliveries as deliveries,
                    payload:attributes:next_run::TIMESTAMP_TZ as next_run,
                    occurred_at
                FROM ACTIVITY.EVENTS
                WHERE action = 'dashboard.schedule_created'
                    AND occurred_at >= DATEADD('day', -30, CURRENT_TIMESTAMP())
            ),
            latest_schedules AS (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY dashboard_id 
                        ORDER BY occurred_at DESC
                    ) as rn
                FROM schedule_events
            ),
            last_runs AS (
                SELECT 
                    payload:attributes:schedule_id::STRING as schedule_id,
                    MAX(occurred_at) as last_run_at
                FROM ACTIVITY.EVENTS
                WHERE action = 'dashboard.snapshot_generated'
                GROUP BY 1
            )
            SELECT 
                s.schedule_id,
                s.dashboard_id,
                s.frequency,
                s.scheduled_time,
                s.timezone,
                s.deliveries,
                s.next_run,
                r.last_run_at
            FROM latest_schedules s
            LEFT JOIN last_runs r ON s.schedule_id = r.schedule_id
            WHERE s.rn = 1
                AND (
                    r.last_run_at IS NULL 
                    OR r.last_run_at < DATEADD('hour', -1, CURRENT_TIMESTAMP())
                )
                AND s.next_run <= CURRENT_TIMESTAMP()
            """
            
            cursor.execute(query)
            schedules = cursor.fetchall()
            cursor.close()
            
            logger.info(f"Found {len(schedules)} schedules due to run")
            return schedules
            
        except Exception as e:
            logger.error(f"Error fetching schedules: {e}")
            return []
    
    def execute_dashboard(self, dashboard_id):
        """Execute dashboard procedures and collect results"""
        try:
            cursor = self.conn.cursor(DictCursor)
            
            # Get dashboard spec
            spec_query = f"""
            SELECT 
                title,
                spec
            FROM MCP.VW_DASHBOARDS
            WHERE dashboard_id = '{dashboard_id}'
            LIMIT 1
            """
            
            cursor.execute(spec_query)
            dashboard = cursor.fetchone()
            
            if not dashboard:
                logger.warning(f"Dashboard {dashboard_id} not found")
                return None
            
            spec = json.loads(dashboard['SPEC']) if isinstance(dashboard['SPEC'], str) else dashboard['SPEC']
            panels = spec.get('panels', [])
            results = []
            
            # Execute each panel's procedure
            for panel in panels:
                if 'plan' in panel:
                    plan = panel['plan']
                    proc = plan.get('proc')
                    params = plan.get('params', {})
                    
                    # Execute procedure (simplified - would need full param handling)
                    if proc == 'DASH_GET_METRICS':
                        proc_sql = f"""
                        CALL MCP.DASH_GET_METRICS(
                            DATEADD('hour', -24, CURRENT_TIMESTAMP()),
                            CURRENT_TIMESTAMP(),
                            NULL
                        )
                        """
                    elif proc == 'DASH_GET_TOPN':
                        proc_sql = f"""
                        CALL MCP.DASH_GET_TOPN(
                            DATEADD('hour', -24, CURRENT_TIMESTAMP()),
                            CURRENT_TIMESTAMP(),
                            '{params.get('dimension', 'action')}',
                            NULL,
                            {params.get('n', 10)}
                        )
                        """
                    else:
                        continue
                    
                    cursor.execute(proc_sql)
                    result = cursor.fetchone()
                    results.append({
                        'panel': panel.get('title', 'Untitled'),
                        'data': result
                    })
            
            cursor.close()
            return {
                'dashboard_id': dashboard_id,
                'title': dashboard['TITLE'],
                'results': results
            }
            
        except Exception as e:
            logger.error(f"Error executing dashboard {dashboard_id}: {e}")
            return None
    
    def generate_snapshot(self, dashboard_data):
        """Generate dashboard snapshot (PNG/PDF)"""
        # In production, this would:
        # 1. Render dashboard using headless browser
        # 2. Generate PNG/PDF
        # 3. Upload to stage
        # 4. Return stage path
        
        snapshot_id = f"snap_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        snapshot_path = f"@MCP.DASH_SNAPSHOTS/{snapshot_id}.json"
        
        # For now, just save JSON representation
        return {
            'snapshot_id': snapshot_id,
            'path': snapshot_path,
            'format': 'json',
            'data': dashboard_data
        }
    
    def deliver_snapshot(self, snapshot, deliveries):
        """Deliver snapshot via email/Slack"""
        for delivery in deliveries:
            if delivery == 'email':
                logger.info(f"Would send email with snapshot {snapshot['snapshot_id']}")
                # Email sending logic here
            elif delivery == 'slack':
                logger.info(f"Would post to Slack with snapshot {snapshot['snapshot_id']}")
                # Slack posting logic here
    
    def log_snapshot_generated(self, schedule_id, dashboard_id, snapshot):
        """Log dashboard.snapshot_generated event"""
        try:
            cursor = self.conn.cursor()
            
            event_sql = f"""
            CALL MCP.LOG_CLAUDE_EVENT(OBJECT_CONSTRUCT(
                'action', 'dashboard.snapshot_generated',
                'actor_id', 'SCHEDULE_EXECUTOR',
                'object', OBJECT_CONSTRUCT(
                    'type', 'snapshot',
                    'id', '{snapshot['snapshot_id']}'
                ),
                'attributes', OBJECT_CONSTRUCT(
                    'schedule_id', '{schedule_id}',
                    'dashboard_id', '{dashboard_id}',
                    'snapshot_path', '{snapshot['path']}',
                    'format', '{snapshot['format']}',
                    'row_count', {len(snapshot.get('data', {}).get('results', []))},
                    'generated_at', CURRENT_TIMESTAMP()
                ),
                'occurred_at', CURRENT_TIMESTAMP()
            ), 'SCHEDULER')
            """
            
            cursor.execute(event_sql)
            cursor.close()
            logger.info(f"Logged snapshot generation for dashboard {dashboard_id}")
            
        except Exception as e:
            logger.error(f"Error logging snapshot: {e}")
    
    def process_schedule(self, schedule):
        """Process a single schedule"""
        try:
            schedule_id = schedule['SCHEDULE_ID']
            dashboard_id = schedule['DASHBOARD_ID']
            deliveries = json.loads(schedule['DELIVERIES']) if isinstance(schedule['DELIVERIES'], str) else schedule['DELIVERIES']
            
            logger.info(f"Processing schedule {schedule_id} for dashboard {dashboard_id}")
            
            # Execute dashboard
            dashboard_data = self.execute_dashboard(dashboard_id)
            if not dashboard_data:
                logger.warning(f"Failed to execute dashboard {dashboard_id}")
                return
            
            # Generate snapshot
            snapshot = self.generate_snapshot(dashboard_data)
            
            # Deliver snapshot
            self.deliver_snapshot(snapshot, deliveries)
            
            # Log completion
            self.log_snapshot_generated(schedule_id, dashboard_id, snapshot)
            
            logger.info(f"Successfully processed schedule {schedule_id}")
            
        except Exception as e:
            logger.error(f"Error processing schedule {schedule['SCHEDULE_ID']}: {e}")
    
    def run(self):
        """Main execution loop"""
        logger.info("Schedule executor started")
        
        while True:
            try:
                # Get due schedules
                schedules = self.get_due_schedules()
                
                # Process each schedule
                for schedule in schedules:
                    self.process_schedule(schedule)
                
                # Wait before next check
                logger.info("Waiting 60 seconds before next check...")
                time.sleep(60)
                
            except KeyboardInterrupt:
                logger.info("Executor stopped by user")
                break
            except Exception as e:
                logger.error(f"Executor error: {e}")
                time.sleep(60)  # Wait before retry
        
        if self.conn:
            self.conn.close()
            logger.info("Connection closed")

def main():
    """Main entry point"""
    executor = ScheduleExecutor()
    executor.run()

if __name__ == "__main__":
    main()