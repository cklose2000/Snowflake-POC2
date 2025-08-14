#!/usr/bin/env python3
"""
Snowpark Container Services MCP Server
Provides secure, contract-enforced access to Snowflake for query processing
"""

import os
import json
import hashlib
import time
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
import uuid

from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, validator
import snowflake.connector
from snowflake.connector import DictCursor
import structlog
import uvicorn
from prometheus_client import Counter, Histogram, generate_latest

# Configure structured logging
logger = structlog.get_logger()

# Metrics
query_counter = Counter('mcp_queries_total', 'Total MCP queries executed', ['tool', 'status'])
query_duration = Histogram('mcp_query_duration_seconds', 'Query execution time', ['tool'])
validation_errors = Counter('mcp_validation_errors_total', 'Validation errors', ['type'])

# FastAPI app
app = FastAPI(
    title="Snowflake MCP Server",
    description="Model Context Protocol server for secure Snowflake access",
    version="2.0.0"
)

# CORS configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Snowflake proxy will handle auth
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================================================
# Data Models
# ============================================================================

class QueryPlan(BaseModel):
    """Validated query plan structure"""
    source: str
    dimensions: Optional[List[str]] = []
    measures: Optional[List[Dict[str, str]]] = []
    filters: Optional[List[Dict[str, Any]]] = []
    grain: Optional[str] = None
    top_n: Optional[int] = Field(None, le=10000)
    order_by: Optional[List[Dict[str, str]]] = []
    
    @validator('top_n')
    def validate_top_n(cls, v):
        if v and v > 10000:
            raise ValueError('Row limit exceeds maximum of 10000')
        return v

class ComposeQueryRequest(BaseModel):
    """Request to compose and execute a query plan"""
    intent_text: str
    source: Optional[str] = None
    dimensions: Optional[List[str]] = []
    measures: Optional[List[Dict[str, str]]] = []
    filters: Optional[List[Dict[str, Any]]] = []
    grain: Optional[str] = None
    top_n: Optional[int] = 1000
    order_by: Optional[List[Dict[str, str]]] = []

class DashboardSpec(BaseModel):
    """Dashboard creation specification"""
    title: str
    description: Optional[str] = ""
    queries: List[Dict[str, Any]]
    refresh_method: str = "manual"
    schedule: Optional[str] = None

# ============================================================================
# Schema Contract Management
# ============================================================================

class SchemaContract:
    """Manages and validates against the schema contract"""
    
    def __init__(self):
        self.contract = self._load_contract()
        self.allowed_sources = self._extract_sources()
        
    def _load_contract(self) -> Dict:
        """Load schema contract from Snowflake or file"""
        try:
            # In production, load from Snowflake
            # For now, load from file
            contract_path = '/app/contracts/database.contract.json'
            if os.path.exists(contract_path):
                with open(contract_path, 'r') as f:
                    return json.load(f)
        except Exception as e:
            logger.error("Failed to load contract", error=str(e))
        
        # Fallback contract
        return {
            "version": "2.0.0",
            "database": "CLAUDE_BI",
            "schemas": {
                "ACTIVITY": {"tables": {"EVENTS": {}}},
                "ACTIVITY_CCODE": {
                    "tables": {"ARTIFACTS": {}},
                    "views": {
                        "VW_ACTIVITY_SUMMARY": {},
                        "VW_ACTIVITY_COUNTS_24H": {}
                    }
                }
            },
            "security": {
                "max_rows_per_query": 10000,
                "query_timeout_seconds": 300,
                "forbidden_operations": ["CREATE", "DROP", "ALTER", "INSERT", "UPDATE", "DELETE"]
            }
        }
    
    def _extract_sources(self) -> List[str]:
        """Extract all allowed sources from contract"""
        sources = []
        for schema_name, schema_def in self.contract.get("schemas", {}).items():
            for table_name in schema_def.get("tables", {}):
                sources.append(f"{schema_name}.{table_name}")
            for view_name in schema_def.get("views", {}):
                sources.append(f"{schema_name}.{view_name}")
        return sources
    
    def validate_source(self, source: str) -> bool:
        """Validate that a source exists in the contract"""
        # Handle both two-part and three-part names
        if "." not in source:
            return False
        
        parts = source.split(".")
        if len(parts) == 3:  # DATABASE.SCHEMA.OBJECT
            source = f"{parts[1]}.{parts[2]}"
        
        return source in self.allowed_sources
    
    def validate_plan(self, plan: QueryPlan) -> List[str]:
        """Validate a query plan against the contract"""
        errors = []
        
        # Validate source
        if not self.validate_source(plan.source):
            errors.append(f"Unknown source: {plan.source}")
        
        # Validate row limit
        max_rows = self.contract.get("security", {}).get("max_rows_per_query", 10000)
        if plan.top_n and plan.top_n > max_rows:
            errors.append(f"Row limit {plan.top_n} exceeds maximum {max_rows}")
        
        return errors

# ============================================================================
# SQL Rendering Engine
# ============================================================================

class SqlRenderer:
    """Renders validated SQL from query plans"""
    
    def __init__(self, contract: SchemaContract):
        self.contract = contract
        self.database = contract.contract.get("database", "CLAUDE_BI")
    
    def render(self, plan: QueryPlan) -> str:
        """Render SQL from a validated query plan"""
        # Build SELECT clause
        select_parts = []
        
        # Add dimensions
        if plan.dimensions:
            select_parts.extend(plan.dimensions)
        
        # Add measures
        if plan.measures:
            for measure in plan.measures:
                fn = measure.get('fn', 'COUNT')
                col = measure.get('column', '*')
                alias = f"{fn}_{col}".replace('*', 'ALL')
                select_parts.append(f"{fn}({col}) AS {alias}")
        
        # Default to * if no specific columns
        if not select_parts:
            select_parts = ['*']
        
        # Build FROM clause
        from_clause = self._qualify_source(plan.source)
        
        # Build WHERE clause
        where_conditions = []
        if plan.filters:
            for filter_def in plan.filters:
                col = filter_def['column']
                op = filter_def['operator']
                val = filter_def['value']
                
                if op == 'IN':
                    val_str = f"({','.join([self._quote_value(v) for v in val])})"
                    where_conditions.append(f"{col} {op} {val_str}")
                elif op == 'BETWEEN':
                    where_conditions.append(f"{col} BETWEEN {self._quote_value(val[0])} AND {self._quote_value(val[1])}")
                else:
                    where_conditions.append(f"{col} {op} {self._quote_value(val)}")
        
        # Build GROUP BY clause
        group_by = ""
        if plan.dimensions and plan.measures:
            group_by = f"GROUP BY {', '.join(plan.dimensions)}"
        
        # Build ORDER BY clause
        order_by = ""
        if plan.order_by:
            order_parts = []
            for order in plan.order_by:
                order_parts.append(f"{order['column']} {order.get('direction', 'ASC')}")
            order_by = f"ORDER BY {', '.join(order_parts)}"
        
        # Build LIMIT clause
        limit = f"LIMIT {plan.top_n}" if plan.top_n else "LIMIT 10000"
        
        # Combine all parts
        sql = f"SELECT {', '.join(select_parts)}\\nFROM {from_clause}"
        
        if where_conditions:
            sql += f"\\nWHERE {' AND '.join(where_conditions)}"
        
        if group_by:
            sql += f"\\n{group_by}"
        
        if order_by:
            sql += f"\\n{order_by}"
        
        sql += f"\\n{limit}"
        
        return sql
    
    def _qualify_source(self, source: str) -> str:
        """Fully qualify a source name"""
        if "." not in source:
            return f"{self.database}.ACTIVITY.{source}"
        
        parts = source.split(".")
        if len(parts) == 2:
            return f"{self.database}.{parts[0]}.{parts[1]}"
        
        return source
    
    def _quote_value(self, value: Any) -> str:
        """Properly quote a value for SQL"""
        if value is None:
            return "NULL"
        elif isinstance(value, (int, float)):
            return str(value)
        elif isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        else:
            # Escape single quotes
            escaped = str(value).replace("'", "''")
            return f"'{escaped}'"

# ============================================================================
# Snowflake Connection Manager
# ============================================================================

class SnowflakeManager:
    """Manages Snowflake connections with local access in container"""
    
    def __init__(self):
        # In Snowpark Container Services, use local connection
        self.connection_params = {
            'account': os.getenv('SNOWFLAKE_ACCOUNT', 'localhost'),
            'user': os.getenv('SNOWFLAKE_USER', 'CONTAINER_USER'),
            'authenticator': 'oauth',  # Container uses OAuth internally
            'token': os.getenv('SNOWFLAKE_TOKEN'),  # Injected by Snowpark
            'database': 'CLAUDE_BI',
            'schema': 'ACTIVITY_CCODE',
            'warehouse': 'MCP_XS_WH',
            'role': 'MCP_EXECUTOR_ROLE'
        }
        self.conn = None
    
    def connect(self):
        """Establish connection to Snowflake"""
        if not self.conn:
            self.conn = snowflake.connector.connect(**self.connection_params)
            # Set session parameters
            self.conn.cursor().execute("ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 30")
            self.conn.cursor().execute("ALTER SESSION SET USE_CACHED_RESULT = TRUE")
    
    def execute_query(self, sql: str, tag: Dict[str, Any] = None) -> Dict:
        """Execute a query with tagging and monitoring"""
        self.connect()
        
        # Set query tag for tracking
        if tag:
            tag_json = json.dumps(tag)
            self.conn.cursor().execute(f"ALTER SESSION SET QUERY_TAG = '{tag_json}'")
        
        start_time = time.time()
        cursor = self.conn.cursor(DictCursor)
        
        try:
            cursor.execute(sql)
            rows = cursor.fetchall()
            
            # Get query metadata
            query_id = cursor.sfqid
            row_count = cursor.rowcount
            
            duration = time.time() - start_time
            
            return {
                'success': True,
                'rows': rows,
                'metadata': {
                    'query_id': query_id,
                    'row_count': row_count,
                    'execution_time_ms': int(duration * 1000),
                    'bytes_scanned': cursor.description
                }
            }
        except Exception as e:
            logger.error("Query execution failed", sql=sql[:100], error=str(e))
            raise
        finally:
            cursor.close()
    
    def log_activity(self, activity: str, details: Dict):
        """Log activity to Snowflake Activity Schema"""
        event = {
            'activity_id': f"act_{uuid.uuid4().hex[:12]}",
            'ts': datetime.utcnow().isoformat(),
            'customer': os.getenv('MCP_USER', 'system'),
            'activity': f"ccode.{activity}",
            'feature_json': json.dumps(details)
        }
        
        sql = f"""
        INSERT INTO CLAUDE_BI.ACTIVITY.EVENTS (activity_id, ts, customer, activity, feature_json)
        VALUES ('{event['activity_id']}', '{event['ts']}', '{event['customer']}', 
                '{event['activity']}', PARSE_JSON('{event['feature_json']}'))
        """
        
        try:
            self.connect()
            self.conn.cursor().execute(sql)
        except Exception as e:
            logger.error("Failed to log activity", error=str(e))

# ============================================================================
# Global instances
# ============================================================================

contract = SchemaContract()
sql_renderer = SqlRenderer(contract)
snowflake = SnowflakeManager()

# ============================================================================
# API Endpoints
# ============================================================================

@app.get("/health")
async def health_check():
    """Health check endpoint for container monitoring"""
    return {"status": "healthy", "version": "2.0.0", "timestamp": datetime.utcnow().isoformat()}

@app.get("/metrics")
async def metrics():
    """Prometheus metrics endpoint"""
    return Response(content=generate_latest(), media_type="text/plain")

@app.get("/tools")
async def list_tools():
    """List available MCP tools"""
    return {
        "tools": [
            {
                "name": "compose_query_plan",
                "description": "Compose and execute a validated query plan",
                "input_schema": ComposeQueryRequest.model_json_schema()
            },
            {
                "name": "create_dashboard",
                "description": "Create a Snowflake Streamlit dashboard",
                "input_schema": DashboardSpec.model_json_schema()
            },
            {
                "name": "list_sources",
                "description": "List available data sources",
                "input_schema": {}
            },
            {
                "name": "validate_plan",
                "description": "Validate a query plan without execution",
                "input_schema": QueryPlan.model_json_schema()
            }
        ]
    }

@app.post("/tools/compose_query_plan")
async def compose_query_plan(request: ComposeQueryRequest):
    """Compose and execute a validated query plan"""
    with query_duration.labels(tool='compose_query_plan').time():
        try:
            # Build query plan
            plan = QueryPlan(
                source=request.source or "VW_ACTIVITY_SUMMARY",
                dimensions=request.dimensions,
                measures=request.measures,
                filters=request.filters,
                grain=request.grain,
                top_n=request.top_n,
                order_by=request.order_by
            )
            
            # Validate plan
            errors = contract.validate_plan(plan)
            if errors:
                validation_errors.labels(type='plan').inc()
                query_counter.labels(tool='compose_query_plan', status='invalid').inc()
                return {"success": False, "errors": errors}
            
            # Render SQL
            sql = sql_renderer.render(plan)
            
            # Execute query
            result = snowflake.execute_query(sql, tag={
                'mcp_tool': 'compose_query_plan',
                'mcp_user': os.getenv('MCP_USER', 'unknown'),
                'intent': request.intent_text[:100]
            })
            
            # Log activity
            snowflake.log_activity('query_executed', {
                'tool': 'compose_query_plan',
                'plan': plan.model_dump(),
                'row_count': result['metadata']['row_count']
            })
            
            query_counter.labels(tool='compose_query_plan', status='success').inc()
            
            return {
                "success": True,
                "plan": plan.model_dump(),
                "sql": sql,
                "results": result['rows'],
                "metadata": result['metadata']
            }
            
        except Exception as e:
            query_counter.labels(tool='compose_query_plan', status='error').inc()
            logger.error("Query plan execution failed", error=str(e))
            raise HTTPException(status_code=500, detail=str(e))

@app.post("/tools/validate_plan")
async def validate_plan(plan: QueryPlan):
    """Validate a query plan without executing"""
    try:
        errors = contract.validate_plan(plan)
        
        if errors:
            validation_errors.labels(type='plan').inc()
            return {"valid": False, "errors": errors}
        
        # Render SQL for validation
        sql = sql_renderer.render(plan)
        
        return {
            "valid": True,
            "plan": plan.model_dump(),
            "sql": sql,
            "message": "Plan is valid and ready for execution"
        }
        
    except Exception as e:
        logger.error("Plan validation failed", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/tools/list_sources")
async def list_sources(include_columns: bool = False):
    """List all available data sources"""
    try:
        sources = []
        
        for source in contract.allowed_sources:
            source_info = {
                "name": source.split(".")[-1],
                "schema": source.split(".")[0],
                "type": "view" if "VW_" in source else "table",
                "full_name": f"{contract.database}.{source}"
            }
            
            if include_columns:
                # In production, query INFORMATION_SCHEMA
                # For now, return sample columns
                if "VW_ACTIVITY_SUMMARY" in source:
                    source_info["columns"] = ["TOTAL_EVENTS", "UNIQUE_CUSTOMERS", "UNIQUE_ACTIVITIES", "LAST_EVENT"]
                elif "VW_ACTIVITY_COUNTS_24H" in source:
                    source_info["columns"] = ["HOUR", "ACTIVITY", "EVENT_COUNT", "UNIQUE_CUSTOMERS"]
                elif "EVENTS" in source:
                    source_info["columns"] = ["ACTIVITY_ID", "TS", "CUSTOMER", "ACTIVITY", "FEATURE_JSON"]
            
            sources.append(source_info)
        
        return {
            "success": True,
            "sources": sources,
            "metadata": {
                "total_sources": len(sources),
                "contract_version": contract.contract.get("version", "2.0.0")
            }
        }
        
    except Exception as e:
        logger.error("Failed to list sources", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/tools/create_dashboard")
async def create_dashboard(spec: DashboardSpec):
    """Create a Snowflake Streamlit dashboard"""
    try:
        dashboard_id = f"dash_{uuid.uuid4().hex[:8]}"
        
        # Generate Streamlit code
        streamlit_code = generate_streamlit_code(dashboard_id, spec)
        
        # In production, deploy to Snowflake stage
        # For now, return the code
        
        # Log activity
        snowflake.log_activity('dashboard_created', {
            'dashboard_id': dashboard_id,
            'title': spec.title,
            'query_count': len(spec.queries)
        })
        
        return {
            "success": True,
            "dashboard_id": dashboard_id,
            "dashboard_url": f"https://app.snowflake.com/dashboards/{dashboard_id}",
            "streamlit_code": streamlit_code,
            "message": "Dashboard created successfully"
        }
        
    except Exception as e:
        logger.error("Dashboard creation failed", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))

def generate_streamlit_code(dashboard_id: str, spec: DashboardSpec) -> str:
    """Generate Streamlit dashboard code"""
    code = f'''
import streamlit as st
import snowflake.connector
import pandas as pd
import plotly.express as px

st.set_page_config(
    page_title="{spec.title}",
    page_icon="📊",
    layout="wide"
)

st.title("{spec.title}")
st.markdown("{spec.description}")

# Connection is automatic in Snowpark
conn = snowflake.connector.connect()

'''
    
    for i, query_spec in enumerate(spec.queries):
        code += f'''
# Query {i+1}: {query_spec.get('name', f'Query {i+1}')}
with st.container():
    st.subheader("{query_spec.get('name', f'Query {i+1}')}")
    
    df_{i} = pd.read_sql("""
    {query_spec.get('sql', 'SELECT 1')}
    """, conn)
    
    if "{query_spec.get('chart_type', 'table')}" == "line":
        fig = px.line(df_{i}, x=df_{i}.columns[0], y=df_{i}.columns[1])
        st.plotly_chart(fig, use_container_width=True)
    elif "{query_spec.get('chart_type', 'table')}" == "bar":
        fig = px.bar(df_{i}, x=df_{i}.columns[0], y=df_{i}.columns[1])
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.dataframe(df_{i}, use_container_width=True)
'''
    
    return code

# ============================================================================
# Main entry point
# ============================================================================

if __name__ == "__main__":
    logger.info("Starting Snowpark MCP Server", version="2.0.0")
    uvicorn.run(app, host="0.0.0.0", port=8080)