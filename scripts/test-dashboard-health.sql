-- ============================================================================
-- Dashboard Health Check and Testing Suite
-- WORK-00402: Automated testing to ensure dashboards remain accessible
-- ============================================================================

USE DATABASE CLAUDE_BI;
USE SCHEMA MCP;

-- ============================================================================
-- 1. Test Dashboard Health Procedure
-- ============================================================================
CREATE OR REPLACE PROCEDURE MCP.TEST_DASHBOARD_HEALTH()
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
const SF = snowflake;

try {
    const results = {
        timestamp: new Date().toISOString(),
        dashboards: [],
        files: [],
        warehouse: null,
        overall_health: 'UNKNOWN',
        issues: []
    };
    
    // 1. Check Streamlit apps
    const streamlitSQL = `SHOW STREAMLITS IN SCHEMA CLAUDE_BI.MCP`;
    const streamlitStmt = SF.createStatement({ sqlText: streamlitSQL });
    const streamlitRS = streamlitStmt.execute();
    
    while (streamlitRS.next()) {
        const dashboard = {
            name: streamlitRS.getColumnValue('name'),
            url_id: streamlitRS.getColumnValue('url_id'),
            warehouse: streamlitRS.getColumnValue('query_warehouse'),
            created: streamlitRS.getColumnValue('created_on'),
            url: 'https://app.snowflake.com/uec18397/us-east-1/streamlit-apps/CLAUDE_BI/MCP/' + 
                 streamlitRS.getColumnValue('name') + '/' + 
                 streamlitRS.getColumnValue('url_id')
        };
        results.dashboards.push(dashboard);
    }
    
    // 2. Check files in stage
    const filesSQL = `LIST @CLAUDE_BI.MCP.DASH_APPS`;
    const filesStmt = SF.createStatement({ sqlText: filesSQL });
    const filesRS = filesStmt.execute();
    
    while (filesRS.next()) {
        const file = {
            name: filesRS.getColumnValue('name'),
            size: filesRS.getColumnValue('size'),
            last_modified: filesRS.getColumnValue('last_modified')
        };
        results.files.push(file);
    }
    
    // 3. Check warehouse status
    const warehouseSQL = `SHOW WAREHOUSES LIKE 'CLAUDE_AGENT_WH'`;
    const warehouseStmt = SF.createStatement({ sqlText: warehouseSQL });
    const warehouseRS = warehouseStmt.execute();
    
    if (warehouseRS.next()) {
        results.warehouse = {
            name: warehouseRS.getColumnValue('name'),
            state: warehouseRS.getColumnValue('state'),
            size: warehouseRS.getColumnValue('size')
        };
    }
    
    // 4. Determine overall health
    const hasDashboards = results.dashboards.length > 0;
    const hasFiles = results.files.length > 0;
    const warehouseOK = results.warehouse && 
                       (results.warehouse.state === 'STARTED' || 
                        results.warehouse.state === 'SUSPENDED');
    
    if (!hasDashboards) {
        results.issues.push('No dashboards found');
    }
    if (!hasFiles) {
        results.issues.push('No Python files in stage');
    }
    if (!warehouseOK) {
        results.issues.push('Warehouse issue: ' + (results.warehouse ? results.warehouse.state : 'Not found'));
    }
    
    // Check specific critical files
    const hasCOODashboard = results.files.some(f => f.name.includes('coo_dashboard.py'));
    const hasStreamlitApp = results.files.some(f => f.name.includes('streamlit_app.py'));
    
    if (!hasCOODashboard) {
        results.issues.push('Missing coo_dashboard.py');
    }
    if (!hasStreamlitApp) {
        results.issues.push('Missing streamlit_app.py');
    }
    
    results.overall_health = results.issues.length === 0 ? 'HEALTHY' : 'UNHEALTHY';
    
    // 5. Log health check event
    const eventPayload = {
        event_id: 'health_check_' + Date.now(),
        action: 'dashboard.health_check',
        occurred_at: new Date().toISOString(),
        actor_id: 'system',
        source: 'monitoring',
        attributes: {
            dashboard_count: results.dashboards.length,
            file_count: results.files.length,
            warehouse_status: results.warehouse ? results.warehouse.state : 'UNKNOWN',
            health_status: results.overall_health,
            issues: results.issues
        }
    };
    
    const insertSQL = `
        INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS (PAYLOAD, _SOURCE_LANE, _RECV_AT)
        SELECT OBJECT_CONSTRUCT(
            'event_id', ?,
            'action', ?,
            'occurred_at', ?,
            'actor_id', ?,
            'source', ?,
            'attributes', PARSE_JSON(?)
        ), 'SYSTEM', CURRENT_TIMESTAMP()
    `;
    
    const insertStmt = SF.createStatement({
        sqlText: insertSQL,
        binds: [
            eventPayload.event_id,
            eventPayload.action,
            eventPayload.occurred_at,
            eventPayload.actor_id,
            eventPayload.source,
            JSON.stringify(eventPayload.attributes)
        ]
    });
    insertStmt.execute();
    
    return {
        result: 'ok',
        health_status: results.overall_health,
        dashboard_count: results.dashboards.length,
        file_count: results.files.length,
        issues: results.issues,
        dashboards: results.dashboards,
        timestamp: results.timestamp
    };
    
} catch (err) {
    return {
        result: 'error',
        error: err.toString(),
        health_status: 'ERROR'
    };
}
$$;

-- ============================================================================
-- 2. Test Specific Dashboard
-- ============================================================================
CREATE OR REPLACE PROCEDURE MCP.TEST_DASHBOARD(dashboard_name STRING)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
const SF = snowflake;

try {
    // Check if dashboard exists
    const checkSQL = `
        SHOW STREAMLITS LIKE '${DASHBOARD_NAME}' IN SCHEMA CLAUDE_BI.MCP
    `;
    
    const checkStmt = SF.createStatement({ sqlText: checkSQL });
    const checkRS = checkStmt.execute();
    
    if (!checkRS.next()) {
        return {
            result: 'error',
            message: 'Dashboard not found: ' + DASHBOARD_NAME
        };
    }
    
    const dashboard = {
        name: checkRS.getColumnValue('name'),
        url_id: checkRS.getColumnValue('url_id'),
        warehouse: checkRS.getColumnValue('query_warehouse'),
        created: checkRS.getColumnValue('created_on'),
        comment: checkRS.getColumnValue('comment')
    };
    
    // Generate correct URL
    const url = 'https://app.snowflake.com/uec18397/us-east-1/streamlit-apps/CLAUDE_BI/MCP/' + 
                dashboard.name + '/' + dashboard.url_id;
    
    // Check if main file exists
    const mainFile = dashboard.name === 'COO_EXECUTIVE_DASHBOARD' ? 
                     'coo_dashboard.py' : 'streamlit_app.py';
    
    const fileSQL = `LIST @CLAUDE_BI.MCP.DASH_APPS PATTERN='.*${mainFile}.*'`;
    const fileStmt = SF.createStatement({ sqlText: fileSQL });
    const fileRS = fileStmt.execute();
    
    const fileExists = fileRS.next();
    
    // Test data procedures (if COO dashboard)
    let procedureTests = [];
    if (dashboard.name === 'COO_EXECUTIVE_DASHBOARD') {
        const procedures = ['DASH_GET_METRICS', 'DASH_GET_SERIES', 'DASH_GET_TOPN', 'DASH_GET_EVENTS'];
        
        for (const proc of procedures) {
            try {
                const testSQL = `CALL MCP.${proc}(OBJECT_CONSTRUCT('start_ts', DATEADD('day', -1, CURRENT_TIMESTAMP()), 'end_ts', CURRENT_TIMESTAMP()))`;
                const testStmt = SF.createStatement({ sqlText: testSQL });
                testStmt.execute();
                procedureTests.push({ procedure: proc, status: 'OK' });
            } catch (e) {
                procedureTests.push({ procedure: proc, status: 'FAILED', error: e.toString() });
            }
        }
    }
    
    return {
        result: 'ok',
        dashboard: dashboard.name,
        url: url,
        file_exists: fileExists,
        warehouse: dashboard.warehouse,
        procedure_tests: procedureTests,
        test_timestamp: new Date().toISOString()
    };
    
} catch (err) {
    return {
        result: 'error',
        error: err.toString(),
        dashboard: DASHBOARD_NAME
    };
}
$$;

-- ============================================================================
-- 3. Run Full Test Suite
-- ============================================================================
CREATE OR REPLACE PROCEDURE MCP.RUN_DASHBOARD_TESTS()
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
const SF = snowflake;
const testResults = {
    timestamp: new Date().toISOString(),
    tests: [],
    summary: {
        total: 0,
        passed: 0,
        failed: 0
    }
};

try {
    // Test 1: Overall health
    const healthSQL = `CALL MCP.TEST_DASHBOARD_HEALTH()`;
    const healthStmt = SF.createStatement({ sqlText: healthSQL });
    const healthRS = healthStmt.execute();
    healthRS.next();
    const healthResult = JSON.parse(healthRS.getColumnValue(1));
    
    testResults.tests.push({
        test: 'Overall Health',
        result: healthResult.health_status === 'HEALTHY' ? 'PASSED' : 'FAILED',
        details: healthResult
    });
    
    // Test 2: COO Dashboard specifically
    const cooSQL = `CALL MCP.TEST_DASHBOARD('COO_EXECUTIVE_DASHBOARD')`;
    const cooStmt = SF.createStatement({ sqlText: cooSQL });
    const cooRS = cooStmt.execute();
    cooRS.next();
    const cooResult = JSON.parse(cooRS.getColumnValue(1));
    
    testResults.tests.push({
        test: 'COO Dashboard',
        result: cooResult.result === 'ok' ? 'PASSED' : 'FAILED',
        details: cooResult
    });
    
    // Test 3: URL generation
    const urlTest = {
        test: 'URL Generation',
        result: 'PASSED',
        urls: []
    };
    
    const dashboards = healthResult.dashboards || [];
    for (const dash of dashboards) {
        if (dash.url && dash.url.includes('app.snowflake.com')) {
            urlTest.urls.push({ name: dash.name, url: dash.url, valid: true });
        } else {
            urlTest.result = 'FAILED';
            urlTest.urls.push({ name: dash.name, url: dash.url, valid: false });
        }
    }
    testResults.tests.push(urlTest);
    
    // Calculate summary
    testResults.summary.total = testResults.tests.length;
    testResults.summary.passed = testResults.tests.filter(t => t.result === 'PASSED').length;
    testResults.summary.failed = testResults.tests.filter(t => t.result === 'FAILED').length;
    
    // Log test results
    const eventSQL = `
        INSERT INTO CLAUDE_BI.LANDING.RAW_EVENTS (PAYLOAD, _SOURCE_LANE, _RECV_AT)
        SELECT OBJECT_CONSTRUCT(
            'event_id', 'test_run_' || UUID_STRING(),
            'action', 'dashboard.test_suite',
            'occurred_at', CURRENT_TIMESTAMP(),
            'actor_id', 'system',
            'source', 'testing',
            'attributes', PARSE_JSON(?)
        ), 'SYSTEM', CURRENT_TIMESTAMP()
    `;
    
    const eventStmt = SF.createStatement({
        sqlText: eventSQL,
        binds: [JSON.stringify(testResults)]
    });
    eventStmt.execute();
    
    return testResults;
    
} catch (err) {
    return {
        result: 'error',
        error: err.toString(),
        timestamp: new Date().toISOString()
    };
}
$$;

-- ============================================================================
-- 4. Schedule Automated Health Checks
-- ============================================================================
CREATE OR REPLACE TASK MCP.DASHBOARD_HEALTH_CHECK_TASK
  WAREHOUSE = CLAUDE_AGENT_WH
  SCHEDULE = 'USING CRON 0 * * * * UTC'  -- Every hour
  COMMENT = 'Hourly dashboard health check'
AS
  CALL MCP.TEST_DASHBOARD_HEALTH();

-- Start the task
ALTER TASK MCP.DASHBOARD_HEALTH_CHECK_TASK RESUME;

-- ============================================================================
-- 5. Grant Permissions
-- ============================================================================
GRANT USAGE ON PROCEDURE MCP.TEST_DASHBOARD_HEALTH() TO ROLE MCP_AGENT_ROLE;
GRANT USAGE ON PROCEDURE MCP.TEST_DASHBOARD(STRING) TO ROLE MCP_AGENT_ROLE;
GRANT USAGE ON PROCEDURE MCP.RUN_DASHBOARD_TESTS() TO ROLE MCP_AGENT_ROLE;

-- ============================================================================
-- 6. Run Initial Test
-- ============================================================================
CALL MCP.RUN_DASHBOARD_TESTS();