const WebSocket = require('ws');

const ws = new WebSocket('ws://localhost:8080');

ws.on('open', () => {
    console.log('Connected to WebSocket server');
    
    // Test 1: Execute a panel query
    console.log('\nTest 1: Executing panel query for activity summary...');
    ws.send(JSON.stringify({
        type: 'execute_panel',
        panel: {
            source: 'VW_ACTIVITY_SUMMARY',
            type: 'metrics'
        }
    }));
    
    // Test 2: Natural language query
    setTimeout(() => {
        console.log('\nTest 2: Sending natural language query...');
        ws.send(JSON.stringify({
            type: 'chat',
            message: 'Show me the last 10 events'
        }));
    }, 2000);
    
    // Test 3: Time series query
    setTimeout(() => {
        console.log('\nTest 3: Executing time series query...');
        ws.send(JSON.stringify({
            type: 'execute_panel',
            panel: {
                source: 'VW_ACTIVITY_COUNTS_24H',
                x: 'HOUR',
                metric: 'EVENT_COUNT',
                type: 'time_series'
            }
        }));
    }, 4000);
    
    // Close after tests
    setTimeout(() => {
        console.log('\nClosing connection...');
        ws.close();
    }, 6000);
});

ws.on('message', (data) => {
    const response = JSON.parse(data);
    
    if (response.type === 'query_result') {
        console.log(`✅ Query Result: ${response.result.rowCount || 0} rows returned`);
        if (response.result.rows && response.result.rows[0]) {
            console.log('Sample data:', JSON.stringify(response.result.rows[0], null, 2));
        }
    } else if (response.type === 'error') {
        console.log(`❌ Error: ${response.message}`);
    } else if (response.type === 'response') {
        console.log(`ℹ️ Response: ${response.message}`);
    } else {
        console.log('Other response:', response.type);
    }
});

ws.on('error', (error) => {
    console.error('WebSocket error:', error);
});

ws.on('close', () => {
    console.log('Disconnected from server');
    process.exit(0);
});