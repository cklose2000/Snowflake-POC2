const WebSocket = require('ws');

// Connect to the WebSocket server
const ws = new WebSocket('ws://localhost:8080');

ws.on('open', () => {
    console.log('Connected to server');
    
    // Send a test dashboard creation request
    const testRequest = {
        type: 'dashboard-create',
        sessionId: 'test_session_' + Date.now(),
        conversationHistory: [
            {
                role: 'user',
                content: 'Show me the top 10 customers by query count in the last 24 hours'
            },
            {
                role: 'assistant',
                content: 'I\'ll help you see the top 10 customers by query count. Let me run that analysis for you.'
            },
            {
                role: 'user',
                content: 'Also show me the activity breakdown by type for these customers'
            }
        ],
        customerID: 'test_customer_001'
    };
    
    console.log('Sending dashboard creation request:', testRequest);
    ws.send(JSON.stringify(testRequest));
});

ws.on('message', (data) => {
    const message = JSON.parse(data.toString());
    console.log('Received message:', message);
    
    // Handle different message types
    switch(message.type) {
        case 'dashboard.progress':
            console.log(`📊 Progress: ${message.message}`);
            break;
            
        case 'dashboard.created':
            console.log('✅ Dashboard created successfully!');
            console.log(`   Spec ID: ${message.spec_id}`);
            console.log(`   URL: ${message.url}`);
            console.log(`   Panels: ${message.panelCount}`);
            console.log(`   Estimated Cost: ${message.estimatedCost}`);
            ws.close();
            break;
            
        case 'dashboard.error':
            console.error('❌ Dashboard creation failed');
            console.error(`   Code: ${message.code}`);
            console.error(`   Message: ${message.message}`);
            ws.close();
            break;
            
        default:
            console.log('Other message:', message.type);
    }
});

ws.on('error', (error) => {
    console.error('WebSocket error:', error);
});

ws.on('close', () => {
    console.log('Connection closed');
    process.exit(0);
});