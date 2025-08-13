// Test Dashboard Factory v1 Components
// Tests the completed components without requiring Snowflake connection

const { validateSpec, generateSpecHash, generateObjectNames, EXAMPLE_SPECS } = require('./schema');
const ConversationAnalyzer = require('./conversation-analyzer');
const SpecGenerator = require('./spec-generator');

async function testComponents() {
  console.log('🧪 Testing Dashboard Factory v1 Components\n');
  
  // Test 1: Schema Validation
  console.log('1️⃣ Testing Frozen Schema Validation');
  console.log('=====================================');
  
  const validSpec = EXAMPLE_SPECS.sales_executive;
  const validation = validateSpec(validSpec);
  console.log(`✅ Valid spec validation: ${validation.valid ? 'PASSED' : 'FAILED'}`);
  
  // Test invalid spec
  const invalidSpec = { ...validSpec };
  delete invalidSpec.timezone; // Remove required field
  const invalidValidation = validateSpec(invalidSpec);
  console.log(`✅ Invalid spec rejection: ${!invalidValidation.valid ? 'PASSED' : 'FAILED'}`);
  console.log(`   Errors detected: ${invalidValidation.errors?.length || 0}`);
  
  // Test hash generation
  const hash1 = generateSpecHash(validSpec);
  const hash2 = generateSpecHash(validSpec);
  const hash3 = generateSpecHash(EXAMPLE_SPECS.ops_monitoring);
  console.log(`✅ Hash consistency: ${hash1 === hash2 ? 'PASSED' : 'FAILED'}`);
  console.log(`✅ Hash uniqueness: ${hash1 !== hash3 ? 'PASSED' : 'FAILED'}`);
  console.log(`   Spec hash: ${hash1}`);
  
  // Test object naming
  const objectNames = generateObjectNames(validSpec);
  console.log(`✅ Object names generated: ${Object.keys(objectNames).length} names`);
  console.log(`   Streamlit app: ${objectNames.streamlit_app}`);
  
  console.log('\n');
  
  // Test 2: Conversation Analyzer
  console.log('2️⃣ Testing Conversation Analyzer');
  console.log('==================================');
  
  const analyzer = new ConversationAnalyzer();
  
  // Test dashboard intent detection
  const testConversations = [
    {
      name: 'Clear Dashboard Request',
      messages: [
        { type: 'user', content: 'Create a sales dashboard showing top 10 customers by revenue' }
      ]
    },
    {
      name: 'Multiple Metrics Request',
      messages: [
        { type: 'user', content: 'I need to track revenue and customer count trends over time' }
      ]
    },
    {
      name: 'Non-Dashboard Request',
      messages: [
        { type: 'user', content: 'What is our total revenue this quarter?' }
      ]
    },
    {
      name: 'Executive Dashboard Request',
      messages: [
        { type: 'user', content: 'Build an executive dashboard with KPIs and quarterly trends' }
      ]
    }
  ];
  
  for (const test of testConversations) {
    const intent = await analyzer.analyzeDashboardIntent(test.messages);
    console.log(`📊 ${test.name}:`);
    console.log(`   Dashboard intent: ${intent.isDashboardRequest ? 'YES' : 'NO'} (${intent.confidence}%)`);
    if (intent.isDashboardRequest) {
      console.log(`   Detected metrics: ${intent.requirements.metrics.join(', ')}`);
      console.log(`   Suggested name: ${intent.requirements.name}`);
      console.log(`   Panel types: ${intent.requirements.panels.map(p => p.type).join(', ')}`);
    }
    console.log('');
  }
  
  // Test 3: Spec Generator
  console.log('3️⃣ Testing Spec Generator');
  console.log('===========================');
  
  const generator = new SpecGenerator();
  
  // Test with high-confidence dashboard intent
  const dashboardIntent = {
    isDashboardRequest: true,
    confidence: 85,
    requirements: {
      name: 'sales_performance',
      timezone: 'America/New_York',
      metrics: ['SUM(revenue)', 'COUNT(DISTINCT customer_id)'],
      panels: [
        { type: 'table', top_n: 10 },
        { type: 'timeseries' }
      ],
      schedule: {
        mode: 'exact',
        extracted_time: '9:00 AM'
      },
      sources: [{ table: 'fact_sales', success: true }]
    }
  };
  
  try {
    const generatedSpec = await generator.generateFromIntent(dashboardIntent);
    const specValidation = validateSpec(generatedSpec);
    
    console.log(`✅ Spec generation: ${specValidation.valid ? 'PASSED' : 'FAILED'}`);
    console.log(`   Generated name: ${generatedSpec.name}`);
    console.log(`   Panels count: ${generatedSpec.panels.length}`);
    console.log(`   Schedule mode: ${generatedSpec.schedule.mode}`);
    console.log(`   Schedule cron: ${generatedSpec.schedule.cron_utc}`);
    
    if (!specValidation.valid) {
      console.log(`   Validation errors: ${specValidation.errors.length}`);
      specValidation.errors.forEach(err => {
        console.log(`     - ${err.path}: ${err.message}`);
      });
    }
  } catch (error) {
    console.log(`❌ Spec generation failed: ${error.message}`);
  }
  
  console.log('\n');
  
  // Test 4: Fallback Generation
  console.log('4️⃣ Testing Fallback Generation');
  console.log('===============================');
  
  const badIntent = {
    isDashboardRequest: true,
    confidence: 90,
    requirements: {
      name: 'bad-name!@#', // Invalid characters
      metrics: ['INVALID_METRIC()'],
      panels: [],
      schedule: { mode: 'invalid' }
    }
  };
  
  try {
    const fallbackSpec = await generator.generateFromIntent(badIntent);
    const fallbackValidation = validateSpec(fallbackSpec);
    
    console.log(`✅ Fallback generation: ${fallbackValidation.valid ? 'PASSED' : 'FAILED'}`);
    console.log(`   Fallback name: ${fallbackSpec.name}`);
    console.log(`   Using template: ${fallbackSpec.name === 'bad_name_dashboard' ? 'cleaned' : 'template'}`);
    
  } catch (error) {
    console.log(`❌ Fallback generation failed: ${error.message}`);
  }
  
  console.log('\n');
  
  // Test 5: Component Integration
  console.log('5️⃣ Testing Component Integration');
  console.log('=================================');
  
  const integrationConversation = [
    { type: 'user', content: 'I want a dashboard showing our top performing customers' },
    { type: 'user', content: 'Show revenue trends by quarter and include the top 15 customers' },
    { type: 'user', content: 'Refresh it every morning at 8am Eastern time' }
  ];
  
  try {
    // Full pipeline test
    const analysisResult = await analyzer.analyzeDashboardIntent(integrationConversation);
    
    if (analysisResult.isDashboardRequest) {
      const finalSpec = await generator.generateFromIntent(analysisResult);
      const finalValidation = validateSpec(finalSpec);
      
      console.log(`✅ Full pipeline: ${finalValidation.valid ? 'PASSED' : 'FAILED'}`);
      console.log(`   Analysis confidence: ${analysisResult.confidence}%`);
      console.log(`   Generated spec: ${finalSpec.name}`);
      console.log(`   Final hash: ${generateSpecHash(finalSpec)}`);
      
      // Show final object names that would be created
      const finalObjectNames = generateObjectNames(finalSpec);
      console.log(`   Objects to create:`);
      Object.entries(finalObjectNames).forEach(([key, name]) => {
        console.log(`     ${key}: ${name}`);
      });
      
      // Show panel details
      console.log(`   Panel details:`);
      finalSpec.panels.forEach((panel, idx) => {
        console.log(`     Panel ${idx + 1}: ${panel.type} showing ${panel.metric}`);
        console.log(`       Source: ${panel.source}, Group by: ${panel.group_by?.join(', ')}`);
      });
      
    } else {
      console.log(`❌ Integration test failed: No dashboard intent detected`);
    }
    
  } catch (error) {
    console.log(`❌ Integration test failed: ${error.message}`);
  }
  
  console.log('\n');
  
  // Test 6: Component Versions
  console.log('6️⃣ Component Version Information');
  console.log('==================================');
  
  const analyzerInfo = analyzer.getVersion();
  const generatorInfo = generator.getVersion();
  
  console.log(`📊 Conversation Analyzer v${analyzerInfo.version}`);
  console.log(`   Dashboard patterns: ${analyzerInfo.capabilities.dashboard_patterns}`);
  console.log(`   Metric patterns: ${analyzerInfo.capabilities.metric_patterns}`);
  console.log(`   Panel types: ${analyzerInfo.capabilities.panel_types}`);
  
  console.log(`📝 Spec Generator v${generatorInfo.version}`);
  console.log(`   Metric mappings: ${generatorInfo.capabilities.metric_mappings}`);
  console.log(`   Panel configurations: ${generatorInfo.capabilities.panel_types}`);
  console.log(`   Schedule modes: ${generatorInfo.capabilities.schedule_modes}`);
  console.log(`   Fallback specs: ${generatorInfo.capabilities.fallback_specs}`);
  
  console.log('\n🎉 Component testing completed!');
}

// Run the tests
testComponents().catch(console.error);