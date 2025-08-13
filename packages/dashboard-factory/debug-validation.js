// Debug validation issue
const SpecGenerator = require('./spec-generator');
const { validateSpec } = require('./schema');

async function debugValidation() {
  const generator = new SpecGenerator();
  
  const badIntent = {
    isDashboardRequest: true,
    confidence: 90,
    requirements: {
      name: 'bad-name!@#', 
      metrics: ['INVALID_METRIC()'],
      panels: [],
      schedule: { mode: 'invalid' }
    }
  };
  
  try {
    console.log('🔍 Testing bad intent:');
    console.log('Input metrics:', badIntent.requirements.metrics);
    
    // Test metric selection
    const selectedMetric = generator.selectMetric(badIntent.requirements.metrics);
    console.log('Selected metric:', selectedMetric);
    
    // Generate spec
    const spec = await generator.generateFromIntent(badIntent);
    console.log('Generated spec:', JSON.stringify(spec, null, 2));
    
    const validation = validateSpec(spec);
    console.log('Validation result:', validation);
    
  } catch (error) {
    console.error('Error:', error.message);
  }
}

debugValidation();