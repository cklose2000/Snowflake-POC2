// Victory Audit - Validates all success claims
const fs = require('fs');
const path = require('path');
const glob = require('glob');

const SUCCESS_PATTERNS = [
  /✅/g,
  /\bcomplete\b/gi,
  /\bsuccessfully\b/gi, 
  /\bread(y|iness)\b/gi,
  /\d+%/g,
  /production.ready/gi
];

function auditClaims() {
  const files = [
    'README.md', 
    'CHANGELOG.md',
    'apps/*/README.md',
    'packages/*/README.md'
  ];
  
  let claims = [];
  
  files.forEach(pattern => {
    const matches = pattern.includes('*') 
      ? glob.sync(pattern)
      : [pattern];
      
    matches.forEach(file => {
      if (fs.existsSync(file)) {
        const content = fs.readFileSync(file, 'utf8');
        SUCCESS_PATTERNS.forEach(pattern => {
          const found = content.match(pattern);
          if (found) {
            claims.push(...found.map(m => ({ 
              file, 
              claim: m,
              line: content.split('\n').findIndex(line => line.includes(m)) + 1
            })));
          }
        });
      }
    });
  });
  
  console.log(`🔍 Victory Audit: Found ${claims.length} success claims`);
  
  if (claims.length > 0) {
    console.log('\n📋 Claims requiring verification:');
    claims.forEach(c => console.log(`  ${c.file}:${c.line} - "${c.claim}"`));
    console.log('\n⚠️  All claims must be verified before production deployment');
    return false;
  }
  
  console.log('No unverified success claims found');
  return true;
}

if (require.main === module) {
  const passed = auditClaims();
  process.exit(passed ? 0 : 1);
}

module.exports = { auditClaims };