#!/usr/bin/env node

/**
 * Contract Compliance Linter
 * Validates that the codebase follows the schema contract rules
 */

const fs = require('fs');
const path = require('path');
const { exec } = require('child_process');
const { promisify } = require('util');

const execAsync = promisify(exec);

// Load validation patterns from generated schema
const { VALIDATION_PATTERNS, CONTRACT_HASH } = require('../packages/snowflake-schema/generated.js');

class ContractLinter {
  constructor() {
    this.violations = [];
    this.warnings = [];
    this.checkedFiles = 0;
  }

  async run() {
    console.log('🔍 Running contract compliance checks...');
    console.log(`📋 Contract hash: ${CONTRACT_HASH}\n`);

    try {
      // Check 1: Raw FQN violations
      await this.checkRawFQNs();
      
      // Check 2: Unqualified view references
      await this.checkUnqualifiedViews();
      
      // Check 3: SQL injection patterns
      await this.checkSQLInjection();
      
      // Check 4: Generated files sync
      await this.checkGeneratedSync();
      
      // Check 5: Package structure compliance
      await this.checkPackageStructure();

      // Report results
      this.reportResults();
      
    } catch (error) {
      console.error('❌ Linter error:', error.message);
      process.exit(1);
    }
  }

  async checkRawFQNs() {
    console.log('🔧 Checking raw FQN usage...');
    
    try {
      const { stdout } = await execAsync(
        `find packages -name "*.js" -not -path "*/node_modules/*" -not -name "generated.js" | xargs grep -l '\\b[A-Za-z0-9_]\\+\\.[A-Za-z0-9_]\\+\\.[A-Za-z0-9_]\\+'`
      );
      
      if (stdout.trim()) {
        const files = stdout.trim().split('\n');
        files.forEach(file => {
          this.violations.push({
            type: 'raw_fqn',
            file,
            message: 'Raw FQN detected - use fqn() or qualifySource() helpers',
            rule: 'no_raw_fqns'
          });
        });
      }
    } catch (error) {
      // No matches found (grep returns non-zero exit code)
      if (error.code !== 1) {
        throw error;
      }
    }
  }

  async checkUnqualifiedViews() {
    console.log('🔧 Checking unqualified view references...');
    
    try {
      const { stdout } = await execAsync(
        `find packages -name "*.js" -not -path "*/node_modules/*" -not -name "generated.js" | xargs grep -H 'VW_[A-Z0-9_]\\+' | grep -v 'ACTIVITY_CCODE\\.'`
      );
      
      if (stdout.trim()) {
        stdout.trim().split('\n').forEach(line => {
          const [file, ...content] = line.split(':');
          this.violations.push({
            type: 'unqualified_view',
            file,
            content: content.join(':'),
            message: 'Unqualified view reference - use ACTIVITY_CCODE.VW_* or qualifySource()',
            rule: 'no_unqualified_views'
          });
        });
      }
    } catch (error) {
      if (error.code !== 1) {
        throw error;
      }
    }
  }

  async checkSQLInjection() {
    console.log('🔧 Checking SQL injection patterns...');
    
    try {
      const { stdout } = await execAsync(
        `find packages -name "*.js" -not -path "*/node_modules/*" -not -path "*/test/*" | xargs grep -H '\\$\{[^}]\\+\}'`
      );
      
      if (stdout.trim()) {
        stdout.trim().split('\n').forEach(line => {
          const [file, ...content] = line.split(':');
          this.violations.push({
            type: 'sql_injection',
            file,
            content: content.join(':'),
            message: 'Potential SQL injection - use parameterized queries with ? placeholders',
            rule: 'parameterized_sql'
          });
        });
      }
    } catch (error) {
      if (error.code !== 1) {
        throw error;
      }
    }
  }

  async checkGeneratedSync() {
    console.log('🔧 Checking generated files synchronization...');
    
    const generatedPath = path.join(__dirname, '../packages/snowflake-schema/generated.js');
    const contractPath = path.join(__dirname, '../schemas/activity_v2.contract.json');
    
    // Check if generated file is newer than contract
    const generatedStat = fs.statSync(generatedPath);
    const contractStat = fs.statSync(contractPath);
    
    if (contractStat.mtime > generatedStat.mtime) {
      this.violations.push({
        type: 'sync_issue',
        file: 'packages/snowflake-schema/generated.js',
        message: 'Generated file is older than contract - run npm run codegen',
        rule: 'generated_sync'
      });
    }

    // Verify hash matches
    const { generateContractHash } = require('./codegen-schema.js');
    const currentHash = generateContractHash();
    
    if (currentHash !== CONTRACT_HASH) {
      this.violations.push({
        type: 'hash_mismatch',
        file: 'packages/snowflake-schema/generated.js',
        message: `Contract hash mismatch: expected ${currentHash}, got ${CONTRACT_HASH}`,
        rule: 'generated_sync'
      });
    }
  }

  async checkPackageStructure() {
    console.log('🔧 Checking package structure compliance...');
    
    // Check that all packages use the generated schema
    const packageDirs = fs.readdirSync(path.join(__dirname, '../packages'))
      .filter(dir => fs.statSync(path.join(__dirname, '../packages', dir)).isDirectory());

    for (const dir of packageDirs) {
      const packagePath = path.join(__dirname, '../packages', dir);
      const files = this.getJSFiles(packagePath);
      
      for (const file of files) {
        this.checkedFiles++;
        
        // Skip generated.js and test files
        if (file.includes('generated.js') || file.includes('/test/')) {
          continue;
        }
        
        const content = fs.readFileSync(file, 'utf8');
        
        // Look for schema imports - they should be from generated.js
        if (content.includes('snowflake-schema') && !content.includes('generated.js')) {
          this.warnings.push({
            type: 'import_warning',
            file,
            message: 'Consider importing from generated.js for type safety',
            rule: 'package_structure'
          });
        }
      }
    }
  }

  getJSFiles(dir) {
    const files = [];
    const items = fs.readdirSync(dir);
    
    for (const item of items) {
      const fullPath = path.join(dir, item);
      const stat = fs.statSync(fullPath);
      
      if (stat.isDirectory() && item !== 'node_modules') {
        files.push(...this.getJSFiles(fullPath));
      } else if (item.endsWith('.js')) {
        files.push(fullPath);
      }
    }
    
    return files;
  }

  reportResults() {
    console.log('\n📊 Contract Compliance Report');
    console.log('=' .repeat(50));
    
    if (this.violations.length === 0 && this.warnings.length === 0) {
      console.log('✅ All checks passed!');
      console.log(`📁 Checked ${this.checkedFiles} files`);
      return;
    }

    // Report violations
    if (this.violations.length > 0) {
      console.log(`\n❌ ${this.violations.length} violation(s) found:\n`);
      
      this.violations.forEach((violation, i) => {
        console.log(`${i + 1}. ${violation.type.toUpperCase()}`);
        console.log(`   File: ${violation.file}`);
        console.log(`   Rule: ${violation.rule}`);
        console.log(`   Message: ${violation.message}`);
        if (violation.content) {
          console.log(`   Context: ${violation.content.substring(0, 100)}...`);
        }
        console.log('');
      });
    }

    // Report warnings
    if (this.warnings.length > 0) {
      console.log(`\n⚠️  ${this.warnings.length} warning(s):\n`);
      
      this.warnings.forEach((warning, i) => {
        console.log(`${i + 1}. ${warning.message}`);
        console.log(`   File: ${warning.file}`);
        console.log('');
      });
    }

    console.log(`📁 Checked ${this.checkedFiles} files`);
    
    // Exit with error code if violations found
    if (this.violations.length > 0) {
      console.log('\n💡 Fix violations above before committing');
      process.exit(1);
    }
  }
}

// Run if called directly
if (require.main === module) {
  const linter = new ContractLinter();
  linter.run().catch(console.error);
}

module.exports = ContractLinter;