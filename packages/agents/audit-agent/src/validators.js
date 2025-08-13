// Claim Validators - Specific validation logic for different claim types
export class ClaimValidator {
  constructor() {
    this.validators = {
      percentage: this.validatePercentageClaim,
      completion: this.validateCompletionClaim,
      performance: this.validatePerformanceClaim,
      readiness: this.validateReadinessClaim,
      quality: this.validateQualityClaim
    };
  }

  async validate(claimType, claim, evidence) {
    const validator = this.validators[claimType];
    if (!validator) {
      return this.genericValidation(claim, evidence);
    }
    
    return validator.call(this, claim, evidence);
  }

  validatePercentageClaim(claim, evidence) {
    const percentageMatch = claim.match(/(\d+(?:\.\d+)?)%/);
    if (!percentageMatch) {
      return {
        valid: false,
        reason: 'No percentage value found in claim'
      };
    }

    const claimedPercentage = parseFloat(percentageMatch[1]);
    
    // Check if percentage is realistic
    if (claimedPercentage > 100) {
      return {
        valid: false,
        reason: `Percentage ${claimedPercentage}% exceeds 100%`
      };
    }

    // Check if evidence supports the percentage
    if (evidence && evidence.actualPercentage !== undefined) {
      const tolerance = 5; // 5% tolerance
      const difference = Math.abs(claimedPercentage - evidence.actualPercentage);
      
      if (difference <= tolerance) {
        return {
          valid: true,
          reason: `Percentage claim verified (${claimedPercentage}% ≈ ${evidence.actualPercentage}%)`
        };
      } else {
        return {
          valid: false,
          reason: `Claimed ${claimedPercentage}% but actual is ${evidence.actualPercentage}%`
        };
      }
    }

    return {
      valid: null,
      reason: 'Cannot verify percentage without baseline data'
    };
  }

  validateCompletionClaim(claim, evidence) {
    const completionKeywords = [
      'complete', 'completed', 'done', 'finished', 
      'ready', 'deployed', 'implemented'
    ];
    
    const hasCompletionKeyword = completionKeywords.some(keyword => 
      claim.toLowerCase().includes(keyword)
    );

    if (!hasCompletionKeyword) {
      return {
        valid: null,
        reason: 'Completion claim unclear'
      };
    }

    // Check for evidence of incompleteness
    if (evidence) {
      if (evidence.pendingTasks && evidence.pendingTasks > 0) {
        return {
          valid: false,
          reason: `${evidence.pendingTasks} tasks still pending`
        };
      }

      if (evidence.failedTests && evidence.failedTests > 0) {
        return {
          valid: false,
          reason: `${evidence.failedTests} tests failing`
        };
      }

      if (evidence.missingArtifacts && evidence.missingArtifacts.length > 0) {
        return {
          valid: false,
          reason: `Missing artifacts: ${evidence.missingArtifacts.join(', ')}`
        };
      }
    }

    return {
      valid: true,
      reason: 'Completion claim appears valid'
    };
  }

  validatePerformanceClaim(claim, evidence) {
    const performanceMetrics = {
      latency: /(\d+)\s*(ms|milliseconds|seconds)/i,
      throughput: /(\d+)\s*(qps|rps|tps)/i,
      responseTime: /(\d+(?:\.\d+)?)\s*(ms|s)/i,
      speed: /([\d.]+)x\s*faster/i
    };

    const extractedMetrics = {};
    for (const [metric, pattern] of Object.entries(performanceMetrics)) {
      const match = claim.match(pattern);
      if (match) {
        extractedMetrics[metric] = {
          value: parseFloat(match[1]),
          unit: match[2]
        };
      }
    }

    if (Object.keys(extractedMetrics).length === 0) {
      return {
        valid: null,
        reason: 'No measurable performance metrics in claim'
      };
    }

    // Validate against evidence
    if (evidence && evidence.actualMetrics) {
      const tolerance = 0.15; // 15% tolerance for performance
      
      for (const [metric, claimed] of Object.entries(extractedMetrics)) {
        if (evidence.actualMetrics[metric]) {
          const actual = evidence.actualMetrics[metric].value;
          const claimedValue = claimed.value;
          const percentDiff = Math.abs(claimedValue - actual) / actual;
          
          if (percentDiff > tolerance) {
            return {
              valid: false,
              reason: `${metric}: claimed ${claimedValue}${claimed.unit} but actual is ${actual}${claimed.unit}`
            };
          }
        }
      }
    }

    return {
      valid: true,
      reason: 'Performance metrics validated'
    };
  }

  validateReadinessClaim(claim, evidence) {
    const readinessIndicators = [
      'production ready',
      'ready for deployment',
      'ready to ship',
      'ready for release',
      'ready for production'
    ];

    const hasReadinessIndicator = readinessIndicators.some(indicator =>
      claim.toLowerCase().includes(indicator)
    );

    if (!hasReadinessIndicator) {
      return {
        valid: null,
        reason: 'Not a readiness claim'
      };
    }

    // Production readiness checklist
    const readinessChecks = {
      hasTests: evidence?.hasTests ?? false,
      hasDocumentation: evidence?.hasDocumentation ?? false,
      hasErrorHandling: evidence?.hasErrorHandling ?? false,
      hasLogging: evidence?.hasLogging ?? false,
      hasMonitoring: evidence?.hasMonitoring ?? false,
      passesLinting: evidence?.passesLinting ?? false,
      hasSecurityReview: evidence?.hasSecurityReview ?? false
    };

    const failedChecks = Object.entries(readinessChecks)
      .filter(([_, passed]) => !passed)
      .map(([check, _]) => check);

    if (failedChecks.length > 0) {
      return {
        valid: false,
        reason: `Not production ready: ${failedChecks.join(', ')} missing`
      };
    }

    return {
      valid: true,
      reason: 'Production readiness criteria met'
    };
  }

  validateQualityClaim(claim, evidence) {
    const qualityKeywords = [
      'high quality',
      'bug free',
      'error free',
      'robust',
      'reliable',
      'accurate'
    ];

    const hasQualityKeyword = qualityKeywords.some(keyword =>
      claim.toLowerCase().includes(keyword)
    );

    if (!hasQualityKeyword) {
      return {
        valid: null,
        reason: 'Not a quality claim'
      };
    }

    if (evidence) {
      // Check for quality issues
      if (evidence.bugCount && evidence.bugCount > 0) {
        return {
          valid: false,
          reason: `${evidence.bugCount} known bugs exist`
        };
      }

      if (evidence.codeSmells && evidence.codeSmells > 5) {
        return {
          valid: false,
          reason: `${evidence.codeSmells} code smells detected`
        };
      }

      if (evidence.testCoverage && evidence.testCoverage < 80) {
        return {
          valid: false,
          reason: `Test coverage only ${evidence.testCoverage}%`
        };
      }

      if (evidence.duplicateCode && evidence.duplicateCode > 5) {
        return {
          valid: false,
          reason: `${evidence.duplicateCode}% code duplication`
        };
      }
    }

    return {
      valid: true,
      reason: 'Quality standards met'
    };
  }

  genericValidation(claim, evidence) {
    // For claims that don't fit specific categories
    if (claim.includes('✅')) {
      // Check mark implies completion/success
      if (evidence && evidence.hasErrors) {
        return {
          valid: false,
          reason: 'Success indicator present but errors detected'
        };
      }
    }

    return {
      valid: null,
      reason: 'Generic claim - manual verification recommended'
    };
  }
}