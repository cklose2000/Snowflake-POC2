// Progress Reporter for Dashboard Factory
// Sends real-time updates via WebSocket during dashboard creation

class ProgressReporter {
  constructor(websocket, sessionId) {
    this.ws = websocket;
    this.sessionId = sessionId;
    this.startTime = Date.now();
    
    // Step definitions with estimated duration
    this.steps = {
      'analyze_conversation': { 
        message: 'Analyzing conversation for dashboard intent...', 
        pct: 10,
        estimatedMs: 500
      },
      'generate_spec': { 
        message: 'Generating dashboard specification...', 
        pct: 20,
        estimatedMs: 1000
      },
      'validate_spec': { 
        message: 'Validating dashboard configuration...', 
        pct: 30,
        estimatedMs: 200
      },
      'preflight_checks': { 
        message: 'Running preflight checks (data availability, permissions)...', 
        pct: 40,
        estimatedMs: 2000
      },
      'create_objects': { 
        message: 'Creating Snowflake views and tasks...', 
        pct: 60,
        estimatedMs: 3000
      },
      'generate_streamlit': { 
        message: 'Generating Streamlit dashboard code...', 
        pct: 80,
        estimatedMs: 500
      },
      'deploy_app': { 
        message: 'Deploying dashboard application...', 
        pct: 90,
        estimatedMs: 1000
      },
      'log_completion': { 
        message: 'Finalizing dashboard...', 
        pct: 95,
        estimatedMs: 200
      }
    };
    
    this.currentStep = null;
    this.completedSteps = [];
  }
  
  // Send progress update via WebSocket
  sendProgress(step, metadata = {}) {
    if (!this.ws || this.ws.readyState !== 1) {
      console.warn('WebSocket not available for progress reporting');
      return;
    }
    
    const stepInfo = this.steps[step];
    if (!stepInfo) {
      console.warn(`Unknown step: ${step}`);
      return;
    }
    
    this.currentStep = step;
    this.completedSteps.push(step);
    
    const elapsed = Date.now() - this.startTime;
    
    const progressMessage = {
      type: 'dashboard.progress',
      sessionId: this.sessionId,
      step: step,
      message: stepInfo.message,
      pct: stepInfo.pct,
      elapsed_ms: elapsed,
      completed_steps: this.completedSteps.length,
      total_steps: Object.keys(this.steps).length,
      metadata: metadata
    };
    
    try {
      this.ws.send(JSON.stringify(progressMessage));
      console.log(`📊 Progress: ${step} (${stepInfo.pct}%)`);
    } catch (error) {
      console.error('Failed to send progress:', error);
    }
  }
  
  // Send error notification
  sendError(step, error) {
    if (!this.ws || this.ws.readyState !== 1) return;
    
    const errorMessage = {
      type: 'dashboard.error',
      sessionId: this.sessionId,
      step: step,
      error: error.message || error,
      elapsed_ms: Date.now() - this.startTime,
      last_completed_step: this.completedSteps[this.completedSteps.length - 1]
    };
    
    try {
      this.ws.send(JSON.stringify(errorMessage));
      console.error(`❌ Error at ${step}: ${error.message || error}`);
    } catch (err) {
      console.error('Failed to send error:', err);
    }
  }
  
  // Send completion notification
  sendCompletion(result) {
    if (!this.ws || this.ws.readyState !== 1) return;
    
    const completionMessage = {
      type: 'dashboard.complete',
      sessionId: this.sessionId,
      success: result.success,
      url: result.url,
      name: result.name,
      spec_id: result.specId,
      elapsed_ms: Date.now() - this.startTime,
      objects_created: result.objectsCreated,
      panels_count: result.panelsCount
    };
    
    try {
      this.ws.send(JSON.stringify(completionMessage));
      console.log(`✅ Dashboard complete: ${result.url}`);
    } catch (error) {
      console.error('Failed to send completion:', error);
    }
  }
  
  // Check if WebSocket is still alive
  isConnected() {
    return this.ws && this.ws.readyState === 1;
  }
  
  // Get estimated remaining time
  getEstimatedRemaining() {
    const remainingSteps = Object.keys(this.steps).filter(
      s => !this.completedSteps.includes(s)
    );
    
    const remainingMs = remainingSteps.reduce((sum, step) => {
      return sum + (this.steps[step].estimatedMs || 1000);
    }, 0);
    
    return remainingMs;
  }
}

module.exports = ProgressReporter;