import React from 'react';
import { CheckCircle, XCircle, AlertCircle, Clock } from 'lucide-react';

interface AuditResult {
  id: string;
  claim: string;
  status: 'passed' | 'failed' | 'pending';
  findings: string[];
  timestamp: Date;
  remediation?: string;
}

interface AuditPanelProps {
  results: AuditResult[];
}

export default function AuditPanel({ results }: AuditPanelProps) {
  const getStatusIcon = (status: string) => {
    switch (status) {
      case 'passed':
        return <CheckCircle className="status-icon passed" size={20} />;
      case 'failed':
        return <XCircle className="status-icon failed" size={20} />;
      case 'pending':
        return <Clock className="status-icon pending" size={20} />;
      default:
        return <AlertCircle className="status-icon" size={20} />;
    }
  };

  const stats = {
    total: results.length,
    passed: results.filter(r => r.status === 'passed').length,
    failed: results.filter(r => r.status === 'failed').length,
    pending: results.filter(r => r.status === 'pending').length
  };

  const passRate = stats.total > 0 
    ? ((stats.passed / stats.total) * 100).toFixed(1)
    : '0';

  return (
    <div className="audit-container">
      <div className="audit-header">
        <h2>Victory Audit Results</h2>
        <div className="audit-stats">
          <div className="stat">
            <span className="stat-label">Pass Rate</span>
            <span className="stat-value">{passRate}%</span>
          </div>
          <div className="stat">
            <span className="stat-label">Total</span>
            <span className="stat-value">{stats.total}</span>
          </div>
          <div className="stat passed">
            <span className="stat-label">Passed</span>
            <span className="stat-value">{stats.passed}</span>
          </div>
          <div className="stat failed">
            <span className="stat-label">Failed</span>
            <span className="stat-value">{stats.failed}</span>
          </div>
          <div className="stat pending">
            <span className="stat-label">Pending</span>
            <span className="stat-value">{stats.pending}</span>
          </div>
        </div>
      </div>

      <div className="audit-list">
        {results.length === 0 ? (
          <div className="empty-state">
            <AlertCircle size={48} />
            <p>No audit results yet</p>
            <p className="hint">Claims will be automatically audited as they're made</p>
          </div>
        ) : (
          results.map(result => (
            <div key={result.id} className={`audit-item ${result.status}`}>
              <div className="audit-item-header">
                {getStatusIcon(result.status)}
                <div className="audit-item-info">
                  <div className="audit-claim">{result.claim}</div>
                  <div className="audit-time">
                    {result.timestamp.toLocaleString()}
                  </div>
                </div>
              </div>
              
              {result.findings.length > 0 && (
                <div className="audit-findings">
                  <h4>Findings:</h4>
                  <ul>
                    {result.findings.map((finding, idx) => (
                      <li key={idx}>{finding}</li>
                    ))}
                  </ul>
                </div>
              )}
              
              {result.remediation && (
                <div className="audit-remediation">
                  <h4>Remediation:</h4>
                  <p>{result.remediation}</p>
                </div>
              )}
            </div>
          ))
        )}
      </div>
    </div>
  );
}