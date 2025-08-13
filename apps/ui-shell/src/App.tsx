import React, { useState, useEffect } from 'react';
import Chat from './components/Chat';
import ResultsTable from './components/ResultsTable';
import AuditPanel from './components/AuditPanel';
import { Database, Activity, Shield } from 'lucide-react';

function App() {
  const [ws, setWs] = useState<WebSocket | null>(null);
  const [sessionId, setSessionId] = useState<string>('');
  const [activeTab, setActiveTab] = useState<'chat' | 'results' | 'audit'>('chat');
  const [queryResults, setQueryResults] = useState<any[]>([]);
  const [auditResults, setAuditResults] = useState<any[]>([]);

  useEffect(() => {
    // Connect to Claude Code Bridge WebSocket
    const websocket = new WebSocket('ws://localhost:8080');
    
    websocket.onopen = () => {
      console.log('Connected to Claude Code Bridge');
    };

    websocket.onmessage = (event) => {
      const data = JSON.parse(event.data);
      
      switch (data.type) {
        case 'connection':
          setSessionId(data.sessionId);
          break;
        case 'query_results':
          setQueryResults(data.results);
          setActiveTab('results');
          break;
        case 'audit_result':
          setAuditResults(prev => [...prev, data.audit]);
          break;
      }
    };

    websocket.onerror = (error) => {
      console.error('WebSocket error:', error);
    };

    setWs(websocket);

    return () => {
      websocket.close();
    };
  }, []);

  return (
    <div className="app">
      <header className="header">
        <div className="header-title">
          <Database className="icon" />
          <h1>SnowflakePOC2</h1>
          <span className="subtitle">Powered by Claude Code</span>
        </div>
        <div className="session-info">
          Session: {sessionId.slice(0, 8)}...
        </div>
      </header>

      <nav className="tabs">
        <button 
          className={`tab ${activeTab === 'chat' ? 'active' : ''}`}
          onClick={() => setActiveTab('chat')}
        >
          <Activity size={16} />
          Chat
        </button>
        <button 
          className={`tab ${activeTab === 'results' ? 'active' : ''}`}
          onClick={() => setActiveTab('results')}
        >
          <Database size={16} />
          Results ({queryResults.length})
        </button>
        <button 
          className={`tab ${activeTab === 'audit' ? 'active' : ''}`}
          onClick={() => setActiveTab('audit')}
        >
          <Shield size={16} />
          Audit ({auditResults.length})
        </button>
      </nav>

      <main className="content">
        {activeTab === 'chat' && <Chat ws={ws} sessionId={sessionId} />}
        {activeTab === 'results' && <ResultsTable data={queryResults} />}
        {activeTab === 'audit' && <AuditPanel results={auditResults} />}
      </main>
    </div>
  );
}

export default App;