// WebSocket Server - Handles UI communication
import { WebSocketServer } from 'ws';
import { v4 as uuidv4 } from 'uuid';

export class WSServer {
  constructor(port = 8080) {
    this.wss = new WebSocketServer({ port });
    this.clients = new Map();
    this.sessions = new Map();
    
    this.setupHandlers();
  }

  setupHandlers() {
    this.wss.on('connection', (ws) => {
      const clientId = uuidv4();
      const sessionId = `session_${uuidv4()}`;
      
      this.clients.set(clientId, {
        ws,
        sessionId,
        connected: new Date()
      });
      
      console.log(`Client ${clientId} connected with session ${sessionId}`);
      
      // Send initial connection confirmation
      ws.send(JSON.stringify({
        type: 'connection',
        clientId,
        sessionId
      }));

      ws.on('message', (message) => {
        this.handleMessage(clientId, message);
      });

      ws.on('close', () => {
        console.log(`Client ${clientId} disconnected`);
        this.clients.delete(clientId);
      });

      ws.on('error', (error) => {
        console.error(`WebSocket error for client ${clientId}:`, error);
      });
    });
  }

  handleMessage(clientId, message) {
    const client = this.clients.get(clientId);
    if (!client) return;

    try {
      const data = JSON.parse(message);
      
      // Add session context
      data.sessionId = client.sessionId;
      data.clientId = clientId;
      
      // Emit message for bridge to handle
      this.emit('message', data, client.ws);
    } catch (error) {
      console.error('Failed to parse message:', error);
      client.ws.send(JSON.stringify({
        type: 'error',
        error: 'Invalid message format'
      }));
    }
  }

  broadcast(message) {
    const messageStr = JSON.stringify(message);
    this.clients.forEach(client => {
      if (client.ws.readyState === 1) { // WebSocket.OPEN
        client.ws.send(messageStr);
      }
    });
  }

  sendToClient(clientId, message) {
    const client = this.clients.get(clientId);
    if (client && client.ws.readyState === 1) {
      client.ws.send(JSON.stringify(message));
    }
  }

  sendToSession(sessionId, message) {
    const messageStr = JSON.stringify(message);
    this.clients.forEach(client => {
      if (client.sessionId === sessionId && client.ws.readyState === 1) {
        client.ws.send(messageStr);
      }
    });
  }

  // Event emitter functionality
  emit(event, ...args) {
    if (this.handlers && this.handlers[event]) {
      this.handlers[event].forEach(handler => handler(...args));
    }
  }

  on(event, handler) {
    if (!this.handlers) this.handlers = {};
    if (!this.handlers[event]) this.handlers[event] = [];
    this.handlers[event].push(handler);
  }
}