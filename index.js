const WebSocket = require('ws');
const express = require('express');
const http = require('http');
const { LRUCache } = require('lru-cache');

// Configuration
const PORT = process.env.PORT || 3002;
const BYBIT_WS_URL = 'wss://stream.bybit.com/v5/public/spot';
const RECONNECT_INTERVAL = 5000;
const PING_INTERVAL = 20000;

// Initialize Express app and HTTP server
const app = express();
const server = http.createServer(app);

// Initialize WebSocket server
const wss = new WebSocket.Server({ server });

// Cache for storing latest data
const dataCache = new LRUCache({
  max: 100,
  ttl: 60 * 60 * 1000, // 1 hour
});

// Connection state tracking
let bybitWs = null;
let isConnecting = false;
let subscribedTopics = new Set();
let clientSubscriptions = new Map();
let pingInterval;

// Connect to Bybit WebSocket
function connectToBybit() {
  if (isConnecting || (bybitWs && bybitWs.readyState === WebSocket.OPEN)) return;

  isConnecting = true;
  console.log('Connecting to Bybit WebSocket...');

  bybitWs = new WebSocket(BYBIT_WS_URL);

  bybitWs.on('open', () => {
    console.log('Connected to Bybit WebSocket');
    isConnecting = false;

    if (subscribedTopics.size > 0) {
      const topics = Array.from(subscribedTopics);
      subscribeToBybit(topics);
    }

    startPingInterval();
  });

  bybitWs.on('message', (data) => {
    try {
      const message = JSON.parse(data);
      console.log('Received from Bybit:', JSON.stringify(message));

      if (message.op === 'pong') return;

      if (message.op === 'subscribe' && message.success === true) {
        console.log('Successfully subscribed to Bybit topics:', message.args);
        return;
      }

      if (message.op === 'subscribe' && message.success === false) {
        console.error('Subscription failed:', message.ret_msg);
        return;
      }

      if (message.topic && message.data) {
        dataCache.set(message.topic, message);
        broadcastToSubscribers(message.topic, JSON.stringify(message));
      } else {
        console.log('No topic or data in message:', message);
      }
    } catch (err) {
      console.error('Error processing Bybit message:', err);
    }
  });

  bybitWs.on('error', (error) => {
    console.error('Bybit WebSocket error:', error);
  });

  bybitWs.on('close', () => {
    console.log('Bybit WebSocket connection closed, attempting to reconnect...');
    clearInterval(pingInterval);
    isConnecting = false;
    setTimeout(connectToBybit, RECONNECT_INTERVAL);
  });
}

// Keep-alive ping
function startPingInterval() {
  clearInterval(pingInterval);
  pingInterval = setInterval(() => {
    if (bybitWs && bybitWs.readyState === WebSocket.OPEN) {
      bybitWs.send(JSON.stringify({ op: 'ping' }));
    }
  }, PING_INTERVAL);
}

// Subscribe to Bybit topics
function subscribeToBybit(topics) {
  if (!bybitWs || bybitWs.readyState !== WebSocket.OPEN) {
    connectToBybit();
    return;
  }

  bybitWs.send(JSON.stringify({ op: 'subscribe', args: topics }));
  topics.forEach(topic => subscribedTopics.add(topic));
}

// Clean up subscriptions
function cleanupSubscriptions() {
  const clientTopics = new Set();
  for (const topics of clientSubscriptions.values()) {
    topics.forEach(topic => clientTopics.add(topic));
  }

  const topicsToUnsubscribe = Array.from(subscribedTopics).filter(
    topic => !clientTopics.has(topic)
  );

  if (
    topicsToUnsubscribe.length > 0 &&
    bybitWs &&
    bybitWs.readyState === WebSocket.OPEN
  ) {
    bybitWs.send(JSON.stringify({ op: 'unsubscribe', args: topicsToUnsubscribe }));
    topicsToUnsubscribe.forEach(topic => subscribedTopics.delete(topic));
  }
}

// Broadcast to subscribers
function broadcastToSubscribers(topic, data) {
  wss.clients.forEach(client => {
    if (
      client.readyState === WebSocket.OPEN &&
      clientSubscriptions.has(client) &&
      clientSubscriptions.get(client).has(topic)
    ) {
      client.send(data);
      console.log(`Sent to client for topic ${topic}:`, data);
    }
  });
}

// Handle client connections
wss.on('connection', (ws) => {
  console.log('Client connected');
  clientSubscriptions.set(ws, new Set());

  ws.send(
    JSON.stringify({
      event: 'connection',
      status: 'connected',
      timestamp: Date.now(),
    })
  );

  ws.on('message', (message) => {
    try {
      const data = JSON.parse(message);

      if (data.op === 'subscribe' && Array.isArray(data.args)) {
        const clientTopics = clientSubscriptions.get(ws) || new Set();
        data.args.forEach(topic => clientTopics.add(topic));
        clientSubscriptions.set(ws, clientTopics);

        const newTopics = data.args.filter(topic => !subscribedTopics.has(topic));
        if (newTopics.length > 0) {
          subscribeToBybit(newTopics);
          console.log('Client subscribed to:', newTopics);
        }

        data.args.forEach(topic => {
          const cachedData = dataCache.get(topic);
          if (cachedData) {
            ws.send(JSON.stringify(cachedData));
            console.log(`Sent cached data for ${topic}:`, JSON.stringify(cachedData));
          }
        });

        ws.send(
          JSON.stringify({
            op: 'subscribe',
            success: true,
            args: data.args,
          })
        );
      }

      if (data.op === 'unsubscribe' && Array.isArray(data.args)) {
        const clientTopics = clientSubscriptions.get(ws);
        if (clientTopics) {
          data.args.forEach(topic => clientTopics.delete(topic));
        }
        cleanupSubscriptions();

        ws.send(
          JSON.stringify({
            op: 'unsubscribe',
            success: true,
            args: data.args,
          })
        );
      }

      if (data.op === 'ping') {
        ws.send(JSON.stringify({ op: 'pong', ts: Date.now() }));
      }
    } catch (err) {
      console.error('Error processing client message:', err);
      ws.send(
        JSON.stringify({
          error: 'Invalid message format',
          timestamp: Date.now(),
        })
      );
    }
  });

  ws.on('close', () => {
    console.log('Client disconnected');
    clientSubscriptions.delete(ws);
    cleanupSubscriptions();
  });

  ws.on('error', (error) => {
    console.error('WebSocket error:', error);
    clientSubscriptions.delete(ws);
  });
});

// Status endpoint
app.get('/status', (req, res) => {
  res.json({
    status: 'ok',
    bybitConnection: bybitWs ? bybitWs.readyState : 'not connected',
    activeTopics: Array.from(subscribedTopics),
    connectedClients: wss.clients.size,
  });
});

// Start server
server.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
  connectToBybit();
});

// Graceful shutdown
process.on('SIGTERM', () => {
  console.log('SIGTERM received, shutting down gracefully');
  server.close(() => {
    console.log('HTTP server closed');
    if (bybitWs) bybitWs.close();
    process.exit(0);
  });
});