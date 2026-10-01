require('dotenv').config();
const express = require('express');
const { createServer } = require('http');
const WebSocket = require('ws');
const { Tickerall } = require('@tickerall/sdk');
const cors = require('cors');

const app = express();
app.use(cors());
app.use(express.json());

const server = createServer(app);
const wss = new WebSocket.Server({ server });

const ticker = new Tickerall({
  apiKey: process.env.TICKERALL_API_KEY,
});

let currentSessionId = null;

async function startTickerAllSession() {
  try {
    console.log('⏳ Starting TickerAll session...');
    
    const session = await ticker.sessions.start({
      broker: process.env.BROKER,
      server: process.env.SERVER,
      account: parseInt(process.env.ACCOUNT_ID),
      password: process.env.PASSWORD,
    });

    currentSessionId = session.accountId;
    console.log(`✅ Connected to broker. Account ID: ${currentSessionId}`);

    startStreamingData(currentSessionId);

  } catch (error) {
    console.error('❌ Failed to start TickerAll session:', error.message);
    setTimeout(startTickerAllSession, 10000);
  }
}

function startStreamingData(accountId) {
  const stream = ticker.stream.connect();
  stream.subscribeTicks(accountId, ['XAUUSDm', 'EURUSDm', 'US100m']);
  console.log('📡 Subscribed to ticks');

  stream.on('tick', (tick) => {
    wss.clients.forEach((client) => {
      if (client.readyState === WebSocket.OPEN) {
        client.send(JSON.stringify(tick));
      }
    });
  });
}

app.get('/health', (req, res) => {
  res.status(200).send('OK');
});

app.get('/account', async (req, res) => {
  if (!currentSessionId) {
    return res.status(503).json({ error: 'Session not ready yet.' });
  }
  try {
    const accountInfo = await ticker.accounts.accountInfo(currentSessionId);
    res.json(accountInfo);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`🚀 Server listening on port ${PORT}`);
  startTickerAllSession();
});

process.on('SIGTERM', () => {
  console.log('SIGTERM received');
  if (currentSessionId) {
    ticker.sessions.end(currentSessionId).catch(console.error);
  }
  server.close(() => process.exit(0));
});
