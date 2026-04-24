import ws from 'k6/ws';
import { check, sleep } from 'k6';
import { Trend, Counter, Rate } from 'k6/metrics';

const messageLatency  = new Trend('ws_message_latency_ms', true);
const messagesTotal   = new Counter('ws_messages_received');
const connectionErrors = new Rate('ws_connection_errors');

export const options = {
  scenarios: {
    ramp_up: {
      executor: 'ramping-vus',
      stages: [
        { duration: '20s', target: 100  },
        { duration: '30s', target: 500  },
        { duration: '30s', target: 1000 },
        { duration: '20s', target: 0    },
      ],
    },
  },
  thresholds: {
    ws_message_latency_ms: ['p(95)<500'],  // matches our NFR
    ws_connection_errors:  ['rate<0.01'],
  },
};

const SYMBOLS = ['AAPL', 'TSLA', 'GOOGL', 'MSFT', 'NVDA', 'META', 'AMZN', 'NFLX'];

export default function () {
  const symbol = SYMBOLS[Math.floor(Math.random() * SYMBOLS.length)];
  const url = `ws://localhost:8081/ws/prices/${symbol}`;

  const res = ws.connect(url, {}, function (socket) {
    socket.on('open', () => {
      // connected, just listen
    });

    socket.on('message', (data) => {
      const tick = JSON.parse(data);
      if (tick.timestamp) {
        messageLatency.add(Date.now() - tick.timestamp);
      }
      messagesTotal.add(1);
    });

    socket.on('error', (e) => {
      connectionErrors.add(1);
    });

    // hold connection open for 30 seconds then close
    socket.setTimeout(() => socket.close(), 30000);
  });

  check(res, {
    'connected successfully': (r) => r && r.status === 101,
  });

  connectionErrors.add(res === null || res.status !== 101 ? 1 : 0);
}
