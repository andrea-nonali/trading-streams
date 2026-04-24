import http from 'k6/http';
import { check, sleep } from 'k6';
import { Trend, Rate, Counter } from 'k6/metrics';

const latency   = new Trend('api_latency_ms', true);
const errorRate = new Rate('api_errors');
const requests  = new Counter('api_requests');

export const options = {
  scenarios: {
    baseline: {
      executor: 'constant-vus',
      vus: 50,
      duration: '30s',
      tags: { scenario: 'baseline' },
    },
    spike: {
      executor: 'ramping-vus',
      startTime: '35s',
      stages: [
        { duration: '15s', target: 500 },
        { duration: '15s', target: 500 },
        { duration: '10s', target: 0 },
      ],
      tags: { scenario: 'spike' },
    },
  },
  thresholds: {
    api_latency_ms: ['p(95)<50', 'p(99)<100'],
    api_errors: ['rate<0.01'],
  },
};

export default function () {
  const res = http.get('http://localhost:8083/api/top-stocks');

  check(res, {
    'status 200':        (r) => r.status === 200,
    'has 5 stocks':      (r) => JSON.parse(r.body).length === 5,
    'response under 50ms': (r) => r.timings.duration < 50,
  });

  latency.add(res.timings.duration);
  errorRate.add(res.status !== 200);
  requests.add(1);

  sleep(0.1);
}
