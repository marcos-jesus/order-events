// Teste de carga do POST /orders.
// Uso: k6 run -e TOTAL=10000000 -e VUS=200 loadtest/orders.js
import http from 'k6/http';
import { check } from 'k6';

const TOTAL = Number(__ENV.TOTAL || 10000);
const VUS = Number(__ENV.VUS || 100);
const URL = __ENV.URL || 'http://localhost:8080/orders';

export const options = {
  discardResponseBodies: true,
  scenarios: {
    orders: {
      executor: 'shared-iterations',
      iterations: TOTAL,
      vus: VUS,
      maxDuration: '3h',
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(95)<500'],
  },
};

const HEADERS = { headers: { 'Content-Type': 'application/json' } };

export default function () {
  const body = JSON.stringify({
    product: `produto-${__ITER % 1000}`,
    quantity: 1 + (__ITER % 5),
    price: 10 + (__ITER % 990) / 10,
  });
  const res = http.post(URL, body, HEADERS);
  check(res, { 'status 202': (r) => r.status === 202 });
}
