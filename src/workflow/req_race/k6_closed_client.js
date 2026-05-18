import http from 'k6/http';

export const options = {
  discardResponseBodies: true,
  scenarios: {
    default: {
      executor: 'constant-vus',
      vus: Number(__ENV.REQ_RACE_VUS || 1),
      duration: __ENV.DURATION,
      gracefulStop: __ENV.GRACEFUL_STOP || '0s',
    },
  },
};

export default function () {
  const body = JSON.stringify({
    timestamp: Date.now() / 1000,
    sender: __ENV.SENDER,
    op: 'default',
    op_payload: {},
  });

  http.post(__ENV.TARGET_URL, body, {
    headers: { 'Content-Type': 'application/json' },
  });
}
