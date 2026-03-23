import http from 'k6/http';

export const options = {
  scenarios: {
    default: {
      executor: 'constant-vus',
      vus: Number(__ENV.EXTERNAL_SRV_VUS || 1),
      duration: __ENV.DURATION,
    },
  },
};

export default function () {
  const body = JSON.stringify({
    timestamp: Date.now() / 1000,
    sender: __ENV.SENDER,
    op: 'external_call',
    op_payload: {},
  });

  http.post(__ENV.TARGET_URL, body, {
    headers: { 'Content-Type': 'application/json' },
  });
}
