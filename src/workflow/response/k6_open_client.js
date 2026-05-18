import http from 'k6/http';

export const options = {
  discardResponseBodies: true,
  scenarios: {
    default: {
      executor: 'constant-arrival-rate',
      rate: Number(__ENV.RESPONSE_RATE),
      timeUnit: '1s',
      duration: __ENV.RESPONSE_DURATION,
      gracefulStop: __ENV.RESPONSE_GRACEFUL_STOP || '30s',
      preAllocatedVUs: Number(__ENV.RESPONSE_PRE_ALLOCATED_VUS || 1),
      maxVUs: Number(__ENV.RESPONSE_MAX_VUS || __ENV.RESPONSE_PRE_ALLOCATED_VUS || 1),
    },
  },
};

export default function () {
  const body = JSON.stringify({
    timestamp: Date.now() / 1000,
    sender: __ENV.RESPONSE_SENDER,
    op: 'response',
    op_payload: {},
  });

  http.post(__ENV.RESPONSE_TARGET_URL, body, {
    headers: { 'Content-Type': 'application/json' },
  });
}
