import http from 'k6/http';

export const options = {
  discardResponseBodies: true,
  scenarios: {
    default: {
      executor: 'constant-arrival-rate',
      rate: Number(__ENV.SPIN_RATE),
      timeUnit: '1s',
      duration: __ENV.SPIN_DURATION,
      gracefulStop: __ENV.SPIN_GRACEFUL_STOP || '0s',
      preAllocatedVUs: Number(__ENV.SPIN_PRE_ALLOCATED_VUS || 1),
      maxVUs: Number(__ENV.SPIN_MAX_VUS || __ENV.SPIN_PRE_ALLOCATED_VUS || 1),
    },
  },
};

export default function () {
  const body = JSON.stringify({
    timestamp: Date.now() / 1000,
    sender: __ENV.SPIN_SENDER,
    op: 'spin',
    op_payload: {},
  });

  http.post(__ENV.SPIN_TARGET_URL, body, {
    headers: { 'Content-Type': 'application/json' },
  });
}
