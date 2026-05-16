import http from 'k6/http';

export const options = {
  discardResponseBodies: true,
  scenarios: {
    default: {
      executor: 'constant-arrival-rate',
      rate: Number(__ENV.WORKER_RATE),
      timeUnit: '1s',
      duration: __ENV.WORKER_DURATION,
      preAllocatedVUs: Number(__ENV.WORKER_PRE_ALLOCATED_VUS || 1),
      maxVUs: Number(__ENV.WORKER_MAX_VUS || __ENV.WORKER_PRE_ALLOCATED_VUS || 1),
    },
  },
};

export default function () {
  const body = JSON.stringify({
    timestamp: Date.now() / 1000,
    sender: __ENV.WORKER_SENDER,
    op: 'worker',
    op_payload: {},
  });

  http.post(__ENV.WORKER_TARGET_URL, body, {
    headers: { 'Content-Type': 'application/json' },
  });
}
