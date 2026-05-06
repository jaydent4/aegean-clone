import http from 'k6/http';

export const options = {
  discardResponseBodies: true,
  scenarios: {
    default: {
      executor: 'constant-arrival-rate',
      rate: Number(__ENV.REQ_RACE_RATE),
      timeUnit: '1s',
      duration: __ENV.REQ_RACE_DURATION,
      preAllocatedVUs: Number(__ENV.REQ_RACE_PRE_ALLOCATED_VUS || 1),
      maxVUs: Number(__ENV.REQ_RACE_MAX_VUS || __ENV.REQ_RACE_PRE_ALLOCATED_VUS || 1),
    },
  },
};

export default function () {
  const body = JSON.stringify({
    timestamp: Date.now() / 1000,
    sender: __ENV.REQ_RACE_SENDER,
    op: 'default',
    op_payload: {},
  });

  http.post(__ENV.REQ_RACE_TARGET_URL, body, {
    headers: { 'Content-Type': 'application/json' },
  });
}
