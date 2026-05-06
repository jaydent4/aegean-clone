import http from 'k6/http';

export const options = {
  discardResponseBodies: true,
  scenarios: {
    default: {
      executor: 'constant-arrival-rate',
      rate: Number(__ENV.EXTERNAL_SRV_RATE),
      timeUnit: '1s',
      duration: __ENV.EXTERNAL_SRV_DURATION,
      preAllocatedVUs: Number(__ENV.EXTERNAL_SRV_PRE_ALLOCATED_VUS || 1),
      maxVUs: Number(__ENV.EXTERNAL_SRV_MAX_VUS || __ENV.EXTERNAL_SRV_PRE_ALLOCATED_VUS || 1),
    },
  },
};

export default function () {
  const body = JSON.stringify({
    timestamp: Date.now() / 1000,
    sender: __ENV.EXTERNAL_SRV_SENDER,
    op: 'external_call',
    op_payload: {},
  });

  http.post(__ENV.EXTERNAL_SRV_TARGET_URL, body, {
    headers: { 'Content-Type': 'application/json' },
  });
}
