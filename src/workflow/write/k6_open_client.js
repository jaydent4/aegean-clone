import http from 'k6/http';

export const options = {
  discardResponseBodies: true,
  scenarios: {
    default: {
      executor: 'constant-arrival-rate',
      rate: Number(__ENV.WRITE_RATE),
      timeUnit: '1s',
      duration: __ENV.WRITE_DURATION,
      gracefulStop: __ENV.WRITE_GRACEFUL_STOP || '0s',
      preAllocatedVUs: Number(__ENV.WRITE_PRE_ALLOCATED_VUS || 1),
      maxVUs: Number(__ENV.WRITE_MAX_VUS || __ENV.WRITE_PRE_ALLOCATED_VUS || 1),
    },
  },
};

export default function () {
  http.post(__ENV.WRITE_TARGET_URL, '');
}
