import http from 'k6/http';
import exec from 'k6/execution';

const userCount = Number(__ENV.SOCIAL_USER_COUNT || 32);

export const options = {
  discardResponseBodies: true,
  scenarios: {
    default: {
      executor: 'constant-arrival-rate',
      rate: Number(__ENV.SOCIAL_RATE),
      timeUnit: '1s',
      duration: __ENV.SOCIAL_DURATION,
      gracefulStop: __ENV.SOCIAL_GRACEFUL_STOP || '0s',
      preAllocatedVUs: Number(__ENV.SOCIAL_PRE_ALLOCATED_VUS || 1),
      maxVUs: Number(__ENV.SOCIAL_MAX_VUS || __ENV.SOCIAL_PRE_ALLOCATED_VUS || 1),
    },
  },
};

export default function () {
  const requestIdx = exec.scenario.iterationInTest + 1;
  const userId = `user-${requestIdx % userCount}`;
  const body = JSON.stringify({
    timestamp: Date.now() / 1000,
    sender: __ENV.SOCIAL_SENDER,
    op: 'read_user_timeline',
    op_payload: {
      user_id: userId,
    },
  });

  http.post(__ENV.SOCIAL_TARGET_URL, body, {
    headers: { 'Content-Type': 'application/json' },
  });
}
