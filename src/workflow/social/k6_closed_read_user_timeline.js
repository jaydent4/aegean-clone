import http from 'k6/http';
import exec from 'k6/execution';

const userCount = Number(__ENV.SOCIAL_USER_COUNT || 32);

export const options = {
  discardResponseBodies: true,
  scenarios: {
    default: {
      executor: 'constant-vus',
      vus: Number(__ENV.SOCIAL_VUS || 1),
      duration: __ENV.DURATION,
      gracefulStop: __ENV.GRACEFUL_STOP || '0s',
    },
  },
};

export default function () {
  const requestIdx = exec.scenario.iterationInTest + 1;
  const userId = `user-${requestIdx % userCount}`;
  const body = JSON.stringify({
    timestamp: Date.now() / 1000,
    sender: __ENV.SENDER,
    op: 'read_user_timeline',
    op_payload: {
      user_id: userId,
    },
  });

  http.post(__ENV.TARGET_URL, body, {
    headers: { 'Content-Type': 'application/json' },
  });
}
