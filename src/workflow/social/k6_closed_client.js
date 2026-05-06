import http from 'k6/http';
import exec from 'k6/execution';

const userCount = Number(__ENV.SOCIAL_USER_COUNT || 32);
const postTextLength = Number(__ENV.SOCIAL_POST_TEXT_LENGTH || 64);

function makePostText(requestIdx) {
  const prefix = `compose-post-${requestIdx}-`;
  if (prefix.length >= postTextLength) {
    return prefix.slice(0, postTextLength);
  }
  return prefix + 'x'.repeat(postTextLength - prefix.length);
}

export const options = {
  discardResponseBodies: true,
  scenarios: {
    default: {
      executor: 'constant-vus',
      vus: Number(__ENV.SOCIAL_VUS || 1),
      duration: __ENV.DURATION,
    },
  },
};

export default function () {
  const requestIdx = exec.scenario.iterationInTest + 1;
  const creatorId = `user-${requestIdx % userCount}`;
  const body = JSON.stringify({
    timestamp: Date.now() / 1000,
    sender: __ENV.SENDER,
    op: 'compose_post',
    op_payload: {
      creator_id: creatorId,
      text: makePostText(requestIdx),
    },
  });

  http.post(__ENV.TARGET_URL, body, {
    headers: { 'Content-Type': 'application/json' },
  });
}
