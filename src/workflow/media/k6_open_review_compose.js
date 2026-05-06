import http from 'k6/http';
import exec from 'k6/execution';

const userCount = Number(__ENV.MEDIA_USER_COUNT || 1000);
const movieCount = Number(__ENV.MEDIA_MOVIE_COUNT || 1000);
const textLength = Number(__ENV.MEDIA_REVIEW_TEXT_LENGTH || 256);

function makeReviewText(requestIdx) {
  const prefix = `review-${requestIdx}-`;
  if (prefix.length >= textLength) {
    return prefix.slice(0, textLength);
  }
  return prefix + 'x'.repeat(textLength - prefix.length);
}

export const options = {
  discardResponseBodies: true,
  scenarios: {
    default: {
      executor: 'constant-arrival-rate',
      rate: Number(__ENV.MEDIA_RATE),
      timeUnit: '1s',
      duration: __ENV.MEDIA_DURATION,
      preAllocatedVUs: Number(__ENV.MEDIA_PRE_ALLOCATED_VUS || 1),
      maxVUs: Number(__ENV.MEDIA_MAX_VUS || __ENV.MEDIA_PRE_ALLOCATED_VUS || 1),
    },
  },
};

export default function () {
  const requestIdx = exec.scenario.iterationInTest + 1;
  const userIdx = requestIdx % userCount;
  const movieIdx = requestIdx % movieCount;
  const body = JSON.stringify({
    timestamp: Date.now() / 1000,
    sender: __ENV.MEDIA_SENDER,
    op: 'compose_review',
    op_payload: {
      username: `username_${userIdx}`,
      password: `password_${userIdx}`,
      title: `movie-title-${movieIdx}`,
      rating: requestIdx % 11,
      text: makeReviewText(requestIdx),
    },
  });

  http.post(__ENV.MEDIA_TARGET_URL, body, {
    headers: { 'Content-Type': 'application/json' },
  });
}
