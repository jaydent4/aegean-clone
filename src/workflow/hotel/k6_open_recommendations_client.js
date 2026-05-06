import http from 'k6/http';
import exec from 'k6/execution';

const locale = __ENV.HOTEL_LOCALE || 'en';
const latBase = Number(__ENV.HOTEL_LAT_BASE || 37.7867);
const lonBase = Number(__ENV.HOTEL_LON_BASE || -122.4071);
const requirements = ['dis', 'rate', 'price'];

export const options = {
  discardResponseBodies: true,
  scenarios: {
    default: {
      executor: 'constant-arrival-rate',
      rate: Number(__ENV.HOTEL_RATE),
      timeUnit: '1s',
      duration: __ENV.HOTEL_DURATION,
      preAllocatedVUs: Number(__ENV.HOTEL_PRE_ALLOCATED_VUS || 1),
      maxVUs: Number(__ENV.HOTEL_MAX_VUS || __ENV.HOTEL_PRE_ALLOCATED_VUS || 1),
    },
  },
};

function randomOffset(radius) {
  return (Math.random() * 2 - 1) * radius;
}

export default function () {
  const requestIdx = exec.scenario.iterationInTest + 1;
  const requirement = requirements[requestIdx % requirements.length];

  const body = JSON.stringify({
    timestamp: Date.now() / 1000,
    sender: __ENV.HOTEL_SENDER,
    op: 'recommend_hotels',
    op_payload: {
      require: requirement,
      lat: latBase + randomOffset(0.02),
      lon: lonBase + randomOffset(0.02),
      locale,
    },
  });

  http.post(__ENV.HOTEL_TARGET_URL, body, {
    headers: { 'Content-Type': 'application/json' },
  });
}
