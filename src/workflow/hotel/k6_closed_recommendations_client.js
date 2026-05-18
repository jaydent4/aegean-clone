import http from 'k6/http';

const locale = __ENV.HOTEL_LOCALE || 'en';
const latBase = Number(__ENV.HOTEL_LAT_BASE || 37.7867);
const lonBase = Number(__ENV.HOTEL_LON_BASE || -122.4071);
const requirements = ['dis', 'rate', 'price'];

export const options = {
  discardResponseBodies: true,
  scenarios: {
    default: {
      executor: 'constant-vus',
      vus: Number(__ENV.HOTEL_VUS || 1),
      duration: __ENV.DURATION,
      gracefulStop: __ENV.GRACEFUL_STOP || '0s',
    },
  },
};

function randomOffset(radius) {
  return (Math.random() * 2 - 1) * radius;
}

export default function () {
  const requirement = requirements[Math.floor(Math.random() * requirements.length)];

  const body = JSON.stringify({
    timestamp: Date.now() / 1000,
    sender: __ENV.SENDER,
    op: 'recommend_hotels',
    op_payload: {
      require: requirement,
      lat: latBase + randomOffset(0.02),
      lon: lonBase + randomOffset(0.02),
      locale,
    },
  });

  http.post(__ENV.TARGET_URL, body, {
    headers: { 'Content-Type': 'application/json' },
  });
}
