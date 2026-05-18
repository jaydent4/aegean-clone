import http from 'k6/http';
import exec from 'k6/execution';

const locale = __ENV.HOTEL_LOCALE || 'en';
const latBase = Number(__ENV.HOTEL_LAT_BASE || 37.7867);
const lonBase = Number(__ENV.HOTEL_LON_BASE || -122.4071);

export const options = {
  discardResponseBodies: true,
  scenarios: {
    default: {
      executor: 'constant-arrival-rate',
      rate: Number(__ENV.HOTEL_RATE),
      timeUnit: '1s',
      duration: __ENV.HOTEL_DURATION,
      gracefulStop: __ENV.HOTEL_GRACEFUL_STOP || '0s',
      preAllocatedVUs: Number(__ENV.HOTEL_PRE_ALLOCATED_VUS || 1),
      maxVUs: Number(__ENV.HOTEL_MAX_VUS || __ENV.HOTEL_PRE_ALLOCATED_VUS || 1),
    },
  },
};

function randomDate(minDay, maxDayInclusive) {
  const day = minDay + Math.floor(Math.random() * (maxDayInclusive - minDay + 1));
  return `2015-04-${String(day).padStart(2, '0')}`;
}

function randomOffset(radius) {
  return (Math.random() * 2 - 1) * radius;
}

export default function () {
  const requestIdx = exec.scenario.iterationInTest + 1;
  const inDay = 9 + (requestIdx % 15);
  const outDay = Math.min(inDay + 1 + (requestIdx % 5), 24);

  const body = JSON.stringify({
    timestamp: Date.now() / 1000,
    sender: __ENV.HOTEL_SENDER,
    op: 'search_hotels',
    op_payload: {
      in_date: randomDate(inDay, inDay),
      out_date: randomDate(outDay, outDay),
      lat: latBase + randomOffset(0.02),
      lon: lonBase + randomOffset(0.02),
      locale,
      room_number: 1,
    },
  });

  http.post(__ENV.HOTEL_TARGET_URL, body, {
    headers: { 'Content-Type': 'application/json' },
  });
}
