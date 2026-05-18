import http from 'k6/http';

const locale = __ENV.HOTEL_LOCALE || 'en';
const latBase = Number(__ENV.HOTEL_LAT_BASE || 37.7867);
const lonBase = Number(__ENV.HOTEL_LON_BASE || -122.4071);

export const options = {
  discardResponseBodies: true,
  scenarios: {
    default: {
      executor: 'constant-vus',
      vus: Number(__ENV.HOTEL_VUS || 1),
      duration: __ENV.DURATION,
      gracefulStop: __ENV.GRACEFUL_STOP || '30s',
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
  const inDay = 9 + Math.floor(Math.random() * 15);
  const outDay = Math.min(inDay + 1 + Math.floor(Math.random() * 5), 24);

  const body = JSON.stringify({
    timestamp: Date.now() / 1000,
    sender: __ENV.SENDER,
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

  http.post(__ENV.TARGET_URL, body, {
    headers: { 'Content-Type': 'application/json' },
  });
}
