import http from 'k6/http';
import exec from 'k6/execution';

const userCount = Number(__ENV.HOTEL_USER_COUNT || 501);
const hotelCount = Number(__ENV.HOTEL_HOTEL_COUNT || 80);

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

function randomDate(day) {
  return `2015-04-${String(day).padStart(2, '0')}`;
}

function makeUser(userIdx) {
  const suffix = String(userIdx);
  return {
    username: `Cornell_${suffix}`,
    password: suffix.repeat(10),
  };
}

export default function () {
  const requestIdx = exec.scenario.iterationInTest + 1;
  const userIdx = requestIdx % userCount;
  const user = makeUser(userIdx);

  const reserveInDay = 9 + (requestIdx % 15);
  const reserveOutDay = Math.min(reserveInDay + 1 + (requestIdx % 5), 28);
  const reserveHotelID = String(1 + (requestIdx % hotelCount));

  const body = JSON.stringify({
    timestamp: Date.now() / 1000,
    sender: __ENV.HOTEL_SENDER,
    op: 'make_reservation',
    op_payload: {
      in_date: randomDate(reserveInDay),
      out_date: randomDate(reserveOutDay),
      hotel_id: reserveHotelID,
      customer_name: user.username,
      username: user.username,
      password: user.password,
      room_number: 1,
    },
  });

  http.post(__ENV.HOTEL_TARGET_URL, body, {
    headers: { 'Content-Type': 'application/json' },
  });
}
