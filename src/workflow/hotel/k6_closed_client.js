import http from 'k6/http';
import exec from 'k6/execution';

const userCount = Number(__ENV.HOTEL_USER_COUNT || 501);
const hotelCount = Number(__ENV.HOTEL_HOTEL_COUNT || 80);

export const options = {
  discardResponseBodies: true,
  scenarios: {
    default: {
      executor: 'constant-vus',
      vus: Number(__ENV.HOTEL_VUS || 1),
      duration: __ENV.DURATION,
    },
  },
};

function randomDate(minDay, maxDayInclusive) {
  const day = minDay + Math.floor(Math.random() * (maxDayInclusive - minDay + 1));
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

  const reserveInDay = 9 + Math.floor(Math.random() * 15);
  const reserveOutDay = reserveInDay + 1 + Math.floor(Math.random() * 5);
  const reserveHotelID = String(1 + (requestIdx % hotelCount));

  const body = JSON.stringify({
    timestamp: Date.now() / 1000,
    sender: __ENV.SENDER,
    op: 'make_reservation',
    op_payload: {
      in_date: randomDate(reserveInDay, reserveInDay),
      out_date: randomDate(Math.min(reserveOutDay, 28), Math.min(reserveOutDay, 28)),
      hotel_id: reserveHotelID,
      customer_name: user.username,
      username: user.username,
      password: user.password,
      room_number: 1,
    },
  });

  http.post(__ENV.TARGET_URL, body, {
    headers: { 'Content-Type': 'application/json' },
  });
}
