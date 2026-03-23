import http from 'k6/http';
import exec from 'k6/execution';

const spinTimeSeconds = Number(__ENV.SPIN_TIME_SECONDS);
const writeKeyMod = Number(__ENV.WRITE_KEY_MOD);
const readKeyMod = Number(__ENV.READ_KEY_MOD);
const valueLength = Number(__ENV.VALUE_LENGTH);

function makeLargeWriteValue(requestIdx) {
  const prefix = `value-${requestIdx}-`;
  if (prefix.length >= valueLength) {
    return prefix.slice(0, valueLength);
  }
  return prefix + 'x'.repeat(valueLength - prefix.length);
}

export const options = {
  scenarios: {
    default: {
      executor: 'constant-vus',
      vus: Number(__ENV.AEGEAN_VUS || 1),
      duration: __ENV.AEGEAN_DURATION,
    },
  },
};

export default function () {
  const requestIdx = exec.scenario.iterationInTest + 1;
  const body = JSON.stringify({
    timestamp: Date.now() / 1000,
    sender: __ENV.AEGEAN_SENDER,
    op: 'spin_write_read',
    op_payload: {
      spin_time: spinTimeSeconds,
      write_key: String(requestIdx % writeKeyMod),
      write_value: makeLargeWriteValue(requestIdx),
      read_key: String(requestIdx % readKeyMod),
    },
  });

  http.post(__ENV.AEGEAN_TARGET_URL, body, {
    headers: { 'Content-Type': 'application/json' },
  });
}
