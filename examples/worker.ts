import * as async from 'async';

import { RateLimitQueue } from '../client-src';

async function sleep(delaySec) {
  return new Promise((resolve) => setTimeout(resolve, delaySec * 1000));
}

const rlq = new RateLimitQueue({
  host: 'localhost',
  port: 6398,
});

(async () => {
  await rlq.createWorker(async (payload, metadata) => {
    console.log('>> PERFORMING JOB -', payload.desc);
    await sleep(2 + Math.random() * 5);
  }, {
    maxConcurrent: 500,
    pullInterval: 500,
  });
})();

