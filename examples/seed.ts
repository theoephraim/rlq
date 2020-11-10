import * as _ from 'lodash';
import * as async from 'async';

import { RateLimitQueue } from '../client-src';

async function sleep(delaySec) {
  return new Promise((resolve) => setTimeout(resolve, delaySec * 1000));
}


const rlq = new RateLimitQueue({
  host: 'localhost',
  port: 6398,
});

function getUser() {
  const userId = Math.ceil(100*Math.random()); // 100 users
  return {
    userId,
    workspaceId: userId % 10, // 10 workspaces
    accountId: Math.ceil(Math.random() * 5), // 5 accounts
  }
}
let jobCounter = 1;
function getJobId() {
  return jobCounter++;
}

async function enqueueJobs(count, priority) {
  const { userId, workspaceId, accountId } = getUser();

  const jobPayloadsById = {};
  _.times(count, () => {
    const i = getJobId();
    jobPayloadsById[`j${i}`] = {
      desc: `Job #${i} - W${workspaceId}/U${userId}/A${accountId} - P${priority}`,
    }
  });

  await rlq.enqueueJobs(jobPayloadsById, {
    priority,
    limits: [
      ['USER', userId],
      ['WORKSPACE', workspaceId],
      ['ACCOUNT', accountId],
    ],
  });
}

(async () => {
  // flushes the queue, starts the cron tick
  await rlq.init(null, true);

  await rlq.defineLimitType('USER', {
    type: 'concurrent',
    limit: 100
  });

  await rlq.defineLimitType('WORKSPACE', {
    type: 'concurrent',
    limit: 200,
  });

  await rlq.defineLimitType('ACCOUNT', {
    type: 'concurrent',
    limit: 100,
  });

  setInterval(async () => {
    await enqueueJobs(3, 1);
  }, 500);

  setInterval(async () => {
    await enqueueJobs(50, 2);
  }, 2000);

  setInterval(async () => {
    await enqueueJobs(100, 3);
  }, 5000);

  setInterval(async () => {
    await enqueueJobs(1000, 4);
  }, 15000);

  setInterval(async () => {
    await enqueueJobs(10000, 5);
  }, 30000);
})();

