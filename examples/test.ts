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
  await rlq.init({ enableLogs: false }, true);


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


  await enqueueJobs(1000, 5);
  await enqueueJobs(100, 4);
  await enqueueJobs(100, 3);
  await enqueueJobs(100, 2);
  await enqueueJobs(100, 1);
  await enqueueJobs(100, 1);
  await enqueueJobs(100, 1);
  await enqueueJobs(100, 1);
  await enqueueJobs(100, 1);

  // await enqueueJobs(100, 1);
  // await enqueueJobs(10, 2);
  // await enqueueJobs(10, 3);
  // await enqueueJobs(10, 4);



  console.log('enqueued initial jobs');


  await rlq.createWorker(async (payload, metadata) => {
    console.log('>> PERFORMING JOB -', payload.desc);
    await sleep(3 + Math.random() * 3);
  }, { maxConcurrent: 50 });

})();

