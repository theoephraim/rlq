import * as _ from 'lodash-es';

// also attaches some helpers to keydb object
import { log, safeReturnValue } from './helpers';

import { KEYS, GLOBAL_SETTINGS } from './settings';
import { getJobInfo, getJobWaitingRank } from './job-info';
/**
 * function to complete a job
 * usually it will be called with a single job, but it is written to handle multiple jobs
 * so that we can also use it for cleanup purposes
 */
export function RLQ_JOB_COMPLETED(resultCode: string, workerId: string, ...rawJobIds: string[]) {
  // resultCode can be
  // - SUCCESS
  // - FAIL or FAIL|BURY
  // - TIMEOUT
  // - RERUN or RERUN|N (N = number of seconds delay before rerunning)

  const resultCodeParts = resultCode.split('|');
  const resultStatus = resultCodeParts[0];

  const jobIds = _.map(rawJobIds, (rawJobId) => KEYS.jobId(rawJobId));

  log('job(s) complete', resultStatus, jobIds);

  // remove from global active set
  log('removing job(s) from global active set')
  keydb.call('SREM', KEYS.ACTIVE_JOBS, ...jobIds);

  // remove from worker active set
  // (and do a quick check that the worker ID matches)
  log('removing job(s) from worker active set')
  const firstJobInfo = getJobInfo(jobIds[0]);
  if (workerId !== firstJobInfo.workerId) throw new Error('Worker ID does not match');
  keydb.call('SREM', KEYS.workerActiveSet(workerId), ...jobIds);


  // remove job(s) from each rate limit "active" set
  log('> removing job(s) from rate limit active sets');
  const jobLimitsStrings = keydb.call('HMGET', KEYS.JOB_LIMIT_RULES, ...jobIds);
  const jobIdsByRateLimitId = {};
  _.each(jobIds, (jobId, jobIndex) => {
    const limitString = jobLimitsStrings[jobIndex];
    const limitIds = limitString.split('|');
    _.each(limitIds, (limitId) => {
      if (!jobIdsByRateLimitId[limitId]) jobIdsByRateLimitId[limitId] = [];
      jobIdsByRateLimitId[limitId].push(jobId);
    });
  });
  _.each(jobIdsByRateLimitId, (jobIdsForRateLimit, limitId) => {
    log('> SREM', KEYS.rateLimitActiveSet(limitId), jobIdsForRateLimit);
    keydb.call('SREM', KEYS.rateLimitActiveSet(limitId), ...jobIdsForRateLimit);
  });

  if (resultStatus === 'SUCCESS') {
    _.each(jobIds, (jobId) => {
      keydb.setHash(jobId, {
        status: 'SUCCESS',
        completedAt: +new Date(),
      });
    });
    // move jobs to success
    keydb.call('SADD', KEYS.SUCCESS_JOBS, ...jobIds);
    // delete set of limit IDs we stored for the job
    keydb.call('HDEL', KEYS.JOB_LIMIT_RULES, ...jobIds);

  } else if (resultStatus === 'TIMEOUT') {
    // move the jobs back to waiting
    const moveToWaitingPayload = [];
    _.each(jobIds, (jobId) => {
      const jobInfo = getJobInfo(jobId);
      keydb.setHash(jobId, { status: 'WAITING' });
      // we rank the jobs by priority and then enqueued time
      const rank = getJobWaitingRank(jobInfo);
      moveToWaitingPayload.push(rank, jobId)
    });

    log('moving timed out jobs to waiting')
    // add all jobIds to waiting set
    keydb.call('ZADD', KEYS.WAITING_JOBS, ...moveToWaitingPayload);

  } else if (resultStatus === 'RERUN') {
    const afterDelay = resultCodeParts[1] && parseInt(resultCodeParts[1]);
    const runAt = +new Date() + ((afterDelay || 0) * 1000);
    const moveToDelayedPayload = [];
    _.each(jobIds, (jobId) => {
      const rerunCount = keydb.call('HINCRBY', jobId, 'rerunCount', 1);
      // if we reach max reruns, we will bury the job
      if (rerunCount > GLOBAL_SETTINGS.maxRerunRetries) {
        keydb.setHash(jobId, {
          status: 'BURY',
          lastFailedAt: +new Date(),
        });
        keydb.call('SADD', KEYS.BURIED_JOBS, jobId);
      } else {

      }
      keydb.setHash(jobId, {
        status: 'DELAYED',

        // TODO: not sure what other metadata we want to store
      });
      moveToDelayedPayload.push(runAt, jobId);
    });
    keydb.call('ZADD', KEYS.DELAYED_JOBS, ...moveToDelayedPayload);

  } else if (resultStatus === 'FAIL') {
    const forceBury = resultCodeParts[1] === 'BURY';
    const moveToDelayedPayload = [];
    _.each(jobIds, (jobId) => {
      const failCount = keydb.call('HINCRBY', jobId, 'failCount', 1);
      const buryJob = forceBury || failCount > GLOBAL_SETTINGS.maxFailureRetries;

      keydb.setHash(jobId, {
        status: buryJob ? 'BURY' : 'FAIL',
        lastFailedAt: +new Date(),
      });

      if (buryJob) {
        keydb.call('SADD', KEYS.BURIED_JOBS, jobId);
      } else {
        // TODO: exponential backoff instead of linear
        const runAt = +new Date() + (failCount * 15 * 1000);
        moveToDelayedPayload.push(runAt, jobId);
      }
    });
    keydb.call('ZADD', KEYS.DELAYED_JOBS, ...moveToDelayedPayload);
  }
}

/**
 * internal helper to call RLQ_JOB_COMPLETED internally rather than from a client
 * externally, the client only cares about a "raw" job id (ex: `123`) whereas in our keys we
 * add a prefix (ex: `JOB--123`)
 *
 * So when calling from our client, we would pass the raw job id
 * whereas if using to cleanup timed out jobs, we would have the prefixed job id available
 */
export function runJobsCompletedWithFullJobIds(newStatus: string, workerId: string , ...jobIds: string[]) {
  const rawJobIds = _.map(jobIds, (jobId) => KEYS.rawJobId(jobId));
  return RLQ_JOB_COMPLETED(newStatus, workerId, ...rawJobIds);
}
