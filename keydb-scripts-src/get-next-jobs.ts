import * as _ from 'lodash-es';

// also attaches some helpers to keydb object
import { log, safeReturnValue } from './helpers';
import { KEYS, GLOBAL_SETTINGS } from './settings';
import { getJobInfo, getJobWaitingRank } from './job-info';
import { getLimitTypeInfo, getLimitTypeIdFromLimitId } from './limits';
import { RLQ_PROMOTE_LIMITED } from './internals';
/**
 * Tries to fetch a group of jobs to process as a batch
 *
 * Because fetching jobs is somewhat expensive, it's better to process a chunk of jobs at a time
 * so we can perform most operations in bulk (moving ids from one set to another, etc)
 *
 * We go through the list of waiting jobs and check if their rate limits have available capacity
 * and if any rate limits are at capacity, we mark the job as rate limited and put it back in the
 * queue to check again later. We also have a limit on the number of jobs we can check at a time
 * so a single check may end up just marking many jobs as rate limited and not returning any.
 */
export function RLQ_GET_NEXT_JOBS(numJobs: number, workerId: string) {
  const nowTs = +new Date();

  numJobs = parseInt(numJobs as any);

  // update worker heartbeat
  keydb.call('HSET', KEYS.WORKERS_LAST_SEEN, workerId, nowTs);
  if (numJobs === 0) return [0];

  // first check global concurrent limit
  // we special case this so we don't store a duplicate of all active IDs
  // although we maybe want to check it below instead so we can mark the jobs as rate limited?
  const totalActiveJobs = keydb.call('SCARD', KEYS.ACTIVE_JOBS);
  if (totalActiveJobs >= GLOBAL_SETTINGS.concurrentLimit) {
    log('global rate limit at capacity');
    return [0];
  }


  log('\n\n\nPULLING NEXT JOBS');

  const remainingGlobalLimitSlots = GLOBAL_SETTINGS.concurrentLimit - totalActiveJobs;
  log(`> Global limit remaining: ${remainingGlobalLimitSlots} / ${GLOBAL_SETTINGS.concurrentLimit}`);

  const numWaiting = keydb.call('ZCARD', KEYS.WAITING_JOBS);
  if (numWaiting < GLOBAL_SETTINGS.maxSweepSize) {
    // make sure we have a full "sweep" of jobs in the waiting queue
    RLQ_PROMOTE_LIMITED(GLOBAL_SETTINGS.maxSweepSize - numWaiting);
  }

  const maxJobsToFetch = Math.min(numJobs, remainingGlobalLimitSlots);
  const jobIdsToActivate = [];
  const jobIdsToLimit = [];
  const limitTypeInfoCache = {};
  const limitsCache = {};
  const limitsByJobId = {};

  // we'll pull a full "sweep" of job ids at once to check
  const waitingJobIdsToCheck = keydb.call('ZRANGEBYSCORE', KEYS.WAITING_JOBS, '-inf', 'inf', 'LIMIT', 0, GLOBAL_SETTINGS.maxSweepSize);
  const checkedJobIds = [];
  if (!waitingJobIdsToCheck?.length) {
    log('> no more jobs to check');
    return [0];
  }

  // fetch all of the rate limit ids for these jobs (these are stored as strings, but we will split back to an array)
  const jobLimitsStrings = keydb.call('HMGET', KEYS.JOB_LIMIT_RULES, ...waitingJobIdsToCheck);
  // build up cache of info about each limit which we can reuse for next jobs we check
  _.each(waitingJobIdsToCheck, (jobId, jobToCheckIndex) => {
    checkedJobIds.push(jobId);
    const limitIdsString = jobLimitsStrings[jobToCheckIndex];
    const limitIds = limitIdsString?.split('|');
    limitsByJobId[jobId] = limitIds;
    _.each(limitIds, (limitId) => {
      if (!limitsCache[limitId]) {
        const limitTypeId = getLimitTypeIdFromLimitId(limitId);
        if (!limitTypeInfoCache[limitTypeId]) {
          limitTypeInfoCache[limitTypeId] = getLimitTypeInfo(limitTypeId);
        }
        limitsCache[limitId] = {
          // total limit according to the limit type info
          limit: limitTypeInfoCache[limitTypeId].limit,
          // the current number of members in the limit's active set
          current: keydb.call('SCARD', KEYS.rateLimitActiveSet(limitId)),
        }
      }
    });

    const allLimitsHaveCapacity = _.every(limitIds, (limitId) => (
      limitsCache[limitId].current < limitsCache[limitId].limit
    ));

    if (allLimitsHaveCapacity) {
      jobIdsToActivate.push(jobId);
      // increase the current counter in the cache for each limit
      _.each(limitIds, (limitId) => { limitsCache[limitId].current++ });
    } else {
      jobIdsToLimit.push(jobId);
    }

    if (jobIdsToActivate.length === maxJobsToFetch) return false; // breaks out of _.each
  });

  // remove jobs we actually checked from waiting list - we will be either activating or limiting them
  log('> removing jobs from waiting', checkedJobIds);
  keydb.call('ZREM', KEYS.WAITING_JOBS, ...checkedJobIds);

  log('> Activate - ', jobIdsToActivate);
  log('> Limit - ', jobIdsToLimit);
  log('> limits by job id', limitsByJobId);

  // move newly active jobs from waiting to active
  const jobInfoToReturn = [];
  if (jobIdsToActivate.length) {
    // keydb.call('ZREM', KEYS.WAITING_JOBS, ...jobIdsToActivate); // remove from waiting
    log('> adding jobs to global active set', jobIdsToActivate)
    keydb.call('SADD', KEYS.ACTIVE_JOBS, ...jobIdsToActivate); // add to active
    const newJobIdsByLimitId = {};
    _.each(jobIdsToActivate, (jobId) => {
      log('> updating job info to active', jobId);
      keydb.setHash(jobId, { // update job info
        status: 'ACTIVE',
        activatedAt: nowTs,
        workerId,
      });

      const jobInfo = keydb.getHash(jobId);
      jobInfoToReturn.push(
        KEYS.rawJobId(jobId),
        jobInfo.payload, // already stringified
        JSON.stringify(_.omit(jobInfo, 'payload')),
      );

      // build up list of job ids for each rate limit so we can change in batches
      _.each(limitsByJobId[jobId], (limitId) => {
        if (!newJobIdsByLimitId[limitId]) newJobIdsByLimitId[limitId] = [];
        newJobIdsByLimitId[limitId].push(jobId);
      });
    });


    // add the job ids to the worker's active set
    keydb.call('SADD', KEYS.workerActiveSet(workerId), ...jobIdsToActivate);

    log('> new job ids for each limit id', newJobIdsByLimitId);
    // move the job ids from waiting to active in each limit in batches
    _.each(newJobIdsByLimitId, (jobIds, limitId) => {
      log('> moving jobs in rate limit sets', KEYS.rateLimitWaitingSet(limitId), KEYS.rateLimitActiveSet(limitId), jobIds);
      keydb.call('SREM', KEYS.rateLimitWaitingSet(limitId), ...jobIds);
      keydb.call('SADD', KEYS.rateLimitActiveSet(limitId), ...jobIds);
      // TODO: if time-window rule, need to add expiry
      // might need to also add backup expiry unless we handle cleanup another way?
    });
  }

  // move limited jobs from waiting to limited
  if (jobIdsToLimit.length) {
    log('> moving limited jobs');
    _.each(jobIdsToLimit, (jobId) => {
      // TODO: not sure how we want to handle this - how long to wait before retrying
      // maybe want to make backoff exponential?
      // maybe want to take the limit settings into account?
      keydb.setHash(jobId, {
        status: 'LIMITED',
      });
      // increment rate limited count
      const limitedCount = keydb.call('HINCRBY', jobId, 'limitedCount', 1);
    });
    keydb.call('RPUSH', KEYS.LIMITED_JOBS, ...jobIdsToLimit);
  }
  log('> FINISH PULL <<<<<<<<<<<<<<<<<<<<<<<<<<<');

  // returns an array of [ # jobs, id 1, payload 1, metadata 1, id 2, payload 2, ...]
  return [
    jobIdsToActivate.length,
    ...jobInfoToReturn
  ];
}
