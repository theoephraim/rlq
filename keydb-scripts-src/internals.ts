import * as _ from 'lodash-es';

// also attaches some helpers to keydb object
import { log, safeReturnValue } from './helpers';
import { getJobInfo, getJobWaitingRank } from './job-info';

import {
  KEYS, GLOBAL_SETTINGS, loadGlobalSettings
} from './settings';

import { runJobsCompletedWithFullJobIds} from './complete-jobs';


/** Handles setup - loading in cron scripts */
export function RLQ_INIT() {
  loadGlobalSettings();
  log('> Starting up RLQ!');
  RLQ_START_TICK();
}

export function RLQ_START_TICK() {
  log('> starting RLQ tick')
  // clear the count
  keydb.call('DEL', KEYS.TICK_COUNT);

  // KEYDB.CRON call will trigger a script on a timer
  // note that it does not understand js, so the script here is Lua and seems to only work
  // if we trigger using "redis.call"
  const cron1 = keydb.call('KEYDB.CRON', KEYS.TICK_CRON, 'repeat', 1000, "redis.pcall('RLQ_TICK')");
  const cron2 = keydb.call('KEYDB.CRON', KEYS.CLEANUP_CRON, 'repeat', 10000, "redis.pcall('RLQ_CLEANUP')");
  return [cron1, cron2];

}

export function RLQ_STOP_TICK() {
  const cron1 = keydb.call('DEL', KEYS.TICK_CRON);
  const cron2 = keydb.call('DEL', KEYS.CLEANUP_CRON);
  return [cron1, cron2];
}

/** Triggered once every second to trigger scripts */
export function RLQ_TICK() {
  RLQ_PROMOTE_DELAYED();
  return keydb.call('INCR', KEYS.TICK_COUNT);
}

export function RLQ_CHECK_TICK() {
  return keydb.call('GET', KEYS.TICK_COUNT);
}


export function RLQ_PROMOTE_DELAYED() {
  const nowTs = +new Date();

  // we try to move all delayed jobs that have had their scheduled time pass (up to a limit)
  const delayedJobIdsToMove = keydb.call('ZRANGEBYSCORE', KEYS.DELAYED_JOBS, '-inf', nowTs, 'LIMIT', 0, GLOBAL_SETTINGS.promoteDelayedLimit);
  if (!delayedJobIdsToMove?.length) return 0;
  log(`promoting ${delayedJobIdsToMove.length} delayed jobs`);

  // remove the job ids from the delayed set
  keydb.call('ZREM', KEYS.DELAYED_JOBS, ...delayedJobIdsToMove);

  // delayed jobs (either enqueued with a delay or re-running after a failure/rerun result) are no longer listed
  // in each rate limit's waiting set, so as we move them back to waiting, we add them
  const delayedJobIdLimitsStrings = keydb.call('HMGET', KEYS.JOB_LIMIT_RULES, ...delayedJobIdsToMove);
  const newJobIdsByLimitId = {};
  _.each(delayedJobIdsToMove, (jobId, jobIndex) => {
    const limitString = delayedJobIdLimitsStrings[jobIndex];
    const limitIds = limitString.split('|');
    _.each(limitIds, (limitId) => {
      if (!newJobIdsByLimitId[limitId]) newJobIdsByLimitId[limitId] = [];
      newJobIdsByLimitId[limitId].push(jobId);
    });
  });
  _.each(newJobIdsByLimitId, (jobIds, limitId) => {
    keydb.call('SADD', KEYS.rateLimitWaitingSet(limitId), ...jobIds);
  });

  const moveToWaitingPayload = [];
  _.each(delayedJobIdsToMove, (jobId) => {
    const jobInfo = getJobInfo(jobId);
    keydb.setHash(jobId, { status: 'WAITING' });

    // we rank the jobs by priority and then enqueued time
    const rank = getJobWaitingRank(jobInfo);
    moveToWaitingPayload.push(rank, jobId)
  });

  // add all jobIds to waiting set
  keydb.call('ZADD', KEYS.WAITING_JOBS, ...moveToWaitingPayload);
  return delayedJobIdsToMove.length;
}


export function RLQ_PROMOTE_LIMITED(numJobs: number) {
  numJobs = parseInt(numJobs as any);
  // const limitedJobIdsToMove = keydb.call('ZRANGEBYSCORE', KEYS.LIMITED_JOBS, '-inf', 'inf', 'LIMIT', 0, numJobs);
  const limitedJobIdsToMove = keydb.call('LRANGE', KEYS.LIMITED_JOBS, 0, numJobs - 1);
  if (!limitedJobIdsToMove.length) return 0;

  // remove jobs from limited set
  // keydb.call('ZREM', KEYS.LIMITED_JOBS, ...limitedJobIdsToMove);
  keydb.call('LTRIM', KEYS.LIMITED_JOBS, limitedJobIdsToMove.length, -1);

  log(`promoting ${limitedJobIdsToMove.length} limited jobs`);

  const moveToWaitingPayload = [];
  _.each(limitedJobIdsToMove, (jobId) => {
    const jobInfo = getJobInfo(jobId);
    keydb.setHash(jobId, { status: 'WAITING' });

    // we rank the jobs by priority and then enqueued time
    const rank = getJobWaitingRank(jobInfo);
    moveToWaitingPayload.push(rank, jobId)
  });

  // add all jobIds to waiting set
  keydb.call('ZADD', KEYS.WAITING_JOBS, ...moveToWaitingPayload);
  return limitedJobIdsToMove.length;
}


// CLEANUP
/**
 * this script is called on a schedule to clean up disconnected workers
 */
export function RLQ_CLEANUP() {
  log('running cleanup')
  const workersLastSeen = keydb.getHash(KEYS.WORKERS_LAST_SEEN);
  const nowTs = +new Date();
  _.each(workersLastSeen, (lastSeenTs, workerId) => {
    if (nowTs - lastSeenTs < GLOBAL_SETTINGS.workerDisconnectTimeout * 1000) return;

    // remove worker from list
    keydb.call('HDEL', KEYS.WORKERS_LAST_SEEN, workerId);

    const stalledJobIds = keydb.call('SMEMBERS', KEYS.workerActiveSet(workerId));
    keydb.call('DEL', KEYS.workerActiveSet(workerId));

    runJobsCompletedWithFullJobIds('TIMEOUT', workerId, ...stalledJobIds);
  });
}
