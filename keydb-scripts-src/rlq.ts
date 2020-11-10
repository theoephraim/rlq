/**
 * These scripts are run within the keydb process
 * This is similar to running Lua scripts within redis except we are running javascript using ModJS
 *
 * we can trigger keydb calls by calling `keydb.call(command, arg1, arg2, ...)`
 *
 * We will make the conventions of naming our functions in UPPER_SNAKE_CASE prefixed with "RLQ"
 * And we will name our keys starting with a RLQ prefix and separating chunks using double dash
 *
 * RLQ_SOME_FUNCTION
 * RLQ--SOME_KEY
 */

declare var keydb: any;

import * as _ from 'lodash-es';

// also attaches some helpers to keydb object
import { safeReturnValue } from './helpers';

const KEYS = {
  GLOBAL_SETTINGS: 'RLQ--SETTINGS', // HASH of global settings

  TICK_CRON: 'RLQ--TICK_CRON', // holds the reference to the "tick" cron job
  TICK_COUNT: 'RLQ--TICK_COUNT', // counter that increases with each tick

  ACTIVE_JOBS: 'RLQ--ACTIVE', // SET of active job ids
  WAITING_JOBS: 'RLQ--WAITING', // ZSET of job ids ready to be evaluated
  DELAYED_JOBS: 'RLQ--DELAYED', // ZSET of job ids that are in a delayed state, will move to waiting after timestamp passes
  LIMITED_JOBS: 'RLQ--LIMITED', // LIST of job ids that are currently rate limited, will move back to waiting
  SUCCESS_JOBS: 'RLQ--SUCCESS', // SET of successful job ids
  BURIED_JOBS: 'RLQ--BURIED', // SET of job ids that failed and are no longer retrying

  JOB_LIMIT_RULES: 'RLQ--JOB_LIMITS', // HASH of job id to rate limit ids (string separated by "|")

  WORKERS_LAST_SEEN: 'RLQ--WORKERS_LAST_SEEN', // HASH of job id to rate limit ids (string separated by "|")

  jobId: (rawJobId) => `JOB--${rawJobId}`,
  rawJobId: (jobId) => jobId.split('--')[1],
  jobLimitIds: (jobId) => `${jobId}--LIMITS`,

  workerActiveSet: (workerId) => `RLW--${workerId}--ACTIVE`,

  rateLimitType: (limitTypeName) => `RLT--${limitTypeName}`,
  rateLimitId: (limitTypeName, limitValue) => `RL--${limitTypeName}--${limitValue}`,
  rateLimitWaitingSet: (limitId) => `${limitId}--WAITING`,
  rateLimitActiveSet: (limitId) => `${limitId}--ACTIVE`,
}

const PROMOTE_DELAYED_LIMIT = 10000;
const MAX_SWEEP_SIZE = 2000;
// multiplier larger than current timestamp in ms
// so that sorted jobs are in order of -- (priority * PRIORITY_MULTIPLIER) + enqueuedTimestamp
const PRIORITY_MULTIPLIER = 100000000000000;

// LOGGING
function log(...toLog) {
  const toLogAsStrings = _.map(toLog, (item) => {
    if (_.isObject(item)) return JSON.stringify(item);
    return item;
  })
  if (GLOBAL_SETTINGS.enableLogs) {
    keydb.log(toLogAsStrings.join(', '));
  }
}

// SETTINGS
type RLQGlobalSettings = {
  concurrentLimit: number,
  defaultPriority: number,
  activeJobTimeout: number,
  maxFailureRetries: number,
  enableLogs: boolean,
}
const GLOBAL_SETTING_DEFAULTS: RLQGlobalSettings = {
  concurrentLimit: 2000,
  defaultPriority: 3,
  activeJobTimeout: 60 * 5, // in seconds -- 5 minutes
  maxFailureRetries: 5,
  enableLogs: false,
}
// we will store the settings in memory so we dont have to make any calls to fetch
let GLOBAL_SETTINGS:  RLQGlobalSettings = _.clone(GLOBAL_SETTING_DEFAULTS);

function RLQ_SET_GLOBAL_SETTINGS(settingsJson) {
  const newSettings = JSON.parse(settingsJson);
  _.each(GLOBAL_SETTINGS, (val, key) => {
    if (newSettings[key]) GLOBAL_SETTINGS[key] = newSettings[key];
  });
  // persist the new settings to redis
  keydb.setHash(KEYS.GLOBAL_SETTINGS, GLOBAL_SETTINGS);
  keydb.log('SET RLQ SETTINGS', GLOBAL_SETTINGS);
}
function loadGlobalSettings() {
  // we store the settings in memory so we dont have to look them up
  // but we will also persist them, so we have to load them on startup
  GLOBAL_SETTINGS = keydb.getHash(KEYS.GLOBAL_SETTINGS);
  _.defaults(GLOBAL_SETTINGS, GLOBAL_SETTING_DEFAULTS); // load defaults

  // because the settings have been loaded from the db, they have been stringified
  // TODO: convert all options
  if (GLOBAL_SETTINGS.enableLogs as any === 'false') GLOBAL_SETTINGS.enableLogs = false;
  else if (GLOBAL_SETTINGS.enableLogs as any === 'true') GLOBAL_SETTINGS.enableLogs = true;

  log('> loaded global settings', GLOBAL_SETTINGS);
}
function RLQ_GET_GLOBAL_SETTINGS() {
  return safeReturnValue(GLOBAL_SETTINGS);
}


// SETUP + CRON ////////////////////////////////////////////////////////////////////////////////////

/** Handles setup - loading in cron scripts */
function RLQ_INIT() {
  loadGlobalSettings();
  log('> Starting up RLQ!');
  RLQ_START_TICK();
}

function RLQ_START_TICK() {
  log('> starting RLQ tick')
  // clear the count
  keydb.call('DEL', KEYS.TICK_COUNT);

  // KEYDB.CRON call will trigger a script on a timer
  // note that it does not understand js, so the script here is Lua and seems to only work
  // if we trigger using "redis.call"
  return keydb.call('KEYDB.CRON', KEYS.TICK_CRON, 'repeat', 1000, "redis.pcall('RLQ_TICK')");
}

function RLQ_STOP_TICK() {
  return keydb.call('DEL', KEYS.TICK_CRON);
}

/** Triggered once every second to trigger scripts */
function RLQ_TICK() {
  RLQ_PROMOTE_DELAYED();
  return keydb.call('INCR', KEYS.TICK_COUNT);
}

function RLQ_CHECK_TICK() {
  return keydb.call('GET', KEYS.TICK_COUNT);
}

// DEFINING LIMITS /////////////////////////////////////////////////////////////////////////////////
function getLimitTypeIdFromLimitId(limitId) {
  // limit id looks like "RL--LIMITTYPE--LIMITVALUE"
  // and we just want the type
  const limitTypeName = limitId.split('--')[1];
  return KEYS.rateLimitType(limitTypeName);
}
function getLimitTypeInfo(limitTypeId) {
  const limitTypeObj = keydb.getHash(limitTypeId);
  limitTypeObj.limit = parseInt(limitTypeObj.limit);
  return limitTypeObj;
}

function RLQ_DEFINE_LIMIT_TYPE(limitTypeName, optionsJson) {
  const limitTypeId = KEYS.rateLimitType(limitTypeName);
  let options = JSON.parse(optionsJson);

  // TODO: add validation of options
  options = _.pick(options, 'type', 'limit', 'duration');

  keydb.setHash(limitTypeId, options);
}

function RLQ_DESTROY_LIMIT_TYPE(limitTypeName) {
  const limitTypeId = KEYS.rateLimitType(limitTypeName);
  keydb.call('DEL', limitTypeId);
  // TODO: destroy waiting/active sets - what do do about any jobs that are enqueued?
}

// CREATING NEW JOBS ///////////////////////////////////////////////////////////////////////////////

function getJobInfo(jobId) {
  const jobObj = keydb.getHash(jobId);
  jobObj.priority = parseInt(jobObj.priority);
  jobObj.enqueuedAt = parseInt(jobObj.enqueuedAt);
  // jobObj.limitIds = jobObj.limitIds.split('|');
  return jobObj;
}
function getJobLimitIds(jobId) {
  const limitIdsStr = keydb.call('HGET', KEYS.JOB_LIMIT_RULES, jobId);
  return limitIdsStr.split('|');
}


/**
 * Adds multiple jobs to the queue that have the same options
 *
 * passed in as RLQ_ADD_JOBS(options, id1, payload1, id2, payload2)
 */
function RLQ_ADD_JOBS(optionsJson, ...rawJobIdsAndPayloads) {
  const options = JSON.parse(optionsJson);
  options.priority = options.priority || GLOBAL_SETTINGS.defaultPriority;

  // check each limit that was passed in to see if it is defined
  const limitDefinitions = {};
  const limitIds = [];
  _.each(options.limits, ([limitTypeName, limitValue]) => {
    const limitTypeId = KEYS.rateLimitType(limitTypeName);

    const limitTypeInfo = getLimitTypeInfo(limitTypeId);
    log('checking limit type definition', limitTypeId, limitTypeInfo);
    limitDefinitions[limitTypeName] = limitTypeInfo;
    if (!limitDefinitions[limitTypeName]) {
      throw new Error(`Unable to find limit type definition - ${limitTypeName}`);
    }
    limitIds.push(KEYS.rateLimitId(limitTypeName, limitValue))
  });

  const nowTs = +new Date();
  const rawJobs = _.chunk(rawJobIdsAndPayloads, 2);

  const commonJobInfo = _.pickBy({ // _.pickBy removes empty values or keydb.setHash will make an extra call to delete those keys
    initialDelay: options.delay,
    priority: options.priority,
    enqueuedAt: nowTs,
    status: options.delay ? 'DELAYED' : 'WAITING',
  });

  const jobIds = [];
  _.each(rawJobs, ([rawJobId, jobPayload]) => {
    const jobId = KEYS.jobId(rawJobId);
    jobIds.push(jobId);
    keydb.setHash(jobId, { // save job info
      ...commonJobInfo,
      payload: jobPayload,
    });
  });

  const limitRulesSetPayload = [];
  const addToWaitingPayload = [];
  const addToDelayedPayload = [];
  const limitIdsString = limitIds.join('|');
  const runAt = nowTs + ((options.delay || 0) * 1000);
  _.each(jobIds, (jobId, jobIndex) => {
    limitRulesSetPayload.push(jobId, limitIdsString);
    if (options.delay) {
      addToDelayedPayload.push(runAt, jobId);
    } else {
      const waitingRank = options.priority * PRIORITY_MULTIPLIER + nowTs + jobIndex;
      addToWaitingPayload.push(waitingRank, jobId);
    }
  })
  // store the limit ids for each job in another hash
  keydb.call('HMSET', KEYS.JOB_LIMIT_RULES, ...limitRulesSetPayload);
  if (options.delay) {
    keydb.call('ZADD', KEYS.DELAYED_JOBS, ...addToDelayedPayload);
  } else {
    keydb.call('ZADD', KEYS.WAITING_JOBS, ...addToWaitingPayload);
    // add to each rate limit's waiting set
    _.each(limitIds, (limitId) => {
      keydb.call('SADD', KEYS.rateLimitWaitingSet(limitId), ...jobIds);
    });
  }

  return jobIds.length;
}

function RLQ_PROMOTE_DELAYED() {
  const nowTs = +new Date();

  // we try to move all delayed jobs that have had their scheduled time pass (up to a limit)
  const delayedJobIdsToMove = keydb.call('ZRANGEBYSCORE', KEYS.DELAYED_JOBS, '-inf', nowTs, 'LIMIT', 0, PROMOTE_DELAYED_LIMIT);
  if (!delayedJobIdsToMove?.length) return 0;
  log(`promoting ${delayedJobIdsToMove.length} delayed jobs`);

  // remove the job ids from the limited set
  keydb.call('ZREM', KEYS.DELAYED_JOBS, ...delayedJobIdsToMove);

  // delayed jobs (either enqueued with a delay or retrying after a failure) are no longer listed
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
    const rank = jobInfo.priority * PRIORITY_MULTIPLIER + jobInfo.enqueuedAt;
    moveToWaitingPayload.push(rank, jobId)
  });

  // add all jobIds to waiting set
  keydb.call('ZADD', KEYS.WAITING_JOBS, ...moveToWaitingPayload);
  return delayedJobIdsToMove.length;
}


function RLQ_PROMOTE_LIMITED(numJobs) {
  // const limitedJobIdsToMove = keydb.call('ZRANGEBYSCORE', KEYS.LIMITED_JOBS, '-inf', 'inf', 'LIMIT', 0, numJobs);
  const limitedJobIdsToMove = keydb.call('LRANGE', KEYS.LIMITED_JOBS, 0, numJobs - 1);
  if (!limitedJobIdsToMove.length) return 0;

  // keydb.call('ZREM', KEYS.LIMITED_JOBS, ...limitedJobIdsToMove);
  keydb.call('LTRIM', KEYS.LIMITED_JOBS, limitedJobIdsToMove.length, -1);

  log(`promoting ${limitedJobIdsToMove.length} limited jobs`);

  const moveToWaitingPayload = [];
  _.each(limitedJobIdsToMove, (jobId) => {
    const jobInfo = getJobInfo(jobId);
    keydb.setHash(jobId, { status: 'WAITING' });

    // we rank the jobs by priority and then enqueued time
    const rank = jobInfo.priority * PRIORITY_MULTIPLIER + jobInfo.enqueuedAt;
    moveToWaitingPayload.push(rank, jobId)
  });

  // add all jobIds to waiting set
  keydb.call('ZADD', KEYS.WAITING_JOBS, ...moveToWaitingPayload);
  return limitedJobIdsToMove.length;
}

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
function RLQ_GET_NEXT_JOBS(numJobs: number, workerId: string) {
  const nowTs = +new Date();

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
  if (numWaiting < MAX_SWEEP_SIZE) {
    // make sure we have a full "sweep" of jobs in the waiting queue
    RLQ_PROMOTE_LIMITED(MAX_SWEEP_SIZE - numWaiting);
  }

  const maxJobsToFetch = Math.min(numJobs, remainingGlobalLimitSlots);
  const jobIdsToActivate = [];
  const jobIdsToLimit = [];
  const limitTypeInfoCache = {};
  const limitsCache = {};
  const limitsByJobId = {};

  // we'll pull a full "sweep" of job ids at once to check
  const waitingJobIdsToCheck = keydb.call('ZRANGEBYSCORE', KEYS.WAITING_JOBS, '-inf', 'inf', 'LIMIT', 0, MAX_SWEEP_SIZE);
  const checkedJobIds = [];
  if (!waitingJobIdsToCheck?.length) {
    log('> no more jobs to check');
    return [0];
  }

  // fetch all of the rate limit ids for these jobs (these are stored as strings, but we will split back to an array)
  const jobLimitsStrings = keydb.call('HMGET', KEYS.JOB_LIMIT_RULES, ...waitingJobIdsToCheck);
  // build up cache of info about each limit which we can reuse for later next jobs we encounter
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
        const limitActiveCount = keydb.call('SCARD', KEYS.rateLimitActiveSet(limitId));
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
      log('> updating job info to active');
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
      log('> setting active job timeout');
      // set backup timeout to remove from active set
      // TODO: need to figure out how to clean these up from other places
      keydb.call('EXPIREMEMBER', KEYS.ACTIVE_JOBS, jobId, GLOBAL_SETTINGS.activeJobTimeout);

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


function RLQ_JOB_COMPLETED(rawJobId, jobSucceeded) {
  const jobId = KEYS.jobId(rawJobId);

  log('job complete', jobId, jobSucceeded);

  // update the job info
  keydb.setHash(jobId, {
    ...jobSucceeded ? {
      status: 'SUCCESS',
      completedAt: +new Date(),
    } : {
      status: 'FAILED',
      lastFailedAt: +new Date(),
    }
  });

  // remove from global active set
  log('removing job from global active set')
  keydb.call('SREM', KEYS.ACTIVE_JOBS, jobId);

  // remove from worker active set
  const jobInfo = getJobInfo(jobId);
  log('removing job from worker active set')
  keydb.call('SREM', KEYS.workerActiveSet(jobInfo.workerId), jobId);

  // remove job from each rate limit "active" set
  const limitIds = getJobLimitIds(jobId);
  _.each(limitIds, (limitId) => {
    log('> removing job from active in rate limit', limitId);
    keydb.call('SREM', KEYS.rateLimitActiveSet(limitId), jobId);
  });

  if (jobSucceeded) {
    keydb.call('SADD', KEYS.SUCCESS_JOBS, jobId);
    // delete set of limit IDs we stored for the job
    keydb.call('HDEL', KEYS.JOB_LIMIT_RULES, jobId);
  } else {
    const failCount = keydb.call('HINCRBY', jobId, 'failedCount', 1);
    if (failCount > GLOBAL_SETTINGS.maxFailureRetries) {
      keydb.call('SADD', KEYS.BURIED_JOBS, jobId);
    } else {
      // TODO: exponential backoff instead of linear
      const runAt = +new Date() + (failCount * 15 * 1000);
      keydb.call('ZADD', KEYS.DELAYED_JOBS, runAt, jobId);
    }
  }
}


///
function RLQ_STATS() {
  const stats = {
    active: keydb.call('SCARD', KEYS.ACTIVE_JOBS),
    waiting: keydb.call('ZCARD', KEYS.WAITING_JOBS),
    limited: keydb.call('LLEN', KEYS.LIMITED_JOBS),
    delayed: keydb.call('ZCARD', KEYS.DELAYED_JOBS),
    success: keydb.call('SCARD', KEYS.SUCCESS_JOBS),
    buried: keydb.call('SCARD', KEYS.BURIED_JOBS),
  }
  return safeReturnValue(stats);
}

// Register all externally callable functions
keydb.register(RLQ_SET_GLOBAL_SETTINGS);
keydb.register(RLQ_GET_GLOBAL_SETTINGS);

keydb.register(RLQ_INIT);
keydb.register(RLQ_START_TICK);
keydb.register(RLQ_STOP_TICK);
keydb.register(RLQ_TICK);
keydb.register(RLQ_CHECK_TICK);

keydb.register(RLQ_PROMOTE_DELAYED);
keydb.register(RLQ_PROMOTE_LIMITED);

keydb.register(RLQ_DEFINE_LIMIT_TYPE);
keydb.register(RLQ_DESTROY_LIMIT_TYPE);

keydb.register(RLQ_ADD_JOBS);
keydb.register(RLQ_GET_NEXT_JOBS);
keydb.register(RLQ_JOB_COMPLETED);

keydb.register(RLQ_STATS);