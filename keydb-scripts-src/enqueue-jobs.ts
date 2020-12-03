import * as _ from 'lodash-es';

// also attaches some helpers to keydb object
import { log, safeReturnValue } from './helpers';

import { KEYS, GLOBAL_SETTINGS } from './settings';
import { getJobInfo, getJobWaitingRank } from './job-info';
import { getLimitTypeInfo, getLimitTypeIdFromLimitId } from './limits';

/**
 * Adds multiple jobs to the queue that have the same options
 *
 * passed in as RLQ_ADD_JOBS(options, id1, payload1, id2, payload2)
 */
export function RLQ_ADD_JOBS(optionsJson, ...rawJobIdsAndPayloads) {
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
      const waitingRank = getJobWaitingRank(commonJobInfo);
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
