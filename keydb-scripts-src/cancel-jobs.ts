import * as _ from 'lodash-es';

// also attaches some helpers to keydb object
import { log, safeReturnValue } from './helpers';

import { KEYS, GLOBAL_SETTINGS } from './settings';
import { getJobInfo, getJobWaitingRank } from './job-info';

export function RLQ_JOB_CANCEL(...rawJobIds: string[]) {
  const jobIds = _.map(rawJobIds, (rawJobId) => KEYS.jobId(rawJobId));

  // remove job(s) from each rate limit "active" set
  log('> removing job(s) from rate limit active/waiting sets');
  const jobLimitsStrings = keydb.call('HMGET', KEYS.JOB_LIMIT_RULES, ...jobIds);
  const jobIdsByRateLimitId = {};
  const removeFromWaitingPayload = [];
  // const removeFromLimitedPayload = [];

  _.each(jobIds, (jobId, jobIndex) => {
    const limitString = jobLimitsStrings[jobIndex];
    const limitIds = limitString.split('|');
    const jobInfo = getJobInfo(jobId);
    if (jobInfo.status === 'ACTIVE') {
      // we cannot cancel active jobs, so nothing to do here
      return;
    } else if (jobInfo.status === 'WAITING') {
      removeFromWaitingPayload.push(jobId);

    } else if (jobInfo.status === 'LIMITED') {
      // must remove list items one at a time
      keydb.call('LREM', KEYS.LIMITED_JOBS, 0, jobId);

    } else if (jobInfo.status === 'FAIL') {
      // nothing to do here

    } else if (jobInfo.status === 'RETRY') {
      // job is retrying -

    } else if (jobInfo.status === 'BURY') {

    }
    keydb.setHash(jobId, { status: 'CANCEL' });

    _.each(limitIds, (limitId) => {
      if (!jobIdsByRateLimitId[limitId]) jobIdsByRateLimitId[limitId] = [];
      jobIdsByRateLimitId[limitId].push(jobId);
    });
  });
  _.each(jobIdsByRateLimitId, (jobIdsForRateLimit, limitId) => {
    log('> SREM', KEYS.rateLimitActiveSet(limitId), jobIdsForRateLimit);
    keydb.call('SREM', KEYS.rateLimitActiveSet(limitId), ...jobIdsForRateLimit);
  });

  if (removeFromWaitingPayload.length) {
    keydb.call('ZREM', KEYS.WAITING_JOBS, ...removeFromWaitingPayload);
  }




}