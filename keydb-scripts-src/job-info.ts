
import { KEYS } from './settings';

// multiplier larger than current timestamp in ms
// so that sorted jobs are in order of -- (priority * PRIORITY_MULTIPLIER) + enqueuedTimestamp
export const PRIORITY_MULTIPLIER = 100000000000000;

export function getJobInfo(jobId) {
  const jobObj = keydb.getHash(jobId);
  jobObj.priority = parseInt(jobObj.priority);
  jobObj.enqueuedAt = parseInt(jobObj.enqueuedAt);
  // jobObj.limitIds = jobObj.limitIds.split('|');
  return jobObj;
}

export function getJobLimitIds(jobId) {
  const limitIdsStr = keydb.call('HGET', KEYS.JOB_LIMIT_RULES, jobId);
  return limitIdsStr.split('|');
}

export function getJobWaitingRank(jobInfo) {
  return jobInfo.priority * PRIORITY_MULTIPLIER + jobInfo.enqueuedAt;
}


