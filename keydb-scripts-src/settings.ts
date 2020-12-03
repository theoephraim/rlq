import * as _ from 'lodash-es';

import { log, safeReturnValue } from './helpers';

// SETTINGS / CONSTANTS
type RLQGlobalSettings = {
  concurrentLimit: number,
  defaultPriority: number,
  activeJobTimeout: number,
  maxFailureRetries: number,
  maxRerunRetries: number,
  enableLogs: boolean,
  workerDisconnectTimeout: number,
  maxSweepSize: number,
  promoteDelayedLimit: number,
}
const GLOBAL_SETTING_DEFAULTS: RLQGlobalSettings = {
  concurrentLimit: 2000,
  defaultPriority: 3,
  activeJobTimeout: 60 * 5, // in seconds -- 5 minutes
  maxFailureRetries: 5,
  maxRerunRetries: 5,
  enableLogs: false,
  workerDisconnectTimeout: 15, // in seconds
  maxSweepSize: 5000,
  promoteDelayedLimit: 10000,
}

// we will store the settings in memory so we dont have to make any calls to fetch
export let GLOBAL_SETTINGS:  RLQGlobalSettings = _.clone(GLOBAL_SETTING_DEFAULTS);

export function RLQ_SET_GLOBAL_SETTINGS(settingsJson) {
  const newSettings = JSON.parse(settingsJson);
  _.each(GLOBAL_SETTINGS, (val, key) => {
    if (newSettings[key]) GLOBAL_SETTINGS[key] = newSettings[key];
  });
  // persist the new settings to redis
  keydb.setHash(KEYS.GLOBAL_SETTINGS, GLOBAL_SETTINGS);
  log('SET RLQ SETTINGS', GLOBAL_SETTINGS);
}
export function loadGlobalSettings() {
  // we store the settings in memory so we dont have to look them up
  // but we will also persist them, so we have to load them on startup
  GLOBAL_SETTINGS = <RLQGlobalSettings> keydb.getHash(KEYS.GLOBAL_SETTINGS)
  _.defaults(GLOBAL_SETTINGS, GLOBAL_SETTING_DEFAULTS); // load defaults

  // because the settings have been loaded from the db, they have been stringified
  // TODO: convert all options
  if (GLOBAL_SETTINGS.enableLogs as any === 'false') GLOBAL_SETTINGS.enableLogs = false;
  else if (GLOBAL_SETTINGS.enableLogs as any === 'true') GLOBAL_SETTINGS.enableLogs = true;

  log('> loaded global settings', GLOBAL_SETTINGS);
}
export function RLQ_GET_GLOBAL_SETTINGS() {
  return safeReturnValue(GLOBAL_SETTINGS);
}



export const KEYS = {
  GLOBAL_SETTINGS: 'RLQ--SETTINGS', // HASH of global settings

  TICK_CRON: 'RLQ--TICK_CRON', // holds the reference to the "tick" cron job
  TICK_COUNT: 'RLQ--TICK_COUNT', // counter that increases with each tick
  CLEANUP_CRON: 'RLQ--CLEANUP_CRON', // holds the reference to the "cleanup" cron job

  ACTIVE_JOBS: 'RLQ--ACTIVE', // SET of active job ids
  WAITING_JOBS: 'RLQ--WAITING', // ZSET of job ids ready and waiting to be evaluated
  DELAYED_JOBS: 'RLQ--DELAYED', // ZSET of job ids that are in a delayed state, will move to waiting after timestamp passes
  LIMITED_JOBS: 'RLQ--LIMITED', // LIST of job ids that are currently rate limited, will move back to waiting
  SUCCESS_JOBS: 'RLQ--SUCCESS', // SET of successful job ids
  BURIED_JOBS: 'RLQ--BURIED', // SET of job ids that failed and are no longer retrying

  JOB_LIMIT_RULES: 'RLQ--JOB_LIMITS', // HASH of job id to rate limit ids (string separated by "|")

  WORKERS_LAST_SEEN: 'RLQ--WORKERS_LAST_SEEN', // HASH of job id to rate limit ids (string separated by "|")

  jobId: (rawJobId) => `JOB--${rawJobId}`,
  rawJobId: (jobId) => jobId.split('--')[1],

  workerActiveSet: (workerId) => `RLW--${workerId}--ACTIVE`,

  rateLimitType: (limitTypeName) => `RLT--${limitTypeName}`,
  rateLimitId: (limitTypeName, limitValue) => `RL--${limitTypeName}--${limitValue}`,
  rateLimitWaitingSet: (limitId) => `${limitId}--WAITING`,
  rateLimitActiveSet: (limitId) => `${limitId}--ACTIVE`,
}

