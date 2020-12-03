import * as _ from 'lodash-es';

// also attaches some helpers to keydb object
import { log, safeReturnValue } from './helpers';

import { KEYS, GLOBAL_SETTINGS } from './settings';
import { getLimitTypeInfo, getLimitTypeIdFromLimitId } from './limits';

export function RLQ_STATS() {
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
export function RLQ_LIMIT_STATS() {
  log('>>>>>>>>>>>>>>>>>>> LIMIT STATS!');
  const totalActive = keydb.call('SCARD', KEYS.ACTIVE_JOBS);
  log('active jobs', totalActive)
  // const totalQueued = keydb.call('ZCARD', KEYS.WAITING_JOBS) + keydb.call('LLEN', KEYS.LIMITED_JOBS);
  const stats = {
    globalActive: totalActive,
    globalLimit: GLOBAL_SETTINGS.concurrentLimit,
  };
  const rateLimitTypeIds = keydb.call('KEYS', 'RLT--*');
  log('rate limit types', rateLimitTypeIds);
  const limitTypeInfo = {};
  _.each(rateLimitTypeIds, (limitTypeId) => {
    limitTypeInfo[limitTypeId] = getLimitTypeInfo(limitTypeId);
  });
  const rateLimitSets = keydb.call('KEYS', 'RL--*');
  log(rateLimitSets);
  const rateLimits = {};
  _.each(rateLimitSets, (limitSetKey) => {
    const keyParts = limitSetKey.split('--');
    const activeOrWaiting = keyParts.pop().toLowerCase(); // active or waiting
    const limitId = keyParts.join('--');
    const limitTypeId = getLimitTypeIdFromLimitId(limitId);
    if (!rateLimits[limitId]) {
      rateLimits[limitId] = {
        limit: limitTypeInfo[limitTypeId].limit,
      }
    }
    rateLimits[limitId][activeOrWaiting] = keydb.call('SCARD', limitSetKey) || 0;
  });
  log(rateLimits)

  const summary = _.map(rateLimits, (limitStats, limitId) => (
    `${limitId} -- ${limitStats.active} / ${limitStats.limit}  (${limitStats.waiting || 0} waiting)`
  ));
  log(summary);

  return safeReturnValue(rateLimits);
}