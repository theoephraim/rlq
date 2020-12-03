import * as _ from 'lodash-es';

// also attaches some helpers to keydb object
import { log, safeReturnValue } from './helpers';

import {
  KEYS, GLOBAL_SETTINGS
} from './settings';


export function getLimitTypeIdFromLimitId(limitId) {
  // limit id looks like "RL--LIMITTYPE--LIMITVALUE"
  // and we just want the type
  const limitTypeName = limitId.split('--')[1];
  return KEYS.rateLimitType(limitTypeName);
}
export function getLimitTypeInfo(limitTypeId) {
  const limitTypeObj = keydb.getHash(limitTypeId);
  limitTypeObj.limit = parseInt(limitTypeObj.limit);
  return limitTypeObj;
}

export function RLQ_DEFINE_LIMIT_TYPE(limitTypeName, optionsJson) {
  const limitTypeId = KEYS.rateLimitType(limitTypeName);
  let options = JSON.parse(optionsJson);

  // TODO: add validation of options
  options = _.pick(options, 'type', 'limit', 'duration');

  keydb.setHash(limitTypeId, options);
}

export function RLQ_DESTROY_LIMIT_TYPE(limitTypeName) {
  const limitTypeId = KEYS.rateLimitType(limitTypeName);
  keydb.call('DEL', limitTypeId);
  // TODO: destroy waiting/active sets - what do do about any jobs that are enqueued?
}
