type Hash = { [key: string]: any };

declare var keydb: {
  // built-in
  log(toLog: string): void,
  call(fnName: string, ...args: (string | number)[]),
  register(fn: (...any) => any),

  // added funciton - see helpers.ts
  setHash(key: string, hash: Hash): void,
  getHash(key: string): Hash,
};

import * as _ from 'lodash-es';

import { KEYS, GLOBAL_SETTINGS } from './settings';


// add some helpers to keydb object directly
// since these are very general and not specific to our setup
keydb.setHash = function(key, obj) {
  const keysToRemove = _.keys(_.pickBy(obj, _.isNil));
  const keysAndValuesToSet = _.omit(obj, keysToRemove);
  if (keysToRemove.length) {
    keydb.call('HDEL', key, ...keysToRemove);
  }
  if (_.keys(keysAndValuesToSet).length) {
    keydb.call('HMSET', key, ..._.flatten(_.toPairs(keysAndValuesToSet)));
  }
}
keydb.getHash = function(key) {
  // HGETALL returns an array of [key1, val1, key2, val2]
  const hashAsArray = keydb.call('HGETALL', key);
  // so we use lodash helpers to reconstruct a key/value object
  const hashObj = _.fromPairs(_.chunk(hashAsArray, 2));
  return hashObj;
}


// redis/keydb is unable to return an object
// so we must reconvert to an array before a custom script returns a final value
// in this case we return an array with a special first element so that our client
// can know that it should reconstruct an object from this data
export function safeReturnValue(val) {
  if (_.isObject(val)) return JSON.stringify(val);
  return val;
}

export function log(...toLog) {
  const toLogAsStrings = _.map(toLog, (item) => {
    if (_.isObject(item)) return JSON.stringify(item);
    return item;
  })
  if (GLOBAL_SETTINGS.enableLogs) {
    keydb.log(toLogAsStrings.join(', '));
  }
}
