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

import * as _ from 'lodash-es';

// attaches some helper methdos to keydb object directly
import './helpers';


// import all of the functions we need to register
// this makes them callable from keydb as if they were built-in
import { RLQ_JOB_CANCEL } from './cancel-jobs';
import { RLQ_JOB_COMPLETED } from './complete-jobs';
import { RLQ_ADD_JOBS } from './enqueue-jobs';
import { RLQ_GET_NEXT_JOBS } from './get-next-jobs';
import {
  RLQ_INIT,
  RLQ_START_TICK, RLQ_STOP_TICK, RLQ_TICK, RLQ_CHECK_TICK,
  RLQ_PROMOTE_DELAYED, RLQ_PROMOTE_LIMITED,
  RLQ_CLEANUP
} from './internals';
import { RLQ_DEFINE_LIMIT_TYPE, RLQ_DESTROY_LIMIT_TYPE } from './limits';
import { RLQ_GET_GLOBAL_SETTINGS, RLQ_SET_GLOBAL_SETTINGS } from './settings';
import { RLQ_STATS, RLQ_LIMIT_STATS } from './stats';

// Register all externally callable functions
keydb.register(RLQ_SET_GLOBAL_SETTINGS);
keydb.register(RLQ_GET_GLOBAL_SETTINGS);

keydb.register(RLQ_INIT);
keydb.register(RLQ_START_TICK);
keydb.register(RLQ_STOP_TICK);
keydb.register(RLQ_TICK);
keydb.register(RLQ_CHECK_TICK);
keydb.register(RLQ_CLEANUP);

keydb.register(RLQ_PROMOTE_DELAYED);
keydb.register(RLQ_PROMOTE_LIMITED);

keydb.register(RLQ_DEFINE_LIMIT_TYPE);
keydb.register(RLQ_DESTROY_LIMIT_TYPE);

keydb.register(RLQ_ADD_JOBS);
keydb.register(RLQ_GET_NEXT_JOBS);
keydb.register(RLQ_JOB_COMPLETED);

keydb.register(RLQ_JOB_CANCEL);

keydb.register(RLQ_STATS);
keydb.register(RLQ_LIMIT_STATS);
