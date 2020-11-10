import * as _ from 'lodash';
import IORedis from 'ioredis';
import { Redis } from 'ioredis';

export type ExtendedIORedis = Redis & {
  customCommand(command: string, ...commandArgs: any[]);
}

export function createRedisConnection(options) {
  const redis = new IORedis(options) as ExtendedIORedis;

  redis.customCommand = async function customCommand(commandStr: string, ...commandArgs: any[]) {
    const commandArgsStrings = _.map(commandArgs, (arg) => {
      if (_.isString(arg)) return arg;
      if (_.isObject(arg)) return JSON.stringify(arg);
      return arg.toString();
    });

    const response = await redis.send_command(commandStr, commandArgsStrings)
    if (_.isArray(response)) return response;
    return response?.toString ? response?.toString() : null;
  };

  return redis;

}



