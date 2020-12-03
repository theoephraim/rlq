import * as _ from 'lodash';
import * as async from 'async';
import Redis from 'ioredis';
import { v4 as uuidv4 } from 'uuid';
import Debug from "debug";

import { ExtendedIORedis, createRedisConnection } from './ioredis';

const debug = Debug('rlq');

function generateJobId() {
  // replace with UUID
  return _.uniqueId(+new Date());
}

type ConcurrentLimitOptions = {
  type: 'concurrent',
  limit: number,
}
type TimeWindowLimitOptions = {
  type: 'time_window',
  limit: number,
  durationSeconds: number,
}
type LimitOptions = ConcurrentLimitOptions | TimeWindowLimitOptions;

type JobOptions = {
  id?: string | number,
  delay?: number,
  priority?: number,
  limits?: [string, string|number][],
}

type WorkerOptions = {
  maxConcurrent: number,
  pullInterval: number, // ms
}
type JobHandler = (payload: any, metadata: any) => void;


const DEFAULT_WORKER_OPTIONS: WorkerOptions = {
  maxConcurrent: 50, // 50 concurrent jobs max on this worker
  pullInterval: 1000, // pull jobs once every second
}

export class RateLimitQueue {

  private keydb: ExtendedIORedis;
  private workerId: string;

  constructor(connectionOptions) {
    this.workerId = uuidv4();
    this.keydb = createRedisConnection(connectionOptions);
  }

  async init(config, flush) {
    if (flush) await this.keydb.customCommand('FLUSHALL');
    if (!_.isEmpty(config)) await this.keydb.customCommand('RLQ_SET_GLOBAL_SETTINGS', config);
    await this.keydb.customCommand('RLQ_INIT');
  }

  async defineLimitType(bucketName, options: LimitOptions) {
    const result = await this.keydb.customCommand('RLQ_DEFINE_LIMIT_TYPE', bucketName, options);
  }

  async enqueueJobs(
    jobs: { [jobId: string]: Record<string, any>} | any[],
    options: JobOptions = {}
  ) {
    // if we received an object, we treat it as a map of jobId -> payload
    // otherwise we treat it as an array of payloads
    let jobPayloadsById;
    if (_.isObject(jobs)) jobPayloadsById = jobs;
    else jobPayloadsById = _.keyBy(jobs, () => generateJobId());

    const jobArgs = _.flatten(_.toPairs(jobPayloadsById));
    const numJobsEnqueued = await this.keydb.customCommand('RLQ_ADD_JOBS', options, ...jobArgs);
    if (!numJobsEnqueued) throw new Error('Failed to enqueue job(s)');
    return _.keys(jobPayloadsById);
  }


  private workerOptions: WorkerOptions;
  private workerHandlerFn: JobHandler;
  private workerRunning = false;
  private workerPullInterval: NodeJS.Timeout;
  private numUsedSlots = 0;

  createWorker(handlerFn: JobHandler, options?: Partial<WorkerOptions>) {
    this.workerOptions = _.defaults({}, options, DEFAULT_WORKER_OPTIONS);
    debug('starting worker!', this.workerOptions);
    this.workerHandlerFn = handlerFn;
    this.startWorker();
  }
  async startWorker() {
    this.workerRunning = true;
    this.workerPullInterval = setInterval(this.pullNextJobs.bind(this), this.workerOptions.pullInterval);

    // await async.times(this.workerOptions.maxConcurrent, this.pullNextJob.bind(this));
  }
  pauseWorker() {
    this.workerRunning = false;
  }
  async pullNextJobs() {
    try {
      // if the worker is paused, dont do anything
      if (!this.workerRunning) return false;

      debug('Pulling next chunk of jobs');

      // if we've reached the concurrent limit, stop pulling jobs
      if (this.numUsedSlots >= this.workerOptions.maxConcurrent) {
        debug('> all slots full');
        // TODO: maybe send a different command?
        await this.keydb.customCommand('RLQ_GET_NEXT_JOBS', 0, this.workerId);
        return false;
      }

      const availableSlots = this.workerOptions.maxConcurrent - this.numUsedSlots;
      this.numUsedSlots += availableSlots; // temporarily mark all slots as used

      let nextJobs = await this.keydb.customCommand('RLQ_GET_NEXT_JOBS', availableSlots, this.workerId);
      const pulledCount = nextJobs.shift(); // first item in array is number of jobs pulled
      this.numUsedSlots -= availableSlots - pulledCount; // subtract any slots we didnt actually use

      debug(`Pulled ${pulledCount} jobs`);
      debug('used slots now = ', this.numUsedSlots);
      nextJobs = _.chunk(nextJobs, 3);
      _.each(nextJobs, ([jobId, jobPayloadString, jobMetadataString]) => {
        this.processJob(jobId, jobPayloadString, jobMetadataString)
      });

    } catch (err) {
      debug('error pulling job!', err);
    }
  }
  async processJob(jobId, jobPayloadString, jobMetadataString) {

    try {
      const jobPayload = JSON.parse(jobPayloadString);
      const jobMetadata = JSON.parse(jobMetadataString);
      await this.workerHandlerFn(jobPayload, { id: jobId, ...jobMetadata });
      debug(`job ${jobId} succeeded`);
      await this.keydb.customCommand('RLQ_JOB_COMPLETED', 'SUCCESS', this.workerId, jobId);
    } catch (err) {

      let resultCode = 'FAIL';
      if (err.rlqNoRetry) {
        // this tells RLQ to move the job directly to "buried" state
        // instead of following the normal retry w/ exponential backoff flow
        resultCode = 'FAIL|BURY';
      } else if (err.rlqRetryAfterDelay) {
        resultCode = 'RETRY';

        if (_.isNumber(err.rlqRetryAfterDelay)) {
          resultCode = `RETRY|${err.rlqRetryAfterDelay}`
        }
      }

      debug(`job ${jobId} caught error (${resultCode})`, err);

      await this.keydb.customCommand('RLQ_JOB_COMPLETED', resultCode, this.workerId, jobId);
    }
    debug(`> job removed ${jobId}`);
    this.numUsedSlots--; // slot is now free
    debug(`> now there are ${this.numUsedSlots} active slots`);
  }

}