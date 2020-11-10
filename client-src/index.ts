import * as _ from 'lodash';
import * as async from 'async';
import Redis from 'ioredis';
import { v4 as uuidv4 } from 'uuid';

import { ExtendedIORedis, createRedisConnection } from './ioredis';

function generateJobId() {
  // replace with UUID
  return _.uniqueId(+new Date());
}

type ConcurrentLimitOptions = {
  type: 'concurrent',
  limit: number,
}
type TimeWindowLimitOptions = {
  type: 'concurrent',
  limit: number,
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
  emptyQueueWaitTime: number,
}
type JobHandler = (payload: any, metadata: any) => void;


const DEFAULT_WORKER_OPTIONS: WorkerOptions = {
  maxConcurrent: 50, // 50 concurrent jobs max on this worker
  emptyQueueWaitTime: 100, // 100 ms
}

export class RateLimitQueue {

  private keydb: ExtendedIORedis;
  private clientId: string;

  constructor(connectionOptions) {
    this.clientId = uuidv4();
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


  async enqueueJob(payload, options: JobOptions = {}) {
    const jobId = options.id || generateJobId();
    console.log('enqueueing job', payload);
    const result = await this.keydb.customCommand('RLQ_ADD_JOB', jobId, payload, options);
    if (!result) throw new Error('Failed to enqueue job');
    return result;
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
  private workerPullNextJobTimeout: NodeJS.Timeout;
  private workerPullInterval: NodeJS.Timeout;
  private workerLoadedJobs: any = {};
  private lastWorkerPullWasEmpty = false;
  private numUsedSlots = 0;

  createWorker(handlerFn: JobHandler, options?: Partial<WorkerOptions>) {
    this.workerOptions = _.defaults({}, options, DEFAULT_WORKER_OPTIONS);
    console.log('starting worker!', this.workerOptions);
    this.workerHandlerFn = handlerFn;
    this.startWorker();
  }
  async startWorker() {
    this.workerRunning = true;
    this.workerPullInterval = setInterval(this.pullNextJobs.bind(this), 1000);

    // await async.times(this.workerOptions.maxConcurrent, this.pullNextJob.bind(this));
  }
  pauseWorker() {
    this.workerRunning = false;
  }

  /**
   * This method tries to pull the next job from the queue
   *
   * It takes up 1 concurrency "slot" while it is pulling
   *
   * If we get a job back, it kicks off processing of the job
   * and tries to pull another immediately (assuming there is another slot available)
   * when the job completes, the slot is released and we try to pull another job
   *
   * If we get back a "sweep" signal, this means that the queue went through a batch of jobs but all
   * were rate limited, so we should pull again immediately
   *
   * If we get back an "empty" signal or a "global rate limit exceeded" we set a timeout to try
   * again later, although if the timeout already exists we do nothing. This avoids hammering the
   * queue with repeated requests while it is empty. When it starts pulling jobs again, it will
   * ramp back up.
   */
  async pullNextJob() {
    try {
      // if the worker is paused, dont do anything
      if (!this.workerRunning) return false;

      // if we've reached the concurrent limit, stop pulling jobs
      if (this.numUsedSlots >= this.workerOptions.maxConcurrent) {
        console.log('> all slots full')
        return false;
      }
      this.numUsedSlots++;
      console.log(`[${this.numUsedSlots}] Pulling the next job`);

      // try to get the next job
      const nextJob = await this.keydb.customCommand('RLQ_GET_NEXT_JOB');
      const pullStatus = nextJob?.[0] || 'nostatus';

      if (pullStatus === 'job') {
        const [status, jobId, jobPayloadStr] = nextJob;
        console.log('> loaded job', jobId);
        const jobPayload = JSON.parse(jobPayloadStr);
        this.workerLoadedJobs[jobId] = { payload: jobPayload, status: 'loaded' };
        // we split the processing out so we can pull the jobs quickly and process them in parallel
        this.processJob(jobId);
        if (this.numUsedSlots < this.workerOptions.maxConcurrent) this.pullNextJob();
        return true;
      } else if (pullStatus === 'sweep') {
        console.log('> failed sweep - pull again');
        this.numUsedSlots--;
        this.pullNextJob();
        return true;
      } else {
        // if the queue is currently empty try again later
        // but we will use a single timeout to start pulling again, so we avoid hammering the queue
        // with concurrent requests when we know it is empty
        if (pullStatus === 'empty') {
          console.log('> waiting queue empty');
        } else if (pullStatus === 'global_cap_exceeded') {
          console.log('> global concurrency cap exceeded');
        } else {
          console.log('> unknown status - '+pullStatus);
        }
        const timeoutDelayMs = this.workerOptions.emptyQueueWaitTime;
        if (!this.workerPullNextJobTimeout || (this.workerPullNextJobTimeout as any)._destroyed) {
          this.workerPullNextJobTimeout = setTimeout(this.pullNextJob.bind(this), timeoutDelayMs);
        }
        this.numUsedSlots--;
        return false;
      }

    } catch (err) {
      console.log('error pulling job!', err);
    }
  }
  async processJob(jobId) {
    try {
      const { payload } = this.workerLoadedJobs[jobId];
      await this.workerHandlerFn(payload, { id: jobId });
      console.log(`job ${jobId} succeeded`);
      await this.keydb.customCommand('RLQ_JOB_SUCCESS', jobId);
    } catch (err) {
      console.log(`job ${jobId} failed`, err);
      await this.keydb.customCommand('RLQ_JOB_FAILURE', jobId);
    }
    delete this.workerLoadedJobs[jobId];
    console.log(`> job removed ${jobId}`);
    this.numUsedSlots--;
    console.log(`> now there are ${this.numUsedSlots} active slots`);
    this.pullNextJob();
  }

  async pullNextJobs() {
    try {
      // if the worker is paused, dont do anything
      if (!this.workerRunning) return false;

      console.log('Pulling next chunk of jobs');

      // if we've reached the concurrent limit, stop pulling jobs
      if (this.numUsedSlots >= this.workerOptions.maxConcurrent) {
        console.log('> all slots full');
        // TODO: maybe send a different command?
        await this.keydb.customCommand('RLQ_GET_NEXT_JOBS', 0, this.clientId);
        return false;
      }

      const availableSlots = this.workerOptions.maxConcurrent - this.numUsedSlots;
      this.numUsedSlots += availableSlots; // temporarily mark all slots as used

      let nextJobs = await this.keydb.customCommand('RLQ_GET_NEXT_JOBS', availableSlots, this.clientId);
      const pulledCount = nextJobs.shift(); // first item in array is number of jobs pulled
      this.numUsedSlots -= availableSlots - pulledCount; // subtract any slots we didnt actually use

      console.log(`Pulled ${pulledCount} jobs`);
      console.log('used slots now = ', this.numUsedSlots);
      nextJobs = _.chunk(nextJobs, 3);
      _.each(nextJobs, ([jobId, jobPayloadString, jobMetadataString]) => {
        this.processJob2(jobId, jobPayloadString, jobMetadataString)
      });

    } catch (err) {
      console.log('error pulling job!', err);
    }
  }
  async processJob2(jobId, jobPayloadString, jobMetadataString) {

    try {
      const jobPayload = JSON.parse(jobPayloadString);
      const jobMetadata = JSON.parse(jobMetadataString);
      await this.workerHandlerFn(jobPayload, { id: jobId, ...jobMetadata });
      console.log(`job ${jobId} succeeded`);
      await this.keydb.customCommand('RLQ_JOB_COMPLETED', jobId, true);
    } catch (err) {
      console.log(`job ${jobId} failed`, err);
      await this.keydb.customCommand('RLQ_JOB_COMPLETED', jobId, false);
    }
    delete this.workerLoadedJobs[jobId];
    console.log(`> job removed ${jobId}`);
    this.numUsedSlots--; // slot is now free
    console.log(`> now there are ${this.numUsedSlots} active slots`);
  }



}