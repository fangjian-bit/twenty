import { Logger } from '@nestjs/common';

import { type Job } from 'bullmq';
import { type Redis } from 'ioredis';
import RedisMock from 'ioredis-mock';
import { isDefined } from 'twenty-shared/utils';

import { ENQUEUE_JOB_PRIORITY } from 'src/engine/core-modules/application/application-job/constants/enqueue-job-priority.constant';
import { BullMQDriver } from 'src/engine/core-modules/message-queue/drivers/bullmq.driver';
import { type QueueJobOptions } from 'src/engine/core-modules/message-queue/drivers/interfaces/job-options.interface';
import { MessageQueue } from 'src/engine/core-modules/message-queue/message-queue.constants';
import { buildApplicationGroupKey } from 'src/engine/core-modules/message-queue/utils/build-application-group-key.util';
import { buildWorkspaceGroupKey } from 'src/engine/core-modules/message-queue/utils/build-workspace-group-key.util';
import { MetricsKeys } from 'src/engine/core-modules/metrics/types/metrics-keys.type';
import { SERVER_ROUTE_DISPATCH_JOB_PRIORITY } from 'src/engine/core-modules/server-route-trigger/constants/server-route-dispatch-job-priority.constant';

type ScenarioJobData = { label: string; workspaceId?: string };

type StoredJob = {
  id: string;
  name: string;
  data: ScenarioJobData;
  opts: { priority?: number; fairTag?: number };
  timestamp: number;
  insertionIndex: number;
};

type RedisClientDouble = {
  runCommand: (name: string, args: (string | number)[]) => Promise<unknown>;
};

type BullMQDouble = {
  jobs: StoredJob[];
  nextInsertionIndex: number;
  processor: ((job: Job) => Promise<void>) | undefined;
  listeners: Record<string, () => void>;
  client: RedisClientDouble | undefined;
  store: (
    name: string,
    data: ScenarioJobData,
    opts: StoredJob['opts'],
  ) => { id: string };
  takeNext: () => StoredJob | undefined;
  reset: (client: RedisClientDouble) => void;
};

const mockBullMQ: BullMQDouble = {
  jobs: [],
  nextInsertionIndex: 0,
  processor: undefined,
  listeners: {},
  client: undefined,
  store: (name, data, opts) => {
    const job: StoredJob = {
      id: `job-${mockBullMQ.nextInsertionIndex}`,
      name,
      data,
      opts,
      timestamp: Date.now(),
      insertionIndex: mockBullMQ.nextInsertionIndex,
    };

    mockBullMQ.nextInsertionIndex += 1;
    mockBullMQ.jobs.push(job);

    return { id: job.id };
  },
  // BullMQ serves the lowest priority first and keeps the insertion order on ties
  takeNext: () => {
    const [next] = [...mockBullMQ.jobs].sort(
      (left, right) =>
        (left.opts.priority ?? 0) - (right.opts.priority ?? 0) ||
        left.insertionIndex - right.insertionIndex,
    );

    if (!isDefined(next)) {
      return undefined;
    }

    mockBullMQ.jobs = mockBullMQ.jobs.filter((job) => job !== next);

    return next;
  },
  reset: (client) => {
    mockBullMQ.jobs = [];
    mockBullMQ.nextInsertionIndex = 0;
    mockBullMQ.processor = undefined;
    mockBullMQ.listeners = {};
    mockBullMQ.client = client;
  },
};

jest.mock('bullmq', () => ({
  Queue: jest.fn().mockImplementation(() => ({
    get client() {
      return Promise.resolve(mockBullMQ.client);
    },
    add: async (name: string, data: ScenarioJobData, opts: StoredJob['opts']) =>
      mockBullMQ.store(name, data, opts),
    addBulk: async (
      jobs: { name: string; data: ScenarioJobData; opts: StoredJob['opts'] }[],
    ) => jobs.map(({ name, data, opts }) => mockBullMQ.store(name, data, opts)),
    setGlobalConcurrency: async () => undefined,
    removeGlobalConcurrency: async () => undefined,
  })),
  Worker: jest
    .fn()
    .mockImplementation(
      (_queueName: string, processor: (job: Job) => Promise<void>) => {
        mockBullMQ.processor = processor;

        return {
          on: (event: string, listener: () => void) => {
            mockBullMQ.listeners[event] = listener;
          },
        };
      },
    ),
  MetricsTime: { ONE_WEEK: 1 },
}));

const QUEUE = MessageQueue.logicFunctionQueue;
const KEY_PREFIX = 'bull';
const JOB_NAME = 'scenario-job';
const FAIRNESS_FLAG = 'IS_WORKER_FAIRNESS_ENABLED';

const flushAsyncWork = async () => {
  for (let round = 0; round < 10; round += 1) {
    await new Promise((resolve) => setImmediate(resolve));
  }
};

const labels = (prefix: string, count: number, from = 1) =>
  Array.from({ length: count }, (_, index) => `${prefix}${from + index}`);

const createRedisClientDouble = (redis: Redis): RedisClientDouble => ({
  runCommand: async (name, args) => {
    if (name === 'eval') {
      const [script, numberOfKeys, ...scriptArguments] = args;

      return redis.eval(
        String(script),
        Number(numberOfKeys),
        ...scriptArguments,
      );
    }

    // ioredis-mock has no ZADD GT, the only ZADD the driver issues
    if (name === 'zadd') {
      const [key, , score, member] = args;
      const currentScore = await redis.zscore(String(key), String(member));

      if (currentScore === null || Number(score) > Number(currentScore)) {
        await redis.zadd(String(key), Number(score), String(member));
      }

      return undefined;
    }

    throw new Error(`Unexpected Redis command ${name}`);
  },
});

type EnqueueOptions = { workspaceId?: string } & Pick<
  QueueJobOptions,
  'groupKeys' | 'priority'
>;

const createHarness = async ({
  isFairnessEnabled,
}: {
  isFairnessEnabled: boolean;
}) => {
  const redis = new RedisMock();

  await redis.flushall();
  mockBullMQ.reset(createRedisClientDouble(redis));

  const metricsService = {
    recordHistogram: jest.fn(),
    incrementCounterForEvent: jest.fn().mockResolvedValue(undefined),
  };
  const driver = new BullMQDriver(
    { prefix: KEY_PREFIX } as never,
    metricsService as never,
    {
      get: (key: string) =>
        key === FAIRNESS_FLAG ? isFairnessEnabled : undefined,
    } as never,
    { emit: jest.fn() } as never,
  );

  driver.register(QUEUE);

  const processedLabels: string[] = [];

  driver.work<ScenarioJobData>(QUEUE, async ({ data }) => {
    processedLabels.push(data.label);
  });
  await flushAsyncWork();

  const processOne = async (): Promise<boolean> => {
    const job = mockBullMQ.takeNext();

    if (!isDefined(job) || !isDefined(mockBullMQ.processor)) {
      return false;
    }

    await mockBullMQ.processor({
      ...job,
      attemptsStarted: 1,
      updateData: jest.fn(),
      updateProgress: jest.fn(),
    } as unknown as Job);
    await flushAsyncWork();

    return true;
  };

  const enqueue = async (
    label: string,
    { workspaceId, ...options }: EnqueueOptions = {},
  ) => {
    await driver.add<ScenarioJobData>(
      QUEUE,
      JOB_NAME,
      { label, workspaceId },
      options,
    );
  };

  return {
    metricsService,
    enqueue,
    enqueueMany: async (jobLabels: string[], options?: EnqueueOptions) => {
      for (const label of jobLabels) {
        await enqueue(label, options);
      }
    },
    enqueueBulk: async (jobLabels: string[], workspaceId: string) => {
      await driver.bulkAdd<ScenarioJobData>(
        QUEUE,
        JOB_NAME,
        jobLabels.map((label) => ({ data: { label, workspaceId } })),
      );
    },
    processNext: async (count: number) => {
      const alreadyProcessed = processedLabels.length;

      for (let index = 0; index < count; index += 1) {
        if (!(await processOne())) {
          break;
        }
      }

      return processedLabels.slice(alreadyProcessed);
    },
    processAll: async () => {
      const alreadyProcessed = processedLabels.length;
      let hasProcessedJob = await processOne();

      while (hasProcessedJob) {
        hasProcessedJob = await processOne();
      }

      mockBullMQ.listeners.drained?.();
      await flushAsyncWork();

      return processedLabels.slice(alreadyProcessed);
    },
    setWeight: async (groupKey: string, weight: number) => {
      await redis.hset(`${KEY_PREFIX}:${QUEUE}:fair:weights`, groupKey, weight);
    },
    breakRedis: () => {
      const runCommand = jest
        .fn()
        .mockRejectedValue(new Error('Redis connection lost'));

      mockBullMQ.client = { runCommand };

      return runCommand;
    },
  };
};

type Harness = Awaited<ReturnType<typeof createHarness>>;

describe('BullMQDriver fairness scenarios', () => {
  beforeAll(() => {
    jest.useRealTimers();
    jest.spyOn(Logger.prototype, 'log').mockImplementation(() => undefined);
    jest.spyOn(Logger.prototype, 'warn').mockImplementation(() => undefined);
  });

  describe('S1: A enqueues a burst of 10 jobs, then B enqueues 1 job (group keys deduced from workspaceId)', () => {
    const enqueue = async (harness: Harness) => {
      await harness.enqueueMany(labels('A', 10), { workspaceId: 'A' });
      await harness.enqueue('B1', { workspaceId: 'B' });
    };

    it('flag off: B waits behind the whole burst of A', async () => {
      const harness = await createHarness({ isFairnessEnabled: false });

      await enqueue(harness);

      expect(await harness.processAll()).toEqual([...labels('A', 10), 'B1']);
    });

    it('flag on: B runs right after the first job of A', async () => {
      const harness = await createHarness({ isFairnessEnabled: true });

      await enqueue(harness);

      expect(await harness.processAll()).toEqual([
        'A1',
        'B1',
        ...labels('A', 9, 2),
      ]);
    });
  });

  describe('S2: A and B each enqueue a bulk of 4 jobs', () => {
    const enqueue = async (harness: Harness) => {
      await harness.enqueueBulk(labels('A', 4), 'A');
      await harness.enqueueBulk(labels('B', 4), 'B');
    };

    it('flag off: the bulk of B waits behind the bulk of A', async () => {
      const harness = await createHarness({ isFairnessEnabled: false });

      await enqueue(harness);

      expect(await harness.processAll()).toEqual([
        ...labels('A', 4),
        ...labels('B', 4),
      ]);
    });

    it('flag on: the two bulks alternate, each one keeping its internal order', async () => {
      const harness = await createHarness({ isFairnessEnabled: true });

      await enqueue(harness);

      expect(await harness.processAll()).toEqual([
        'A1',
        'B1',
        'A2',
        'B2',
        'A3',
        'B3',
        'A4',
        'B4',
      ]);
    });
  });

  describe('S3: A enqueues 5 default jobs, B enqueues a route dispatch (tier 0) and an app enqueueJobs job (tier 2)', () => {
    const enqueue = async (harness: Harness) => {
      await harness.enqueueMany(labels('A', 5), { workspaceId: 'A' });
      await harness.enqueue('R_B', {
        workspaceId: 'B',
        priority: SERVER_ROUTE_DISPATCH_JOB_PRIORITY,
      });
      await harness.enqueue('E_B', {
        workspaceId: 'B',
        priority: ENQUEUE_JOB_PRIORITY,
      });
    };
    const expectedOrder = ['R_B', ...labels('A', 5), 'E_B'];

    it('flag off: the tiers decide, the route dispatch first and the app job last', async () => {
      const harness = await createHarness({ isFairnessEnabled: false });

      await enqueue(harness);

      expect(await harness.processAll()).toEqual(expectedOrder);
    });

    it('flag on: same order, fairness only reorders the jobs of a tier', async () => {
      const harness = await createHarness({ isFairnessEnabled: true });

      await enqueue(harness);

      expect(await harness.processAll()).toEqual(expectedOrder);
    });
  });

  describe('S4: W1, W2 and W3 each enqueue 2 jobs of application H, then W4 enqueues 1 job of application X', () => {
    const enqueue = async (
      harness: Harness,
      { withApplicationKey }: { withApplicationKey: boolean },
    ) => {
      for (const workspaceId of ['W1', 'W2', 'W3']) {
        for (const suffix of ['a', 'b']) {
          await harness.enqueue(`${workspaceId}${suffix}`, {
            workspaceId,
            groupKeys: [
              buildWorkspaceGroupKey(workspaceId),
              ...(withApplicationKey ? [buildApplicationGroupKey('H')] : []),
            ],
          });
        }
      }
      await harness.enqueue('W4', {
        workspaceId: 'W4',
        groupKeys: [
          buildWorkspaceGroupKey('W4'),
          ...(withApplicationKey ? [buildApplicationGroupKey('X')] : []),
        ],
      });
    };

    it('flag off: W4 waits behind the whole herd', async () => {
      const harness = await createHarness({ isFairnessEnabled: false });

      await enqueue(harness, { withApplicationKey: true });

      expect(await harness.processAll()).toEqual([
        'W1a',
        'W1b',
        'W2a',
        'W2b',
        'W3a',
        'W3b',
        'W4',
      ]);
    });

    it('flag on: the application key makes the herd share one clock, W4 runs second', async () => {
      const harness = await createHarness({ isFairnessEnabled: true });

      await enqueue(harness, { withApplicationKey: true });

      expect(await harness.processAll()).toEqual([
        'W1a',
        'W4',
        'W1b',
        'W2a',
        'W2b',
        'W3a',
        'W3b',
      ]);
    });

    it('flag on, workspace key only: each workspace of the herd gets a slot before W4, which runs fourth', async () => {
      const harness = await createHarness({ isFairnessEnabled: true });

      await enqueue(harness, { withApplicationKey: false });

      expect(await harness.processAll()).toEqual([
        'W1a',
        'W2a',
        'W3a',
        'W4',
        'W1b',
        'W2b',
        'W3b',
      ]);
    });
  });

  describe('S5: C keeps the queue busy with 12 jobs, A gets a burst of 6 processed, then B bursts 5 jobs and A enqueues 1 job', () => {
    const enqueueFirstPhase = async (harness: Harness) => {
      await harness.enqueueMany(labels('C', 12), { workspaceId: 'C' });
      await harness.enqueueMany(labels('A', 6), { workspaceId: 'A' });
    };
    const enqueueSecondPhase = async (harness: Harness) => {
      await harness.enqueueMany(labels('B', 5), { workspaceId: 'B' });
      await harness.enqueue('A7', { workspaceId: 'A' });
    };

    it('flag off: A7 runs last, behind the rest of C and the whole burst of B', async () => {
      const harness = await createHarness({ isFairnessEnabled: false });

      await enqueueFirstPhase(harness);

      expect(await harness.processNext(12)).toEqual(labels('C', 12));

      await enqueueSecondPhase(harness);

      expect(await harness.processAll()).toEqual([
        ...labels('A', 6),
        ...labels('B', 5),
        'A7',
      ]);
    });

    it('flag on: the processed burst of A is forgotten, A7 interleaves with the burst of B', async () => {
      const harness = await createHarness({ isFairnessEnabled: true });

      await enqueueFirstPhase(harness);

      expect(await harness.processNext(12)).toEqual([
        'C1',
        'A1',
        'C2',
        'A2',
        'C3',
        'A3',
        'C4',
        'A4',
        'C5',
        'A5',
        'C6',
        'A6',
      ]);

      await enqueueSecondPhase(harness);

      expect(await harness.processAll()).toEqual([
        'B1',
        'C7',
        'B2',
        'A7',
        'C8',
        'B3',
        'C9',
        'B4',
        'C10',
        'B5',
        'C11',
        'C12',
      ]);
    });
  });

  describe('S6: A enqueues 5 jobs that get fully processed and the queue drains, then B enqueues 2 jobs and A enqueues 2 jobs', () => {
    const drainFirstBurst = async (harness: Harness) => {
      await harness.enqueueMany(labels('A', 5), { workspaceId: 'A' });

      expect(await harness.processAll()).toEqual(labels('A', 5));
    };
    const enqueueSecondPhase = async (harness: Harness) => {
      await harness.enqueueMany(labels('B', 2), { workspaceId: 'B' });
      await harness.enqueueMany(labels('A', 2, 6), { workspaceId: 'A' });
    };

    it('flag off: B first, then A, in arrival order', async () => {
      const harness = await createHarness({ isFairnessEnabled: false });

      await drainFirstBurst(harness);
      await enqueueSecondPhase(harness);

      expect(await harness.processAll()).toEqual(['B1', 'B2', 'A6', 'A7']);
    });

    it('flag on: the drain reset the clocks, A is not held back by its earlier burst', async () => {
      const harness = await createHarness({ isFairnessEnabled: true });

      await drainFirstBurst(harness);
      await enqueueSecondPhase(harness);

      expect(await harness.processAll()).toEqual(['B1', 'A6', 'B2', 'A7']);
    });
  });

  describe('S7: application P weighs 2, application Q weighs 1, each enqueues 4 jobs keyed by application only', () => {
    const enqueue = async (harness: Harness) => {
      await harness.setWeight(buildApplicationGroupKey('P'), 2);
      await harness.enqueueMany(labels('P', 4), {
        workspaceId: 'WP',
        groupKeys: [buildApplicationGroupKey('P')],
      });
      await harness.enqueueMany(labels('Q', 4), {
        workspaceId: 'WQ',
        groupKeys: [buildApplicationGroupKey('Q')],
      });
    };

    it('flag off: the weight is ignored, Q waits behind P', async () => {
      const harness = await createHarness({ isFairnessEnabled: false });

      await enqueue(harness);

      expect(await harness.processAll()).toEqual([
        ...labels('P', 4),
        ...labels('Q', 4),
      ]);
    });

    it('flag on: P gets two slots for each slot of Q', async () => {
      const harness = await createHarness({ isFairnessEnabled: true });

      await enqueue(harness);

      expect(await harness.processAll()).toEqual([
        'P1',
        'P2',
        'Q1',
        'P3',
        'P4',
        'Q2',
        'Q3',
        'Q4',
      ]);
    });
  });

  describe('S8: Redis is unavailable, A enqueues 3 jobs and B enqueues 1 job', () => {
    const enqueue = async (harness: Harness) => {
      await harness.enqueueMany(labels('A', 3), { workspaceId: 'A' });
      await harness.enqueue('B1', { workspaceId: 'B' });
    };
    const expectedOrder = [...labels('A', 3), 'B1'];

    it('flag off: Redis is never called and the order is FIFO', async () => {
      const harness = await createHarness({ isFairnessEnabled: false });
      const runCommand = harness.breakRedis();

      await enqueue(harness);

      expect(await harness.processAll()).toEqual(expectedOrder);
      expect(runCommand).not.toHaveBeenCalled();
      expect(
        harness.metricsService.incrementCounterForEvent,
      ).not.toHaveBeenCalledWith(
        expect.objectContaining({ key: MetricsKeys.JobFairTagFallback }),
      );
    });

    it('flag on: every job falls back to FIFO and the fallback is counted', async () => {
      const harness = await createHarness({ isFairnessEnabled: true });

      harness.breakRedis();
      await enqueue(harness);

      expect(await harness.processAll()).toEqual(expectedOrder);
      expect(
        harness.metricsService.incrementCounterForEvent.mock.calls.filter(
          ([event]) => event.key === MetricsKeys.JobFairTagFallback,
        ),
      ).toHaveLength(4);
    });
  });

  describe('S9: 3 jobs without workspaceId, then 1 job of workspace W', () => {
    const enqueue = async (harness: Harness) => {
      await harness.enqueueMany(labels('S', 3));
      await harness.enqueue('W1', { workspaceId: 'W' });
    };

    it('flag off: the workspace job waits behind the system jobs', async () => {
      const harness = await createHarness({ isFairnessEnabled: false });

      await enqueue(harness);

      expect(await harness.processAll()).toEqual([...labels('S', 3), 'W1']);
    });

    it('flag on: the system jobs form one group, the workspace job runs second', async () => {
      const harness = await createHarness({ isFairnessEnabled: true });

      await enqueue(harness);

      expect(await harness.processAll()).toEqual(['S1', 'W1', 'S2', 'S3']);
    });
  });
});
