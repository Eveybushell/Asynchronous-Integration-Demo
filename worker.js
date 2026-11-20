import { Worker, Queue, UnrecoverableError } from 'bullmq';

import {
  QUEUE_NAME,
  DLQ_NAME,
  redisConnection
} from './shared/queue.js';

const deadLetterQueue = new Queue(DLQ_NAME, {
  connection: redisConnection,
});

const sleep = (ms) =>
  new Promise(resolve => setTimeout(resolve, ms));

const worker = new Worker(
  QUEUE_NAME,

  async (job) => {
    const attemptNumber = job.attemptsMade + 1;
    const maxAttempts = job.opts.attempts || 1;
    const idempotencyKey = `processed:${job.id}`;

    try {
      // 0. Idempotency Guard: Short-circuit if already processed
      const cachedResult = await redisConnection.get(idempotencyKey);
      if (cachedResult) {
        console.log(`[Worker] Skipping duplicate delivery for ${job.id}`);
        return JSON.parse(cachedResult);
      }

      // 1. Validate the message
      if (
        !job.data.taskName ||
        typeof job.data.taskName !== 'string'
      ) {
        throw new UnrecoverableError(
          'Invalid payload: taskName is required'
        );
      }

      // 2. Simulate processing
      await sleep(3000);

      // 3. Simulate transient failure
      if (
        job.data.simulateTransientFailure &&
        attemptNumber === 1
      ) {
        throw new Error('Temporary downstream timeout');
      }

      const result = {
        success: true,
        processedBy: `worker-${process.pid}`,
        completedAt: new Date().toISOString(),
      };

      // 4. Record completion ONLY after all fallible work succeeds (24h TTL)
      await redisConnection.set(
        idempotencyKey,
        JSON.stringify(result),
        'EX',
        86400,
        'NX'
      );

      // 5. Successful processing = ACK
      return result;

    } catch (error) {

      const permanentFailure =
        error instanceof UnrecoverableError;

      const retriesExhausted =
        attemptNumber >= maxAttempts;

      if (permanentFailure || retriesExhausted) {

        // Deterministic ID prevents duplicate DLQ entries
        await deadLetterQueue.add(
          'dead-letter-message',
          {
            originalJobId: job.id,
            originalData: job.data,
            error: error.message,
            attemptsMade: attemptNumber,
            failedAt: new Date().toISOString(),
          },
          {
            jobId: `dlq-${job.id}`,
          }
        );

        console.error(
          `[DLQ] Job ${job.id} moved to dead-letter queue`
        );

        // Permanent failures must not be retried.
        // Exhausted transient failures are also terminal.
        throw new UnrecoverableError(error.message);
      }

      // Transient failure: BullMQ handles retry/backoff
      throw error;
    }
  },

  {
    connection: redisConnection,
    concurrency: 2,
  }
);

worker.on('completed', (job) => {
  console.log(`[ACK] Job ${job.id} completed`);
});

worker.on('failed', (job, error) => {
  console.error(
    `[FAILED] ${job?.id}: ${error.message}`
  );
});

async function shutdown() {
  await worker.close();
  await deadLetterQueue.close();
  await redisConnection.quit();
  process.exit(0);
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

console.log('[Worker] Listening for jobs...');