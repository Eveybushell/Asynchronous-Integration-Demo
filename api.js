import express from 'express';
import crypto from 'node:crypto';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { Queue } from 'bullmq';
import { QUEUE_NAME, DLQ_NAME, redisConnection } from './shared/queue.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

const jobQueue = new Queue(QUEUE_NAME, {
  connection: redisConnection,
  defaultJobOptions: {
    attempts: 3, // Max NACK retries before moving to 'failed'
    backoff: {
      type: 'exponential',
      delay: 1500, // 1.5s -> 3s -> 6s
    },
    removeOnComplete: false, // Retain in Redis for status polling
    removeOnFail: false,
  },
});

export const deadLetterQueue = new Queue(DLQ_NAME, {
  connection: redisConnection,
  defaultJobOptions: {
    removeOnComplete: false,
    removeOnFail: false,
  },
});

// Map BullMQ internal states to the public API contract
function mapJobState(state) {
  switch (state) {
    case 'waiting':
    case 'delayed':
    case 'prioritized':
      return 'queued';
    case 'active':
      return 'processing';
    case 'completed':
      return 'completed';
    case 'failed':
      return 'failed';
    default:
      return state;
  }
}

// POST /api/jobs — Synchronous ingestion, asynchronous handoff
app.post('/api/jobs', async (req, res) => {
  const { taskName, payload, simulateTransientFailure = false } = req.body;

  if (!taskName || typeof taskName !== 'string' || !taskName.trim()) {
    return res.status(400).json({ error: 'taskName is required and must be a non-empty string.' });
  }

  const jobId = `job_${crypto.randomUUID()}`;

  try {
    await jobQueue.add(
      'process-task',
      {
        taskName: taskName.trim(),
        payload: payload ?? {},
        simulateTransientFailure: Boolean(simulateTransientFailure),
        submittedAt: new Date().toISOString(),
      },
      { jobId }
    );
    console.log(`[API] Published ${jobId} ("${taskName.trim()}")`);

    // Immediately acknowledge ingestion without waiting for execution
    return res.status(202).json({
      jobId,
      status: 'queued',
      statusUrl: `/api/jobs/${jobId}`,
    });
  } catch (err) {
    console.error('[API] Failed to enqueue job:', err);
    return res.status(503).json({ error: 'Message broker unavailable.' });
  }
});

// GET /api/jobs/:id — Status store lookup
app.get('/api/jobs/:id', async (req, res) => {
  try {
    const job = await jobQueue.getJob(req.params.id);

    if (!job) {
      return res.status(404).json({ error: 'Job not found.' });
    }

    const rawState = await job.getState();

    return res.json({
      jobId: job.id,
      status: mapJobState(rawState),
      attemptsMade: job.attemptsMade,
      result: job.returnvalue ?? null,
      failedReason: job.failedReason ?? null,
      createdAt: new Date(job.timestamp).toISOString(),
      finishedAt: job.finishedOn ? new Date(job.finishedOn).toISOString() : null,
    });
  } catch (err) {
    console.error('[API] Failed to fetch job status:', err);
    return res.status(500).json({ error: 'Failed to retrieve job status.' });
  }
});

app.listen(PORT, () => {
  console.log(`[API] Ingress Producer running at http://localhost:${PORT}`);
});