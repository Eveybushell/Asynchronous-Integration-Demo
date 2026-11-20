import IORedis from 'ioredis';

export const QUEUE_NAME = 'order-processing';
export const DLQ_NAME = 'order-processing-dlq';

export const redisConnection = new IORedis({
  host: process.env.REDIS_HOST || '127.0.0.1',
  port: Number(process.env.REDIS_PORT) || 6379,
  maxRetriesPerRequest: null,
});