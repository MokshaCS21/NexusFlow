/**
 * Analytics Consumer background service for HydraGateway.
 * Consumes Kafka events from all topics and aggregates metrics into Redis.
 * Also publishes live event payloads to the Redis Pub/Sub channel
 * `kafka:live_events` so the gateway SSE endpoint can broadcast them
 * to connected dashboard clients in real-time.
 */

'use strict';

const path = require('path');
require('dotenv').config();
require('dotenv').config({ path: path.resolve(__dirname, '../../../.env') });

const { Kafka, logLevel } = require('kafkajs');
const Redis = require('ioredis');
const { createLogger, format, transports } = require('winston');

const logger = createLogger({
  level: process.env.LOG_LEVEL || 'info',
  format: format.combine(
    format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }),
    format((info) => { info.service = 'analytics-consumer'; return info; })(),
    format.errors({ stack: true }),
    format.json()
  ),
  transports: [new transports.Console()],
});

const BROKERS     = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',').map(b => b.trim());
const CLIENT_ID   = process.env.KAFKA_CLIENT_ID || 'hydragateway';
const ENABLED     = process.env.KAFKA_ENABLED !== 'false';
const REDIS_HOST  = process.env.REDIS_HOST || 'localhost';
const REDIS_PORT  = parseInt(process.env.REDIS_PORT || '6379', 10);

const CONSUME_TOPICS = [
  'order.created',
  'payment.completed',
  'payment.failed',
  'inventory.updated',
  'analytics.event',
];

// Redis Pub/Sub channel the gateway subscribes to for live SSE broadcasts
const LIVE_EVENTS_CHANNEL = 'kafka:live_events';

const GROUP_ID = 'analytics-event-consumer';

let redisMetrics   = null; // used for INCR / EXPIRE counters
let redisPublisher = null; // dedicated publisher (ioredis cannot sub+pub on same connection)
let consumer       = null;

function buildRedis(label) {
  const client = new Redis({
    host:    REDIS_HOST,
    port:    REDIS_PORT,
    password: process.env.REDIS_PASSWORD || undefined,
    lazyConnect: true,
    maxRetriesPerRequest: 3,
    retryStrategy: (times) => Math.min(times * 200, 2000),
  });

  client.on('error', (err) => {
    logger.warn(`[AnalyticsConsumer] Redis error (${label})`, { error: err.message });
  });

  return client;
}

async function incrementMetrics(topic, eventType) {
  if (!redisMetrics) return;

  try {
    const pipe   = redisMetrics.pipeline();
    pipe.incr('kafka:consumed:total');
    pipe.incr(`kafka:consumed:topic:${topic}`);
    if (eventType) {
      pipe.incr(`kafka:consumed:event:${eventType}`);
    }

    const now    = Date.now();
    const bucket = Math.floor(now / 1000);
    pipe.incr(`kafka:events_per_sec:${bucket}`);
    pipe.expire(`kafka:events_per_sec:${bucket}`, 120);

    await pipe.exec();
  } catch (err) {
    logger.warn('[AnalyticsConsumer] Redis metrics write error', { error: err.message });
  }
}

/**
 * Publish a live event payload to the Redis Pub/Sub channel so that
 * the gateway's SSE subscribers can fan-out to connected browsers.
 */
async function publishLiveEvent(topic, eventType, payload, headers) {
  try {
    const livePayload = {
      topic,
      eventType,
      orderId:       payload.orderId       || null,
      productId:     payload.productId     || null,
      userId:        payload.userId        || null,
      correlationId: payload.correlationId || headers.correlationId || null,
      status:        payload.status        || null,
      totalAmount:   payload.totalAmount   || null,
      ts:            Date.now(),
      timestamp:     new Date().toISOString(),
    };

    if (redisPublisher) {
      await redisPublisher.publish(LIVE_EVENTS_CHANNEL, JSON.stringify(livePayload));
    }

    if (redisMetrics) {
      const pipe = redisMetrics.pipeline();
      pipe.lpush('kafka:recent_events', JSON.stringify(livePayload));
      pipe.ltrim('kafka:recent_events', 0, 49);
      await pipe.exec();
    }

    logger.info('[AnalyticsConsumer] Live event published to Redis Pub/Sub and cached in list', {
      channel: LIVE_EVENTS_CHANNEL,
      topic,
      eventType,
    });
  } catch (err) {
    logger.warn('[AnalyticsConsumer] Redis Pub/Sub publish error', { error: err.message });
  }
}

async function start() {
  if (!ENABLED) {
    logger.info('[AnalyticsConsumer] Kafka is disabled — exiting');
    return;
  }

  // ── Redis: metrics counter connection ──────────────────────────────────────
  redisMetrics = buildRedis('metrics');
  try {
    await redisMetrics.connect();
    logger.info('[AnalyticsConsumer] Redis (metrics) connected', { host: REDIS_HOST, port: REDIS_PORT });
  } catch (err) {
    logger.warn('[AnalyticsConsumer] Redis (metrics) unavailable — counters will be dropped', { error: err.message });
  }

  // ── Redis: publisher connection for live Pub/Sub ───────────────────────────
  redisPublisher = buildRedis('publisher');
  try {
    await redisPublisher.connect();
    logger.info('[AnalyticsConsumer] Redis (publisher) connected — live events will stream via SSE', {
      host:    REDIS_HOST,
      port:    REDIS_PORT,
      channel: LIVE_EVENTS_CHANNEL,
    });
  } catch (err) {
    logger.warn('[AnalyticsConsumer] Redis (publisher) unavailable — SSE live feed will be silent', { error: err.message });
  }

  // ── Kafka Consumer ─────────────────────────────────────────────────────────
  const kafka = new Kafka({
    clientId: `${CLIENT_ID}-analytics-consumer`,
    brokers:  BROKERS,
    logLevel: logLevel.WARN,
    retry: { initialRetryTime: 300, retries: 8 },
  });

  consumer = kafka.consumer({
    groupId:           GROUP_ID,
    sessionTimeout:    30000,
    heartbeatInterval: 3000,
  });

  try {
    await consumer.connect();
    logger.info('[AnalyticsConsumer] Kafka connected', { brokers: BROKERS, groupId: GROUP_ID });

    await consumer.subscribe({ topics: CONSUME_TOPICS, fromBeginning: false });
    logger.info('[AnalyticsConsumer] Subscribed to topics', { topics: CONSUME_TOPICS });

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const raw = message.value ? message.value.toString() : null;
        if (!raw) return;

        let payload = {};
        try {
          payload = JSON.parse(raw);
        } catch {
          logger.warn('[AnalyticsConsumer] Failed to parse message', { topic });
          return;
        }

        const headers = {};
        if (message.headers) {
          for (const [k, v] of Object.entries(message.headers)) {
            headers[k] = v ? v.toString() : '';
          }
        }

        const eventType = headers.eventType || payload.eventType || topic;

        logger.info('[AnalyticsConsumer] Event consumed', {
          topic,
          partition,
          offset:        message.offset,
          eventType,
          orderId:       payload.orderId       || null,
          userId:        payload.userId        || null,
          correlationId: headers.correlationId || null,
          timestamp:     headers.timestamp     || new Date().toISOString(),
        });

        // 1. Increment Redis counters (for the Kafka metrics page)
        await incrementMetrics(topic, eventType);

        // 2. Push live event to Redis Pub/Sub (for the SSE Live Event Feed)
        await publishLiveEvent(topic, eventType, payload, headers);
      },
    });

    logger.info('[AnalyticsConsumer] Consumer running — ready to process events');
  } catch (err) {
    logger.error('[AnalyticsConsumer] Failed to start Kafka consumer', { error: err.message, stack: err.stack });
  }
}

async function shutdown(signal) {
  logger.info(`[AnalyticsConsumer] ${signal} received – shutting down`);
  try {
    if (consumer)       await consumer.disconnect();
    if (redisPublisher) await redisPublisher.quit();
    if (redisMetrics)   await redisMetrics.quit();
  } catch (err) {
    logger.warn('[AnalyticsConsumer] Shutdown error', { error: err.message });
  }
  process.exit(0);
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT',  () => shutdown('SIGINT'));
process.on('uncaughtException', (err) => {
  logger.error('[AnalyticsConsumer] Uncaught exception', { error: err.message, stack: err.stack });
});
process.on('unhandledRejection', (reason) => {
  logger.error('[AnalyticsConsumer] Unhandled rejection', { reason: String(reason) });
});

start();
