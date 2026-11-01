/**
 * Express router exposing analytics reporting, Kafka metrics, and real-time SSE streaming endpoints.
 * Serves metric summaries, timeline data, endpoint hit counts, circuit breaker state, Kafka event stats, and live log streams.
 * Also manages a Redis Pub/Sub subscriber that forwards live Kafka events to all connected SSE clients.
 * Exports Express router instance.
 */

'use strict';

const express   = require('express');
const fs        = require('fs');
const path      = require('path');
const Redis     = require('ioredis');
const { getRedisClient } = require('../../../../shared/config/redisClient');
const { createServiceLogger } = require('../../../../shared/utils/logger');
const { getRegistry } = require('../config/serviceRegistry');
const { requestEventBus, getRecentRequests } = require('../middleware/analyticsCollector');
const { getCircuitBreakerSnapshot } = require('./gatewayRoutes');
const { isConnected, producerEvents } = require('../../../../shared/kafka/producer');

// ── Redis Pub/Sub subscriber for live Kafka events ────────────────────────────
// The analytics-consumer publishes to this channel after consuming every Kafka
// message. The gateway picks them up here and fans them out to SSE clients.
const LIVE_EVENTS_CHANNEL = 'kafka:live_events';
const REDIS_HOST  = process.env.REDIS_HOST  || 'localhost';
const REDIS_PORT  = parseInt(process.env.REDIS_PORT  || '6379', 10);

// Set of per-client SSE send functions so we can broadcast to everyone
const kafkaSseClients = new Set();

let redisSubscriber = null;

function buildRedisSubscriber() {
  const client = new Redis({
    host:     REDIS_HOST,
    port:     REDIS_PORT,
    password: process.env.REDIS_PASSWORD || undefined,
    lazyConnect:          true,
    maxRetriesPerRequest: null,            // keep retrying forever
    retryStrategy: (times) => Math.min(times * 300, 5000),
    enableOfflineQueue: true,
  });

  client.on('error', (err) => {
    logger.warn('[SSE/Redis] Subscriber error — will auto-reconnect', { error: err.message });
  });

  client.on('ready', () => {
    logger.info('[SSE/Redis] Redis subscriber connected', {
      host:    REDIS_HOST,
      port:    REDIS_PORT,
      channel: LIVE_EVENTS_CHANNEL,
    });
  });

  return client;
}

async function initRedisSubscriber() {
  redisSubscriber = buildRedisSubscriber();
  try {
    await redisSubscriber.connect();
    await redisSubscriber.subscribe(LIVE_EVENTS_CHANNEL);
    logger.info('[SSE/Redis] Subscribed to live events channel', { channel: LIVE_EVENTS_CHANNEL });

    redisSubscriber.on('message', (channel, rawMessage) => {
      if (channel !== LIVE_EVENTS_CHANNEL) return;
      let payload;
      try {
        payload = JSON.parse(rawMessage);
      } catch {
        logger.warn('[SSE/Redis] Failed to parse live event message', { rawMessage });
        return;
      }

      logger.info('[SSE/Redis] Broadcasting kafka_event to SSE clients', {
        topic:      payload.topic,
        eventType:  payload.eventType,
        clients:    kafkaSseClients.size,
      });

      for (const sendFn of kafkaSseClients) {
        try {
          sendFn('kafka_event', payload);
        } catch (err) {
          logger.warn('[SSE/Redis] Failed to send to SSE client', { error: err.message });
        }
      }
    });
  } catch (err) {
    logger.warn('[SSE/Redis] Could not connect Redis subscriber — live Kafka feed disabled', {
      error: err.message,
    });
  }
}

// Kick off subscriber immediately (non-blocking)
// NOTE: initRedisSubscriber() is called below, AFTER logger is initialized.

const logger = createServiceLogger('gateway-analytics-api');
const router = express.Router();

const SSE_MAX_CLIENTS      = parseInt(process.env.SSE_MAX_CLIENTS      || '20',    10);
const SSE_HEARTBEAT_MS     = parseInt(process.env.SSE_HEARTBEAT_MS     || '15000', 10);
let   sseClientCount       = 0;

const ALLOWED_ORIGIN = process.env.DASHBOARD_ORIGIN || 'http://localhost:5173';

// Kick off the Redis Pub/Sub subscriber now that logger is ready.
initRedisSubscriber();


router.use((req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', ALLOWED_ORIGIN);
  res.setHeader('Access-Control-Allow-Methods', 'GET, DELETE, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  res.setHeader('Access-Control-Allow-Credentials', 'true');
  if (req.method === 'OPTIONS') {
    return res.sendStatus(204);
  }
  return next();
});

async function safeGet(redis, key) {
  try {
    const val = await redis.get(key);
    return val ? parseInt(val, 10) : 0;
  } catch {
    return 0;
  }
}

async function safeMget(redis, keys) {
  if (!keys.length) return [];
  try {
    const vals = await redis.mget(...keys);
    return vals.map((v) => (v ? parseInt(v, 10) : 0));
  } catch {
    return keys.map(() => 0);
  }
}

async function scanKeys(redis, pattern) {
  const keys = [];
  let cursor = '0';
  do {
    const [newCursor, batch] = await redis.scan(cursor, 'MATCH', pattern, 'COUNT', 100);
    cursor = newCursor;
    keys.push(...batch);
  } while (cursor !== '0');
  return keys;
}

router.get('/summary', async (req, res) => {
  let redis;
  try {
    redis = getRedisClient();
  } catch (err) {
    logger.error('[AnalyticsAPI] Redis unavailable', { error: err.message });
    return res.status(503).json({
      success: false,
      error: { code: 'REDIS_UNAVAILABLE', message: 'Analytics store is unreachable.' },
    });
  }

  try {
    const [totalRequests, failedRequests, latencyTotalMs, latencyCount] = await safeMget(
      redis,
      [
        'analytics:total_requests',
        'analytics:failed_requests',
        'analytics:latency:total_ms',
        'analytics:latency:count',
      ]
    );

    const successRate       = totalRequests > 0
      ? (((totalRequests - failedRequests) / totalRequests) * 100).toFixed(2)
      : '100.00';
    const avgResponseTimeMs = latencyCount > 0
      ? Math.round(latencyTotalMs / latencyCount)
      : 0;

    const [s2xx, s3xx, s4xx, s5xx] = await safeMget(redis, [
      'analytics:status:2xx',
      'analytics:status:3xx',
      'analytics:status:4xx',
      'analytics:status:5xx',
    ]);

    const statusCodeBreakdown = { '2xx': s2xx, '3xx': s3xx, '4xx': s4xx, '5xx': s5xx };

    const registry = getRegistry();
    const serviceNames = [...registry.map((s) => s.name), 'gateway'];
    const serviceKeys  = serviceNames.map((n) => `analytics:service:${n}`);
    const serviceCounts = await safeMget(redis, serviceKeys);

    const perServiceBreakdown = {};
    serviceNames.forEach((name, i) => {
      perServiceBreakdown[name] = serviceCounts[i];
    });

    const gatewayKeys = await scanKeys(redis, 'analytics:gateway:*');
    const gatewayCounts = await safeMget(redis, gatewayKeys);
    const perGatewayBreakdown = {};
    gatewayKeys.forEach((key, i) => {
      const instanceId = key.replace('analytics:gateway:', '');
      perGatewayBreakdown[instanceId] = gatewayCounts[i];
    });

    return res.json({
      success: true,
      data: {
        total_requests:       totalRequests,
        failed_requests:      failedRequests,
        success_rate:         `${successRate}%`,
        avg_response_time_ms: avgResponseTimeMs,
        status_code_breakdown: statusCodeBreakdown,
        per_service_breakdown: perServiceBreakdown,
        per_gateway_breakdown: perGatewayBreakdown,
        collected_at:         new Date().toISOString(),
      },
    });
  } catch (err) {
    logger.error('[AnalyticsAPI] Error building summary', { error: err.message });
    return res.status(500).json({
      success: false,
      error: { code: 'ANALYTICS_ERROR', message: 'Failed to retrieve analytics summary.' },
    });
  }
});

router.get('/timeline', async (req, res) => {
  let redis;
  try {
    redis = getRedisClient();
  } catch (err) {
    return res.status(503).json({
      success: false,
      error: { code: 'REDIS_UNAVAILABLE', message: 'Analytics store is unreachable.' },
    });
  }

  const dateParam = req.query.date;
  let date;
  if (dateParam && /^\d{4}-\d{2}-\d{2}$/.test(dateParam)) {
    date = dateParam;
  } else {
    const now = new Date();
    date = `${now.getUTCFullYear()}-${String(now.getUTCMonth() + 1).padStart(2, '0')}-${String(now.getUTCDate()).padStart(2, '0')}`;
  }

  try {
    const timelineKey = `analytics:timeline:${date}`;
    const raw = await redis.hgetall(timelineKey);

    const timeline = [];
    if (raw) {
      const sortedMinutes = Object.keys(raw).sort();
      sortedMinutes.forEach((minute) => {
        timeline.push({ minute, requests: parseInt(raw[minute], 10) });
      });
    }

    return res.json({
      success: true,
      data: {
        date,
        total_requests: timeline.reduce((sum, t) => sum + t.requests, 0),
        timeline,
      },
    });
  } catch (err) {
    logger.error('[AnalyticsAPI] Error fetching timeline', { error: err.message });
    return res.status(500).json({
      success: false,
      error: { code: 'ANALYTICS_ERROR', message: 'Failed to retrieve timeline data.' },
    });
  }
});

router.get('/endpoints', async (req, res) => {
  let redis;
  try {
    redis = getRedisClient();
  } catch (err) {
    return res.status(503).json({
      success: false,
      error: { code: 'REDIS_UNAVAILABLE', message: 'Analytics store is unreachable.' },
    });
  }

  const limit = Math.min(parseInt(req.query.limit || '20', 10), 100);

  try {
    const endpointKeys = await scanKeys(redis, 'analytics:endpoint:*');

    if (!endpointKeys.length) {
      return res.json({ success: true, data: { endpoints: [] } });
    }

    const counts = await safeMget(redis, endpointKeys);

    const paired = endpointKeys.map((key, i) => {
      const withoutPrefix = key.replace('analytics:endpoint:', '');
      const colonIdx      = withoutPrefix.indexOf(':');
      const method        = withoutPrefix.substring(0, colonIdx);
      const path          = withoutPrefix.substring(colonIdx + 1);
      return { method, path, requests: counts[i] };
    });

    paired.sort((a, b) => b.requests - a.requests);
    const top = paired.slice(0, limit);

    return res.json({
      success: true,
      data: {
        limit,
        total_unique_endpoints: endpointKeys.length,
        endpoints: top,
      },
    });
  } catch (err) {
    logger.error('[AnalyticsAPI] Error fetching endpoints', { error: err.message });
    return res.status(500).json({
      success: false,
      error: { code: 'ANALYTICS_ERROR', message: 'Failed to retrieve endpoint data.' },
    });
  }
});

router.delete('/reset', async (req, res) => {
  let redis;
  try {
    redis = getRedisClient();
  } catch (err) {
    return res.status(503).json({
      success: false,
      error: { code: 'REDIS_UNAVAILABLE', message: 'Analytics store is unreachable.' },
    });
  }

  if (process.env.NODE_ENV === 'production' && process.env.ALLOW_ANALYTICS_RESET !== 'true') {
    return res.status(403).json({
      success: false,
      error: {
        code: 'FORBIDDEN',
        message: 'Analytics reset is disabled in production. Set ALLOW_ANALYTICS_RESET=true to override.',
      },
    });
  }

  try {
    const keys = await scanKeys(redis, 'analytics:*');
    if (keys.length > 0) {
      await redis.del(...keys);
    }

    logger.warn(`[AnalyticsAPI] Analytics reset: ${keys.length} keys deleted`, {
      correlationId: req.correlationId,
    });

    return res.json({
      success: true,
      data: {
        message: `Analytics reset complete. ${keys.length} keys deleted.`,
        deleted_count: keys.length,
        reset_at: new Date().toISOString(),
      },
    });
  } catch (err) {
    logger.error('[AnalyticsAPI] Error during reset', { error: err.message });
    return res.status(500).json({
      success: false,
      error: { code: 'ANALYTICS_ERROR', message: 'Failed to reset analytics.' },
    });
  }
});

router.get('/circuit-breakers', (req, res) => {
  try {
    const snapshot = getCircuitBreakerSnapshot();
    return res.json({
      success: true,
      data: snapshot,
      collected_at: new Date().toISOString(),
    });
  } catch (err) {
    logger.error('[AnalyticsAPI] Error fetching circuit breaker snapshot', { error: err.message });
    return res.status(500).json({
      success: false,
      error: { code: 'CB_ERROR', message: 'Failed to retrieve circuit breaker states.' },
    });
  }
});

router.get('/requests/live', (req, res) => {
  const limit  = Math.min(parseInt(req.query.limit || '50', 10), 500);
  const recent = getRecentRequests();
  const sliced = recent.slice(-limit).reverse();

  return res.json({
    success: true,
    data: {
      requests: sliced,
      total:    recent.length,
      limit,
    },
  });
});

router.get('/stream', (req, res) => {
  if (sseClientCount >= SSE_MAX_CLIENTS) {
    logger.warn('[SSE] Max client limit reached, rejecting new SSE connection', {
      count: sseClientCount,
      max:   SSE_MAX_CLIENTS,
    });
    return res.status(503).json({
      success: false,
      error: { code: 'SSE_CAPACITY', message: 'Too many active SSE connections. Try again later.' },
    });
  }

  res.setHeader('Content-Type',                'text/event-stream');
  res.setHeader('Cache-Control',               'no-cache, no-transform');
  res.setHeader('Connection',                  'keep-alive');
  res.setHeader('X-Accel-Buffering',           'no');
  res.setHeader('Access-Control-Allow-Origin', ALLOWED_ORIGIN);
  res.flushHeaders();

  sseClientCount++;
  const clientId = `sse-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
  logger.info(`[SSE] Client connected: ${clientId} (total: ${sseClientCount})`);

  const sendEvent = (eventName, data) => {
    if (res.writableEnded) return;
    try {
      res.write(`event: ${eventName}\n`);
      res.write(`data: ${JSON.stringify(data)}\n\n`);
    } catch (err) {
      logger.warn(`[SSE] Write error for ${clientId}: ${err.message}`);
    }
  };

  // Send initial connected event
  sendEvent('connected', { clientId, timestamp: new Date().toISOString() });

  // ── In-process event listeners (gateway-local events) ─────────────────────
  const onRequest = (record) => sendEvent('request', record);
  const onCB      = (cbData)  => sendEvent('circuit_breaker', cbData);

  // producerEvents fires ONLY for events published by THIS gateway process.
  // Cross-service kafka events come via the Redis Pub/Sub path below.
  const onLocalKafkaEvent = (data) => sendEvent('kafka_event', data);

  requestEventBus.on('request',         onRequest);
  requestEventBus.on('circuit_breaker', onCB);
  producerEvents.on('kafka_event',      onLocalKafkaEvent);

  // ── Redis Pub/Sub kafka_event listener (cross-process events) ────────────
  // Register this client's sendEvent function so the Redis subscriber
  // can fan out incoming live events to this connection.
  kafkaSseClients.add(sendEvent);
  logger.info(`[SSE] Client ${clientId} registered for Redis Pub/Sub kafka events (channel: ${LIVE_EVENTS_CHANNEL})`);

  // ── Heartbeat ──────────────────────────────────────────────────────────────
  const heartbeatTimer = setInterval(() => {
    sendEvent('heartbeat', {
      ts:             Date.now(),
      kafkaConnected: isConnected(),
      liveClients:    kafkaSseClients.size,
    });
  }, SSE_HEARTBEAT_MS);

  // ── Cleanup on disconnect ──────────────────────────────────────────────────
  const cleanup = () => {
    clearInterval(heartbeatTimer);
    requestEventBus.off('request',         onRequest);
    requestEventBus.off('circuit_breaker', onCB);
    producerEvents.off('kafka_event',      onLocalKafkaEvent);
    kafkaSseClients.delete(sendEvent);           // deregister from Redis fan-out
    sseClientCount = Math.max(0, sseClientCount - 1);
    logger.info(`[SSE] Client disconnected: ${clientId} (total: ${sseClientCount}, redis clients: ${kafkaSseClients.size})`);
  };

  req.on('close',   cleanup);
  req.on('aborted', cleanup);
});

router.get('/kafka', async (req, res) => {
  let redis;
  try {
    redis = getRedisClient();
  } catch (err) {
    return res.json({
      success: true,
      data: {
        connected:         isConnected(),
        published_total:   0,
        consumed_total:    0,
        events_per_sec:    0,
        topics:            {},
        events:            {},
        consumer_lag:      {},
        collected_at:      new Date().toISOString(),
      },
    });
  }

  try {
    const consumedTotal    = parseInt((await redis.get('kafka:consumed:total'))    || '0', 10);

    const topicKeys = await (async () => {
      const keys = [];
      let cursor = '0';
      do {
        const [cur, batch] = await redis.scan(cursor, 'MATCH', 'kafka:consumed:topic:*', 'COUNT', 50);
        cursor = cur;
        keys.push(...batch);
      } while (cursor !== '0');
      return keys;
    })();

    const eventKeys = await (async () => {
      const keys = [];
      let cursor = '0';
      do {
        const [cur, batch] = await redis.scan(cursor, 'MATCH', 'kafka:consumed:event:*', 'COUNT', 50);
        cursor = cur;
        keys.push(...batch);
      } while (cursor !== '0');
      return keys;
    })();

    const topicBreakdown = {};
    if (topicKeys.length > 0) {
      const vals = await redis.mget(...topicKeys);
      topicKeys.forEach((k, i) => {
        const topicName = k.replace('kafka:consumed:topic:', '');
        topicBreakdown[topicName] = parseInt(vals[i] || '0', 10);
      });
    }

    const eventBreakdown = {};
    if (eventKeys.length > 0) {
      const vals = await redis.mget(...eventKeys);
      eventKeys.forEach((k, i) => {
        const evName = k.replace('kafka:consumed:event:', '');
        eventBreakdown[evName] = parseInt(vals[i] || '0', 10);
      });
    }

    const nowSec = Math.floor(Date.now() / 1000);
    const secKeys = Array.from({ length: 5 }, (_, i) => `kafka:events_per_sec:${nowSec - i}`);
    const secVals = await redis.mget(...secKeys);
    const eventsPerSec = secVals.reduce((sum, v) => sum + parseInt(v || '0', 10), 0) / 5;

    return res.json({
      success: true,
      data: {
        connected:        isConnected(),
        consumed_total:   consumedTotal,
        events_per_sec:   Math.round(eventsPerSec * 10) / 10,
        topics:           topicBreakdown,
        events:           eventBreakdown,
        consumer_lag:     {},
        collected_at:     new Date().toISOString(),
      },
    });
  } catch (err) {
    logger.error('[AnalyticsAPI] Error fetching Kafka metrics', { error: err.message });
    return res.status(500).json({
      success: false,
      error: { code: 'ANALYTICS_ERROR', message: 'Failed to retrieve Kafka metrics.' },
    });
  }
});

router.get('/kafka/live', async (req, res) => {
  let redis;
  try {
    redis = getRedisClient();
    const rawEvents = await redis.lrange('kafka:recent_events', 0, 29);
    const events = (rawEvents || []).map((r) => {
      try { return JSON.parse(r); } catch { return null; }
    }).filter(Boolean);

    return res.json({
      success: true,
      data: { events },
    });
  } catch (err) {
    return res.json({
      success: true,
      data: { events: [] },
    });
  }
});

router.get('/logs', (req, res) => {
  const levelFilter   = req.query.level   || null;
  const serviceFilter = req.query.service || null;

  const logDir = process.env.LOG_DIR
    ? (path.isAbsolute(process.env.LOG_DIR) ? process.env.LOG_DIR : path.resolve(__dirname, '../../../../', process.env.LOG_DIR))
    : path.resolve(__dirname, '../../../../logs');
  const logFile = path.join(logDir, 'gateway-combined.log');

  res.setHeader('Content-Type',                'text/event-stream');
  res.setHeader('Cache-Control',               'no-cache, no-transform');
  res.setHeader('Connection',                  'keep-alive');
  res.setHeader('X-Accel-Buffering',           'no');
  res.setHeader('Access-Control-Allow-Origin', ALLOWED_ORIGIN);
  res.flushHeaders();

  logger.info('[SSE/Logs] Log stream client connected');

  const sendLogEvent = (data) => {
    if (res.writableEnded) return;
    try {
      res.write(`event: log\n`);
      res.write(`data: ${JSON.stringify(data)}\n\n`);
    } catch { }
  };

  sendLogEvent({ type: 'connected', message: 'Log stream connected', timestamp: new Date().toISOString() });

  if (!fs.existsSync(logFile)) {
    sendLogEvent({ type: 'warning', message: 'Log file does not exist yet. Start sending requests to generate logs.', timestamp: new Date().toISOString() });
  }

  let filePosition = 0;
  try {
    const stat = fs.statSync(logFile);
    filePosition = stat.size;
  } catch { }

  const readNewLines = () => {
    try {
      const stat = fs.statSync(logFile);
      if (stat.size < filePosition) {
        filePosition = 0;
      }
      if (stat.size <= filePosition) return;

      const fd     = fs.openSync(logFile, 'r');
      const length = stat.size - filePosition;
      const buffer = Buffer.alloc(length);
      fs.readSync(fd, buffer, 0, length, filePosition);
      fs.closeSync(fd);

      filePosition = stat.size;

      const newContent = buffer.toString('utf8');
      const lines      = newContent.split('\n').filter((l) => l.trim());

      for (const line of lines) {
        try {
          const parsed = JSON.parse(line);

          if (levelFilter && parsed.level !== levelFilter) continue;
          if (serviceFilter && parsed.service !== serviceFilter) continue;

          sendLogEvent({
            type:      'log',
            level:     parsed.level,
            service:   parsed.service,
            message:   parsed.message,
            timestamp: parsed.timestamp,
            meta:      parsed,
          });
        } catch {
        }
      }
    } catch (err) {
    }
  };

  const pollInterval = setInterval(readNewLines, 500);

  const heartbeat = setInterval(() => {
    if (!res.writableEnded) {
      res.write(': heartbeat\n\n');
    }
  }, 15000);

  const cleanup = () => {
    clearInterval(pollInterval);
    clearInterval(heartbeat);
    logger.info('[SSE/Logs] Log stream client disconnected');
  };

  req.on('close',   cleanup);
  req.on('aborted', cleanup);
});

module.exports = router;
