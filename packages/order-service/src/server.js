/**
 * Express application server entry point for Order Service.
 * Sets up middleware, MongoDB database connection, order routes, Kafka producer/consumer lifecycle, and graceful shutdown handlers.
 * Launches HTTP server on configured port.
 */

const path = require('path');
require('dotenv').config();
require('dotenv').config({ path: path.resolve(__dirname, '../../../.env') });
const express = require('express');
const { connectDB } = require('../../../shared/config/dbConnect');
const { correlationId } = require('../../../shared/middleware/correlationId');
const { createServiceLogger } = require('../../../shared/utils/logger');
const orderRoutes = require('./routes/orderRoutes');
const { errorHandler } = require('./middleware/errorHandler');
const { createRequestLogger } = require('../../../shared/middleware/requestLogger');
const producer = require('../../../shared/kafka/producer');
const paymentResultConsumer = require('./consumers/paymentResultConsumer');

const logger = createServiceLogger('order-service');
const app = express();

app.use(express.json({ limit: '10kb' }));
app.use(express.urlencoded({ extended: false }));
app.use(correlationId);
app.use(createRequestLogger(logger));

app.get('/health', (_req, res) => {
  res.json({
    status: 'ok',
    service: 'order-service',
    uptime: process.uptime(),
    timestamp: new Date().toISOString(),
  });
});

app.use('/v1/orders', orderRoutes);

app.use((_req, res) => {
  res.status(404).json({
    success: false,
    error: { code: 'NOT_FOUND', message: 'Route not found' },
  });
});

app.use(errorHandler);

const PORT = parseInt(process.env.ORDER_PORT || '4004', 10);

async function start() {
  logger.info('[OrderService] Starting up...', {
    port:    PORT,
    mongoUri: process.env.MONGO_URI ? process.env.MONGO_URI.replace(/:\/\/[^@]*@/, '://***@') : 'NOT SET',
    kafka:   process.env.KAFKA_BROKERS || 'localhost:9092',
    env:     process.env.NODE_ENV || 'development',
  });

  // ── Connect MongoDB ────────────────────────────────────────────────────────
  try {
    await connectDB();
    logger.info('[OrderService] ✅ MongoDB connected successfully');
  } catch (err) {
    logger.error('[OrderService] ❌ MongoDB connection failed — exiting', {
      error: err.message,
    });
    process.exit(1);
  }

  // ── Connect Kafka Producer ─────────────────────────────────────────────────
  try {
    await producer.connect();
    logger.info('[OrderService] ✅ Kafka producer connected');
  } catch (err) {
    logger.warn('[OrderService] ⚠️ Kafka producer connection failed — events will be dropped', {
      error: err.message,
    });
    // Non-fatal: service still handles orders, just no Kafka events
  }

  // ── Start Payment Result Consumer ─────────────────────────────────────────
  try {
    await paymentResultConsumer.start();
    logger.info('[OrderService] ✅ Payment result Kafka consumer started');
  } catch (err) {
    logger.warn('[OrderService] ⚠️ Payment result consumer failed to start', {
      error: err.message,
    });
  }

  const server = app.listen(PORT, () => {
    logger.info(`[OrderService] ✅ Order Service listening on port ${PORT}`);
  });

  const shutdown = (signal) => {
    logger.info(`[OrderService] ${signal} received – shutting down gracefully`);
    server.close(async () => {
      try { await paymentResultConsumer.disconnect(); } catch { }
      try { await producer.disconnect(); } catch { }
      logger.info('[OrderService] HTTP server closed');
      process.exit(0);
    });
    setTimeout(() => process.exit(1), 15000).unref();
  };

  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT',  () => shutdown('SIGINT'));

  process.on('unhandledRejection', (reason, promise) => {
    logger.error('[OrderService] Unhandled Promise Rejection', {
      reason: String(reason),
      stack:  reason instanceof Error ? reason.stack : undefined,
    });
    // Do NOT exit — log and continue so the service stays alive
  });

  process.on('uncaughtException', (err) => {
    logger.error('[OrderService] Uncaught Exception — forcing shutdown', {
      error: err.message,
      stack: err.stack,
    });
    process.exit(1);
  });
}

start();
