/**
 * Express application server entry point for Product Service.
 * Initializes database connection, correlation middleware, product routes, Kafka producer/consumer lifecycle, and graceful shutdown handlers.
 * Launches HTTP server on configured port.
 */

const path = require('path');
require('dotenv').config();
require('dotenv').config({ path: path.resolve(__dirname, '../../../.env') });
const express = require('express');
const { connectDB } = require('../../../shared/config/dbConnect');
const { correlationId } = require('../../../shared/middleware/correlationId');
const { createServiceLogger } = require('../../../shared/utils/logger');
const { createRequestLogger } = require('../../../shared/middleware/requestLogger');
const productRoutes = require('./routes/productRoutes');
const { errorHandler } = require('./middleware/errorHandler');
const producer = require('../../../shared/kafka/producer');
const inventoryConsumer = require('./consumers/inventoryConsumer');

const logger = createServiceLogger('product-service');
const app = express();

app.use(express.json({ limit: '10kb' }));
app.use(express.urlencoded({ extended: false }));
app.use(correlationId);
app.use(createRequestLogger(logger));

app.get('/health', (_req, res) => {
  res.json({
    status: 'ok',
    service: 'product-service',
    uptime: process.uptime(),
    timestamp: new Date().toISOString(),
  });
});

app.use('/v1/products', productRoutes);

app.use((_req, res) => {
  res.status(404).json({
    success: false,
    error: { code: 'NOT_FOUND', message: 'Route not found' },
  });
});

app.use(errorHandler);

const PORT = parseInt(process.env.PRODUCT_PORT || '4002', 10);

const Product = require('./models/Product');

async function seedDefaultProducts() {
  try {
    const count = await Product.countDocuments();
    if (count === 0) {
      logger.info('[ProductService] No products found in database — seeding default product catalog...');
      await Product.create([
        {
          name: 'Wireless Noise-Canceling Headphones',
          description: 'High-fidelity audio with active noise cancellation and 30-hour battery life.',
          price: 199.99,
          category: 'Electronics',
          stock: 100,
          isActive: true,
        },
        {
          name: 'Mechanical Gaming Keyboard',
          description: 'RGB backlit mechanical keyboard with tactile switches.',
          price: 89.99,
          category: 'Electronics',
          stock: 50,
          isActive: true,
        },
        {
          name: 'Ergonomic Wireless Mouse',
          description: 'Precision wireless mouse with adjustable DPI and long battery life.',
          price: 49.99,
          category: 'Electronics',
          stock: 75,
          isActive: true,
        },
      ]);
      logger.info('[ProductService] ✅ Sample products successfully seeded into MongoDB');
    }
  } catch (err) {
    logger.warn('[ProductService] Failed to seed default products', { error: err.message });
  }
}

async function start() {
  await connectDB();
  await seedDefaultProducts();

  await producer.connect();
  await inventoryConsumer.start();

  const server = app.listen(PORT, () => {
    logger.info(`Product Service listening on port ${PORT}`);
  });

  const shutdown = (signal) => {
    logger.info(`${signal} received – shutting down gracefully`);
    server.close(async () => {
      await inventoryConsumer.disconnect();
      await producer.disconnect();
      logger.info('HTTP server closed');
      process.exit(0);
    });
    setTimeout(() => process.exit(1), 15000).unref();
  };

  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));
}

start();
