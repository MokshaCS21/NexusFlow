/**
 * Business logic service layer for order processing, item validation, and payment trigger integration.
 * Communicates with Product and Payment microservices using circuit breakers.
 * After order creation, publishes order.created to Kafka for async downstream processing.
 * Exports OrderService instance.
 */

'use strict';

const axios = require('axios');
const mongoose = require('mongoose');
const Order = require('../models/Order');
const { AppError } = require('../../../../shared/utils/errorResponse');
const { createServiceLogger } = require('../../../../shared/utils/logger');
const { CircuitBreaker } = require('../../../../shared/utils/circuitBreaker');
const producer = require('../../../../shared/kafka/producer');
const { TOPICS } = require('../../../../shared/kafka/topics');

const logger = createServiceLogger('order-service');

const PRODUCT_SERVICE_URL = process.env.PRODUCT_SERVICE_URL || 'http://localhost:4002';
const PAYMENT_SERVICE_URL = process.env.PAYMENT_SERVICE_URL || 'http://localhost:4003';

const paymentCircuitBreaker = new CircuitBreaker({ name: 'payment-service' });
const productCircuitBreaker = new CircuitBreaker({ name: 'product-service' });

class OrderService {
  async createOrder(orderData) {
    const { userId, items, shippingAddress, paymentMethod, correlationId } = orderData;

    logger.info('[OrderService] Receiving POST /orders — starting order creation', {
      userId,
      itemCount: items?.length,
      paymentMethod,
      correlationId,
    });

    // ── Validate userId is a valid MongoDB ObjectId ──────────────────────────
    // The Order schema requires userId to be an ObjectId. If a plain string is
    // passed (e.g. from a JWT sub claim), Mongoose throws a CastError before
    // save(). We convert it here and give a clear 400 if it's invalid.
    let parsedUserId;
    try {
      parsedUserId = new mongoose.Types.ObjectId(userId);
    } catch {
      logger.warn('[OrderService] userId is not a valid ObjectId — rejecting', { userId });
      throw new AppError(
        `userId "${userId}" is not a valid MongoDB ObjectId. ` +
        'Use the MongoDB ObjectId returned from POST /auth/register.',
        400,
        'INVALID_USER_ID'
      );
    }

    // ── Validate each productId ──────────────────────────────────────────────
    for (const item of items) {
      if (!mongoose.Types.ObjectId.isValid(item.productId)) {
        logger.warn('[OrderService] productId is not a valid ObjectId — rejecting', { productId: item.productId });
        throw new AppError(
          `productId "${item.productId}" is not a valid MongoDB ObjectId.`,
          400,
          'INVALID_PRODUCT_ID'
        );
      }
    }

    // ── Validate & enrich items via Product Service ──────────────────────────
    logger.info('[OrderService] Validating products via Product Service...', {
      productIds: items.map(i => i.productId),
    });

    let totalAmount = 0;
    const enrichedItems = [];

    for (const item of items) {
      try {
        const response = await productCircuitBreaker.fire(async () => {
          return await axios.get(`${PRODUCT_SERVICE_URL}/v1/products/${item.productId}`);
        });
        const product = response.data.data.product;

        if (!product || !product.isActive) {
          throw new AppError(`Product ${item.productId} is not available`, 400, 'PRODUCT_UNAVAILABLE');
        }

        const priceAtTime = product.price;
        const itemTotal   = priceAtTime * item.quantity;
        totalAmount      += itemTotal;

        enrichedItems.push({
          productId:   item.productId,
          name:        product.name,
          quantity:    item.quantity,
          priceAtTime: priceAtTime,
        });

        logger.info('[OrderService] Product validated', {
          productId: item.productId,
          name:      product.name,
          price:     priceAtTime,
          quantity:  item.quantity,
        });
      } catch (err) {
        if (err instanceof AppError) throw err;
        logger.error('[OrderService] Product validation failed', {
          productId: item.productId,
          error:     err.message,
          status:    err.response?.status,
        });
        throw new AppError(
          `Validation failed for product ${item.productId}`,
          err.response?.status || 500,
          'PRODUCT_SERVICE_ERROR'
        );
      }
    }

    // ── Save order to MongoDB ────────────────────────────────────────────────
    logger.info('[OrderService] Saving order to MongoDB...', {
      userId:      parsedUserId,
      totalAmount,
      itemCount:   enrichedItems.length,
    });

    const order = new Order({
      userId:          parsedUserId,
      items:           enrichedItems,
      totalAmount,
      shippingAddress,
      status:          'PENDING',
    });

    try {
      await order.save();
      logger.info('[OrderService] ✅ MongoDB save successful', {
        orderId:     order.id,
        status:      order.status,
        totalAmount: order.totalAmount,
      });
    } catch (err) {
      // Surface the exact Mongoose error so it never silently swallows
      logger.error('[OrderService] ❌ MongoDB save FAILED', {
        error:   err.message,
        name:    err.name,           // e.g. ValidationError, CastError
        kind:    err.kind,
        path:    err.path,
        stack:   err.stack,
      });
      throw new AppError(
        `Failed to save order to database: ${err.message}`,
        500,
        'DB_SAVE_ERROR'
      );
    }

    // ── Call Payment Service synchronously ───────────────────────────────────
    logger.info('[OrderService] Initiating payment...', {
      orderId:       order.id,
      amount:        totalAmount,
      paymentMethod,
    });

    try {
      const paymentResponse = await paymentCircuitBreaker.fire(async () => {
        return await axios.post(`${PAYMENT_SERVICE_URL}/v1/payments`, {
          userId:        parsedUserId,
          amount:        totalAmount,
          paymentMethod,
          orderId:       order.id,
        });
      });

      const payment = paymentResponse.data.data.payment;
      order.paymentId = payment.transactionId;

      if (payment.status === 'COMPLETED') {
        order.status = 'PAID';
        logger.info('[OrderService] Payment COMPLETED', {
          orderId:       order.id,
          transactionId: payment.transactionId,
        });
      } else {
        order.status = 'FAILED';
        logger.warn('[OrderService] Payment FAILED', {
          orderId:       order.id,
          transactionId: payment.transactionId,
          paymentStatus: payment.status,
        });
      }
    } catch (err) {
      logger.error('[OrderService] Payment initiation failed', {
        orderId: order.id,
        error:   err.message,
        code:    err.code,
        status:  err.status,
      });
      order.status = 'FAILED';
    }

    // ── Persist final status ─────────────────────────────────────────────────
    try {
      await order.save();
      logger.info('[OrderService] ✅ Order status persisted', {
        orderId: order.id,
        status:  order.status,
      });
    } catch (err) {
      logger.error('[OrderService] ❌ Failed to persist final order status', {
        orderId: order.id,
        error:   err.message,
      });
      // Don't throw here — the order was already saved in PENDING state, return it
    }

    // ── Publish order.created to Kafka (async, non-blocking) ─────────────────
    logger.info('[OrderService] Publishing order.created event to Kafka...', {
      orderId: order.id,
      topic:   TOPICS.ORDER_CREATED,
    });

    setImmediate(async () => {
      try {
        await producer.publish(
          TOPICS.ORDER_CREATED,
          order.id,
          {
            eventType:      'order.created',
            orderId:        order.id,
            userId:         String(parsedUserId),
            items:          enrichedItems,
            totalAmount,
            paymentMethod,
            shippingAddress,
            status:         order.status,
            paymentId:      order.paymentId || null,
            correlationId:  correlationId || null,
            timestamp:      new Date().toISOString(),
          }
        );
        logger.info('[OrderService] ✅ Kafka publish successful', {
          orderId: order.id,
          topic:   TOPICS.ORDER_CREATED,
        });
      } catch (err) {
        logger.error('[OrderService] ❌ Kafka publish failed', {
          orderId: order.id,
          topic:   TOPICS.ORDER_CREATED,
          error:   err.message,
        });
      }
    });

    logger.info('[OrderService] ✅ createOrder complete — returning saved order', {
      orderId: order.id,
      status:  order.status,
    });

    return order;
  }

  async getOrdersByUser(userId) {
    logger.info('[OrderService] Fetching orders for user', { userId });
    return await Order.find({ userId }).sort('-createdAt');
  }

  async getOrderById(orderId) {
    logger.info('[OrderService] Fetching order by ID', { orderId });
    const order = await Order.findById(orderId);
    if (!order) {
      throw new AppError('Order not found', 404, 'NOT_FOUND');
    }
    return order;
  }

  async updateOrderStatus(orderId, status) {
    const order = await Order.findByIdAndUpdate(
      orderId,
      { status },
      { new: true, runValidators: true }
    );

    if (!order) {
      throw new AppError('Order not found', 404, 'NOT_FOUND');
    }

    logger.info('[OrderService] Order status updated', { orderId, status });
    return order;
  }

  async getAllOrders() {
    return await Order.find().sort('-createdAt');
  }
}

module.exports = new OrderService();
