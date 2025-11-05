const express = require('express');
const amqp = require('amqplib');
const config = require('../config/rabbitmq.config');
const Logger = require('./utils/logger');

const logger = new Logger('PUBLISHER');
const app = express();

// Middleware
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Global variables for RabbitMQ connection and channel
let connection = null;
let channel = null;

/**
 * Initialize RabbitMQ Connection
 * Creates a connection and channel, then asserts the queue exists
 */
async function initRabbitMQ() {
  try {
    logger.info('Connecting to RabbitMQ...');
    
    // Create connection
    connection = await amqp.connect(config.url);
    logger.info('Connected to RabbitMQ successfully');

    // Create channel
    channel = await connection.createChannel();
    logger.info('Channel created successfully');

    // Assert queue exists (create if it doesn't)
    await channel.assertQueue(config.queue.name, config.queue.options);
    logger.info(`Queue '${config.queue.name}' is ready`, {
      queueName: config.queue.name,
      durable: config.queue.options.durable,
    });

    // Handle connection errors
    connection.on('error', (err) => {
      logger.error('RabbitMQ connection error', { error: err.message });
    });

    // Handle connection close
    connection.on('close', () => {
      logger.warn('RabbitMQ connection closed');
    });

    return { connection, channel };
  } catch (error) {
    logger.error('Failed to initialize RabbitMQ', { error: error.message });
    throw error;
  }
}

/**
 * Publish message to queue
 * @param {Object} message - Message payload
 * @returns {Boolean} - Success status
 */
async function publishMessage(message) {
  try {
    if (!channel) {
      throw new Error('Channel not initialized');
    }

    // Convert message to Buffer
    const messageBuffer = Buffer.from(JSON.stringify(message));

    // Send message to queue
    const result = channel.sendToQueue(
      config.queue.name,
      messageBuffer,
      config.messageOptions
    );

    if (result) {
      logger.info('Message published successfully', {
        queueName: config.queue.name,
        messageId: message.id,
      });
      return true;
    } else {
      logger.warn('Message buffered (queue full)', {
        messageId: message.id,
      });
      return false;
    }
  } catch (error) {
    logger.error('Failed to publish message', { error: error.message });
    throw error;
  }
}

/**
 * API Routes
 */

// Health check endpoint
app.get('/health', (req, res) => {
  const health = {
    status: 'OK',
    timestamp: new Date().toISOString(),
    rabbitMQ: {
      connected: connection !== null,
      channelActive: channel !== null,
    },
  };
  res.json(health);
});

// Send notification endpoint
app.post('/send-notification', async (req, res) => {
  try {
    const { type, recipient, subject, body } = req.body;

    // Validate input
    if (!type || !recipient || !body) {
      return res.status(400).json({
        success: false,
        error: 'Missing required fields: type, recipient, body',
      });
    }

    // Create message object
    const message = {
      id: `msg_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
      type, // email, sms, push
      recipient,
      subject: subject || 'Notification',
      body,
      timestamp: new Date().toISOString(),
      status: 'queued',
    };

    // Publish to RabbitMQ
    await publishMessage(message);

    logger.info('Notification queued', {
      messageId: message.id,
      type: message.type,
      recipient: message.recipient,
    });

    res.status(202).json({
      success: true,
      message: 'Notification queued for processing',
      data: {
        messageId: message.id,
        queuedAt: message.timestamp,
      },
    });
  } catch (error) {
    logger.error('Error in send-notification endpoint', {
      error: error.message,
    });
    res.status(500).json({
      success: false,
      error: 'Failed to queue notification',
    });
  }
});

// Bulk send notifications
app.post('/send-bulk-notifications', async (req, res) => {
  try {
    const { notifications } = req.body;

    if (!Array.isArray(notifications) || notifications.length === 0) {
      return res.status(400).json({
        success: false,
        error: 'notifications must be a non-empty array',
      });
    }

    const results = [];
    
    for (const notif of notifications) {
      const message = {
        id: `msg_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
        type: notif.type,
        recipient: notif.recipient,
        subject: notif.subject || 'Notification',
        body: notif.body,
        timestamp: new Date().toISOString(),
        status: 'queued',
      };

      await publishMessage(message);
      results.push({ messageId: message.id, status: 'queued' });
    }

    logger.info('Bulk notifications queued', {
      count: results.length,
    });

    res.status(202).json({
      success: true,
      message: `${results.length} notifications queued`,
      data: results,
    });
  } catch (error) {
    logger.error('Error in bulk send endpoint', {
      error: error.message,
    });
    res.status(500).json({
      success: false,
      error: 'Failed to queue bulk notifications',
    });
  }
});

// Get queue stats
app.get('/queue-stats', async (req, res) => {
  try {
    if (!channel) {
      return res.status(503).json({
        success: false,
        error: 'RabbitMQ channel not available',
      });
    }

    // Check queue status
    const queueInfo = await channel.checkQueue(config.queue.name);

    res.json({
      success: true,
      data: {
        queueName: config.queue.name,
        messageCount: queueInfo.messageCount,
        consumerCount: queueInfo.consumerCount,
      },
    });
  } catch (error) {
    logger.error('Error getting queue stats', {
      error: error.message,
    });
    res.status(500).json({
      success: false,
      error: 'Failed to get queue stats',
    });
  }
});

/**
 * Graceful Shutdown
 */
async function gracefulShutdown(signal) {
  logger.info(`Received ${signal}, starting graceful shutdown...`);

  try {
    // Close channel
    if (channel) {
      await channel.close();
      logger.info('Channel closed');
    }

    // Close connection
    if (connection) {
      await connection.close();
      logger.info('Connection closed');
    }

    // Close Express server
    server.close(() => {
      logger.info('Express server closed');
      process.exit(0);
    });

    // Force close after 10 seconds
    setTimeout(() => {
      logger.error('Forced shutdown after timeout');
      process.exit(1);
    }, 10000);
  } catch (error) {
    logger.error('Error during shutdown', { error: error.message });
    process.exit(1);
  }
}

// Handle shutdown signals
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));

/**
 * Start Server
 */
let server;

async function startServer() {
  try {
    // Initialize RabbitMQ
    await initRabbitMQ();

    // Start Express server
    server = app.listen(config.publisherPort, () => {
      logger.info(`Publisher API running on port ${config.publisherPort}`, {
        port: config.publisherPort,
        endpoints: {
          health: `http://localhost:${config.publisherPort}/health`,
          sendNotification: `http://localhost:${config.publisherPort}/send-notification`,
          bulkSend: `http://localhost:${config.publisherPort}/send-bulk-notifications`,
          queueStats: `http://localhost:${config.publisherPort}/queue-stats`,
        },
      });
    });
  } catch (error) {
    logger.error('Failed to start server', { error: error.message });
    process.exit(1);
  }
}

// Start the application
startServer();