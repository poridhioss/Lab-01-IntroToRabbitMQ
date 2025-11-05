const amqp = require('amqplib')
const config = require('../config/rabbitmq.config')
const Logger = require('./utils/logger')

const logger = new Logger('CONSUMER');

let connection = null;
let channel = null;


/**
 * Simulate sending an email
 * In production, this would use SendGrid, AWS SES, etc.
 */
async function sendEmail(recipient, subject, body) {
  return new Promise((resolve) => {
    // Simulate network delay
    setTimeout(() => {
      logger.info('Email sent successfully', {
        recipient,
        subject,
      });
      resolve({ success: true, sentAt: new Date().toISOString() });
    }, 1000);
  });
}

/**
 * Simulate sending an SMS
 * In production, this would use Twilio, AWS SNS, etc.
 */
async function sendSMS(recipient, body) {
  return new Promise((resolve) => {
    setTimeout(() => {
      logger.info('SMS sent successfully', {
        recipient,
        body: body.substring(0, 50) + '...',
      });
      resolve({ success: true, sentAt: new Date().toISOString() });
    }, 800);
  });
}


/**
 * Simulate sending a push notification
 * In production, this would use Firebase, OneSignal, etc.
 */
async function sendPushNotification(recipient, subject, body) {
  return new Promise((resolve) => {
    setTimeout(() => {
      logger.info('Push notification sent successfully', {
        recipient,
        subject,
      });
      resolve({ success: true, sentAt: new Date().toISOString() });
    }, 500);
  });
}

/**
 * Process notification based on type
 */

async function processNotification(message) {
    const { id, type, recipient, subject, body, timestamp } = message;

    logger.info('Processing notification', {
        messageId: id,
        type,
        recipient,
        queuedAt: timestamp,
    });

    try {
        let result;

        switch (type) {
            case 'email':
                result = await sendEmail(recipient, subject, body);
                break;
            case 'sms':
                result = await sendSMS(recipient, body);
                break;
            case 'push':
                result = await sendPushNotification(recipient, subject, body);
                break;
            default:
                throw new Error(`Unsupported notification type: ${type}`);
        }

        logger.info('Notification processed successfully', {
            messageId: id,
            type,
            processingTime: Date.now() - new Date(timestamp).getTime() + 'ms',
        });

        return result;
    } catch (error) {
        logger.error('Error processing notification', {
            messageId: id,
            error: error.message,
        });
        throw error;
    }
}

/**
 * Initialize RabbitMQ Consumer
 */
async function initConsumer() {
    try {
        logger.info('Connecting to RabbitMQ...');

        // Create connection
        connection = await amqp.connect(config.url);
        logger.info('RabbitMQ connection established');

        // Create channel
        channel = await connection.createChannel();
        logger.info('RabbitMQ channel created');

        // Set prefetch count (number of unacknowledged messages allowed)
        await channel.prefetch(1);
        logger.info('Channel prefetch count set to 1');

        // Assert Queue
        await channel.assertQueue(config.queue.name, config.queue.options);
        logger.info(`Queue '${config.queue.name}' is ready`);

        // Start consuming messages
        await channel.consume(
            config.queue.name,
            async (msg) => {
                if (msg === null) {
                    logger.warn('Received null message. Consumer cancelled by server');
                    return;
                }

                try {
                    const content = msg.content.toString();
                    const message = JSON.parse(content);

                    logger.info('Message received', {
                        messageId: message.id,
                        type: message.type,
                    })

                    // Process the notification
                    await processNotification(message);

                    // Acknowledge message upon successful processing
                    channel.ack(msg);
                    logger.info('Message acknowledged', { messageId: message.id });
                } catch (error) {
                    logger.error('Error processing message', { error: error.message });

                    channel.nack(msg, false, true);
                    logger.warn('Message requeued and requed for processing', { messageId: msg.properties.messageId });
                }
            },
            config.consumerOptions
        );

        logger.info('Consumer started, waiting for messages...', {
            queueName: config.queue.name,
        })

        connection.on('error', (err) => {
            logger.error('RabbitMQ connection error:', { error: err.message });
        });

        // handle connection close
        connection.on('close', () => {
            logger.warn('RabbitMQ connection closed. Attempting to reconnect...');
            setTimeout(initConsumer, 5000); // Retry connection after 5 seconds
        });
    } catch (error) {
        logger.error('Failed to initialize RabbitMQ Consumer.', { error: error.message });
        setTimeout(initConsumer, 5000); // Retry connection after 5 seconds
    }
}

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

    process.exit(0);
  } catch (error) {
    logger.error('Error during shutdown', { error: error.message });
    process.exit(1);
  }
}


// Handle shutdown signals
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));

/**
 * Start Consumer
 */
initConsumer();